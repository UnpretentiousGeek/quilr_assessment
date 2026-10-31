import multiprocessing
import os
import sqlite3
import threading

import anyio
import pytest

from task4_router.limiter import LimiterUnavailable, RateLimited, SQLiteLimiter, key_bucket


BUCKET = key_bucket("a" * 32)
OTHER = key_bucket("b" * 32)


def test_window_boundary_lazy_eviction_and_isolation(tmp_path):
    async def check():
        clock = [100_000]
        limiter = SQLiteLimiter(tmp_path / "ledger.db", clock_ms=lambda: clock[0])
        await limiter.initialize()
        await limiter.admit(BUCKET, 30, 50)
        await limiter.admit(BUCKET, 20, 50)
        await limiter.admit(OTHER, 50, 50)
        with pytest.raises(RateLimited):
            await limiter.admit(BUCKET, 1, 50)
        clock[0] = 159_999
        assert await limiter.used(BUCKET) == 50
        clock[0] = 160_000
        await limiter.admit(BUCKET, 50, 50)
        assert await limiter.used(BUCKET) == 50
        assert await limiter.used(OTHER) == 0
    anyio.run(check)


def test_settlement_is_once_and_expired_events_do_not_resurrect(tmp_path):
    async def check():
        clock = [100_000]
        limiter = SQLiteLimiter(tmp_path / "ledger.db", clock_ms=lambda: clock[0])
        await limiter.initialize()
        event = await limiter.admit(BUCKET, 50, 100)
        assert await limiter.settle(event, 12)
        assert not await limiter.settle(event, 0)
        assert await limiter.used(BUCKET) == 12
        clock[0] += 60_000
        assert not await limiter.settle(event, 99)
        assert await limiter.used(BUCKET) == 0
        await limiter.admit(BUCKET, 100, 100)
    anyio.run(check)


def test_concurrent_double_settlement_and_overage_debt(tmp_path):
    async def check():
        limiter = SQLiteLimiter(tmp_path / "ledger.db")
        await limiter.initialize()
        event = await limiter.admit(BUCKET, 20, 50)
        results = []
        async def settle():
            results.append(await limiter.settle(event, 80))
        async with anyio.create_task_group() as group:
            for _ in range(10):
                group.start_soon(settle)
        assert sum(results) == 1
        assert await limiter.used(BUCKET) == 80
        with pytest.raises(RateLimited):
            await limiter.admit(BUCKET, 1, 50)
    anyio.run(check)


def test_backward_clock_cannot_refund_or_backdate_new_admission(tmp_path):
    async def check():
        clock = [100_000]
        path = tmp_path / "ledger.db"
        limiter = SQLiteLimiter(path, clock_ms=lambda: clock[0])
        await limiter.initialize()
        await limiter.admit(BUCKET, 20, 100)
        clock[0] = 140_000
        await limiter.admit(BUCKET, 20, 100)
        restarted = SQLiteLimiter(path, clock_ms=lambda: 1)
        await restarted.initialize()
        await restarted.admit(BUCKET, 20, 100)
        assert await restarted.used(BUCKET) == 60
        clock[0] = 160_000
        assert await limiter.used(BUCKET) == 40
        clock[0] = 200_000
        assert await limiter.used(BUCKET) == 0
    anyio.run(check)


def _process_admit(path, barrier, queue):
    async def check():
        limiter = SQLiteLimiter(path)
        barrier.wait(timeout=10)
        try:
            await limiter.admit(BUCKET, 10, 30)
            queue.put("admitted")
        except RateLimited:
            queue.put("limited")
        except Exception as error:
            queue.put(type(error).__name__)
    anyio.run(check)


def test_atomic_admission_across_processes(tmp_path):
    path = tmp_path / "ledger.db"
    anyio.run(SQLiteLimiter(path).initialize)
    context = multiprocessing.get_context("spawn")
    barrier, queue = context.Barrier(8), context.Queue()
    processes = [context.Process(target=_process_admit, args=(str(path), barrier, queue)) for _ in range(8)]
    try:
        for process in processes:
            process.start()
        results = [queue.get(timeout=20) for _ in processes]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        assert results.count("admitted") == 3
        assert results.count("limited") == 5
        assert anyio.run(SQLiteLimiter(path).used, BUCKET) == 30
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        queue.close()


def _crash_after_admission(path):
    anyio.run(SQLiteLimiter(path).admit, BUCKET, 30, 30)
    os._exit(0)


def test_committed_charge_survives_process_exit_without_settlement(tmp_path):
    path = tmp_path / "ledger.db"
    anyio.run(SQLiteLimiter(path).initialize)
    process = multiprocessing.get_context("spawn").Process(target=_crash_after_admission, args=(str(path),))
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 0
    assert anyio.run(SQLiteLimiter(path).used, BUCKET) == 30
    with pytest.raises(RateLimited):
        anyio.run(SQLiteLimiter(path).admit, BUCKET, 1, 30)


def test_cancellation_waits_for_committed_thread_admission(tmp_path, monkeypatch):
    async def check():
        limiter = SQLiteLimiter(tmp_path / "ledger.db")
        await limiter.initialize()
        started, release = threading.Event(), threading.Event()
        original = limiter._admit
        def slow(*arguments):
            started.set()
            assert release.wait(timeout=5)
            return original(*arguments)
        monkeypatch.setattr(limiter, "_admit", slow)
        scopes = []
        async def worker():
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                await limiter.admit(BUCKET, 20, 50)
        async with anyio.create_task_group() as group:
            group.start_soon(worker)
            while not started.is_set():
                await anyio.sleep(0.001)
            scopes[0].cancel()
            release.set()
        assert await limiter.used(BUCKET) == 20
    anyio.run(check)


def test_database_failure_is_closed_and_connections_release_files(tmp_path):
    async def check():
        limiter = SQLiteLimiter(tmp_path / "missing" / "ledger.db")
        with pytest.raises(LimiterUnavailable):
            await limiter.initialize()
        path = tmp_path / "ledger.db"
        limiter = SQLiteLimiter(path)
        await limiter.initialize()
        await limiter.admit(BUCKET, 10, 20)
        path.unlink()
        with pytest.raises(LimiterUnavailable):
            await limiter.admit(BUCKET, 10, 20)
    anyio.run(check)


@pytest.mark.parametrize("path", ["", ":memory:", "file:shared?mode=memory"])
def test_disk_path_required(path):
    with pytest.raises(ValueError):
        SQLiteLimiter(path)


@pytest.mark.parametrize("reserve,limit", [(True, 50), (0, 50), (-1, 50), (1, False), (1, 0), (1, 10**20)])
def test_invalid_admission_numbers(tmp_path, reserve, limit):
    limiter = SQLiteLimiter(tmp_path / "ledger.db")
    with pytest.raises(ValueError):
        anyio.run(limiter.admit, BUCKET, reserve, limit)


def test_sqlite_contains_only_digest_and_accounting(tmp_path):
    async def check():
        path = tmp_path / "ledger.db"
        limiter = SQLiteLimiter(path)
        await limiter.initialize()
        await limiter.admit(key_bucket("private-key-" + "x" * 32), 12, 50)
        with sqlite3.connect(path) as connection:
            row = connection.execute("SELECT * FROM admissions").fetchone()
            columns = [item[1] for item in connection.execute("PRAGMA table_info(admissions)")]
        assert columns == ["event_id", "bucket", "admitted_ms", "reserved", "charged", "settled"]
        assert row[1] == key_bucket("private-key-" + "x" * 32)
        assert b"private-key" not in path.read_bytes()
    anyio.run(check)
