import hashlib
import sqlite3
import time
import uuid
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import anyio


WINDOW_MS = 60_000
MAX_ACCOUNTED_TOKENS = 1_000_000_000


class LimiterUnavailable(Exception):
    pass


class RateLimited(Exception):
    pass


def key_bucket(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("ascii")).hexdigest()


@dataclass(frozen=True)
class Admission:
    event_id: str
    bucket: str
    reserved: int


class SQLiteLimiter:
    """Charge each provider attempt at admission for the next 60 seconds.

    Failed, cancelled, or interrupted attempts retain their reservation. A valid
    response can settle it once. This ledger does not limit active concurrency.
    """

    def __init__(self, path: str | Path, *, clock_ms: Callable[[], int] | None = None):
        if not isinstance(path, (str, Path)):
            raise ValueError("A disk SQLite path is required")
        self.path = str(path)
        if not self.path or "\x00" in self.path or self.path == ":memory:" or self.path.startswith("file:"):
            raise ValueError("A disk SQLite path is required")
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
        except BaseException:
            connection.close()
            raise
        return connection

    async def initialize(self) -> None:
        await self._run(self._initialize)

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS clock (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    high_water_ms INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO clock VALUES (1, 0);
                CREATE TABLE IF NOT EXISTS admissions (
                    event_id TEXT PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    admitted_ms INTEGER NOT NULL,
                    reserved INTEGER NOT NULL CHECK(reserved > 0),
                    charged INTEGER NOT NULL CHECK(charged >= 0),
                    settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0, 1))
                );
                CREATE INDEX IF NOT EXISTS admissions_bucket_time
                    ON admissions(bucket, admitted_ms);
                CREATE INDEX IF NOT EXISTS admissions_time ON admissions(admitted_ms);
            """)

    def _now_and_evict(self, connection: sqlite3.Connection) -> int:
        wall = self.clock_ms()
        if type(wall) is not int or not 0 <= wall <= 9_000_000_000_000_000_000:
            raise ValueError("Invalid clock")
        previous = connection.execute("SELECT high_water_ms FROM clock WHERE singleton = 1").fetchone()[0]
        now = max(wall, previous)
        connection.execute("UPDATE clock SET high_water_ms = ? WHERE singleton = 1", (now,))
        connection.execute("DELETE FROM admissions WHERE admitted_ms <= ?", (now - WINDOW_MS,))
        return now

    async def _run(self, function, *arguments):
        try:
            return await anyio.to_thread.run_sync(function, *arguments, abandon_on_cancel=False)
        except (sqlite3.Error, OSError, ValueError, OverflowError):
            raise LimiterUnavailable() from None

    async def admit(self, bucket: str, reserve: int, limit: int = 50_000) -> Admission:
        if (
            not isinstance(bucket, str) or len(bucket) != 64
            or any(char not in "0123456789abcdef" for char in bucket)
            or type(reserve) is not int or not 1 <= reserve <= MAX_ACCOUNTED_TOKENS
            or type(limit) is not int or not 1 <= limit <= MAX_ACCOUNTED_TOKENS
        ):
            raise ValueError("Invalid admission")
        return await self._run(self._admit, bucket, reserve, limit)

    def _admit(self, bucket: str, reserve: int, limit: int) -> Admission:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._now_and_evict(connection)
            used = connection.execute(
                "SELECT COALESCE(SUM(charged), 0) FROM admissions WHERE bucket = ?", (bucket,)
            ).fetchone()[0]
            if used + reserve > limit:
                connection.commit()
                raise RateLimited()
            event = Admission(uuid.uuid4().hex, bucket, reserve)
            connection.execute(
                "INSERT INTO admissions(event_id, bucket, admitted_ms, reserved, charged) VALUES (?, ?, ?, ?, ?)",
                (event.event_id, bucket, now, reserve, reserve),
            )
            connection.commit()
            return event

    async def settle(self, admission: Admission, actual: int) -> bool:
        if type(actual) is not int or not 0 <= actual <= MAX_ACCOUNTED_TOKENS:
            raise ValueError("Invalid usage")
        return await self._run(self._settle, admission, actual)

    def _settle(self, admission: Admission, actual: int) -> bool:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._now_and_evict(connection)
            changed = connection.execute(
                "UPDATE admissions SET charged = ?, settled = 1 "
                "WHERE event_id = ? AND bucket = ? AND reserved = ? AND settled = 0",
                (actual, admission.event_id, admission.bucket, admission.reserved),
            ).rowcount
            connection.commit()
            return bool(changed)

    async def used(self, bucket: str) -> int:
        return await self._run(self._used, bucket)

    def _used(self, bucket: str) -> int:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._now_and_evict(connection)
            used = connection.execute(
                "SELECT COALESCE(SUM(charged), 0) FROM admissions WHERE bucket = ?", (bucket,)
            ).fetchone()[0]
            connection.commit()
            return used
