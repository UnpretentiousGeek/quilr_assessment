import json
import os
import secrets
import tempfile
import time
from pathlib import Path

import httpx2

from task2_gateway.demo import read_records, running_server


ROOT = Path(__file__).resolve().parents[1]
GENERATION = {"messages": [{"role": "user", "content": "hello"}], "max_tokens": 8}


def environment(primary: str, secondary: str, database: Path, key: str, limit: int) -> dict:
    return {
        "PYTHONPATH": str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        "ROUTER_PRIMARY_URL": primary + "/complete", "ROUTER_SECONDARY_URL": secondary + "/complete",
        "ROUTER_PRIMARY_BEARER": "trusted-primary-secret", "ROUTER_SECONDARY_BEARER": "trusted-secondary-secret",
        "ROUTER_DB_PATH": str(database), "ROUTER_TENANTS_JSON": json.dumps({key: {"tenant_id": "demo", "tokens_per_minute": limit}}),
    }


def main():
    key = secrets.token_urlsafe(32)
    child_env = {"PYTHONPATH": str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")}
    with tempfile.TemporaryDirectory(prefix="task4-demo-") as directory:
        root = Path(directory)
        primary_records, secondary_records = root / "primary.jsonl", root / "secondary.jsonl"
        with running_server("task4_router.mock_server", environ=child_env, arguments=["--record-file", str(primary_records)]) as primary:
            with running_server("task4_router.mock_server", environ=child_env, arguments=["--record-file", str(secondary_records)]) as secondary:
                config = environment(primary, secondary, root / "success.sqlite3", key, 40)
                with running_server("task4_router.server", environ=config) as gateway:
                    with httpx2.Client(trust_env=False, timeout=8) as client:
                        before = len(read_records(primary_records))
                        denied = client.post(gateway + "/complete", json=GENERATION)
                        assert denied.status_code == 401 and len(read_records(primary_records)) == before
                        success = client.post(gateway + "/complete", json=GENERATION, headers={"Authorization": "Bearer " + key, "Cookie": "caller=secret", "X-Role": "admin"})
                        assert success.status_code == 200 and success.json()["provider"] == "primary"
                        assert read_records(primary_records)[-1]["authorization"] == "Bearer trusted-primary-secret"
                        assert read_records(primary_records)[-1]["cookie"] is None
                        print("Authenticated primary success uses trusted provider credentials. Missing auth makes zero provider calls.")
                with running_server("task4_router.server", environ=config) as gateway:
                    with httpx2.Client(trust_env=False, timeout=8) as client:
                        headers = {"Authorization": "Bearer " + key}
                        denied = client.post(gateway + "/complete", json=GENERATION, headers=headers)
                        assert denied.status_code == 429
                        print("A gateway restart preserves the disk token ledger and blocks the next reservation.")
        with running_server("task4_router.mock_server", environ=child_env, arguments=["--mode", "429", "--record-file", str(primary_records)]) as primary:
            with running_server("task4_router.mock_server", environ=child_env, arguments=["--record-file", str(secondary_records)]) as secondary:
                with running_server("task4_router.server", environ=environment(primary, secondary, root / "fallback.sqlite3", key, 60)) as gateway:
                    with httpx2.Client(trust_env=False, timeout=8) as client:
                        headers = {"Authorization": "Bearer " + key}
                        result = client.post(gateway + "/complete", json=GENERATION, headers=headers)
                        assert result.status_code == 200 and result.json()["provider"] == "secondary"
                        before = (len(read_records(primary_records)), len(read_records(secondary_records)))
                        denied = client.post(gateway + "/complete", json=GENERATION, headers=headers)
                        assert denied.status_code == 429
                        assert before == (len(read_records(primary_records)), len(read_records(secondary_records)))
                        print("A primary 429 triggers one backup. Both attempts are charged. Local denial makes zero provider calls.")
        with running_server("task4_router.mock_server", environ=child_env, arguments=["--mode", "body-timeout"]) as primary:
            with running_server("task4_router.mock_server", environ=child_env) as secondary:
                with running_server("task4_router.server", environ=environment(primary, secondary, root / "timeout.sqlite3", key, 100)) as gateway:
                    with httpx2.Client(trust_env=False, timeout=8) as client:
                        start = time.monotonic()
                        result = client.post(gateway + "/complete", json=GENERATION, headers={"Authorization": "Bearer " + key})
                        elapsed = time.monotonic() - start
                        assert result.status_code == 200 and result.json()["provider"] == "secondary"
                        assert 3.0 <= elapsed < 6
                        print("A stalled primary body reaches the real 3-second total deadline, closes, then falls back.")
    print("Demo passed. All local processes stopped and temporary files removed.")


if __name__ == "__main__":
    main()
