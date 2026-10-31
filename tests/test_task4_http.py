import os
from pathlib import Path

import httpx2
import pytest

from task2_gateway.demo import read_records, running_server
from task4_router.demo import GENERATION, environment, main


ROOT = Path(__file__).resolve().parents[1]


def test_subprocess_demo_auth_success_fallback_timeout_rate_limit_and_restart(capsys):
    main()
    output = capsys.readouterr().out
    assert "Demo passed." in output
    assert "restart preserves" in output
    assert "real 3-second" in output


@pytest.mark.parametrize("mode", ["error", "malformed", "oversized"])
def test_real_http_sanitized_provider_failure_does_not_fall_back(tmp_path, mode):
    child_env = {"PYTHONPATH": str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")}
    primary_records, secondary_records = tmp_path / "primary.jsonl", tmp_path / "secondary.jsonl"
    key = "live-http-key-" + "x" * 32
    with running_server("task4_router.mock_server", environ=child_env, arguments=["--mode", mode, "--record-file", str(primary_records)]) as primary:
        with running_server("task4_router.mock_server", environ=child_env, arguments=["--record-file", str(secondary_records)]) as secondary:
            with running_server("task4_router.server", environ=environment(primary, secondary, tmp_path / "ledger.db", key, 100)) as gateway:
                with httpx2.Client(trust_env=False, timeout=8) as client:
                    result = client.post(gateway + "/complete", json=GENERATION, headers={"Authorization": "Bearer " + key})
                assert result.status_code == 502
                assert set(result.json()) == {"error"} and set(result.json()["error"]) == {"code", "message"}
                assert "private" not in result.text and "stack" not in result.text and str(tmp_path) not in result.text
                assert "X-Private" not in result.headers
                assert len(read_records(primary_records)) == 1 and not read_records(secondary_records)
