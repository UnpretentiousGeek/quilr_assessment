import json
import tempfile
import time
from pathlib import Path

import httpx2

from task2_gateway.demo import read_records, running_server

from .mock_server import EXPECTED, PREFIX


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="task3-demo-") as directory:
        recorder = Path(directory) / "provider.jsonl"
        gate = Path(directory) / "continue"
        with running_server("task3_gateway.mock_server", arguments=["--record-file", str(recorder), "--gate-file", str(gate)]) as upstream:
            environment = {"LLM_UPSTREAM_URL": upstream + "/v1/chat/completions"}
            with running_server("task3_gateway.server", environ=environment) as gateway:
                start = time.monotonic()
                first_output = None
                output = ""
                done = False
                with httpx2.Client(trust_env=False, timeout=5) as client:
                    with client.stream(
                        "POST", gateway + "/generate",
                        json={"model": "local-demo", "messages": [{"role": "user", "content": "Show the local fixture"}]},
                    ) as response:
                        assert response.status_code == 200
                        event = ""
                        for line in response.iter_lines():
                            if line.startswith("event: "):
                                event = line[7:]
                            elif line.startswith("data: "):
                                data = json.loads(line[6:])
                                if event == "text":
                                    if first_output is None:
                                        first_output = time.monotonic()
                                        assert not any(record["event"] == "completed" for record in read_records(recorder))
                                        gate.touch()
                                    output += data["text"]
                                elif event == "done":
                                    done = True
                                else:
                                    raise AssertionError("Gateway reported a stream error")
                completed = time.monotonic()
                assert done and output == PREFIX + EXPECTED
                assert first_output is not None and first_output < completed
                deadline = time.monotonic() + 2
                records = read_records(recorder)
                while not any(record["event"] == "completed" for record in records) and time.monotonic() < deadline:
                    time.sleep(0.01)
                    records = read_records(recorder)
                provider_completed = next(record["time"] for record in records if record["event"] == "completed")
                assert first_output < provider_completed
                requests = [record for record in records if record["event"] == "request"]
                assert len(requests) == 1
                assert requests[0]["body"]["stream"] is True and requests[0]["body"]["n"] == 1
                print(output)
                print(f"First safe output: {first_output - start:.3f}s. Stream complete: {completed - start:.3f}s.")
                print("Output and streaming request validated against the local fixture.")
    print("Demo passed. Both local server processes stopped.")


if __name__ == "__main__":
    main()
