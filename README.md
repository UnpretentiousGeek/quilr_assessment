# Assessment implementation

## Run the Task 1 MCP server

Use Python 3.12 or newer and [uv](https://docs.astral.sh/uv/).
Install the pinned dependencies from this directory.

```sh
uv sync
```

Start the stdio server.

```sh
uv run python -m task1_mcp.server
```

Connect an MCP client using that command, or run the included client demo.
The demo discovers both tools, looks up a customer, simulates a refund, and shows an invalid amount error.

```sh
uv run python -m task1_mcp.demo
```

Run the behavior checks.

```sh
uv run pytest -q
```

Use `get_customer_record` with this arguments object.

```json
{"customer_id": "CUST-00001"}
```

Use `trigger_refund` with this arguments object.

```json
{"customer_id": "CUST-00001", "amount": 25.5, "reason": "Duplicate charge"}
```

`CUST-00001` and `CUST-00002` are immutable mock fixtures.
Both tools return JSON text and structured content that standard MCP clients can read.
Refunds return `status="mock_refund_simulated"`, `mock=true`, and `payment_processed=false`.
They do not contact a payment service, change customer data, or store refund history.
Repeated calls create mock receipts without any persistence or deduplication guarantee.

Interpret `CUST-XXXXX` as `CUST-` followed by exactly five ASCII digits.
Pydantic validates strict, frozen input models and rejects extra fields.
An amount must be a positive, finite JSON number. Integer and fractional numbers are accepted.
Booleans and numeric strings are rejected. Amounts use floating point because this simulation performs no money arithmetic.
A reason must have at least ten raw characters. Whitespace counts, and the server does not trim it.

Invalid tool arguments and unknown tool names return JSON-RPC `-32602` errors.
An unknown, correctly formatted customer ID returns a tool result with `isError=true` and `code="CUSTOMER_NOT_FOUND"`.
Unexpected execution or result-encoding failures return a generic JSON-RPC `-32603` error.
The direct official SDK handlers preserve the assessment's required JSON-RPC validation errors instead of automatically wrapping them as tool errors.
The SDK owns initialization, JSON-RPC dispatch, and stdio transport.
Discovery schemas come from the same Pydantic models used for validation.

The server writes only protocol messages to stdout. Startup, validation, and failure diagnostics go to stderr.
The tests launch actual server processes with separate streams and a supported MCP handshake.
They check request IDs, strict inputs, session recovery, mock behavior, and internal-error classification.

## Run the Task 2 security gateway

Task 2 has its own HTTP mock. Keep Task 1's stdio server separate.

Run the self-contained HTTP demonstration.

```sh
uv run python -m task2_gateway.demo
```

The demo starts the mock and gateway locally, discovers tools, calls a public tool as a viewer, blocks a viewer's admin call, and permits an admin call. It checks the downstream request count to prove that the blocked call never reached the mock. It also checks authentication failure and stops its servers when finished.

To run the servers yourself, start the mock in one terminal.

```sh
uv run python -m task2_gateway.mock_server --host 127.0.0.1 --port 8002
```

In a second terminal, create two random tokens and configure the gateway.

```sh
export VIEWER_TOKEN="$(uv run python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export ADMIN_TOKEN="$(uv run python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export GATEWAY_TOKEN_ROLES="$(uv run python -c 'import json, os; print(json.dumps({os.environ["VIEWER_TOKEN"]: "viewer", os.environ["ADMIN_TOKEN"]: "admin"}))')"
export GATEWAY_UPSTREAM_URL=http://127.0.0.1:8002/mcp
uv run python -m task2_gateway.server --host 127.0.0.1 --port 8001
```

Use the same viewer token in a client terminal to discover tools.

```sh
curl http://127.0.0.1:8001/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

The gateway authenticates both discovery and calls. It returns the downstream tool list unchanged, including admin tools. A viewer cannot invoke a name beginning with the case-sensitive prefix `admin_`.

```sh
curl http://127.0.0.1:8001/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"blocked","method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
```

The denied call returns this JSON-RPC error without contacting the downstream server.

```json
{"jsonrpc":"2.0","id":"blocked","error":{"code":-32001,"message":"Unauthorized Tool Call"}}
```

Replace `$VIEWER_TOKEN` with `$ADMIN_TOKEN` to permit that call. Tokens are opaque credentials, not encoded roles or unverified JWTs. Keep them private. Their server-side mapping is the authority. Tokens must contain 32 to 256 URL-safe ASCII characters. Removing a token from configuration and restarting revokes it. There is no identity provider or automatic token expiry in this local assessment.

`GATEWAY_TIMEOUT_SECONDS` sets a positive finite downstream timeout in seconds and defaults to 5. `GATEWAY_UPSTREAM_BEARER` optionally supplies a separate downstream credential. Client authorization, cookies, and arbitrary request headers do not pass through. The gateway does not follow redirects or retry tool calls.

This implementation supports one JSON-RPC message per HTTP POST at `/mcp`. It is not a full MCP Streamable HTTP implementation. It does not support SSE or MCP session multiplexing. Requests are limited to 65536 bytes. Batch requests and null IDs are rejected. String and integer IDs are preserved, including zero and the empty string. See [DESIGN.md](DESIGN.md) for the authorization boundary.

The mock is an unauthenticated local fixture. Keep it on loopback for this demonstration. A deployed gateway needs TLS for client credentials and a downstream that clients cannot access around the gateway.

Run all Task 1 and Task 2 checks.

```sh
uv run pytest -q
```

Tasks 3 and 4 are described below.

## Run the Task 3 streaming guardrail

Run the self-contained local demonstration.

```sh
uv run --locked python -m task3_gateway.demo
```

The demo starts a local streaming provider and gateway, checks the redacted output, and stops both servers. It demonstrates local HTTP streaming, not verification against a real provider. No provider credentials are needed for the mock.

Run the complete assessment tests.

```sh
uv run --locked pytest -q
```

Task 3 uses an explicit, bounded pattern policy. It handles ASCII emails, SSNs, and card-like digit sequences across stream boundaries. It does not claim to identify every real-world email or every representation of PII. See [DESIGN.md](DESIGN.md) for the supported syntax, false positives, protocol limits, and failure behavior.

To run the two Task 3 servers yourself, start the mock in one terminal.

```sh
uv run --locked python -m task3_gateway.mock_server --host 127.0.0.1 --port 8004
```

Start the gateway in another terminal.

```sh
export LLM_UPSTREAM_URL=http://127.0.0.1:8004/v1/chat/completions
uv run --locked python -m task3_gateway.server --host 127.0.0.1 --port 8003
```

Send a streaming request.

```sh
curl -N http://127.0.0.1:8003/generate \
  -H 'Content-Type: application/json' \
  -d '{"model":"local-demo","messages":[{"role":"user","content":"Show the local fixture"}]}'
```

The client receives `event: text` with a JSON `text` field, followed by `event: done` with `{}` on success. An `event: error` contains `{"error":"Generation failed"}` and ends the stream without a done event. This is the gateway's own SSE contract, not a transparent copy of the provider response.

`LLM_UPSTREAM_URL` is required and names the complete upstream endpoint. Remote endpoints require HTTPS; HTTP is allowed for loopback hosts. `LLM_UPSTREAM_BEARER` optionally supplies the upstream credential. `LLM_IDLE_TIMEOUT_SECONDS` defaults to 5 and must be positive and finite. Caller credentials and cookies do not pass through. The gateway disables redirects and retries.

Requests contain only `model` and `messages`. Message roles are `system`, `user`, or `assistant`, and content must be text. The gateway sends `stream: true` and `n: 1` upstream. It does not support tools, images, multiple choices, or arbitrary provider parameters. The local endpoint has no caller authentication; keep this assessment demo on loopback. Task 2's authentication belongs to its separate JSON-RPC gateway.

## Run the Task 4 completion router

Run the local demonstration without provider credentials.

```sh
uv run --locked python -m task4_router.demo
```

The demo uses deterministic providers and a temporary on-disk quota database. It checks primary success, HTTP 429 fallback, the actual 3000 ms primary timeout, quota enforcement, and restart persistence. It shuts down its servers afterward. This demonstrates local behavior, with no real-provider validation.

To run the service yourself, configure two complete provider endpoint URLs, an on-disk database, and a trusted key-to-tenant mapping. API keys contain 32 through 512 visible ASCII characters. Use a random key for your local client.

```sh
export TASK4_API_KEY="$(uv run python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export ROUTER_TENANTS_JSON="$(uv run python -c 'import json, os; print(json.dumps({os.environ["TASK4_API_KEY"]: {"tenant_id": "local-tenant", "tokens_per_minute": 50000}}))')"
export ROUTER_DB_PATH="$PWD/task4-quota.sqlite3"
export ROUTER_PRIMARY_URL=https://primary.example/v1/chat/completions
export ROUTER_SECONDARY_URL=https://secondary.example/v1/chat/completions
export ROUTER_PRIMARY_MODEL=primary-text-model
export ROUTER_SECONDARY_MODEL=backup-text-model
uv run --locked python -m task4_router.server --host 127.0.0.1 --port 8005
```

Replace the example URLs and models with your configured providers. `ROUTER_PRIMARY_BEARER` and `ROUTER_SECONDARY_BEARER` optionally supply separate provider credentials. Remote endpoints require HTTPS. Loopback HTTP is allowed for local fixtures. The demo manages fixture configuration automatically.

Send a non-streaming text completion request.

```sh
curl http://127.0.0.1:8005/complete \
  -H "Authorization: Bearer $TASK4_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Explain a sliding window briefly."}],"max_tokens":128}'
```

The router owns the model choice and provider parameters. Caller fields cannot change the tenant, quota, token usage, provider endpoint, or role. Each API key has its own SHA-256 quota bucket. `tenant_id` is a trusted label, so rotating to a new key creates a new bucket. Multiple keys for the same label have separate allowances. Keep configuration consistent across processes sharing the database.

The limit defaults to 50000 estimated tokens per rolling 60 seconds. Each provider attempt reserves UTF-8 prompt bytes with framing overhead plus `max_tokens`. Valid successful usage reconciles that charge once. Unknown usage after a failure, timeout, cancellation, or crash keeps the reservation until expiry. Fallback consumes a second admission and can be refused if local quota is exhausted. No network request occurs for a denied admission.

The estimate is not an exact provider tokenizer. Reported usage above the estimate becomes debt and returns a generic failure. This enforces atomic admission against recorded budget, with the provider-tokenization limits described in [DESIGN.md](DESIGN.md). Only primary HTTP 429 and the total 3000 ms timeout trigger fallback. Other provider failures return standardized gateway errors.

Preserve the SQLite file and its sidecars between restarts. Use a local disk with SQLite locking. The database stores quota metadata and key digests without raw API keys, prompts, or outputs. The gateway has no external identity service or distributed multi-machine quota store. Keep this local assessment service on loopback; remote operation requires appropriate credential transport and network controls.

Run all four tasks' checks.

```sh
uv run --locked pytest -q
```

