# Task 1 design

Task 1 exposes two customer tools through the official Python MCP SDK. Run the module through stdio. The SDK owns framing, initialization, request IDs, method dispatch, and transport cleanup.

## Data and ownership

`CustomerArgs` and `RefundArgs` define strict Pydantic inputs. Discovery derives JSON Schema from those same models. One server module owns the input boundary, immutable mock customer fixtures, simulated refund results, and SDK handlers.

The client calls `get_customer_record` with a customer ID. It calls `trigger_refund` with a customer ID, amount, and reason. These operations return explicitly marked mock data. Refund simulation does not transfer money, change customer records, or promise persistence or deduplication.

Assumptions follow the assessment's literal constraints:

- `CUST-XXXXX` means `CUST-` followed by exactly five ASCII digits.
- Amount accepts positive finite JSON numbers, including integers. It rejects booleans and numeric strings. Floating-point values are sufficient for simulation because the server performs no financial arithmetic.
- Reason length counts raw characters, with a minimum of ten.
- Unknown argument fields fail validation.

## Error behavior

| Condition | Response |
| --- | --- |
| Invalid tool arguments or unknown tool | JSON-RPC `-32602` |
| Unknown RPC method | SDK-owned JSON-RPC `-32601` |
| Correctly formatted customer ID absent from fixtures | Tool result with `isError: true` |
| Unexpected execution or result-encoding failure | JSON-RPC `-32603` with a generic message |

Input validation has its own exception boundary. Execution and result encoding have a separate boundary. This prevents an output-model validation bug from appearing to be a caller's input error. Diagnostics go to stderr.

The assessment explicitly requests JSON-RPC errors for invalid inputs. Direct low-level SDK handlers give this control. The usual high-level tool wrapper can turn argument failures into tool results instead.

## Alternatives

| Structure | Decision |
| --- | --- |
| Direct SDK handlers and Pydantic inputs | Chosen. One validation path and explicit error classification. |
| High-level tool registration and prevalidation middleware | Rejected. Adds a second validation path and middleware ordering concerns. |

Keep a single compact production module. Separate transport, validation, repository, and service layers would add call paths without additional requirements.

# Task 2 design

Task 2 adds a separate HTTP JSON-RPC gateway and mock downstream. Task 1 continues to use stdio. The gateway owns authentication and the `admin_` prefix policy. The downstream owns tool discovery and execution.

## Request ownership

A single Starlette endpoint at `/mcp` reads a bounded request, parses its JSON-RPC envelope, authenticates the caller, and authorizes the tool before forwarding. Allowed messages retain their original bytes. Discovery includes every downstream tool, including admin tools. Discovery does not grant permission to execute them.

The data model distinguishes an absent request ID from its value. A closed role set contains `viewer` and `admin`. Server configuration maps opaque bearer credentials to those roles. Callers cannot supply their role through headers or token claims. Generate credentials randomly and keep configuration private.

The assessment needs individual HTTP JSON-RPC messages. This implementation does not promise the full MCP Streamable HTTP transport, SSE, or session multiplexing. Batch requests fail closed. The policy applies to the case-sensitive tool-name prefix `admin_`.

## Design alternatives

| Shape | Decision |
| --- | --- |
| One endpoint with explicit authorization before forwarding | Chosen. The one network call has a visible access check. |
| Authorization middleware around a separate proxy | Rejected. Body replay and middleware ordering add complexity for one route. |
| Signed JWT with role claims | Not needed. A configured opaque-token map verifies possession without issuer, signing, and expiry configuration. |

The destination comes from trusted startup configuration. The gateway does not forward caller credentials, cookies, or arbitrary headers. It disables redirects and automatic retries. Per-request clients avoid sharing downstream cookies between callers. A failed response does not prove a tool did not execute, so clients must not assume exactly-once delivery.

## Task 2 error behavior

| Condition | HTTP status | JSON-RPC code |
| --- | --- | --- |
| Malformed JSON, duplicate keys, invalid Unicode, or non-finite numbers | 400 | -32700 |
| Invalid envelope, unsupported batch, or invalid ID | 400 | -32600 |
| Missing or unsupported content type | 415 | -32600 |
| Body exceeds 65536 bytes | 413 | -32600 |
| Missing, malformed, or unknown credential | 401 with `WWW-Authenticate: Bearer` | -32000 |
| Invalid `tools/call` name or params | 200 | -32602 |
| Viewer calls an `admin_` tool | 200 | -32001, `Unauthorized Tool Call` |
| Downstream connection, protocol, or redirect failure | 502 | -32002 |
| Downstream timeout | 504 | -32002 |

Valid downstream JSON-RPC responses retain their body bytes and HTTP status. The gateway checks the response envelope and matching request ID. A malformed response becomes a generic upstream failure. Response headers are restricted to content type so downstream cookies and redirects do not reach clients.

A valid notification has no `id` field. Allowed notifications forward once. Successful notifications and locally denied notifications return HTTP 204 with no JSON-RPC body. Authentication failures and upstream failures return bodyless HTTP errors for valid notifications. A malformed envelope still receives an invalid-request response. Null IDs are not notifications and are rejected.

# Task 3 design

Task 3 owns a separate text-generation endpoint. It reads an upstream OpenAI-compatible Chat Completions stream and emits a smaller client SSE contract containing redacted text, completion, or a sanitized error. It does not forward provider metadata or tool calls.

## Design alternatives

| Structure | Decision |
| --- | --- |
| Fixed suffix window with regular expressions | Rejected. A window alone cannot protect an arbitrarily long candidate and adds delay to ordinary text. An overflow scanner would duplicate candidate state. |
| Incremental candidate state machine with bounded storage | Selected. Only undecided text waits. Explicit overflow failure prevents releasing a sensitive prefix when the supported bound is exceeded. |

The pure redactor owns text ambiguity. The protocol parser owns incremental UTF-8, SSE framing, and the single-choice text-delta profile. The HTTP response owns upstream lifetime and disconnect cleanup. This keeps transport boundaries out of the matching policy.

The upstream profile follows the [Chat Completions streaming event reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events). SSE framing follows the [WHATWG event-stream format](https://html.spec.whatwg.org/multipage/server-sent-events.html). The implemented subset and error behavior are documented below.

## Matching policy

The redactor buffers at most 320 unresolved characters per response. Longer ambiguous candidates fail closed. This also rejects an ordinary unbroken word longer than the bound. Delimiters resolve normal prose as it arrives, so no complete response is accumulated. A producer that stops in the middle of a possible email can delay that candidate until more text arrives or the stream ends.

Email matching supports ASCII dot-atom local parts up to 64 characters and DNS-style domains with at least two labels. Each domain label starts and ends with an ASCII letter or digit, permits hyphens internally, and contains at most 63 characters. The full address contains at most 254 characters. Local atoms use ``[A-Za-z0-9!#$%&'*+/=?^_`{|}~\-]``. Dots separate nonempty atoms. Quoted local parts, Unicode addresses, and obfuscated addresses are outside the supported grammar. Email matches take precedence over overlapping numeric matches. Email-shaped values above the supported local, domain-label, or address length limits fail closed. Trailing sentence periods and surrounding punctuation remain outside a completed match. Unmatched pending text is preserved only at trusted completion.

Numeric matching uses ASCII digits. SSNs are nine contiguous digits or the `DDD-DD-DDDD` form. Card-like candidates contain 13 through 19 digits, with ASCII spaces or hyphens permitted between digits. No Luhn check applies. This intentionally redacts some non-sensitive numeric identifiers. Numeric boundaries use maximal runs, so a nine-digit prefix cannot be released before the code knows whether more digits follow. Repeated spaces and hyphens are permitted within a numeric run. Runs with 10 through 12 digits or at least 20 digits remain unchanged unless the candidate exceeds the storage bound. Numeric substrings inside alphanumeric text also match, so `abc123456789def` becomes `abc[REDACTED]def`.

Only upstream `[DONE]` authorizes finalization of pending text. An upstream finish reason does not authorize success by itself. EOF without `[DONE]` is a truncated stream. Already emitted safe text remains available, but undecided text is discarded and no success event follows the error.

## HTTP and provider contract

`POST /generate` accepts JSON with a model name and 1 through 128 text messages. Each message has at most 16384 characters. The total request body is limited to 65536 bytes. Unknown request fields fail validation. The upstream destination and optional bearer credential come from trusted startup configuration.

The upstream must return HTTP 200 with `text/event-stream` and identity content encoding. The parser reads raw bytes without waiting to fill a fixed-size batch. It supports strict incremental UTF-8, an initial BOM, CR, LF, CRLF, comments, and multiline `data` fields. Other SSE fields are outside this profile. A network chunk is limited to 65536 bytes, a line to 16384 characters, and an event to 32768 characters and 64 nonblank lines. These limits also constrain comments before an event boundary.

The provider sends one choice at index zero, text in `delta.content`, and an optional assistant role. A `stop` or `length` finish reason must precede `[DONE]`. Usage-only frames, log probabilities, tool calls, and other non-text generation modes are unsupported. The gateway reconstructs its own output events rather than copying provider envelopes.

The response owns the HTTP client, upstream response, byte iterator, redactor, and protocol state. A disconnect watcher cancels a stalled upstream read even when no new bytes arrive. Cleanup runs in a shielded cancellation scope. Backpressure comes from awaiting downstream sends before reading more upstream data. There is no background output queue.

The idle timeout applies to upstream connection setup and individual reads, and to incoming request-body reads. It is not a total generation deadline. A provider can keep a request open with timely valid frames. Unresolved PII text remains buffered until classification or successful completion, so no fixed worst-case time to first safe output is promised.

## Error behavior

| Condition | Response |
| --- | --- |
| Invalid JSON, Unicode, duplicate keys, or request fields | HTTP 400 with a generic JSON error |
| Wrong request content type | HTTP 415 |
| Request body exceeds 65536 bytes | HTTP 413 |
| Request body read timeout | HTTP 408 |
| Connection or response-header failure before downstream headers | HTTP 502 with the generic SSE error |
| Upstream timeout before downstream headers | HTTP 504 with the generic SSE error |
| Malformed frame, unsupported mode, truncation, limit, or read failure after downstream headers | Existing HTTP 200 stream ends with the generic SSE error and no done event |
| Client disconnect or cancellation | Stop reading upstream, discard undecided text, and close resources |

An upstream HTTP 200 establishes downstream streaming headers before the first text frame. After headers are sent, an SSE error is the error signal. Clients must check for `event: done` before treating a generation as complete. Errors never copy upstream bodies, hostnames, credentials, or exception messages.

All storage bounds apply per active response. The implementation has no global concurrency limit or deployment authentication. The local mock can record request bodies and headers for tests; those records are test fixtures and are not enabled by default.

# Task 4 design

Task 4 adds `task4_router`, a separate non-streaming text completion service. Task 3 already demonstrates streaming. A bounded JSON completion makes complete usage reconciliation and primary timeout ownership explicit without changing the earlier tasks.

## Admission and accounting

A provider attempt is the accounting unit. Before dispatch, the router reserves estimated prompt tokens plus the requested maximum completion tokens in SQLite. The estimator uses UTF-8 message content bytes and explicit framing overhead. It is token-aware, but it is not an exact provider tokenizer. Provider model names and framing policy belong to trusted configuration.

Every attempt has a random identifier, a digest for its authenticated quota bucket, an immutable admission timestamp, a reserved charge, a current charge, and a settlement flag. A short `BEGIN IMMEDIATE` transaction evicts expired events, sums the bucket, checks the allowance, and inserts the full reservation. The default allowance is 50000 tokens in 60000 milliseconds. Events in `(now - 60000, now]` count. Events at the exact lower boundary expire. Ledger operations remove expired rows lazily; the SQLite file can retain reusable free pages afterward. An estimate above the allowance fails before network access.

A successful response supplies validated provider usage. Settlement replaces the reservation once. Caller-supplied usage is never accepted. A repeated or late settlement cannot refund twice or recreate an expired event. Unknown consumption from a 429, timeout, cancellation, malformed response, or process death keeps the full reservation. A crash between database commit and dispatch therefore consumes quota conservatively. No recovery worker retries provider work. Abandoned charges expire through the ordinary sliding window.

A valid provider usage report above the estimate becomes actual debt and causes a sanitized provider failure. Later admission observes the debt. The router cannot retroactively undo other admissions. The estimate cannot guarantee an exact cap on arbitrary provider tokenizers; strict real-token enforcement requires a verified model-specific tokenizer and output bound. The implemented invariant is atomic admission against recorded estimates and reconciled usage. Provider usage reports are trusted only as validated output from the configured provider, and cannot be cryptographically verified by this module.

A primary 429 or timeout permits one backup attempt. That attempt needs its own atomic quota admission. If capacity is insufficient, local quota denial takes precedence and the backup receives no request. Primary failures with unknown consumption remain charged. The backup has its own model and provider credential. There is no recursive retry.

## SQLite and concurrency

SQLite owns admission and settlement across independent processes sharing the same file. Connections are short-lived, transactions run in worker threads, and no database transaction spans network I/O. A persisted wall-clock high-water mark prevents a backward clock adjustment from restoring quota. Expiry pauses until wall time catches up. A forward clock jump advances the accounting window. Cross-restart physical-time guarantees under arbitrary clock changes require an external clock authority.

The window counts provider attempts at admission time. It does not count remote token-generation timestamps or limit active request concurrency. Local connection cancellation cannot guarantee that a remote provider stopped computing. Reservations represent that uncertainty until expiry. The database stores quota metadata rather than credentials, prompts, or generated text. The operator must keep the file and its WAL sidecars on a local filesystem with working SQLite locking and must preserve them across restarts. Multiple machines or network filesystems are outside this assessment.

## Routing and error boundary

The primary has a 3000 ms total deadline covering connection, write, headers, body, and parsing. Monotonic time controls the deadline. A late result cannot win the timeout race. Per-phase HTTP timeouts are disabled. A transport that reports a timeout before the total deadline produces a provider error without fallback. HTTP 429 is recognized by exact status. Other statuses, malformed responses, redirects, and connection failures return sanitized errors without fallback. The secondary has a finite total deadline as well. Local quota transactions and bounded cleanup are outside the provider deadline, so the complete HTTP request can take longer than three seconds. Cleanup has a 500 ms bound and closes the primary before dispatching the backup. Cleanup failure prevents backup dispatch. SQLite lock waits are bounded at five seconds per connection, and incoming request bodies have a five-second total deadline. Cancellation propagates after resource cleanup and does not authorize fallback.

Trusted startup configuration maps opaque API keys to tenant configuration. Request fields and role headers cannot select a quota, tenant identity, upstream destination, or usage value. Only gateway-owned provider headers and parameters pass upstream. Redirects, environment proxies, and automatic retries are disabled. The service validates bounded JSON input and provider responses, and returns a reconstructed completion. Errors use a fixed gateway-owned payload with no upstream bodies, credentials, URLs, or exception messages.

## Design comparison

| Structure | Result |
| --- | --- |
| Reserve both attempts before starting a request | Guarantees backup capacity but can reject a primary that fits. Adds request-wide recovery states and unused-backup refunds. |
| Charge each provider attempt before dispatch | Selected. Small durable event model, atomic local quota precedence, and conservative unknown-usage accounting. |

## HTTP profile and errors

`POST /complete` accepts 1 through 128 messages with roles `system`, `user`, or `assistant`. Content is text with at most 16384 characters per message. `max_tokens` is a strict integer from 1 through 32768. The complete incoming body is limited to 65536 bytes. Unknown fields, duplicate JSON keys, non-finite numbers, and invalid Unicode fail validation. Framing contributes eight estimate units per message and four per request, in addition to UTF-8 content bytes and `max_tokens`.

The configured provider receives its own model name, those messages, `max_tokens`, `stream: false`, and `n: 1`. The supported response is a bounded JSON chat completion with one text choice at index zero, an assistant message, a `stop` or `length` finish reason, and strict integer `prompt_tokens`, `completion_tokens`, and `total_tokens`. Each usage count is bounded at 1000000000. The total must equal its components, and completion usage cannot exceed the requested maximum. Missing or malformed usage rejects the response and retains the reservation. Provider bodies are limited to 131072 bytes with identity encoding. The router returns only `text`, validated `usage`, and `provider`, which is `primary` or `secondary`.

Every application HTTP failure uses `{"error":{"code":"...","message":"..."}}` with fixed public text. Upstream statuses and headers are not transparently returned.

| Status | Code | Condition |
| --- | --- | --- |
| 400 | `invalid_request` | Malformed completion input |
| 401 | `unauthorized` | Missing or invalid API key |
| 404 | `not_found` | Unknown endpoint |
| 405 | `method_not_allowed` | Unsupported method |
| 408 | `request_timeout` | Incoming body deadline exceeded |
| 413 | `request_too_large` | Incoming body over the byte limit |
| 415 | `unsupported_media_type` | Content type is not JSON |
| 429 | `rate_limited` | Primary or backup admission exceeds local quota |
| 500 | `gateway_error` | Unexpected application failure |
| 502 | `provider_error` | Unsupported status, invalid response, usage overage, or provider failure |
| 503 | `limiter_unavailable` | SQLite admission or settlement unavailable |
| 504 | `provider_timeout` | Backup total deadline exceeded |

Authentication occurs before body parsing and provider admission. Provider errors retain conservative quota charges. A database failure during admission sends no request. A database failure during reconciliation returns a generic failure while preserving the existing reservation.

