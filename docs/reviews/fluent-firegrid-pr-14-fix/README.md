# Corrected implementation — fluent-firegrid PR #14 durable streams HTTP API

Production-grade replacements for `packages/fluent-durable-streams/src/api.ts` and
`server.ts` on `fluent-firegrid` branch `codex/s2-durable-streams-server`, implementing
the **S2-aligned Durable Streams core data plane** in full.

They live in this repo because this session is scoped to `gurdasnijor/effect-encore`
and cannot push to `fluent-firegrid` (the git proxy reports "repository not
authorized", and no tool can add it to scope). `errors.ts` / `s2.ts` are unchanged
from the PR and are referenced as-is.

## Scope: the full S2-native core (spec §11.1 / §1–§8, §13)

| Capability | Endpoint(s) |
| --- | --- |
| Stream lifecycle | `PUT /streams/:stream` |
| Append (+ `matchSeqNum`, `fencingToken`) | `POST /streams/:stream/records`, `…/records/raw` |
| Close / EOF (`ds-kind: close`, spec §8) | `POST /streams/:stream/close` |
| Unary + long-poll read | `GET /streams/:stream/records` |
| Live SSE read session | `GET /streams/:stream/records?live=sse` |
| Zero-copy byte read | `GET /streams/:stream/records/raw` |
| Tail check (`TailResponse`) | `GET /streams/:stream/records/tail` |
| State append / read (+ live) | `POST /state/:stream/records`, `GET /state/:stream/records` |

> **Deliberately out of scope** (spec §11–§12 places these in a *separate* control
> plane over S2, not the core profile): subscriptions, forks, soft-delete, scheduled
> append, producer-tuple replay. They need admission control / background workers and
> should not contaminate the S2-native data plane.

## What changed vs PR #14

| Defect | Fix |
| --- | --- |
| No live read — the spec's headline `S2 read sessions / SSE for live reads` was unimplemented | `read`/`readState` declare `success: [Batch, HttpApiSchema.StreamSse({ events, error })]`, gated on `?live=sse`, streaming S2 `batch` events over a read session and terminating on EOF |
| Buffered array, no backpressure; base64-in-JSON on the read hot path | live/raw handlers return a `Stream`; new `readRaw` via `HttpApiSchema.StreamUint8Array` streams record bodies with no base64 |
| **Single `status(502)` masking every error** | one tagged error per status — S2's native code is preserved across the whole table (400/403/404/408/409/412/416), unknown→502 |
| **Append-condition failures mislabeled** | `SeqNumMismatch` / `FencingTokenMismatch` now map to **412 `AppendConditionFailed`** with a `reason` (`seq_num_mismatch` / `fencing_token_mismatch`) per spec §6.2/§6.3 — not 409 |
| Headers base64'd as `Uint8Array` pairs | plain ASCII `[name, value]` pairs per spec §6.1/§7.1 |
| No close / EOF concept | `close` endpoint appends a `ds-kind: close` record; reads surface `closed: true`; live reads end after it |
| `tail` at a non-spec path returning a bare position | `GET /streams/:stream/records/tail` → `TailResponse { tail }` (spec §7.2) |
| State reads silently lossy | unchanged filter behaviour, but EOF (`closed`) is now surfaced and live tailing is available |

Streaming primitives pinned to the v4 HttpApi reference
(`HttpApiSchema.StreamSse` / `StreamUint8Array`, `Stream`-returning handlers, the
`success: [buffered, stream]` form) and effect-smol `HTTPAPI.md`.

## One integration point to confirm in-repo

`server.ts` marks the S2 read-**session** call `// ⟵ confirm S2 SDK`
(`profile.basin.stream(stream).readSession(readInput(query), { as: "bytes" })`). The
PR's unary `.read(...)` returns a single batch; the live surface needs the SDK's
session / `AsyncIterable<ReadBatch>`. Everything else uses calls already present in the
PR (`append`, `read`, `checkTail`, `streams.ensure`, `AppendRecord.bytes/string`).
Run `bun run` typecheck + tests after dropping these into the package.

## Test matrix to add (`apiError` is a pure function — highest-value coverage)

- `SeqNumMismatchError`  → `AppendConditionFailed { reason: "seq_num_mismatch" }`, HTTP **412**
- `FencingTokenMismatchError` → `AppendConditionFailed { reason: "fencing_token_mismatch" }`, HTTP **412**
- `S2Error{ status: 404 }` → `NotFoundError`, HTTP **404** (status passthrough, *not* 502)
- `RangeNotSatisfiableError` → HTTP **416**
- unknown cause → `UpstreamError`, HTTP **502**
- `read?live=sse` yields a `Stream` emitting `batch` events and ends after a `ds-kind: close` record
- `readRaw` yields `Stream<Uint8Array>` with no base64 framing
- a `close` append makes the next read report `closed: true`
