# Corrected implementation — fluent-firegrid PR #14 durable streams HTTP API

Drop-in replacements for `packages/fluent-durable-streams/src/api.ts` and `server.ts`
on `fluent-firegrid` branch `codex/s2-durable-streams-server`. They live in this repo
because this session is scoped to `gurdasnijor/effect-encore` and cannot push to
`fluent-firegrid` (the git proxy reports "repository not authorized", and no tool can
add it to scope).

## What changed and why

| Defect in the PR | Fix |
| --- | --- |
| No live read — the spec's headline `S2 read sessions / SSE for live reads` was unimplemented; reads only returned a buffered JSON array | `read` / `readState` now declare `success: [Batch, HttpApiSchema.StreamSse({ events, error })]` and stream S2 `batch` events when called with `?live=sse` |
| Bytes round-tripped as base64-in-JSON on the read hot path | new `readRaw` endpoint via `HttpApiSchema.StreamUint8Array`, streaming record bodies straight from S2's `as: "bytes"` read |
| Buffered array → no backpressure, whole batch in memory before first byte | live/raw handlers return a `Stream` over an S2 read session |
| Single `HttpApiSchema.status(502)` masked every real status (422/409/416…) → 4xx errors went out as `502 Bad Gateway` | error model split into status-distinct tagged errors; `apiError` routes S2 failures onto 409 / 416 / 422 / 502 |

Grounded in the v4 HttpApi streaming docs:
https://github.com/Effect-TS/mintlify-docs-v4/blob/f6d3306b18a664905d767aeaee31398ab4f1ccfd/unstable/http-api.mdx#streaming-responses
and effect-smol `HTTPAPI.md`.

## One thing to confirm against the repo

The S2 read-**session** entry point is marked `// ⟵ confirm S2 SDK` in `server.ts`
(`profile.basin.stream(stream).readSession(...)`). The PR's existing unary
`.read(...)` returns a single batch; the live surface needs the SDK's session /
`AsyncIterable<ReadBatch>`. Reconcile that one call with `@s2-dev/streamstore`, then
`bun run` typecheck + tests. `errors.ts` and `s2.ts` are unchanged from the PR.

## Suggested tests

- `read?live=sse` yields a `Stream` and emits `batch` events.
- a `SeqNumMismatchError` from S2 exits as HTTP **409** (not 502) — the regression the
  old single-status encoding hid.
- `readRaw` returns `Stream<Uint8Array>` with no base64 framing.
