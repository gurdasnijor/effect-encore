# Critique — fluent-firegrid PR #14 "Add S2-native Durable Streams HTTP API"

**PR:** https://github.com/gurdasnijor/fluent-firegrid/pull/14
**Branch reviewed:** `codex/s2-durable-streams-server`
**Package:** `packages/fluent-durable-streams`

**Reference material**

- Effect HttpApi streaming (effect-smol): https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/HTTPAPI.md
- Effect HttpApi streaming (v4 docs, pinned): https://github.com/Effect-TS/mintlify-docs-v4/blob/f6d3306b18a664905d767aeaee31398ab4f1ccfd/unstable/http-api.mdx#streaming-responses
- Spec: `docs/sdds/s2-aligned-durable-streams-coordination-sdd.md`
- Protocol: `docs/reference/durable-streams/S2_DS_PROTOCOL.md`, `PROTOCOL.md`

---

## Thesis

This PR implements a durable **streams** product as a set of **unary, fully-buffered** request/response endpoints. The one capability that the SDD and the protocol call out as the core of the surface — **live reads via S2 read sessions / SSE** — is not implemented at all. And it is omitted despite the fact that Effect's HttpApi already exposes purpose-built primitives for it (`HttpApiSchema.StreamSse`, `HttpApiSchema.StreamUint8Array`, `Stream`-returning handlers). The PR's own `api.ts` already imports and uses `HttpApiSchema` (`asUint8Array()`, `status(502)`), so the streaming constructors were one symbol away. This is an availability-aware omission, not a version gap.

The result is the right *nouns* (ensure / tail / append / read) with the wrong *shape*: a CRUD-over-S2 façade rather than the streaming substrate the spec describes.

---

## What the spec actually asks for

`S2_DS_PROTOCOL.md` (the protocol this PR claims to align to) defines reads in two tiers:

- **Unary catch-up read:** `GET /streams/{stream}/records?seq_num=42&count=100` → S2 `ReadBatch`. Clients resume from the next `seq_num`.
- **Live read:** *"Live reads should use S2 read sessions through SDKs … S2 read endpoints support waiting and SSE/S2S session behavior as described by S2."* And crucially: *"This profile does not define a second SSE event shape … S2 read SSE emits S2 read events such as `batch`, `error`, and `ping`, carrying S2 `ReadBatch` payloads."*

`s2-aligned-durable-streams-coordination-sdd.md` lists, as a core design direction, *"S2 read sessions / SSE for live reads."*

So the spec is explicit: there is a live-tail surface, it is SSE-shaped, and the event vocabulary is already fixed (`batch` / `error` / `ping`). The implementer's job was to bind that vocabulary to an HttpApi streaming endpoint — not to invent a buffered JSON envelope.

## What the PR shipped

Both read endpoints are unary and buffered (`packages/fluent-durable-streams/src/api.ts`):

```ts
HttpApiEndpoint.get("read", "/streams/:stream/records", {
  params: { stream: Schema.String },
  query: ReadQuery,
  success: ReadBatch,          // { records: Array<ReadRecord>, tail?: StreamPosition }
  error: ApiErrorResponse,
})
// …
HttpApiEndpoint.get("readState", "/state/:stream/records", {
  query: ReadQuery,
  success: StateReadBatch,      // { records: Array<StateRecord>, tail?: StreamPosition }
  error: ApiErrorResponse,
})
```

The only nod to liveness is a `waitSecs` field on `ReadQuery` threaded into the S2 SDK call (`server.ts` → `readInput` → `stop.waitSecs`). That is a **single long-poll round trip that still returns one buffered array** — not a session, not SSE, not a follow stream.

---

## Provable defects

### 1. There is no live/streaming read endpoint — the headline capability is missing

`read` and `readState` are `GET`s returning a buffered struct. The spec's `live=sse` / read-session surface (events `batch`/`error`/`ping`) is absent. `checkTail` (`GET /streams/:stream/tail`) is a **point query of the tail position** (`server.ts` returns `streamPosition(response.tail)`), not a live tail. The PR description's claim to "expose S2-native stream … tail … endpoints" therefore overstates: there is no tail/follow stream anywhere in the package.

### 2. The correct Effect primitive exists and was ignored

The v4 HttpApi docs the review was pointed at describe two streaming `success` constructors on `HttpApiSchema`:

- **`HttpApiSchema.StreamSse({ events })`** — *"Server-Sent Events. Set `events` to a schema describing each event, or `data` to a schema describing the JSON payload of a `message` event."*
- **`HttpApiSchema.StreamUint8Array({ contentType })`** — *"a raw `Uint8Array` byte stream, defaulting to `application/octet-stream`."*

Handlers return a `Stream` directly and typed clients consume a `Stream` — *"Streaming endpoints on generated clients return a `Stream` directly without unwrapping. Server failures surface as typed errors in the stream."* The effect-smol `HTTPAPI.md` shows the equivalent `HttpServerResponse.stream(stream)` over `Stream<Uint8Array>`.

A spec-faithful read would be a `StreamSse` endpoint whose `events` schema models S2's `batch`/`error`/`ping` carrying `ReadBatch`. `api.ts` already does `import { … HttpApiSchema } from "effect/unstable/httpapi"` and uses it twice — so this was a reach for a sibling constructor, not new infrastructure.

### 3. Buffered `ReadBatch` defeats backpressure and memory bounds

`ReadBatch = { records: Array<ReadRecord>, tail? }` forces the **entire** batch into memory and full JSON serialization before the first byte reaches the client. For a durable, replayable, potentially-large stream this is precisely the anti-pattern that the Stream-based HttpApi exists to avoid: incremental delivery, backpressure, and the `curl --no-buffer` smoke behavior the docs demonstrate. Even *catch-up* reads of large `seq_num` ranges should stream; here `count` is the only safety valve and it is optional.

### 4. Base64-in-JSON on the read hot path; asymmetric with append

Reads encode every record body and every header as base64 strings (`api.ts`):

```ts
export const ReadRecord = Schema.Struct({
  seqNum: NonNegativeInt,
  bodyBase64: Schema.String,                 // ~33% size tax + an encode/decode hop per record
  headers: Schema.Array(HeaderPair),         // header bytes also base64'd (see server.ts toHeaderPair)
  timestamp: Schema.String,
})
```

The append path is *better* and proves the team knows the idiom — `appendRaw` accepts a real byte body:

```ts
HttpApiEndpoint.post("appendRaw", "/streams/:stream/records/raw", {
  payload: Schema.Uint8Array.pipe(HttpApiSchema.asUint8Array()),
  success: AppendAck,
})
```

But reads never reciprocate: there is no raw byte read and no `StreamUint8Array` read. So a *byte-stream* product pays a base64 tax and an extra `TextEncoder`/`btoa` hop on every record read, throwing away S2's native `as: "bytes"` read.

### 5. Single `502` error encoding masks the handler's real statuses (correctness bug)

Every endpoint declares exactly one error, pinned to HTTP 502 (`api.ts`):

```ts
const ApiErrorResponse = ApiError.pipe(HttpApiSchema.status(502))
```

But the handlers construct `ApiError` with *meaningful, varying* statuses (`server.ts`):

```ts
// apiError(): status: error.status  (S2's real status — e.g. 409/416/…)
// decodeStateRecord(): new ApiError({ status: 422, code: "invalid-state-record" })
// apiError() fallback:  new ApiError({ status: 500, message: "Unknown S2 error" })
```

The wire status is fixed by the `HttpApiSchema.status(502)` annotation, so a `422 invalid-state-record`, or a `409`/`416` surfaced from S2, **goes out as HTTP 502** with the real status demoted to a body field. `502 Bad Gateway` is exactly the status proxies and clients treat as a transient upstream failure and retry/alarm on — so genuine 4xx client errors get retried and misreported. The richer status modeling in the handler is dead on arrival.

**Fix:** distinct `addError` variants with per-status `HttpApiSchema.status(...)` (e.g. 404/409/416/422 and a 5xx bucket) instead of a single 502 catch-all; on the streaming path, surface S2 `error` events in-band via the SSE event schema (per the spec's `error` event).

### 6. State reads inherit the gap *and* silently drop records

`readState` runs the same unary `readBytes`, then `stateReadBatch` **filters** to `ds-kind === "state"` and decodes the rest (`server.ts`):

```ts
batch.records.filter((record) => textHeaderValue(record.headers, "ds-kind") === "state")
```

Non-state records inside the requested range are dropped from the response with no signal to the caller (no gap marker, no count). For state projection — `snapshot-start` / `snapshot-end` / `reset` control plus change events — live tailing is *more* essential than for raw streams (consumers want to follow state as it changes), yet this is unary-only and lossy.

### 7. Minor: path drift and a silent default

- Spec example is `GET /streams/{stream}/records/tail`; PR uses `/streams/:stream/tail`. Small, but the PR sells S2-alignment.
- `readInput` defaults `ignoreCommandRecords` to `true`, silently dropping S2 command records unless the caller opts in — a behavioral default worth stating against the spec rather than burying in a `??`.

---

## Recommended shape

1. **Add a live read endpoint** alongside the unary one, e.g. `GET /streams/:stream/records?live=sse`, declared with `HttpApiSchema.StreamSse({ events })` where `events` models S2's `batch` / `error` / `ping` carrying `ReadBatch`. Handler returns the S2 read session as a `Stream`; the typed client gets a `Stream` back. This is the spec's "do not define a second SSE event shape" requirement satisfied directly.
2. **Offer a raw byte read** via `HttpApiSchema.StreamUint8Array({ contentType })` to mirror `appendRaw` and drop the base64 tax for byte consumers.
3. **Keep the unary `read`** for catch-up, but consider streaming its body too (`HttpServerResponse.stream` over `Stream<Uint8Array>`) for large ranges rather than buffering the whole array.
4. **Split the error model** into status-specific `addError`s so 4xx stays 4xx; reserve 502 for genuine upstream-S2 failures. Surface stream-time errors as SSE `error` events.
5. **Make state reads honest** — either tail them via the same SSE surface or document/emit the dropped-record gaps.

The nouns are right; the missing dimension is **time**. As written this is a snapshot API over a stream store. The streaming HttpApi primitives the team was pointed at turn it back into a streams API.
