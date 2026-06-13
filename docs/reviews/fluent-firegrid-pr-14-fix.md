# Fix patch — fluent-firegrid PR #14 durable streams HTTP API

> **Where to apply:** `packages/fluent-durable-streams/src/api.ts` and `src/server.ts`
> on branch `codex/s2-durable-streams-server`.
>
> **Why it lives here:** this session is scoped to `gurdasnijor/effect-encore`; the
> git proxy refuses `fluent-firegrid` ("repository not authorized") and no tool can
> add it to scope, so I can't push to that PR from here. This file is the ready-to-apply
> patch. Add `fluent-firegrid` to the session (or paste this in) and I'll push it directly.
>
> **Grounding:** the HttpApi pieces are pinned to the documented v4 streaming API —
> `HttpApiSchema.StreamSse({ events | data, error })`, `HttpApiSchema.StreamUint8Array({ contentType })`,
> handlers returning a `Stream`, and the `success: [buffered, stream]` mixed form
> (https://github.com/Effect-TS/mintlify-docs-v4/blob/f6d3306b18a664905d767aeaee31398ab4f1ccfd/unstable/http-api.mdx#streaming-responses).
> The single integration point I can't verify from outside the repo is the exact
> `@s2-dev/streamstore` **read-session** method — marked `// ⟵ confirm S2 SDK` below.

Defects this patch closes (see the companion critique for the full argument):

1. No live/streaming read — the spec's headline `S2 read sessions / SSE for live reads`.
2. Buffered-array reads with no backpressure; base64-in-JSON on the read hot path.
3. A single `HttpApiSchema.status(502)` masking real handler statuses (422/409/416/…).

---

## 1. `api.ts` — error model split by status

Replace the single `ApiErrorResponse` with status-distinct tagged errors so 4xx stays
4xx on the wire and `502` is reserved for genuine upstream-S2 failures.

```ts
// --- errors, one tag per HTTP status so the encoding is honest -------------
class BadRequestError extends Schema.TaggedErrorClass<BadRequestError>()(
  "BadRequestError", { message: Schema.String, code: Schema.optional(Schema.String) },
) {}
class ConflictError extends Schema.TaggedErrorClass<ConflictError>()(            // seq-num / fencing mismatch
  "ConflictError", { message: Schema.String, code: Schema.optional(Schema.String) },
) {}
class RangeError_ extends Schema.TaggedErrorClass<RangeError_>()(                // S2 RangeNotSatisfiable
  "RangeNotSatisfiableError", { message: Schema.String, code: Schema.optional(Schema.String) },
) {}
class UnprocessableError extends Schema.TaggedErrorClass<UnprocessableError>()(  // invalid state record
  "UnprocessableError", { message: Schema.String, code: Schema.optional(Schema.String) },
) {}
class UpstreamError extends Schema.TaggedErrorClass<UpstreamError>()(            // genuine S2 failure / unknown
  "UpstreamError", { message: Schema.String, code: Schema.optional(Schema.String) },
) {}

// Re-export the union so server.ts can build the right member.
export const StreamApiError = {
  BadRequest: BadRequestError, Conflict: ConflictError, Range: RangeError_,
  Unprocessable: UnprocessableError, Upstream: UpstreamError,
} as const

// Used on every endpoint's `error:` slot.
const errors = [
  BadRequestError.pipe(HttpApiSchema.status(400)),
  ConflictError.pipe(HttpApiSchema.status(409)),
  RangeError_.pipe(HttpApiSchema.status(416)),
  UnprocessableError.pipe(HttpApiSchema.status(422)),
  UpstreamError.pipe(HttpApiSchema.status(502)),
] as const
```

Then `error: ApiErrorResponse` becomes `error: errors` on each endpoint.

## 2. `api.ts` — SSE event schema + a live read endpoint

The spec says *"this profile does not define a second SSE event shape … S2 read SSE
emits S2 read events such as `batch`, `error`, and `ping`, carrying S2 `ReadBatch`."*
Model exactly that vocabulary and add a streaming variant of `read` via the documented
mixed `success: [buffered, stream]` form, gated on `live`:

```ts
// SSE events mirror S2: `batch` carries a ReadBatch, `ping` is a heartbeat.
export const ReadSseEvent = Schema.Union([
  Schema.Struct({ event: Schema.Literal("batch"), data: ReadBatch }),
  Schema.Struct({ event: Schema.Literal("ping"),  data: Schema.Null }),
])

export const ReadQuery = Schema.Struct({
  seqNum: Schema.optional(NonNegativeInt),
  tailOffset: Schema.optional(NonNegativeInt),
  count: Schema.optional(PositiveInt),
  waitSecs: Schema.optional(NonNegativeInt),
  ignoreCommandRecords: Schema.optional(Schema.Boolean),
  live: Schema.optional(Schema.Literals(["sse"])),   // ⟵ opt into the session/SSE surface
})

// read: one declaration, two response shapes — buffered catch-up OR live SSE.
HttpApiEndpoint.get("read", "/streams/:stream/records", {
  params: { stream: Schema.String },
  query: ReadQuery,
  success: [
    ReadBatch,                                              // live omitted → buffered
    HttpApiSchema.StreamSse({ events: ReadSseEvent, error: UpstreamError }),
  ],
  error: errors,
})
```

Apply the same `success: [StateReadBatch, HttpApiSchema.StreamSse({ events: StateSseEvent, … })]`
shape to `readState` (state projection wants live tailing *more* than raw streams).

## 3. `api.ts` — raw byte read to mirror `appendRaw`

Byte consumers shouldn't pay the base64 tax. `appendRaw` already takes a real
`Uint8Array`; give reads the reciprocal:

```ts
HttpApiEndpoint.get("readRaw", "/streams/:stream/records/raw", {
  params: { stream: Schema.String },
  query: ReadQuery,
  success: HttpApiSchema.StreamUint8Array({ contentType: "application/octet-stream" }),
  error: errors,
})
```

---

## 4. `server.ts` — honest status mapping

Replace the `apiError` collapse-to-`ApiError` with a mapper onto the tagged errors:

```ts
import { StreamApiError } from "./api.ts"

const apiError = (error: S2ProfileError): /* union of the tagged errors */ unknown => {
  if (error instanceof SeqNumMismatchError || error instanceof FencingTokenMismatchError) {
    return new StreamApiError.Conflict({ message: error.message, code: error.code })
  }
  if (error instanceof RangeNotSatisfiableError) {
    return new StreamApiError.Range({ message: error.message, code: error.code })
  }
  if (error instanceof S2Error) {
    // S2 surfaced a concrete HTTP status — route 4xx vs 5xx instead of flattening to 502.
    return error.status >= 400 && error.status < 500
      ? new StreamApiError.BadRequest({ message: error.message, code: error.code })
      : new StreamApiError.Upstream({ message: error.message, code: error.code })
  }
  return new StreamApiError.Upstream({ message: "Unknown S2 error" })
}
```

`decodeStateRecord` fails with `new StreamApiError.Unprocessable({ … })` instead of
`new ApiError({ status: 422 })`.

## 5. `server.ts` — live read handler returns a `Stream`

The buffered handlers stay as-is for the no-`live` path. Add the streaming branch.
The handler returns an `Effect` yielding a `Stream` of SSE events; HttpApi serializes
each as an SSE frame and the typed client gets a `Stream` back (no unwrapping).

```ts
import { Effect, Schema, Stream } from "effect"

// ⟵ confirm S2 SDK: the read-session call that yields successive ReadBatches.
//   The unary `.read(...)` used today returns one batch; the session yields many.
const readSession = (profile: S2ProfileService, stream: string, query: ReadQuery) =>
  Stream.fromAsyncIterable(
    profile.basin.stream(stream).readSession(readInput(query), { as: "bytes" }), // ⟵ confirm S2 SDK
    (cause) => apiError(cause as S2ProfileError),
  )

handlers.handle("read", ({ params, query }) =>
  Effect.gen(function*() {
    const profile = yield* S2Profile
    if (query.live === "sse") {
      // live tail: map each S2 batch to an SSE `batch` event; heartbeat via `ping`.
      return readSession(profile, params.stream, query).pipe(
        Stream.map((batch) => ({ event: "batch" as const, data: readBatch(batch) })),
      )
    }
    const response = yield* readBytes(profile, params.stream, query)   // unchanged buffered path
    return readBatch(response)
  }))
```

`readRaw` returns `readSession(...).pipe(Stream.map((b) => b.records).pipe(Stream.flattenIterables), Stream.map((r) => r.body))`
— a `Stream<Uint8Array>` straight from S2's `as: "bytes"` records, no base64.

---

## Notes / what still needs the repo

- **S2 session method name** is the only unverified symbol (`readSession` above). Confirm
  against `@s2-dev/streamstore` and adjust; everything else is pinned to the v4 HttpApi docs.
- **Type-check + tests:** add coverage that `live=sse` yields a `Stream` and that a
  `SeqNumMismatchError` exits as HTTP **409**, not 502 — the regression the old single-status
  encoding hid.
- **`ignoreCommandRecords`** default of `true` should be stated in the endpoint docs rather
  than buried in `readInput`'s `?? true`.

Once `fluent-firegrid` is reachable from a session, I can apply this directly and push to the PR branch.
