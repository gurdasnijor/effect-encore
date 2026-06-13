/**
 * `@firegrid/fluent-durable-streams` — HTTP API handlers.
 *
 * Binds {@link DurableStreamsApi} to the official `@s2-dev/streamstore` SDK via
 * the {@link S2Profile} service. Reads come in three shapes — buffered catch-up,
 * long-poll, and live SSE read sessions — and S2 failures keep their native HTTP
 * status (spec §13) instead of collapsing to a single code.
 *
 * The only call that must be reconciled with the installed SDK version is the
 * live read **session** (`stream.readSession`), marked `// ⟵ confirm S2 SDK`.
 */
import {
  AppendInput,
  AppendRecord,
  FencingTokenMismatchError,
  RangeNotSatisfiableError as S2RangeNotSatisfiableError,
  S2Error,
  SeqNumMismatchError,
  type AppendAck as S2AppendAck,
  type ReadBatch as S2ReadBatch,
  type ReadRecord as S2ReadRecord,
  type StreamPosition as S2StreamPosition,
} from "@s2-dev/streamstore"
import { Effect, Layer, Schema, Stream } from "effect"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import type {
  AppendAck,
  ApiError,
  ReadBatch,
  ReadQuery,
  ReadRecord,
  RecordKind,
  StateMessage,
  StateReadBatch,
  StateRecord,
  StreamPosition,
} from "./api.ts"
import {
  AppendConditionFailed,
  BadRequestError,
  ConflictError,
  DurableStreamsApi,
  ForbiddenError,
  NotFoundError,
  RangeNotSatisfiableError,
  StateMessage as StateMessageSchema,
  StateRecordError,
  TimeoutError,
  UpstreamError,
} from "./api.ts"
import { S2Profile, type S2ProfileError, type S2ProfileService } from "./s2.ts"
import { tryS2 } from "./errors.ts"

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

const KIND_HEADER = "ds-kind"
const CLOSE_HEADER: readonly [string, string] = [KIND_HEADER, "close"]

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

const decodeBase64 = (value: string): Uint8Array =>
  Uint8Array.from(atob(value), (char) => char.charCodeAt(0))

const encodeBase64 = (bytes: Uint8Array): string =>
  btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""))

const fromHeaderPair = ([name, value]: readonly [string, string]): readonly [Uint8Array, Uint8Array] => [
  textEncoder.encode(name),
  textEncoder.encode(value),
]

const decodeHeaders = (
  headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
): ReadonlyArray<readonly [string, string]> =>
  headers.map(([name, value]) => [textDecoder.decode(name), textDecoder.decode(value)] as const)

const textHeaderValue = (
  headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
  name: string,
): string | undefined => {
  const value = headers.find(([candidate]) => textDecoder.decode(candidate) === name)?.[1]
  return value === undefined ? undefined : textDecoder.decode(value)
}

const recordKind = (headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>): RecordKind => {
  const value = textHeaderValue(headers, KIND_HEADER)
  return value === "close" || value === "meta" ? value : "data"
}

// ---------------------------------------------------------------------------
// S2 → API projections
// ---------------------------------------------------------------------------

const streamPosition = (position: S2StreamPosition): StreamPosition => ({
  seqNum: position.seqNum,
  timestamp: position.timestamp.toISOString(),
})

const appendAck = (ack: S2AppendAck): AppendAck => ({
  start: streamPosition(ack.start),
  end: streamPosition(ack.end),
  tail: streamPosition(ack.tail),
})

const readRecord = (record: S2ReadRecord<"bytes">): ReadRecord => ({
  seqNum: record.seqNum,
  body: encodeBase64(record.body),
  kind: recordKind(record.headers),
  headers: decodeHeaders(record.headers),
  timestamp: record.timestamp.toISOString(),
})

const isClosed = (batch: S2ReadBatch<"bytes">): boolean =>
  batch.records.some((record) => recordKind(record.headers) === "close")

const readBatch = (batch: S2ReadBatch<"bytes">): ReadBatch => ({
  records: batch.records.map(readRecord),
  ...(batch.tail === undefined ? {} : { tail: streamPosition(batch.tail) }),
  ...(isClosed(batch) ? { closed: true } : {}),
})

// ---------------------------------------------------------------------------
// State projection
// ---------------------------------------------------------------------------

const isStateChange = (message: StateMessage): message is Extract<StateMessage, { readonly type: string }> =>
  "type" in message

const stringHeader = (name: string, value: string): readonly [string, string] => [name, value]

const optionalStateHeaders = (
  headers: Readonly<{ readonly txid?: string | undefined; readonly schema?: string | undefined }>,
): ReadonlyArray<readonly [string, string]> => [
  ...(headers.txid === undefined ? [] : [stringHeader("ds-state-txid", headers.txid)]),
  ...(headers.schema === undefined ? [] : [stringHeader("ds-state-schema", headers.schema)]),
]

const stateHeaders = (message: StateMessage): ReadonlyArray<readonly [string, string]> => {
  const base = [
    stringHeader(KIND_HEADER, "state"),
    stringHeader("ds-content-type", "application/vnd.firegrid.state+json"),
  ]
  if (isStateChange(message)) {
    return [
      ...base,
      stringHeader("ds-state-kind", "change"),
      stringHeader("ds-state-type", message.type),
      stringHeader("ds-state-key", message.key),
      stringHeader("ds-state-operation", message.headers.operation),
      ...optionalStateHeaders(message.headers),
    ]
  }
  return [
    ...base,
    stringHeader("ds-state-kind", "control"),
    stringHeader("ds-state-control", message.headers.control),
    ...optionalStateHeaders(message.headers),
  ]
}

const stateAppendRecord = (message: StateMessage): AppendRecord =>
  AppendRecord.string({ body: JSON.stringify(message), headers: stateHeaders(message) })

const decodeStateRecord = (record: S2ReadRecord<"bytes">): Effect.Effect<StateRecord, ApiError> => {
  if (textHeaderValue(record.headers, KIND_HEADER) !== "state") {
    return Effect.fail(
      new StateRecordError({ message: `S2 record ${record.seqNum} is not a state record`, code: "not-state-record" }),
    )
  }
  return Schema.decodeUnknownEffect(StateMessageSchema)(JSON.parse(textDecoder.decode(record.body))).pipe(
    Effect.map((message): StateRecord => ({
      seqNum: record.seqNum,
      timestamp: record.timestamp.toISOString(),
      message,
    })),
    Effect.mapError((error) =>
      new StateRecordError({ message: `Invalid state record ${record.seqNum}: ${String(error)}`, code: "invalid-state-record" })
    ),
  )
}

const stateReadBatch = (batch: S2ReadBatch<"bytes">): Effect.Effect<StateReadBatch, ApiError> =>
  Effect.forEach(
    batch.records.filter((record) => textHeaderValue(record.headers, KIND_HEADER) === "state"),
    decodeStateRecord,
  ).pipe(
    Effect.map((records) => ({
      records,
      ...(batch.tail === undefined ? {} : { tail: streamPosition(batch.tail) }),
      ...(isClosed(batch) ? { closed: true } : {}),
    })),
  )

// ---------------------------------------------------------------------------
// Error mapping — preserve S2's native HTTP status (spec §13)
// ---------------------------------------------------------------------------

const codeOf = (error: unknown): string | undefined => (error as { readonly code?: string }).code

/**
 * Map an {@link S2ProfileError} onto a status-distinct API error. Append-condition
 * failures become `412` (not `409`); every other S2 status is preserved exactly;
 * unknown/5xx failures become `502`.
 */
export const apiError = (error: S2ProfileError): ApiError => {
  const code = codeOf(error)
  const fields = (message: string) => ({ message, ...(code === undefined ? {} : { code }) })

  if (error instanceof SeqNumMismatchError) {
    return new AppendConditionFailed({ ...fields(error.message), reason: "seq_num_mismatch" })
  }
  if (error instanceof FencingTokenMismatchError) {
    return new AppendConditionFailed({ ...fields(error.message), reason: "fencing_token_mismatch" })
  }
  if (error instanceof S2RangeNotSatisfiableError) {
    return new RangeNotSatisfiableError(fields(error.message))
  }
  if (error instanceof S2Error) {
    switch (error.status) {
      case 400:
        return new BadRequestError(fields(error.message))
      case 403:
        return new ForbiddenError(fields(error.message))
      case 404:
        return new NotFoundError(fields(error.message))
      case 408:
        return new TimeoutError(fields(error.message))
      case 409:
        return new ConflictError(fields(error.message))
      case 412:
        return new AppendConditionFailed(fields(error.message))
      case 416:
        return new RangeNotSatisfiableError(fields(error.message))
      default:
        return new UpstreamError(fields(error.message))
    }
  }
  return new UpstreamError({ message: "Unknown S2 error" })
}

const catchS2 = <A, R>(effect: Effect.Effect<A, S2ProfileError, R>): Effect.Effect<A, ApiError, R> =>
  Effect.mapError(effect, apiError)

// ---------------------------------------------------------------------------
// S2 calls
// ---------------------------------------------------------------------------

const appendOptions = (input: {
  readonly matchSeqNum?: number | undefined
  readonly fencingToken?: string | undefined
}): { readonly matchSeqNum?: number; readonly fencingToken?: string } => ({
  ...(input.matchSeqNum === undefined ? {} : { matchSeqNum: input.matchSeqNum }),
  ...(input.fencingToken === undefined ? {} : { fencingToken: input.fencingToken }),
})

const readInput = (query: ReadQuery) => ({
  start: query.tailOffset === undefined
    ? query.seqNum === undefined
      ? { clamp: true }
      : { from: { seqNum: query.seqNum }, clamp: true }
    : { from: { tailOffset: query.tailOffset }, clamp: true },
  stop: {
    limits: { ...(query.count === undefined ? {} : { count: query.count }) },
    ...(query.waitSecs === undefined ? {} : { waitSecs: query.waitSecs }),
  },
  ignoreCommandRecords: query.ignoreCommandRecords ?? true,
})

const appendRecords = (
  profile: S2ProfileService,
  stream: string,
  records: ReadonlyArray<AppendRecord>,
  conditions: { readonly matchSeqNum?: number | undefined; readonly fencingToken?: string | undefined },
) => catchS2(tryS2(() => profile.basin.stream(stream).append(AppendInput.create(records, appendOptions(conditions)))))

/** Buffered single-batch catch-up read (used when `live` is absent). */
const readBytes = (profile: S2ProfileService, stream: string, query: ReadQuery) =>
  catchS2(tryS2(() => profile.basin.stream(stream).read(readInput(query), { as: "bytes" })))

/**
 * Live read session — successive S2 `ReadBatch`es as a backpressured Stream,
 * terminating after a `ds-kind: close` record (EOF, spec §8.2).
 *
 * ⟵ confirm S2 SDK: `readSession` is the SDK's streaming entry point (the unary
 *   `read` above returns one batch; the session yields an AsyncIterable of batches).
 */
const readSession = (
  profile: S2ProfileService,
  stream: string,
  query: ReadQuery,
): Stream.Stream<S2ReadBatch<"bytes">, ApiError> =>
  Stream.fromAsyncIterable(
    profile.basin.stream(stream).readSession(readInput(query), { as: "bytes" }), // ⟵ confirm S2 SDK
    (cause) => apiError(cause as S2ProfileError),
  ).pipe(Stream.takeUntil(isClosed))

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

export const StreamsLive = HttpApiBuilder.group(
  DurableStreamsApi,
  "Streams",
  (handlers) =>
    handlers
      .handle("ensureStream", ({ params }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* catchS2(tryS2(() => profile.basin.streams.ensure({ stream: params.stream })))
          return { result: response.result, stream: response.stream.name }
        }))
      .handle("checkTail", ({ params }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const response = yield* catchS2(tryS2(() => profile.basin.stream(params.stream).checkTail()))
          return { tail: streamPosition(response.tail) }
        }))
      .handle("append", ({ params, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const records = payload.records.map((record) =>
            AppendRecord.bytes({
              body: decodeBase64(record.body),
              headers: (record.headers ?? []).map(fromHeaderPair),
            })
          )
          return appendAck(yield* appendRecords(profile, params.stream, records, payload))
        }))
      .handle("appendRaw", ({ params, query, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const record = AppendRecord.bytes({ body: payload })
          return appendAck(yield* appendRecords(profile, params.stream, [record], query))
        }))
      .handle("close", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const record = AppendRecord.bytes({ body: new Uint8Array(0), headers: [fromHeaderPair(CLOSE_HEADER)] })
          return appendAck(yield* appendRecords(profile, params.stream, [record], query))
        }))
      .handle("read", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          if (query.live === "sse") {
            return readSession(profile, params.stream, query).pipe(
              Stream.map((batch) => ({ event: "batch" as const, data: readBatch(batch) })),
            )
          }
          return readBatch(yield* readBytes(profile, params.stream, query))
        }))
      .handle("readRaw", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          // Stream raw record bodies straight from S2's `as: "bytes"` read — no base64 hop.
          return readSession(profile, params.stream, query).pipe(
            Stream.flatMap((batch) => Stream.fromIterable(batch.records)),
            Stream.map((record) => record.body),
          )
        })),
)

export const StateLive = HttpApiBuilder.group(
  DurableStreamsApi,
  "State",
  (handlers) =>
    handlers
      .handle("appendState", ({ params, payload }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          const records = payload.records.map(stateAppendRecord)
          return appendAck(yield* appendRecords(profile, params.stream, records, payload))
        }))
      .handle("readState", ({ params, query }) =>
        Effect.gen(function*() {
          const profile = yield* S2Profile
          if (query.live === "sse") {
            return readSession(profile, params.stream, query).pipe(
              Stream.mapEffect((batch) => stateReadBatch(batch)),
              Stream.map((batch) => ({ event: "batch" as const, data: batch })),
            )
          }
          return yield* stateReadBatch(yield* readBytes(profile, params.stream, query))
        })),
)

export const DurableStreamsApiLive = HttpApiBuilder.layer(DurableStreamsApi).pipe(
  Layer.provide(StreamsLive),
  Layer.provide(StateLive),
)
