/**
 * `@firegrid/fluent-durable-streams` — HTTP API definition.
 *
 * A thin, S2-native HTTP surface over the official `@s2-dev/streamstore` SDK,
 * implementing the **S2-aligned Durable Streams** core data plane:
 *
 *   - stream lifecycle  (`ensureStream`)
 *   - append            (`append`, `appendRaw`, `appendState`) with
 *                        `matchSeqNum` (expected-tail) and `fencingToken`
 *   - close / EOF        (`close`, `ds-kind: close` control record — spec §8)
 *   - read              (`read`, `readState`, `readRaw`) with unary catch-up,
 *                        long-poll waiting, and live SSE read sessions
 *   - tail check        (`checkTail` → `TailResponse`)
 *
 * Control-plane parity (subscriptions, forks, schedules, soft-delete) is
 * intentionally out of scope: the protocol (§11–§12) places it in a separate
 * control plane over S2, not the S2-native core profile.
 *
 * Byte bodies travel as base64 (`s2-format: base64` semantics) inside the JSON
 * envelope; record headers are plain ASCII `[name, value]` pairs per spec
 * §6.1/§7.1. Clients wanting zero-copy bytes use `appendRaw` / `readRaw`.
 *
 * Streaming primitives follow the v4 HttpApi reference:
 * https://github.com/Effect-TS/mintlify-docs-v4/blob/f6d3306b18a664905d767aeaee31398ab4f1ccfd/unstable/http-api.mdx#streaming-responses
 */
import { Schema } from "effect"
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "effect/unstable/httpapi"

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

/** S2 `StreamPosition`: a sequence number paired with its assignment time. */
export const StreamPosition = Schema.Struct({
  seqNum: NonNegativeInt,
  timestamp: Schema.String,
})
export type StreamPosition = typeof StreamPosition.Type

/** S2 `AppendAck` — the half-open range `[start, end)` plus the new `tail`. */
export const AppendAck = Schema.Struct({
  start: StreamPosition,
  end: StreamPosition,
  tail: StreamPosition,
})
export type AppendAck = typeof AppendAck.Type

/** S2 `TailResponse` — wraps the tail position (spec §7.2). */
export const TailResponse = Schema.Struct({
  tail: StreamPosition,
})
export type TailResponse = typeof TailResponse.Type

/** Reserved `ds-kind` values (spec §4.2 / §8.1). */
export const RecordKind = Schema.Literals(["data", "close", "meta"])
export type RecordKind = typeof RecordKind.Type

/** A record header: plain ASCII `[name, value]` (spec §4.2). */
export const HeaderPair = Schema.Tuple([Schema.String, Schema.String])
export type HeaderPair = typeof HeaderPair.Type

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

/** One record to append; `body` is base64 (`s2-format: base64`). */
export const AppendRecordInput = Schema.Struct({
  body: Schema.String,
  headers: Schema.optional(Schema.Array(HeaderPair)),
})
export type AppendRecordInput = typeof AppendRecordInput.Type

/** Conditions shared by every append form. */
export const AppendConditions = Schema.Struct({
  /** Expected-tail write: append only if the tail equals this `seqNum` (spec §6.2). */
  matchSeqNum: Schema.optional(NonNegativeInt),
  /** Cooperative writer fencing token (spec §6.3). */
  fencingToken: Schema.optional(Schema.String),
})
export type AppendConditions = typeof AppendConditions.Type

export const AppendPayload = Schema.Struct({
  records: Schema.Array(AppendRecordInput).check(Schema.isMinLength(1)),
  ...AppendConditions.fields,
})
export type AppendPayload = typeof AppendPayload.Type

/** Query for raw-byte append / close (conditions only — body is the request stream). */
export const AppendConditionsQuery = AppendConditions
export type AppendConditionsQuery = typeof AppendConditionsQuery.Type

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

export const ReadQuery = Schema.Struct({
  /** Start reading at this absolute `seqNum` (mutually exclusive with `tailOffset`). */
  seqNum: Schema.optional(NonNegativeInt),
  /** Start `tailOffset` records before the tail. */
  tailOffset: Schema.optional(NonNegativeInt),
  /** Maximum records to return. */
  count: Schema.optional(PositiveInt),
  /** Long-poll: wait up to this many seconds for new records before returning. */
  waitSecs: Schema.optional(NonNegativeInt),
  /** Drop S2 command records from the result (defaults to `true`). */
  ignoreCommandRecords: Schema.optional(Schema.Boolean),
  /** Opt into a live S2 read session delivered as Server-Sent Events. */
  live: Schema.optional(Schema.Literals(["sse"])),
})
export type ReadQuery = typeof ReadQuery.Type

export const ReadRecord = Schema.Struct({
  seqNum: NonNegativeInt,
  /** Base64-encoded record body (`s2-format: base64`). */
  body: Schema.String,
  /** `ds-kind` of the record; `data` when the header is absent (spec §4.2). */
  kind: RecordKind,
  headers: Schema.Array(HeaderPair),
  timestamp: Schema.String,
})
export type ReadRecord = typeof ReadRecord.Type

export const ReadBatch = Schema.Struct({
  records: Schema.Array(ReadRecord),
  tail: Schema.optional(StreamPosition),
  /** `true` once a `ds-kind: close` record has been delivered — EOF (spec §8.2). */
  closed: Schema.optional(Schema.Boolean),
})
export type ReadBatch = typeof ReadBatch.Type

export const EnsureStreamResponse = Schema.Struct({
  result: Schema.Literals(["created", "updated", "noop"]),
  stream: Schema.String,
})
export type EnsureStreamResponse = typeof EnsureStreamResponse.Type

// ---------------------------------------------------------------------------
// State profile (spec: S2_DS_STATE_PROTOCOL)
// ---------------------------------------------------------------------------

export const StateOperation = Schema.Literals(["insert", "update", "delete"])
export type StateOperation = typeof StateOperation.Type

export const StateControlKind = Schema.Literals(["snapshot-start", "snapshot-end", "reset"])
export type StateControlKind = typeof StateControlKind.Type

export const StateChangeHeaders = Schema.Struct({
  operation: StateOperation,
  txid: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.String),
  eventId: Schema.optional(Schema.String),
  schema: Schema.optional(Schema.String),
})
export type StateChangeHeaders = typeof StateChangeHeaders.Type

export const StateControlHeaders = Schema.Struct({
  control: StateControlKind,
  txid: Schema.optional(Schema.String),
  timestamp: Schema.optional(Schema.String),
  seqNum: Schema.optional(Schema.String),
  schema: Schema.optional(Schema.String),
})
export type StateControlHeaders = typeof StateControlHeaders.Type

export const StateChange = Schema.Struct({
  type: Schema.NonEmptyString,
  key: Schema.NonEmptyString,
  value: Schema.optional(Schema.Json),
  old_value: Schema.optional(Schema.Json),
  headers: StateChangeHeaders,
})
export type StateChange = typeof StateChange.Type

export const StateControl = Schema.Struct({
  headers: StateControlHeaders,
})
export type StateControl = typeof StateControl.Type

export const StateMessage = Schema.Union([StateChange, StateControl])
export type StateMessage = typeof StateMessage.Type

export const StateAppendPayload = Schema.Struct({
  records: Schema.Array(StateMessage).check(Schema.isMinLength(1)),
  ...AppendConditions.fields,
})
export type StateAppendPayload = typeof StateAppendPayload.Type

export const StateRecord = Schema.Struct({
  seqNum: NonNegativeInt,
  timestamp: Schema.String,
  message: StateMessage,
})
export type StateRecord = typeof StateRecord.Type

export const StateReadBatch = Schema.Struct({
  records: Schema.Array(StateRecord),
  tail: Schema.optional(StreamPosition),
  closed: Schema.optional(Schema.Boolean),
})
export type StateReadBatch = typeof StateReadBatch.Type

// ---------------------------------------------------------------------------
// SSE event shapes — mirror S2 read events (`batch`, `ping`); no second SSE
// wire format is invented (spec §7.3, state §"live read").
// ---------------------------------------------------------------------------

export const ReadSseEvent = Schema.Union([
  Schema.Struct({ event: Schema.Literal("batch"), data: ReadBatch }),
  Schema.Struct({ event: Schema.Literal("ping"), data: Schema.Null }),
])
export type ReadSseEvent = typeof ReadSseEvent.Type

export const StateSseEvent = Schema.Union([
  Schema.Struct({ event: Schema.Literal("batch"), data: StateReadBatch }),
  Schema.Struct({ event: Schema.Literal("ping"), data: Schema.Null }),
])
export type StateSseEvent = typeof StateSseEvent.Type

// ---------------------------------------------------------------------------
// Errors — one tag per HTTP status so S2's status table (spec §13) is preserved
// on the wire instead of collapsing every failure into a single code.
// ---------------------------------------------------------------------------

const errorFields = {
  message: Schema.String,
  code: Schema.optional(Schema.String),
} as const

/** 400 — malformed request or invalid parameters. */
export class BadRequestError extends Schema.TaggedErrorClass<BadRequestError>()("BadRequestError", errorFields) {}
/** 403 — authorization or fencing failure. */
export class ForbiddenError extends Schema.TaggedErrorClass<ForbiddenError>()("ForbiddenError", errorFields) {}
/** 404 — missing basin or stream. */
export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("NotFoundError", errorFields) {}
/** 408 — request timeout. */
export class TimeoutError extends Schema.TaggedErrorClass<TimeoutError>()("TimeoutError", errorFields) {}
/** 409 — resource conflict. */
export class ConflictError extends Schema.TaggedErrorClass<ConflictError>()("ConflictError", errorFields) {}
/** 412 — append condition failed (`matchSeqNum` or `fencingToken`) (spec §6.2/§6.3). */
export class AppendConditionFailed extends Schema.TaggedErrorClass<AppendConditionFailed>()("AppendConditionFailed", {
  ...errorFields,
  reason: Schema.optional(Schema.Literals(["seq_num_mismatch", "fencing_token_mismatch"])),
}) {}
/** 416 — read range not satisfiable. */
export class RangeNotSatisfiableError extends Schema.TaggedErrorClass<RangeNotSatisfiableError>()(
  "RangeNotSatisfiableError",
  errorFields,
) {}
/** 422 — a stored record could not be decoded into a state message (this profile). */
export class StateRecordError extends Schema.TaggedErrorClass<StateRecordError>()("StateRecordError", errorFields) {}
/** 502 — unexpected upstream-S2 failure. */
export class UpstreamError extends Schema.TaggedErrorClass<UpstreamError>()("UpstreamError", errorFields) {}

/** The union the server's `apiError` maps S2 failures onto. */
export type ApiError =
  | BadRequestError
  | ForbiddenError
  | NotFoundError
  | TimeoutError
  | ConflictError
  | AppendConditionFailed
  | RangeNotSatisfiableError
  | StateRecordError
  | UpstreamError

const badRequest = BadRequestError.pipe(HttpApiSchema.status(400))
const forbidden = ForbiddenError.pipe(HttpApiSchema.status(403))
const notFound = NotFoundError.pipe(HttpApiSchema.status(404))
const timeout = TimeoutError.pipe(HttpApiSchema.status(408))
const conflict = ConflictError.pipe(HttpApiSchema.status(409))
const conditionFailed = AppendConditionFailed.pipe(HttpApiSchema.status(412))
const rangeNotSatisfiable = RangeNotSatisfiableError.pipe(HttpApiSchema.status(416))
const stateRecord = StateRecordError.pipe(HttpApiSchema.status(422))
const upstream = UpstreamError.pipe(HttpApiSchema.status(502))

// Scoped per endpoint shape so each surface advertises only what it can raise.
const ensureErrors = [badRequest, forbidden, conflict, timeout, upstream] as const
const writeErrors = [badRequest, forbidden, notFound, timeout, conflict, conditionFailed, upstream] as const
const readErrors = [badRequest, forbidden, notFound, timeout, rangeNotSatisfiable, upstream] as const
const stateReadErrors = [...readErrors, stateRecord] as const

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

export const StreamsGroup = HttpApiGroup.make("Streams")
  .add(
    HttpApiEndpoint.put("ensureStream", "/streams/:stream", {
      params: { stream: Schema.String },
      success: EnsureStreamResponse,
      error: ensureErrors,
    }),
  )
  .add(
    HttpApiEndpoint.get("checkTail", "/streams/:stream/records/tail", {
      params: { stream: Schema.String },
      success: TailResponse,
      error: readErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("append", "/streams/:stream/records", {
      params: { stream: Schema.String },
      payload: AppendPayload,
      success: AppendAck,
      error: writeErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("appendRaw", "/streams/:stream/records/raw", {
      params: { stream: Schema.String },
      query: AppendConditionsQuery,
      payload: Schema.Uint8Array.pipe(HttpApiSchema.asUint8Array()),
      success: AppendAck,
      error: writeErrors,
    }),
  )
  .add(
    HttpApiEndpoint.post("close", "/streams/:stream/close", {
      params: { stream: Schema.String },
      query: AppendConditionsQuery,
      success: AppendAck,
      error: writeErrors,
    }),
  )
  // One endpoint, two response shapes: buffered catch-up (default) or live SSE.
  .add(
    HttpApiEndpoint.get("read", "/streams/:stream/records", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: [
        ReadBatch,
        HttpApiSchema.StreamSse({ events: ReadSseEvent, error: UpstreamError }),
      ],
      error: readErrors,
    }),
  )
  // Zero-copy byte stream read; reciprocal of `appendRaw`, no base64 envelope.
  .add(
    HttpApiEndpoint.get("readRaw", "/streams/:stream/records/raw", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: HttpApiSchema.StreamUint8Array({ contentType: "application/octet-stream" }),
      error: readErrors,
    }),
  )

export const StateGroup = HttpApiGroup.make("State")
  .add(
    HttpApiEndpoint.post("appendState", "/state/:stream/records", {
      params: { stream: Schema.String },
      payload: StateAppendPayload,
      success: AppendAck,
      error: writeErrors,
    }),
  )
  .add(
    HttpApiEndpoint.get("readState", "/state/:stream/records", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: [
        StateReadBatch,
        HttpApiSchema.StreamSse({ events: StateSseEvent, error: UpstreamError }),
      ],
      error: stateReadErrors,
    }),
  )

export const DurableStreamsApi = HttpApi.make("DurableStreams")
  .add(StreamsGroup)
  .add(StateGroup)
