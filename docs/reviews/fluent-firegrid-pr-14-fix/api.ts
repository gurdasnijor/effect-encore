// Corrected `packages/fluent-durable-streams/src/api.ts` for fluent-firegrid PR #14.
//
// Changes vs the PR:
//   1. Live SSE read surface (HttpApiSchema.StreamSse) on `read` / `readState`,
//      gated on `?live=sse`, using the S2 `batch` / `ping` event vocabulary the
//      protocol mandates ("this profile does not define a second SSE event shape").
//   2. Raw byte read (HttpApiSchema.StreamUint8Array) mirroring `appendRaw`, so
//      byte consumers skip the base64-in-JSON tax.
//   3. Error model split into status-distinct tagged errors so 4xx stays 4xx on
//      the wire instead of every failure flattening to HTTP 502.
//
// HttpApi streaming primitives pinned to:
// https://github.com/Effect-TS/mintlify-docs-v4/blob/f6d3306b18a664905d767aeaee31398ab4f1ccfd/unstable/http-api.mdx#streaming-responses

import { Schema } from "effect"
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "effect/unstable/httpapi"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const StreamPosition = Schema.Struct({
  seqNum: NonNegativeInt,
  timestamp: Schema.String,
})
export type StreamPosition = typeof StreamPosition.Type

export const AppendAck = Schema.Struct({
  start: StreamPosition,
  end: StreamPosition,
  tail: StreamPosition,
})
export type AppendAck = typeof AppendAck.Type

export const HeaderPair = Schema.Tuple([Schema.String, Schema.String])
export type HeaderPair = typeof HeaderPair.Type

export const AppendRecordInput = Schema.Struct({
  bodyBase64: Schema.String,
  headers: Schema.optional(Schema.Array(HeaderPair)),
})
export type AppendRecordInput = typeof AppendRecordInput.Type

export const AppendPayload = Schema.Struct({
  records: Schema.Array(AppendRecordInput).check(Schema.isMinLength(1)),
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
})
export type AppendPayload = typeof AppendPayload.Type

export const AppendRawQuery = Schema.Struct({
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
})
export type AppendRawQuery = typeof AppendRawQuery.Type

// `live` opts a read into the S2 read-session / SSE surface. Omitted => buffered catch-up read.
export const ReadQuery = Schema.Struct({
  seqNum: Schema.optional(NonNegativeInt),
  tailOffset: Schema.optional(NonNegativeInt),
  count: Schema.optional(PositiveInt),
  waitSecs: Schema.optional(NonNegativeInt),
  ignoreCommandRecords: Schema.optional(Schema.Boolean),
  live: Schema.optional(Schema.Literals(["sse"])),
})
export type ReadQuery = typeof ReadQuery.Type

export const ReadRecord = Schema.Struct({
  seqNum: NonNegativeInt,
  bodyBase64: Schema.String,
  headers: Schema.Array(HeaderPair),
  timestamp: Schema.String,
})
export type ReadRecord = typeof ReadRecord.Type

export const ReadBatch = Schema.Struct({
  records: Schema.Array(ReadRecord),
  tail: Schema.optional(StreamPosition),
})
export type ReadBatch = typeof ReadBatch.Type

export const EnsureStreamResponse = Schema.Struct({
  result: Schema.Literals(["created", "updated", "noop"]),
  stream: Schema.String,
})
export type EnsureStreamResponse = typeof EnsureStreamResponse.Type

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
  matchSeqNum: Schema.optional(NonNegativeInt),
  fencingToken: Schema.optional(Schema.String),
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
})
export type StateReadBatch = typeof StateReadBatch.Type

// --- SSE event shapes: mirror S2 read events (`batch`, `ping`) carrying a ReadBatch. ---
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

// --- Errors: one tag per HTTP status, so the wire status is honest. ---
export class BadRequestError extends Schema.TaggedErrorClass<BadRequestError>()("BadRequestError", {
  message: Schema.String,
  code: Schema.optional(Schema.String),
}) {}
export class ConflictError extends Schema.TaggedErrorClass<ConflictError>()("ConflictError", {
  message: Schema.String,
  code: Schema.optional(Schema.String),
}) {}
export class RangeNotSatisfiableError extends Schema.TaggedErrorClass<RangeNotSatisfiableError>()(
  "RangeNotSatisfiableError",
  { message: Schema.String, code: Schema.optional(Schema.String) },
) {}
export class UnprocessableError extends Schema.TaggedErrorClass<UnprocessableError>()("UnprocessableError", {
  message: Schema.String,
  code: Schema.optional(Schema.String),
}) {}
export class UpstreamError extends Schema.TaggedErrorClass<UpstreamError>()("UpstreamError", {
  message: Schema.String,
  code: Schema.optional(Schema.String),
}) {}

export type ApiError =
  | BadRequestError
  | ConflictError
  | RangeNotSatisfiableError
  | UnprocessableError
  | UpstreamError

// Every endpoint declares the full set; 502 is reserved for genuine upstream-S2 failures.
const errors = [
  BadRequestError.pipe(HttpApiSchema.status(400)),
  ConflictError.pipe(HttpApiSchema.status(409)),
  RangeNotSatisfiableError.pipe(HttpApiSchema.status(416)),
  UnprocessableError.pipe(HttpApiSchema.status(422)),
  UpstreamError.pipe(HttpApiSchema.status(502)),
] as const

export const StreamsGroup = HttpApiGroup.make("Streams")
  .add(
    HttpApiEndpoint.put("ensureStream", "/streams/:stream", {
      params: { stream: Schema.String },
      success: EnsureStreamResponse,
      error: errors,
    }),
  )
  .add(
    HttpApiEndpoint.get("checkTail", "/streams/:stream/tail", {
      params: { stream: Schema.String },
      success: StreamPosition,
      error: errors,
    }),
  )
  .add(
    HttpApiEndpoint.post("append", "/streams/:stream/records", {
      params: { stream: Schema.String },
      payload: AppendPayload,
      success: AppendAck,
      error: errors,
    }),
  )
  .add(
    HttpApiEndpoint.post("appendRaw", "/streams/:stream/records/raw", {
      params: { stream: Schema.String },
      query: AppendRawQuery,
      payload: Schema.Uint8Array.pipe(HttpApiSchema.asUint8Array()),
      success: AppendAck,
      error: errors,
    }),
  )
  // One endpoint, two response shapes: buffered catch-up (live omitted) OR live SSE.
  .add(
    HttpApiEndpoint.get("read", "/streams/:stream/records", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: [
        ReadBatch,
        HttpApiSchema.StreamSse({ events: ReadSseEvent, error: UpstreamError }),
      ],
      error: errors,
    }),
  )
  // Raw byte stream read, reciprocal of `appendRaw`; no base64 tax.
  .add(
    HttpApiEndpoint.get("readRaw", "/streams/:stream/records/raw", {
      params: { stream: Schema.String },
      query: ReadQuery,
      success: HttpApiSchema.StreamUint8Array({ contentType: "application/octet-stream" }),
      error: errors,
    }),
  )

export const StateGroup = HttpApiGroup.make("State")
  .add(
    HttpApiEndpoint.post("appendState", "/state/:stream/records", {
      params: { stream: Schema.String },
      payload: StateAppendPayload,
      success: AppendAck,
      error: errors,
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
      error: errors,
    }),
  )

export const DurableStreamsApi = HttpApi.make("DurableStreams")
  .add(StreamsGroup)
  .add(StateGroup)
