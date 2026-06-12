import { Effect, Option, Stream } from "effect";
import type { DurableTableError } from "../vendor/durable-operators/index.ts";
import type { ActorTableService } from "./actor-table.ts";
import { decodeOutcome } from "./outcome.ts";
import { type ExecId, isTerminal, Pending, type PeekResult } from "./receipt.ts";

/** Non-blocking read of a completion row. Pending if absent. */
export const peekReply = (
  table: ActorTableService,
  execId: ExecId,
): Effect.Effect<PeekResult, DurableTableError> =>
  Effect.map(table.replies.get(execId), (opt) =>
    Option.isSome(opt) ? decodeOutcome(opt.value.exit) : Pending,
  );

/**
 * Block until the completion row for `execId` exists, then return it. Built
 * from the public replay-then-tail `rows()` feed (DurableTable exposes no
 * public `waitForStoredRow`): the reply replays immediately if already
 * present, otherwise the subscription tails until it arrives. `runHead`
 * resolves on the first matching row.
 */
export const waitForReply = (
  table: ActorTableService,
  execId: ExecId,
): Effect.Effect<PeekResult, DurableTableError> =>
  table.replies.rows().pipe(
    Stream.filter((row) => row.execId === execId),
    Stream.map((row) => decodeOutcome(row.exit)),
    Stream.runHead,
    Effect.map((opt) => Option.getOrElse(opt, () => Pending)),
  );

/**
 * Stream the completion outcome for `execId`. Built from the same
 * replay-then-tail `rows()` feed as `waitForReply`: the reply replays
 * immediately if already present, otherwise the subscription tails until it
 * arrives. Closes once a terminal outcome is observed (a reply row is written
 * exactly once per execution, so this emits a single terminal `PeekResult` and
 * completes — mirroring effect-encore's `OperationHandle.watch`).
 */
export const watchReply = (
  table: ActorTableService,
  execId: ExecId,
): Stream.Stream<PeekResult, DurableTableError> =>
  table.replies.rows().pipe(
    Stream.filter((row) => row.execId === execId),
    Stream.map((row) => decodeOutcome(row.exit)),
    Stream.takeUntil((r) => isTerminal(r)),
  );
