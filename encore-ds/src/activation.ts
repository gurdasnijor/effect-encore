import { Effect, Option, type Scope, Stream } from "effect";
import type { DurableTableError } from "../vendor/durable-operators/index.ts";
import type { ActorTableService } from "./actor-table.ts";
import { runStep } from "./driver.ts";
import { CANCEL_TAG } from "./mailbox.ts";
import { encodeOutcome, exitToOutcome } from "./outcome.ts";
import { makeExecId } from "./receipt.ts";

export type Handlers<R> = Readonly<
  Record<string, (payload: never) => Effect.Effect<unknown, unknown, R>>
>;

/** The behavior an `activate` hosts: either a ready handlers map, or an Effect
 *  that builds one once per owned activation. The Effect form is what lets a
 *  handler set up per-entity state (a `SubscriptionRef`) and publish it via
 *  `Actor.registerState` before the drain starts — mirroring effect-encore's
 *  entity behavior, where state is created at activation and shared across
 *  message handlers. */
export type Behavior<R> = Handlers<R> | Effect.Effect<Handlers<R>, never, R>;

export interface ActivateOptions {
  /** Drain claim epoch. Takeover re-keys to a higher epoch (never revokes the
   *  immortal `insertOrGet` row). v1 default: 0. */
  readonly epoch?: number;
}

const drainOne = <R>(
  table: ActorTableService,
  msg: { readonly msgId: string; readonly tag: string; readonly payload: string },
  handlers: Handlers<R>,
): Effect.Effect<void, DurableTableError, R> =>
  Effect.gen(function* () {
    if (msg.tag === CANCEL_TAG) return; // control message, not a job
    // Replay-safe: a completion row IS the durable "already executed" marker.
    // After a crash mid-drain, re-subscription replays this message but the
    // existing reply short-circuits it — no double-execute.
    const existing = yield* table.replies.get(msg.msgId);
    if (Option.isSome(existing)) return;
    const handler = handlers[msg.tag];
    if (handler === undefined) return;
    const execId = makeExecId(msg.msgId);
    const payload = JSON.parse(msg.payload) as never;
    const exit = yield* Effect.exit(runStep(table, execId, handler(payload)));
    yield* table.replies.upsert({
      execId: msg.msgId,
      exit: encodeOutcome(exitToOutcome(exit)),
    });
  });

/**
 * Activate the actor: claim the single-writer drain, then drain the inbox feed.
 *
 * Claim == `insertOrGet` on the owner collection (not a lock — first-writer by
 * key). `Inserted` => we own the drain; `Found` with a different worker => we
 * back off. The drain subscribes the replay-then-tail messages feed and runs
 * one handler at a time, recording each outcome to the replies collection.
 *
 * Runs until interrupted (the feed tails forever) — the caller forks it and
 * interrupts to release. No resident state: wake -> claim -> drain -> release.
 *
 * The whole owned drain runs in its own `Scope`, so a behavior that publishes
 * live state via `Actor.registerState` is deregistered when the activation ends
 * (interrupt closes the scope, running the deregister finalizer). The behavior
 * runs ONLY for the owner — a loser claim returns before building it, so a
 * non-owning process never registers state for the entity.
 */
export const activate = <R>(
  table: ActorTableService,
  workerId: string,
  behavior: Behavior<R>,
  options?: ActivateOptions,
): Effect.Effect<void, DurableTableError, Exclude<R, Scope.Scope>> =>
  Effect.scoped(
    Effect.gen(function* () {
      const epoch = options?.epoch ?? 0;
      const claim = yield* table.owner.insertOrGet({
        key: `drain/${epoch}`,
        worker: workerId,
        epoch,
      });
      if (claim._tag === "Found" && claim.row.worker !== workerId) {
        return; // another worker owns this drain epoch
      }
      const handlers = Effect.isEffect(behavior) ? yield* behavior : behavior;
      yield* Stream.runForEach(table.messages.rows(), (msg) => drainOne(table, msg, handlers));
    }),
  );
