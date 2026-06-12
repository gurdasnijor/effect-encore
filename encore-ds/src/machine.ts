import { Effect, type Scope, SubscriptionRef } from "effect";
import type { Behavior } from "./activation.ts";
import {
  type ActorStateRegistry,
  type CurrentActorAddress,
  registerState,
} from "./actor-state.ts";

// ─────────────────────────────────────────────────────────────────────────
// machine.ts — a declarative state-machine behavior for entity actors.
//
// Inspired by the surviving half of Effect-TS/effect-smol#2351 (`unstable/
// machine`): the part that landed is the statechart with PURE handlers that
// return the next state (commit "handlers return state no effect") + `enabled`
// guards. The part that was REMOVED — the in-process Actor/ActorSystem runtime
// — is exactly what encore-ds replaces with a durable substrate, and upstream
// dropped it because its semantics collide with a cluster layer. So we take the
// good idea (a pure, guarded transition table + snapshots) and host it on the
// durable drain instead of an in-memory mailbox.
//
// Deliberately FLAT (tagged state union × tagged event union, no compound/
// parallel states): that keeps it small and owned. If upstream's `Machine`
// stabilises in a pinnable release, `behavior()` is the seam to wrap it.
//
// The machine becomes an entity `Behavior`: a `SubscriptionRef` of state,
// published as live actor state (so `getState`/`watchState` observe it), with
// one handler per declared event tag. Inbox operations ARE the events — the
// durable drain feeds them in arrival order, and the pure handler makes replay
// safe (re-running a recorded event recomputes the same next state).
// ─────────────────────────────────────────────────────────────────────────

type Tagged = { readonly _tag: string };

export interface Transition<
  S extends Tagged,
  E extends Tagged,
  ST extends S["_tag"],
  ET extends E["_tag"],
> {
  /** Guard. When present and false, this transition does not fire and the
   *  event falls through to `onUnhandled`. */
  readonly enabled?: (
    state: Extract<S, { readonly _tag: ST }>,
    event: Extract<E, { readonly _tag: ET }>,
  ) => boolean;
  /** Pure next state — no Effect, so it is deterministic under replay. */
  readonly to: (
    state: Extract<S, { readonly _tag: ST }>,
    event: Extract<E, { readonly _tag: ET }>,
  ) => S;
}

export interface MachineDef<S extends Tagged, E extends Tagged> {
  readonly initial: S;
  /**
   * Transition table keyed by `[current state tag][event tag]`. Encoding
   * legality in the table's SHAPE is the expressive win over scattered
   * `if (phase !== ...)` guards: an event with no enabled entry for the current
   * state is, by construction, not a legal transition there.
   */
  readonly on: {
    readonly [ST in S["_tag"]]?: {
      readonly [ET in E["_tag"]]?: Transition<S, E, ST, ET>;
    };
  };
  /** Fallback when no enabled transition matches (an illegal/out-of-phase
   *  event). Defaults to identity — the event is ignored, state unchanged. */
  readonly onUnhandled?: (state: S, event: E) => S;
}

export interface Machine<S extends Tagged, E extends Tagged> {
  readonly initial: S;
  /** Pure: apply one event to a state, honouring guards + `onUnhandled`.
   *  Independently testable without any durable substrate. */
  readonly step: (state: S, event: E) => S;
  /**
   * Build an entity `Behavior`: seeds a `SubscriptionRef` at `initial`,
   * registers it as live actor state, and returns one handler per declared
   * event tag. Each handler maps the inbox payload to an event (default:
   * `{ _tag: <op tag>, ...payload }`), applies `step`, and returns the new
   * snapshot (so `execute` resolves to the post-event state).
   */
  readonly behavior: (
    toEvent?: (tag: string, payload: unknown) => E,
  ) => Behavior<ActorStateRegistry | CurrentActorAddress | Scope.Scope>;
}

const defaultToEvent = (tag: string, payload: unknown): Tagged =>
  ({ _tag: tag, ...(payload as object) }) as Tagged;

export const make = <S extends Tagged, E extends Tagged>(def: MachineDef<S, E>): Machine<S, E> => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- erase the per-(state,event) generic params for the runtime table lookup
  const table = def.on as Record<string, Record<string, Transition<S, E, any, any>>>;

  const step = (state: S, event: E): S => {
    const transition = table[state._tag]?.[event._tag];
    if (
      transition !== undefined &&
      (transition.enabled === undefined ||
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- table lookup already narrowed by tag at runtime
        transition.enabled(state as any, event as any))
    ) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- table lookup already narrowed by tag at runtime
      return transition.to(state as any, event as any);
    }
    return def.onUnhandled !== undefined ? def.onUnhandled(state, event) : state;
  };

  // The handler set = every event tag named anywhere in the transition table.
  const eventTags = new Set<string>();
  for (const byEvent of Object.values(def.on)) {
    for (const eventTag of Object.keys(byEvent ?? {})) eventTags.add(eventTag);
  }

  const behavior = (
    toEvent: (tag: string, payload: unknown) => E = defaultToEvent as (
      tag: string,
      payload: unknown,
    ) => E,
  ): Behavior<ActorStateRegistry | CurrentActorAddress | Scope.Scope> =>
    Effect.gen(function* () {
      const ref = yield* SubscriptionRef.make(def.initial);
      yield* registerState({
        get: SubscriptionRef.get(ref),
        watch: SubscriptionRef.changes(ref),
      });
      const handlers: Record<string, (payload: never) => Effect.Effect<S>> = {};
      for (const tag of eventTags) {
        handlers[tag] = (payload) =>
          SubscriptionRef.modify(ref, (s) => {
            const next = step(s, toEvent(tag, payload));
            return [next, next] as const;
          });
      }
      return handlers;
    });

  return { initial: def.initial, step, behavior };
};
