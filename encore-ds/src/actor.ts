import { Effect, type Schema, type Scope, Stream } from "effect";
import type { DurableTableError } from "../vendor/durable-operators/index.ts";
import { type Behavior, type Handlers, activate } from "./activation.ts";
import { type EntityIdReturn, deriveExecId, resolveId } from "./addressing.ts";
import { type ActorTableService, withActor, withActorStream } from "./actor-table.ts";
import {
  type ActorStateRegistry,
  type ActorStateUnavailable,
  CurrentActorAddress,
  listStateEntityIds,
  stateOf,
  waitForStateOf,
  watchStateOf,
} from "./actor-state.ts";
import type { EncoreConfig } from "./config.ts";
import { type InboxMessage, enqueue } from "./mailbox.ts";
import { peekReply, waitForReply, watchReply } from "./replies.ts";
import { type ExecId, type PeekResult, isFailure, isSuccess } from "./receipt.ts";

// Re-exported under the `Actor` namespace so entity handlers can publish live
// state exactly as effect-encore: `Actor.registerState({ get, watch })`.
export { registerState } from "./actor-state.ts";
export type { ActorStateHandle } from "./actor-state.ts";

// ── Operation definition ─────────────────────────────────────────────────

export interface OperationDef<P = unknown, S = unknown, E = unknown> {
  /** Deterministic id: string (entityId === primaryKey) or
   *  { entityId, primaryKey? }. Drives addressing + dedup + ExecId. */
  readonly id: (payload: P) => EntityIdReturn;
  readonly payload?: Schema.Codec<P, unknown>;
  readonly success?: Schema.Codec<S, unknown>;
  readonly error?: Schema.Codec<E, unknown>;
}

// `any` in the constraint position keeps the invariant `Schema.Codec` fields
// from rejecting concrete operation defs (exactOptionalPropertyTypes variance).
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyOperationDef = OperationDef<any, any, any>;

type PayloadOf<D> = D extends OperationDef<infer P, infer _S, infer _E> ? P : never;
type SuccessOf<D> = D extends OperationDef<infer _P, infer S, infer _E> ? S : never;
type ErrorOf<D> = D extends OperationDef<infer _P, infer _S, infer E> ? E : never;

// ── Per-operation handle (payload-only methods) ──────────────────────────

export interface OperationHandle<P, S, E> {
  /** Pure, deterministic ExecId for this dispatch. */
  readonly executionId: (payload: P) => ExecId<S, E>;
  /** Producer-only: enqueue (idempotent) and return the ExecId. No handler
   *  needs to be registered in this process. */
  readonly send: (payload: P) => Effect.Effect<ExecId<S, E>, DurableTableError, EncoreConfig>;
  /** Enqueue then await the terminal outcome; surface the success value or fail
   *  with the operation's error. */
  readonly execute: (payload: P) => Effect.Effect<S, E | DurableTableError, EncoreConfig>;
  /** Non-blocking status. */
  readonly peek: (payload: P) => Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Block until terminal, then return the PeekResult. */
  readonly waitFor: (
    payload: P,
  ) => Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Live status: replays the current outcome (if any) then tails until a
   *  terminal `PeekResult` arrives, at which point the stream completes. */
  readonly watch: (
    payload: P,
  ) => Stream.Stream<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Escape hatch: build the inbox message without dispatching. */
  readonly make: (payload: P) => InboxMessage;
}

const makeHandle = <P, S, E>(
  actorType: string,
  tag: string,
  def: OperationDef<P, S, E>,
): OperationHandle<P, S, E> => {
  const idOf = (payload: P): { execId: ExecId<S, E>; entityId: string } => {
    const resolved = resolveId(def.id(payload));
    return { execId: deriveExecId<S, E>(tag, resolved), entityId: resolved.entityId };
  };

  const make = (payload: P): InboxMessage => {
    const { execId } = idOf(payload);
    return { msgId: execId, tag, payload: JSON.stringify(payload) };
  };

  const executionId = (payload: P): ExecId<S, E> => idOf(payload).execId;

  const send = (payload: P): Effect.Effect<ExecId<S, E>, DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    return withActor(actorType, entityId, (table) =>
      Effect.as(enqueue(table, make(payload)), execId),
    );
  };

  const peek = (payload: P): Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    return withActor(
      actorType,
      entityId,
      (table) => peekReply(table, execId) as Effect.Effect<PeekResult<S, E>, DurableTableError>,
    );
  };

  const waitFor = (
    payload: P,
  ): Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    return withActor(
      actorType,
      entityId,
      (table) => waitForReply(table, execId) as Effect.Effect<PeekResult<S, E>, DurableTableError>,
    );
  };

  const execute = (payload: P): Effect.Effect<S, E | DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    return withActor(actorType, entityId, (table) =>
      Effect.gen(function* () {
        yield* enqueue(table, make(payload));
        const result = (yield* waitForReply(table, execId)) as PeekResult<S, E>;
        if (isSuccess(result)) return result.value;
        if (isFailure(result)) return yield* Effect.fail(result.error);
        if (result._tag === "Defect") return yield* Effect.die(result.cause);
        return yield* Effect.interrupt;
      }),
    );
  };

  const watch = (
    payload: P,
  ): Stream.Stream<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    return withActorStream(
      actorType,
      entityId,
      (table) =>
        watchReply(table, execId) as Stream.Stream<PeekResult<S, E>, DurableTableError>,
    );
  };

  return { executionId, send, execute, peek, waitFor, watch, make };
};

// ── Entity actor ─────────────────────────────────────────────────────────

/** The per-operation handlers an `activate` hosts, keyed by tag. */
export type EntityHandlers<Defs extends Record<string, AnyOperationDef>, R> = {
  readonly [Tag in keyof Defs & string]?: (
    payload: PayloadOf<Defs[Tag]>,
  ) => Effect.Effect<SuccessOf<Defs[Tag]>, ErrorOf<Defs[Tag]>, R>;
};

/** Options for the state reads — `materialize` runs first (e.g. a durable
 *  hydration) before the live handle is read, mirroring effect-encore. */
export interface ActorStateOptions<MError = never, MR = never> {
  readonly materialize?: Effect.Effect<unknown, MError, MR>;
}

export type EntityActor<Defs extends Record<string, AnyOperationDef>> = {
  readonly name: string;
} & {
  readonly [Tag in keyof Defs & string]: OperationHandle<
    PayloadOf<Defs[Tag]>,
    SuccessOf<Defs[Tag]>,
    ErrorOf<Defs[Tag]>
  >;
} & {
  /**
   * Host this entity: claim the drain for `entityId` and run the behavior until
   * interrupted. Activation-based — fork it, interrupt to release.
   *
   * The behavior is either a ready handlers map or an Effect that builds one
   * once per owned activation. The Effect form runs with `CurrentActorAddress`
   * in scope, so a handler can create per-entity state and publish it via
   * `Actor.registerState` for `getState`/`watchState` observers.
   */
  readonly activate: <R>(
    entityId: string,
    behavior:
      | EntityHandlers<Defs, R>
      | Effect.Effect<EntityHandlers<Defs, R>, never, R>
      // A prebuilt behavior (e.g. `Machine.make(...).behavior()`) — Effect-only
      // so an inline handlers object still resolves to the typed member above.
      | Effect.Effect<Handlers<R>, never, R>,
    options?: { readonly workerId?: string; readonly epoch?: number },
  ) => Effect.Effect<
    void,
    DurableTableError,
    Exclude<R, Scope.Scope | CurrentActorAddress> | EncoreConfig
  >;

  /** Read the live state handle registered by the entity's active drain (this
   *  process). Fails `ActorStateUnavailable` if no drain has registered state. */
  readonly getState: <State, StateError = never, MError = never, MR = never>(
    entityId: string,
    options?: ActorStateOptions<MError, MR>,
  ) => Effect.Effect<
    State,
    StateError | MError | ActorStateUnavailable,
    ActorStateRegistry | MR
  >;
  /** Stream the registered state changes for `entityId`. */
  readonly watchState: <State, StateError = never, MError = never, MR = never>(
    entityId: string,
    options?: ActorStateOptions<MError, MR>,
  ) => Stream.Stream<
    State,
    StateError | MError | ActorStateUnavailable,
    ActorStateRegistry | MR
  >;
  /** Block until the registered state satisfies `predicate`, then return it. */
  readonly waitForState: <State, StateError = never, MError = never, MR = never>(
    entityId: string,
    predicate: (state: State) => boolean,
    options?: ActorStateOptions<MError, MR>,
  ) => Effect.Effect<
    State,
    StateError | MError | ActorStateUnavailable,
    ActorStateRegistry | MR
  >;
  /** Entity ids with currently-registered state handles in this process. */
  readonly listStateEntityIds: () => Effect.Effect<
    ReadonlyArray<string>,
    never,
    ActorStateRegistry
  >;
};

export const fromEntity = <const Defs extends Record<string, AnyOperationDef>>(
  name: string,
  defs: Defs,
): EntityActor<Defs> => {
  const handles: Record<string, OperationHandle<unknown, unknown, unknown>> = {};
  for (const tag of Object.keys(defs)) {
    handles[tag] = makeHandle(name, tag, defs[tag] as OperationDef);
  }

  const activateEntity = <R>(
    entityId: string,
    behavior: Behavior<R>,
    options?: { readonly workerId?: string; readonly epoch?: number },
  ): Effect.Effect<void, DurableTableError, Exclude<R, Scope.Scope | CurrentActorAddress> | EncoreConfig> =>
    withActor(name, entityId, (table: ActorTableService) =>
      Effect.provideService(
        activate(table, options?.workerId ?? "worker", behavior, { epoch: options?.epoch ?? 0 }),
        CurrentActorAddress,
        { entityType: name, entityId },
      ),
    ) as Effect.Effect<void, DurableTableError, Exclude<R, Scope.Scope | CurrentActorAddress> | EncoreConfig>;

  const address = (entityId: string) => ({ entityType: name, entityId });

  const getStateFn = <State, StateError = never, MError = never, MR = never>(
    entityId: string,
    options?: ActorStateOptions<MError, MR>,
  ): Effect.Effect<State, StateError | MError | ActorStateUnavailable, ActorStateRegistry | MR> =>
    Effect.gen(function* () {
      if (options?.materialize !== undefined) yield* options.materialize;
      return yield* stateOf<State, StateError, never>(address(entityId));
    });

  const watchStateFn = <State, StateError = never, MError = never, MR = never>(
    entityId: string,
    options?: ActorStateOptions<MError, MR>,
  ): Stream.Stream<State, StateError | MError | ActorStateUnavailable, ActorStateRegistry | MR> =>
    Stream.unwrap(
      Effect.gen(function* () {
        if (options?.materialize !== undefined) yield* options.materialize;
        return watchStateOf<State, StateError, never>(address(entityId));
      }),
    );

  const waitForStateFn = <State, StateError = never, MError = never, MR = never>(
    entityId: string,
    predicate: (state: State) => boolean,
    options?: ActorStateOptions<MError, MR>,
  ): Effect.Effect<State, StateError | MError | ActorStateUnavailable, ActorStateRegistry | MR> =>
    Effect.gen(function* () {
      if (options?.materialize !== undefined) yield* options.materialize;
      return yield* waitForStateOf<State, StateError, never>(address(entityId), predicate);
    });

  return {
    name,
    ...handles,
    activate: activateEntity,
    getState: getStateFn,
    watchState: watchStateFn,
    waitForState: waitForStateFn,
    listStateEntityIds: () => listStateEntityIds(name),
  } as unknown as EntityActor<Defs>;
};
