import { Effect, Layer, type Schema, type Scope, Stream } from "effect";
import type { DurableTableError } from "../vendor/durable-operators/index.ts";
import { type Behavior, type Handlers, activate } from "./activation.ts";
import { type EntityIdReturn, deriveExecId, resolveId } from "./addressing.ts";
import { type ActorTableService, withActor, withActorStream } from "./actor-table.ts";
import {
  ActorStateRegistry,
  type ActorStateUnavailable,
  CurrentActorAddress,
  listStateEntityIds,
  stateOf,
  waitForStateOf,
  watchStateOf,
} from "./actor-state.ts";
import type { EncoreConfig } from "./config.ts";
import { advertiseEntity, directoryEntities } from "./directory.ts";
import { ActorKindId } from "./kind.ts";
import { type InboxMessage, enqueue } from "./mailbox.ts";
import { peekReply, waitForReply, waitForReplyMatching, watchReply } from "./replies.ts";
import { type ExecId, type PeekResult, isFailure, isSuccess, parseExecId } from "./receipt.ts";

// Re-exported under the `Actor` namespace so entity handlers can publish live
// state exactly as effect-encore: `Actor.registerState({ get, watch })`.
export { registerState } from "./actor-state.ts";
export type { ActorStateHandle } from "./actor-state.ts";

// Workflow constructors live under the `Actor` namespace too: `Actor.fromWorkflow`.
export { fromWorkflow, workflowEngineLayer } from "./workflow.ts";
export type { WorkflowActor, WorkflowDef } from "./workflow.ts";

import { type ActorKind, kindOf } from "./kind.ts";
import type { WorkflowActor } from "./workflow.ts";

/** Narrow an unknown value to an entity actor. */
export const isEntity = (
  value: unknown,
): value is EntityActor<Record<string, AnyOperationDef>> => kindOf(value) === "entity";

/** Narrow an unknown value to a workflow actor. */
export const isWorkflow = (
  value: unknown,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- WorkflowActor's params are erased at the guard boundary
): value is WorkflowActor<string, any, any, any> => kindOf(value) === "workflow";

/** The actor kind, or `undefined` for non-actors. */
export const actorKind = (value: unknown): ActorKind | undefined => kindOf(value);

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

// ── Operation value — the README's `Order.Place({...})` constructor ────────

declare const OpValueBrand: unique symbol;

/** A built-but-undispatched operation: `{ _tag, payload }`. Produced by calling
 *  an operation handle (`Order.Place({...})`) or `Op.make(payload)`, and
 *  consumed by the actor-level `execute`/`send`. The phantom brand carries the
 *  success/error types so the unified call site stays typed. */
export interface OperationValue<S = unknown, E = unknown> {
  readonly _tag: string;
  readonly payload: unknown;
  readonly [OpValueBrand]?: { readonly success: S; readonly error: E };
}

/** Options for `waitFor`. */
export interface WaitForOptions<S = unknown, E = unknown> {
  /** Resolve on the first `PeekResult` matching this predicate. Defaults to the
   *  first terminal result. (The drain is push-based, so unlike the cluster
   *  original no polling `schedule` is needed.) */
  readonly filter?: (result: PeekResult<S, E>) => boolean;
}

// ── Per-operation handle (callable; payload-only methods) ──────────────────

export type OperationHandle<P, S, E> = {
  /** Build an `OperationValue` without dispatching — the README's
   *  `Order.Place({...})` constructor. */
  (payload: P): OperationValue<S, E>;
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
  /** Block until the reply matches (default: first terminal), then return it. */
  readonly waitFor: (
    payload: P,
    options?: WaitForOptions<S, E>,
  ) => Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Live status: replays the current outcome (if any) then tails until a
   *  terminal `PeekResult` arrives, at which point the stream completes. */
  readonly watch: (
    payload: P,
  ) => Stream.Stream<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Escape hatch: build the `OperationValue` without dispatching (same as
   *  calling the handle). */
  readonly make: (payload: P) => OperationValue<S, E>;
};

const makeHandle = <P, S, E>(
  actorType: string,
  tag: string,
  def: OperationDef<P, S, E>,
): OperationHandle<P, S, E> => {
  const idOf = (payload: P): { execId: ExecId<S, E>; entityId: string } => {
    const resolved = resolveId(def.id(payload));
    return { execId: deriveExecId<S, E>(tag, resolved), entityId: resolved.entityId };
  };

  /** Public, undispatched value — the README's `Order.Place({...})`. */
  const make = (payload: P): OperationValue<S, E> => ({ _tag: tag, payload });

  /** Internal: the durable inbox row for a dispatch (PK = ExecId). */
  const toInbox = (payload: P): InboxMessage => {
    const { execId } = idOf(payload);
    return { msgId: execId, tag, payload: JSON.stringify(payload) };
  };

  const executionId = (payload: P): ExecId<S, E> => idOf(payload).execId;

  const send = (payload: P): Effect.Effect<ExecId<S, E>, DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    // Advertise the entity so a `toLayer` host can discover + drain it, then
    // durably enqueue. (Advertising is process-cached, so usually a no-op.)
    return Effect.andThen(
      advertiseEntity(actorType, entityId),
      withActor(actorType, entityId, (table) => Effect.as(enqueue(table, toInbox(payload)), execId)),
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
    options?: WaitForOptions<S, E>,
  ): Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    const filter = options?.filter as ((r: PeekResult) => boolean) | undefined;
    return withActor(
      actorType,
      entityId,
      (table) =>
        (filter === undefined
          ? waitForReply(table, execId)
          : waitForReplyMatching(table, execId, filter)) as Effect.Effect<
          PeekResult<S, E>,
          DurableTableError
        >,
    );
  };

  const execute = (payload: P): Effect.Effect<S, E | DurableTableError, EncoreConfig> => {
    const { execId, entityId } = idOf(payload);
    const run = withActor(actorType, entityId, (table) =>
      Effect.gen(function* () {
        yield* enqueue(table, toInbox(payload));
        const result = (yield* waitForReply(table, execId)) as PeekResult<S, E>;
        if (isSuccess(result)) return result.value;
        if (isFailure(result)) return yield* Effect.fail(result.error);
        if (result._tag === "Defect") return yield* Effect.die(result.cause);
        return yield* Effect.interrupt;
      }),
    );
    return Effect.andThen(advertiseEntity(actorType, entityId), run);
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

  // The handle is callable (`Op(payload)` builds an OperationValue) with the
  // payload-only methods attached.
  const callable = (payload: P): OperationValue<S, E> => make(payload);
  return Object.assign(callable, {
    executionId,
    send,
    execute,
    peek,
    waitFor,
    watch,
    make,
  }) as OperationHandle<P, S, E>;
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
  /** The actor's type tag (for an entity, identical to `name`). */
  readonly type: string;
} & {
  readonly [Tag in keyof Defs & string]: OperationHandle<
    PayloadOf<Defs[Tag]>,
    SuccessOf<Defs[Tag]>,
    ErrorOf<Defs[Tag]>
  >;
} & {
  // ── Unified call site — dispatch a built OperationValue ──────────────────
  /** Enqueue then await the terminal outcome of a built operation
   *  (`Order.execute(Order.Place({...}))`). */
  readonly execute: <S, E>(
    op: OperationValue<S, E>,
  ) => Effect.Effect<S, E | DurableTableError, EncoreConfig>;
  /** Fire-and-forget dispatch of a built operation; returns its ExecId. */
  readonly send: <S, E>(
    op: OperationValue<S, E>,
  ) => Effect.Effect<ExecId<S, E>, DurableTableError, EncoreConfig>;
  /** Pure ExecId for a built operation (entityId comes from its `id` fn). */
  readonly executionId: <S, E>(op: OperationValue<S, E>) => ExecId<S, E>;

  // ── Status tracking, keyed by opaque ExecId ─────────────────────────────
  /** One-shot status of an execution. */
  readonly peek: <S, E>(
    execId: ExecId<S, E>,
  ) => Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Live status stream that completes on the terminal outcome. */
  readonly watch: <S, E>(
    execId: ExecId<S, E>,
  ) => Stream.Stream<PeekResult<S, E>, DurableTableError, EncoreConfig>;
  /** Block until the reply matches (default: first terminal). */
  readonly waitFor: <S, E>(
    execId: ExecId<S, E>,
    options?: WaitForOptions<S, E>,
  ) => Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig>;

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

  /** Typed identity for handler construction — infers handler types from the
   *  defs when building handlers inside an `Effect.gen` that yields services.
   *  Mirrors effect-encore's `Actor.of`. */
  readonly of: <R>(handlers: EntityHandlers<Defs, R>) => EntityHandlers<Defs, R>;
  /** Type guard narrowing an `OperationValue` to a specific operation tag. */
  readonly $is: <Tag extends keyof Defs & string>(
    tag: Tag,
  ) => (
    value: unknown,
  ) => value is OperationValue<SuccessOf<Defs[Tag]>, ErrorOf<Defs[Tag]>>;
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

  // ── Unified call site: dispatch a built OperationValue by delegating to its
  // operation handle (which carries the `id` fn → entityId + ExecId). ────────
  const handleFor = (op: OperationValue): OperationHandle<unknown, unknown, unknown> => {
    const handle = handles[op._tag];
    if (handle === undefined) {
      return Effect.die(new Error(`encore-ds: unknown operation '${op._tag}' on ${name}`)) as never;
    }
    return handle;
  };
  const executeOp = <S, E>(
    op: OperationValue<S, E>,
  ): Effect.Effect<S, E | DurableTableError, EncoreConfig> =>
    handleFor(op).execute(op.payload) as Effect.Effect<S, E | DurableTableError, EncoreConfig>;
  const sendOp = <S, E>(
    op: OperationValue<S, E>,
  ): Effect.Effect<ExecId<S, E>, DurableTableError, EncoreConfig> =>
    handleFor(op).send(op.payload) as Effect.Effect<ExecId<S, E>, DurableTableError, EncoreConfig>;
  const executionIdOp = <S, E>(op: OperationValue<S, E>): ExecId<S, E> =>
    handleFor(op).executionId(op.payload) as ExecId<S, E>;

  // ── Status tracking by ExecId: parse the entityId out of the ExecId and read
  // that actor's replies stream directly (no payload needed). ────────────────
  const peekById = <S, E>(
    execId: ExecId<S, E>,
  ): Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { entityId } = parseExecId(execId);
    return withActor(
      name,
      entityId,
      (table) => peekReply(table, execId) as Effect.Effect<PeekResult<S, E>, DurableTableError>,
    );
  };
  const watchById = <S, E>(
    execId: ExecId<S, E>,
  ): Stream.Stream<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { entityId } = parseExecId(execId);
    return withActorStream(
      name,
      entityId,
      (table) => watchReply(table, execId) as Stream.Stream<PeekResult<S, E>, DurableTableError>,
    );
  };
  const waitForById = <S, E>(
    execId: ExecId<S, E>,
    options?: WaitForOptions<S, E>,
  ): Effect.Effect<PeekResult<S, E>, DurableTableError, EncoreConfig> => {
    const { entityId } = parseExecId(execId);
    const filter = options?.filter as ((r: PeekResult) => boolean) | undefined;
    return withActor(
      name,
      entityId,
      (table) =>
        (filter === undefined
          ? waitForReply(table, execId)
          : waitForReplyMatching(table, execId, filter)) as Effect.Effect<
          PeekResult<S, E>,
          DurableTableError
        >,
    );
  };

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
    [ActorKindId]: "entity",
    name,
    type: name,
    ...handles,
    execute: executeOp,
    send: sendOp,
    executionId: executionIdOp,
    peek: peekById,
    watch: watchById,
    waitFor: waitForById,
    activate: activateEntity,
    getState: getStateFn,
    watchState: watchStateFn,
    waitForState: waitForStateFn,
    listStateEntityIds: () => listStateEntityIds(name),
    of: <R>(h: EntityHandlers<Defs, R>) => h,
    $is:
      (tag: string) =>
      (value: unknown): value is OperationValue =>
        typeof value === "object" &&
        value !== null &&
        (value as { readonly _tag?: unknown })._tag === tag,
  } as unknown as EntityActor<Defs>;
};

// ── Hosting: toLayer / toTestLayer ─────────────────────────────────────────

/**
 * Host an entity TYPE: a Layer that, once provided, drains every entity id that
 * receives a dispatch — the encore-ds analogue of effect-encore's `toLayer`.
 *
 * It tails the per-type directory (populated by `send`/`execute`) and forks a
 * drain for each id, deduped per process; it also provides `ActorStateRegistry`
 * so clients in the same runtime can `getState`/`watchState`. When several hosts
 * run, the per-id owner claim elects exactly one drainer.
 *
 * v1 caveat: discovery is forward-only — a host that already saw an id won't
 * re-elect a drain if the current owner dies (epoch takeover is the deferred
 * S2-fencing evolution). The directory also grows unbounded (no TTL yet).
 */
export const toLayer = <Defs extends Record<string, AnyOperationDef>, R = never>(
  actor: EntityActor<Defs>,
  behavior: EntityHandlers<Defs, R> | Effect.Effect<EntityHandlers<Defs, R>, never, R>,
  options?: { readonly workerId?: string },
): Layer.Layer<
  ActorStateRegistry,
  never,
  EncoreConfig | Exclude<R, Scope.Scope | CurrentActorAddress | ActorStateRegistry>
> => {
  const hosting = new Set<string>();
  const manager = Effect.forkScoped(
    Stream.runForEach(directoryEntities(actor.type), (entityId) => {
      if (hosting.has(entityId)) return Effect.void;
      hosting.add(entityId);
      return Effect.asVoid(
        Effect.forkScoped(
          actor.activate(entityId, behavior as EntityHandlers<Defs, R>, options),
        ),
      );
    }),
  );
  return Layer.effectDiscard(manager).pipe(
    Layer.provideMerge(ActorStateRegistry.Live),
  ) as unknown as Layer.Layer<
    ActorStateRegistry,
    never,
    EncoreConfig | Exclude<R, Scope.Scope | CurrentActorAddress | ActorStateRegistry>
  >;
};

/** Test host. On the durable-streams backbone this is identical to `toLayer`
 *  (no separate sharding config to bundle); kept for README parity. */
export const toTestLayer = toLayer;
