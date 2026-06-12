import { Context, Data, Effect, Layer, Option, Ref, type Scope, Stream } from "effect";

// ─────────────────────────────────────────────────────────────────────────
// actor-state.ts — live entity-state registry.
//
// Ported from effect-encore's `src/actor-state.ts` to keep exact API + protocol
// fidelity. The ONE substitution: cluster's `EntityAddress`/`CurrentAddress`
// become encore-ds's own `(entityType, entityId)` pair + `CurrentActorAddress`
// context service (provided by `activate` for the lifetime of an owned drain).
//
// This is a LIVE-HEAP protocol, exactly like upstream: a `register`-ing entity
// publishes a `get`/`watch` handle into an in-process `Ref<Map>`, and observers
// in the SAME process read it. It is not durable storage — a cross-process
// reader cannot observe another runner's in-memory entity state. (The design
// doc's durable-row variant is a deliberately separate, more-capable path.)
// ─────────────────────────────────────────────────────────────────────────

export class ActorStateUnavailable extends Data.TaggedError(
  "encore-ds/actor-state/ActorStateUnavailable",
)<{
  readonly entityType: string;
  readonly entityId: string;
}> {}

/** The address of the entity whose drain is currently executing. Provided by
 *  `activate` around the owned drain so `registerState` can attribute the
 *  handle to the right `(entityType, entityId)` — the encore-ds analogue of
 *  cluster's `CurrentAddress`. */
export interface ActorAddress {
  readonly entityType: string;
  readonly entityId: string;
}

export class CurrentActorAddress extends Context.Service<CurrentActorAddress, ActorAddress>()(
  "encore-ds/actor-state/CurrentActorAddress",
) {}

export interface ActorStateHandle<State, Error = never, Requirements = never> {
  readonly get: Effect.Effect<State, Error, Requirements>;
  readonly watch: Stream.Stream<State, Error, Requirements>;
}

type AnyActorStateHandle = ActorStateHandle<unknown, unknown, unknown>;

export interface ActorStateRegistryShape {
  readonly register: (address: ActorAddress, handle: AnyActorStateHandle) => Effect.Effect<void>;
  readonly deregister: (address: ActorAddress, handle: AnyActorStateHandle) => Effect.Effect<void>;
  readonly get: (
    address: ActorAddress,
  ) => Effect.Effect<AnyActorStateHandle, ActorStateUnavailable>;
  readonly list: (entityType: string) => Effect.Effect<ReadonlyArray<string>>;
}

export class ActorStateRegistry extends Context.Service<
  ActorStateRegistry,
  ActorStateRegistryShape
>()("encore-ds/actor-state/ActorStateRegistry") {
  static Live: Layer.Layer<ActorStateRegistry> = Layer.effect(
    ActorStateRegistry,
    Effect.gen(function* () {
      const entries = yield* Ref.make<ReadonlyMap<string, AnyActorStateHandle>>(new Map());

      return {
        register: (address, handle) =>
          Ref.update(entries, (current) => {
            const next = new Map(current);
            next.set(addressKey(address), handle);
            return next;
          }),
        deregister: (address, handle) =>
          Ref.update(entries, (current) => {
            const key = addressKey(address);
            if (current.get(key) !== handle) return current;
            const next = new Map(current);
            next.delete(key);
            return next;
          }),
        get: (address) =>
          Ref.get(entries).pipe(
            Effect.flatMap((current) => {
              const handle = current.get(addressKey(address));
              return handle === undefined
                ? Effect.fail(
                    new ActorStateUnavailable({
                      entityType: address.entityType,
                      entityId: address.entityId,
                    }),
                  )
                : Effect.succeed(handle);
            }),
          ),
        list: (entityType) =>
          Ref.get(entries).pipe(
            Effect.map((current) =>
              Array.from(current.keys()).flatMap((key) => {
                const parsed = parseAddressKey(key);
                return parsed.entityType === entityType ? [parsed.entityId] : [];
              }),
            ),
          ),
      };
    }),
  );
}

/** Publish a live state handle for the entity whose drain is executing. The
 *  handle is deregistered when the activation scope closes. Mirrors
 *  effect-encore's `Actor.registerState`. */
export const registerState = <State, Error = never, Requirements = never>(
  handle: ActorStateHandle<State, Error, Requirements>,
): Effect.Effect<void, never, ActorStateRegistry | CurrentActorAddress | Scope.Scope> =>
  Effect.gen(function* () {
    const registry = yield* ActorStateRegistry;
    const address = yield* CurrentActorAddress;
    const erased = handle as AnyActorStateHandle;
    yield* registry.register(address, erased);
    yield* Effect.addFinalizer(() => registry.deregister(address, erased));
  });

export const stateOf = <State, Error = never, Requirements = never>(
  address: ActorAddress,
): Effect.Effect<State, Error | ActorStateUnavailable, ActorStateRegistry | Requirements> =>
  Effect.gen(function* () {
    const registry = yield* ActorStateRegistry;
    const handle = yield* registry.get(address);
    return yield* (handle.get as Effect.Effect<State, Error, Requirements>);
  });

export const watchStateOf = <State, Error = never, Requirements = never>(
  address: ActorAddress,
): Stream.Stream<State, Error | ActorStateUnavailable, ActorStateRegistry | Requirements> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const registry = yield* ActorStateRegistry;
      const handle = yield* registry.get(address);
      return handle.watch as Stream.Stream<State, Error, Requirements>;
    }),
  );

export const listStateEntityIds = (
  entityType: string,
): Effect.Effect<ReadonlyArray<string>, never, ActorStateRegistry> =>
  Effect.gen(function* () {
    const registry = yield* ActorStateRegistry;
    return yield* registry.list(entityType);
  });

export const waitForStateOf = <State, Error = never, Requirements = never>(
  address: ActorAddress,
  predicate: (state: State) => boolean,
): Effect.Effect<State, Error | ActorStateUnavailable, ActorStateRegistry | Requirements> =>
  watchStateOf<State, Error, Requirements>(address).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap((option) =>
      Option.match(option, {
        onNone: () =>
          Effect.die(
            new Error(
              `encore-ds/waitForStateOf: state stream ended before predicate matched for ${address.entityType}:${address.entityId}`,
            ),
          ),
        onSome: Effect.succeed,
      }),
    ),
  );

const addressKey = (address: ActorAddress): string =>
  `${address.entityType}\x00${address.entityId}`;

const parseAddressKey = (key: string): { readonly entityType: string; readonly entityId: string } => {
  const first = key.indexOf("\x00");
  return {
    entityType: first < 0 ? key : key.slice(0, first),
    entityId: first < 0 ? "" : key.slice(first + 1),
  };
};
