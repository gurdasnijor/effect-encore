import { DurableStreamTestServer } from "@durable-streams/server";
import { Effect, type Scope, SubscriptionRef } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Actor from "../src/actor.ts";
import type { OperationDef } from "../src/actor.ts";
import { EncoreConfig } from "../src/config.ts";

// ─────────────────────────────────────────────────────────────────────────
// `Actor.toLayer(actor, behavior)` hosts the entity TYPE: provide the layer and
// every id that receives a dispatch is drained — no explicit `activate(id)`.
// The host discovers ids via the per-type directory that send/execute populate,
// and provides `ActorStateRegistry` so clients can read live state.
// ─────────────────────────────────────────────────────────────────────────

let server: DurableStreamTestServer;
let streamRoot: string;
beforeAll(async () => {
  server = new DurableStreamTestServer({ port: 0, host: "127.0.0.1" });
  streamRoot = `${await server.start()}/v1/stream`;
});
afterAll(async () => {
  if (server !== undefined) await server.stop();
});

const run = <A, E>(eff: Effect.Effect<A, E, EncoreConfig | Scope.Scope>) =>
  Effect.runPromise(
    Effect.scoped(Effect.provideService(eff, EncoreConfig, { baseUrl: streamRoot })),
  );

interface PlacePayload {
  readonly item: string;
  readonly qty: number;
}

const Order = Actor.fromEntity("OrderHosted", {
  Place: { id: (p: PlacePayload) => `${p.item}-${p.qty}` } as OperationDef<PlacePayload, string, never>,
});
const OrderLive = Actor.toLayer(Order, {
  Place: (p) => Effect.succeed(`order: ${p.item} x${p.qty}`),
});

interface IncPayload {
  readonly id: string;
  readonly seq: number;
  readonly by: number;
}
const Counter = Actor.fromEntity("CounterHosted", {
  // entityId = counter id (one mailbox + one state per counter); primaryKey
  // carries the seq so each increment is a distinct ExecId (not deduped).
  Inc: {
    id: (p: IncPayload) => ({ entityId: p.id, primaryKey: `${p.id}:${p.seq}` }),
  } as OperationDef<IncPayload, number, never>,
});
// Behavior form: create + register per-entity state, then return handlers.
const counterBehavior = Effect.gen(function* () {
  const state = yield* SubscriptionRef.make(0);
  yield* Actor.registerState({
    get: SubscriptionRef.get(state),
    watch: SubscriptionRef.changes(state),
  });
  return { Inc: (p: IncPayload) => SubscriptionRef.updateAndGet(state, (n) => n + p.by) };
});
const CounterLive = Actor.toLayer(Counter, counterBehavior);

describe("toLayer hosting", () => {
  it("drains any entity id of the type without an explicit activate", async () => {
    const out = await run(
      Effect.gen(function* () {
        // Two distinct entity ids ("widget-1", "gadget-2"); neither is activated
        // explicitly — the hosted layer discovers + drains both.
        const r1 = yield* Order.execute(Order.Place({ item: "widget", qty: 1 }));
        const r2 = yield* Order.execute(Order.Place({ item: "gadget", qty: 2 }));
        return { r1, r2 };
      }).pipe(Effect.provide(OrderLive)),
    );
    expect(out.r1).toBe("order: widget x1");
    expect(out.r2).toBe("order: gadget x2");
  });

  it("provides the state registry; getState reads live state after a hosted dispatch", async () => {
    const out = await run(
      Effect.gen(function* () {
        const a = yield* Counter.Inc.execute({ id: "c1", seq: 1, by: 5 });
        const b = yield* Counter.Inc.execute({ id: "c1", seq: 2, by: 3 });
        const state = yield* Counter.getState<number>("c1");
        return { a, b, state };
      }).pipe(Effect.provide(CounterLive)),
    );
    expect(out.a).toBe(5);
    expect(out.b).toBe(8);
    expect(out.state).toBe(8);
  });
});
