import { DurableStreamTestServer } from "@durable-streams/server";
import { Effect, Fiber, Option, type Scope, Stream } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Actor from "../src/actor.ts";
import type { OperationDef } from "../src/actor.ts";
import { EncoreConfig } from "../src/config.ts";

// ─────────────────────────────────────────────────────────────────────────
// The README's unified call site + ExecId-keyed status API:
//   const op = Order.Place({ item, qty });        // callable op -> OperationValue
//   const id = yield* Order.send(op);             // dispatch a built value
//   const status = yield* Order.peek(id);         // status by opaque ExecId
//   const final = yield* Order.waitFor(id, { filter });
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

const Order = Actor.fromEntity("Order", {
  Place: { id: (p: PlacePayload) => `${p.item}-${p.qty}` } as OperationDef<PlacePayload, string, never>,
});

const placeHandlers = {
  Place: (p: PlacePayload) => Effect.succeed(`order: ${p.item} x${p.qty}`),
};

describe("unified call site + ExecId status API", () => {
  it("calling an op builds an OperationValue; actor executionId matches the per-op one", async () => {
    const out = await run(
      Effect.gen(function* () {
        const op = Order.Place({ item: "widget", qty: 3 });
        const viaActor = Order.executionId(op);
        const viaOp = Order.Place.executionId({ item: "widget", qty: 3 });
        return { op, viaActor, viaOp };
      }),
    );
    expect(out.op._tag).toBe("Place");
    expect(out.op.payload).toEqual({ item: "widget", qty: 3 });
    expect(typeof out.viaActor).toBe("string");
    expect(out.viaActor).toBe(out.viaOp); // unified + per-op derive the same ExecId
  });

  it("peek by ExecId is Pending before a drain, Success after the reply lands", async () => {
    const out = await run(
      Effect.gen(function* () {
        const op = Order.Place({ item: "gizmo", qty: 1 });
        // producer-only: enqueue, then peek — no handler yet → Pending
        const execId = yield* Order.send(op);
        const before = yield* Order.peek(execId);
        // now host the entity and wait for the reply via the ExecId
        const drain = yield* Effect.forkScoped(Order.activate("gizmo-1", placeHandlers));
        const final = yield* Order.waitFor(execId);
        const after = yield* Order.peek(execId);
        yield* Fiber.interrupt(drain);
        return { before, final, after };
      }),
    );
    expect(out.before._tag).toBe("Pending");
    expect(out.final).toEqual({ _tag: "Success", value: "order: gizmo x1" });
    expect(out.after).toEqual({ _tag: "Success", value: "order: gizmo x1" });
  });

  it("execute dispatches a built OperationValue and returns its success", async () => {
    const value = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(Order.activate("widget-3", placeHandlers));
        const result = yield* Order.execute(Order.Place({ item: "widget", qty: 3 }));
        yield* Fiber.interrupt(drain);
        return result;
      }),
    );
    expect(value).toBe("order: widget x3");
  });

  it("waitFor with a filter resolves on the first matching terminal; watch streams it", async () => {
    const out = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(Order.activate("thing-2", placeHandlers));
        const execId = yield* Order.send(Order.Place({ item: "thing", qty: 2 }));
        const matched = yield* Order.waitFor(execId, { filter: (r) => r._tag === "Success" });
        const streamed = yield* Stream.runHead(Order.watch(execId));
        yield* Fiber.interrupt(drain);
        return { matched, streamed: Option.getOrNull(streamed) };
      }),
    );
    expect(out.matched).toEqual({ _tag: "Success", value: "order: thing x2" });
    expect(out.streamed?._tag).toBe("Success");
  });
});
