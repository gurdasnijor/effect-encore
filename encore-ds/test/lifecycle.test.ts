import { DurableStreamTestServer } from "@durable-streams/server";
import { Effect, Fiber, Ref, type Scope } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Actor from "../src/actor.ts";
import type { OperationDef } from "../src/actor.ts";
import { EncoreConfig } from "../src/config.ts";

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
const Order = Actor.fromEntity("OrderLifecycle", {
  Place: { id: (p: PlacePayload) => `${p.item}-${p.qty}` } as OperationDef<PlacePayload, string, never>,
});
const handlers = { Place: (p: PlacePayload) => Effect.succeed(`order: ${p.item} x${p.qty}`) };

describe("lifecycle", () => {
  it("sendAndAwait fails SendAndAwaitTimeout when nothing drains", async () => {
    const tag = await run(
      Order.Place.sendAndAwait({ item: "nohost", qty: 1 }, { timeout: "200 millis" }).pipe(
        Effect.map(() => "ok" as const),
        Effect.catch((e) => Effect.succeed(e._tag)),
      ),
    );
    expect(tag).toBe("encore-ds/SendAndAwaitTimeout");
  });

  it("sendAndAwait returns the persisted reply once a host drains it", async () => {
    const value = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(Order.activate("y-1", handlers));
        const v = yield* Order.Place.sendAndAwait({ item: "y", qty: 1 }, { timeout: "5 seconds" });
        yield* Fiber.interrupt(drain);
        return v;
      }),
    );
    expect(value).toBe("order: y x1");
  });

  it("flush clears the mailbox so a later activation processes nothing", async () => {
    const count = await run(
      Effect.gen(function* () {
        const ran = yield* Ref.make(0);
        const counting = {
          Place: (p: PlacePayload) => Effect.as(Ref.update(ran, (n) => n + 1), `${p.item}`),
        };
        // enqueue with no drainer, then flush the mailbox
        yield* Order.Place.send({ item: "z", qty: 1 });
        yield* Order.flush("z-1");
        // a fresh activation now finds an empty mailbox
        const drain = yield* Effect.forkScoped(Order.activate("z-1", counting));
        yield* Effect.sleep("300 millis");
        const c = yield* Ref.get(ran);
        yield* Fiber.interrupt(drain);
        return c;
      }),
    );
    expect(count).toBe(0);
  });

  it("interrupt clears the mailbox too", async () => {
    const count = await run(
      Effect.gen(function* () {
        const ran = yield* Ref.make(0);
        const counting = {
          Place: (p: PlacePayload) => Effect.as(Ref.update(ran, (n) => n + 1), `${p.item}`),
        };
        yield* Order.Place.send({ item: "w", qty: 1 });
        yield* Order.interrupt("w-1");
        const drain = yield* Effect.forkScoped(Order.activate("w-1", counting));
        yield* Effect.sleep("300 millis");
        const c = yield* Ref.get(ran);
        yield* Fiber.interrupt(drain);
        return c;
      }),
    );
    expect(count).toBe(0);
  });
});
