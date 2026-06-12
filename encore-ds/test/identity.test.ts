import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import * as Actor from "../src/actor.ts";
import type { OperationDef } from "../src/actor.ts";

interface PlacePayload {
  readonly item: string;
  readonly qty: number;
}

const Order = Actor.fromEntity("Order", {
  Place: { id: (p: PlacePayload) => `${p.item}-${p.qty}` } as OperationDef<PlacePayload, string, never>,
  Cancel: { id: (p: { reason: string }) => p.reason } as OperationDef<{ reason: string }, void, never>,
});

const ProcessOrder = Actor.fromWorkflow("ProcessOrder", {
  payload: { orderId: Schema.String },
  id: (p) => p.orderId,
});

describe("identity & type guards", () => {
  it("exposes name and type tags", () => {
    expect(Order.name).toBe("Order");
    expect(Order.type).toBe("Order");
    expect(ProcessOrder.name).toBe("ProcessOrder");
    expect(ProcessOrder.type).toBe("Workflow/ProcessOrder");
  });

  it("isEntity / isWorkflow discriminate the two actor kinds", () => {
    expect(Actor.isEntity(Order)).toBe(true);
    expect(Actor.isWorkflow(Order)).toBe(false);
    expect(Actor.isWorkflow(ProcessOrder)).toBe(true);
    expect(Actor.isEntity(ProcessOrder)).toBe(false);
    expect(Actor.isEntity({})).toBe(false);
    expect(Actor.isEntity(null)).toBe(false);
    expect(Actor.actorKind(Order)).toBe("entity");
    expect(Actor.actorKind(ProcessOrder)).toBe("workflow");
  });

  it("$is narrows an OperationValue to a tag", () => {
    const place = Order.Place({ item: "widget", qty: 3 });
    const cancel = Order.Cancel({ reason: "oops" });
    expect(Order.$is("Place")(place)).toBe(true);
    expect(Order.$is("Place")(cancel)).toBe(false);
    expect(Order.$is("Cancel")(cancel)).toBe(true);
    expect(Order.$is("Place")({ nope: true })).toBe(false);
  });

  it("of is an identity that types handlers from the defs", () => {
    const handlers = Order.of({
      Place: (p) => Promise.resolve(`${p.item} x${p.qty}`) as never,
    });
    expect(typeof handlers.Place).toBe("function");
  });
});
