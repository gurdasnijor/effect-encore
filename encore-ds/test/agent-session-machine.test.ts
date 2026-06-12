import { DurableStreamTestServer } from "@durable-streams/server";
import { Effect, Fiber, Layer, type Scope } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Actor from "../src/actor.ts";
import type { OperationDef } from "../src/actor.ts";
import { ActorStateRegistry } from "../src/actor-state.ts";
import { EncoreConfig } from "../src/config.ts";
import * as Machine from "../src/machine.ts";

// ─────────────────────────────────────────────────────────────────────────
// The SAME agent session as agent-session.test.ts, but expressed as a
// declarative `Machine` instead of a hand-rolled SubscriptionRef reducer. The
// win: legality lives in the TABLE SHAPE — `ApproveTool` only exists under
// `awaiting_approval`, `ToolResult` only under `running_tool` — so the
// permission gate and out-of-phase rejection are structural, not scattered
// `if (phase !== ...)` checks. `to`/`enabled` get the state already narrowed to
// the current tag (typed `s.pending` with no manual narrowing).
// ─────────────────────────────────────────────────────────────────────────

interface Ctx {
  readonly transcript: ReadonlyArray<string>;
  readonly toolRuns: number;
  readonly rejected: number;
}
interface PendingTool {
  readonly callId: string;
  readonly tool: string;
  readonly input: string;
}
type SessionState =
  | ({ readonly _tag: "idle" } & Ctx)
  | ({ readonly _tag: "awaiting_approval"; readonly pending: PendingTool } & Ctx)
  | ({ readonly _tag: "running_tool"; readonly pending: PendingTool } & Ctx);

type SessionEvent =
  | { readonly _tag: "UserPrompt"; readonly sessionId: string; readonly eventId: string; readonly text: string }
  | { readonly _tag: "ApproveTool"; readonly sessionId: string; readonly eventId: string; readonly callId: string }
  | { readonly _tag: "DenyTool"; readonly sessionId: string; readonly eventId: string; readonly callId: string }
  | {
      readonly _tag: "ToolResult";
      readonly sessionId: string;
      readonly eventId: string;
      readonly callId: string;
      readonly output: string;
    };

const initial: SessionState = { _tag: "idle", transcript: [], toolRuns: 0, rejected: 0 };
const base = (s: SessionState): Ctx => ({
  transcript: s.transcript,
  toolRuns: s.toolRuns,
  rejected: s.rejected,
});
const logged = (s: SessionState, msg: string): Ctx => ({
  ...base(s),
  transcript: [...s.transcript, msg],
});
const needsTool = (text: string) => /search|weather|lookup|stock|price/i.test(text);

const machine = Machine.make<SessionState, SessionEvent>({
  initial,
  on: {
    idle: {
      UserPrompt: {
        to: (s, e) => {
          const ctx = logged(s, `user: ${e.text}`);
          if (!needsTool(e.text)) {
            return { _tag: "idle", ...logged({ ...s, ...ctx }, `assistant: answered "${e.text}"`) };
          }
          const callId = `call-${e.eventId}`;
          return {
            _tag: "awaiting_approval",
            pending: { callId, tool: "search", input: e.text },
            ...logged({ ...s, ...ctx }, `assistant: requests tool 'search' (awaiting approval)`),
          };
        },
      },
    },
    awaiting_approval: {
      // Guard ties the decision to the pending call. `s.pending` is typed here.
      ApproveTool: {
        enabled: (s, e) => s.pending.callId === e.callId,
        to: (s, e) => ({
          _tag: "running_tool",
          pending: s.pending,
          ...logged(s, `approved ${e.callId}`),
        }),
      },
      DenyTool: {
        enabled: (s, e) => s.pending.callId === e.callId,
        to: (s, e) => ({ _tag: "idle", ...logged(s, `denied ${e.callId} — tool will not run`) }),
      },
    },
    running_tool: {
      ToolResult: {
        enabled: (s, e) => s.pending.callId === e.callId,
        to: (s, e) => ({
          _tag: "idle",
          ...logged(s, `tool ${e.callId} -> ${e.output}`),
          toolRuns: s.toolRuns + 1,
        }),
      },
    },
  },
  // Any event with no enabled transition for the current state is refused.
  onUnhandled: (s, e) => ({ ...s, ...logged(s, `REJECTED ${e._tag} in ${s._tag}`), rejected: s.rejected + 1 }),
});

// ── pure transition tests (no durable substrate needed) ─────────────────────

describe("agent session machine — pure transitions", () => {
  const prompt = (text: string): SessionEvent => ({ _tag: "UserPrompt", sessionId: "s", eventId: "e1", text });

  it("a tool prompt enters the approval gate", () => {
    const s = machine.step(initial, prompt("search the weather"));
    expect(s._tag).toBe("awaiting_approval");
    expect(s._tag === "awaiting_approval" && s.pending.callId).toBe("call-e1");
  });

  it("approve → running, result → idle, tool runs exactly once", () => {
    const awaiting = machine.step(initial, prompt("lookup price"));
    const running = machine.step(awaiting, { _tag: "ApproveTool", sessionId: "s", eventId: "e2", callId: "call-e1" });
    expect(running._tag).toBe("running_tool");
    const done = machine.step(running, {
      _tag: "ToolResult",
      sessionId: "s",
      eventId: "e3",
      callId: "call-e1",
      output: "sunny",
    });
    expect(done._tag).toBe("idle");
    expect(done.toolRuns).toBe(1);
    expect(done.rejected).toBe(0);
  });

  it("a denied tool never runs", () => {
    const awaiting = machine.step(initial, prompt("search stocks"));
    const denied = machine.step(awaiting, { _tag: "DenyTool", sessionId: "s", eventId: "e2", callId: "call-e1" });
    expect(denied._tag).toBe("idle");
    expect(denied.toolRuns).toBe(0);
  });

  it("the table shape rejects out-of-phase events (gate + guard)", () => {
    // ToolResult with no approval — no entry under `idle` → onUnhandled.
    const stray = machine.step(initial, {
      _tag: "ToolResult",
      sessionId: "s",
      eventId: "e1",
      callId: "call-x",
      output: "nope",
    });
    expect(stray._tag).toBe("idle");
    expect(stray.toolRuns).toBe(0);
    expect(stray.rejected).toBe(1);

    // Approval for the wrong call — entry exists but the guard is false.
    const awaiting = machine.step(initial, prompt("search"));
    const wrong = machine.step(awaiting, { _tag: "ApproveTool", sessionId: "s", eventId: "e2", callId: "WRONG" });
    expect(wrong._tag).toBe("awaiting_approval"); // still gated
    expect(wrong.rejected).toBe(1);
  });

  it("a non-tool prompt is answered directly, staying idle", () => {
    const s = machine.step(initial, prompt("hello there"));
    expect(s._tag).toBe("idle");
  });
});

// ── hosted on the durable drain (machine as entity Behavior) ────────────────

let server: DurableStreamTestServer;
let streamRoot: string;
beforeAll(async () => {
  server = new DurableStreamTestServer({ port: 0, host: "127.0.0.1" });
  streamRoot = `${await server.start()}/v1/stream`;
});
afterAll(async () => {
  if (server !== undefined) await server.stop();
});

const run = <A, E>(eff: Effect.Effect<A, E, EncoreConfig | ActorStateRegistry | Scope.Scope>) =>
  Effect.runPromise(
    Effect.scoped(
      eff.pipe(
        Effect.provide(Layer.mergeAll(ActorStateRegistry.Live)),
        Effect.provideService(EncoreConfig, { baseUrl: streamRoot }),
      ),
    ),
  );

const sessionRoute = (p: { readonly sessionId: string; readonly eventId: string }) => ({
  entityId: p.sessionId,
  primaryKey: `${p.sessionId}:${p.eventId}`,
});

// The dispatch surface: op tags MUST match the machine's event tags. The
// machine's default `toEvent` rebuilds `{ _tag: <op tag>, ...payload }`.
const Session = Actor.fromEntity("AgentSessionM", {
  UserPrompt: { id: sessionRoute } as OperationDef<Extract<SessionEvent, { _tag: "UserPrompt" }>, SessionState, never>,
  ApproveTool: { id: sessionRoute } as OperationDef<Extract<SessionEvent, { _tag: "ApproveTool" }>, SessionState, never>,
  DenyTool: { id: sessionRoute } as OperationDef<Extract<SessionEvent, { _tag: "DenyTool" }>, SessionState, never>,
  ToolResult: { id: sessionRoute } as OperationDef<Extract<SessionEvent, { _tag: "ToolResult" }>, SessionState, never>,
});

describe("agent session machine — hosted on the durable drain", () => {
  it("drives the approved-tool path and exposes the snapshot via getState", async () => {
    const sessionId = `sess-${crypto.randomUUID()}`;
    const out = await run(
      Effect.gen(function* () {
        // `machine.behavior()` registers the snapshot as live state and returns
        // one handler per event tag — no per-op wiring.
        const drain = yield* Effect.forkScoped(Session.activate(sessionId, machine.behavior()));
        yield* Session.UserPrompt.execute({ _tag: "UserPrompt", sessionId, eventId: "e1", text: "search the weather" });
        const gated = yield* Session.getState<SessionState>(sessionId);
        yield* Session.ApproveTool.execute({ _tag: "ApproveTool", sessionId, eventId: "e2", callId: "call-e1" });
        const final = yield* Session.ToolResult.execute({
          _tag: "ToolResult",
          sessionId,
          eventId: "e3",
          callId: "call-e1",
          output: "sunny, 72F",
        });
        yield* Fiber.interrupt(drain);
        return { gatedTag: gated._tag, final };
      }),
    );
    expect(out.gatedTag).toBe("awaiting_approval");
    expect(out.final._tag).toBe("idle");
    expect(out.final.toolRuns).toBe(1);
    expect(out.final.rejected).toBe(0);
  });
});
