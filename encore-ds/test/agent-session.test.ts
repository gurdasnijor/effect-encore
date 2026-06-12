import { DurableStreamTestServer } from "@durable-streams/server";
import { Effect, Fiber, Layer, type Scope, Stream, SubscriptionRef } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Actor from "../src/actor.ts";
import type { OperationDef } from "../src/actor.ts";
import { ActorStateRegistry } from "../src/actor-state.ts";
import { EncoreConfig } from "../src/config.ts";

// ─────────────────────────────────────────────────────────────────────────
// A richer conformance slice: an entity actor that ingests a STREAM OF EVENTS
// and drives a state machine modelling an AI agent session. The machine gates
// tool execution behind an explicit approval ("permission check") step:
//
//   idle ──UserPrompt(needs tool)──▶ awaiting_approval
//   awaiting_approval ──ApproveTool──▶ running_tool ──ToolResult──▶ idle (+toolRun)
//   awaiting_approval ──DenyTool────▶ idle (tool NEVER runs)
//   <any illegal event for the current phase> ─▶ rejected (no tool run)
//
// It exercises, together: per-event execute back-pressure (ordered ingestion),
// the divergent `{ entityId, primaryKey }` id form (one mailbox per session,
// distinct ExecId per event), and the live entity-state registry
// (`Actor.registerState` / `getState` / `watchState` / `waitForState`).
// ─────────────────────────────────────────────────────────────────────────

let server: DurableStreamTestServer;
let streamRoot: string;

beforeAll(async () => {
  server = new DurableStreamTestServer({ port: 0, host: "127.0.0.1" });
  const base = await server.start();
  streamRoot = `${base}/v1/stream`;
});
afterAll(async () => {
  if (server !== undefined) await server.stop();
});

// Provide both the durable-streams config and the live-state registry, exactly
// as effect-encore's `Actor.toTestLayer` wires `ActorStateRegistry.Live`.
const layer = Layer.mergeAll(ActorStateRegistry.Live);
const run = <A, E>(eff: Effect.Effect<A, E, EncoreConfig | ActorStateRegistry | Scope.Scope>) =>
  Effect.runPromise(
    Effect.scoped(
      eff.pipe(Effect.provide(layer), Effect.provideService(EncoreConfig, { baseUrl: streamRoot })),
    ),
  );

const freshSession = () => `sess-${crypto.randomUUID()}`;

// ── the session state machine ──────────────────────────────────────────────

type Phase = "idle" | "awaiting_approval" | "running_tool";
interface PendingTool {
  readonly callId: string;
  readonly tool: string;
  readonly input: string;
}
interface SessionState {
  readonly phase: Phase;
  readonly pending: PendingTool | null;
  readonly transcript: ReadonlyArray<string>;
  readonly toolRuns: number; // tools that actually executed (passed the gate)
  readonly rejected: number; // events refused by the current-phase guard
}
const initial: SessionState = {
  phase: "idle",
  pending: null,
  transcript: [],
  toolRuns: 0,
  rejected: 0,
};

const log = (s: SessionState, msg: string): SessionState => ({
  ...s,
  transcript: [...s.transcript, msg],
});
const reject = (s: SessionState, msg: string): SessionState => ({
  ...log(s, `REJECTED: ${msg}`),
  rejected: s.rejected + 1,
});

// The "agent" decides a tool is needed when the prompt looks like a lookup.
const needsTool = (text: string) => /search|weather|lookup|stock|price/i.test(text);

// ── operation (event) payloads ─────────────────────────────────────────────

interface PromptEvent {
  readonly sessionId: string;
  readonly eventId: string;
  readonly text: string;
}
interface ToolDecisionEvent {
  readonly sessionId: string;
  readonly eventId: string;
  readonly callId: string;
}
interface ToolResultEvent {
  readonly sessionId: string;
  readonly eventId: string;
  readonly callId: string;
  readonly output: string;
}

// entityId = sessionId (one FIFO mailbox per session); primaryKey carries the
// eventId so every event gets a distinct ExecId (the divergent id form).
const sessionRoute = (p: { readonly sessionId: string; readonly eventId: string }) => ({
  entityId: p.sessionId,
  primaryKey: `${p.sessionId}:${p.eventId}`,
});

const AgentSession = Actor.fromEntity("AgentSession", {
  UserPrompt: { id: sessionRoute } as OperationDef<PromptEvent, SessionState, never>,
  ApproveTool: { id: sessionRoute } as OperationDef<ToolDecisionEvent, SessionState, never>,
  DenyTool: { id: sessionRoute } as OperationDef<ToolDecisionEvent, SessionState, never>,
  ToolResult: { id: sessionRoute } as OperationDef<ToolResultEvent, SessionState, never>,
});

// The behavior: create the durable-for-this-activation session state, publish a
// live handle, and return the per-event transition handlers. Each handler
// transitions the machine atomically and returns the resulting snapshot so the
// caller's `execute` observes the post-event state.
const behavior = Effect.gen(function* () {
  const state = yield* SubscriptionRef.make(initial);
  yield* Actor.registerState({
    get: SubscriptionRef.get(state),
    watch: SubscriptionRef.changes(state),
  });

  const transition = (f: (s: SessionState) => SessionState) =>
    SubscriptionRef.modify(state, (s) => {
      const next = f(s);
      return [next, next] as const;
    });

  return {
    UserPrompt: (p: PromptEvent) =>
      transition((s) => {
        if (s.phase !== "idle") return reject(s, `UserPrompt while ${s.phase}`);
        const withUser = log(s, `user: ${p.text}`);
        if (!needsTool(p.text)) return log(withUser, `assistant: answered "${p.text}" directly`);
        const callId = `call-${p.eventId}`;
        return {
          ...log(withUser, `assistant: requests tool 'search' (awaiting approval)`),
          phase: "awaiting_approval",
          pending: { callId, tool: "search", input: p.text },
        };
      }),

    ApproveTool: (p: ToolDecisionEvent) =>
      transition((s) =>
        s.phase === "awaiting_approval" && s.pending?.callId === p.callId
          ? { ...log(s, `approved ${p.callId}`), phase: "running_tool" }
          : reject(s, `ApproveTool ${p.callId} while ${s.phase}`),
      ),

    DenyTool: (p: ToolDecisionEvent) =>
      transition((s) =>
        s.phase === "awaiting_approval" && s.pending?.callId === p.callId
          ? { ...log(s, `denied ${p.callId} — tool will not run`), phase: "idle", pending: null }
          : reject(s, `DenyTool ${p.callId} while ${s.phase}`),
      ),

    ToolResult: (p: ToolResultEvent) =>
      transition((s) =>
        // The permission gate: a result is only accepted for a tool that was
        // approved (running_tool). An unapproved/denied tool can never land a
        // result, so `toolRuns` only counts gate-passing executions.
        s.phase === "running_tool" && s.pending?.callId === p.callId
          ? {
              ...log(s, `tool ${p.callId} -> ${p.output}`),
              phase: "idle",
              pending: null,
              toolRuns: s.toolRuns + 1,
            }
          : reject(s, `ToolResult ${p.callId} while ${s.phase}`),
      ),
  };
});

// ── conformance ────────────────────────────────────────────────────────────

describe("agent-session state machine", () => {
  it("ingests an approved-tool event stream and runs the tool exactly once", async () => {
    const sessionId = freshSession();
    const events = [
      AgentSession.UserPrompt.execute({ sessionId, eventId: "e1", text: "search the weather" }),
      AgentSession.ApproveTool.execute({ sessionId, eventId: "e2", callId: "call-e1" }),
      AgentSession.ToolResult.execute({
        sessionId,
        eventId: "e3",
        callId: "call-e1",
        output: "sunny, 72F",
      }),
    ];

    const finalState = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(AgentSession.activate(sessionId, behavior));
        // Ingest the events as a stream; execute back-pressures each event so
        // the machine sees them in order.
        let last: SessionState | undefined;
        yield* Stream.runForEach(Stream.fromIterable(events), (ev) =>
          Effect.map(ev, (s) => {
            last = s;
          }),
        );
        const state = yield* AgentSession.getState<SessionState>(sessionId);
        yield* Fiber.interrupt(drain);
        return { state, last };
      }),
    );

    expect(finalState.state.phase).toBe("idle");
    expect(finalState.state.toolRuns).toBe(1);
    expect(finalState.state.rejected).toBe(0);
    expect(finalState.state.pending).toBeNull();
    // mid-stream the machine passed through the approval gate
    expect(finalState.state.transcript).toContain("approved call-e1");
    expect(finalState.state.transcript).toContain("tool call-e1 -> sunny, 72F");
  });

  it("reaches awaiting_approval and is observable via waitForState before approval", async () => {
    const sessionId = freshSession();
    const phase = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(AgentSession.activate(sessionId, behavior));
        // execute drives the prompt through the handler (which also registers
        // the live state handle), then waitForState reads it via the registry.
        yield* AgentSession.UserPrompt.execute({ sessionId, eventId: "e1", text: "lookup AAPL price" });
        // observe the live state sitting at the approval gate
        const s = yield* AgentSession.waitForState<SessionState>(
          sessionId,
          (st) => st.phase === "awaiting_approval",
        );
        yield* Fiber.interrupt(drain);
        return { phase: s.phase, pending: s.pending };
      }),
    );
    expect(phase.phase).toBe("awaiting_approval");
    expect(phase.pending?.callId).toBe("call-e1");
  });

  it("a denied tool never runs (permission gate blocks execution)", async () => {
    const sessionId = freshSession();
    const result = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(AgentSession.activate(sessionId, behavior));
        yield* AgentSession.UserPrompt.execute({ sessionId, eventId: "e1", text: "search stocks" });
        const afterDeny = yield* AgentSession.DenyTool.execute({
          sessionId,
          eventId: "e2",
          callId: "call-e1",
        });
        // A result for the denied call must be refused — the gate was closed.
        const afterResult = yield* AgentSession.ToolResult.execute({
          sessionId,
          eventId: "e3",
          callId: "call-e1",
          output: "should-never-apply",
        });
        yield* Fiber.interrupt(drain);
        return { afterDeny, afterResult };
      }),
    );
    expect(result.afterDeny.phase).toBe("idle");
    expect(result.afterDeny.toolRuns).toBe(0);
    expect(result.afterResult.toolRuns).toBe(0); // tool never ran
    expect(result.afterResult.rejected).toBe(1); // the stray ToolResult was refused
  });

  it("guards illegal transitions: a ToolResult with no approval is rejected", async () => {
    const sessionId = freshSession();
    const state = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(AgentSession.activate(sessionId, behavior));
        const out = yield* AgentSession.ToolResult.execute({
          sessionId,
          eventId: "e1",
          callId: "call-x",
          output: "out-of-nowhere",
        });
        yield* Fiber.interrupt(drain);
        return out;
      }),
    );
    expect(state.phase).toBe("idle");
    expect(state.toolRuns).toBe(0);
    expect(state.rejected).toBe(1);
  });

  it("lists the session as a live state entity while its drain is active", async () => {
    const sessionId = freshSession();
    const ids = await run(
      Effect.gen(function* () {
        const drain = yield* Effect.forkScoped(AgentSession.activate(sessionId, behavior));
        // wait until the behavior has registered its state handle
        yield* AgentSession.UserPrompt.execute({ sessionId, eventId: "e1", text: "hello there" });
        const listed = yield* AgentSession.listStateEntityIds();
        yield* Fiber.interrupt(drain);
        return listed;
      }),
    );
    expect(ids).toContain(sessionId);
  });
});
