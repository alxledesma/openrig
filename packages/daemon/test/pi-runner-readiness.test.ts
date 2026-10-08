import { describe, it, expect } from "vitest";
import { RunnerCore, type RunnerIo } from "../src/adapters/pi-runner.js";
import { buildPendingRunnerState, type PiRunnerState } from "../src/adapters/pi-runner-protocol.js";

const model = { provider: "provider", id: "model", contextWindow: 1000, maxTokens: 100 };
const sessionFile = "/native/session.jsonl";
function harness(runtime: "pi" | "omp" = "pi") {
  let time = Date.parse("2026-10-07T12:00:00Z");
  const sidecars: PiRunnerState[] = [], rpc: Record<string, unknown>[] = [];
  const io: RunnerIo = {
    sendRpc: c => { rpc.push(c); }, mirrorLine: () => {}, mirrorAppend: () => {}, postActivity: () => {},
    writeSidecar: s => { sidecars.push(s); }, now: () => new Date(time).toISOString(),
  };
  const core = new RunnerCore(io, { sessionName: "worker@rig", launchId: "launch", generation: "generation" }, { runtime });
  const send = (e: Record<string, unknown>) => core.handlePiLine(JSON.stringify(e));
  const state = (id = "pi-runner-get-state", m: unknown = model) => send({ type: "response", id, success: true,
    data: { sessionFile, sessionId: "session", isStreaming: false, isCompacting: false, pendingMessageCount: 0, model: m } });
  core.start(); state();
  return { core, rpc, send, state, advance: () => { time += 5000; }, latest: () => sidecars.at(-1)!,
    evidence: () => sidecars.at(-1)!.runtimeReadiness!, now: io.now };
}
function assistant(h: ReturnType<typeof harness>, overrides: Record<string, unknown> = {}) {
  h.send({ type: "message_end", message: { role: "assistant", provider: model.provider, model: model.id,
    stopReason: "stop", timestamp: 123, usage: { input: 800, output: 50, cacheRead: 20, cacheWrite: 10, totalTokens: 880 },
    ...overrides } });
}
function controlResponse(h: ReturnType<typeof harness>, command: string, success: boolean, data?: unknown) {
  h.send({ type: "response", id: "pi-runner-native-control", command, success, data,
    ...(success ? {} : { error: "sensitive provider text must not enter readiness" }) });
  h.state("pi-runner-control-state");
}

describe("native Pi readiness producer", () => {
  it("binds native observations to one launch and generation; pending reset carries no readiness", () => {
    const h = harness();
    expect(h.evidence()).toEqual({ launchId: "launch", generation: "generation", sessionFile, model, thinkingLevel: null,
      observedAt: h.now(), failures: [] });
    expect(h.evidence().context).toBeUndefined();
    expect(buildPendingRunnerState("next", h.now(), h.latest()).runtimeReadiness).toBeUndefined();
    expect(harness("omp").latest().runtimeReadiness).toBeUndefined();
  });

  it("retains original native compaction failure over a fresh idle read", () => {
    const h = harness();
    h.send({ type: "compaction_end", reason: "overflow", aborted: false, errorMessage: "secret", result: undefined });
    const first = h.evidence();
    h.send({ type: "agent_settled" }); h.advance();
    expect(h.core.refreshQuiescence()).toBe(true);
    h.state(h.rpc.at(-1)!.id as string);
    expect(h.evidence().observedAt).toBe(h.now());
    expect(h.evidence().failures).toEqual(first.failures);
    expect(h.evidence().failures[0]).toEqual({ code: "compaction_failed", observedAt: first.observedAt });
    expect(JSON.stringify(h.evidence())).not.toContain("secret");
  });

  it("records manual RPC refusal and resolves only on real compaction result", () => {
    const h = harness();
    h.core.handleUserBlock("/compact"); controlResponse(h, "compact", false);
    expect(h.evidence().failures[0]?.code).toBe("compaction_failed");
    h.send({ type: "agent_start" }); h.send({ type: "agent_settled" });
    expect(h.evidence().failures).toHaveLength(1);
    h.core.handleUserBlock("/compact");
    controlResponse(h, "compact", true, { summary: "private history", estimatedTokensAfter: 5 });
    expect(h.evidence().failures).toEqual([]);
    expect(h.evidence().context).toBeUndefined();
    expect(JSON.stringify(h.evidence())).not.toContain("private history");
  });

  it.each([
    [{ aborted: true }, "compaction_aborted"],
    [{ aborted: false }, "compaction_no_result"],
    [{ aborted: false, errorMessage: "failed" }, "compaction_failed"],
  ])("handles typed automatic outcomes %j", (outcome, code) => {
    const h = harness(); h.send({ type: "compaction_end", reason: "threshold", ...outcome });
    expect(h.evidence().failures[0]?.code).toBe(code);
    h.send({ type: "compaction_end", reason: "threshold", aborted: false, result: { summary: "ok" } });
    expect(h.evidence().failures).toEqual([]);
  });

  it("does not overwrite a precise manual aborted event with its following RPC failure", () => {
    const h = harness(); h.core.handleUserBlock("/compact");
    h.send({ type: "compaction_end", reason: "manual", aborted: true });
    controlResponse(h, "compact", false);
    expect(h.evidence().failures[0]?.code).toBe("compaction_aborted");
  });

  it("resolves failed model selection separately from retained compaction failure", () => {
    const h = harness(); h.send({ type: "compaction_end", errorMessage: "failed" });
    h.core.handleUserBlock("/model provider/missing"); controlResponse(h, "set_model", false);
    expect(h.evidence().failures.map(f => f.code)).toEqual(["model_change_failed", "compaction_failed"]);
    h.core.handleUserBlock("/model provider/model"); controlResponse(h, "set_model", true, model);
    expect(h.evidence().failures.map(f => f.code)).toEqual(["compaction_failed"]);
  });

  it("requires an actual matching successful assistant response to resolve model errors", () => {
    const h = harness(); assistant(h, { stopReason: "error", errorMessage: "private error" });
    h.send({ type: "agent_settled" }); h.advance(); h.state();
    expect(h.evidence().failures[0]?.code).toBe("model_error");
    assistant(h, { model: "another-model" });
    expect(h.evidence().failures[0]?.code).toBe("model_error");
    assistant(h); expect(h.evidence().failures).toEqual([]);
    h.send({ type: "auto_retry_end", success: false, finalError: "private error" });
    expect(h.evidence().failures[0]?.code).toBe("model_error");
    expect(JSON.stringify(h.evidence())).not.toContain("private error");
  });

  it("preserves actual usage time across idle refresh and invalidates changed context", () => {
    const h = harness(); assistant(h);
    const context = h.evidence().context;
    expect(context).toEqual({ usedTokens: 880, remainingTokens: 120, source: "assistant_usage", observedAt: h.now() });
    h.advance(); h.core.refreshQuiescence(); h.state(h.rpc.at(-1)!.id as string);
    expect(h.evidence().context).toEqual(context);
    h.send({ type: "response", id: "pi-runner-cursor-refresh", success: true, data: { entries: [
      { id: "entry", message: { role: "assistant", provider: model.provider, model: model.id, timestamp: 123 } },
    ] } });
    expect(h.evidence().context).toEqual(context);
    h.send({ type: "entry_appended", entry: { type: "context_edit" } });
    expect(h.evidence().context).toBeUndefined();
    assistant(h); h.core.handleUserBlock("new user input");
    expect(h.evidence().context).toBeUndefined();
  });

  it("rejects unknown usage and model mismatch; computes native totals without double-counting reasoning", () => {
    const h = harness();
    assistant(h, { usage: undefined }); expect(h.evidence().context).toBeUndefined();
    assistant(h, { usage: { totalTokens: 0, input: 2 } }); expect(h.evidence().context).toBeUndefined();
    assistant(h, { model: "wrong" }); expect(h.evidence().context).toBeUndefined();
    assistant(h, { usage: { totalTokens: 0, input: 1000, output: 20, cacheRead: 30, cacheWrite: 40, reasoning: 10 } });
    expect(h.evidence().context?.usedTokens).toBe(1090);
    expect(h.evidence().context?.remainingTokens).toBe(-90);
    h.state("pi-runner-get-state", { ...model, contextWindow: 2000 });
    expect(h.evidence().context).toBeUndefined();
  });
  it("does not refresh readiness on token deltas or resolve provider failure by model selection", () => {
    const h = harness(); assistant(h, { stopReason: "error" });
    const observedAt = h.evidence().observedAt;
    h.advance(); h.send({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "text" } });
    expect(h.evidence().observedAt).toBe(observedAt);
    h.core.handleUserBlock("/model provider/missing"); controlResponse(h, "set_model", false);
    h.core.handleUserBlock("/model provider/model"); controlResponse(h, "set_model", true, model);
    expect(h.evidence().failures[0]?.code).toBe("model_error");
    assistant(h); expect(h.evidence().failures).toEqual([]);
  });

  // D1: the installed Pi manual compact preconditions are no-attempt outcomes.
  function precondition(h: ReturnType<typeof harness>, error: string, typed: boolean) {
    h.core.handleUserBlock("/compact");
    h.send({ type: "compaction_start", reason: "manual" });
    if (typed) h.send({ type: "compaction_end", reason: "manual", aborted: false,
      errorMessage: `Compaction failed: ${error}` });
    h.send({ type: "response", id: "pi-runner-native-control", command: "compact", success: false, error });
    h.state("pi-runner-control-state");
  }

  it("D1 leaves already-compacted sessions without a failure after event and RPC refusal", () => {
    for (const typed of [true, false]) {
      const h = harness();
      h.core.handleUserBlock("/compact");
      h.send({ type: "compaction_end", reason: "manual", aborted: false, result: { summary: "complete" } });
      controlResponse(h, "compact", true, { summary: "complete" });
      precondition(h, "Already compacted", typed);
      expect(h.evidence().failures).toEqual([]);
      expect(h.latest().quiescence?.settled).toBe(true);
    }
  });

  it("D1 leaves too-small sessions without a failure after event and RPC refusal", () => {
    for (const typed of [true, false]) {
      const h = harness();
      precondition(h, "Nothing to compact (session too small)", typed);
      expect(h.evidence().failures).toEqual([]);
      expect(h.latest().quiescence?.settled).toBe(true);
    }
  });

  it("D1 retains genuine summarization errors and extension cancellation", () => {
    for (const [event, rpcError, code] of [
      [{ aborted: false, errorMessage: "Compaction failed: provider error" }, "provider error", "compaction_failed"],
      [{ aborted: true }, "Compaction cancelled by extension", "compaction_aborted"],
      // Similar text is not the exact native no-attempt precondition.
      [{ aborted: false, errorMessage: "Compaction failed: Already compacted: provider error" },
        "Already compacted: provider error", "compaction_failed"],
    ] as const) {
      const h = harness(); h.core.handleUserBlock("/compact");
      h.send({ type: "compaction_end", reason: "manual", ...event });
      h.send({ type: "response", id: "pi-runner-native-control", command: "compact", success: false, error: rpcError });
      h.state("pi-runner-control-state");
      expect(h.evidence().failures[0]?.code).toBe(code);
    }
  });

  it("D1 precondition refusals do not clear or retimestamp unresolved compaction failure", () => {
    for (const error of ["Already compacted", "Nothing to compact (session too small)"]) {
      for (const typed of [true, false]) {
        const h = harness();
        h.send({ type: "compaction_end", reason: "manual", aborted: false, errorMessage: "Compaction failed: provider error" });
        const original = h.evidence().failures;
        h.advance(); precondition(h, error, typed);
        expect(h.evidence().failures).toEqual(original);
        expect(h.evidence().failures[0]?.code).toBe("compaction_failed");
      }
    }
  });

});
