import { describe, expect, it } from "vitest";
import { RunnerCore, type RunnerIo } from "../src/adapters/pi-runner.js";
import {
  buildPiChildArgs, buildPiRunnerArgs, type PiRunnerLaunchOpts, type PiRunnerState,
} from "../src/adapters/pi-runner-protocol.js";
import { piReservedLaunchSelection } from "../src/domain/pi-rotation-native-proof.js";

const model = { provider: "openrouter", id: "fresh-model", contextWindow: 128000, maxTokens: 8192 };
const sessionFile = "/native/pi/session.jsonl";

function runner() {
  const now = "2026-10-08T15:00:00.000Z";
  const states: PiRunnerState[] = [];
  const io: RunnerIo = {
    sendRpc: () => {}, mirrorLine: () => {}, mirrorAppend: () => {}, postActivity: () => {},
    writeSidecar: state => { states.push(state); }, now: () => now,
  };
  const core = new RunnerCore(io, {
    sessionName: "builder@app-handy-conveyor", nodeId: "builder-node", launchId: "launch-one",
    generation: "generation-one", childPid: 4521,
  });
  core.start();
  return {
    core,
    latest: () => states.at(-1)!,
    getState: (thinkingLevel?: unknown) => core.handlePiLine(JSON.stringify({
      type: "response", id: "pi-runner-get-state", success: true,
      data: { sessionFile, sessionId: "native-session", model, thinkingLevel,
        isStreaming: false, isCompacting: false, pendingMessageCount: 0 },
    })),
  };
}

describe("effective Pi model and thinking contract", () => {
  it("derives reserved successor model and effort only from a valid Pi contract", () => {
    const contract = { runtime: "pi", provider: "openrouter", model: "fresh-model", thinkingLevel: "xhigh" };
    expect(piReservedLaunchSelection(contract)).toEqual({ model: "openrouter/fresh-model", effort: "xhigh" });
    expect(() => piReservedLaunchSelection({ ...contract, thinkingLevel: "ultra" })).toThrow();
    expect(() => piReservedLaunchSelection({ ...contract, provider: "openrouter/other" })).toThrow();
  });

  it("records model and thinking only from the successful current-child get_state", () => {
    const h = runner();
    h.getState("xhigh");
    expect(h.latest().runtimeReadiness).toMatchObject({
      launchId: "launch-one", generation: "generation-one", sessionFile,
      model, thinkingLevel: "xhigh", observedAt: "2026-10-08T15:00:00.000Z", failures: [],
    });
  });

  it("invalidates an effective level on a native thinking-change event until a fresh state response", () => {
    const h = runner(); h.getState("high");
    h.core.handlePiLine(JSON.stringify({ type: "thinking_level_changed", level: "low" }));
    expect(h.latest().runtimeReadiness?.thinkingLevel).toBeNull();
    h.getState("low");
    expect(h.latest().runtimeReadiness?.thinkingLevel).toBe("low");
  });

  it.each([undefined, "ultra", "HIGH"]) ("keeps missing or invalid get_state thinking unknown (%s)", thinkingLevel => {
    const h = runner();
    h.getState(thinkingLevel);
    expect(h.latest().runtimeReadiness?.thinkingLevel ?? null).toBeNull();
  });

  it("passes the pinned effective model and thinking explicitly to the runner and Pi child", () => {
    const launch: PiRunnerLaunchOpts = {
      runtime: "pi", runnerEntryPath: "/installed/daemon/pi-runner.js",
      sessionName: "builder@app-handy-conveyor", stateRoot: "/state/pi", cwd: "/workspace/handy",
      launchId: "successor-launch", model: "openrouter/fresh-model", thinkingLevel: "xhigh", trust: "no-approve",
    };
    const runnerArgs = buildPiRunnerArgs(launch);
    expect(runnerArgs.slice(runnerArgs.indexOf("--model"), runnerArgs.indexOf("--model") + 2))
      .toEqual(["--model", "openrouter/fresh-model"]);
    expect(runnerArgs.slice(runnerArgs.indexOf("--thinking"), runnerArgs.indexOf("--thinking") + 2))
      .toEqual(["--thinking", "xhigh"]);

    const childArgs = buildPiChildArgs({
      sessionsDir: "/state/pi/builder@app-handy-conveyor/sessions",
      sessionName: launch.sessionName, model: launch.model, thinkingLevel: launch.thinkingLevel,
      trust: launch.trust, runtime: "pi",
    });
    expect(childArgs.slice(childArgs.indexOf("--model"), childArgs.indexOf("--model") + 2))
      .toEqual(["--model", "openrouter/fresh-model"]);
    expect(childArgs.slice(childArgs.indexOf("--thinking"), childArgs.indexOf("--thinking") + 2))
      .toEqual(["--thinking", "xhigh"]);
  });
});
