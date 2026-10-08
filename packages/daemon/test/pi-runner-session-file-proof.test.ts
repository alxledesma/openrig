import { describe, expect, it } from "vitest";
import { RunnerCore, type RunnerIo } from "../src/adapters/pi-runner.js";
import type { PiRunnerState } from "../src/adapters/pi-runner-protocol.js";

const sessionFile = "/seat/pi/lead/sessions/current.jsonl";

function fixture(input: { childPid?: number; currentChildPid?: number } = {}) {
  const writes: PiRunnerState[] = [];
  const requests: Record<string, unknown>[] = [];
  const io: RunnerIo = {
    sendRpc: (request) => { requests.push(request); },
    mirrorLine: () => {},
    mirrorAppend: () => {},
    postActivity: () => {},
    writeSidecar: (state) => writes.push(state),
    now: () => "2026-10-07T23:00:00.000Z",
    currentChildPid: () => input.currentChildPid ?? input.childPid,
  };
  const core = new RunnerCore(io, {
    sessionName: "lead@app",
    nodeId: "node-1",
    generation: "generation-1",
    launchId: "launch-1",
    childPid: input.childPid,
  }, { runtime: "pi" });
  return { core, writes, requests };
}

const successfulState = () => JSON.stringify({
  type: "response", id: "pi-runner-get-state", success: true,
  data: { sessionFile, sessionId: "native-session-1", model: null, isStreaming: false, isCompacting: false, pendingMessageCount: 0 },
});

describe("managed Pi get_state session-file provenance", () => {
  it("records only a successful matching response from the current spawned child", () => {
    const f = fixture({ childPid: 4321 });
    f.core.handlePiLine(successfulState());
    expect(f.writes.at(-1)?.rpcSessionFileProof).toEqual({
      launchId: "launch-1", generation: "generation-1", childPid: 4321,
      sessionFile, responseId: "pi-runner-get-state", observedAt: "2026-10-07T23:00:00.000Z",
    });
  });

  it("does not accept a late response after the current child identity changes", () => {
    const f = fixture({ childPid: 4321, currentChildPid: 8765 });
    f.core.handlePiLine(successfulState());
    expect(f.writes.at(-1)?.rpcSessionFileProof).toBeUndefined();
  });

  it("clears the previous proof on a failed startup get_state response", () => {
    const f = fixture({ childPid: 4321 });
    f.core.handlePiLine(successfulState());
    f.core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-get-state", success: false, error: "unavailable" }));
    expect(f.writes.at(-1)?.rpcSessionFileProof).toBeUndefined();
  });

  it("does not derive provenance from a different RPC response", () => {
    const f = fixture({ childPid: 4321 });
    f.core.handlePiLine(JSON.stringify({ ...JSON.parse(successfulState()), id: "pi-runner-control-state" }));
    expect(f.writes.at(-1)?.rpcSessionFileProof).toBeUndefined();
  });

  it("records the exact successful bounded quiescence refresh response id", () => {
    const f = fixture({ childPid: 4321 });
    f.core.handlePiLine(successfulState());
    expect(f.core.refreshQuiescence()).toBe(true);
    const request = f.requests.at(-1);
    expect(request?.type).toBe("get_state");
    expect(typeof request?.id).toBe("string");
    f.core.handlePiLine(JSON.stringify({
      type: "response", id: request?.id, success: true,
      data: { sessionFile, sessionId: "native-session-1", model: null, isStreaming: false, isCompacting: false, pendingMessageCount: 0 },
    }));
    expect(f.writes.at(-1)?.rpcSessionFileProof).toEqual({
      launchId: "launch-1", generation: "generation-1", childPid: 4321,
      sessionFile, responseId: request?.id, observedAt: "2026-10-07T23:00:00.000Z",
    });
  });
});
