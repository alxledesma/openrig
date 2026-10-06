// OPR.0.4.6.PI1 — hermetic unit tests for the pi-runner core: submitted paste
// boundaries (including delayed multi-line sends), stdin→RPC routing
// (idle→prompt / streaming→steer / prefix conventions), the event→mirror and
// event→activity mapping, the get_state identity capture + sidecar, the
// durable catch-up cursor, and honest pi-exit reporting. No live pi.

import { describe, it, expect, vi } from "vitest";
import { PassThrough } from "node:stream";
import {
  createRunnerInput, MAX_PI_INPUT_BYTES, RunnerCore, mapPiEvent, parseRunnerArgs,
  prepareRunnerSidecar,
  type RunnerIo,
} from "../src/adapters/pi-runner.js";
import { PI_RUNNER_READY_MARKER, PI_RUNNER_EXIT_MARKER, parsePiRunnerState, type PiRunnerState } from "../src/adapters/pi-runner-protocol.js";

const SESSION = "devpi-a@some-rig";
const SESSION_FILE = "/state/pi/devpi-a@some-rig/sessions/2026_0197.jsonl";

function fakeIo() {
  const rpc: Record<string, unknown>[] = [];
  const lines: string[] = [];
  const appends: string[] = [];
  const activity: Record<string, unknown>[] = [];
  const sidecars: PiRunnerState[] = [];
  const io: RunnerIo = {
    sendRpc: (cmd) => rpc.push(cmd),
    mirrorLine: (line) => lines.push(line),
    mirrorAppend: (text) => appends.push(text),
    postActivity: (payload) => activity.push(payload),
    writeSidecar: (state) => sidecars.push(state),
    now: () => "2026-07-06T10:00:00Z",
  };
  return { io, rpc, lines, appends, activity, sidecars };
}

function readyCore(f = fakeIo()) {
  const core = new RunnerCore(f.io, { sessionName: SESSION, nodeId: "node-1", launchId: "launch-77" });
  core.start();
  core.handlePiLine(JSON.stringify({
    // success is explicit: idle credit requires a positively successful
    // get_state, never merely an absent failure flag.
    type: "response", id: "pi-runner-get-state", success: true,
    data: { sessionFile: SESSION_FILE, sessionId: "0197a2f0", isStreaming: false, isCompacting: false, pendingMessageCount: 0 },
  }));
  return { core, ...f };
}

// ── Actual Node line editor + framed input ──────────────────────────────────

describe("runner input", () => {
  const start = "\u001b[200~";
  const end = "\u001b[201~";
  function terminal(onSubmit?: (s: string) => void) {
    const input = Object.assign(new PassThrough(), { isTTY: true, isRaw: false,
      setRawMode: vi.fn(function (this: { isRaw: boolean }, value: boolean) { this.isRaw = value; }) });
    const output = Object.assign(new PassThrough(), { isTTY: true, columns: 80 });
    const blocks: string[] = [];
    let screen = "";
    output.on("data", chunk => { screen += chunk.toString(); });
    const editor = createRunnerInput(input as unknown as NodeJS.ReadStream,
      output as unknown as NodeJS.WriteStream, s => { blocks.push(s); onSubmit?.(s); });
    return { input, output, editor, blocks, screen: () => screen };
  }

  it.each([1011, 1012, 1023, 2048, 16384])("accepts %i bytes then abort and next", size => {
    const t = terminal();
    try {
      t.input.write(start + "x".repeat(size) + end + "\r");
      t.input.write("/abort\rnext\r");
      expect(t.blocks).toEqual(["x".repeat(size), "/abort", "next"]);
      expect(t.input.setRawMode).toHaveBeenCalledWith(true);
    } finally { t.editor.close(); }
    expect(t.input.isRaw).toBe(false);
  });

  it("holds delayed paste and preserves whitespace, Unicode and CRLF bytes", () => {
    vi.useFakeTimers();
    const t = terminal();
    try {
      t.input.write(start + " \n café\r\n\n日本語 ");
      vi.advanceTimersByTime(60_000);
      expect(t.blocks).toEqual([]);
      t.input.write(end);
      expect(t.blocks).toEqual([]);
      t.input.write("\r");
      expect(t.blocks).toEqual([" \n café\r\n\n日本語 "]);
    } finally { t.editor.close(); vi.useRealTimers(); }
  });

  it("reassembles UTF-8 and markers across single-byte chunks", () => {
    const t = terminal();
    try {
      for (const byte of Buffer.from(start + "café\n日本語" + end)) t.input.write(Buffer.from([byte]));
      expect(t.blocks).toEqual([]);
      t.input.write("\r");
      expect(t.blocks).toEqual(["café\n日本語"]);
    } finally { t.editor.close(); }
  });

  it("retains mixed typing/paste, cursor insertion, backspace and line clear", () => {
    const t = terminal();
    try {
      t.input.write("prefix " + start + "paste\n café " + end + " suffix\r");
      t.input.write("ac\u001b[Db\r"); // Node's left-arrow editing.
      t.input.write("removeX\u007f\r");
      t.input.write("clear this\u0015kept\r");
      expect(t.blocks).toEqual(["prefix paste\n café  suffix", "abc", "remove", "kept"]);
    } finally { t.editor.close(); }
  });

  it("keeps rapid submissions distinct, with current core routing", () => {
    const { core, rpc } = readyCore();
    const t = terminal(s => core.handleUserBlock(s));
    try {
      t.input.write(start + "first" + end + "\r");
      core.handlePiLine(JSON.stringify({ type: "agent_start" }));
      t.input.write(start + "second" + end + "\r/followup later\r/abort\r");
      expect(rpc.filter(x => ["prompt", "steer", "follow_up", "abort"].includes(String(x.type)))).toEqual([
        { type: "prompt", message: "first", streamingBehavior: "followUp" },
        { type: "steer", message: "second" },
        { type: "follow_up", message: "later" }, { type: "abort" },
      ]);
    } finally { t.editor.close(); }
  });

  it("Ctrl-C cancels an unfinished paste and leaves the next input reachable", () => {
    const t = terminal();
    try {
      t.input.write(start + "unfinished\n/abort\r");
      expect(t.blocks).toEqual([]); // Text in a paste is content, not a command.
      t.input.write("\u0003");
      expect(t.blocks).toEqual(["/abort"]);
      t.input.write(start + "next" + end + "\r");
      expect(t.blocks).toEqual(["/abort", "next"]);
      expect(t.screen()).toContain("input cleared");
    } finally { t.editor.close(); }
  });

  it("rejects an oversized paste without submitting a prefix; controls recover", () => {
    const t = terminal();
    try {
      t.input.write("prefix " + start + "x".repeat(MAX_PI_INPUT_BYTES + 1));
      expect(t.screen()).toContain("input rejected");
      expect(t.blocks).toEqual([]);
      t.input.write(end + "\r/abort\rnext\r");
      expect(t.blocks).toEqual(["/abort", "next"]);
      t.input.write(start + "x".repeat(MAX_PI_INPUT_BYTES + 1) + "\u0003next again\r");
      expect(t.blocks.slice(-2)).toEqual(["/abort", "next again"]);
    } finally { t.editor.close(); }
  });

  it("does not echo framing markers or submit empty input; EOF restores raw mode", () => {
    const t = terminal();
    t.input.write("\r" + start + end + "\r");
    expect(t.blocks).toEqual([]);
    expect(t.screen()).not.toContain(start);
    expect(t.screen()).not.toContain(end);
    t.input.write("\u0004");
    expect(t.input.isRaw).toBe(false);
  });

  it("uses newline-delimited messages for nonterminal input", () => {
    const input = new PassThrough(), output = new PassThrough(), blocks: string[] = [];
    const editor = createRunnerInput(input as unknown as NodeJS.ReadStream,
      output as unknown as NodeJS.WriteStream, block => blocks.push(block));
    input.write(" \nraw one\nraw two\n");
    expect(blocks).toEqual(["raw one", "raw two"]);
    input.end(); editor.close();
  });

  it("preserves terminal line editing under TERM=dumb, while normal terminal and piped input still behave", () => {
    try {
      // 1. Under TERM=dumb with TTY streams: editing (type, backspace, retype) must be retained
      vi.stubEnv("TERM", "dumb");
      const dumbTerm = terminal();
      dumbTerm.input.write("hello worlx\u007fd\r");
      expect(dumbTerm.blocks).toEqual(["hello world"]);
      dumbTerm.editor.close();

      // 2. Under normal terminal: editing behaves as before
      vi.stubEnv("TERM", "xterm-256color");
      const normalTerm = terminal();
      normalTerm.input.write("normal worlx\u007fd\r");
      expect(normalTerm.blocks).toEqual(["normal world"]);
      normalTerm.editor.close();

      // 3. Piped nonterminal input under TERM=dumb: newline-delimited messages behave as before
      vi.stubEnv("TERM", "dumb");
      const pipedIn = new PassThrough(), pipedOut = new PassThrough(), pipedBlocks: string[] = [];
      const pipedEditor = createRunnerInput(pipedIn as unknown as NodeJS.ReadStream,
        pipedOut as unknown as NodeJS.WriteStream, block => pipedBlocks.push(block));
      pipedIn.write("piped one\npiped two\n");
      expect(pipedBlocks).toEqual(["piped one", "piped two"]);
      pipedIn.end();
      pipedEditor.close();
    } finally {
      vi.unstubAllEnvs();
    }
  });
});

// ── stdin → RPC routing ──────────────────────────────────────────────────────

describe("RunnerCore.handleUserBlock", () => {
  it("native model and compact controls never become prompts and refuse unsettled turns", () => {
    const { core, rpc } = readyCore();
    core.handleUserBlock("/model openrouter/inclusionai/ling-3.1-flash");
    expect(rpc.at(-1)).toEqual({ type: "set_model", provider: "openrouter", modelId: "inclusionai/ling-3.1-flash", id: "pi-runner-native-control" });
    const before = rpc.length;
    core.handleUserBlock("/compact");
    expect(rpc).toHaveLength(before);
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-native-control", command: "set_model", success: true }));
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    core.handleUserBlock("/compact");
    expect(rpc.at(-1)?.type).not.toBe("compact");
    core.handlePiLine(JSON.stringify({ type: "agent_settled" }));
    core.handleUserBlock("/compact Preserve current assignments");
    expect(rpc.at(-1)).toEqual({ type: "compact", customInstructions: "Preserve current assignments", id: "pi-runner-native-control" });
  });

  it("invalid model control is refused without prompt or RPC", () => {
    const { core, rpc } = readyCore();
    const before = rpc.length;
    core.handleUserBlock("/model bad");
    expect(rpc).toHaveLength(before);
  });
  it.each([{ isCompacting: true }, { pendingMessageCount: 1 }, { isCompacting: undefined }])("controls refuse busy or unknown native state %j without reannouncing identity", (patch) => {
    const { core, rpc, activity, lines } = readyCore();
    core.handleUserBlock("/model");
    const announcements = activity.length, readyLines = lines.filter(l => l.startsWith(PI_RUNNER_READY_MARKER)).length;
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-native-control", command: "get_available_models", success: true, data: { models: [] } }));
    expect(rpc.at(-1)).toEqual({ type: "get_state", id: "pi-runner-control-state" });
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-control-state", success: true, data: { isStreaming: false, isCompacting: false, pendingMessageCount: 0, ...patch } }));
    const before = rpc.length;
    core.handleUserBlock("/compact");
    expect(rpc).toHaveLength(before);
    expect(activity).toHaveLength(announcements);
    expect(lines.filter(l => l.startsWith(PI_RUNNER_READY_MARKER))).toHaveLength(readyLines);
  });
  it("idle → RPC prompt", () => {
    const { core, rpc } = readyCore();
    core.handleUserBlock("hello pi");
    expect(rpc.at(-1)).toEqual({ type: "prompt", message: "hello pi", streamingBehavior: "followUp" });
  });

  it("streaming → RPC steer (Pi's documented mid-stream delivery)", () => {
    const { core, rpc } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handleUserBlock("change course");
    expect(rpc.at(-1)).toEqual({ type: "steer", message: "change course" });
  });

  it("back to prompt after agent_end", () => {
    const { core, rpc } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    core.handleUserBlock("next task");
    // agent_end stops the low-level run, but Pi keeps isStreaming=true until
    // agent_settled; the followUp declaration makes this same RPC correct in
    // both cases (idle: ignored, started; still-processing: queued).
    expect(rpc.at(-1)).toEqual({ type: "prompt", message: "next task", streamingBehavior: "followUp" });
  });

  it("busy window after agent_end (compaction while mirrored idle) → prompt queues via followUp, never a bare rejected prompt", () => {
    // Live symptom reproduction: agent_end flips the runner's streaming mirror
    // to false, then automatic compaction runs while Pi's isStreaming is
    // still true. A bare prompt there got "ERROR rpc: Agent is already
    // processing…" and the message was lost.
    const { core, rpc } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    core.handlePiLine(JSON.stringify({ type: "compaction_start" }));
    core.handleUserBlock("queued during compaction");
    expect(rpc.at(-1)).toEqual({ type: "prompt", message: "queued during compaction", streamingBehavior: "followUp" });
    // Exactly one RPC for the message: no steer/prompt pair, no replay.
    expect(rpc.filter((c) => c.message === "queued during compaction")).toHaveLength(1);
  });

  it("compaction events alone never flip the streaming mirror or bypass steer mid-stream", () => {
    const { core, rpc } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify({ type: "compaction_start" }));
    core.handleUserBlock("still steering mid-stream");
    expect(rpc.at(-1)).toEqual({ type: "steer", message: "still steering mid-stream" });
    core.handlePiLine(JSON.stringify({ type: "compaction_end" }));
    core.handleUserBlock("still steering after compaction_end");
    expect(rpc.at(-1)).toEqual({ type: "steer", message: "still steering after compaction_end" });
  });

  it("/abort → RPC abort; /followup → RPC follow_up", () => {
    const { core, rpc } = readyCore();
    core.handleUserBlock("/abort");
    expect(rpc.at(-1)).toEqual({ type: "abort" });
    core.handleUserBlock("/followup after this turn");
    expect(rpc.at(-1)).toEqual({ type: "follow_up", message: "after this turn" });
  });
});

// ── identity capture + sidecar + catch-up cursor ─────────────────────────────

describe("RunnerCore identity + sidecar", () => {
  it("get_state response → READY marker + sidecar + session_identity POST with sessionFile", () => {
    const { lines, activity, sidecars } = readyCore();
    expect(lines.some((l) => l.startsWith(PI_RUNNER_READY_MARKER))).toBe(true);
    const sidecar = sidecars.at(-1)!;
    expect(sidecar).toMatchObject({ ready: true, launchId: "launch-77", sessionFile: SESSION_FILE, sessionId: "0197a2f0" });
    const identity = activity.find((a) => a.eventFamily === "session_identity")!;
    expect(identity).toMatchObject({
      runtime: "pi", sessionName: SESSION, sessionId: "0197a2f0", sessionFile: SESSION_FILE,
    });
  });

  it("start() with a catch-up cursor issues get_entries since (durable catch-up, FR-5)", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION }, { catchUpSince: "entry-42" });
    core.start();
    expect(f.rpc).toContainEqual({ type: "get_entries", since: "entry-42", id: "pi-runner-catch-up" });
  });

  it("events carrying entry ids advance the sidecar cursor", () => {
    const { core, sidecars } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "agent_start", entryId: "entry-7" }));
    expect(sidecars.at(-1)!.lastEntryId).toBe("entry-7");
  });

  it.each(["notify", "setStatus", "select"])("extension UI %s request IDs cannot replace a durable cursor", (method) => {
    const { core, sidecars } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-cursor-refresh", data: { entries: [{ id: "durable-entry" }] } }));
    core.handlePiLine(JSON.stringify({ type: "extension_ui_request", id: "ephemeral-request-uuid", method }));
    expect(sidecars.at(-1)!.lastEntryId).toBe("durable-entry");
  });

  it("pi exit → EXIT marker + sidecar exited + idle activity (honest, never frozen)", () => {
    const { core, lines, sidecars, activity } = readyCore();
    core.handlePiExit(1);
    expect(lines.some((l) => l.startsWith(PI_RUNNER_EXIT_MARKER))).toBe(true);
    expect(sidecars.at(-1)!.exited).toEqual({ code: 1, at: "2026-07-06T10:00:00Z" });
    expect(activity.at(-1)).toMatchObject({ hookEvent: "Stop", subtype: "pi_exited" });
  });

  it("non-JSON pi stdout noise is mirrored verbatim, never swallowed", () => {
    const { core, lines } = readyCore();
    core.handlePiLine("some stray warning");
    expect(lines).toContain("some stray warning");
  });
});

// ── event → mirror / activity mapping ────────────────────────────────────────

// Shapes from a real `pi --mode rpc` 0.87.1 run (OpenRig's child argv) that
// streamed a reply from a local OpenAI-compatible mock. Since Pi 0.84.0,
// message_update carries only `usage` and the `assistantMessageEvent` delta.
const PI_USAGE = {
  input: 12, output: 5, cacheRead: 0, cacheWrite: 0, reasoning: 0, totalTokens: 17,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
const PI_REPLY = {
  role: "assistant",
  content: [
    { type: "thinking", thinking: "User wants a greeting.", thinkingSignature: "reasoning_content" },
    { type: "text", text: "Hello from the mock server." },
  ],
  api: "openai-completions", provider: "mock", model: "mock-model", usage: PI_USAGE,
  stopReason: "stop", timestamp: 1790281248272, responseId: "chatcmpl-mock",
};
const piUpdate = (assistantMessageEvent: Record<string, unknown>) =>
  ({ type: "message_update", usage: PI_USAGE, assistantMessageEvent });
const PI_REPLY_EVENTS = [
  { type: "message_start", message: { ...PI_REPLY, content: [], stopReason: "pending" } },
  piUpdate({ type: "thinking_start", contentIndex: 0 }),
  piUpdate({ type: "thinking_delta", contentIndex: 0, delta: "User wants a greeting." }),
  piUpdate({ type: "text_start", contentIndex: 1 }),
  piUpdate({ type: "text_delta", contentIndex: 1, delta: "Hello" }),
  piUpdate({ type: "text_delta", contentIndex: 1, delta: " from the" }),
  piUpdate({ type: "text_delta", contentIndex: 1, delta: " mock server." }),
  piUpdate({ type: "thinking_end", contentIndex: 0, content: "User wants a greeting." }),
  piUpdate({ type: "text_end", contentIndex: 1, content: "Hello from the mock server." }),
  { type: "message_end", message: PI_REPLY },
];

describe("mapPiEvent", () => {
  it("agent_start/agent_end drive streaming + running/idle activity", () => {
    expect(mapPiEvent({ type: "agent_start" })).toMatchObject({
      streaming: true, activity: { hookEvent: "active", subtype: "agent_start" },
    });
    expect(mapPiEvent({ type: "agent_end" })).toMatchObject({
      streaming: false, activity: { hookEvent: "Stop", subtype: "agent_end" },
    });
  });

  it("pre-0.84 message_update appends only the delta, never the cumulative message", () => {
    const partial = { ...PI_REPLY, content: [{ type: "text", text: "Hello from the" }], stopReason: "pending" };
    const legacy = mapPiEvent({
      type: "message_update",
      message: partial,
      assistantMessageEvent: { type: "text_delta", contentIndex: 1, delta: " from the", partial },
    });
    expect(legacy.mirrorAppend).toBe(" from the");
  });

  it("tool executions render compact one-line summaries + PreToolUse activity", () => {
    const start = mapPiEvent({ type: "tool_execution_start", toolName: "bash" });
    expect(start.mirrorLines[0]).toContain("bash");
    expect(start.activity).toEqual({ hookEvent: "PreToolUse", subtype: "bash" });
    const failed = mapPiEvent({ type: "tool_execution_end", toolName: "bash", isError: true });
    expect(failed.mirrorLines[0]).toContain("FAILED");
  });

  it("compaction completion distinguishes success, failure, cancellation and missing evidence", () => {
    expect(mapPiEvent({ type: "compaction_end", result: { summary: "retained" }, aborted: false }).mirrorLines).toEqual(["[pi] compaction done"]);
    const failed = mapPiEvent({ type: "compaction_end", errorMessage: "Context overflow recovery failed", aborted: false });
    expect(failed.mirrorLines).toEqual(["[pi] compaction failed"]);
    expect(failed.errorNotice).toBeDefined();
    expect(mapPiEvent({ type: "compaction_end", aborted: true }).mirrorLines).toEqual(["[pi] compaction aborted"]);
    expect(mapPiEvent({ type: "compaction_end" }).mirrorLines).toEqual(["[pi] compaction ended without a result"]);
  });

  it("compaction and retry map to their honest states", () => {
    expect(mapPiEvent({ type: "compaction_start" }).activity).toEqual({ hookEvent: "active", subtype: "compaction" });
    expect(mapPiEvent({ type: "auto_retry_start" }).activity).toEqual({ hookEvent: "active", subtype: "auto_retry" });
  });

  it("leaves auto_compaction_* unmapped for Pi so an idle seat never reads as running", () => {
    for (const type of ["auto_compaction_start", "auto_compaction_end"]) {
      expect(mapPiEvent({ type })).toEqual({ mirrorLines: [] });
      expect(mapPiEvent({ type }, "pi")).toEqual({ mirrorLines: [] });
    }
    expect(mapPiEvent({ type: "auto_compaction_start" }, "omp").activity).toEqual({ hookEvent: "active", subtype: "compaction" });
  });
});

describe("RunnerCore assistant reply mirror", () => {
  it("streams a Pi 0.84+ reply from text deltas and ends the line on message_end", () => {
    const f = fakeIo();
    const { core } = readyCore(f);
    let pane = "";
    f.io.mirrorLine = (line) => { pane += `${line}\n`; };
    f.io.mirrorAppend = (text) => { pane += text; };
    for (const event of PI_REPLY_EVENTS) core.handlePiLine(JSON.stringify(event));
    expect(pane).toBe("Hello from the mock server.\n");
  });
});

describe("RunnerCore terminal assistant failures", () => {
  const failure = (errorMessage?: unknown, content: unknown[] = []) => ({
    type: "message_end", message: { role: "assistant", stopReason: "error", errorMessage, content },
  });
  const errors = (lines: string[]) => lines.filter(line => line.startsWith("[pi-runner] ERROR"));

  it("shows an empty native error and ends partial text without replaying its content", () => {
    const { core, lines, appends } = readyCore();
    core.handlePiLine(JSON.stringify(failure('400 "field_not_allowed"')));
    expect(errors(lines)).toEqual(['[pi-runner] ERROR 400 "field_not_allowed"']);
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify(piUpdate({ type: "text_delta", delta: "Partial answer" })));
    core.handlePiLine(JSON.stringify(failure("connection ended", [{ type: "text", text: "Partial answer" }])));
    expect(appends).toEqual(["Partial answer"]);
    expect(lines.slice(-2)).toEqual(["", "[pi-runner] ERROR connection ended"]);
  });

  it.each([undefined, null, {}, 7, "", " \n\t "])("uses a useful fallback for invalid error detail %j", (detail) => {
    const { core, lines } = readyCore();
    core.handlePiLine(JSON.stringify(failure(detail)));
    expect(errors(lines)).toEqual(["[pi-runner] ERROR request failed"]);
  });

  it("ignores malformed message envelopes and does not dump thinking or tool arguments", () => {
    const { core, lines, appends } = readyCore();
    for (const message of [undefined, null, 7, [], { role: "user", stopReason: "error" }]) {
      core.handlePiLine(JSON.stringify({ type: "message_end", message }));
    }
    core.handlePiLine(JSON.stringify(piUpdate({ type: "thinking_delta", delta: "private-thought" })));
    core.handlePiLine(JSON.stringify(piUpdate({ type: "toolcall_delta", delta: "private-arguments" })));
    core.handlePiLine(JSON.stringify(failure(undefined, [
      { type: "thinking", thinking: "private-thought" }, { type: "toolCall", arguments: "private-arguments" },
    ])));
    expect(errors(lines)).toEqual(["[pi-runner] ERROR request failed"]);
    expect(appends).toEqual([]);
    expect(lines.join("\n")).not.toMatch(/private-thought|private-arguments/);
  });

  it("strips terminal controls, flattens newlines and bounds error notices", () => {
    const { core, lines } = readyCore();
    core.handlePiLine(JSON.stringify(failure("\u001b[2J\u001b]0;title\u0007bad\r\nrequest\u0000\u202e" + "x".repeat(1000))));
    const notice = errors(lines)[0]!;
    expect(notice).toContain("bad request");
    expect(notice).not.toMatch(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u);
    expect(notice).not.toContain("title");
    expect(notice.length).toBeLessThanOrEqual(420);
  });

  it("shows standalone exhausted retry once and suppresses its duplicate terminal notice", () => {
    const { core, lines } = readyCore();
    const retryEnd = { type: "auto_retry_end", success: false, finalError: "busy" };
    core.handlePiLine(JSON.stringify(retryEnd));
    core.handlePiLine(JSON.stringify(retryEnd));
    expect(errors(lines)).toEqual(["[pi-runner] ERROR busy"]);
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify(failure("busy")));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    core.handlePiLine(JSON.stringify({ type: "auto_retry_end", success: false }));
    expect(errors(lines)).toEqual(["[pi-runner] ERROR busy", "[pi-runner] ERROR busy"]);
  });

  it("resets for a subsequent successful turn and a later independent failure", () => {
    const { core, lines, appends, activity, sidecars } = readyCore();
    core.handlePiLine(JSON.stringify(failure("first failure")));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    for (const event of PI_REPLY_EVENTS) core.handlePiLine(JSON.stringify(event));
    core.handlePiLine(JSON.stringify({ type: "auto_retry_end", success: true }));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    expect(appends.join("")).toBe("Hello from the mock server.");
    expect(errors(lines)).toEqual(["[pi-runner] ERROR first failure"]);
    expect(activity.at(-1)).toMatchObject({ hookEvent: "Stop", subtype: "agent_end" });
    expect(sidecars.at(-1)).toMatchObject({ ready: true, sessionFile: SESSION_FILE, launchId: "launch-77" });
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify({ type: "auto_retry_end", success: false, finalError: "first failure" }));
    expect(errors(lines)).toHaveLength(2);
  });
});

// ── argv contract ────────────────────────────────────────────────────────────

describe("parseRunnerArgs", () => {
  const base = ["--session-name", SESSION, "--state-root", "/sr", "--cwd", "/work", "--launch-id", "launch-77"];

  it("requires --launch-id (launch-attempt scoping, guard fold)", () => {
    const noLaunch = ["--session-name", SESSION, "--state-root", "/sr", "--cwd", "/work", "--approve"];
    expect(() => parseRunnerArgs(noLaunch)).toThrow(/--launch-id is required/);
  });

  it("requires an EXPLICIT trust flag (BR-5)", () => {
    expect(() => parseRunnerArgs(base)).toThrow(/explicit trust flag/);
    expect(parseRunnerArgs([...base, "--no-approve"]).trust).toBe("no-approve");
    expect(parseRunnerArgs([...base, "--approve"]).trust).toBe("approve");
  });

  it("refuses --session + --fork together", () => {
    expect(() => parseRunnerArgs([...base, "--approve", "--session", "/a.jsonl", "--fork", "/b.jsonl"]))
      .toThrow(/mutually exclusive/);
  });

  it("rejects unknown flags loudly", () => {
    expect(() => parseRunnerArgs([...base, "--approve", "--resume"])).toThrow(/unknown flag/);
  });
});

// ── FR-5: prepareRunnerSidecar + cursor seed (guard re-verdict fold) ─────────

describe("prepareRunnerSidecar — the cursor survives the runner's own reset", () => {
  function memFsOps(files: Record<string, string>) {
    return {
      files,
      readFile: (p: string) => { if (!(p in files)) throw new Error("ENOENT"); return files[p]!; },
      writeFile: (p: string, c: string) => { files[p] = c; },
      exists: (p: string) => p in files,
    };
  }
  const PATH = "/seat/runner-state.json";
  const prior = JSON.stringify({ ready: true, launchId: "old", lastEntryId: "entry-42", updatedAt: "t" });

  it("reads the prior cursor BEFORE overwriting, carries it in the pending record, returns it when resuming", () => {
    const fs = memFsOps({ [PATH]: prior });
    const { catchUpSince } = prepareRunnerSidecar(fs, PATH, "launch-9", true, () => "t2");
    expect(catchUpSince).toBe("entry-42");
    expect(JSON.parse(fs.files[PATH]!)).toEqual({ ready: false, launchId: "launch-9", lastEntryId: "entry-42", updatedAt: "t2" });
  });

  it("fresh/fork (not resuming) returns no catch-up but still preserves the record's cursor", () => {
    const fs = memFsOps({ [PATH]: prior });
    const { catchUpSince } = prepareRunnerSidecar(fs, PATH, "launch-9", false, () => "t2");
    expect(catchUpSince).toBeUndefined();
    expect(JSON.parse(fs.files[PATH]!).lastEntryId).toBe("entry-42");
  });

  it("absent/unreadable prior sidecar -> pending without cursor, no catch-up", () => {
    const fs = memFsOps({});
    const { catchUpSince } = prepareRunnerSidecar(fs, PATH, "launch-9", true, () => "t2");
    expect(catchUpSince).toBeUndefined();
    expect(JSON.parse(fs.files[PATH]!)).toEqual({ ready: false, launchId: "launch-9", updatedAt: "t2" });
  });

  it("COMPOSED (the guard's red-green case): prior cursor -> prepare -> RunnerCore.start sends get_entries since", () => {
    const fs = memFsOps({ [PATH]: prior });
    const { catchUpSince } = prepareRunnerSidecar(fs, PATH, "launch-9", true, () => "t2");
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-9" }, { catchUpSince });
    core.start();
    expect(f.rpc).toContainEqual({ type: "get_entries", since: "entry-42", id: "pi-runner-catch-up" });
  });

  it("the seeded cursor survives the core's own post-get_state sidecar write", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-9" }, { catchUpSince: "entry-42" });
    core.start();
    core.handlePiLine(JSON.stringify({
      type: "response", id: "pi-runner-get-state",
      data: { sessionFile: SESSION_FILE, sessionId: "0197a2f0" },
    }));
    expect(f.sidecars.at(-1)).toMatchObject({ ready: true, launchId: "launch-9", lastEntryId: "entry-42" });
  });
});

// ── QA RED fold: the cursor refreshes from get_entries, not live-event guesses ─

describe("cursor refresh via get_entries (QA RED, qitem-20260707020922)", () => {
  it("agent_end triggers a cursor-refresh get_entries request", () => {
    const { core, rpc } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    expect(rpc).toContainEqual({ type: "get_entries", id: "pi-runner-cursor-refresh" });
  });

  it("the refresh response advances lastEntryId from the LAST entry and persists it", () => {
    const { core, sidecars } = readyCore();
    core.handlePiLine(JSON.stringify({
      type: "response", id: "pi-runner-cursor-refresh",
      data: { entries: [{ id: "e1" }, { id: "e2" }, { id: "e9" }] },
    }));
    expect(sidecars.at(-1)).toMatchObject({ lastEntryId: "e9", launchId: "launch-77" });
  });

  it("the catch-up response also advances the cursor (restart path)", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-9" }, { catchUpSince: "e1" });
    core.start();
    core.handlePiLine(JSON.stringify({
      type: "response", id: "pi-runner-catch-up",
      entries: [{ id: "e2" }, { id: "e3" }],
    }));
    expect(f.sidecars.at(-1)!.lastEntryId).toBe("e3");
  });

  it("missing inherited cursor refreshes without replaying controls", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION }, { catchUpSince: "poisoned-uuid" });
    core.start();
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-catch-up", command: "get_entries", success: false, error: "Entry not found: poisoned-uuid" }));
    expect(f.rpc.at(-1)).toEqual({ type: "get_entries", id: "pi-runner-cursor-refresh" });
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-cursor-refresh", success: true, data: { entries: [{ id: "saved-tail" }] } }));
    expect(f.sidecars.at(-1)!.lastEntryId).toBe("saved-tail");
    expect(f.rpc.every(r => r.type === "get_state" || r.type === "get_entries")).toBe(true);
  });

  it.each([true, false])("compact response (%s) refreshes the durable cursor", (success) => {
    const { core, rpc } = readyCore();
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-native-control", command: "compact", success }));
    expect(rpc).toContainEqual({ type: "get_entries", id: "pi-runner-cursor-refresh" });
  });

  it("failed cursor response cannot publish an unverified entry", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION }, { catchUpSince: "saved-tail" });
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-cursor-refresh", success: false, data: { entries: [{ id: "unverified" }] } }));
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-get-state", data: { sessionFile: SESSION_FILE, sessionId: "x" } }));
    expect(f.sidecars.at(-1)!.lastEntryId).toBe("saved-tail");
  });

  it("an empty/id-less entries response leaves the cursor untouched (never regresses)", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-9" }, { catchUpSince: "e5" });
    core.start();
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-cursor-refresh", data: { entries: [] } }));
    core.handlePiLine(JSON.stringify({
      type: "response", id: "pi-runner-get-state",
      data: { sessionFile: SESSION_FILE, sessionId: "x" },
    }));
    expect(f.sidecars.at(-1)!.lastEntryId).toBe("e5");
  });
});

// ── Durable native quiescence evidence (evidence only: no authority,
// recovery, queue, guard, qualification or send semantics read it) ─────────

describe("RunnerCore native quiescence projection", () => {
  const q = (f: { sidecars: PiRunnerState[] }) => f.sidecars.at(-1)!.quiescence;

  it("publishes settled from a quiet successful get_state, bound to this launch and native session file", () => {
    const f = readyCore();
    expect(q(f)).toMatchObject({ launchId: "launch-77", sessionFile: SESSION_FILE, settled: true });
  });

  it("agent_start and compaction_start are persisted busy", () => {
    const f = readyCore();
    expect(q(f)!.settled).toBe(true);
    f.core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    expect(q(f)!.settled).toBe(false);
    f.core.handlePiLine(JSON.stringify({ type: "compaction_start" }));
    expect(q(f)!.settled).toBe(false);
  });

  it("agent_end never settles — retries and compaction continue after it", () => {
    const f = readyCore();
    f.core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    f.core.handlePiLine(JSON.stringify({ type: "agent_end" }));
    expect(q(f)!.settled).toBe(false);
  });

  it("agent_settled publishes settled true", () => {
    const f = readyCore();
    f.core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    f.core.handlePiLine(JSON.stringify({ type: "agent_settled" }));
    expect(q(f)!.settled).toBe(true);
  });

  it("a fresh submitted turn marks busy before the RPC is issued", () => {
    const f = readyCore();
    expect(q(f)!.settled).toBe(true);
    f.core.handleUserBlock("do the thing");
    expect(q(f)!.settled).toBe(false);
    expect(f.rpc.at(-1)).toMatchObject({ type: "prompt" });
  });

  it("an in-flight control keeps the seat busy and settles only from a following quiet get_state", () => {
    const f = readyCore();
    f.core.handleUserBlock("/compact Preserve assignments");
    expect(q(f)).toMatchObject({ settled: false });
    f.core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-native-control", command: "compact", success: true }));
    expect(q(f)!.settled).toBe(false);
    f.core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-control-state", success: true, data: { isStreaming: false, isCompacting: false, pendingMessageCount: 0 } }));
    expect(q(f)!.settled).toBe(true);
  });

  it.each([
    ["busy response", { type: "response", id: "pi-runner-control-state", success: true, data: { isStreaming: true, isCompacting: false, pendingMessageCount: 0 } }],
    ["pending messages", { type: "response", id: "pi-runner-control-state", success: true, data: { isStreaming: false, isCompacting: false, pendingMessageCount: 2 } }],
    ["failed control state", { type: "response", id: "pi-runner-control-state", success: false }],
    ["missing control state", { type: "response", id: "pi-runner-control-state" }],
  ])("a %s stays busy and unproven, never idle", (_label, record) => {
    const f = readyCore();
    f.core.handleUserBlock("/compact go");
    f.core.handlePiLine(JSON.stringify(record));
    expect(q(f)!.settled).toBe(false);
  });

  it("a busy initial get_state never claims settled", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-77" });
    core.start();
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-get-state", data: { sessionFile: SESSION_FILE, isStreaming: true } }));
    expect(q(f)!.settled).toBe(false);
  });

  it("an exited seat is non-running, never settled", () => {
    const f = readyCore();
    f.core.handlePiExit(0);
    expect(q(f)!.settled).toBe(false);
    expect(f.sidecars.at(-1)!.exited).toMatchObject({ code: 0 });
  });

  it("every projection is launch-stamped and carries no free-text claim", () => {
    const f = readyCore();
    f.core.handlePiLine(JSON.stringify({ type: "agent_start" }));
    f.core.handlePiLine(JSON.stringify({ type: "agent_settled" }));
    expect(f.sidecars.length).toBeGreaterThan(2);
    for (const s of f.sidecars) expect(s.quiescence).toMatchObject({ launchId: "launch-77", settled: expect.any(Boolean) });
    expect(JSON.stringify(f.sidecars.at(-1)!.quiescence)).not.toMatch(/idle|quiet|free/i);
  });
});

// ── R2 review findings: every sidecar write republishes bound evidence ─────

describe("quiescence evidence survives every projection write", () => {
  const q = (f: { sidecars: PiRunnerState[] }) => f.sidecars.at(-1)!.quiescence;

  it("a get_entries catch-up cursor write retains the current bound evidence", () => {
    const f = readyCore();
    const before = q(f)!;
    f.core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-catch-up", data: { entries: [{ id: "e-42" }] } }));
    const after = q(f)!;
    expect(after).toBeDefined();
    expect(after).toMatchObject({ launchId: "launch-77", sessionFile: SESSION_FILE, settled: before.settled, lastEntryId: "e-42" });
  });

  it("a session-entry cursor write on an event retains the current bound evidence", () => {
    const f = readyCore();
    f.core.handlePiLine(JSON.stringify({ type: "message_start", message: { role: "assistant" }, entryId: "e-77" }));
    expect(q(f)).toMatchObject({ launchId: "launch-77", sessionFile: SESSION_FILE, lastEntryId: "e-77" });
    expect(q(f)!.settled).toBe(false);
  });

  it("no sidecar write anywhere drops the evidence", () => {
    const f = readyCore();
    f.core.handleUserBlock("hello");
    f.core.handlePiLine(JSON.stringify({ type: "agent_settled" }));
    f.core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-cursor-refresh", data: { entries: [{ id: "e-99" }] } }));
    f.core.handlePiExit(0);
    expect(f.sidecars.length).toBeGreaterThan(4);
    for (const s of f.sidecars) {
      expect(s.quiescence).toBeDefined();
      expect(s.quiescence).toMatchObject({ launchId: "launch-77", observedAt: expect.any(String) });
      expect(typeof s.quiescence!.settled).toBe("boolean");
    }
    expect(q(f)!.settled).toBe(false);
  });

  it.each([
    ["success absent", { type: "response", id: "pi-runner-get-state", data: { sessionFile: SESSION_FILE, isStreaming: false, isCompacting: false, pendingMessageCount: 0 } }],
    ["success false", { type: "response", id: "pi-runner-get-state", success: false, data: { sessionFile: SESSION_FILE, isStreaming: false, isCompacting: false, pendingMessageCount: 0 } }],
    ["success true but busy", { type: "response", id: "pi-runner-get-state", success: true, data: { sessionFile: SESSION_FILE, isStreaming: true, isCompacting: false, pendingMessageCount: 0 } }],
  ])("a get_state with %s never proves idle", (_label, record) => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-77" });
    core.start();
    core.handlePiLine(JSON.stringify(record));
    expect(q(f)!.settled).toBe(false);
  });

  it("an explicitly successful quiet get_state is the only initial idle credit", () => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, launchId: "launch-77" });
    core.start();
    // The adapter pre-writes the pending sidecar, so the runner itself has not
    // yet published anything: absent evidence, not idle.
    expect(f.sidecars).toHaveLength(0);
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-get-state", success: true, data: { sessionFile: SESSION_FILE, isStreaming: false, isCompacting: false, pendingMessageCount: 0 } }));
    expect(q(f)!.settled).toBe(true);
  });
});

// ── Bounded producer refresh (F1): a QUIET settled seat must be able to renew
// its evidence from a REAL response, never by retimestamping a stale sidecar.

describe("RunnerCore bounded quiescence refresh", () => {
  const qOf = (f: { sidecars: PiRunnerState[] }) => f.sidecars.at(-1)!.quiescence!;

  it("a quiet settled seat refreshes its evidence from a real get_state response", () => {
    const f = readyCore();
    const first = qOf(f).observedAt;
    expect(qOf(f).settled).toBe(true);
    // Advance the fake clock: the refresh must move observedAt forward because
    // the RUNNER re-proved the state, not because a reader touched the file.
    f.io.now = () => new Date(Date.parse(first) + 5_000).toISOString();
    // No transition occurred: the sidecar is identical until the runner asks.
    expect(qOf(f).observedAt).toBe(first);
    // The refresh issues exactly one bounded get_state.
    expect(f.core.refreshQuiescence()).toBe(true);
    expect(f.rpc.filter(r => String(r.id).startsWith("pi-runner-quiescence-refresh-"))).toHaveLength(1);
    // Only a real response advances the record.
    f.core.handlePiLine(JSON.stringify({
      type: "response", id: f.rpc.at(-1)!.id, success: true,
      data: { sessionFile: SESSION_FILE, isStreaming: false, isCompacting: false, pendingMessageCount: 0 },
    }));
    expect(qOf(f).settled).toBe(true);
    expect(qOf(f).observedAt).not.toBe(first);
  });

  it("a FAILED refresh writes nothing, so the evidence ages out instead of renewing", () => {
    const f = readyCore();
    const before = qOf(f).observedAt;
    const sidecarCount = f.sidecars.length;
    expect(f.core.refreshQuiescence()).toBe(true);
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id, success: false, error: "no state" }));
    // No new sidecar write at all: freshness cannot be manufactured.
    expect(f.sidecars.length).toBe(sidecarCount);
    expect(qOf(f).observedAt).toBe(before);
  });

  it("a missing-success response rewrites nothing, so it cannot renew freshness", () => {
    const f = readyCore();
    const before = qOf(f).observedAt;
    const count = f.sidecars.length;
    expect(f.core.refreshQuiescence()).toBe(true);
    // success absent: the handler refuses to treat it as proof of a quiet state
    // and writes nothing at all.
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id }));
    expect(f.sidecars.length).toBe(count);
    expect(qOf(f).observedAt).toBe(before);
  });

  it("refuses to refresh while a turn is active or a control is pending", () => {
    const f = readyCore();
    f.core.handleUserBlock("do work");
    expect(f.core.refreshQuiescence()).toBe(false);
    expect(f.rpc.filter(r => String(r.id).startsWith("pi-runner-quiescence-refresh-"))).toHaveLength(0);
  });

  it("issues at most ONE refresh in flight", () => {
    const f = readyCore();
    expect(f.core.refreshQuiescence()).toBe(true);
    expect(f.core.refreshQuiescence()).toBe(false);
    // A real response releases the guard for the next bounded tick.
    f.core.handlePiLine(JSON.stringify({
      type: "response", id: f.rpc.at(-1)!.id, success: true,
      data: { sessionFile: SESSION_FILE, isStreaming: false, isCompacting: false, pendingMessageCount: 0 },
    }));
    expect(f.core.refreshQuiescence()).toBe(true);
  });
});

// ── Race: a late idle refresh must never overwrite an ACTIVE turn ───────────

describe("late idle refresh cannot settle an active turn", () => {
  const qOf = (f: { sidecars: PiRunnerState[] }) => f.sidecars.at(-1)!.quiescence!;
  const QUIET = { sessionFile: SESSION_FILE, isStreaming: false, isCompacting: false, pendingMessageCount: 0 };
  const BUSY = { sessionFile: SESSION_FILE, isStreaming: true, isCompacting: false, pendingMessageCount: 0 };

  it("an idle response issued BEFORE agent_start is discarded, and writes nothing", () => {
    const f = readyCore();
    expect(f.core.refreshQuiescence()).toBe(true);      // issued while quiet
    f.core.handlePiLine(JSON.stringify({ type: "agent_start" })); // a turn begins
    expect(qOf(f).settled).toBe(false);
    const afterStart = qOf(f).observedAt;
    const writesAfterStart = f.sidecars.length;
    // The late idle answer now arrives. It is for the PREVIOUS epoch.
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id, success: true, data: QUIET }));
    // The seat must NOT be reported settled, and the sidecar must not be renewed:
    // no write at all follows the discarded response.
    expect(qOf(f).settled).toBe(false);
    expect(qOf(f).observedAt).toBe(afterStart);
    expect(f.sidecars.length).toBe(writesAfterStart);
  });

  it("an idle response issued BEFORE a submitted prompt is discarded", () => {
    const f = readyCore();
    expect(f.core.refreshQuiescence()).toBe(true);
    f.core.handleUserBlock("go");                      // prompt -> new epoch
    expect(qOf(f).settled).toBe(false);
    const stamp = qOf(f).observedAt;
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id, success: true, data: QUIET }));
    expect(qOf(f).settled).toBe(false);
    expect(qOf(f).observedAt).toBe(stamp);
  });

  it("an idle response issued BEFORE a native control is discarded", () => {
    const f = readyCore();
    expect(f.core.refreshQuiescence()).toBe(true);
    f.core.handleUserBlock("/compact go");              // control -> new epoch
    const stamp = qOf(f).observedAt;
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id, success: true, data: QUIET }));
    expect(qOf(f).settled).toBe(false);
    expect(qOf(f).observedAt).toBe(stamp);
  });

  it("a CURRENT-epoch idle response is still honoured", () => {
    const f = readyCore();
    expect(f.core.refreshQuiescence()).toBe(true);
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id, success: true, data: QUIET }));
    expect(qOf(f).settled).toBe(true);
  });

  it("a current-epoch BUSY response still marks the seat running", () => {
    const f = readyCore();
    expect(f.core.refreshQuiescence()).toBe(true);
    f.core.handlePiLine(JSON.stringify({ type: "response", id: f.rpc.at(-1)!.id, success: true, data: BUSY }));
    expect(qOf(f).settled).toBe(false);
  });
});


describe("Pi repair R2 refresh request lifecycle", () => {
  const quiet = { sessionFile: SESSION_FILE, sessionId: "0197a2f0", isStreaming: false, isCompacting: false, pendingMessageCount: 0 };
  const reply = (f: ReturnType<typeof readyCore>, id: unknown, data: Record<string, unknown> = quiet) => f.core.handlePiLine(JSON.stringify({ type: "response", id, success: true, data }));
  it("unsolicited and duplicate quiet replies never renew evidence", () => {
    const f = readyCore();const before = f.sidecars.length;
    reply(f, "pi-runner-quiescence-refresh");expect(f.sidecars).toHaveLength(before);
    f.core.refreshQuiescence();const id = f.rpc.at(-1)!.id;reply(f, id);const accepted = f.sidecars.length;
    f.io.now = () => "2026-07-06T10:00:05Z";reply(f, id);expect(f.sidecars).toHaveLength(accepted);
  });
  it("wrong native session identity cannot grant or renew settled proof", () => {
    const f = readyCore();f.core.refreshQuiescence();const before = f.sidecars.length;
    reply(f, f.rpc.at(-1)!.id, { ...quiet, sessionFile: "/another/session.jsonl" });expect(f.sidecars).toHaveLength(before);
    f.core.refreshQuiescence();reply(f, f.rpc.at(-1)!.id, { ...quiet, sessionId: "foreign-native" });expect(f.sidecars).toHaveLength(before);
  });
  it("missing native state does not renew a prior settled observation", () => {
    const f = readyCore();f.core.refreshQuiescence();const before = f.sidecars.length;
    reply(f, f.rpc.at(-1)!.id, { isStreaming: false, isCompacting: false, pendingMessageCount: 0 });expect(f.sidecars).toHaveLength(before);
  });
  it("a lost response expires; a unique successor request cannot consume its late answer", () => {
    const f = readyCore();f.core.refreshQuiescence();const old = f.rpc.at(-1)!.id;
    f.io.now = () => "2026-07-06T10:00:05Z";expect(f.core.refreshQuiescence()).toBe(true);const fresh = f.rpc.at(-1)!.id;expect(fresh).not.toBe(old);
    const before = f.sidecars.length;reply(f, old);expect(f.sidecars).toHaveLength(before);expect(f.core.refreshQuiescence()).toBe(false);
    reply(f, fresh);expect(f.sidecars).toHaveLength(before+1);
  });
  it("reply after its finite read deadline cannot renew even before the next tick", () => {
    const f = readyCore();f.core.refreshQuiescence();const id = f.rpc.at(-1)!.id;const before = f.sidecars.length;
    f.io.now = () => "2026-07-06T10:00:05Z";reply(f,id);expect(f.sidecars).toHaveLength(before);
  });
  it("compaction begun while read is in flight cannot be cleared by a late quiet response", () => {
    const f = readyCore();f.core.refreshQuiescence();const id = f.rpc.at(-1)!.id;
    f.core.handlePiLine(JSON.stringify({ type: "compaction_start" }));const before = f.sidecars.length;
    reply(f,id);expect(f.sidecars).toHaveLength(before);expect(f.sidecars.at(-1)!.quiescence!.settled).toBe(false);
  });
  it("Pi exit invalidates pending request and clears its own refresh timer", () => {
    const f = readyCore();f.core.refreshQuiescence();const id = f.rpc.at(-1)!.id;const timer = setInterval(() => {},5000);const clear = vi.spyOn(globalThis,"clearInterval");
    try { f.core.setQuiescenceRefreshInterval(timer);f.core.handlePiExit(0);expect(clear).toHaveBeenCalledWith(timer);const before=f.sidecars.length;reply(f,id);expect(f.sidecars).toHaveLength(before); }
    finally { clearInterval(timer);clear.mockRestore(); }
  });
  it("cursor replies after failed refresh cannot retimestamp prior settled evidence", () => {
    const f=readyCore();const observed=f.sidecars.at(-1)!.quiescence!.observedAt;f.core.refreshQuiescence();
    f.core.handlePiLine(JSON.stringify({type:"response",id:f.rpc.at(-1)!.id,success:false,error:"unavailable"}));
    f.io.now=()=>"2026-07-06T10:00:30Z";f.core.handlePiLine(JSON.stringify({type:"response",id:"pi-runner-cursor-refresh",success:true,data:{entries:[{id:"later-entry"}]}}));
    expect(f.sidecars.at(-1)!.quiescence!.observedAt).toBe(observed);
  });
});


describe("native model-window metadata", () => {
  it.each(["pi", "omp"] as const)("publishes allowlisted %s metadata and requests catalog once", runtime => {
    const f = fakeIo();
    const core = new RunnerCore(f.io, { sessionName: SESSION, nodeId: "node-1", launchId: "launch-model" }, { runtime });
    core.start();
    const model = { provider: "openrouter", id: "test", contextWindow: 262144, maxTokens: 8192 };
    const state = { type: "response", id: "pi-runner-get-state", success: true, data: { sessionFile: SESSION_FILE, sessionId: "0197", isStreaming: false, model: { ...model, secret: "excluded" } } };
    core.handlePiLine(JSON.stringify(state));
    core.handlePiLine(JSON.stringify(state));
    expect(f.rpc.filter(r => r.id === "pi-runner-models")).toHaveLength(1);
    expect(f.lines.some(l => l.includes("READY"))).toBe(true);
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-models", success: true, data: { models: [{ ...model, secret: "excluded" }, { ...model, contextWindow: -1 }] } }));
    expect(f.sidecars.at(-1)?.model).toEqual(model);
    expect(f.sidecars.at(-1)?.models).toEqual([model]);
    if (runtime === "omp") expect(f.sidecars.at(-1)?.quiescence).toBeUndefined();
  });
  it("keeps malformed native windows unknown", () => {
    const f = fakeIo(); const { core } = readyCore(f);
    core.handlePiLine(JSON.stringify({ type: "response", id: "pi-runner-models", success: true, data: { models: [{ provider: "p", id: "m", contextWindow: 0 }] } }));
    expect(f.sidecars.at(-1)?.model).toBeNull();
    expect(f.sidecars.at(-1)?.models).toBeNull();
  });
});

 describe("model-window control freshness", () => {
  it("invalidates old serving window and only restores successful native metadata", () => {
    const f = readyCore();
    const model = { provider: "p", id: "large", contextWindow: 1000000 };
    f.core.handlePiLine(JSON.stringify({type:"response",id:"pi-runner-get-state",success:true,data:{sessionFile:SESSION_FILE,sessionId:"0197a2f0",isStreaming:false,isCompacting:false,pendingMessageCount:0,model}}));
    expect(f.sidecars.at(-1)!.model).toEqual(model);
    f.core.handleUserBlock("/model p/small");
    expect(f.sidecars.at(-1)!.model).toBeNull();
    f.core.handlePiLine(JSON.stringify({type:"response",id:"pi-runner-native-control",success:true}));
    const small = {...model,id:"small",contextWindow:32000};
    f.core.handlePiLine(JSON.stringify({type:"response",id:"pi-runner-control-state",success:true,data:{isStreaming:false,isCompacting:false,pendingMessageCount:0,model:small}}));
    expect(f.sidecars.at(-1)!.model).toEqual(small);
    f.core.handlePiLine(JSON.stringify({type:"response",id:"pi-runner-control-state",success:false}));
    expect(f.sidecars.at(-1)!.model).toBeNull();
  });
});

it("sidecar reads reproject untrusted native model fields", () => {
 const m={provider:"p",id:"m",contextWindow:32000};
 const parsed=parsePiRunnerState(JSON.stringify({ready:true,updatedAt:"now",model:{...m,secret:"excluded"},models:[{...m,secret:"excluded"},{...m,contextWindow:0}]}));
 expect(parsed!.model).toEqual(m);expect(parsed!.models).toEqual([m]);
});
