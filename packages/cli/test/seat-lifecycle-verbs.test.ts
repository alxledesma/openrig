// S5 (OPR.0.5.4.7) — CLI surface for rig seat set-model / stop / clean: required
// options, route paths + bodies posted, human output, refusal printing, --json
// pass-through with non-zero exit.
import { describe, it, expect, vi, afterEach } from "vitest";
import { Command } from "commander";
import { seatCommand } from "../src/commands/seat.js";
import { terminalAuthHeaders } from "../src/client.js";
import { STATE_FILE, type DaemonState, type LifecycleDeps } from "../src/daemon-lifecycle.js";
import type { StatusDeps } from "../src/commands/status.js";

function mockLifecycleDeps(): LifecycleDeps {
  return {
    spawn: vi.fn(() => ({ pid: 1, unref: vi.fn() }) as never),
    fetch: vi.fn(async () => ({ ok: true })),
    kill: vi.fn(() => true),
    readFile: vi.fn((p: string) => {
      if (p === STATE_FILE) {
        return JSON.stringify({ pid: 123, port: 7433, db: "test.sqlite", startedAt: "2026-04-20T00:00:00Z" } as DaemonState);
      }
      return null;
    }),
    writeFile: vi.fn(),
    removeFile: vi.fn(),
    exists: vi.fn((p: string) => p === STATE_FILE),
    mkdirp: vi.fn(),
    openForAppend: vi.fn(() => 3),
    isProcessAlive: vi.fn(() => true),
  };
}

function makeDeps(response: { status: number; data: unknown }, calls: Array<{ path: string; body: unknown }>): StatusDeps {
  return {
    lifecycleDeps: mockLifecycleDeps(),
    clientFactory: () => ({
      get: vi.fn(async (path: string) => { calls.push({ path, body: undefined }); return response; }),
      post: vi.fn(async (path: string, body: unknown) => { calls.push({ path, body }); return response; }),
    }) as unknown as ReturnType<StatusDeps["clientFactory"]>,
  };
}

/** Records the FULL request-options object each post() receives, so a test can assert on the
 * timeout and headers the CLI actually passed, not merely that a post happened. */
function makeOptionDeps(
  response: { status: number; data: unknown },
  posts: Array<{ path: string; body: unknown; options: unknown }>,
): { deps: StatusDeps; postCalls: () => number } {
  const post = vi.fn(async (path: string, body: unknown, options?: unknown) => {
    posts.push({ path, body, options });
    return response;
  });
  const deps: StatusDeps = {
    lifecycleDeps: mockLifecycleDeps(),
    clientFactory: () => ({
      get: vi.fn(async () => response),
      post,
    }) as unknown as ReturnType<StatusDeps["clientFactory"]>,
  };
  return { deps, postCalls: () => post.mock.calls.length };
}

function applyExitOverride(cmd: Command): void {
  cmd.exitOverride();
  for (const sub of cmd.commands) applyExitOverride(sub as Command);
}

function makeCommand(deps: StatusDeps): Command {
  const program = new Command();
  program.addCommand(seatCommand(deps));
  applyExitOverride(program);
  return program;
}

async function captureLogs(fn: () => Promise<void>): Promise<{ logs: string[]; errors: string[]; exitCode: number | undefined }> {
  const logs: string[] = [];
  const errors: string[] = [];
  const originalLog = console.log;
  const originalError = console.error;
  const originalExitCode = process.exitCode;
  process.exitCode = undefined;
  vi.spyOn(console, "log").mockImplementation((...args) => logs.push(args.join(" ")));
  vi.spyOn(console, "error").mockImplementation((...args) => errors.push(args.join(" ")));
  try {
    await fn();
  } finally {
    console.log = originalLog;
    console.error = originalError;
  }
  const exitCode = process.exitCode;
  process.exitCode = originalExitCode;
  return { logs, errors, exitCode };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("rig seat set-model", () => {
  it("posts model/reason/operator to /api/seat/set-model/<seat> and prints the from->to summary", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({
      status: 200,
      data: { ok: true, seat: { logicalId: "dev.impl", rigName: "seat-rig" }, from: "fable", to: "claude-fable-5", changed: true },
    }, calls);
    const { logs, exitCode } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync(["node", "rig", "seat", "set-model", "dev-impl@seat-rig", "--model", "claude-fable-5", "--reason", "alias migration", "--operator", "op@rig"]);
    });
    expect(calls[0]!.path).toBe("/api/seat/set-model/dev-impl%40seat-rig");
    expect(calls[0]!.body).toEqual({ model: "claude-fable-5", reason: "alias migration", operator: "op@rig" });
    expect(exitCode).toBeUndefined();
    expect(logs.join("\n")).toContain("fable -> claude-fable-5");
  });

  it("requires --model and --reason (commander rejects before any request)", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({ status: 200, data: {} }, calls);
    await expect(
      makeCommand(deps).parseAsync(["node", "rig", "seat", "set-model", "dev-impl@seat-rig", "--reason", "x"]),
    ).rejects.toThrow(/--model/);
    await expect(
      makeCommand(deps).parseAsync(["node", "rig", "seat", "set-model", "dev-impl@seat-rig", "--model", "m"]),
    ).rejects.toThrow(/--reason/);
    expect(calls).toHaveLength(0);
  });

  it("prints the daemon's refusal (message + guidance) and exits 1 on 4xx", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({
      status: 409,
      data: { ok: false, code: "seat_ambiguous", message: 'Seat "dev.impl" matched multiple nodes', guidance: "List seats with: rig ps --nodes", matches: [{ rig_name: "a", logical_id: "dev.impl", current_occupant: null }] },
    }, calls);
    const { errors, exitCode } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync(["node", "rig", "seat", "set-model", "dev.impl", "--model", "m", "--reason", "x"]);
    });
    expect(exitCode).toBe(1);
    expect(errors.join("\n")).toContain("matched multiple nodes");
    expect(errors.join("\n")).toContain("rig ps --nodes");
  });
});

describe("rig seat set-codex-profile", () => {
  it("posts profile/reason to the audited route", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({ status: 200, data: { ok: true, seat: { logicalId: "dev.qa", rigName: "xv" },
      from: "old", to: "xv-sol61-low-continuity", changed: true, effect: "Future managed launches only." } }, calls);
    const { logs, exitCode } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync(["node", "rig", "seat", "set-codex-profile", "dev-qa@xv", "--profile", "xv-sol61-low-continuity", "--reason", "native tuple"]);
    });
    expect(calls[0]).toEqual({ path: "/api/seat/set-codex-profile/dev-qa%40xv",
      body: { profile: "xv-sol61-low-continuity", reason: "native tuple" } });
    expect(exitCode).toBeUndefined();
    expect(logs.join("\n")).toContain("pinned (audited)");
  });

  it("requires profile and reason before posting", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({ status: 200, data: {} }, calls);
    await expect(makeCommand(deps).parseAsync(["node", "rig", "seat", "set-codex-profile", "dev-qa@xv", "--reason", "x"])).rejects.toThrow(/--profile/);
    await expect(makeCommand(deps).parseAsync(["node", "rig", "seat", "set-codex-profile", "dev-qa@xv", "--profile", "p"])).rejects.toThrow(/--reason/);
    expect(calls).toHaveLength(0);
  });
});

describe("rig seat stop", () => {
  it("posts reason to /api/seat/stop/<seat> and prints the stopped summary", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({
      status: 200,
      data: { ok: true, seat: { logicalId: "dev.impl", rigName: "seat-rig" }, sessionName: "dev-impl@seat-rig", sessionId: "S1" },
    }, calls);
    const { logs } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync(["node", "rig", "seat", "stop", "dev-impl@seat-rig", "--reason", "wave boundary"]);
    });
    expect(calls[0]!.path).toBe("/api/seat/stop/dev-impl%40seat-rig");
    expect(calls[0]!.body).toEqual({ reason: "wave boundary", operator: undefined });
    expect(logs.join("\n")).toContain("Stopped dev-impl@seat-rig");
    expect(logs.join("\n")).toContain("siblings untouched");
  });

  it("--json passes the refusal through verbatim and exits 1", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const refusal = { ok: false, code: "session_not_live", message: "not alive", guidance: "rig seat clean" };
    const deps = makeDeps({ status: 409, data: refusal }, calls);
    const { logs, exitCode } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync(["node", "rig", "seat", "stop", "dev-impl@seat-rig", "--reason", "x", "--json"]);
    });
    expect(exitCode).toBe(1);
    expect(JSON.parse(logs.join(""))).toEqual(refusal);
  });
});

describe("rig seat clean", () => {
  it("posts reason to /api/seat/clean/<seat> and prints the actions summary", async () => {
    const calls: Array<{ path: string; body: unknown }> = [];
    const deps = makeDeps({
      status: 200,
      data: { ok: true, seat: { logicalId: "dev.impl", rigName: "seat-rig" }, actions: { sessionsExited: ["dev-impl@seat-rig"], bindingCleared: true } },
    }, calls);
    const { logs } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync(["node", "rig", "seat", "clean", "dev-impl@seat-rig", "--reason", "clean exit observed"]);
    });
    expect(calls[0]!.path).toBe("/api/seat/clean/dev-impl%40seat-rig");
    expect(logs.join("\n")).toContain("Sessions marked exited: dev-impl@seat-rig");
    expect(logs.join("\n")).toContain("binding cleared: yes");
    expect(logs.join("\n")).toContain("launchable again");
  });
});

// Production Handy rehost CLI timed out at the client default 5000ms while the daemon had
// already completed the rehost durably (durable event + strict native proof). These assert
// the REQUEST OPTIONS, so a future refactor cannot silently drop the extended timeout or
// reintroduce a shortened one, and cannot strip terminal auth from the rehost call.
describe("rehost-runner request timeout", () => {
  const REHOST_ARGS = ["node", "rig", "seat", "rehost-runner", "intake-lead@app-handy-conveyor", "--reason", "same generation rehost", "--operator", "operator-agent@kernel"];

  it("posts rehost-runner with an explicit 60000ms timeout AND the terminal auth headers", async () => {
    const posts: Array<{ path: string; body: unknown; options: unknown }> = [];
    const { deps, postCalls } = makeOptionDeps({ status: 200, data: { ok: true, code: "rehost_runner_ok" } }, posts);
    // terminalAuthHeaders() reads the bearer token from the environment, so a real token is
    // set here; otherwise it returns {} and "auth retained" would be vacuously true.
    const previousToken = process.env.OPENRIG_TERMINAL_BEARER_TOKEN;
    process.env.OPENRIG_TERMINAL_BEARER_TOKEN = "test-terminal-token";
    try {
      await captureLogs(async () => { await makeCommand(deps).parseAsync(REHOST_ARGS); });
    } finally {
      if (previousToken === undefined) delete process.env.OPENRIG_TERMINAL_BEARER_TOKEN;
      else process.env.OPENRIG_TERMINAL_BEARER_TOKEN = previousToken;
    }
    expect(postCalls()).toBe(1);
    expect(posts[0]!.path).toBe("/api/seat/rehost-runner/intake-lead%40app-handy-conveyor");
    // The whole point of the fix: 60000ms, not the 5000ms client default.
    expect((posts[0]!.options as { timeoutMs: number }).timeoutMs).toBe(60_000);
    expect((posts[0]!.options as { timeoutMs: number }).timeoutMs).not.toBe(5_000);
    // Terminal auth must survive alongside the timeout.
    const headers = (posts[0]!.options as { headers: Record<string, string> }).headers;
    expect(headers).toBeDefined();
    expect(headers.Authorization).toBe("Bearer test-terminal-token");
  });

  it("passes the auth headers unchanged: the timeout is added, terminalAuthHeaders is preserved", async () => {
    const posts: Array<{ path: string; body: unknown; options: unknown }> = [];
    const { deps } = makeOptionDeps({ status: 200, data: { ok: true } }, posts);
    await captureLogs(async () => { await makeCommand(deps).parseAsync(REHOST_ARGS); });
    const options = posts[0]!.options as { headers: Record<string, string>; timeoutMs: number };
    // Compare against the real helper, not a hand-written guess.
    expect(options.headers).toEqual(terminalAuthHeaders());
    expect(options.timeoutMs).toBe(60_000);
  });

  it("keeps every OTHER lifecycle verb on the unchanged default timeout (no explicit value)", async () => {
    const others: Array<[string, string[]]> = [
      ["stop", ["node", "rig", "seat", "stop", "s@r", "--reason", "x"]],
      ["clean", ["node", "rig", "seat", "clean", "s@r", "--reason", "x"]],
      ["set-model", ["node", "rig", "seat", "set-model", "s@r", "--model", "m", "--reason", "x"]],
      ["set-permissions", ["node", "rig", "seat", "set-permissions", "s@r", "--mode", "floor", "--reason", "x"]],
    ];
    for (const [name, argv] of others) {
      const posts: Array<{ path: string; body: unknown; options: unknown }> = [];
      const { deps, postCalls } = makeOptionDeps({ status: 200, data: { ok: true } }, posts);
      await captureLogs(async () => { await makeCommand(deps).parseAsync(argv); });
      expect(postCalls(), `${name} should post once`).toBe(1);
      const options = posts[0]!.options as { timeoutMs?: number } | undefined;
      // Absent means the client default (5000ms) still applies: unchanged behaviour.
      expect(options?.timeoutMs, `${name} must not receive an explicit timeout`).toBeUndefined();
    }
  });

  it("keeps set-cwd and set-codex-profile on auth headers WITHOUT a timeout", async () => {
    for (const argv of [
      ["node", "rig", "seat", "set-cwd", "s@r", "--cwd", "/new", "--reason", "x"],
      ["node", "rig", "seat", "set-codex-profile", "s@r", "--profile", "xv-sol61-low-continuity", "--reason", "x"],
    ]) {
      const posts: Array<{ path: string; body: unknown; options: unknown }> = [];
      const { deps } = makeOptionDeps({ status: 200, data: { ok: true } }, posts);
      await captureLogs(async () => { await makeCommand(deps).parseAsync(argv); });
      const options = posts[0]!.options as { headers: Record<string, string>; timeoutMs?: number };
      expect(options.headers).toEqual(terminalAuthHeaders());
      expect(options.timeoutMs).toBeUndefined();
    }
  });

  it("sends the rehost exactly once: no retry is layered on top of the longer timeout", async () => {
    const posts: Array<{ path: string; body: unknown; options: unknown }> = [];
    const { deps, postCalls } = makeOptionDeps({ status: 200, data: { ok: true } }, posts);
    await captureLogs(async () => { await makeCommand(deps).parseAsync(REHOST_ARGS); });
    // A rehost must never be issued twice; the longer timeout must not become a retry.
    expect(postCalls()).toBe(1);
    expect(posts).toHaveLength(1);
  });

  it("still surfaces a rehost refusal verbatim with --json and exit 1, unchanged by the timeout", async () => {
    const posts: Array<{ path: string; body: unknown; options: unknown }> = [];
    const refusal = { ok: false, code: "rehost_pane_root_unresolved", message: "refused before any signal", guidance: "read the pane" };
    const { deps } = makeOptionDeps({ status: 409, data: refusal }, posts);
    const { logs, exitCode } = await captureLogs(async () => {
      await makeCommand(deps).parseAsync([...REHOST_ARGS, "--json"]);
    });
    expect(exitCode).toBe(1);
    expect(JSON.parse(logs.join(""))).toEqual(refusal);
    expect((posts[0]!.options as { timeoutMs: number }).timeoutMs).toBe(60_000);
  });
});

describe("rig seat set-cwd",()=>{
 it("posts saved cwd and reason without any launch request",async()=>{const calls:Array<{path:string;body:unknown}>=[];const deps=makeDeps({status:200,data:{ok:true,from:"/old",to:"/new",changed:true,effect:"future launches only"}},calls);await captureLogs(async()=>{await makeCommand(deps).parseAsync(["node","rig","seat","set-cwd","worker@tagmaster","--cwd","/new","--reason","project move"]);});expect(calls).toEqual([{path:"/api/seat/set-cwd/worker%40tagmaster",body:{cwd:"/new",reason:"project move"}}]);});
 it("requires cwd and reason",async()=>{const deps=makeDeps({status:200,data:{}},[]);await expect(makeCommand(deps).parseAsync(["node","rig","seat","set-cwd","worker@rig","--reason","x"])).rejects.toThrow(/--cwd/);await expect(makeCommand(deps).parseAsync(["node","rig","seat","set-cwd","worker@rig","--cwd","/new"])).rejects.toThrow(/--reason/);});
});
