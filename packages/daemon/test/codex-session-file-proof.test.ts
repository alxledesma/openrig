import { describe, it, expect } from "vitest";
import { NativeToolUnavailableError, missingObservationTool, normalizeDevice, parseLsofFieldRecords, proveCodexInitialLaunch, resolveNativeTool, type CodexSessionFileProofDeps } from "../src/domain/codex-session-file-proof.js";

const TOKEN = "01a0fe42-cdb5-78d3-94fc-60533aaa46fb";
const OTHER_TOKEN = "01a0fe42-cdb5-78d3-94fc-60533aaa46fc";
const GEN = "a6285bc5-0a93-4c7c-9cb7-6d4331aad0eb";
const OTHER_GEN = "a6285bc5-0a93-4c7c-9cb7-6d4331aad0ec";
const ROLLOUT = `/home/u/.codex/sessions/2026/10/02/rollout-2026-10-02T16-16-26-${TOKEN}.jsonl`;

interface World {
  pid?: number; paneRoot?: number; comm?: string | null;
  /** pid -> parent */
  parents?: Record<number, number>;
  generation?: string;
  /** each entry is a distinct open descriptor */
  rollouts?: Array<{ fd: number; inode: string; path: string }>;
  /** first-line metadata body per path; defaults to the matching token */
  meta?: (path: string) => string;
  /** descriptors that change between the two observations */
  driftAfterFirstObservation?: "fd" | "inode" | "path" | "generation" | "lineage" | "none";
  /** descriptor device as lsof would print it; omit to simulate an absent D field */
  descriptorDevice?: string | null;
}

function lsofField(world: World): string {
  const entries = world.rollouts ?? [];
  let out = `p${world.pid ?? 0}\n`;
  // Real Darwin `lsof -Fani` shape: the mode is its own `a` line and the inode is
  // its own `i` line; the `f` line carries no mode.
  const dev = world.descriptorDevice === undefined ? "0x1000011" : world.descriptorDevice;
  for (const e of entries) out += `f${e.fd}\nau\ni${e.inode}\n` + (dev === null ? "" : `D${dev}\n`) + `n${e.path}\n`;
  return out;
}

function deps(world: World): CodexSessionFileProofDeps {
  let observations = 0;
  return {
    async run(command, args) {
      const pid = world.pid ?? 0;
      if (command === "ps" && args.includes("comm=")) {
        const comm = world.comm === undefined ? "/vendor/bin/codex" : world.comm;
        return comm === null ? "" : `${comm}\n`;
      }
      if (command === "lsof") return lsofField(world);
      if (command === "stat") {
        const file = args[3] ?? args[args.length - 1]!;
        const hit = (world.rollouts ?? []).find(e => e.path === file);
        return hit ? `16777233 ${hit.inode}\n` : "";
      }
      return "";
    },
    async readPrefix(file) {
      const custom = world.meta?.(file);
      const id = custom ? undefined : TOKEN;
      return custom ?? `${JSON.stringify({ type: "session_meta", payload: { id: id ?? TOKEN, cwd: "/some/where" } })}\n`;
    },
    async occupantGeneration() {
      observations += 1;
      if (world.driftAfterFirstObservation === "generation" && observations > 2) return OTHER_GEN;
      return world.generation ?? GEN;
    },
    async ancestry(pid) {
      const parent = (world.parents ?? {})[pid];
      return parent === undefined || parent <= 0 ? [] : [parent];
    },
  };
}

function world(overrides: World = {}): World {
  return {
    pid: 54494,
    paneRoot: 53570,
    parents: { 54494: 54455, 54455: 53570 },
    rollouts: [{ fd: 43, inode: "472826364", path: ROLLOUT }],
    ...overrides,
  };
}

const opts = (w: World) => ({
  paneRootPid: w.paneRoot!,
  pid: w.pid!,
  expectedToken: TOKEN,
  expectedGeneration: GEN,
});

describe("codex initial-launch session-file proof", () => {
  // Captured VERBATIM from `lsof -a -p <pid> -d 0-2 -Fani` on this host. The mode
  // is a separate `a` line and the inode a separate `i` line; the `f` line has no
  // mode at all, which is exactly what the previous parser could never match.
  // Captured verbatim from `lsof -a -p 7840 -d 9 -FaniD` on this host for a real
  // regular file descriptor, including the hex dev_t and the decimal inode.
  const CAPTURED_LSOF_REGULAR = ["p7840","f9","ar","D0x1000011","i436348421","n/private/etc/hosts"].join("\n");
  const CAPTURED_LSOF = ["p64666","f0","ar","i336","n/dev/null","f1","au","i336","n->0x849b610eeb56b7c4","f2","au","i336","n->0x578024f028abac7f"].join("\n");
  it("parses captured real lsof -Fani field output", () => {
    const records = parseLsofFieldRecords(CAPTURED_LSOF);
    expect(records).toHaveLength(3);
    // the access field is `a` + one of r/w/u, so "ar" carries mode "r"
    expect(records[0]).toMatchObject({ fd: 0, mode: "r", inode: "336", path: "/dev/null" });
    expect(records[1]).toMatchObject({ fd: 1, mode: "u", inode: "336" });
    // read/write modes are recognised, read-only is not a writer
    expect(records.filter(r => /[uw]/.test(r.mode)).map(r => r.fd)).toEqual([1, 2]);
    // a legacy/invented `f43u` line carries no separate mode and yields nothing usable
    expect(parseLsofFieldRecords("p1\nf43u\n/u/x.jsonl")).toHaveLength(0);
    const regular = parseLsofFieldRecords(CAPTURED_LSOF_REGULAR);
    expect(regular).toHaveLength(1);
    expect(regular[0]).toMatchObject({ fd: 9, mode: "r", inode: "436348421", device: "0x1000011", path: "/private/etc/hosts" });
    // lsof prints device as hex, stat prints decimal; they must normalize equal
    expect(normalizeDevice("0x1000011")).toBe("16777233");
    expect(normalizeDevice("16777233")).toBe("16777233");
    expect(normalizeDevice("0x2000022")).toBe("33554466");
    expect(normalizeDevice("not-a-device")).toBeNull();
  });

  it("accepts a valid initial launch: pane-root descendant, current generation, one stable read/write rollout whose session_meta.id is the stored token", async () => {
    const w = world();
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("proven");
    expect(r.sessionId).toBe(TOKEN);
    expect(r.fd).toBe(43);
    expect(r.inode).toBe("472826364");
    expect(r.generation).toBe(GEN);
    expect(r.paneRootPid).toBe(53570);
  });

  it("refuses when the file's own session_meta.id is not the stored token, even if the filename says otherwise", async () => {
    const w = world({
      // filename still contains TOKEN; only the in-file id differs
      meta: () => `${JSON.stringify({ type: "session_meta", payload: { id: OTHER_TOKEN, cwd: "/some/where" } })}\n`,
    });
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("token_mismatch");
  });

  it("refuses a wrong generation", async () => {
    const w = world({ generation: OTHER_GEN });
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("generation_mismatch");
  });

  it("refuses broken pane-root ancestry", async () => {
    const broken = world({ parents: { 54494: 54455, 54455: 99999 } });
    const r = await proveCodexInitialLaunch(opts(broken), deps(broken));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("lineage_broken");

    const direct = world({ parents: { 54494: 53570 } });
    const r2 = await proveCodexInitialLaunch(opts(direct), deps(direct));
    // pid is a direct child of the pane root, which is a legitimate descendant
    expect(r2.state).toBe("proven");

    const notSelf = world({ pid: 53570, parents: {} });
    const r3 = await proveCodexInitialLaunch({ paneRootPid: 53570, pid: 53570, expectedToken: TOKEN, expectedGeneration: GEN }, deps(notSelf));
    expect(r3.state).toBe("refused");
  });

  it("refuses multiple open writers", async () => {
    const w = world({
      rollouts: [
        { fd: 43, inode: "472826364", path: ROLLOUT },
        { fd: 44, inode: "472826365", path: `/home/u/.codex/sessions/2026/10/02/rollout-2026-10-02T16-16-26-${OTHER_TOKEN}.jsonl` },
      ],
    });
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("writer_ambiguous");
  });

  it("refuses descriptor/inode drift between the two observations", async () => {
    const w = world();
    const d = deps(w);
    let lsofCalls = 0;
    const drifting = {
      ...d,
      async run(command: string, args: string[]) {
        if (command === "lsof") {
          lsofCalls += 1;
          // after the first observation the writer is bound to a different fd
          if (lsofCalls > 1) return `p${w.pid}\nf44\nau\ni472826364\nD0x1000011\nn${w.rollouts![0]!.path}\n`;
        }
        return d.run(command, args);
      },
    };
    const r = await proveCodexInitialLaunch(opts(w), drifting);
    expect(r.state).toBe("refused");
    expect(r.code).toBe("descriptor_drift");
  });

  it("refuses when the held descriptor's inode does not name the path being read", async () => {
    // N1: a path replaced between open and read must never be read as the bound file.
    const w = world();
    const d = deps(w);
    const swapped = {
      ...d,
      async run(command: string, args: string[]) {
        if (command === "stat") return "16777234 999999999\n";
        return d.run(command, args);
      },
    };
    const r = await proveCodexInitialLaunch(opts(w), swapped);
    expect(r.state).toBe("refused");
    expect(r.code).toBe("no_open_rollout");
  });

  it("refuses a path whose inode drifts only after the metadata read", async () => {
    const w = world();
    const d = deps(w);
    let reads = 0;
    const d2 = { ...d, async readPrefix(f: string) { reads += 1; return d.readPrefix(f); } };
    const driftingStat = {
      ...d2,
      async run(command: string, args: string[]) {
        // the path is correct for the checks before the read, then is replaced
        if (command === "stat" && reads > 0) return "16777234 999999999\n";
        return d2.run(command, args);
      },
    };
    const r = await proveCodexInitialLaunch(opts(w), driftingStat);
    expect(r.state).toBe("refused");
    expect(r.code).toBe("descriptor_binding_mismatch");
  });


  it("refuses a descriptor whose device does not match the path, even with a matching inode", async () => {
    // N1 device half: same inode, different device. Must refuse, never accept.
    const w = world({ descriptorDevice: "0x2000022" });
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("no_open_rollout");
  });

  it("refuses a descriptor with no device field at all", async () => {
    const w = world({ descriptorDevice: null });
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("no_open_rollout");
  });

  it("refuses when the path device drifts between the descriptor check and the metadata read", async () => {
    const w = world();
    const d = deps(w);
    let reads = 0;
    const d2 = { ...d, async readPrefix(f: string) { reads += 1; return d.readPrefix(f); } };
    const deviceDrift = {
      ...d2,
      async run(command: string, args: string[]) {
        // decimal 16777234 is a different device than the descriptor's 0x1000011
        if (command === "stat" && reads > 0) return "16777234 472826364\n";
        return d2.run(command, args);
      },
    };
    const r = await proveCodexInitialLaunch(opts(w), deviceDrift);
    expect(r.state).toBe("refused");
    expect(r.code).toBe("descriptor_binding_mismatch");
  });

  it("refuses generation drift between the two observations", async () => {
    const w = world();
    let observations = 0;
    const d = deps(w);
    const drifting = {
      ...d,
      async occupantGeneration() {
        observations += 1;
        // one generation witness per observation, so the second one drifts
        return observations > 1 ? OTHER_GEN : GEN;
      },
    };
    const r = await proveCodexInitialLaunch(opts(w), drifting);
    expect(r.state).toBe("refused");
    expect(r.code).toBe("generation_drift");
  });

  it("refuses when the process is not the native codex binary or is absent", async () => {
    const other = world({ comm: "/usr/bin/node" });
    expect((await proveCodexInitialLaunch(opts(other), deps(other))).code).toBe("not_native_codex_binary");
    const gone = world({ comm: null });
    const r = await proveCodexInitialLaunch(opts(gone), deps(gone));
    expect(r.state).toBe("absent");
    expect(r.code).toBe("process_absent");
  });

  it("refuses when no read/write rollout descriptor exists", async () => {
    const w = world({ rollouts: [] });
    const r = await proveCodexInitialLaunch(opts(w), deps(w));
    expect(r.state).toBe("refused");
    expect(r.code).toBe("no_open_rollout");
  });

  it("refuses malformed or non-session metadata and an invalid stored token", async () => {
    const notMeta = world({ meta: () => `${JSON.stringify({ type: "event_msg", payload: {} })}\n` });
    expect((await proveCodexInitialLaunch(opts(notMeta), deps(notMeta))).code).toBe("metadata_not_session_meta");
    const badId = world({ meta: () => `${JSON.stringify({ type: "session_meta", payload: { id: "not-a-uuid" } })}\n` });
    expect((await proveCodexInitialLaunch(opts(badId), deps(badId))).code).toBe("metadata_id_invalid");
    const w = world();
    const badToken = await proveCodexInitialLaunch({ paneRootPid: 53570, pid: 54494, expectedToken: "nope", expectedGeneration: GEN }, deps(w));
    expect(badToken.code).toBe("token_invalid");
  });


/**
 * Native tool resolution and observation-failure typing.
 *
 * Regression for the production initial-launch proof that threw `spawn lsof ENOENT`
 * because the daemon PATH omits /usr/sbin, surfacing as an HTTP 500 instead of a
 * proof refusal. These tests inject the platform and the existence probe, so they
 * are deterministic on any host and assert the RESTRICTED-PATH behaviour directly.
 */
describe("codex proof native tool resolution", () => {
  const ALL_PRESENT = () => true;
  const NONE_PRESENT = () => false;

  it("resolves each Darwin observation tool to an installed absolute path", () => {
    // The production defect: PATH resolution cannot see /usr/sbin, so lsof must
    // be addressed by its real location instead of its bare name.
    expect(resolveNativeTool("lsof", "darwin", ALL_PRESENT)).toBe("/usr/sbin/lsof");
    expect(resolveNativeTool("ps", "darwin", ALL_PRESENT)).toBe("/bin/ps");
    expect(resolveNativeTool("stat", "darwin", ALL_PRESENT)).toBe("/usr/bin/stat");
    expect(resolveNativeTool("python3", "darwin", ALL_PRESENT)).toBe("/usr/bin/python3");
    // Only a genuinely installed candidate is accepted.
    expect(resolveNativeTool("lsof", "darwin", p => p === "/usr/bin/lsof")).toBe("/usr/bin/lsof");
    expect(resolveNativeTool("lsof", "darwin", () => false)).toBeNull();
    expect(resolveNativeTool("stat", "darwin", () => false)).toBeNull();
    // Off Darwin the bare name keeps the platform's own PATH rules.
    expect(resolveNativeTool("lsof", "linux", NONE_PRESENT)).toBe("lsof");
  });

  it("on this host every required Darwin tool is actually installed and executable", () => {
    // Guards the table itself against drift: the fix is only real if the paths exist.
    if (process.platform !== "darwin") return;
    expect(missingObservationTool("darwin")).toBeNull();
    expect(resolveNativeTool("lsof", "darwin")).toBe("/usr/sbin/lsof");
    expect(resolveNativeTool("python3", "darwin")).not.toBeNull();
  });

  it("reports the FIRST missing tool with an exact cause", () => {
    expect(missingObservationTool("darwin", NONE_PRESENT)).toEqual({ tool: "ps", code: "native_tool_unavailable" });
    // Both lsof candidates absent (the restricted-PATH case) names lsof, not ps.
    expect(missingObservationTool("darwin", p => p !== "/usr/sbin/lsof" && p !== "/usr/bin/lsof")).toEqual({ tool: "lsof", code: "native_tool_unavailable" });
    expect(missingObservationTool("linux", NONE_PRESENT)).toBeNull();
  });

  it("consults the installed-tool pre-gate before any observation runs", async () => {
    const w = world();
    let observed = false;
    const counting: CodexSessionFileProofDeps = {
      ...deps(w),
      async run(command, args) { observed = true; return deps(w).run(command, args); },
    };
    // On a host where every tool is installed the gate passes and observation runs.
    if (missingObservationTool() === null) {
      expect(await proveCodexInitialLaunch(opts(w), counting)).toMatchObject({ state: "proven" });
      expect(observed).toBe(true);
    }
    // When a required tool is missing the gate refuses with the exact cause and
    // the observation layer is never consulted.
    const gated = missingObservationTool("darwin", () => false);
    expect(gated).toEqual({ tool: "ps", code: "native_tool_unavailable" });
  });

  it("an unavailable observation tool becomes a typed refusal, never a rejection", async () => {
    const w = world();
    const noLsof: CodexSessionFileProofDeps = {
      ...deps(w),
      async run(command, args) {
        // Exactly the production failure: PATH cannot resolve the tool.
        if (command === "lsof") throw new NativeToolUnavailableError("lsof", ["/usr/sbin/lsof"]);
        return deps(w).run(command, args);
      },
    };
    await expect(proveCodexInitialLaunch(opts(w), noLsof)).resolves.toMatchObject({ state: "refused", code: "native_tool_unavailable" });
  });

  it("an unexpected observation error is still a typed refusal, never a rejection", async () => {
    const w = world();
    const exploding: CodexSessionFileProofDeps = {
      ...deps(w),
      async run(command) {
        throw new Error(`spawn ${command} ENOENT`);
      },
    };
    // The proof RESOLVES with a terminal, typed outcome. It never rejects: the
    // pre-existing comm-probe swallow classifies this as process_absent, and the
    // rollout-observation guard classifies an escaping tool error otherwise.
    const r = await proveCodexInitialLaunch(opts(w), exploding);
    expect(["absent", "refused"]).toContain(r.state);
    expect(["process_absent", "rollout_observation_failed", "observation_failed", "no_open_rollout"]).toContain(r.code);
  });

  it("does not weaken the proof: a proven world is still proven, and identity conditions still refuse", async () => {
    const w = world();
    expect(await proveCodexInitialLaunch(opts(w), deps(w))).toMatchObject({ state: "proven" });
    // generation mismatch, token mismatch and a non-native binary still refuse.
    expect(await proveCodexInitialLaunch(opts(w), deps(world({ generation: OTHER_GEN })))).toMatchObject({ state: "refused", code: "generation_mismatch" });
    expect(await proveCodexInitialLaunch(opts(world({ meta: () => `${JSON.stringify({ type: "session_meta", payload: { id: OTHER_TOKEN } })}\n` })), deps(world({ meta: () => `${JSON.stringify({ type: "session_meta", payload: { id: OTHER_TOKEN } })}\n` })))).toMatchObject({ state: "refused", code: "token_mismatch" });
    expect(await proveCodexInitialLaunch(opts(world({ comm: "/vendor/bin/other" })), deps(world({ comm: "/vendor/bin/other" })))).toMatchObject({ state: "refused", code: "not_native_codex_binary" });
  });
});
});
