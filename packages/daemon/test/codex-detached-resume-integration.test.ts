import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, writeFileSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { CodexRuntimeAdapter } from "../src/adapters/codex-runtime-adapter.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
import type { CodexRehostBinding, CodexRehostNativeState, CodexRehostOptions } from "../src/domain/codex-rehost.js";
import { createCodexRehostIntegration } from "../src/domain/codex-rehost-integration.js";

const os = vi.hoisted(() => ({ run: vi.fn(), native: vi.fn() }));
vi.mock("node:child_process", async importOriginal => {
  const original = await importOriginal<typeof import("node:child_process")>();
  const { promisify } = await import("node:util");
  const execFile = vi.fn();
  Object.defineProperty(execFile, promisify.custom, { value: (...args: unknown[]) => os.run(...args) });
  return { ...original, execFile };
});
vi.mock("../src/domain/rotation-facts-resolver.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/domain/rotation-facts-resolver.js")>(),
  resolveCodexNativeState: (...args: unknown[]) => os.native(...args),
}));
vi.mock("../src/domain/seat-launch-environment.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/domain/seat-launch-environment.js")>(),
  structuredNativeExecutable: () => ({ executable: "/fixture/codex", args: [] }),
}));

type FactorySeams = { deps: CodexRehostOptions };
describe("detached Codex production integration", () => {
  let db: Database.Database, dir: string, profileFile: string;
  let binding: CodexRehostBinding, native: CodexRehostNativeState, factory: FactorySeams;
  let probeSession: ReturnType<typeof vi.fn>, getSessionEnv: ReturnType<typeof vi.fn>;
  const profile = () => ['model="gpt-6-luna"', 'model_provider="openai"', 'model_reasoning_effort="medium"', 'approval_policy="never"', 'sandbox_mode="workspace-write"'].join("\n") + "\n";

  beforeEach(() => {
    os.run.mockReset().mockResolvedValue({ stdout: "", stderr: "" }); os.native.mockReset();
    db = createDb(); seed(db);
    dir = mkdtempSync(path.join(tmpdir(), "detached-codex-integration-"));
    profileFile = path.join(dir, "exact.config.toml"); writeFileSync(profileFile, profile());
    db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='medium',codex_config_profile='exact',policy_launch_posture='floor' WHERE id='lead@xv'").run(dir);
    db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token='native-exact' WHERE node_id='lead@xv'").run();
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding','lead@xv','lead@xv','%78')").run();
    binding = { nodeId: "lead@xv", sessionId: "session", sessionName: "lead@xv", generation: "lead-g1", runtime: "codex", nativeId: "native-exact", cwd: dir, model: "gpt-6-luna", effort: "medium", codexConfigProfile: "exact" };
    native = { nodeId: binding.nodeId, sessionName: binding.sessionName, nativeId: binding.nativeId, transcriptPath: path.join(dir, "native.jsonl"), runtimeContract: { runtime: "codex", model: "gpt-6-luna", provider: "openai", profile: "exact", effort: "medium", permissions: { sandbox: { type: "workspace-write" }, approval: "never" } } };
    writeFileSync(native.transcriptPath, JSON.stringify({type:"session_meta",payload:{id:binding.nativeId,model_provider:"openai"}})+"\n"+JSON.stringify({type:"turn_context",payload:{model:"gpt-6-luna",effort:"medium",approval_policy:"never",sandbox_policy:{type:"workspace-write"}}})+"\n");
    os.native.mockResolvedValue({ who: { identity: { nodeId: binding.nodeId } }, usage: { sessionId: binding.nativeId, transcriptPath: native.transcriptPath }, runtimeContract: native.runtimeContract });
    probeSession = vi.fn(async () => ({ state: "absent" })); getSessionEnv = vi.fn(async (_seat: string, key: string) => ({OPENRIG_NODE_ID:binding.nodeId,OPENRIG_SESSION_NAME:binding.sessionName}[key]));
    const tmux = { probeSession, getSessionEnv, createSessionForRunnerResume:vi.fn(async()=>({ok:true})),
      listPanes:vi.fn(async()=>[{id:"%79"}]) } as unknown as TmuxAdapter;
    const adapter = new CodexRuntimeAdapter({ tmux, codexHome: dir, fsOps: { readFile: p => readFileSync(p, "utf8"), writeFile: (p,s) => writeFileSync(p,s), exists: existsSync, mkdirp: p => mkdirSync(p,{recursive:true}) } });
    const options = {
      db, tmux, adapter,
      guard: { ownsRunnerRehost: () => true, maybeTarget: vi.fn(() => ({ occupant: binding.generation, pane: "%78", session: binding.sessionName })), target: () => ({ nodeId: binding.nodeId, occupant: binding.generation, pane: "%78", session: binding.sessionName }), protectionFacts: () => null },
      whoami: { resolve: vi.fn() }, activity: {}, resume: {}, store: { assertReady: vi.fn() },
      launchEnvironment: { usesNativeDuty: async () => true }, launchPath: "/fixture", snapshotRoot: path.join(dir, "private"),
      detectDaemonSupport: async () => ({ kind: "supported" }),
      configurationDigest: (session: string) => createHash("sha256").update(JSON.stringify(db.prepare("SELECT n.id,n.runtime,n.model,n.profile,n.codex_config_profile,n.cwd FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE s.session_name=? ORDER BY s.id DESC LIMIT 1").get(session))).digest("hex"),
      sessionEnv: { OPENRIG_HOST: "daemon" },
    } as unknown as Parameters<typeof createCodexRehostIntegration>[0];
    factory = createCodexRehostIntegration(options) as unknown as FactorySeams;
  });
  afterEach(() => { db?.close(); if (dir) rmSync(dir, { recursive: true, force: true }); });

  it("accepts exact missing-session and no-server absence, while arbitrary probe failures remain UNKNOWN", async () => {
    const probe = factory.deps.detachedTerminalAbsent!;
    probeSession.mockResolvedValueOnce({state:"absent"}); expect(await probe(binding)).toBe(true);
    probeSession.mockResolvedValueOnce({state:"transport_unavailable",cause:"no server running"}); expect(await probe(binding)).toBe(true);
    probeSession.mockResolvedValueOnce({state:"present"}); expect(await probe(binding)).toBe(false);
    probeSession.mockRejectedValueOnce(new Error("permission denied while probing")); await expect(probe(binding)).rejects.toThrow("permission denied");
  });

  it("uses a global detached census with no old-pane exemption and detects retained identity", async () => {
    os.run.mockResolvedValueOnce({stdout:"0\n",stderr:""});
    expect(await factory.deps.proveDetachedIdentityAbsent!(binding)).toBe(false);
    const [, args] = os.run.mock.calls[0]!;
    expect(args[0]).toBe("-c"); expect(args[1]).toContain("pid in [pane,os.getpid()]");
    expect(args[3]).toBe("0"); expect(args[4]).toBe(binding.nativeId);
  });

  it("matches detached preflight to normal preflight after replacing the retained pane", async () => {
    const detached = await factory.deps.preflightSupervisedLaunch!(binding, native, true);
    expect(detached.evidenceDigest).toMatch(/^[a-f0-9]{64}$/);
    // The prior %78 is the persisted CAS predecessor. The detached route does
    // not treat it as a live pane and does not require a tmux environment.
    expect(db.prepare("SELECT tmux_pane FROM bindings WHERE node_id='lead@xv'").get()).toEqual({tmux_pane:"%78"});
    db.prepare("UPDATE bindings SET tmux_pane='%79' WHERE node_id='lead@xv'").run();
    const target = (factory.deps.guard as unknown as { maybeTarget: ReturnType<typeof vi.fn> }).maybeTarget;
    target.mockReturnValue({occupant:binding.generation,pane:"%79",session:binding.sessionName});
    getSessionEnv.mockImplementation(async (_seat: string, key: string) => ({OPENRIG_NODE_ID:binding.nodeId,OPENRIG_SESSION_NAME:binding.sessionName}[key]));
    const resumed = await factory.deps.preflightSupervisedLaunch!(binding, native);
    expect(resumed).toEqual(detached);
  });

  it("creates the exact terminal with a retained old pane and leaves binding CAS to the service",async()=>{
    expect(await factory.deps.createDetachedTerminal!(binding)).toEqual({pane:"%79"});
    const tmux=factory.deps.tmux as unknown as {createSessionForRunnerResume:ReturnType<typeof vi.fn>};
    expect(tmux.createSessionForRunnerResume).toHaveBeenCalledWith(binding.sessionName,binding.cwd,
      expect.objectContaining({OPENRIG_NODE_ID:binding.nodeId,OPENRIG_SESSION_NAME:binding.sessionName,
        OPENRIG_OCCUPANT_GENERATION:binding.generation,OPENRIG_RUNTIME:"codex",OPENRIG_HOST:"daemon"}));
    expect(db.prepare("SELECT tmux_pane FROM bindings WHERE node_id='lead@xv'").get()).toEqual({tmux_pane:"%78"});
  });
});
