import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, writeFileSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { CodexRuntimeAdapter } from "../src/adapters/codex-runtime-adapter.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
import type { CodexRehostBinding, CodexRehostNativeState, CodexRehostOptions, CodexRehostPreflight } from "../src/domain/codex-rehost.js";
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

// Read the actual factory's installed callbacks; keep the real S6 binding/native
// and preflightMatches checks and the real adapter/TOML reader. Only OS sources
// are mocked. No rehost effect or provider process is started in this suite.
type FactorySeams = {
  deps: CodexRehostOptions;
  native(binding: CodexRehostBinding): Promise<CodexRehostNativeState>;
  preflightMatches(binding: CodexRehostBinding, native: CodexRehostNativeState, preflight: CodexRehostPreflight): void;
};
describe("S6 inherited native effort production preflight", () => {
  let db: Database.Database, dir: string, file: string, binding: CodexRehostBinding;
  let native: CodexRehostNativeState, factory: FactorySeams, adapter: CodexRuntimeAdapter;
  const profile = (effort = "medium") => [
    'model="gpt-6-luna"', 'model_provider="openai"', `model_reasoning_effort="${effort}"`,
    'approval_policy="never"', 'sandbox_mode="danger-full-access"',
  ].join("\n") + "\n";
  beforeEach(() => {
    os.run.mockReset(); os.native.mockReset();
    os.run.mockResolvedValue({ stdout: "fixture native probe success", stderr: "" });
    db = createDb(); seed(db);
    dir = mkdtempSync(path.join(tmpdir(), "s6-effort-integration-"));
    file = path.join(dir, "exact.config.toml"); writeFileSync(file, profile());
    db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort=NULL,codex_config_profile='exact',policy_launch_posture='full_bypass' WHERE id='lead@xv'").run(dir);
    db.prepare("UPDATE sessions SET resume_type='codex_id',resume_token='thread' WHERE node_id='lead@xv'").run();
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding','lead@xv','lead@xv','%1')").run();
    binding = { nodeId: "lead@xv", sessionId: "session", sessionName: "lead@xv", generation: "lead-g1", runtime: "codex", nativeId: "thread", cwd: dir, model: "gpt-6-luna", effort: null, codexConfigProfile: "exact" };
    native = { nodeId: binding.nodeId, sessionName: binding.sessionName, nativeId: binding.nativeId, transcriptPath: path.join(dir, "native.jsonl"), runtimeContract: {
      runtime: "codex", model: "gpt-6-luna", provider: "openai", profile: "exact", effort: "medium", permissions: { sandbox: { type: "danger-full-access" }, approval: "never" },
    } };
    os.native.mockImplementation(async () => ({ who: { identity: { nodeId: binding.nodeId } }, usage: { sessionId: "thread", transcriptPath: native.transcriptPath }, runtimeContract: native.runtimeContract }));
    const identity: Record<string, string> = { OPENRIG_NODE_ID: binding.nodeId, OPENRIG_SESSION_NAME: binding.sessionName, OPENRIG_OCCUPANT_GENERATION: binding.generation, OPENRIG_RUNTIME: "codex" };
    const tmux = { getSessionEnv: vi.fn(async (_seat: string, key: string) => identity[key]) } as unknown as TmuxAdapter;
    adapter = new CodexRuntimeAdapter({ tmux, codexHome: dir, fsOps: { readFile: p => readFileSync(p, "utf8"), writeFile: (p, s) => writeFileSync(p, s), exists: existsSync, mkdirp: p => { mkdirSync(p, { recursive: true }); } } });
    const options = {
      db, tmux, adapter,
      guard: { ownsRunnerRehost: () => true, maybeTarget: () => ({ occupant: binding.generation, pane: "%1", session: binding.sessionName }), protectionFacts: () => null },
      whoami: {}, activity: {}, resume: {}, store: { assertReady: vi.fn() },
      launchEnvironment: { usesNativeDuty: async () => true }, launchPath: "/fixture", snapshotRoot: path.join(dir, "private"),
      detectDaemonSupport: async () => ({ kind: "supported" }), configurationDigest: () => "a".repeat(64),
    } as unknown as Parameters<typeof createCodexRehostIntegration>[0];
    factory = createCodexRehostIntegration(options) as unknown as FactorySeams;
  });
  afterEach(() => { db?.close(); if (dir) rmSync(dir, { recursive: true, force: true }); });
  const preflight = async () => {
    const observed = await factory.native(binding);
    const result = await factory.deps.preflightSupervisedLaunch(binding, observed);
    factory.preflightMatches(binding, observed, result);
    return result;
  };
  it("null persisted effort inherits independently proven medium through real strict adapter", async () => {
    const call = vi.spyOn(adapter, "preflightRuntimeMigration");
    expect(await preflight()).toMatchObject({ posture: "full_bypass", effective: { effort: "medium", approval: "never", sandbox: "danger-full-access" } });
    expect(call).toHaveBeenCalledWith(expect.objectContaining({ effort: "medium" }));
    expect(binding.effort).toBeNull();
    expect(db.prepare("SELECT effort FROM nodes WHERE id='lead@xv'").get()).toEqual({ effort: null });
    expect(os.run.mock.calls.map(c => c[1])).toEqual([["--version"], ["-p", "exact", "mcp", "list"], ["login", "status"]]);
  });
  it("explicit pin mismatch refuses before adapter probes", async () => {
    binding.effort = "high";
    await expect(preflight()).rejects.toThrow("Actual native thread/model/profile/effort differs");
    expect(os.run).not.toHaveBeenCalled();
  });
  it("changed profile effort does not become an inherited fallback", async () => {
    await preflight(); writeFileSync(file, profile("high"));
    await expect(preflight()).rejects.toThrow("Codex profile model/effort mismatch");
  });
  it("changed native effort still must match the installed profile", async () => {
    await preflight(); native.runtimeContract.effort = "high";
    await expect(preflight()).rejects.toThrow("Codex profile model/effort mismatch");
  });
  it("missing native effort remains unresolved rather than guessing profile medium", async () => {
    native.runtimeContract.effort = null;
    await expect(preflight()).rejects.toThrow("Codex profile model/effort mismatch");
    expect(os.run).not.toHaveBeenCalled();
  });
  it("real adapter refuses profile byte drift during OS preflight", async () => {
    os.run.mockImplementation(async (_command, args) => {
      if (args[0] === "login") writeFileSync(file, profile() + "# changed\n");
      return { stdout: "fixture success", stderr: "" };
    });
    await expect(preflight()).rejects.toThrow("Codex profile changed during preflight");
  });
});
