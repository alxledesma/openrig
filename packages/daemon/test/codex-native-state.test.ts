import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, writeFileSync, rmSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { resolveCodexNativeState, resolveRotationNativeState } from "../src/domain/rotation-facts-resolver.js";
import type { WhoamiService } from "../src/domain/whoami-service.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
const os = vi.hoisted(() => ({ run: vi.fn() }));
vi.mock("node:child_process", async importOriginal => {
  const original = await importOriginal<typeof import("node:child_process")>();
  const { promisify } = await import("node:util");
  const execFile = vi.fn();
  Object.defineProperty(execFile, promisify.custom, { value: (...args: unknown[]) => os.run(...args) });
  return { ...original, execFile };
});
describe("exact native identity independent of token sample freshness", () => {
  let db: Database.Database, dir: string, file: string;
  let usage: { sessionId: string; transcriptPath: string; fresh: boolean; sampledAt: string };
  let identity: { nodeId: string; runtime: string; sessionName: string };
  let deps: Parameters<typeof resolveCodexNativeState>[0], inventory: string, argv: string;
  const profile = (effort = "medium") => `model="gpt-6-luna"\nmodel_provider="openai"\nmodel_reasoning_effort="${effort}"\napproval_policy="never"\nsandbox_mode="workspace-write"\n`;
  const transcript = (id = "thread", model = "gpt-6-luna") => JSON.stringify({ type: "session_meta", payload: { id, model_provider: "openai" } }) + "\n"
    + JSON.stringify({ type: "turn_context", payload: { model, effort: "medium", approval_policy: "never", sandbox_policy: { type: "workspace-write" } } }) + "\n";
  beforeEach(() => {
    os.run.mockReset(); db = createDb(); seed(db);
    dir = realpathSync(mkdtempSync(path.join(tmpdir(), "codex-native-state-"))); file = path.join(dir, "rollout-native.jsonl");
    writeFileSync(file, transcript()); writeFileSync(path.join(dir, "config.toml"), ""); writeFileSync(path.join(dir, "exact.config.toml"), profile());
    vi.stubEnv("CODEX_HOME", dir);
    db.prepare("UPDATE nodes SET runtime='codex',model='gpt-6-luna',codex_config_profile='exact' WHERE id='lead@xv'").run();
    db.prepare("UPDATE sessions SET resume_type='codex_id',resume_token='thread' WHERE node_id='lead@xv'").run();
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding','lead@xv','lead@xv','%1')").run();
    identity = { nodeId: "lead@xv", runtime: "codex", sessionName: "lead@xv" };
    usage = { sessionId: "thread", transcriptPath: file, fresh: false, sampledAt: "2026-10-07T00:00:00Z" };
    inventory = "10 1 /bin/zsh\n20 10 /fixture/codex\n";
    argv = "/fixture/codex -p exact -m gpt-6-luna resume thread";
    os.run.mockImplementation(async (command, args) => ({ stdout: command === "/usr/sbin/lsof" ? `p20\nn${file}\n`
      : args.includes("lstart=") ? "Wed Oct 7 16:01:40 2026" : args[0] === "-axo" ? inventory : argv, stderr: "" }));
    deps = { db, whoami: { resolve: vi.fn(() => ({ identity, contextUsage: usage })) } as unknown as WhoamiService,
      tmux: { getPanePid: vi.fn(async () => 10) } as unknown as TmuxAdapter };
  });
  afterEach(() => { db?.close(); vi.unstubAllEnvs(); if (dir) rmSync(dir, { recursive: true, force: true }); });
  it("stale token telemetry still resolves exact current native thread/header/model/profile without changing it", async () => {
    const before = { ...usage }, result = await resolveCodexNativeState(deps, "lead@xv");
    expect(result.runtimeContract).toMatchObject({ runtime: "codex", model: "gpt-6-luna", profile: "exact", effort: "medium" });
    expect(result.usage).toBe(usage); expect(usage).toEqual(before); expect(result.usage.fresh).toBe(false);
    expect(deps.whoami.resolve).toHaveBeenCalledWith({ sessionName: "lead@xv", compact: false });
  });
  it("fresh launch holds the saved rollout open", async () => {
    argv = "/fixture/codex -p exact -m gpt-6-luna";
    await expect(resolveCodexNativeState(deps, "lead@xv")).resolves.toBeDefined();
  });
  it.each(["fresh", "resumed"])("%s launch on a different open thread holds", async kind => {
    if (kind === "fresh") argv = "/fixture/codex -p exact -m gpt-6-luna";
    const original = os.run.getMockImplementation()!;
    os.run.mockImplementation(async (command, ...args) => command === "/usr/sbin/lsof"
      ? { stdout: `p20\nn${path.join(dir, "rollout-other.jsonl")}\n`, stderr: "" } : original(command, ...args));
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("codex_native_thread_unbound");
  });
  it("legacy rotation still requires fresh capacity usage", async () => {
    await expect(resolveRotationNativeState(deps, "lead@xv")).rejects.toThrow("Current native generation/checkpoint unavailable");
    expect(os.run).not.toHaveBeenCalled(); usage.fresh = true;
    expect((await resolveRotationNativeState(deps, "lead@xv")).usage.fresh).toBe(true);
  });
  it("rejects stale telemetry from another DB native thread", async () => {
    usage.sessionId = "old-thread";
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("Current native thread differs from saved session");
    expect(os.run).not.toHaveBeenCalled();
  });
  it("rejects wrong native transcript header", async () => {
    writeFileSync(file, transcript("other-thread"));
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("Native generation evidence missing or mismatched");
  });
  it.each(["model", "profile"])("rejects actual native %s mismatch", async kind => {
    if (kind === "model") writeFileSync(file, transcript("thread", "other-model"));
    else argv = "/fixture/codex -p other -m gpt-6-luna";
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("Live launch profile or model differs from persistent successor pin");
  });
  it.each(["absent", "ambiguous"])("rejects %s native process lineage", async kind => {
    inventory = kind === "absent" ? "10 1 /bin/zsh\n20 999 /fixture/codex\n" : inventory + "21 10 /fixture/codex\n";
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("Exactly one native Codex process required");
  });
  it.each(["wrong-session", "last-thread", "new-latest-session"])("binds actual latest saved session: %s refuses", async kind => {
    if (kind === "wrong-session") identity.sessionName = "other@xv";
    if (kind === "last-thread") db.prepare("UPDATE sessions SET resume_type='codex_last' WHERE node_id='lead@xv'").run();
    if (kind === "new-latest-session") db.prepare("UPDATE sessions SET resume_token='new-thread' WHERE node_id='lead@xv'").run();
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow(kind === "new-latest-session" ? "Current native thread differs from saved session" : "Current saved Codex session unavailable");
  });
  it("preserves profile continuity refusal rather than a generic UNKNOWN", async () => {
    writeFileSync(path.join(dir, "exact.config.toml"), profile("high"));
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("Successor native profile would change provider, effort or approval policy");
  });
  it("holds a binding change during the OS read", async () => {
    os.run.mockImplementation(async (command, args) => {
      if (args[0] === "-p") db.prepare("UPDATE sessions SET resume_token='next-thread' WHERE node_id='lead@xv'").run();
      return { stdout: command === "/usr/sbin/lsof" ? `p20\nn${file}\n` : args.includes("lstart=") ? "Wed Oct 7 16:01:40 2026" : args[0] === "-axo" ? inventory : argv, stderr: "" };
    });
    await expect(resolveCodexNativeState(deps, "lead@xv")).rejects.toThrow("Current Codex binding changed during native observation");
  });
});
