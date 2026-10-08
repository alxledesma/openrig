import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { PiDetachedResume, type PiDetachedBinding, type PiDetachedResumeOptions } from "../src/domain/pi-detached-resume.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";

const nodeId = "lead@xv";
const generation = "lead-g1";
const nativeHeaderId = "pi-session-header-019c-retained";
const custodyTables = ["nodes", "occupant_tenures", "queue_items", "coordinator_authority", "coordinator_assignments", "coordinator_stage_assignments", "coordinator_resources", "outbox_entries"];
const history = Buffer.from([
  JSON.stringify({ type: "session", version: 3, id: nativeHeaderId, cwd: "/work/lead", timestamp: "2026-10-08T10:00:00.000Z" }),
  JSON.stringify({ type: "model_change", id: "entry-1", provider: "openrouter", modelId: "stealth/space-bunny-alpha", timestamp: "2026-10-08T10:00:01.000Z" }),
  JSON.stringify({ type: "message", id: "entry-2", parentId: "entry-1", message: { role: "assistant", content: [] }, timestamp: "2026-10-08T10:00:02.000Z" }),
  "",
].join("\n"));
type TestPiOptions = PiDetachedResumeOptions & {
  runnerEntryPath?: string;
  preflightAtOriginalRunner?: (binding: PiDetachedBinding, detached: boolean, originalRunnerEntryPath: string) => Promise<{ digest: string; posture: "floor" | "full_bypass" }>;
};

describe("guarded detached Pi continuation engine", () => {
  let db: Database.Database;
  let root: string;
  let historyPath: string;
  let guard: SeatDeliveryGuard;
  let options: TestPiOptions;
  let originalRunnerEntryPath: string;
  let currentRunnerEntryPath: string;
  let currentPreflightDigest: string;
  let panePid: number | null;
  let processes: NativeProcessRow[];
  let createTerminal: ReturnType<typeof vi.fn>;
  let resume: ReturnType<typeof vi.fn>;
  let terminalAbsent: ReturnType<typeof vi.fn>;
  let proveNativeAbsent: ReturnType<typeof vi.fn>;
  let observeReplacement: ReturnType<typeof vi.fn>;
  let validateHistory: ReturnType<typeof vi.fn>;
  let preflight: ReturnType<typeof vi.fn>;
  let failBoundAbsenceOnce: boolean;

  const custody = () => JSON.stringify(Object.fromEntries(custodyTables.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()])));
  const files = () => {
    try { return readdirSync(options.snapshotRoot, { recursive: true }).map(String); }
    catch { return []; }
  };
  const beganEvidence = () => {
    const relative = files().find(file => file.endsWith("began.json"));
    if (!relative) throw new Error("No began receipt found");
    const receiptPath = path.join(options.snapshotRoot, relative);
    const bytes = readFileSync(receiptPath);
    const parsed = JSON.parse(bytes.toString()) as { attemptId: string };
    return { attemptId: parsed.attemptId, beganSha256: createHash("sha256").update(bytes).digest("hex"), directory: path.dirname(receiptPath), bytes };
  };
  const service = (overrides: Partial<TestPiOptions> = {}) => new PiDetachedResume({ ...options, ...overrides });
  const run = (instance = service(), input: Record<string, unknown> = {}) => instance.run({ nodeId, sessionName: nodeId, reason: "Continue the retained detached Pi session", operator: "operator-agent@kernel", actorGeneration: "operator-agent-g1", ...input });
  const pane = (pid: number | null) => { panePid = pid; };

  beforeEach(async () => {
    root = mkdtempSync(path.join(tmpdir(), "pi-detached-resume-"));
    historyPath = path.join(root, "retained.jsonl");
    writeFileSync(historyPath, history, { mode: 0o600 });
    historyPath = realpathSync(historyPath);
    const originalRunnerDir = path.join(root, "release-original");
    const currentRunnerDir = path.join(root, "release-current");
    mkdirSync(originalRunnerDir, { recursive: true, mode: 0o700 });
    mkdirSync(currentRunnerDir, { recursive: true, mode: 0o700 });
    originalRunnerEntryPath = path.join(originalRunnerDir, "pi-runner.js");
    currentRunnerEntryPath = path.join(currentRunnerDir, "pi-runner.js");
    writeFileSync(originalRunnerEntryPath, "identical-pi-runner-bytes\n", { mode: 0o600 });
    copyFileSync(originalRunnerEntryPath, currentRunnerEntryPath);
    db = createDb();
    seed(db);
    const fixtureAt = new Date(Date.UTC(2026, 9, 8, 12)).toISOString();
    db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host',?,?)").run(fixtureAt, fixtureAt);
    db.prepare("UPDATE nodes SET runtime='pi',cwd=?,model='openrouter/nvidia/nemotron-3-ultra-550b-a55b:free',effort=NULL,codex_config_profile='default',policy_launch_posture=NULL WHERE id=?").run("/work/lead", nodeId);
    db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='pi_session_file',resume_token=? WHERE node_id=?").run(historyPath, nodeId);
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_window,tmux_pane) VALUES ('lead-binding',? ,?, '0', '%1')").run(nodeId, nodeId);
    db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed' WHERE node_id='operator-agent@kernel'").run();
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_window,tmux_pane) VALUES ('operator-binding','operator-agent@kernel','operator-agent@kernel','0','%9')").run();
    guard = new SeatDeliveryGuard(db, name => resolveGuardTarget(db, name));
    await guard.set(nodeId, true, "operator", "detached Pi resume fixture");
    panePid = null;
    processes = [];
    createTerminal = vi.fn(async () => {
      pane(301);
      processes = [{ pid: 301, ppid: 1, command: "/bin/zsh", executableName: "zsh", startedAt: "bare-shell-301", pgid: 301, tpgid: 301 }];
      return { pane: "%2" };
    });
    resume = vi.fn(async () => ({ ok: true as const }));
    terminalAbsent = vi.fn(async () => true);
    failBoundAbsenceOnce = false;
    proveNativeAbsent = vi.fn(async (_binding: PiDetachedBinding, pid: number) => {
      const nativePresent = processes.some(row => row.command.includes(historyPath) || row.command.includes(nativeHeaderId));
      if (nativePresent) return false;
      if (pid > 0 && failBoundAbsenceOnce) { failBoundAbsenceOnce = false; return false; }
      return true;
    });
    validateHistory = vi.fn((_binding: PiDetachedBinding, bytes: Buffer) => {
      if (!bytes.equals(history)) throw new Error("history bytes did not match retained fixture");
      return { nativeIdentity: nativeHeaderId, lastEntryId: "entry-2" };
    });
    currentPreflightDigest = "a".repeat(64);
    preflight = vi.fn(async () => ({ digest: currentPreflightDigest, posture: "floor" as const }));
    observeReplacement = vi.fn(async () => ({
      supervisorLaunchId: "managed-launch-1",
      nativeFingerprint: "native-process-fingerprint-1",
      nativeIdentity: nativeHeaderId,
      sessionFile: historyPath,
      generation,
    }));
    options = {
      db,
      guard,
      snapshotRoot: path.join(root, "private-attempts"),
      runnerEntryPath: originalRunnerEntryPath,
      preflightAtOriginalRunner: async (_binding, _detached, candidate) => ({
        digest: candidate === originalRunnerEntryPath ? "a".repeat(64) : "c".repeat(64), posture: "floor",
      }),
      tmux: { getPanePid: async () => panePid },
      validateHistory,
      preflight,
      terminalAbsent,
      proveNativeAbsent,
      createTerminal,
      resume,
      observeReplacement,
      listProcesses: async () => processes,
      now: () => Date.UTC(2026, 9, 8, 12),
      sleep: async () => {},
      waitMs: 1,
      pollMs: 1,
    };
  });

  afterEach(() => {
    db?.close();
    rmSync(root, { recursive: true, force: true });
  });

  it("resumes the exact retained Pi file and generation while preserving custody and the enabled guard", async () => {
    const originalBytes = readFileSync(historyPath);
    const sessionBefore = db.prepare("SELECT * FROM sessions WHERE node_id=?").get(nodeId) as Record<string, unknown>;
    const bindingBefore = db.prepare("SELECT * FROM bindings WHERE node_id=?").get(nodeId) as Record<string, unknown>;
    const unknownId = "unknown-preserved";
    new OutboxHandler(db).record({ outboxId: unknownId, senderSession: nodeId, destinationSession: "peer@xv", body: "opaque" });
    db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(unknownId);
    const before = custody();
    const result = await run();
    expect(result, JSON.stringify(result)).toMatchObject({ ok: true, runtime: "pi", nodeId, sessionName: nodeId, generation, generationUnchanged: true, custodyPreserved: true, guardLeftEnabled: true, authorityRepaired: false, supervisorLaunchId: "managed-launch-1", nativeFingerprint: "native-process-fingerprint-1" });
    expect(custody()).toBe(before);
    expect(db.prepare("SELECT * FROM sessions WHERE node_id=?").get(nodeId)).toMatchObject({ id: sessionBefore.id, node_id: nodeId, session_name: nodeId, status: "running", resume_type: "pi_session_file", resume_token: historyPath });
    expect(db.prepare("SELECT * FROM bindings WHERE node_id=?").get(nodeId)).toMatchObject({ id: bindingBefore.id, node_id: nodeId, tmux_session: nodeId, tmux_window: "0", tmux_pane: "%2" });
    expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get(unknownId)).toEqual({ delivery_state: "indeterminate" });
    expect(readFileSync(historyPath)).toEqual(originalBytes);
    expect(readFileSync(result.backup.path)).toEqual(originalBytes);
    expect(JSON.parse(readFileSync(path.join(path.dirname(result.receiptPath), "began.json"), "utf8")).runnerEntryPath).toBe(originalRunnerEntryPath);
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).toHaveBeenCalledTimes(1);
    expect(preflight).toHaveBeenCalled();
    expect(observeReplacement).toHaveBeenCalledTimes(1);
    expect(guard.preference(nodeId)).toMatchObject({ desired: true, effective: true, pending: false });
    expect(guard.ownsRunnerRehost(nodeId)).toBe(false);
  });

  it.each(["terminal-present", "live-native", "unknown-native", "reservation", "wrong-generation", "wrong-identity", "wrong-config", "invalid-history"] as const)("refuses %s before creating a terminal or resuming", async kind => {
    if (kind === "terminal-present") terminalAbsent.mockResolvedValue(false);
    if (kind === "live-native") { processes = [{ pid: 55, ppid: 1, command: `pi --session-file ${historyPath}`, executableName: "pi", startedAt: "live", pgid: 55, tpgid: 55 }]; }
    if (kind === "unknown-native") proveNativeAbsent.mockResolvedValue(false);
    if (kind === "reservation") db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r','op',?,?,'lead-g1','native','peer@xv','peer-g1','hash','{}','{}','reserved','now','now')").run(nodeId, nodeId);
    if (kind === "wrong-generation") preflight.mockImplementationOnce(async () => {
      db.prepare("UPDATE occupant_tenures SET generation_uuid='lead-g2' WHERE node_id=?").run(nodeId);
      return { digest: "a".repeat(64), posture: "floor" as const };
    });
    if (kind === "wrong-identity") validateHistory.mockImplementation(() => { throw new Error("native header identity does not match retained session file"); });
    if (kind === "wrong-config") preflight.mockRejectedValue(new Error("managed Pi launch configuration is not established"));
    if (kind === "invalid-history") validateHistory.mockImplementation(() => { throw new Error("retained file header or chain is invalid"); });
    const result = await run(service());
    expect(result, JSON.stringify(result)).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    if (["terminal-present", "live-native", "unknown-native"].includes(kind)) {
      expect(result).toMatchObject({ code: "pi_detached_absence" });
      expect(terminalAbsent).toHaveBeenCalled();
      if (kind !== "terminal-present") expect(proveNativeAbsent).toHaveBeenCalled();
    }
    if (kind === "reservation") expect(result).toMatchObject({ code: "seat_dispatch_reserved" });
    if (kind === "wrong-generation") expect(result).toMatchObject({ code: "pi_detached_custody_changed" });
    if (["wrong-identity", "wrong-config", "invalid-history"].includes(kind)) {
      if (kind === "wrong-config") expect(preflight).toHaveBeenCalled();
      else expect(validateHistory).toHaveBeenCalled();
    }
    expect(createTerminal).not.toHaveBeenCalled();
    expect(resume).not.toHaveBeenCalled();
    expect(files().some(file => file.endsWith("began.json"))).toBe(false);
    expect(guard.preference(nodeId)).toMatchObject({ desired: true, effective: true });
  });

  it("continues only an actual terminal-created, terminal-bound pre-resume UNKNOWN attempt without creating again", async () => {
    failBoundAbsenceOnce = true;
    const first = await run();
    expect(first, JSON.stringify(first)).toMatchObject({ ok: false, code: "pi_detached_absence", effectAttempted: true, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    const original = beganEvidence();
    const unknownPath = path.join(original.directory, "unknown.json");
    const unknownBytes = readFileSync(unknownPath);
    const request = { recovery: { attemptId: original.attemptId, beganSha256: original.beganSha256 } };
    const recovered = await run(service(), request);
    expect(recovered, JSON.stringify(recovered)).toMatchObject({ ok: true, generation, generationUnchanged: true, custodyPreserved: true });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).toHaveBeenCalledTimes(1);
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
    expect(readFileSync(unknownPath)).toEqual(unknownBytes);
    expect(files().some(file => file.endsWith("resume-began.json"))).toBe(true);
    expect(files().some(file => file.endsWith("completed.json"))).toBe(true);
    expect(guard.preference(nodeId)).toMatchObject({ desired: true, effective: true });
  });

  it("reproduces the original preflight for a legacy receipt at its explicit runner path, then uses current preflight", async () => {
    createTerminal.mockRejectedValueOnce(new Error("Pi terminal creation refused before command dispatch"));
    // Create a genuine legacy receipt: the original release did not persist
    // runner provenance. Do not edit a durable receipt to manufacture it.
    const first = await run(service({ runnerEntryPath: undefined }));
    expect(first).toMatchObject({ ok: false, effectAttempted: true });
    const original = beganEvidence();
    const beganPath = path.join(original.directory, "began.json");
    const legacyBytes = readFileSync(beganPath);
    expect(JSON.parse(legacyBytes.toString()).runnerEntryPath).toBeUndefined();
    const unknownPath = path.join(original.directory, "unknown.json");
    const unknownBytes = readFileSync(unknownPath);
    const legacyHash = createHash("sha256").update(legacyBytes).digest("hex");
    options.runnerEntryPath = currentRunnerEntryPath;
    currentPreflightDigest = "b".repeat(64);
    const recovered = await run(service(), { recovery: { attemptId: original.attemptId, beganSha256: legacyHash, originalRunnerEntryPath } });
    expect(recovered, JSON.stringify(recovered)).toMatchObject({ ok: true, generation, custodyPreserved: true });
    expect(options.preflightAtOriginalRunner).toBeDefined();
    expect(preflight).toHaveBeenCalled();
    expect(resume).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ digest: currentPreflightDigest, posture: "floor" }));
    expect(readFileSync(beganPath)).toEqual(legacyBytes);
    expect(readFileSync(unknownPath)).toEqual(unknownBytes);
    expect(createTerminal).toHaveBeenCalledTimes(2);
    const repeat = await run(service(), { recovery: { attemptId: original.attemptId, beganSha256: legacyHash, originalRunnerEntryPath } });
    expect(repeat).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(2);
    expect(resume).toHaveBeenCalledTimes(1);
  });

  it.each(["changed-original-hash", "changed-configuration", "contradictory-path", "missing-provider"] as const)("holds relocated runner recovery for %s without changing the original receipt", async kind => {
    createTerminal.mockRejectedValueOnce(new Error("Pi terminal creation refused before command dispatch"));
    const first = await run();
    expect(first).toMatchObject({ ok: false, effectAttempted: true });
    const original = beganEvidence();
    const beganPath = path.join(original.directory, "began.json");
    const beganBytes = readFileSync(beganPath);
    const unknownPath = path.join(original.directory, "unknown.json");
    const unknownBytes = readFileSync(unknownPath);
    options.runnerEntryPath = currentRunnerEntryPath;
    currentPreflightDigest = "b".repeat(64);
    if (kind === "changed-original-hash") {
      options.preflightAtOriginalRunner = async () => ({ digest: "d".repeat(64), posture: "floor" });
    }
    if (kind === "changed-configuration") {
      options.preflightAtOriginalRunner = async () => ({ digest: "a".repeat(64), posture: "full_bypass" });
    }
    if (kind === "missing-provider") options.preflightAtOriginalRunner = undefined;
    const recovery = {
      attemptId: original.attemptId,
      beganSha256: original.beganSha256,
      ...(kind === "contradictory-path" ? { originalRunnerEntryPath: currentRunnerEntryPath } : {}),
    };
    const result = await run(service(), { recovery });
    expect(result).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(readFileSync(beganPath)).toEqual(beganBytes);
    expect(readFileSync(unknownPath)).toEqual(unknownBytes);
  });

  it("continues an original pre-terminal UNKNOWN after exact absence proof, preserving the original receipts and custody", async () => {
    createTerminal.mockRejectedValueOnce(new Error("Pi runtime rejected before issuing a tmux create command"));
    const originalHistory = readFileSync(historyPath);
    const first = await run();
    expect(first, JSON.stringify(first)).toMatchObject({ ok: false, effectAttempted: true, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(db.prepare("SELECT status FROM sessions WHERE node_id=?").get(nodeId)).toEqual({ status: "detached" });
    expect(db.prepare("SELECT tmux_pane FROM bindings WHERE node_id=?").get(nodeId)).toEqual({ tmux_pane: "%1" });
    const original = beganEvidence();
    const unknownPath = path.join(original.directory, "unknown.json");
    const originalUnknown = readFileSync(unknownPath);
    const originalCustody = custody();
    expect(files().some(file => file.endsWith("terminal-created.json"))).toBe(false);
    expect(files().some(file => file.endsWith("terminal-bound.json"))).toBe(false);

    const terminalAbsenceChecks = terminalAbsent.mock.calls.length;
    const nativeAbsenceChecks = proveNativeAbsent.mock.calls.length;
    const result = await run(service(), { recovery: { attemptId: original.attemptId, beganSha256: original.beganSha256 } });
    expect(result, JSON.stringify(result)).toMatchObject({ ok: true, generation, generationUnchanged: true, custodyPreserved: true });
    expect(createTerminal).toHaveBeenCalledTimes(2);
    expect(resume).toHaveBeenCalledTimes(1);
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
    expect(readFileSync(unknownPath)).toEqual(originalUnknown);
    expect(readFileSync(historyPath)).toEqual(originalHistory);
    expect(readFileSync(result.backup.path)).toEqual(originalHistory);
    expect(custody()).toBe(originalCustody);
    expect(files().some(file => file.endsWith("terminal-recovery-began.json"))).toBe(true);
    expect(files().some(file => file.endsWith("terminal-bound.json"))).toBe(true);
    expect(terminalAbsent.mock.calls.length - terminalAbsenceChecks).toBeGreaterThanOrEqual(3);
    expect(proveNativeAbsent.mock.calls.length - nativeAbsenceChecks).toBeGreaterThanOrEqual(3);
  });

  it.each(["terminal-present", "native-present"] as const)("refuses pre-terminal continuation when %s is not absent", async kind => {
    createTerminal.mockRejectedValueOnce(new Error("Pi runtime rejected before issuing a tmux create command"));
    const first = await run();
    expect(first).toMatchObject({ ok: false, effectAttempted: true });
    const original = beganEvidence();
    if (kind === "terminal-present") terminalAbsent.mockResolvedValue(false);
    else proveNativeAbsent.mockResolvedValue(false);

    const result = await run(service(), { recovery: { attemptId: original.attemptId, beganSha256: original.beganSha256 } });
    expect(result).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
    expect(files().some(file => file.endsWith("terminal-recovery-began.json"))).toBe(false);
  });

  it.each(["history", "custody"] as const)("refuses pre-terminal continuation after original %s drift", async kind => {
    createTerminal.mockRejectedValueOnce(new Error("Pi runtime rejected before issuing a tmux create command"));
    const first = await run();
    expect(first).toMatchObject({ ok: false, effectAttempted: true });
    const original = beganEvidence();
    const originalUnknown = readFileSync(path.join(original.directory, "unknown.json"));
    if (kind === "history") writeFileSync(historyPath, Buffer.concat([history, Buffer.from("{}\n")]));
    else db.prepare("UPDATE nodes SET cwd='/changed/custody' WHERE id=?").run(nodeId);

    const result = await run(service(), { recovery: { attemptId: original.attemptId, beganSha256: original.beganSha256 } });
    expect(result).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
    expect(readFileSync(path.join(original.directory, "unknown.json"))).toEqual(originalUnknown);
    expect(files().some(file => file.endsWith("terminal-recovery-began.json"))).toBe(false);
  });

  it.each(["missing-attempt", "bad-began-digest"] as const)("refuses %s pre-terminal recovery evidence without creating a terminal", async kind => {
    createTerminal.mockRejectedValueOnce(new Error("Pi runtime rejected before issuing a tmux create command"));
    const first = await run();
    expect(first).toMatchObject({ ok: false, effectAttempted: true });
    const original = beganEvidence();
    const recovery = kind === "missing-attempt"
      ? { attemptId: "b0067c35-2a34-42c6-a443-54f60370c4fc", beganSha256: "a".repeat(64) }
      : { attemptId: original.attemptId, beganSha256: "0".repeat(64) };
    const result = await run(service(), { recovery });
    expect(result).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
  });

  it("refuses a genuine terminal-created-only partial attempt on explicit recovery", async () => {
    db.exec(`CREATE TRIGGER reject_pi_terminal_binding BEFORE UPDATE OF tmux_pane ON bindings WHEN OLD.node_id='${nodeId}' BEGIN SELECT RAISE(ABORT,'fixture binding CAS interruption'); END`);
    const first = await run();
    expect(first).toMatchObject({ ok: false, effectAttempted: true, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(files().some(file => file.endsWith("terminal-created.json"))).toBe(true);
    expect(files().some(file => file.endsWith("terminal-bound.json"))).toBe(false);
    const original = beganEvidence();
    const result = await run(service(), { recovery: { attemptId: original.attemptId, beganSha256: original.beganSha256 } });
    expect(result).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
  });

  it("fences a repeated uncertain pre-terminal recovery create while retaining the first UNKNOWN", async () => {
    createTerminal.mockRejectedValueOnce(new Error("original Pi pre-terminal refusal"));
    const first = await run();
    expect(first).toMatchObject({ ok: false, effectAttempted: true });
    const original = beganEvidence();
    const unknownPath = path.join(original.directory, "unknown.json");
    const originalUnknown = readFileSync(unknownPath);
    createTerminal.mockRejectedValueOnce(new Error("recovery terminal creation outcome uncertain"));
    const recovery = { recovery: { attemptId: original.attemptId, beganSha256: original.beganSha256 } };
    const second = await run(service(), recovery);
    expect(second).toMatchObject({ ok: false, effectAttempted: true, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(2);
    expect(files().some(file => file.endsWith("terminal-recovery-began.json"))).toBe(true);
    const third = await run(service(), recovery);
    expect(third).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(2);
    expect(readFileSync(path.join(original.directory, "began.json"))).toEqual(original.bytes);
    expect(readFileSync(unknownPath)).toEqual(originalUnknown);
    expect(resume).not.toHaveBeenCalled();
  });

  it.each(["pane", "status"] as const)("holds external %s drift after binding and before native resume", async kind => {
    let mutated = false;
    proveNativeAbsent.mockImplementation(async (_binding: PiDetachedBinding, pid: number) => {
      if (pid > 0 && !mutated) {
        mutated = true;
        if (kind === "pane") db.prepare("UPDATE bindings SET tmux_pane='%9' WHERE node_id=?").run(nodeId);
        else db.prepare("UPDATE sessions SET status='detached' WHERE node_id=?").run(nodeId);
      }
      return true;
    });

    const result = await run();
    expect(result, JSON.stringify(result)).toMatchObject({
      ok: false,
      code: kind === "pane" ? "pi_detached_absence" : "pi_detached_custody_changed",
      effectAttempted: true,
      blindRetryAllowed: false,
    });
    expect(mutated).toBe(true);
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();
    expect(observeReplacement).not.toHaveBeenCalled();
    expect(files().some(file => file.endsWith("began.json"))).toBe(true);
    expect(files().some(file => file.endsWith("resume-began.json"))).toBe(false);
    expect(guard.preference(nodeId)).toMatchObject({ desired: true, effective: true });
  });

  it.each(["resume-began", "completed"] as const)("does not replay a %s attempt", async phase => {
    const original = beganEvidence;
    // First create a genuine pre-resume UNKNOWN; no receipt is forged for this control.
    failBoundAbsenceOnce = true;
    const first = await run();
    expect(first).toMatchObject({ ok: false, code: "pi_detached_absence", effectAttempted: true });
    const evidence = original();
    const request = { recovery: { attemptId: evidence.attemptId, beganSha256: evidence.beganSha256 } };
    if (phase === "resume-began") {
      // The actual engine must durably mark the resume effect before this simulated lost return.
      resume.mockImplementationOnce(async () => { throw new Error("resume return lost after effect began"); });
      await run(service(), request);
      const calls = resume.mock.calls.length;
      const replay = await run(service(), request);
      expect(replay).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
      expect(resume).toHaveBeenCalledTimes(calls);
    } else {
      const completed = await run(service(), request);
      expect(completed).toMatchObject({ ok: true });
      const calls = resume.mock.calls.length;
      const replay = await run(service(), request);
      expect(replay).toMatchObject({ ok: false, effectAttempted: false, blindRetryAllowed: false });
      expect(resume).toHaveBeenCalledTimes(calls);
    }
    expect(createTerminal).toHaveBeenCalledTimes(1);
  });

  it("reports UNKNOWN after resume when the actual replacement witness is missing", async () => {
    observeReplacement.mockResolvedValue(null);
    const result = await run();
    expect(result, JSON.stringify(result)).toMatchObject({ ok: false, effectAttempted: true, blindRetryAllowed: false });
    expect(createTerminal).toHaveBeenCalledTimes(1);
    expect(resume).toHaveBeenCalledTimes(1);
    expect(files().some(file => file.endsWith("resume-began.json"))).toBe(true);
    expect(files().some(file => file.endsWith("unknown.json"))).toBe(true);
    expect(guard.preference(nodeId)).toMatchObject({ desired: true, effective: true });
  });
});
