import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
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

describe("guarded detached Pi continuation engine", () => {
  let db: Database.Database;
  let root: string;
  let historyPath: string;
  let guard: SeatDeliveryGuard;
  let options: PiDetachedResumeOptions;
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
  const service = (overrides: Partial<PiDetachedResumeOptions> = {}) => new PiDetachedResume({ ...options, ...overrides });
  const run = (instance = service(), input: Record<string, unknown> = {}) => instance.run({ nodeId, sessionName: nodeId, reason: "Continue the retained detached Pi session", operator: "operator-agent@kernel", actorGeneration: "operator-agent-g1", ...input });
  const pane = (pid: number | null) => { panePid = pid; };

  beforeEach(async () => {
    root = mkdtempSync(path.join(tmpdir(), "pi-detached-resume-"));
    historyPath = path.join(root, "retained.jsonl");
    writeFileSync(historyPath, history, { mode: 0o600 });
    historyPath = realpathSync(historyPath);
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
    preflight = vi.fn(async () => ({ digest: "a".repeat(64), posture: "floor" as const }));
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
