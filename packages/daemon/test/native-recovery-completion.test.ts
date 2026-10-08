import { TmuxAdapter } from '../src/adapters/tmux.js';
import { ALL_MIGRATIONS } from '../src/db/all-migrations.js';
import { createDb } from '../src/db/connection.js';
import { migrate } from '../src/db/migrate.js';
import { CodexSameGenerationRehost, type CodexRehostOptions } from '../src/domain/codex-rehost.js';
import { digest } from '../src/domain/coordinator-authority-service.js';
import { EventBus } from '../src/domain/event-bus.js';
import { makeLegacyPiNativeWitness } from '../src/domain/legacy-pi-native-witness.js';
import { type NativeProcessRow } from '../src/domain/native-process-lineage.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { PiDetachedResume, type PiDetachedBinding, type PiDetachedResumeOptions } from '../src/domain/pi-detached-resume.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { RigRepository } from '../src/domain/rig-repository.js';
import { SeatDeliveryGuard, resolveGuardTarget } from '../src/domain/seat-delivery-guard.js';
import { SeatLifecycleService, type PiRehostProof, type PiRehostRunnerState } from '../src/domain/seat-lifecycle-service.js';
import { SessionRegistry } from '../src/domain/session-registry.js';
import { parseLegacyNativeWitnessRequest, parseStoppedTargetRecoveryRequest } from '../src/routes/seat.js';
import { seed, token } from './helpers/coordinator-fixture.js';
import type Database from 'better-sqlite3';
import { createHash } from 'node:crypto';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, readFileSync as readWholeFile, readSync as readSyncImpl, readdirSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path, { dirname, join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { NativeRecoveryCompletionStore, recoveryReceiptDigest, type NativeRecoveryProducerEvidence } from "../src/domain/native-recovery-completion.js";
import type { NativeRecoveryCompletion } from "../src/domain/native-recovery-continuation-contract.js";

// Root's kernel/config observer is deliberately a deterministic stub here.
// This tests producer ordering/storage, never claims real native proof.
function normalized(db:Database.Database,e:NativeRecoveryProducerEvidence):NativeRecoveryCompletion {
 const row=db.prepare("SELECT n.rig_id,s.id FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1").get(e.nodeId) as {rig_id:string;id:string};
 expect(e.rigId).toBe(row.rig_id);expect(e.sessionId).toBe(row.id);
 return {schema:"native-recovery-completion.v1",recoveryId:e.recoveryId,producer:e.producer,rigId:row.rig_id,nodeId:e.nodeId,sessionId:row.id,sessionName:e.sessionName,generation:e.generation,runtime:e.runtime,nativeIdentityHash:e.nativeIdentityHash,configurationDigest:"c".repeat(64),completedAt:123,source:e.source,incarnation:{key:recoveryReceiptDigest(e.recoveryId),native:{pid:222,startFingerprint:"a".repeat(64)},...(e.runtime==='pi'?{runtimeLaunchId:e.runtimeLaunchId??"deterministic-detached-runner"}:{}),...(e.supervisorLaunchId?{supervisorLaunchId:e.supervisorLaunchId,supervisor:{pid:221,startFingerprint:"b".repeat(64)}}:{})},custodyPreserved:true,generationUnchanged:true};
}

describe("ordinary Pi completion producer",()=>{
// Focused suite for the SAME-GENERATION pi runner rehost. The class contract is:
// only the native process incarnation changes. Same pane, same persisted session
// file (--session only), same occupant generation. No generation mint, no tenure
// write, no custody, claim, baton or authority write, no lease repair, no
// fresh/handover/fork fallback, and no blind retry after a failed stop or resume.




















const SESSION_FILE = "/state/pi/intake-lead@app-handy-conveyor/sessions/history.jsonl";
const LAUNCH_OLD = "launch-old-0001";
const LAUNCH_NEW = "launch-new-0002";
const GENERATION = "19256a00-5bce-4f22-902c-a6dcd69ea643";
const RUNNER_PID = 4242;
const CHILD_PID = 4243;

interface Harness {
  db: Database.Database;
  service: SeatLifecycleService;
  killed: number[];
  resumeCalls: Array<{ session: string; type: string | null; token: string | null; cwd: string; model?: string | null }>;
  sidecar: { value: PiRehostRunnerState | null };
  proof: { value: PiRehostProof | null };
  /** When set, piProve consumes this sequence instead of the static value, so a
   *  transient-null-then-present re-observation can be driven deterministically. */
  proofSeq: { queue: Array<PiRehostProof | null> };
  processes: NativeProcessRow[];
  tail: { value: string | null };
  fileExists: { value: boolean };
  guard: { enabled: boolean; lifecycleCalls: number; leftEnabled: () => boolean };
  resumeResult: { value: { ok: boolean; code?: string; message?: string } };
  runtime: { value: string };
  resumeTokenType: { value: string | null };
  nodeId: string;
  historyPath: { value: string | null };
  snapshotDirectory: string;
  onStop: { value: (() => void) | null };
  onResume: { value: (() => void) | null };
  processReadCount: { value: number };
  onSecondProcessRead: { value: (() => void) | null };
}

let dirs: string[] = [];
function fullDb(): Database.Database {
  const dir = mkdtempSync(join(tmpdir(), "rehost-"));
  dirs.push(dir);
  const db = createDb(join(dir, "db"));
  migrate(db, ALL_MIGRATIONS);
  return db;
}
function harness(publish?: (e:NativeRecoveryProducerEvidence)=>Promise<void>): Harness {
  const db = fullDb();
  const rigRepo = new RigRepository(db);
  const sessionRegistry = new SessionRegistry(db);
  const eventBus = new EventBus(db);
  const snapshotDirectory = mkdtempSync(join(tmpdir(), "rehost-snapshots-"));
  dirs.push(snapshotDirectory);
  const killed: number[] = [];
  const resumeCalls: Harness["resumeCalls"] = [];
  const h: Harness = {
    db,
    killed,
    resumeCalls,
    historyPath: { value: null },
    snapshotDirectory,
    onStop: { value: null },
    processReadCount: { value: 0 },
    onSecondProcessRead: { value: null },
    sidecar: { value: { ready: true, launchId: LAUNCH_OLD, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" } },
    proof: { value: { state: "present", generation: GENERATION, launchId: LAUNCH_OLD, fingerprint: "{}" } },
    proofSeq: { queue: [] as Array<PiRehostProof | null> },
    processes: [
      // The pane ROOT shell, then the runner and its pi child beneath it.
      { pid: 4000, ppid: 9100, command: "/bin/zsh", startedAt: "root-boot" },
      // the shared tmux server: NOT a shell, and climbing to it is the R3-B1 defect
      { pid: 9100, ppid: 1, command: "/opt/homebrew/bin/tmux -L openrig-xv new-session -d -s intake-lead@app-handy-conveyor", startedAt: "tmux-boot" },
      { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_OLD}`, startedAt: "runner-boot" },
      { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child", startedAt: "child-boot" },
    ],
    tail: { value: "tail-1" },
    // Fires INSIDE the mocked piResume.resume, i.e. only after rehostPlan and the stop
    // witness have already been proven against the OLD launch id.
    onResume: { value: null as null | (() => void) },
    fileExists: { value: true },
    guard: { enabled: true, lifecycleCalls: 0, leftEnabled: () => true },
    resumeResult: { value: { ok: true } },
    runtime: { value: "pi" },
    resumeTokenType: { value: "pi_session_file" },
    nodeId: "",
    service: undefined as unknown as SeatLifecycleService,
  };
  const guard = {
    lifecycle: async <T>(_nodes: string[], fn: () => Promise<T>): Promise<T> => { h.guard.lifecycleCalls++; return fn(); },
    // The service now takes the DEDICATED rehost lease; it counts as a lifecycle lease.
    runnerRehost: async <T>(_name: string, fn: () => Promise<T>): Promise<T> => { h.guard.lifecycleCalls++; return fn(); },
    protectionFacts: () => (h.guard.enabled ? { code: "typing_guard_enabled" as const, fingerprint: "{}" } : null),
    // F3: the real gate reads preference and requires BOTH flags. A pending
    // activation (desired only, or effective only) must refuse.
    preference: () => (h.guard.enabled ? { desired: true, effective: true } : { desired: false, effective: false }),
    set: async () => undefined as never,
    target: () => ({ nodeId: h.nodeId }),
    hasSession: async () => true,
    listSessions: async () => [],
    sendText: async () => ({ ok: true as const }),
    sendKeys: async () => ({ ok: true as const }),
  } as unknown as TmuxAdapter;
  h.guard.leftEnabled = () => h.guard.enabled;
  const tmuxAdapter = { deliveryGuard: guard } as unknown as TmuxAdapter;
  h.service = new SeatLifecycleService({
    db, rigRepo, sessionRegistry, eventBus, tmuxAdapter, recordNativeRecoveryCompletion:publish,
    listProcesses: () => {
      h.processReadCount.value++;
      if (h.processReadCount.value === 2) h.onSecondProcessRead.value?.();
      return h.processes;
    },
    // The shipped resume primitive mints a NEW launch id and the sidecar/census follow.
    // Modelling that here keeps OLD in force through rehostPlan and the stop witness,
    // and makes NEW appear only at the real transition point.
    piResume: { resume: async (session, type, token, cwd, model) => {
      resumeCalls.push({ session, type, token, cwd, model });
      h.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: h.historyPath.value ?? SESSION_FILE, sessionId: "sess-1", lastEntryId: h.tail.value };
      h.proof.value = { ...h.proof.value!, launchId: LAUNCH_NEW };
      h.processes = [
        { pid: 4000, ppid: 9100, command: "/bin/zsh", startedAt: "root-boot" },
      // the shared tmux server: NOT a shell, and climbing to it is the R3-B1 defect
      { pid: 9100, ppid: 1, command: "/opt/homebrew/bin/tmux -L openrig-xv new-session -d -s intake-lead@app-handy-conveyor", startedAt: "tmux-boot" },
        { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${h.historyPath.value ?? SESSION_FILE} --launch-id ${LAUNCH_NEW}`, startedAt: "runner-new-boot" },
        { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child", startedAt: "child-new-boot" },
      ];
      h.onResume.value?.();
      return h.resumeResult.value;
    } },
    piProve: async () => (h.proofSeq.queue.length ? h.proofSeq.queue.shift()! : h.proof.value),
    piRunnerState: () => h.sidecar.value,
    piSessionFileExists: () => h.fileExists.value,
    piRecoverySnapshotDirectory: snapshotDirectory,
    // Bounded identity digest seam: the fixture names a session file that does not
    // exist on this host, so the harness supplies the prefix deterministically.
    piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
    // R3-B1: the AUTHORITATIVE pane root is the shell at pid 4000. The census also
    // carries the shared tmux server (ppid 1), so a climb-based root would fail.
    paneRootPid: async () => 4000,
    piSessionTailEntryId: (path) => {
      if (h.historyPath.value && path === h.historyPath.value) {
        try {
          const lines = readWholeFile(path, "utf8").split("\n").filter(line => line.trim());
          const last = JSON.parse(lines[lines.length - 1]!) as { id?: unknown };
          return typeof last.id === "string" ? last.id : null;
        } catch { return null; }
      }
      return h.tail.value;
    },
    // Normal shutdown: the pi child exits with its runner, leaving only the pane shell.
    killNativeProcess: (pid) => { killed.push(pid); h.onStop.value?.(); h.processes = h.processes.filter(p => p.pid !== pid && p.ppid !== pid); },
    rehostPollMs: 1,
    rehostWaitMs: 30,
  });
  return h;
}
/** One live managed pi seat with a recorded same-session-file resume token. */
function seat(h: Harness, opts?: { leaseUntil?: number; authorityGeneration?: string }) {
  const rigRepo = new RigRepository(h.db);
  const registry = new SessionRegistry(h.db);
  const existing = rigRepo.findRigsByName("app-handy-conveyor")[0] ?? rigRepo.createRig("app-handy-conveyor");
  const node = rigRepo.addNode(existing.id, "intake-lead", { runtime: h.runtime.value, cwd: "/work", model: "openrouter/nvidia/nemotron-3-ultra-550b-a55b:free" });
  h.nodeId = node.id;
  const session = registry.registerSession(node.id, "intake-lead@app-handy-conveyor");
  registry.updateStatus(session.id, "running");
  registry.updateBinding(node.id, { attachmentType: "tmux", tmuxSession: "intake-lead@app-handy-conveyor", tmuxPane: "%4" });
  h.db.prepare("UPDATE nodes SET runtime=? WHERE id=?").run(h.runtime.value, node.id);
  registry.updateResumeToken(session.id, h.resumeTokenType.value, SESSION_FILE, "hook");
  h.db.prepare("UPDATE occupant_tenures SET generation_uuid=? WHERE node_id=? AND generation_ordinal=1").run(GENERATION, node.id);
  // The authority row references a real baton queue item, exactly as production does.
  const nowIso = new Date().toISOString();
  h.db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body,claimed_by_generation_uuid) VALUES ('baton-1',?,?,'coordinator@system','intake-lead@app-handy-conveyor','in-progress','{}',?)").run(nowIso, nowIso, GENERATION);
  h.db.prepare("INSERT INTO coordinator_authority(rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,operation_id,state,coordinators) VALUES (?,'baton-1','intake-lead@app-handy-conveyor',?,3,?,'prod-active-expiry','reconciling','[]')")
    .run(existing.id, opts?.authorityGeneration ?? GENERATION, opts?.leaseUntil ?? Date.now() + 600_000);
  return { rigId: existing.id as string, nodeId: node.id, sessionId: session.id, sessionName: "intake-lead@app-handy-conveyor" };
}

function prepareStoppedTargetHistory(h: Harness): { path: string; original: Buffer } {
  const dir = mkdtempSync(join(tmpdir(), "rehost-history-"));
  dirs.push(dir);
  const path = join(dir, "history.jsonl");
  const original = Buffer.from('{"id":"tail-1","type":"message"}\n');
  writeFileSync(path, original, { mode: 0o600 });
  h.historyPath.value = path;
  h.db.prepare("UPDATE sessions SET resume_token=? WHERE node_id=?").run(path, h.nodeId);
  h.sidecar.value = { ...h.sidecar.value!, sessionFile: path, lastEntryId: "stale-cursor" };
  h.processes = h.processes.map(row => ({ ...row, command: row.command.replace(SESSION_FILE, path) }));
  return { path, original };
}


afterEach(()=>{for(const d of dirs)rmSync(d,{recursive:true,force:true});dirs=[];});
it("publishes ordinary Pi only after its proved replacement completion event",async()=>{
 let h:Harness;const seen:NativeRecoveryProducerEvidence[]=[];
 h=harness(async e=>{seen.push(e);expect(h.resumeCalls).toHaveLength(1);expect(h.guard.leftEnabled()).toBe(true);const seq=Number(e.source.ref.slice(6));const row=h.db.prepare("SELECT type,payload FROM events WHERE seq=?").get(seq) as {type:string;payload:string};expect(row.type).toBe('seat.runner_rehost_completed');expect(recoveryReceiptDigest(row.payload)).toBe(e.source.digest);new NativeRecoveryCompletionStore(new EventBus(h.db)).record(normalized(h.db,e));});
 seat(h);expect(await h.service.rehostRunner({seatRef:'intake-lead@app-handy-conveyor',reason:'producer contract'})).toMatchObject({ok:true});expect(seen).toHaveLength(1);expect(seen[0]).toMatchObject({producer:'pi-runner-rehost',runtimeLaunchId:LAUNCH_NEW,generation:GENERATION});expect(new NativeRecoveryCompletionStore(new EventBus(h.db)).latest(h.nodeId,GENERATION)).not.toBeNull();h.db.close();
});
it("ordinary Pi failed resume never publishes; publication failure forbids another native effect",async()=>{
 let calls=0;const h=harness(async()=>{calls++;throw Error('store unavailable');});seat(h);
 expect(await h.service.rehostRunner({seatRef:'intake-lead@app-handy-conveyor',reason:'producer contract'})).toMatchObject({ok:false,code:'rehost_effect_unknown',blindRetryAllowed:false});expect(calls).toBe(1);expect(h.resumeCalls).toHaveLength(1);
 expect(await h.service.rehostRunner({seatRef:'intake-lead@app-handy-conveyor',reason:'no retry'})).toMatchObject({ok:false,code:'rehost_completion_unresolved'});expect(h.resumeCalls).toHaveLength(1);h.db.close();
 const failed=harness(async()=>{calls++;});seat(failed);failed.resumeResult.value={ok:false};expect(await failed.service.rehostRunner({seatRef:'intake-lead@app-handy-conveyor',reason:'failed'})).toMatchObject({ok:false});expect(calls).toBe(1);failed.db.close();
});

});













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


  it("detached Pi publishes exact immutable receipt only after final proof",async()=>{
   const seen:NativeRecoveryProducerEvidence[]=[];const store=new NativeRecoveryCompletionStore(new EventBus(db));
   options.recordNativeRecoveryCompletion=async e=>{seen.push(e);expect(resume).toHaveBeenCalledTimes(1);expect(guard.ownsRunnerRehost(nodeId)).toBe(true);expect(recoveryReceiptDigest(readFileSync(e.source.ref))).toBe(e.source.digest);store.record(normalized(db,e));};
   expect(await run()).toMatchObject({ok:true});expect(seen).toHaveLength(1);expect(seen[0]).toMatchObject({producer:'pi-detached-resume',supervisorLaunchId:'managed-launch-1',nativeFingerprint:'native-process-fingerprint-1'});expect(store.latest(nodeId,generation)).not.toBeNull();
  });
  it("detached Pi UNKNOWN never publishes; publication failure forbids native retry",async()=>{
   const publish=vi.fn(async()=>{throw Error('store unavailable');});options.recordNativeRecoveryCompletion=publish;
   expect(await run()).toMatchObject({ok:false,effectAttempted:true,blindRetryAllowed:false});expect(publish).toHaveBeenCalledTimes(1);expect(resume).toHaveBeenCalledTimes(1);
   expect(await run()).toMatchObject({ok:false,code:'pi_detached_binding',effectAttempted:false});
   const original=beganEvidence();expect(await run(service(),{recovery:{attemptId:original.attemptId,beganSha256:original.beganSha256}})).toMatchObject({ok:false,effectAttempted:false,blindRetryAllowed:false});expect(resume).toHaveBeenCalledTimes(1);
  });
  it("detached failed resume has no normalized completion",async()=>{
   const publish=vi.fn();options.recordNativeRecoveryCompletion=publish;resume.mockResolvedValue({ok:false});expect(await run()).toMatchObject({ok:false,effectAttempted:true});expect(publish).not.toHaveBeenCalled();expect(new NativeRecoveryCompletionStore(new EventBus(db)).latest(nodeId,generation)).toBeNull();
  });
});



















describe("Codex fixture scope",()=>{
const seat="lead@xv",generation="lead-g1",nativeId="native-thread-exact",now=Date.UTC(2026,9,7,23);
const input={nodeId:seat,sessionName:seat,reason:"Install supervised parent while preserving the adopted holder",operator:"operator-agent@kernel"};
const tables=['nodes','sessions','bindings','occupant_tenures','queue_items','coordinator_authority','coordinator_assignments','coordinator_stage_assignments','coordinator_resources','outbox_entries'];
describe("same-generation Codex process rehost",()=>{
 let db:Database.Database,dir:string,file:string,original:Buffer,guard:SeatDeliveryGuard,options:CodexRehostOptions,service:CodexSameGenerationRehost;
 let processes:NativeProcessRow[],incarnation:number,signal:ReturnType<typeof vi.fn>,resume:ReturnType<typeof vi.fn>,events:EventBus;
 const tree=(n:number):NativeProcessRow[]=>[
  {pid:10,ppid:1,command:'/bin/zsh',executableName:'zsh',startedAt:'root',pgid:10,tpgid:n},
  {pid:n,ppid:10,command:'/usr/bin/node codex-wrapper.js',executableName:'node',startedAt:'wrapper-'+n,pgid:n,tpgid:n},
  {pid:n+1,ppid:n,command:`/usr/bin/codex --no-daemon -p exact -m gpt-6-luna resume ${nativeId}`,executableName:'codex',startedAt:'native-'+n,pgid:n,tpgid:n},
 ];
 const snapshot=()=>JSON.stringify(Object.fromEntries(tables.map(t=>[t,db.prepare('SELECT * FROM '+t).all()])));
 const receipts=()=>readdirSync(options.snapshotRoot,{recursive:true}).map(String);
 beforeEach(async()=>{
  dir=realpathSync(mkdtempSync(path.join(tmpdir(),'codex-rehost-test-')));file=path.join(dir,'native.jsonl');
  original=Buffer.from(JSON.stringify({type:'session_meta',payload:{id:nativeId}})+'\n'+JSON.stringify({type:'turn_context',payload:{model:'gpt-6-luna'}})+'\n');writeFileSync(file,original,{mode:0o600});
  db=createDb();seed(db);events=new EventBus(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,seat);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token=? WHERE node_id=?").run(nativeId,seat);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding',?,?,'%1')").run(seat,seat);
  guard=new SeatDeliveryGuard(db,name=>resolveGuardTarget(db,name));await guard.set(seat,true,'operator','isolated rehost test');
  incarnation=20;processes=tree(incarnation);
  signal=vi.fn(()=>{expect(guard.ownsRunnerRehost(seat)).toBe(true);expect(receipts().some(p=>p.endsWith('began.json'))).toBe(true);processes=[tree(20)[0]!];});
  resume=vi.fn(async()=>{incarnation+=20;processes=tree(incarnation);return {ok:true as const};});
  options={db,guard,tmux:{getPanePid:async()=>10},snapshotRoot:path.join(dir,'private-rehost'),resume:{resume},
   nativeState:async()=>({nodeId:seat,sessionName:seat,nativeId,transcriptPath:file,runtimeContract:{runtime:'codex',model:'gpt-6-luna',provider:'openai',profile:'exact',effort:'high',permissions:{sandbox:{type:'workspace-write'},approval:'never'}}}),
   activityWitness:async()=>({seatNodeId:seat,sessionName:seat,rung:'window-sampling',sourceId:'actual-refreshed-pane',seq:1,observedAt:new Date(now).toISOString(),activity:'idle-at-prompt'}),
   preflightSupervisedLaunch:async()=>({posture:'floor',effective:{model:'gpt-6-luna',provider:'openai',effort:'high',approval:'never',sandbox:'workspace-write'},evidenceDigest:'a'.repeat(64)}),
   observeSupervisedReplacement:async()=>{expect(guard.ownsRunnerRehost(seat)).toBe(true);return {launchId:'supervised-'+incarnation,fingerprint:'independent-os-proof'};},
   listProcesses:async()=>processes,verifyProcessIdentity:async(_pid,identity)=>identity.OPENRIG_NODE_ID===seat&&identity.OPENRIG_SESSION_NAME===seat&&identity.OPENRIG_OCCUPANT_GENERATION===generation&&identity.OPENRIG_RUNTIME==='codex',signal,now:()=>now,sleep:async()=>{},waitMs:1,pollMs:1};
  service=new CodexSameGenerationRehost(options);
 });
 afterEach(()=>{db?.close();rmSync(dir,{recursive:true,force:true});});


 it("Codex publishes exact successful receipt under owned rehost guard",async()=>{
  const seen:NativeRecoveryProducerEvidence[]=[];const store=new NativeRecoveryCompletionStore(events);
  options.recordNativeRecoveryCompletion=async e=>{seen.push(e);expect(resume).toHaveBeenCalledTimes(1);expect(guard.ownsRunnerRehost(seat)).toBe(true);expect(recoveryReceiptDigest(readFileSync(e.source.ref))).toBe(e.source.digest);store.record(normalized(db,e));};
  service=new CodexSameGenerationRehost(options);expect(await service.rehost(input)).toMatchObject({ok:true});expect(seen).toHaveLength(1);expect(seen[0]).toMatchObject({producer:'codex-rehost',supervisorLaunchId:'supervised-40',generation});expect(store.latest(seat,generation)).not.toBeNull();
 });
 it("Codex publication failure preserves receipt and prohibits subsequent native retry",async()=>{
  const publish=vi.fn(async()=>{throw Error('store unavailable');});options.recordNativeRecoveryCompletion=publish;service=new CodexSameGenerationRehost(options);
  expect(await service.rehost(input)).toMatchObject({ok:false,effectAttempted:true,blindRetryAllowed:false});expect(publish).toHaveBeenCalledTimes(1);expect(receipts().some(p=>p.endsWith('completed.json'))).toBe(true);
  expect(await service.rehost(input)).toMatchObject({ok:false,code:'codex_rehost_completion_unresolved'});expect(resume).toHaveBeenCalledTimes(1);expect(signal).toHaveBeenCalledTimes(1);
 });
 it("Codex failed resume never publishes",async()=>{
  const publish=vi.fn();options.recordNativeRecoveryCompletion=publish;resume.mockResolvedValue({ok:false});service=new CodexSameGenerationRehost(options);expect(await service.rehost(input)).toMatchObject({ok:false,effectAttempted:true});expect(publish).not.toHaveBeenCalled();
 });
});

});
describe('immutable recovery event store',()=>{
 let db:Database.Database;let store:NativeRecoveryCompletionStore;let completion:NativeRecoveryCompletion;
 beforeEach(()=>{db=createDb();seed(db);store=new NativeRecoveryCompletionStore(new EventBus(db));completion=normalized(db,{producer:'codex-rehost',recoveryId:'recovery-1',rigId:'xv',sessionId:(db.prepare("SELECT id FROM sessions WHERE node_id='lead@xv' ORDER BY id DESC LIMIT 1").get() as {id:string}).id,nodeId:'lead@xv',sessionName:'lead@xv',generation:'lead-g1',runtime:'codex',nativeIdentityHash:'d'.repeat(64),source:{ref:'private-completed-receipt',digest:'e'.repeat(64)},supervisorLaunchId:'supervisor-1'});});afterEach(()=>db.close());
 it('immutable exact idempotence, scoped latest and input mutation isolation',()=>{const saved=store.record(completion);completion.source.ref='changed';expect(store.latest('lead@xv','lead-g1')).toEqual(saved);expect(store.latest('lead@xv','other')).toBeNull();expect(store.record(saved)).toEqual(saved);expect((db.prepare("SELECT count(*) n FROM events WHERE type='seat.native_recovery_completed'").get() as {n:number}).n).toBe(1);expect(()=>store.record(completion)).toThrow('Conflicting immutable');});
 it('conflicting recovery ID cannot change node/generation or kernel binding',()=>{store.record(completion);for(const patch of [{generation:'new'}, {nodeId:'peer@xv'}, {incarnation:{...completion.incarnation,native:{pid:223,startFingerprint:'f'.repeat(64)}}}])expect(()=>store.record({...completion,...patch})).toThrow('Conflicting immutable');});
 it('missing native start, fake success, unknown extra fields, mismatched runtime refuse',()=>{for(const patch of [{incarnation:{...completion.incarnation,native:{pid:222}}},{custodyPreserved:false},{runtime:'pi'},{credentials:'never retained'}])expect(()=>store.record({...completion,...patch} as NativeRecoveryCompletion)).toThrow();expect(store.latest('lead@xv','lead-g1')).toBeNull();});
 it('record participates in caller transaction/notify envelope and rollback',()=>{const bus=new EventBus(db);const s=new NativeRecoveryCompletionStore(bus);expect(()=>db.transaction(()=>{s.record(completion);throw Error('rollback');})()).toThrow('rollback');expect(s.latest('lead@xv','lead-g1')).toBeNull();bus.withNotifyEnvelope(()=>s.record(completion));expect(s.latest('lead@xv','lead-g1')).toEqual(completion);});
});
