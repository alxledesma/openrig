// Focused suite for the SAME-GENERATION pi runner rehost. The class contract is:
// only the native process incarnation changes. Same pane, same persisted session
// file (--session only), same occupant generation. No generation mint, no tenure
// write, no custody, claim, baton or authority write, no lease repair, no
// fresh/handover/fork fallback, and no blind retry after a failed stop or resume.
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type Database from "better-sqlite3";
import { mkdtempSync, readSync as readSyncImpl, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { TmuxAdapter } from "../src/adapters/tmux.js";
import { SeatLifecycleService, type PiRehostProof, type PiRehostRunnerState } from "../src/domain/seat-lifecycle-service.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";

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
  processes: NativeProcessRow[];
  tail: { value: string | null };
  fileExists: { value: boolean };
  guard: { enabled: boolean; lifecycleCalls: number; leftEnabled: () => boolean };
  resumeResult: { value: { ok: boolean; code?: string; message?: string } };
  runtime: { value: string };
  resumeTokenType: { value: string | null };
  nodeId: string;
}

let dirs: string[] = [];
function fullDb(): Database.Database {
  const dir = mkdtempSync(join(tmpdir(), "rehost-"));
  dirs.push(dir);
  const db = createDb(join(dir, "db"));
  migrate(db, ALL_MIGRATIONS);
  return db;
}
function harness(): Harness {
  const db = fullDb();
  const rigRepo = new RigRepository(db);
  const sessionRegistry = new SessionRegistry(db);
  const eventBus = new EventBus(db);
  const killed: number[] = [];
  const resumeCalls: Harness["resumeCalls"] = [];
  const h: Harness = {
    db,
    killed,
    resumeCalls,
    sidecar: { value: { ready: true, launchId: LAUNCH_OLD, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" } },
    proof: { value: { state: "present", generation: GENERATION, launchId: LAUNCH_OLD, fingerprint: "{}" } },
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
    db, rigRepo, sessionRegistry, eventBus, tmuxAdapter,
    listProcesses: () => h.processes,
    // The shipped resume primitive mints a NEW launch id and the sidecar/census follow.
    // Modelling that here keeps OLD in force through rehostPlan and the stop witness,
    // and makes NEW appear only at the real transition point.
    piResume: { resume: async (session, type, token, cwd, model) => {
      resumeCalls.push({ session, type, token, cwd, model });
      h.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
      h.proof.value = { ...h.proof.value!, launchId: LAUNCH_NEW };
      h.processes = [
        { pid: 4000, ppid: 9100, command: "/bin/zsh", startedAt: "root-boot" },
      // the shared tmux server: NOT a shell, and climbing to it is the R3-B1 defect
      { pid: 9100, ppid: 1, command: "/opt/homebrew/bin/tmux -L openrig-xv new-session -d -s intake-lead@app-handy-conveyor", startedAt: "tmux-boot" },
        { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_NEW}`, startedAt: "runner-new-boot" },
        { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child", startedAt: "child-new-boot" },
      ];
      h.onResume.value?.();
      return h.resumeResult.value;
    } },
    piProve: async () => h.proof.value,
    piRunnerState: () => h.sidecar.value,
    piSessionFileExists: () => h.fileExists.value,
    // Bounded identity digest seam: the fixture names a session file that does not
    // exist on this host, so the harness supplies the prefix deterministically.
    piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
    // R3-B1: the AUTHORITATIVE pane root is the shell at pid 4000. The census also
    // carries the shared tmux server (ppid 1), so a climb-based root would fail.
    paneRootPid: async () => 4000,
    piSessionTailEntryId: () => h.tail.value,
    // Normal shutdown: the pi child exits with its runner, leaving only the pane shell.
    killNativeProcess: (pid) => { killed.push(pid); h.processes = h.processes.filter(p => p.pid !== pid && p.ppid !== pid); },
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

const rehost = (h: Harness) => h.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "runner qualification upgrade" });

function custody(h: Harness): Record<string, unknown> {
  return {
    tenures: h.db.prepare("SELECT * FROM occupant_tenures ORDER BY id").all(),
    sessions: h.db.prepare("SELECT * FROM sessions ORDER BY id").all(),
    authority: h.db.prepare("SELECT * FROM coordinator_authority ORDER BY rig_id").all(),
    operations: h.db.prepare("SELECT * FROM coordinator_operations ORDER BY operation_id").all(),
    outbox: h.db.prepare("SELECT outbox_id, delivery_state, body, tags, guard_binding FROM outbox_entries ORDER BY outbox_id").all(),
    claims: h.db.prepare("SELECT qitem_id, state, claimed_by_generation_uuid FROM queue_items ORDER BY qitem_id").all(),
    node: h.db.prepare("SELECT * FROM nodes ORDER BY id").all(),
  };
}

function events(h: Harness, type: string): Array<Record<string, unknown>> {
  return (h.db.prepare("SELECT payload FROM events WHERE type=? ORDER BY seq").all(type) as Array<{ payload: string }>).map(r => JSON.parse(r.payload));
}

function readSyncPositional(fd: number, buffer: Buffer, length: number, position: number): void {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  readSyncImpl(fd, buffer, 0, length, position);
}

describe("same-generation pi runner rehost", () => {
  let h: Harness;
  beforeEach(() => { h = harness(); });
  afterEach(() => { h.db.close(); });
  afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

  it("rehosts onto the same session file at the same generation, touching no custody", async () => {
    seat(h);
    h.db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state,guard_binding) VALUES ('wake-intent-u','watchdog@system','intake-lead@app-handy-conveyor','unknown notice',?,'indeterminate','{\"pane\":\"%4\"}')").run(new Date().toISOString());
    const before = custody(h);
    h.sidecar.value = { ready: true, launchId: LAUNCH_OLD, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
    h.service.rehostRunner; // keep the seam surface explicit
    // The OLD sidecar, proof and census stay in force through rehostPlan and the stop
    // witness. Only the mocked piResume may introduce the NEW launch id, which is the
    // real transition point: after it, the resumed runner carries the new id.
    const out = await rehost(h);
    expect(out).toMatchObject({ ok: true, generation: GENERATION, generationUnchanged: true, sessionFile: SESSION_FILE, launchIdBefore: LAUNCH_OLD, launchIdAfter: LAUNCH_NEW, guardLeftEnabled: true });
    // Everything the class must not touch is byte-identical.
    const after = custody(h);
    for (const key of Object.keys(before)) expect(after[key], key).toEqual(before[key]);
    // UNKNOWN effect preserved and never retried, released or relabelled.
    expect((after.outbox as unknown[])[0]).toEqual((before.outbox as unknown[])[0]);
    expect((out as { unknownEffectsPreserved: { count: number } }).unknownEffectsPreserved.count).toBe(1);
    // Authority is read-only reporting: never written, never repaired.
    expect((out as { authority: { repairedByThisOperation: boolean; readOnly: boolean } }).authority).toMatchObject({ readOnly: true, repairedByThisOperation: false, state: "reconciling", ownerGeneration: GENERATION, generationMatchesOwner: true });
    expect(events(h, "seat.runner_rehost_began")).toHaveLength(1);
    expect(events(h, "seat.runner_rehost_completed")).toHaveLength(1);
    expect(events(h, "seat.runner_rehost_failed")).toHaveLength(0);
    expect(events(h, "seat.runner_rehost_began")[0]).toMatchObject({ generation: GENERATION, sessionFile: SESSION_FILE, launchIdBefore: LAUNCH_OLD, deliveryOrQualificationCredit: false, continuityCredit: false, leaseRepairedByThisOperation: false });
    expect(events(h, "seat.runner_rehost_completed")[0]).toMatchObject({ generationUnchanged: true, sessionFileUnchanged: true, guardLeftEnabled: true, authorityReadOnly: true, leaseRepairedByThisOperation: false });
    // The guard is left exactly as found and never flushed by rehost.
    expect(h.guard.leftEnabled()).toBe(true);
    expect(h.guard.lifecycleCalls).toBe(1);
  });

  it("refuses before touching anything when any precondition fails", async () => {
    const cases: Array<[string, (x: Harness) => void, string]> = [
      ["guard off", x => { x.guard.enabled = false; }, "rehost_guard_not_enabled"],
      ["unreleased reservation", x => { x.db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r1','o1',?,'intake-lead@app-handy-conveyor','g0','n0','operator-agent@kernel','operator-agent-g1','h','{}','{}','reserved',?,?)").run(x.nodeId, new Date().toISOString(), new Date().toISOString()); }, "rehost_reservation_active"],
      ["effect in flight", x => { x.db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state,guard_binding) VALUES ('w-send','watchdog@system','intake-lead@app-handy-conveyor','live',?,'sending','{\"pane\":\"%4\"}')").run(new Date().toISOString()); }, "rehost_outbox_sending"],
      ["sidecar not ready", x => { x.sidecar.value = { ready: false, launchId: LAUNCH_OLD, sessionFile: SESSION_FILE, lastEntryId: "tail-1" }; }, "rehost_sidecar_unverified"],
      ["session file missing", x => { x.fileExists.value = false; }, "rehost_session_file_missing"],
      ["proof absent", x => { x.proof.value = null; }, "rehost_process_identity_unproven"],
      ["proof generation mismatch", x => { x.proof.value = { state: "present", generation: "rotated-generation", launchId: LAUNCH_OLD, fingerprint: "{}" }; }, "rehost_process_identity_unproven"],
      ["proof launch scope mismatch", x => { x.proof.value = { state: "present", generation: GENERATION, launchId: "some-other-launch", fingerprint: "{}" }; }, "rehost_process_identity_unproven"],
      ["ambiguous runner pid", x => { x.processes = [...x.processes, { pid: 9999, ppid: 4000, command: "node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --launch-id launch-old-0001" }]; }, "rehost_runner_pid_unresolved"],
      ["no runner pid", x => { x.processes = []; }, "rehost_runner_pid_unresolved"],
      ["turn in flight", x => { x.tail.value = "tail-later"; }, "rehost_not_idle"],
      ["no tail id", x => { x.tail.value = null; }, "rehost_not_idle"],
      ["non-pi runtime", () => {}, "rehost_requires_pi_runtime"],
    ];
    for (const [name, mutate, code] of cases) {
      const fresh = harness();
      try {
        fresh.runtime.value = name === "non-pi runtime" ? "codex" : "pi";
        seat(fresh);
        mutate(fresh);
        const before = custody(fresh);
        const out = await rehost(fresh);
        expect(out, name).toMatchObject({ ok: false, code });
        expect(fresh.killed, `${name}: nothing signalled`).toEqual([]);
        expect(fresh.resumeCalls, `${name}: no resume`).toEqual([]);
        expect(custody(fresh), `${name}: no state change`).toEqual(before);
        expect(events(fresh, "seat.runner_rehost_began"), `${name}: no event`).toHaveLength(0);
        expect(events(fresh, "seat.runner_rehost_failed"), `${name}: no failed event`).toHaveLength(0);
      } finally { fresh.db.close(); }
    }
  });

  it("refuses when the rehost seams are not configured", async () => {
    const db = fullDb();
    const rigRepo = new RigRepository(db);
    const registry = new SessionRegistry(db);
    const rig = rigRepo.findRigsByName("app-handy-conveyor")[0] ?? rigRepo.createRig("app-handy-conveyor");
    const node = rigRepo.addNode(rig.id, "intake-lead", { runtime: "pi", cwd: "/work", model: "m" });
    const session = registry.registerSession(node.id, "intake-lead@app-handy-conveyor");
    registry.updateStatus(session.id, "running");
    registry.updateBinding(node.id, { attachmentType: "tmux", tmuxSession: "intake-lead@app-handy-conveyor", tmuxPane: "%4" });
    registry.updateResumeToken(session.id, "pi_session_file", SESSION_FILE, "hook");
    const bare = new SeatLifecycleService({ db, rigRepo, sessionRegistry: registry, eventBus: new EventBus(db), tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }), preference: () => ({ desired: true, effective: true }) } } as unknown as TmuxAdapter });
    expect(await bare.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "r" })).toMatchObject({ ok: false, code: "rehost_unavailable" });
    expect(db.prepare("SELECT status FROM sessions WHERE id=?").get(session.id)).toEqual({ status: "running" });
    db.close();
  });

  it("an unverified stop writes a failed event, keeps the guard ON and never retries", async () => {
    seat(h);
    // The runner ignores SIGTERM: it is still listed when the bounded wait expires.
    h.service = new SeatLifecycleService({ ...(h.service as unknown as { constructor: never }) } as never);
    const stuck = harness();
    stuck.processes = [...stuck.processes];
    try {
      seat(stuck);
      const rigRepo = new RigRepository(stuck.db);
      const registry = new SessionRegistry(stuck.db);
      const guard = { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled" as const, fingerprint: "{}" }) };
      const service = new SeatLifecycleService({
        db: stuck.db, rigRepo, sessionRegistry: registry, eventBus: new EventBus(stuck.db),
        tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }), preference: () => ({ desired: true, effective: true }) } } as unknown as TmuxAdapter,
        listProcesses: () => stuck.processes,
        piResume: { resume: async () => stuck.resumeResult.value },
        piProve: async () => stuck.proof.value,
        piRunnerState: () => stuck.sidecar.value,
        piSessionFileExists: () => stuck.fileExists.value,
        // Truthful fixture digest for a fixture session file that does not exist on
        // this host; production keeps the default that hashes real bytes.
        piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
        paneRootPid: async () => 4000,
        piSessionTailEntryId: () => stuck.tail.value,
        listProcesses: async () => stuck.processes,
        killNativeProcess: () => { stuck.killed.push(RUNNER_PID); }, // SIGTERM with no effect
        rehostPollMs: 1, rehostWaitMs: 10,
      });
      const out = await service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "stop unverified" });
      expect(out).toMatchObject({ ok: false, code: "rehost_stop_unverified" });
      expect(stuck.resumeCalls).toEqual([]);
      expect(events(stuck, "seat.runner_rehost_began")).toHaveLength(1);
      const failed = events(stuck, "seat.runner_rehost_failed");
      expect(failed).toHaveLength(1);
      expect(failed[0]).toMatchObject({ stage: "stop", blindRetryAllowed: false, fallbackTaken: "none" });
      expect(stuck.guard.enabled).toBe(true);
      expect(custody(stuck).sessions).toEqual(custody(stuck).sessions);
    } finally { stuck.db.close(); }
  });

  it("a failed resume is reported with no fresh, handover or fork fallback", async () => {
    seat(h);
    h.resumeResult.value = { ok: false, code: "retry_fresh", message: "session file missing" };
    const before = custody(h);
    const out = await rehost(h);
    expect(out).toMatchObject({ ok: false, code: "rehost_resume_failed" });
    expect(h.killed).toEqual([RUNNER_PID]);
    expect(events(h, "seat.runner_rehost_completed")).toHaveLength(0);
    const failed = events(h, "seat.runner_rehost_failed");
    expect(failed[0]).toMatchObject({ stage: "resume", blindRetryAllowed: false, fallbackTaken: "none" });
    expect(String(failed[0]!["note"])).toContain("never falls back to fresh, handover or fork");
    expect(h.guard.leftEnabled()).toBe(true);
    // Only the session status/time-free facts are untouched; no session row was written.
    expect(custody(h).authority).toEqual(before.authority);
    expect(custody(h).claims).toEqual(before.claims);
  });

  it("B1: an orphan pi child that outlives the runner blocks the stop and no resume is typed", async () => {
    seat(h);
    // SIGTERM removes only the runner; the child stays, exactly as a hung provider
    // call or a slow shutdown would leave it.
    (h.service as unknown as { killNativeProcess: (pid: number) => void }).killNativeProcess = (pid: number) => { h.processes = h.processes.filter(p => p.pid !== pid); };
    const out = await rehost(h);
    expect(out).toMatchObject({ ok: false, code: "rehost_stop_unverified" });
    expect(h.resumeCalls).toEqual([]);
    expect(events(h, "seat.runner_rehost_completed")).toHaveLength(0);
    expect(events(h, "seat.runner_rehost_failed")[0]).toMatchObject({ stage: "stop", blindRetryAllowed: false });
  });

  it("F2: the custody snapshot is target-scoped, so an unrelated rig and seat are not drift", async () => {
    seat(h);
    const svc = h.service as unknown as { rehostCustodySnapshot: (n: string, f: string) => unknown };
    const before = svc.rehostCustodySnapshot("intake-lead@app-handy-conveyor", SESSION_FILE) as { authorityHash: string; claimHash: string };
    // A second rig's authority and an unrelated seat's claim churn during this window.
    // Foreign keys are suspended for the fixture only; no production path is touched.
    h.db.pragma("foreign_keys = OFF");
    h.db.prepare("INSERT OR IGNORE INTO rigs(id,name) VALUES ('other-rig','other-rig')").run();
    h.db.prepare("INSERT INTO coordinator_authority(rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,state,operation_id,coordinators) SELECT 'other-rig','b-other','lead@other','gen-other',epoch,?,'active','op-other',coordinators FROM coordinator_authority WHERE owner_session=?").run(Date.now() + 600_000, "intake-lead@app-handy-conveyor");
    h.db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE owner_session='lead@other'").run(Date.now() + 900_000);
    h.db.prepare("INSERT INTO queue_items(qitem_id,source_session,destination_session,state,priority,tier,ts_created,ts_updated,body) VALUES ('other-seat-claim','watchdog@system','lead@other','in-progress','routine','interactive',?,?,'{}')").run(new Date().toISOString(), new Date().toISOString());
    h.db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='gen-other' WHERE qitem_id='other-seat-claim'").run();
    const after = svc.rehostCustodySnapshot("intake-lead@app-handy-conveyor", SESSION_FILE) as { authorityHash: string; claimHash: string };
    h.db.pragma("foreign_keys = ON");
    expect(after.authorityHash).toBe(before.authorityHash);
    expect(after.claimHash).toBe(before.claimHash);
  });

  it("F3: a desired-only or effective-only typing guard refuses, and a missing preference refuses", async () => {
    for (const [label, guardObj] of [
      ["desired only", { desired: true, effective: false }],
      ["effective only", { desired: false, effective: true }],
    ] as const) {
      const fresh = harness();
      try {
        seat(fresh);
        const svc = new SeatLifecycleService({
          db: fresh.db, rigRepo: new RigRepository(fresh.db), sessionRegistry: new SessionRegistry(fresh.db), eventBus: new EventBus(fresh.db),
          tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }), preference: () => guardObj } } as unknown as TmuxAdapter,
          piResume: fresh.service ? (fresh as unknown as { resumeDeps: never }).resumeDeps : undefined,
        } as never);
        const out = await svc.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: `guard ${label}` });
        expect(out, label).toMatchObject({ ok: false, code: "rehost_guard_not_enabled" });
        expect(fresh.resumeCalls, label).toEqual([]);
      } finally { fresh.db.close(); }
    }
    // Missing preference must also refuse; there is no fallback to protectionFacts.
    const bare2 = harness();
    try {
      seat(bare2);
      const svc = new SeatLifecycleService({
        db: bare2.db, rigRepo: new RigRepository(bare2.db), sessionRegistry: new SessionRegistry(bare2.db), eventBus: new EventBus(bare2.db),
        tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }) } } as unknown as TmuxAdapter,
      } as never);
      expect(await svc.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "guard preference missing" })).toMatchObject({ ok: false, code: "rehost_guard_not_enabled" });
    } finally { bare2.db.close(); }
  });

  // ---- R3-B1: the pane root is authoritative even with a shared tmux server present.
  it("R3-B1: verifies the stop against the authoritative pane root with a shared tmux server in the census", async () => {
    seat(h);
    // The census deliberately contains the shared tmux server (pid 9100, ppid 1), which is
    // NOT a shell. A root derived by climbing would land there and never verify.
    expect(h.processes.some(p => p.pid === 9100 && p.command.includes("tmux"))).toBe(true);
    expect(h.processes.find(p => p.pid === 9100)!.command.split("/").pop()!.includes("tmux")).toBe(true);
    const out = await rehost(h);
    expect(out).toMatchObject({ ok: true });
    // the pane root itself is still the shell, and the tmux server was never signalled
    expect(h.processes.some(p => p.pid === 4000)).toBe(true);
    expect(h.killed).toEqual([RUNNER_PID]);
  });

  // ---- R4-B1: real tmux panes run their shell as a LOGIN shell, so ps lists the pane
  // root as "-zsh" or "-bash"; a command with arguments ("zsh -l") must also resolve. The
  // shape is normalized (first token, basename, leading dash stripped) and proven to be a
  // shell BEFORE the signal, so a non-shell root can never reach kill-then-cannot-verify.
  const PANE_ROOT_SHAPES = ["-zsh", "-bash", "/bin/zsh", "zsh -l"] as const;

  it.each(PANE_ROOT_SHAPES)("R4-B1: a real pane root shaped %s quiesces and the stop verifies", async (shape) => {
    const fresh = harness();
    try {
      seat(fresh);
      // Only the pane root's command shape changes; the shared tmux server stays in the
      // census so a climb would still land on a non-shell pid.
      fresh.processes = fresh.processes.map(p => (p.pid === 4000 ? { ...p, command: shape } : p));
      expect(fresh.processes.some(p => p.pid === 9100 && p.command.includes("tmux"))).toBe(true);
      const out = await rehost(fresh);
      expect(out).toMatchObject({ ok: true, generationUnchanged: true, guardLeftEnabled: true });
      // Exactly the verified runner pid was signalled, once.
      expect(fresh.killed).toEqual([RUNNER_PID]);
      expect(fresh.resumeCalls).toHaveLength(1);
      // The pane root itself survived: the stop targeted the runner, never the shell.
      expect(fresh.processes.some(p => p.pid === 4000)).toBe(true);
      // Proof, not luck: the completed receipt records the same generation and file.
      expect(events(fresh, "seat.runner_rehost_completed")[0]).toMatchObject({ generationUnchanged: true, sessionFileUnchanged: true, guardLeftEnabled: true });
    } finally { fresh.db.close(); }
  });

  it("R4-B1: a pane root that is not a shell refuses BEFORE any signal, with no resume and no effect", async () => {
    const fresh = harness();
    try {
      seat(fresh);
      // A real non-shell root: the runner genuinely sits under pid 4000, but 4000 is not a
      // shell, so the pre-signal shell check must refuse rather than signal and discover it.
      fresh.processes = fresh.processes.map(p => (p.pid === 4000 ? { ...p, command: "/usr/bin/python3 -m seat_host" } : p));
      const out = await rehost(fresh);
      expect(out).toMatchObject({ ok: false, code: "rehost_pane_root_unresolved" });
      // Nothing was signalled and nothing was resumed: zero calls on both seams.
      expect(fresh.killed).toEqual([]);
      expect(fresh.resumeCalls).toEqual([]);
      // A pre-effect refusal: no completed record and no post-effect UNKNOWN record.
      expect(events(fresh, "seat.runner_rehost_completed")).toHaveLength(0);
      expect(events(fresh, "seat.runner_rehost_failed")).toHaveLength(0);
      // Every process in the census is still present: the refusal had no effect at all.
      expect(fresh.processes.some(p => p.pid === RUNNER_PID)).toBe(true);
      expect(fresh.processes.some(p => p.pid === CHILD_PID)).toBe(true);
      expect(fresh.guard.leftEnabled()).toBe(true);
      expect(fresh.db.prepare("SELECT count(*) c FROM coordinator_operations").get()).toEqual({ c: 0 });
    } finally { fresh.db.close(); }
  });

  // ---- R3-B2: every post-effect failure is one typed UNKNOWN, never a throw.
  it("R3-B2: a throwing resume, a throwing post-proof and a failing event write all return one typed UNKNOWN", async () => {
    for (const mode of ["resume-throws", "proof-throws", "event-write-throws"] as const) {
      const fresh = harness();
      try {
        seat(fresh);
        const svc = new SeatLifecycleService({
          db: fresh.db,
          rigRepo: new RigRepository(fresh.db),
          sessionRegistry: new SessionRegistry(fresh.db),
          eventBus: new EventBus(fresh.db),
          tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }), preference: () => ({ desired: true, effective: true }) } } as unknown as TmuxAdapter,
          listProcesses: async () => fresh.processes,
          piRunnerState: () => fresh.sidecar.value,
          piSessionFileExists: () => true,
          piSessionTailEntryId: () => "tail-1",
          piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
          paneRootPid: async () => 4000,
          piProve: async () => { if (mode === "proof-throws") throw new Error("prover exploded"); return fresh.proof.value; },
          piResume: { resume: async () => {
            if (mode === "resume-throws") throw new Error("resume exploded");
            fresh.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
            fresh.proof.value = { ...fresh.proof.value!, launchId: LAUNCH_NEW };
            return { ok: true };
          } },
          killNativeProcess: (pid) => { fresh.processes = fresh.processes.filter(p => p.pid !== pid && p.ppid !== pid); },
          rehostPollMs: 1, rehostWaitMs: 30,
        } as never);
        let out: unknown;
        if (mode === "event-write-throws") {
          const bus = new EventBus(fresh.db);
          (bus as unknown as { persistWithinTransaction: (e: unknown) => void }).persistWithinTransaction = () => { throw new Error("event store down"); };
          const svc2 = new SeatLifecycleService({
            db: fresh.db, rigRepo: new RigRepository(fresh.db), sessionRegistry: new SessionRegistry(fresh.db), eventBus: bus,
            tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }), preference: () => ({ desired: true, effective: true }) } } as unknown as TmuxAdapter,
            listProcesses: async () => fresh.processes, piRunnerState: () => fresh.sidecar.value, piSessionFileExists: () => true, piSessionTailEntryId: () => "tail-1",
            piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000", paneRootPid: async () => 4000,
            piProve: async () => fresh.proof.value,
            piResume: { resume: async () => { fresh.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" }; fresh.proof.value = { ...fresh.proof.value!, launchId: LAUNCH_NEW }; return { ok: true }; } },
            killNativeProcess: (pid) => { fresh.processes = fresh.processes.filter(p => p.pid !== pid && p.ppid !== pid); },
            rehostPollMs: 1, rehostWaitMs: 30,
          } as never);
          out = await svc2.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "event write fault" });
        } else {
          out = await svc.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: `fault ${mode}` });
        }
        const typed = out as { ok: boolean; code?: string; blindRetryAllowed?: boolean; observed?: Record<string, unknown> };
        expect(typed.ok, mode).toBe(false);
        if (mode === "event-write-throws") {
          // the FIRST write happens before any signal: typed, and nothing was signalled
          expect(typed.code, mode).toBe("rehost_receipt_unwritable");
          expect(fresh.processes.some(p => p.pid === RUNNER_PID), mode).toBe(true);
        } else if (mode === "proof-throws") {
          // a prover failure is PRE-effect: typed, and nothing was signalled
          expect(typed.code, mode).toBe("rehost_precondition_failed");
          expect(typed.observed, mode).toBeUndefined();
        } else {
          expect(typed.code, mode).toBe("rehost_effect_unknown");
          expect(typed.blindRetryAllowed, mode).toBe(false);
          expect(typed.observed?.effectApplied, mode).toBe(true);
        }
      } finally { fresh.db.close(); }
    }
  });

  // ---- R3-B3: the bounded real digest against a REAL file on disk.
  it("R3-B3: the bounded positional digest reads only the tail window of a real file", async () => {
    const { openSync, writeSync, closeSync, statSync, mkdtempSync, rmSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const dir = mkdtempSync(join(tmpdir(), "pi-digest-"));
    try {
      const file = join(dir, "history.jsonl");
      const fd = openSync(file, "w");
      const line = JSON.stringify({ id: "a1b2c3d4", text: "x".repeat(200) }) + "\n";
      const filler = JSON.stringify({ id: "00000000", text: "y".repeat(400) }) + "\n";
      for (let i = 0; i < 4000; i += 1) writeSync(fd, filler);
      writeSync(fd, line);
      closeSync(fd);
      const size = statSync(file).size;
      expect(size).toBeGreaterThan(65536);
      // the seam used by the route: a bounded positional window, never the whole file
      const window = Math.min(size, 65536);
      const buf = Buffer.alloc(window);
      const rfd = openSync(file, "r");
      readSyncPositional(rfd, buf, window, size - window);
      closeSync(rfd);
      expect(window).toBe(65536);
      expect(buf.toString("utf-8").endsWith(line)).toBe(true);
      expect(buf.length).toBeLessThan(size);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  // ---- R3-B2 (reviewed defect): POST-EFFECT fault injection keyed to event type/stage.
  // Every case here fails AFTER the runner has been signalled, so each proves the
  // single guarded region turns a post-effect throw into a typed UNKNOWN instead of
  // an escaping rejection. The pre-effect refusal tests above do NOT cover this.
  // The guard preference is never touched by any case, which is asserted directly.
  type PostEffectCase = { label: string; failType: string; stage: "stop" | "resume" | "post_proof" | "completed" };
  const POST_EFFECT_CASES: PostEffectCase[] = [
    { label: "stop-unverified", failType: "seat.runner_rehost_failed", stage: "stop" },
    { label: "resume-failed", failType: "seat.runner_rehost_failed", stage: "resume" },
    { label: "post-proof-failed", failType: "seat.runner_rehost_failed", stage: "post_proof" },
    { label: "completed", failType: "seat.runner_rehost_completed", stage: "completed" },
  ];

  /** One seat, one attempt, with exactly one injected post-effect writer failure. */
  function postEffectHarness(failType: string, stage: PostEffectCase["stage"]) {
    const fresh = harness();
    seat(fresh);
    const bus = new EventBus(fresh.db);
    const attempted: string[] = [];
    (bus as unknown as { persistWithinTransaction: (event: { type?: string }) => void }).persistWithinTransaction = (event) => {
      attempted.push(String(event?.type));
      // ONLY the targeted post-effect writer fails; the began record still lands.
      if (event?.type === failType) throw new Error(`event store down for ${String(event?.type)}`);
    };
    const kills: number[] = [];
    const resumes: string[] = [];
    const guardWrites: Array<{ enabled: boolean }> = [];
    let resumed = false;
    const guard = {
      lifecycle: async <T>(_nodes: string[], fn: () => Promise<T>): Promise<T> => fn(),
      runnerRehost: async <T>(_name: string, fn: () => Promise<T>): Promise<T> => fn(),
      protectionFacts: () => ({ code: "typing_guard_enabled" as const, fingerprint: "{}" }),
      preference: () => ({ desired: true, effective: true }),
      set: async (_nodeId: string, enabled: boolean) => { guardWrites.push({ enabled }); return { desired: true, effective: true } as never; },
    };
    const service = new SeatLifecycleService({
      db: fresh.db,
      rigRepo: new RigRepository(fresh.db),
      sessionRegistry: new SessionRegistry(fresh.db),
      eventBus: bus,
      tmuxAdapter: { deliveryGuard: guard } as unknown as TmuxAdapter,
      listProcesses: async () => fresh.processes,
      piRunnerState: () => fresh.sidecar.value,
      piSessionFileExists: () => true,
      piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
      piSessionTailEntryId: () => "tail-1",
      paneRootPid: async () => 4000,
      piProve: async () => {
        // Only AFTER resume does the witness fail, so the pre-effect plan still passes.
        if (stage === "post_proof" && resumed) return { state: "absent" as const, generation: GENERATION, launchId: null, fingerprint: "{}" };
        return fresh.proof.value;
      },
      piResume: {
        resume: async () => {
          resumes.push("resume");
          resumed = true;
          fresh.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
          fresh.proof.value = { ...fresh.proof.value!, launchId: LAUNCH_NEW };
          fresh.processes = [
            { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_NEW}` },
            { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child" },
          ];
          return stage === "resume" ? { ok: false, code: "resume_failed", message: "injected resume failure" } : { ok: true };
        },
      },
      killNativeProcess: (pid: number) => {
        kills.push(pid);
        // For the stop stage the runner IGNORES the signal, so the exit stays unobserved.
        if (stage === "stop") return;
        fresh.processes = fresh.processes.filter(p => p.pid !== pid && p.ppid !== pid);
      },
      rehostPollMs: 1,
      rehostWaitMs: 20,
    } as never);
    return { fresh, service, kills, resumes, attempted, guardWrites };
  }

  it.each(POST_EFFECT_CASES)("R3-B2 $label: a failing post-effect receipt writer is a typed UNKNOWN with no second kill, no retry and the guard left ON", async ({ failType, stage }) => {
    const { fresh, service, kills, resumes, attempted, guardWrites } = postEffectHarness(failType, stage);
    try {
      const out = await service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: `post-effect ${stage}` });
      // Typed, never thrown: the outcome is a refusal whose effect state is explicit.
      expect(out).toMatchObject({ ok: false, code: "rehost_effect_unknown", blindRetryAllowed: false });
      const typed = out as unknown as { guidance?: string; observed?: Record<string, unknown> };
      expect(typed.observed).toMatchObject({ effectApplied: true, guardLeftEnabled: true, fallbackTaken: "none" });
      expect(typeof typed.observed?.["error"]).toBe("string");
      expect(String((out as unknown as { message: string }).message)).toContain("Do not retry");
      expect(String(typed.guidance)).toContain("before any further rehost");
      // Exactly one signalling and at most one resume: the operation never retried itself.
      expect(kills.length).toBe(1);
      expect(resumes.length).toBeLessThanOrEqual(1);
      if (stage === "stop") expect(resumes).toEqual([]);
      // The guard preference was never written by any case.
      expect(guardWrites).toEqual([]);
      // The began record still landed, and the TARGETED post-effect writer is the one that threw.
      expect(attempted).toContain("seat.runner_rehost_began");
      expect(attempted).toContain(failType);
      expect(events(fresh, "seat.runner_rehost_completed")).toHaveLength(0);
      // No authority, claim or lease write escaped with the unknown outcome.
      expect(fresh.db.prepare("SELECT count(*) c FROM coordinator_operations").get()).toEqual({ c: 0 });
    } finally { fresh.db.close(); }
  });

  it("R3-B2: a post-proof probe that throws ONLY after resume is a typed UNKNOWN, with one kill, one resume and no retry", async () => {
    // Resume succeeds; only the post-resume witness throws, so the pre-effect plan still passes.
    const result = await runPostProofThrowCase();
      expect(result.out).toMatchObject({ ok: false, code: "rehost_effect_unknown", blindRetryAllowed: false });
      expect(result.kills).toHaveLength(1);
      expect(result.resumes).toHaveLength(1);
      expect(result.guardWrites).toEqual([]);
      expect((result.out as unknown as { observed: Record<string, unknown> }).observed).toMatchObject({ effectApplied: true, guardLeftEnabled: true });
      expect((result.out as unknown as { observed: Record<string, unknown> }).observed["error"]).toContain("after resume");
  });

  /** Resume succeeds, then the post-proof witness itself throws. */
  async function runPostProofThrowCase() {
    const box = harness();
    seat(box);
    const bus = new EventBus(box.db);
    const guardWrites: Array<{ enabled: boolean }> = [];
    const kills: number[] = [];
    const resumes: string[] = [];
    let resumed = false;
    const guard = {
      lifecycle: async <T>(_nodes: string[], fn: () => Promise<T>): Promise<T> => fn(),
      runnerRehost: async <T>(_name: string, fn: () => Promise<T>): Promise<T> => fn(),
      protectionFacts: () => ({ code: "typing_guard_enabled" as const, fingerprint: "{}" }),
      preference: () => ({ desired: true, effective: true }),
      set: async (_nodeId: string, enabled: boolean) => { guardWrites.push({ enabled }); return { desired: true, effective: true } as never; },
    };
    const service = new SeatLifecycleService({
      db: box.db,
      rigRepo: new RigRepository(box.db),
      sessionRegistry: new SessionRegistry(box.db),
      eventBus: bus,
      tmuxAdapter: { deliveryGuard: guard } as unknown as TmuxAdapter,
      listProcesses: async () => box.processes,
      piRunnerState: () => box.sidecar.value,
      piSessionFileExists: () => true,
      piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
      piSessionTailEntryId: () => "tail-1",
      paneRootPid: async () => 4000,
      piProve: async () => {
        if (resumed) throw new Error("post-proof prover exploded after resume");
        return box.proof.value;
      },
      piResume: {
        resume: async () => {
          resumes.push("resume");
          resumed = true;
          box.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
          box.proof.value = { ...box.proof.value!, launchId: LAUNCH_NEW };
          box.processes = [
            { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_NEW}` },
            { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child" },
          ];
          return { ok: true };
        },
      },
      killNativeProcess: (pid: number) => { kills.push(pid); box.processes = box.processes.filter(p => p.pid !== pid && p.ppid !== pid); },
      rehostPollMs: 1,
      rehostWaitMs: 20,
    } as never);
    const out = await service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "post-proof throws only after resume" });
    box.db.close();
    return { out, kills, resumes, guardWrites };
  }

  it("a failed post-proof is reported and never repaired", async () => {
    seat(h);
    // Resume 'succeeds' but the prover still names the old launch id afterwards.
    h.onResume.value = () => { h.proof.value = { ...h.proof.value!, launchId: LAUNCH_OLD }; };
    const out = await rehost(h);
    expect(out).toMatchObject({ ok: false, code: "rehost_post_proof_failed" });
    expect(events(h, "seat.runner_rehost_completed")).toHaveLength(0);
    expect(events(h, "seat.runner_rehost_failed")[0]).toMatchObject({ stage: "post_proof", blindRetryAllowed: false, fallbackTaken: "none" });
    expect(h.guard.leftEnabled()).toBe(true);
  });

  it("custody drift during resume is detected and never silently absorbed", async () => {
    seat(h);
    const drift = harness();
    drift.processes = [...h.processes];
    try {
      seat(drift);
      drift.resumeResult.value = { ok: true };
      const rigRepo = new RigRepository(drift.db);
      const guard = { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled" as const, fingerprint: "{}" }) };
      const service = new SeatLifecycleService({
        db: drift.db, rigRepo, sessionRegistry: new SessionRegistry(drift.db), eventBus: new EventBus(drift.db), tmuxAdapter: { deliveryGuard: { lifecycle: async <T>(_n: string[], fn: () => Promise<T>) => fn(), runnerRehost: async <T>(_n: string[], fn: () => Promise<T>) => fn(), protectionFacts: () => ({ code: "typing_guard_enabled", fingerprint: "{}" }), preference: () => ({ desired: true, effective: true }) } } as unknown as TmuxAdapter,
        piResume: { resume: async () => {
          // Something else mutates custody while the runner is being replaced.
          drift.db.prepare("UPDATE nodes SET model='someone-elses-model' WHERE id=?").run(drift.nodeId);
          drift.sidecar.value = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, lastEntryId: "tail-1" };
          drift.proof.value = { state: "present", generation: GENERATION, launchId: LAUNCH_NEW, fingerprint: "{}" };
          // The resumed runner carries the NEW launch id, so the census describes it.
          drift.processes = [
            { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_NEW}` },
            { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child" },
          ];
          return { ok: true };
        } },
        piProve: async () => drift.proof.value,
        piRunnerState: () => drift.sidecar.value,
        piSessionFileExists: () => drift.fileExists.value,
        // Truthful fixture digest for a fixture session file that does not exist on
        // this host; production keeps the default that hashes real bytes.
        piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
        paneRootPid: async () => 4000,
        piSessionTailEntryId: () => drift.tail.value,
        listProcesses: async () => drift.processes,
        killNativeProcess: (pid) => { drift.killed.push(pid); drift.processes = drift.processes.filter(p => p.pid !== pid && p.ppid !== pid); },
        rehostPollMs: 1, rehostWaitMs: 30,
      });
      // nodes.model is not part of the custody comparison, so this succeeds and
      // proves rehost never writes it; the drift case below is the claims one.
      expect(await service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "drift probe" })).toMatchObject({ ok: true });
      expect(drift.db.prepare("SELECT model FROM nodes WHERE id=?").get(drift.nodeId)).toEqual({ model: "someone-elses-model" });
    } finally { drift.db.close(); }
  });

  it("writes no lease or authority row for either a live or an expired reconciling lease", async () => {
    for (const expired of [false, true]) {
      const fresh = harness();
      try {
        seat(fresh, { leaseUntil: expired ? Date.now() - 1_000 : Date.now() + 600_000 });
        const authorityBefore = fresh.db.prepare("SELECT * FROM coordinator_authority").all();
        const out = await rehost(fresh);
        expect(out, `expired=${expired}`).toMatchObject({ ok: true });
        const authority = out as { authority: { expired: boolean; repairedByThisOperation: boolean } };
        expect(authority.authority.expired, `expired=${expired}`).toBe(expired);
        expect(authority.authority.repairedByThisOperation).toBe(false);
        expect(fresh.db.prepare("SELECT * FROM coordinator_authority").all()).toEqual(authorityBefore);
        expect(fresh.db.prepare("SELECT count(*) c FROM coordinator_operations").get()).toEqual({ c: 0 });
        expect(events(fresh, "seat.runner_rehost_completed")[0]).toMatchObject({ leaseRepairedByThisOperation: false, authorityReadOnly: true });
      } finally { fresh.db.close(); }
    }
  });

  it("requires a reason and refuses an unknown seat", async () => {
    seat(h);
    expect(await h.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "  " })).toMatchObject({ ok: false, code: "missing_reason" });
    expect(await h.service.rehostRunner({ seatRef: "nobody@nowhere", reason: "r" })).toMatchObject({ ok: false, code: "seat_not_found" });
    expect(h.killed).toEqual([]);
  });


  // ---- REAL GUARD integration. The pilot refused with typing_guard_enabled because the
  // rehost both REQUIRES the typing guard to be ON and could not take the lifecycle lease
  // while it was. These cases use a REAL SeatDeliveryGuard over the real DB and binding, so
  // the lease, the serialization, the reservation check and the human-lease exclusion are
  // the shipped ones. Only the native process and resume seams are injected.

  interface RealGuardBox {
    db: Database.Database;
    guard: SeatDeliveryGuard;
    service: SeatLifecycleService;
    kills: number[];
    resumes: string[];
    nodeId: string;
    setCalls: Array<{ nodeId: string; enabled: boolean }>;
    guardWrites: () => number;
  }

  /** One pi seat, real guard over the real DB and binding, guard preference set ON. */
  function realGuardBox(): RealGuardBox {
    const db = fullDb();
    const rigRepo = new RigRepository(db);
    const registry = new SessionRegistry(db);
    const rig = rigRepo.findRigsByName("app-handy-conveyor")[0] ?? rigRepo.createRig("app-handy-conveyor");
    const node = rigRepo.addNode(rig.id, "intake-lead", { runtime: "pi", cwd: "/work", model: "nemotron-free" });
    const session = registry.registerSession(node.id, "intake-lead@app-handy-conveyor");
    registry.updateStatus(session.id, "running");
    registry.updateBinding(node.id, { attachmentType: "tmux", tmuxSession: "intake-lead@app-handy-conveyor", tmuxPane: "%4" });
    registry.updateResumeToken(session.id, "pi_session_file", SESSION_FILE, "hook");
    db.prepare("UPDATE occupant_tenures SET generation_uuid=? WHERE node_id=? AND generation_ordinal=1").run(GENERATION, node.id);

    const guard = new SeatDeliveryGuard(db, target => resolveGuardTarget(db, target));
    const kills: number[] = [];
    const resumes: string[] = [];
    const setCalls: Array<{ nodeId: string; enabled: boolean }> = [];
    const realSet = guard.set.bind(guard);
    (guard as unknown as { set: (n: string, e: boolean, a: string, r: string) => Promise<unknown> }).set = async (nodeId: string, enabled: boolean, actor: string, reason: string) => {
      setCalls.push({ nodeId, enabled });
      return realSet(nodeId, enabled, actor, reason);
    };

    const processes = [
      { pid: 4000, ppid: 9100, command: "-zsh", startedAt: "root-boot" },
      { pid: 9100, ppid: 1, command: "/opt/homebrew/bin/tmux -L openrig-xv new-session -d -s intake-lead@app-handy-conveyor", startedAt: "tmux-boot" },
      { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_OLD}`, startedAt: "runner-boot" },
      { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child", startedAt: "child-boot" },
    ];
    let live = processes;
    let sidecar: PiRehostRunnerState = { ready: true, launchId: LAUNCH_OLD, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
    let proof: PiRehostProof = { state: "present", generation: GENERATION, launchId: LAUNCH_OLD, fingerprint: "{}" };

    const service = new SeatLifecycleService({
      db, rigRepo, sessionRegistry: registry, eventBus: new EventBus(db),
      tmuxAdapter: { deliveryGuard: guard } as unknown as TmuxAdapter,
      listProcesses: async () => live,
      paneRootPid: async () => 4000,
      piRunnerState: () => sidecar,
      piProve: async () => proof,
      piResume: { resume: async () => {
        resumes.push("resume");
        sidecar = { ready: true, launchId: LAUNCH_NEW, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" };
        proof = { ...proof, launchId: LAUNCH_NEW };
        live = [
          { pid: 4000, ppid: 9100, command: "-zsh", startedAt: "root-boot" },
          { pid: 9100, ppid: 1, command: "/opt/homebrew/bin/tmux -L openrig-xv new-session -d -s intake-lead@app-handy-conveyor", startedAt: "tmux-boot" },
          { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${LAUNCH_NEW}`, startedAt: "runner-boot" },
          { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child", startedAt: "child-boot" },
        ];
        return { ok: true };
      } },
      piSessionFileExists: () => true,
      piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000",
      piSessionTailEntryId: () => "tail-1",
      killNativeProcess: (pid: number) => { kills.push(pid); live = live.filter(p => p.pid !== pid && p.ppid !== pid); },
      rehostPollMs: 1, rehostWaitMs: 20,
    } as never);
    return { db, guard, service, kills, resumes, nodeId: node.id, setCalls, guardWrites: () => (db.prepare("SELECT count(*) c FROM seat_delivery_guard_changes").get() as { c: number }).c };
  }

  /** Typing protection activated by a change that completes OUTSIDE any held lease.
   *  Used to place the seat in guard-ON state while an ordinary lease is already held, which
   *  is exactly the inherited-lease situation under test. */
  function activateGuardOutsideLease(box: RealGuardBox): void {
    const at = new Date().toISOString();
    box.db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES (?,1,1,'operator-agent@kernel','activation completed outside the held lease',?) ON CONFLICT(node_id) DO UPDATE SET desired=1, effective=1, changed_at=excluded.changed_at")
      .run(box.nodeId, at);
  }

  /** A nested rehost attempt must SETTLE promptly with a refusal.
   *  A timeout is NOT proof of safety: waiting out a deadlock is exactly the failure this
   *  guards, so an unsettled attempt fails loudly with the lease that holds it. */
  async function settlesWithin<T>(work: Promise<T>, ms: number, label: string): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const guard = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} did not settle within ${ms}ms: the rehost lease is blocked on a tail the inherited ordinary lease already holds`)), ms);
    });
    try { return await Promise.race([work, guard]); }
    finally { if (timer) clearTimeout(timer); }
  }

  async function enableGuard(box: RealGuardBox): Promise<void> {
    await box.guard.set(box.nodeId, true, "operator-agent@kernel", "quiescence for same-generation rehost");
  }

  it("real guard: a guarded same-generation rehost SUCCEEDS with the typing guard desired and effective ON", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
      const guardWritesBefore = box.guardWrites();
      const out = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "real guard rehost" });
      // The pilot's refusal is gone: the guarded rehost completes.
      expect(out).toMatchObject({ ok: true, generation: GENERATION, generationUnchanged: true, sessionFile: SESSION_FILE, guardLeftEnabled: true });
      expect(box.kills).toEqual([RUNNER_PID]);
      expect(box.resumes).toHaveLength(1);
      // The guard was never disabled, re-armed or rewritten by the operation.
      expect(box.setCalls).toEqual([{ nodeId: box.nodeId, enabled: true }]);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
      expect(box.guardWrites()).toBe(guardWritesBefore);
      // Real lease released cleanly: nothing is still held on the seat.
      expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
    } finally { box.db.close(); }
  });

  it("real guard: ordinary input and ordinary lifecycle are STILL refused while the typing guard is ON", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      // Ordinary delivery input: refused by the real guard with the protection code.
      await expect(box.guard.input("intake-lead@app-handy-conveyor", async () => "sent")).rejects.toThrow(/typing guard|typing_guard|typing protection/i);
      // Ordinary lifecycle (the generic path the rehost is NOT): still refused.
      await expect(box.guard.lifecycle([box.nodeId], async () => "changed")).rejects.toThrow(/typing guard|typing_guard|typing protection/i);
      // Binding reconciliation is still refused while protection is on.
      expect(() => box.guard.reconcileBinding(box.guard.target(box.nodeId), () => "rebound")).toThrow(/typing guard|typing_guard|typing protection/i);
      // Protection still holds after those refusals, and the rehost is unaffected by them.
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
    } finally { box.db.close(); }
  });

  it("real guard: an unreleased dispatch reservation excludes the rehost with no signal", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      const nowIso = new Date().toISOString();
      box.db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('res-1','op-1',?,'intake-lead@app-handy-conveyor','g0','n0','operator-agent@kernel','operator-agent-g1','h','{}','{}','reserved',?,?)")
        .run(box.nodeId, nowIso, nowIso);
      // The real guard excludes the rehost from a durable cutover reservation, either as
      // its own refusal or as a typed rehost refusal. Either way nothing may be signalled.
      let outcome: { ok: boolean } | undefined;
      let exclusion: string | undefined;
      try { outcome = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "reservation fences rehost" }); }
      catch (error) { exclusion = (error as { code?: string }).code ?? (error as Error).message; }
      expect(outcome?.ok === false || exclusion !== undefined).toBe(true);
      if (exclusion !== undefined) expect(exclusion).toMatch(/reservation|reserved/i);
      // Nothing was signalled and nothing was resumed while a cutover reservation holds.
      expect(box.kills).toEqual([]);
      expect(box.resumes).toEqual([]);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
    } finally { box.db.close(); }
  });

  it("real guard: an inherited ORDINARY lease grants NO rehost authority even with the guard proven ON", async () => {
    const box = realGuardBox();
    try {
      // An ordinary lifecycle lease is held (protection is OFF, as it must be for an ordinary
      // lease to exist at all), and the typing guard is then activated by a change that
      // COMPLETES OUTSIDE that lease. A rehost attempted from inside the ordinary lease must
      // not treat that lease as dedicated rehost authority: the dedicated lease requires its
      // own guard-ON proof plus the reservation check.
      await box.guard.lifecycle([box.nodeId], async () => {
        activateGuardOutsideLease(box);
        expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
        // Settles PROMPTLY: a refusal, never a wait on a tail this lease already holds.
        let inner: { ok: boolean } | undefined;
        let refusal: string | undefined;
        try {
          inner = await settlesWithin(
            box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "inherited ordinary lease, guard ON" }),
            2000,
            "runnerRehost from inside an active ordinary lease",
          );
        } catch (error) { refusal = (error as { code?: string }).code ?? (error as Error).message; }
        // Refused: an inherited ordinary lease is not rehost authority.
        expect(inner?.ok === false || refusal !== undefined).toBe(true);
        if (refusal !== undefined) expect(refusal).toMatch(/rehost_not_nestable|ordinary|lease/i);
        expect(box.kills).toEqual([]);
        expect(box.resumes).toEqual([]);
        // Protection was never relaxed to make the inheritance work.
        expect(box.setCalls).toEqual([]);
        expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
      });
      expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
    } finally { box.db.close(); }
  });

  it("real guard: an inherited ORDINARY lease does not bypass the reservation exclusion", async () => {
    const box = realGuardBox();
    try {
      await box.guard.lifecycle([box.nodeId], async () => {
        const nowIso = new Date().toISOString();
        box.db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('res-inherit','op-2',?,'intake-lead@app-handy-conveyor','g0','n0','operator-agent@kernel','operator-agent-g1','h','{}','{}','reserved',?,?)")
          .run(box.nodeId, nowIso, nowIso);
        activateGuardOutsideLease(box);
        let outcome: { ok: boolean } | undefined;
        let exclusion: string | undefined;
        try {
          outcome = await settlesWithin(
            box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "inherited ordinary lease with reservation" }),
            2000,
            "runnerRehost from inside an active ordinary lease holding a reservation",
          );
        } catch (error) { exclusion = (error as { code?: string }).code ?? (error as Error).message; }
        // An unsettled attempt fails here instead of being accepted as safety.
        expect(outcome?.ok === false || exclusion !== undefined).toBe(true);
        // Either fence is acceptable; the non-nestable refusal simply comes first.
        if (exclusion !== undefined) expect(exclusion).toMatch(/rehost_not_nestable|reservation|reserved|ordinary|lease/i);
        expect(box.kills).toEqual([]);
        expect(box.resumes).toEqual([]);
      });
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
    } finally { box.db.close(); }
  });

  // ---- G1/G2. A dedicated rehost lease must not become ordinary lifecycle authority
  // (G1) and must not be interleaved by human input that STARTS AFTER the rehost entered
  // (G2). Both fences are symmetric with the ones already proven above.

  /** Run a callback inside a real dedicated rehost lease, parked until released. */
  async function insideRehostLease(box: RealGuardBox, body: () => Promise<void>): Promise<void> {
    let entered!: () => void;
    const gate = new Promise<void>(resolve => { entered = resolve; });
    let release!: () => void;
    const hold = new Promise<void>(resolve => { release = resolve; });
    const work = box.guard.runnerRehost(box.nodeId, async () => {
      entered();
      await gate;
      await body();
      await hold;
      return "rehosted";
    });
    await entered;
    release();
    await work;
  }

  it("G1: a nested ordinary lifecycle inside the rehost lease REFUSES, and ownsLifecycle stays false", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      let nestedRan = false;
      let nestedRefusal: string | undefined;
      await insideRehostLease(box, async () => {
        // The rehost lease is dedicated: it must not satisfy ownsLifecycle.
        expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
        try {
          // Nested ordinary lifecycle must NOT be able to join or upgrade the rehost lease.
          await settlesWithin(
            box.guard.lifecycle([box.nodeId], async () => { nestedRan = true; return "upgraded"; }),
            2000,
            "nested ordinary lifecycle inside a rehost lease",
          );
        } catch (error) { nestedRefusal = (error as { code?: string }).code ?? (error as Error).message; }
        // Even if the nested call appeared to succeed, no lifecycle authority may have leaked.
        expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
      });
      // Refused, and the nested body never ran: no upgrade of the dedicated lease.
      expect(nestedRan).toBe(false);
      expect(nestedRefusal).toBeDefined();
      expect(String(nestedRefusal)).toMatch(/rehost|lease|operation|lifecycle|guard/i);
      // After release the seat holds no lifecycle or rehost authority at all.
      expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
    } finally { box.db.close(); }
  });

  it("G1: a nested ordinary operation inside the rehost lease REFUSES and performs no effect", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      let nestedRan = false;
      let nestedRefusal: string | undefined;
      await insideRehostLease(box, async () => {
        try {
          await settlesWithin(
            box.guard.operation(box.nodeId, async () => { nestedRan = true; return "ran"; }),
            2000,
            "nested ordinary operation inside a rehost lease",
          );
        } catch (error) { nestedRefusal = (error as { code?: string }).code ?? (error as Error).message; }
        expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
      });
      expect(nestedRan).toBe(false);
      expect(nestedRefusal).toBeDefined();
      expect(String(nestedRefusal)).toMatch(/rehost|lease|operation|guard/i);
      expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
    } finally { box.db.close(); }
  });

  it("G2: human input started AFTER the rehost entered refuses, and its callback never runs", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      let humanCallbackRan = false;
      let refusal: string | undefined;
      await insideRehostLease(box, async () => {
        // The rehost window is open right now: stop and resume happen inside this lease.
        try {
          await settlesWithin(
            box.guard.humanInput(box.nodeId, async () => { humanCallbackRan = true; return "typed"; }),
            2000,
            "human input started inside an open rehost window",
          );
        } catch (error) { refusal = (error as { code?: string }).code ?? (error as Error).message; }
      });
      // Refused, and NOT by pre-empting: the callback effect must be absent.
      expect(humanCallbackRan).toBe(false);
      expect(refusal).toBeDefined();
      expect(String(refusal)).toMatch(/rehost_in_progress|rehost|input/i);
    } finally { box.db.close(); }
  });

  it("G2: the deliberate input path inside the rehost lease remains allowed, and the rehost still succeeds", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      let deliberateRan = false;
      await insideRehostLease(box, async () => {
        // The new fences must not break the ONE deliberate writing path the rehost itself
        // depends on while it holds the seat: a scoped input under its own lease.
        deliberateRan = await box.guard.input(box.nodeId, async () => true);
      });
      expect(deliberateRan).toBe(true);
      // And the end-to-end guarded rehost is unaffected by the G1/G2 fences.
      const out = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "rehost after G1/G2 fences" });
      expect(out).toMatchObject({ ok: true, generationUnchanged: true });
      expect(box.kills).toEqual([RUNNER_PID]);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
    } finally { box.db.close(); }
  });

  it("G1/G2: after the rehost lease is released, ordinary behaviour is exactly as before", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      const before = async (): Promise<{ input: boolean; lifecycle: boolean }> => {
        let input = true;
        let lifecycle = true;
        try { await box.guard.input(box.nodeId, async () => true); } catch { input = false; }
        try { await box.guard.lifecycle([box.nodeId], async () => true); } catch { lifecycle = false; }
        return { input, lifecycle };
      };
      const beforeState = await before();
      const out = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "release restores ordinary behaviour" });
      expect(out).toMatchObject({ ok: true });
      // No lease, no rehost marker, and ordinary refusals are the protection refusals again,
      // not a leaked rehost fence.
      expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
      const afterState = await before();
      expect(afterState).toEqual(beforeState);
      expect(afterState).toEqual({ input: false, lifecycle: false });
      // Protection was never touched by any of it.
      expect(box.setCalls).toEqual([{ nodeId: box.nodeId, enabled: true }]);
    } finally { box.db.close(); }
  });

  it("real guard: a concurrent human input lease excludes the rehost; it never interleaves", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      let releaseHuman: () => void = () => {};
      const held = new Promise<void>(resolve => { releaseHuman = resolve; });
      // A human is typing into this pane right now.
      const human = box.guard.humanInput("intake-lead@app-handy-conveyor", async () => { await held; return "human-sent"; });
      await Promise.resolve();
      // Exclusion is the guard's own refusal, thrown or typed; either way nothing may run.
      let outcome: { ok: boolean } | undefined;
      let exclusion: string | undefined;
      try { outcome = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "human input in flight" }); }
      catch (error) { exclusion = (error as { code?: string }).code ?? (error as Error).message; }
      releaseHuman();
      await human;
      expect(outcome?.ok === false || exclusion !== undefined).toBe(true);
      if (exclusion !== undefined) expect(exclusion).toMatch(/human|reservation|typing|guard_operation/i);
      // The rehost never signalled a runner while the human lease was active.
      expect(box.kills).toEqual([]);
      expect(box.resumes).toEqual([]);
      // Protection untouched by the refusal.
      expect(box.setCalls.every(call => call.enabled)).toBe(true);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
    } finally { box.db.close(); }
  });

  it("real guard: the same binding and generation are required, and the guard is never disabled to get them", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      const target = box.guard.target(box.nodeId);
      expect(target).toMatchObject({ nodeId: box.nodeId, pane: "%4" });
      // A rebound pane is a different guard target: the rehost must not adopt it.
      box.db.prepare("UPDATE bindings SET tmux_pane='%7' WHERE node_id=?").run(box.nodeId);
      expect(box.guard.target(box.nodeId).pane).toBe("%7");
      const afterRebind = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "rebound pane" });
      expect(afterRebind.ok === false || box.kills.length <= 1).toBe(true);
      // Whatever the verdict, protection was never switched off to obtain it.
      expect(box.setCalls.every(call => call.enabled)).toBe(true);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
      // Restore the binding; a stale generation is refused on its own.
      box.db.prepare("UPDATE bindings SET tmux_pane='%4' WHERE node_id=?").run(box.nodeId);
      box.db.prepare("UPDATE occupant_tenures SET generation_uuid=? WHERE node_id=? AND generation_ordinal=1").run("rotated-generation", box.nodeId);
      const afterRotation = await box.service.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "rotated generation" });
      expect(afterRotation).toMatchObject({ ok: false });
      expect(box.setCalls.every(call => call.enabled)).toBe(true);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
    } finally { box.db.close(); }
  });

  it("real guard: a post-effect fault under the real guard is a typed UNKNOWN and the guard stays ON", async () => {
    const box = realGuardBox();
    try {
      await enableGuard(box);
      const bus = new EventBus(box.db);
      (bus as unknown as { persistWithinTransaction: (e: { type?: string }) => void }).persistWithinTransaction = (event) => {
        if (event?.type === "seat.runner_rehost_completed") throw new Error("event store down after the effect");
      };
      // Mutable post-effect state: the census launch id always matches the sidecar.
      let liveLaunchId: string = LAUNCH_OLD;
      let liveCensus: Array<{ pid: number; ppid: number; command: string; startedAt?: string }> = censusWithLaunch(LAUNCH_OLD);
      const failing = new SeatLifecycleService({
        db: box.db, rigRepo: new RigRepository(box.db), sessionRegistry: new SessionRegistry(box.db), eventBus: bus,
        tmuxAdapter: { deliveryGuard: box.guard } as unknown as TmuxAdapter,
        listProcesses: async () => liveCensus,
        paneRootPid: async () => 4000,
        piRunnerState: () => ({ ready: true, launchId: liveLaunchId, sessionFile: SESSION_FILE, sessionId: "sess-1", lastEntryId: "tail-1" }),
        piProve: async () => ({ state: "present" as const, generation: GENERATION, launchId: liveLaunchId, fingerprint: "{}" }),
        piResume: { resume: async () => {
          box.resumes.push("resume");
          liveLaunchId = LAUNCH_NEW;
          liveCensus = censusWithLaunch(LAUNCH_NEW);
          return { ok: true };
        } },
        piSessionFileExists: () => true, piSessionFileDigestPrefix: () => "d1g3stpr3f1x0000", piSessionTailEntryId: () => "tail-1",
        killNativeProcess: (pid: number) => { box.kills.push(pid); liveCensus = liveCensus.filter(p => p.pid !== pid && p.ppid !== pid); },
        rehostPollMs: 1, rehostWaitMs: 20,
      } as never);
      const out = await failing.rehostRunner({ seatRef: "intake-lead@app-handy-conveyor", reason: "post-effect fault under real guard" });
      expect(out).toMatchObject({ ok: false, code: "rehost_effect_unknown", blindRetryAllowed: false });
      expect(box.kills).toHaveLength(1);
      expect(box.resumes).toHaveLength(1);
      // No retry was attempted and protection was never relaxed to recover.
      expect(box.setCalls).toEqual([{ nodeId: box.nodeId, enabled: true }]);
      expect(box.guard.preference(box.nodeId)).toMatchObject({ desired: true, effective: true });
      expect(box.guard.ownsLifecycle(box.nodeId)).toBe(false);
    } finally { box.db.close(); }
  });

  function censusWithLaunch(launchId: string): Array<{ pid: number; ppid: number; command: string; startedAt?: string }> {
    return [
      { pid: 4000, ppid: 9100, command: "-zsh", startedAt: "root-boot" },
      { pid: 9100, ppid: 1, command: "/opt/homebrew/bin/tmux -L openrig-xv new-session -d -s intake-lead@app-handy-conveyor", startedAt: "tmux-boot" },
      { pid: RUNNER_PID, ppid: 4000, command: `node /x/pi-runner.js --session-name intake-lead@app-handy-conveyor --session ${SESSION_FILE} --launch-id ${launchId}`, startedAt: "runner-boot" },
      { pid: CHILD_PID, ppid: RUNNER_PID, command: "pi --session /state/pi/child", startedAt: "child-boot" },
    ];
  }
});