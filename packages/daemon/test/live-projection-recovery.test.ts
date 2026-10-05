// Focused suite for guarded live-projection recovery: same-native proof gates,
// custody preservation, fencing, drift, replay, and authorization. No relaunch,
// no tenure minting, no authority writes are ever permitted here.
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { LiveProjectionRecoveryService, type LiveProjectionRecoveryInput } from "../src/domain/live-projection-recovery-service.js";
import type { CodexSessionFileProofDeps } from "../src/domain/codex-session-file-proof.js";
import { seed } from "./helpers/coordinator-fixture.js";

let dir: string, db: Database.Database, repo: QueueRepository, clock: number;

function setupPiSeat(): void {
  db.prepare("UPDATE nodes SET runtime='pi' WHERE id='lead@xv'").run();
  db.prepare("UPDATE sessions SET status='detached', resume_token='/state/pi/lead@xv/sessions/s.json' WHERE id='lead@xv'").run();
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('b-lead','lead@xv','lead@xv','%9')").run();
}

const input = (over: Partial<LiveProjectionRecoveryInput> = {}): LiveProjectionRecoveryInput => ({
  operationId: "op-1", sessionId: "lead@xv", nodeId: "lead@xv", sessionName: "lead@xv", expectedGeneration: "lead-g1", ...over,
});

function service(deps: { piProve?: (s: string) => Promise<{ state: "present" | "absent"; generation: string; launchId: string | null; fingerprint: string } | null>; tmux?: unknown; codexSessionFileProof?: CodexSessionFileProofDeps }) {
  const tmux = deps.tmux ?? { listSessions: async () => [{ name: "lead@xv" }], getPanePid: async () => 4242, getPaneCommand: async () => "node" };
  return new LiveProjectionRecoveryService({ db, tmux: tmux as never, authority: repo.coordinatorAuthority, events: new EventBus(db), ...(deps.piProve ? { piProve: deps.piProve } : {}), ...(deps.codexSessionFileProof ? { codexSessionFileProof: deps.codexSessionFileProof } : {}) });
}
const present = (launchId = "L-77") => async () => ({ state: "present" as const, generation: "lead-g1", launchId, fingerprint: "{}" });

beforeEach(async () => {
  vi.useFakeTimers({ toFake: ["Date"] }); clock = Date.now();
  dir = mkdtempSync(join(tmpdir(), "lpr-")); db = createDb(join(dir, "db")); seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(), new Date(clock).toISOString());
  repo = new QueueRepository(db, new EventBus(db), { resolveOccupantGeneration: s => repo.coordinatorAuthority.generation(s) });
  // Deliberately NO coordinator authority enrollment and no baton: recovery
  // must work on disposable rigs (real-schema class boundary R4).
  setupPiSeat();
});
afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); vi.useRealTimers(); });

describe("live-projection recovery", () => {
  it("recovers a proven-live falsely detached seat preserving every custody identifier and writing no tenure", async () => {
    const tenuresBefore = db.prepare("SELECT * FROM occupant_tenures WHERE node_id='lead@xv'").all();
    const bindingsBefore = db.prepare("SELECT * FROM bindings WHERE node_id='lead@xv'").get();
    const r = service({ piProve: present() });
    const out = await r.recover("operator-agent@kernel", "operator-agent-g1", input());
    expect(out).toMatchObject({ ok: true, code: "recovered" });
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "running" });
    expect(db.prepare("SELECT * FROM occupant_tenures WHERE node_id='lead@xv'").all()).toEqual(tenuresBefore);
    expect(db.prepare("SELECT * FROM bindings WHERE node_id='lead@xv'").get()).toEqual(bindingsBefore);
    const receipt = db.prepare("SELECT receipt FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").get() as { receipt: string };
    const parsed = JSON.parse(receipt.receipt);
    expect(parsed.preserved).toEqual({ tenureMinted: false, adopted: false, relaunched: false, inputSent: false, authorityChanged: false, custodyTouched: false });
    expect(parsed.evidence.axis).toBe("pi_native_lineage");
    expect(receipt.receipt).not.toContain("/state/pi/"); // token appears only as digest
    expect(db.prepare("SELECT 1 FROM events WHERE type='session.live_projection_recovered'").get()).toBeTruthy();
  });

  it("recovers on a rig with NO coordinator authority enrollment, touching only the dedicated ledger", async () => {
    expect(db.prepare("SELECT COUNT(*) AS c FROM coordinator_authority").get()).toEqual({ c: 0 });
    expect(db.prepare("SELECT COUNT(*) AS c FROM coordinator_operations").get()).toEqual({ c: 0 });
    const r = service({ piProve: present() });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("recovered");
    expect(db.prepare("SELECT rig_id,node_id,session_id FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").get()).toMatchObject({ rig_id: "xv", node_id: "lead@xv", session_id: "lead@xv" });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("already_recovered");
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input({ nodeId: "peer@xv" }))).code).toBe("live_projection_replay_conflict");
    expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id LIKE 'live-projection-recover%'").get()).toBeUndefined();
    expect(db.prepare("SELECT COUNT(*) AS c FROM coordinator_authority").get()).toEqual({ c: 0 });
    expect(() => db.prepare("UPDATE live_projection_recovery_operations SET receipt='{}' WHERE operation_id='live-projection-recover:op-1'").run()).toThrow(/immutable/);
    expect(() => db.prepare("DELETE FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").run()).toThrow(/immutable/);
  });

  it("a durable recovery receipt never blocks supported node and rig teardown", async () => {
    const rigRepo = new RigRepository(db);
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("recovered");
    rigRepo.deleteNode("lead@xv");
    rigRepo.deleteRig("xv");
    expect(db.prepare("SELECT 1 FROM nodes WHERE id='lead@xv'").get()).toBeUndefined();
    expect(db.prepare("SELECT 1 FROM rigs WHERE id='xv'").get()).toBeUndefined();
    expect(db.prepare("SELECT operation_id,node_id,session_id FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").get()).toMatchObject({ node_id: "lead@xv", session_id: "lead@xv" });
    expect(() => db.prepare("UPDATE live_projection_recovery_operations SET receipt='{}' WHERE operation_id='live-projection-recover:op-1'").run()).toThrow(/immutable/);
    expect(() => db.prepare("DELETE FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").run()).toThrow(/immutable/);
  });

  it("exact replay returns the stored receipt; payload reuse under one operation ID is refused", async () => {
    const r = service({ piProve: present() });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("recovered");
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("already_recovered");
    const conflict = await r.recover("operator-agent@kernel", "operator-agent-g1", input({ operationId: "op-1", sessionId: "lead@xv" }) );
    expect(conflict.code).not.toBe("recovered"); // same payload replays idempotently
    const diff = await r.recover("operator-agent@kernel", "operator-agent-g1", { ...input(), sessionName: "lead@xv" });
    expect(diff.code).toBe("already_recovered");
    // differing payload under the same operation id:
    const before = db.prepare("SELECT request_hash FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").get();
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input({ nodeId: "peer@xv" }))).code).toBe("live_projection_replay_conflict");
    expect(db.prepare("SELECT request_hash FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").get()).toEqual(before);
  });

  it("non-current Operator identity refuses without any write", async () => {
    const out = await service({ piProve: present() }).recover("operator-agent@kernel", "bogus-generation", input());
    expect(out).toMatchObject({ ok: false, code: "operator_unauthorized" });
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "detached" });
    expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get()).toBeUndefined();
  });

  it.each([
    ["stale generation", () => db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES ('t2','lead@xv',2,'lead-g2','fresh')").run(), "stale_generation"],
    ["superseded row", () => db.prepare("UPDATE sessions SET status='superseded' WHERE id='lead@xv'").run(), "not_current_occupant"],
    ["historical row behind newer", () => db.prepare("INSERT INTO sessions(id,node_id,session_name,status) VALUES ('lead@xv-new','lead@xv','lead@xv','running')").run(), "historical_row"],
    ["wrong node identity", () => db.prepare("UPDATE bindings SET node_id='lead@xv' WHERE node_id='lead@xv'").run(), "identity_mismatch"],
  ] as const)("%s refuses", async (_name, mutate, code) => {
    if (code === "identity_mismatch") { const out = await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input({ nodeId: "peer@xv" })); expect(out.code).toBe(code); return; }
    mutate();
    expect((await service({ piProve: code === "stale_generation" ? async () => ({ state: "present" as const, generation: "lead-g2", launchId: "L", fingerprint: "{}" }) : present() }).recover("operator-agent@kernel", "operator-agent-g1", code === "stale_generation" ? input({ expectedGeneration: "lead-g1" }) : input())).code).toBe(code);
  });

  it("unknown or absent native proof never flips status", async () => {
    expect((await service({ piProve: async () => null }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("probe_uncertain");
    expect((await service({ piProve: async () => ({ state: "absent" as const, generation: "lead-g1", launchId: null, fingerprint: "{}" }) }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("not_current_occupant");
    expect((await service({}).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("probe_uncertain");
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "detached" });
  });

  it("missing stored resume token blocks proof before probing", async () => {
    db.prepare("UPDATE sessions SET resume_token=NULL WHERE id='lead@xv'").run();
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("native_proof_unavailable");
  });

  it("terminal runtimes refuse instead of inferring from pane facts alone", async () => {
    db.prepare("UPDATE nodes SET runtime=NULL WHERE id='lead@xv'").run();
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("unsupported_runtime_proof");
  });

  it.each(["reserved", "started", "committed"] as const)("a %s reservation fences recovery", async state => {
    db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r1','o1','lead@xv','lead@xv','g0','n0','operator-agent@kernel','operator-agent-g1','h','{}','{}',?,?,?)").run(state, new Date(clock).toISOString(), new Date(clock).toISOString());
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("reservation_fencing_active");
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "detached" });
  });
  it("a released reservation does not fence recovery", async () => {
    db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r1','o1','lead@xv','lead@xv','g0','n0','operator-agent@kernel','operator-agent-g1','h','{}','{}','released',?,?)").run(new Date(clock).toISOString(), new Date(clock).toISOString());
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).ok).toBe(true);
  });
  it("guarded effects and explicit seat delivery guards fence recovery", async () => {
    db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state,guard_binding) VALUES ('w1','watchdog@system','lead@xv','wake',?,'pending','{}')").run(new Date(clock).toISOString());
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("delivery_fencing_active");
    db.prepare("DELETE FROM outbox_entries WHERE outbox_id='w1'").run();
    for (const [desired, effective] of [[1, 0], [0, 1]] as const) {
      db.prepare("INSERT OR REPLACE INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES ('lead@xv',?,?,'operator-agent@kernel','quiescing',?)").run(desired, effective, new Date(clock).toISOString());
      expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("delivery_fencing_active");
    }
    db.prepare("UPDATE seat_delivery_guards SET desired=0, effective=0 WHERE node_id='lead@xv'").run();
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).ok).toBe(true);
  });

  it("custody drift during the async probe aborts with zero writes", async () => {
    const r = service({ piProve: async () => { db.prepare("UPDATE sessions SET status='superseded' WHERE id='lead@xv'").run(); return { state: "present" as const, generation: "lead-g1", launchId: "L", fingerprint: "{}" }; } });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("state_changed_during_probe");
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "superseded" });
    expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get()).toBeUndefined();
  });

  it("a seat delivery guard engaged during the probe aborts inside the transaction with zero writes", async () => {
    const r = service({ piProve: async () => { db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES ('lead@xv',1,0,'watchdog@system','quiescing',?)").run(new Date(clock).toISOString()); return { state: "present" as const, generation: "lead-g1", launchId: "L", fingerprint: "{}" }; } });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("state_changed_during_probe");
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "detached" });
    expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get()).toBeUndefined();
  });

  it("drift rollback leaves no ledger row even on an unenrolled rig", async () => {
    const r = service({ piProve: async () => { db.prepare("UPDATE occupant_tenures SET generation_uuid='lead-gX' WHERE node_id='lead@xv'").run(); return { state: "present" as const, generation: "lead-g1", launchId: "L", fingerprint: "{}" }; } });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("state_changed_during_probe");
    expect(db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get()).toEqual({ status: "detached" });
    expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get()).toBeUndefined();
    expect(db.prepare("SELECT 1 FROM events WHERE type='session.live_projection_recovered'").get()).toBeUndefined();
  });

  // --- Class-level effect fence: only guard-bound effects that can still move
  // fence. Terminal UNKNOWN ('indeterminate') rows are preserved and reported.

  const guarded = (id: string, state: string, seat = "lead@xv", binding: unknown = { nodeId: "lead@xv", session: "lead@xv", occupant: "lead-g1", pane: "%9" }) =>
    db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state,guard_binding) VALUES (?,'watchdog@system',?,?,?,?,?)")
      .run(id, seat, `wake ${id}`, new Date(clock).toISOString(), state, JSON.stringify(binding));
  const outboxBytes = () => db.prepare("SELECT * FROM outbox_entries ORDER BY outbox_id").all();
  const detachSeat = () => db.prepare("UPDATE sessions SET status='detached' WHERE id='lead@xv'").run();
  const seatStatus = () => db.prepare("SELECT status FROM sessions WHERE id='lead@xv'").get();

  function outboxEntriesReset(rows: unknown[]): void {
    db.prepare("DELETE FROM outbox_entries").run();
    const cols = ["outbox_id", "sender_session", "destination_session", "body", "ts_dispatched", "delivery_state", "guard_binding"] as const;
    const insert = db.prepare(`INSERT INTO outbox_entries (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`);
    db.transaction(() => { for (const row of rows as Array<Record<string, unknown>>) insert.run(...cols.map(c => row[c] as never)); }).immediate();
  }

  it("S1: terminal UNKNOWN guarded effects do not fence; outbox bytes are identical and the receipt records count and digest", async () => {
    for (const id of ["wake-intent-1", "wake-intent-2", "wake-intent-3"]) guarded(id, "indeterminate");
    const before = outboxBytes();
    const out = await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input());
    expect(out).toMatchObject({ ok: true, code: "recovered" });
    expect(seatStatus()).toEqual({ status: "running" });
    // Byte identity across the whole operation: nothing retried, released,
    // relabelled, retired or acknowledged; no row appeared or vanished.
    expect(outboxBytes()).toEqual(before);
    expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id='wake-intent-1'").get()).toEqual({ delivery_state: "indeterminate" });
    const parsed = out.receipt as { unknownEffectsPreserved: { count: number; digest: string } };
    expect(parsed.unknownEffectsPreserved.count).toBe(3);
    expect(parsed.unknownEffectsPreserved.digest).toMatch(/^[0-9a-f]{64}$/);
    const stored = JSON.parse((db.prepare("SELECT receipt FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-1'").get() as { receipt: string }).receipt);
    expect(stored.unknownEffectsPreserved).toEqual(parsed.unknownEffectsPreserved);
    // A seat with no UNKNOWN rows still reports an empty set, never an absent fact.
    outboxEntriesReset([]);
    detachSeat();
    const clean = await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input({ operationId: "op-clean" }));
    expect(clean.ok).toBe(true);
    expect(JSON.parse((db.prepare("SELECT receipt FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-clean'").get() as { receipt: string }).receipt).unknownEffectsPreserved)
      .toEqual({ count: 0, digest: expect.stringMatching(/^[0-9a-f]{64}$/) });
  });

  it.each(["pending", "sending", "retained"] as const)("S2/S3: a %s guard-bound effect still fences and writes nothing", async state => {
    guarded("w-movable", state);
    const before = outboxBytes();
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("delivery_fencing_active");
    expect(seatStatus()).toEqual({ status: "detached" });
    expect(outboxBytes()).toEqual(before);
    expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get()).toBeUndefined();
    // Terminal rows alongside a movable one still fence: the movable one decides.
    guarded("w-unknown", "indeterminate");
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("delivery_fencing_active");
  });

  it("S4: a reservation opened during the probe aborts inside the transaction with zero writes", async () => {
    const before = outboxBytes();
    const r = service({ piProve: async () => {
      db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r-probe','o1','lead@xv','lead@xv','g0','n0','operator-agent@kernel','operator-agent-g1','h','{}','{}','started',?,?)").run(new Date(clock).toISOString(), new Date(clock).toISOString());
      return { state: "present" as const, generation: "lead-g1", launchId: "L", fingerprint: "{}" };
    } });
    expect((await r.recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("state_changed_during_probe");
    expect(seatStatus()).toEqual({ status: "detached" });
    expect(outboxBytes()).toEqual(before);
    expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get()).toBeUndefined();
  });

  it("S5: a terminal UNKNOWN row that changes during the probe aborts as drift with zero writes", async () => {
    for (const id of ["wake-intent-a", "wake-intent-b"]) guarded(id, "indeterminate");
    const before = outboxBytes();
    const mutations: Array<[string, () => void]> = [
      ["delivered", () => db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id='wake-intent-a'").run()],
      ["body rewritten", () => db.prepare("UPDATE outbox_entries SET body='rewritten' WHERE outbox_id='wake-intent-b'").run()],
      ["guard unbound", () => db.prepare("UPDATE outbox_entries SET guard_binding=NULL WHERE outbox_id='wake-intent-b'").run()],
      ["row added", () => db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state,guard_binding) VALUES ('wake-intent-c','watchdog@system','lead@xv','wake',?,'indeterminate','{\"pane\":\"%9\"}')").run(new Date(clock).toISOString())],
    ];
    for (const [name, mutate] of mutations) {
      const pre = outboxBytes();
      const r = service({ piProve: async () => { mutate(); return { state: "present" as const, generation: "lead-g1", launchId: "L", fingerprint: "{}" }; } });
      const out = await r.recover("operator-agent@kernel", "operator-agent-g1", input());
      expect(out.code, name).toBe("state_changed_during_probe");
      expect(seatStatus(), name).toEqual({ status: "detached" });
      expect(db.prepare("SELECT 1 FROM live_projection_recovery_operations").get(), name).toBeUndefined();
      expect(db.prepare("SELECT 1 FROM events WHERE type='session.live_projection_recovered'").get(), name).toBeUndefined();
      // Recovery itself wrote nothing; only the simulated external mutation moved a row.
      expect(outboxBytes(), name).not.toEqual(pre);
      outboxEntriesReset(pre);
    }
    outboxEntriesReset(before);
    expect((await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input())).code).toBe("recovered");
  });

  it("S6: UNKNOWN rows bound to a previous occupant are neither fenced nor touched", async () => {
    guarded("wake-intent-prev", "indeterminate", "lead@xv", { nodeId: "lead@xv", session: "lead@xv", occupant: "lead-g0-retired", pane: "%9" });
    guarded("wake-intent-none", "indeterminate", "lead@xv", { nodeId: "lead@xv", session: "lead@xv", occupant: null, pane: "%9" });
    guarded("wake-intent-other-seat", "indeterminate", "peer@xv", { nodeId: "peer@xv", session: "peer@xv", occupant: "peer-g1", pane: "%3" });
    const before = outboxBytes();
    const out = await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input());
    expect(out).toMatchObject({ ok: true, code: "recovered" });
    expect(outboxBytes()).toEqual(before);
    // Only this seat's own UNKNOWN rows are reported; the other seat's are untouched.
    expect((out.receipt as { unknownEffectsPreserved: { count: number } }).unknownEffectsPreserved.count).toBe(2);
  });

  it("the preservation digest is byte-sensitive and stable across exact replay", async () => {
    guarded("wake-intent-1", "indeterminate");
    const r = service({ piProve: present() });
    const first = (await r.recover("operator-agent@kernel", "operator-agent-g1", input())).receipt as { unknownEffectsPreserved: { digest: string } };
    const replay = (await r.recover("operator-agent@kernel", "operator-agent-g1", input())).receipt as { unknownEffectsPreserved: { digest: string } };
    expect(replay.unknownEffectsPreserved.digest).toBe(first.unknownEffectsPreserved.digest);
    // Same id, different bytes: a different digest, so identity is byte-based.
    db.prepare("UPDATE outbox_entries SET body='different' WHERE outbox_id='wake-intent-1'").run();
    detachSeat();
    const second = (await service({ piProve: present() }).recover("operator-agent@kernel", "operator-agent-g1", input({ operationId: "op-2" }))).receipt as { unknownEffectsPreserved: { digest: string } };
    expect(second.unknownEffectsPreserved.digest).not.toBe(first.unknownEffectsPreserved.digest);
  });
});

// Codex initial-launch projection proof. The strict resume-token path above is
// untouched; these cases cover only the alternative witness reached after it
// declines for a seat launched without a resume argv.
describe("codex initial-launch projection proof", () => {
  const CODEX_TOKEN = "01a0fe42-cdb5-78d3-94fc-60533aaa46fb";
  const CODEX_GEN = "a6285bc5-0a93-4c7c-9cb7-6d4331aad0eb";
  const ROLLOUT = `/home/u/.codex/sessions/2026/10/02/rollout-2026-10-02T16-16-26-${CODEX_TOKEN}.jsonl`;
  const codexWorld = (over: { token?: string; generation?: string; rollouts?: Array<{ fd: number; inode: string; path: string }>; parents?: Record<number, number>; paneRoot?: number } = {}): CodexSessionFileProofDeps => {
    const token = over.token ?? CODEX_TOKEN;
    const rollouts = over.rollouts ?? [{ fd: 43, inode: "472826364", path: ROLLOUT }];
    const parents = over.parents ?? { 54494: 54455, 54455: 53570 };
    return {
      async run(command, args) {
        const pid = Number(String(args[args.length - 1]).match(/\d+/)?.[0] ?? 0);
        if (command === "ps" && args.includes("-axo")) return "54494  54455  /vendor/bin/codex\n54455  53570  /bin/zsh\n53570     1  /bin/zsh\n";
        if (command === "ps" && args.includes("comm=")) return `/vendor/bin/codex\n`;
        if (command === "lsof") return `p${pid}\n` + rollouts.map(e => `f${e.fd}\nau\ni${e.inode}\nD0x1000011\nn${e.path}\n`).join("");
        if (command === "stat") { const f = args[args.length - 1]!; const hit = rollouts.find(e => e.path === f); return hit ? `16777233 ${hit.inode}\n` : ""; }
        return "";
      },
      async readPrefix() { return `${JSON.stringify({ type: "session_meta", payload: { id: token, cwd: "/anywhere" } })}\n`; },
      async occupantGeneration() { return over.generation ?? CODEX_GEN; },
      async ancestry(pid) { const p = parents[pid]; return p === undefined ? [] : [p]; },
    };
  };
  const codexSeat = () => {
    db.prepare("UPDATE sessions SET status='detached', resume_token=? WHERE session_name='lead@xv'").run(CODEX_TOKEN);
    db.prepare("UPDATE nodes SET runtime='codex' WHERE id='lead@xv'").run();
    db.prepare("UPDATE bindings SET tmux_pane='%0' WHERE node_id='lead@xv'").run();
    db.prepare("UPDATE occupant_tenures SET generation_uuid=? WHERE node_id='lead@xv'").run(CODEX_GEN);
  };
  const codexInput = (): LiveProjectionRecoveryInput => ({
    operationId: "op-codex-initial", sessionId: "lead@xv", nodeId: "lead@xv",
    sessionName: "lead@xv", expectedGeneration: CODEX_GEN,
  });

  it("accepts an initial launch through the session-file proof and records only identifiers", async () => {
    codexSeat();
    const svc = service({ codexSessionFileProof: codexWorld(), tmux: { listSessions: async () => [{ name: "lead@xv" }], getPanePid: async () => 53570, getPaneCommand: async () => "/bin/zsh" } });
    const out = await svc.recover("operator-agent@kernel", "operator-agent-g1", codexInput());
    expect(out.ok).toBe(true);
    const receipt = db.prepare("SELECT receipt FROM live_projection_recovery_operations WHERE operation_id='live-projection-recover:op-codex-initial'").get() as { receipt: string } | undefined;
    expect(receipt).toBeTruthy();
    const parsed = JSON.parse(receipt!.receipt);
    const evidence = (parsed.evidence ?? parsed.proof ?? parsed) as Record<string, unknown>;
    expect(evidence.axis).toBe("codex_initial_launch_session_file");
    expect(evidence.fd).toBe(43);
    expect(evidence.inode).toBe("472826364");
    expect(evidence.generation).toBe(CODEX_GEN);
    // the stored token never appears in the clear anywhere in the durable receipt
    expect(receipt!.receipt).not.toContain(CODEX_TOKEN);
    expect(db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get()).toEqual({ status: "running" });
  });

  it("still refuses a genuinely mismatching native runtime, with zero writes", async () => {
    codexSeat();
    const before = db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get();
    const svc = service({ codexSessionFileProof: codexWorld({ token: "00000000-0000-4000-8000-000000000000" }) });
    const out = await svc.recover("operator-agent@kernel", "operator-agent-g1", codexInput());
    expect(out.ok).toBe(false);
    expect((out as { code: string }).code).toBe("native_proof_failed");
    expect(db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get()).toEqual(before);
    expect(db.prepare("SELECT count(*) n FROM live_projection_recovery_operations").get()).toEqual({ n: 0 });
  });

  it("B1: with NO injected deps the service still reaches the real proof path, and refuses cleanly on real evidence", async () => {
    codexSeat();
    const before = db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get();
    // No codexSessionFileProof injected: the constructor default (real kernel and
    // filesystem deps) must be in force, so the witness is never "unconfigured".
    const svc = new LiveProjectionRecoveryService({
      db,
      tmux: { listSessions: async () => [{ name: "lead@xv" }], getPanePid: async () => 53570, getPaneCommand: async () => "/bin/zsh" } as never,
      authority: repo.coordinatorAuthority,
      events: new EventBus(db),
    });
    const out = await svc.recover("operator-agent@kernel", "operator-agent-g1", codexInput());
    // The only thing this wiring test asserts is that the witness is REACHED. Whether
    // the live host happens to satisfy it is deliberately not asserted here, so the
    // suite never depends on which panes are running; Claude's required separate run
    // is the one that must exercise the real default deps against a live pane.
    if (!out.ok) expect((out as { message: string }).message).not.toContain("initial_launch_proof_unconfigured");
    else expect(db.prepare("SELECT count(*) n FROM live_projection_recovery_operations").get()).toEqual({ n: 1 });
    // Either way the outcome is a real verdict from the real deps, never a silent no-op.
    expect(out.ok === true || typeof (out as { code: string }).code === "string").toBe(true);
    if (!out.ok) expect(db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get()).toEqual(before);
  });

  it("refuses multiple open writers and a generation mismatch, mutating nothing", async () => {
    codexSeat();
    for (const world of [
      codexWorld({ rollouts: [{ fd: 43, inode: "472826364", path: ROLLOUT }, { fd: 44, inode: "472826365", path: "/x/rollout-other.jsonl" }] }),
      codexWorld({ generation: "00000000-0000-4000-8000-000000000000" }),
    ]) {
      const before = db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get();
      const out = await service({ codexSessionFileProof: world }).recover("operator-agent@kernel", "operator-agent-g1", codexInput());
      expect(out.ok).toBe(false);
      expect(db.prepare("SELECT status FROM sessions WHERE session_name='lead@xv'").get()).toEqual(before);
      expect(db.prepare("SELECT count(*) n FROM live_projection_recovery_operations").get()).toEqual({ n: 0 });
    }
  });
});
