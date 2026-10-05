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

function service(deps: { piProve?: (s: string) => Promise<{ state: "present" | "absent"; generation: string; launchId: string | null; fingerprint: string } | null>; tmux?: unknown }) {
  const tmux = deps.tmux ?? { listSessions: async () => [{ name: "lead@xv" }], getPanePid: async () => 4242, getPaneCommand: async () => "node" };
  return new LiveProjectionRecoveryService({ db, tmux: tmux as never, authority: repo.coordinatorAuthority, events: new EventBus(db), ...(deps.piProve ? { piProve: deps.piProve } : {}) });
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
});
