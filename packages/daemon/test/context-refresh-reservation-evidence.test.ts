import { afterEach, beforeEach, expect, it, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import Database from "better-sqlite3";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { SeatDispatchReservationService, type ReservationRequest } from "../src/domain/seat-dispatch-reservation.js";

let db: Database.Database, guard: SeatDeliveryGuard, service: SeatDispatchReservationService;
let dir: string, nativeId: string;
const target = "builder@xv", operator = "operator-agent@kernel", profile = "evidence-profile";
const runtimeContract = { runtime: "codex", model: "gpt-6.1-sol", provider: "openai", profile, effort: "low", permissions: { sandbox: { type: "workspace-write" }, approval: "never" } };
const profileHash = () => createHash("sha256").update(fs.readFileSync(path.join(dir, `${profile}.config.toml`))).digest("hex");
function request(): ReservationRequest {
  return {
    reservationId: "evidence-attempt-1", operationId: "evidence-operation-1", nodeId: target,
    generation: "builder-g1", reason: "inspect durable reservation evidence", profileSha256: profileHash(),
    expected: { protocol: "generation-queue-runtime-idle-v1", generation: "native-old", queue: [], runtimeContract, checkpointHash: "evidence-checkpoint", reservationId: "evidence-attempt-1", operationId: "evidence-operation-1" },
  };
}
async function reserve() { return service.reserve(operator, "operator-agent-g1", request()); }
async function inspect(id = "evidence-attempt-1") {
  return guard.lifecycle([target, operator], () => service.inspectEvidence(id), id);
}
async function commitSuccessor() {
  const r = await reserve();
  await guard.lifecycle([target, operator], async () => {
    service.assertHandover(r.reservation_id, r.operation_id, target, operator, "operator-agent-g1", request().expected);
    service.start(r, operator, "operator-agent-g1");
    db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('evidence-successor',?,2,'builder-g2','fresh')").run(target);
    db.prepare("UPDATE nodes SET handover_result='complete' WHERE id=?").run(target);
    db.prepare("UPDATE sessions SET resume_token='native-new' WHERE node_id=?").run(target);
    guard.rebindLifecycle(target);
    service.committed(r.reservation_id, operator, "operator-agent-g1");
  }, r.reservation_id);
  return r;
}
const receipt = (kind: "successor_ack" | "independent_acceptance") => ({ operationId: "evidence-operation-1", checkpointHash: "evidence-checkpoint", kind, evidenceRef: "fixture:current-successor-evidence" });

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), "reservation-evidence-"));
  db = new Database(path.join(dir, "state.sqlite"));
  seed(db);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready'").run();
  db.prepare("UPDATE nodes SET runtime='codex',model='gpt-6.1-sol',cwd=?,codex_config_profile=? WHERE id=?").run(dir, profile, target);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date().toISOString(), new Date().toISOString());
  fs.writeFileSync(path.join(dir, `${profile}.config.toml`), 'model="gpt-6.1-sol"\nmodel_provider="openai"\n', { mode: 0o600 });
  vi.stubEnv("CODEX_HOME", dir);
  nativeId = "native-new";
  guard = new SeatDeliveryGuard(db, n => resolveGuardTarget(db, n));
  service = new SeatDispatchReservationService({ db, guard, verifyPredecessor: async () => {}, observeSuccessor: async () => ({ nativeId, runtimeContract }) });
});
afterEach(() => { db.close(); fs.rmSync(dir, { recursive: true, force: true }); vi.unstubAllEnvs(); });

it("returns false verification on successor mismatch without changing committed evidence", async () => {
  await commitSuccessor();
  nativeId = "native-drift";
  const before = db.prepare("SELECT * FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all("evidence-attempt-1");
  const evidence = await inspect();
  expect(evidence).toMatchObject({ reservation: { state: "committed" }, successorVerified: false, custodyVerified: true });
  expect(db.prepare("SELECT * FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all("evidence-attempt-1")).toEqual(before);
});

it("recognizes a released accepted successor from immutable release evidence without writing a new receipt", async () => {
  await commitSuccessor();
  await service.attest(target, "builder-g2", "evidence-attempt-1", receipt("successor_ack"));
  await service.attest("reviewer@xv", "reviewer-g1", "evidence-attempt-1", receipt("independent_acceptance"));
  await service.release(operator, "operator-agent-g1", "evidence-attempt-1", { operationId: "evidence-operation-1", reason: "accepted current successor", mode: "accepted_successor" });
  const before = db.prepare("SELECT * FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all("evidence-attempt-1");
  const evidence = await inspect();
  expect(evidence).toMatchObject({ reservation: { state: "released" }, successorVerified: true, custodyVerified: true });
  expect(db.prepare("SELECT * FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all("evidence-attempt-1")).toEqual(before);
});

it("keeps started precommit custody only when its frozen snapshot is exact and claim-release ledger is empty", async () => {
  const r = await reserve();
  await service.withAttemptLock(r.reservation_id, operator, "operator-agent-g1", async () => guard.lifecycle([target, operator], async () => {
    service.start(r, operator, "operator-agent-g1");
    const evidence = await service.inspectEvidence(r.reservation_id);
    expect(evidence).toMatchObject({ reservation: { state: "started" }, successorVerified: false, custodyVerified: true });
  }, r.reservation_id));
  expect(db.prepare("SELECT COUNT(*) AS n FROM seat_dispatch_claim_releases WHERE reservation_id=?").get(r.reservation_id)).toEqual({ n: 0 });
});

it("fails closed for an unknown reservation without inventing evidence", async () => {
  await expect(service.inspectEvidence("missing-reservation")).rejects.toMatchObject({ code: "reservation_not_found" });
});
