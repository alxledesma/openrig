// P3a focused suite: the durable coordinator-OPERATION readback used by finite native
// lease duty to classify an uncertain renew without replaying it. Contract under test:
// exact keyed lookup, typed 404 for genuinely unrecorded operations (absence alone never
// proves rejection), cross-rig isolation on the composite key, bearer authentication
// identical to every other coordinator read, and ZERO mutation of any durable state.
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Hono } from "hono";
import { createDb } from "../src/db/connection.js";
import { createHash } from "node:crypto";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { coordinatorRoutes } from "../src/routes/coordinator.js";
import { seed, token } from "./helpers/coordinator-fixture.js";
import type Database from "better-sqlite3";

describe("coordinator operation readback (P3a)", () => {
  let db: Database.Database, repo: QueueRepository, app: Hono;
  const auth = { Authorization: "Bearer test-token" };

  // Every durable user table, rowid-ordered: the zero-mutation claim covers the WHOLE DB.
  const dump = () => JSON.stringify(
    (db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all() as { name: string }[])
      .map(t => [t.name, db.prepare(`SELECT * FROM "${t.name}" ORDER BY rowid`).all()]));

  beforeEach(async () => {
    db = createDb(); seed(db);
    const bus = new EventBus(db); repo = new QueueRepository(db, bus); repo.attachOutbox(new OutboxHandler(db));
    await repo.create({ qitemId: "baton", sourceSession: "operator-agent@kernel", destinationSession: "lead@xv", body: "coordinate", nudge: false });
    repo.coordinatorAuthority.enable("operator-agent@kernel", "operator-agent-g1", { rigId: "xv", batonId: "baton", owner: "lead@xv", ownerGeneration: "lead-g1", coordinators: ["lead@xv", "peer@xv"], leaseMs: 3600000, operationId: "enable-xv" });
    // Owner acknowledgment moves the authority to ACTIVE so renew records a committed row.
    repo.coordinatorAuthority.acknowledge("lead@xv", token, { operationId: "ack-xv", obligationsDigest: repo.coordinatorAuthority.reconciliationDigest("xv") });
    app = new Hono(); app.use("*", async (c, next) => { c.set("queueRepo" as never, repo); await next(); });
    app.route("/api/coordinator", coordinatorRoutes({ bearerToken: "test-token" }));
  });
  afterEach(() => db.close());

  it("returns the EXACT committed renew row: rigId, operationId, kind, requestHash, receiptDigest, receipt", async () => {
    repo.coordinatorAuthority.renew("lead@xv", token, 3600000, "renew-1");
    const res = await app.request("/api/coordinator/xv/operations/renew-1", { headers: auth });
    expect(res.status).toBe(200);
    const body = await res.json() as Record<string, unknown>;
    expect(body).toMatchObject({ rigId: "xv", operationId: "renew-1", kind: "renew" });
    expect(typeof body["requestHash"]).toBe("string"); expect((body["requestHash"] as string).length).toBe(64);
    expect(typeof body["receiptDigest"]).toBe("string"); expect((body["receiptDigest"] as string).length).toBe(64);
    // The receipt is the durable authority-after record itself: epoch and generation match ours.
    expect(body["receipt"]).toMatchObject({ rig_id: "xv", owner_generation: "lead-g1", epoch: 1, operation_id: "renew-1" });
    // digest(receipt text) equals the stored canonical receipt, computed independently here.
    const stored = db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND operation_id='renew-1'").get() as { receipt: string };
    expect(body["receiptDigest"]).toBe(createHash("sha256").update(stored.receipt).digest("hex"));
  });

  it("a genuinely unrecorded operation is a typed 404 — never presented as proof of rejection", async () => {
    const res = await app.request("/api/coordinator/xv/operations/never-submitted", { headers: auth });
    expect(res.status).toBe(404);
    expect(((await res.json()) as { error: string }).error).toBe("operation_not_recorded");
  });

  it("cross-rig isolation: identical operationIds resolve only their own rig's row", async () => {
    repo.coordinatorAuthority.renew("lead@xv", token, 3600000, "shared-op");
    // Fixture-only second rig state: the operations table FK-keys to coordinator_authority,
    // so seed ONE inert authority row plus the SAME operationId with a distinct receipt/hash.
    // No second live authority is enabled through the service, per the bounded-fixture rule.
    await repo.create({ qitemId: "baton9", sourceSession: "operator-agent@kernel", destinationSession: "worker@other", body: "fixture", nudge: false });
    db.prepare("INSERT INTO coordinator_authority(rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,state,operation_id,coordinators,recovery_queue_id) VALUES ('other','baton9','worker@other','worker-g1',1,?,'active','fixture-seed','[\"worker@other\"]',NULL)").run(Date.now() + 3_600_000);
    db.prepare("INSERT INTO coordinator_operations(rig_id,operation_id,kind,receipt,request_hash) VALUES ('other','shared-op','renew',?,?)")
      .run('{"rig_id":"other","epoch":9,"owner_generation":"foreign-g1"}', "f".repeat(64));
    const mine = await app.request("/api/coordinator/xv/operations/shared-op", { headers: auth });
    expect(((await mine.json()) as { rigId: string; receipt: { epoch: number } }).rigId).toBe("xv");
    const theirs = await app.request("/api/coordinator/other/operations/shared-op", { headers: auth });
    const tb = await theirs.json() as { rigId: string; requestHash: string };
    expect(tb.rigId).toBe("other"); expect(tb.requestHash).toBe("f".repeat(64));
    const absent = await app.request("/api/coordinator/kernel/operations/shared-op", { headers: auth });
    expect(absent.status).toBe(404);
  });

  it("a malformed durable receipt is refused TYPED, never disguised as absence", async () => {
    db.prepare("INSERT INTO coordinator_operations(rig_id,operation_id,kind,receipt,request_hash) VALUES ('xv','corrupt-1','renew','{not-json',?)").run("a".repeat(64));
    const res = await app.request("/api/coordinator/xv/operations/corrupt-1", { headers: auth });
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: string }).error).toBe("coordinator_operation_receipt_unreadable");
  });

  it("unauthorized reads are refused by the bearer boundary before any lookup", async () => {
    for (const headers of [{}, { Authorization: "Bearer wrong" }] as Array<Record<string, string>>) {
      const res = await app.request("/api/coordinator/xv/operations/enable-xv", { headers });
      expect(res.status).toBe(401);
    }
  });

  it("the readback mutates NOTHING: full durable dumps are byte-identical across calls", async () => {
    repo.coordinatorAuthority.renew("lead@xv", token, 3600000, "renew-audit");
    const before = dump();
    for (let i = 0; i < 3; i++) {
      const ok = await app.request("/api/coordinator/xv/operations/renew-audit", { headers: auth }); expect(ok.status).toBe(200); await ok.json();
      const gone = await app.request("/api/coordinator/xv/operations/absent", { headers: auth }); expect(gone.status).toBe(404); await gone.json();
    }
    expect(dump()).toBe(before);
  });
});
