import { describe, it, expect, afterEach } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb } from "../src/db/connection.js";
import { seed as seedOpsFixture } from "./helpers/coordinator-fixture.js";

/** OPR measured repair: the coordinator operation lookup is the one query whose shape cannot be
 *  served by the (rig_id, operation_id) primary key, because it is keyed on operation_id alone.
 *  This proves the index is actually used by the planner and that it preserves results exactly,
 *  including the same operation id under several rigs and under several kinds. */
describe("coordinator_operations operation_id+kind lookup index", () => {
  const LOOKUP = "SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind=?";
  let dir: string;
  let db: ReturnType<typeof createDb>;
  afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

  const seedOps = () => {
    seedOpsFixture(db);
    db.prepare("INSERT INTO rigs(id,name) VALUES ('x1','x1')").run();
    db.prepare("INSERT INTO rigs(id,name) VALUES ('x2','x2')").run();
    db.prepare("INSERT INTO rigs(id,name) VALUES ('x3','x3')").run();
    for(const rig of ["x1","x2","x3"]){
      const baton=`b-${rig}`;
      db.prepare("INSERT INTO queue_items(qitem_id,source_session,destination_session,body,state,ts_created,ts_updated) VALUES (?,?,?,?,'pending',?,?)").run(baton,"operator-agent@kernel","lead@xv","{}",new Date().toISOString(),new Date().toISOString());
      db.prepare("INSERT INTO coordinator_authority(rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,state,operation_id,coordinators) VALUES (?,?,?,'lead-g1',1,1,'reconciling','seed','[]')").run(rig,baton,"lead@xv");
    }
    db.prepare("INSERT INTO coordinator_operations VALUES ('x1','shared-op','enable','r1','h1')").run();
    db.prepare("INSERT INTO coordinator_operations VALUES ('x2','shared-op','enable','r2','h2')").run();
    // The same operation id under a different kind lives on a different rig: the primary key is
    // (rig_id, operation_id), so one rig cannot hold the same id twice. That is exactly why the
    // new index must be NONUNIQUE.
    db.prepare("INSERT INTO coordinator_operations VALUES ('x3','shared-op','acknowledge','r3','h3')").run();
  };

  it("uses the index for the lookup instead of scanning the table", () => {
    dir = mkdtempSync(join(tmpdir(), "opidx-plan-"));
    db = createDb(join(dir, "db.sqlite"));
    seedOps();
    const plan = db.prepare(`EXPLAIN QUERY PLAN ${LOOKUP}`).all("shared-op", "enable")
      .map((r: { detail: string }) => r.detail).join(" ");
    expect(plan).toMatch(/SEARCH coordinator_operations USING INDEX idx_coordinator_operations_operation_kind \(operation_id=\? AND kind=\?\)/);
    expect(plan).not.toMatch(/SCAN coordinator_operations/);
  });

  it("preserves ordered results across duplicate rigs and differing kinds, and is nonunique", () => {
    dir = mkdtempSync(join(tmpdir(), "opidx-equiv-"));
    db = createDb(join(dir, "db.sqlite"));
    seedOps();
    // Duplicate operation ids across rigs both survive the indexed plan.
    const indexed = db.prepare(LOOKUP).all("shared-op", "enable");
    expect(indexed.map((r: { rig_id: string }) => r.rig_id).sort()).toEqual(["x1", "x2"]);
    // A different kind under the same id is not returned by a kind-scoped lookup.
    expect(db.prepare(LOOKUP).all("shared-op", "acknowledge").map((r: { rig_id: string }) => r.rig_id)).toEqual(["x3"]);
    // The index is explicitly NONUNIQUE: duplicates must be accepted, not rejected.
    const indexes = db.prepare("PRAGMA index_list(coordinator_operations)").all() as { name: string; unique: number }[];
    const added = indexes.find((i) => i.name === "idx_coordinator_operations_operation_kind");
    expect(added).toBeTruthy();
    expect(added!.unique).toBe(0);
    // The primary key is untouched.
    expect(indexes.some((i) => i.name === "sqlite_autoindex_coordinator_operations_1")).toBe(true);
    // Row count and full-table content are unchanged by the additive index.
    expect(db.prepare("SELECT count(*) c FROM coordinator_operations").get()).toEqual({ c: 3 });
    expect(db.prepare("SELECT rig_id,operation_id,kind,receipt,request_hash FROM coordinator_operations ORDER BY rig_id,operation_id,kind").all())
      .toEqual([
        { rig_id: "x1", operation_id: "shared-op", kind: "enable", receipt: "r1", request_hash: "h1" },
        { rig_id: "x2", operation_id: "shared-op", kind: "enable", receipt: "r2", request_hash: "h2" },
        { rig_id: "x3", operation_id: "shared-op", kind: "acknowledge", receipt: "r3", request_hash: "h3" },
      ]);
  });
});