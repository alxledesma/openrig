import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";
import { lifecycleTargetLookupIndexSchema } from "../src/db/migrations/099_lifecycle_target_lookup_index.js";

describe("lifecycle target lookup index", () => {
  it("indexes the production predicate without changing retained records or excluding duplicate targets", () => {
    const db = new Database(":memory:");
    try {
      db.exec("CREATE TABLE coordinator_operations(rig_id TEXT,operation_id TEXT,kind TEXT,receipt TEXT,request_hash TEXT,PRIMARY KEY(rig_id,operation_id))");
      const put = db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)");
      for (const [id, kind] of [["a", "lifecycle-retirement"], ["b", "held-history-retirement"], ["c", "acceptance"]]) {
        put.run("rig", id, "coordinator-lifecycle-control", JSON.stringify({ targetQueueId: "target", kind }), "immutable");
      }
      put.run("other", "d", "coordinator-lifecycle-control", JSON.stringify({ targetQueueId: "target", kind: "lifecycle-retirement" }), "immutable");
      put.run("rig", "opaque", "legacy", "not-json", "retained");
      const query = "SELECT operation_id,receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.targetQueueId')=? AND json_extract(receipt,'$.kind') IN ('held-history-retirement','lifecycle-retirement')";
      const before = db.prepare(query).all("target");
      const snapshot = db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all();
      db.exec(lifecycleTargetLookupIndexSchema.sql);
      db.exec(lifecycleTargetLookupIndexSchema.sql);
      const plan = db.prepare("EXPLAIN QUERY PLAN " + query).all("target") as Array<{ detail: string }>;
      expect(plan.map(x => x.detail).join(" ")).toContain("USING INDEX idx_coordinator_lifecycle_target_kind");
      expect(db.prepare(query).all("target")).toEqual(expect.arrayContaining(before));
      expect(db.prepare(query).all("target")).toHaveLength(3);
      expect(db.prepare(query).all("missing")).toEqual([]);
      expect(db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all()).toEqual(snapshot);
      const indexes = db.prepare("PRAGMA index_list(coordinator_operations)").all() as Array<{ name: string; unique: number }>;
      expect(indexes.find(x => x.name === "idx_coordinator_lifecycle_target_kind")?.unique).toBe(0);
    } finally { db.close(); }
  });
});
