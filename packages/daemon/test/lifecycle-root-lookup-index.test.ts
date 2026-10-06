import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";
import { lifecycleRootLookupIndexSchema } from "../src/db/migrations/100_lifecycle_root_lookup_index.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";

describe("lifecycle exact-subject lookup indexes (migration 100)", () => {
  it("is additive and nonunique, only indexes lifecycle-control rows, is idempotent and is used by the exact-subject queries", () => {
    const db = new Database(":memory:");
    try {
      db.exec("CREATE TABLE coordinator_operations(rig_id TEXT,operation_id TEXT,kind TEXT,receipt TEXT,request_hash TEXT,PRIMARY KEY(rig_id,operation_id))");
      const put = db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)");
      put.run("xv", "a", "coordinator-lifecycle-control", JSON.stringify({ kind: "acceptance", rootId: "root", packageKey: "p", semanticKey: "k" }), "h");
      put.run("xv", "b", "coordinator-lifecycle-control", JSON.stringify({ kind: "acceptance", rootId: "root", packageKey: "p", semanticKey: "k" }), "h");   // two links, one root
      put.run("other", "c", "coordinator-lifecycle-control", JSON.stringify({ kind: "acceptance", rootId: "root", packageKey: "p", semanticKey: "k" }), "h");
      put.run("xv", "legacy", "legacy-opaque", "not-json", "retained");                                                                                    // never indexed
      const snapshot = db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all();
      db.exec(lifecycleRootLookupIndexSchema.sql); db.exec(lifecycleRootLookupIndexSchema.sql);
      const rootPlan = (db.prepare("EXPLAIN QUERY PLAN SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.rootId')=? ORDER BY rowid DESC LIMIT 1").all("xv", "root") as Array<{ detail: string }>).map(r => r.detail).join(" ");
      const pkgPlan = (db.prepare("EXPLAIN QUERY PLAN SELECT operation_id FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')=? ORDER BY rowid DESC LIMIT 16").all("xv", "p") as Array<{ detail: string }>).map(r => r.detail).join(" ");
      expect(rootPlan).toContain("idx_coordinator_lifecycle_root");
      expect(pkgPlan).toContain("idx_coordinator_lifecycle_kind_package");
      expect(db.prepare("SELECT operation_id FROM coordinator_operations WHERE rig_id='xv' AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.rootId')='root' ORDER BY rowid DESC").all()).toEqual([{ operation_id: "b" }, { operation_id: "a" }]);
      expect(db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all()).toEqual(snapshot);
      const idx = db.prepare("PRAGMA index_list(coordinator_operations)").all() as Array<{ name: string; unique: number }>;
      for (const n of ["idx_coordinator_lifecycle_root", "idx_coordinator_lifecycle_kind_package"]) expect(idx.find(x => x.name === n)?.unique).toBe(0);
    } finally { db.close(); }
  });
  it("is registered once, after migration 099, in the canonical list", () => {
    const names = ALL_MIGRATIONS.map(m => m.name);
    expect(names.filter(n => n === "100_lifecycle_root_lookup_index.sql")).toHaveLength(1);
    expect(names.indexOf("100_lifecycle_root_lookup_index.sql")).toBeGreaterThan(names.indexOf("099_lifecycle_target_lookup_index.sql"));
  });
});
