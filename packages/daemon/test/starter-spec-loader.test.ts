// Built-in workflow spec loader tests.
//
// Drives the loader against a temp builtin directory + an in-memory
// workflow_specs cache so the test stays deterministic and parallel-safe.
// Pins the load-bearing behaviors:
//
//   - cold start with N specs: all N seeded
//   - re-run on already-cached spec: SKIPPED (no clobber of operator
//     overrides under workspace-surface reconciliation)
//   - operator override at workspace path: SKIPPED + source_path in
//     cache stays the operator's path
//   - malformed spec file: error collected, not thrown; other specs
//     in the same dir still load
//   - missing builtin dir: empty result, no throw (graceful)
//   - non-yaml files in dir: silently ignored

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { coreSchema } from "../src/db/migrations/001_core_schema.js";
import { workflowSpecsSchema } from "../src/db/migrations/033_workflow_specs.js";
import { workflowSpecsDiagnosticSchema } from "../src/db/migrations/040_workflow_specs_diagnostic.js";
import { workflowSpecJsonSchema } from "../src/db/migrations/050_workflow_spec_json.js";
import { WorkflowSpecCache } from "../src/domain/workflow-spec-cache.js";
import { loadStarterWorkflowSpecs, defaultBuiltinSpecsDir } from "../src/domain/workflow/starter-spec-loader.js";
import { projectSpecGraph } from "../src/domain/workflow/slice-workflow-projection.js";

const ALPHA_SPEC = `workflow:
  id: alpha-spec
  version: 1
  objective: alpha test
  roles:
    a:
      preferred_targets: [a@r]
  steps:
    - id: only
      actor_role: a
      allowed_exits: [handoff]
`;

const BETA_SPEC = `workflow:
  id: beta-spec
  version: 1
  objective: beta test
  roles:
    b:
      preferred_targets: [b@r]
  steps:
    - id: only
      actor_role: b
      allowed_exits: [handoff]
`;

describe("built-in workflow spec loader", () => {
  let db: Database.Database;
  let cache: WorkflowSpecCache;
  let builtinDir: string;
  let cleanupRoot: string;

  beforeEach(() => {
    db = createDb();
    migrate(db, [coreSchema, workflowSpecsSchema]);
    cache = new WorkflowSpecCache(db);
    cleanupRoot = mkdtempSync(join(tmpdir(), "starter-loader-"));
    builtinDir = join(cleanupRoot, "workflow-specs");
    require("node:fs").mkdirSync(builtinDir, { recursive: true });
  });

  afterEach(() => {
    db.close();
    rmSync(cleanupRoot, { recursive: true, force: true });
  });

  it("returns empty result with no throw when builtinDir doesn't exist", () => {
    const missing = join(cleanupRoot, "definitely-missing");
    const result = loadStarterWorkflowSpecs({ cache, builtinDir: missing });
    expect(result).toEqual({ loaded: [], skipped: [], relocated: [], errors: [] });
  });

  it("cold start: seeds every .yaml spec in the directory", () => {
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    writeFileSync(join(builtinDir, "beta.yaml"), BETA_SPEC);
    const result = loadStarterWorkflowSpecs({ cache, builtinDir });
    expect(result.loaded).toHaveLength(2);
    expect(result.loaded.map((r) => r.name).sort()).toEqual(["alpha-spec", "beta-spec"]);
    expect(result.skipped).toEqual([]);
    expect(result.errors).toEqual([]);
    // Confirm the cache actually has them.
    expect(cache.getByNameVersion("alpha-spec", "1")).not.toBeNull();
    expect(cache.getByNameVersion("beta-spec", "1")).not.toBeNull();
  });

  it("idempotent: second call skips already-cached specs (no clobber)", () => {
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    const first = loadStarterWorkflowSpecs({ cache, builtinDir });
    expect(first.loaded).toHaveLength(1);
    const second = loadStarterWorkflowSpecs({ cache, builtinDir });
    expect(second.loaded).toEqual([]);
    expect(second.skipped).toHaveLength(1);
    expect(second.skipped[0]?.name).toBe("alpha-spec");
  });

  it("operator override (workspace-surface reconciliation): existing cache row at non-builtin source_path wins; loader does NOT overwrite", () => {
    // Operator authors a spec at a "workspace" path with same (name, version)
    // and reads it through the cache first (simulates operator workflow).
    const operatorPath = join(cleanupRoot, "operator-override-alpha.yaml");
    writeFileSync(operatorPath, ALPHA_SPEC);
    cache.readThrough(operatorPath);
    const beforeRow = cache.getByNameVersion("alpha-spec", "1");
    expect(beforeRow?.sourcePath).toBe(operatorPath);

    // Now the daemon starts and runs the starter loader on the bundled
    // builtin dir, which contains the same (name, version).
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    const result = loadStarterWorkflowSpecs({ cache, builtinDir });

    // Loader skipped (operator wins).
    expect(result.loaded).toEqual([]);
    expect(result.skipped).toHaveLength(1);
    expect(result.skipped[0]?.sourcePathInCache).toBe(operatorPath);

    // Cache row's source_path is STILL the operator's path — not the
    // bundled built-in path (workspace-surface reconciliation preserved).
    const afterRow = cache.getByNameVersion("alpha-spec", "1");
    expect(afterRow?.sourcePath).toBe(operatorPath);
  });

  it("malformed spec file: error collected, other specs in same dir still load", () => {
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    writeFileSync(join(builtinDir, "broken.yaml"), "this is: not [a valid spec");
    const result = loadStarterWorkflowSpecs({ cache, builtinDir });
    expect(result.loaded.map((r) => r.name)).toEqual(["alpha-spec"]);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]?.sourcePath).toContain("broken.yaml");
    // Cache has alpha but NOT broken (which has no name to begin with).
    expect(cache.getByNameVersion("alpha-spec", "1")).not.toBeNull();
  });

  it("non-yaml files in dir are silently ignored (.md, .txt, .json)", () => {
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    writeFileSync(join(builtinDir, "README.md"), "# notes about the bundled specs");
    writeFileSync(join(builtinDir, "scratch.txt"), "ignore me");
    writeFileSync(join(builtinDir, "metadata.json"), `{"hint":"ignored"}`);
    const result = loadStarterWorkflowSpecs({ cache, builtinDir });
    expect(result.loaded).toHaveLength(1);
    expect(result.loaded[0]?.name).toBe("alpha-spec");
    expect(result.errors).toEqual([]);
  });

  it("supports both .yaml and .yml extensions", () => {
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    writeFileSync(join(builtinDir, "beta.yml"), BETA_SPEC);
    const result = loadStarterWorkflowSpecs({ cache, builtinDir });
    expect(result.loaded.map((r) => r.name).sort()).toEqual(["alpha-spec", "beta-spec"]);
  });

  it("relocates stale builtin rows from old install root to new install root", () => {
    // Simulate install A: create a builtin dir under install A (matching real structure)
    const installA = join(cleanupRoot, "install-A", "builtins", "workflow-specs");
    require("node:fs").mkdirSync(installA, { recursive: true });
    writeFileSync(join(installA, "alpha.yaml"), ALPHA_SPEC);

    // Boot under install A: load specs from install A
    const cache = new WorkflowSpecCache(db);
    let resultA = loadStarterWorkflowSpecs({ cache, builtinDir: installA });
    expect(resultA.loaded).toHaveLength(1);
    expect(resultA.loaded[0]?.name).toBe("alpha-spec");

    // Record the spec_id and source_path from install A
    const rowA = cache.getByNameVersion("alpha-spec", "1");
    expect(rowA).not.toBeNull();
    const specIdA = rowA!.specId;
    const sourcePathA = rowA!.sourcePath;
    expect(sourcePathA).toContain("install-A");

    // Simulate install B: create a new builtin dir under install B
    const installB = join(cleanupRoot, "install-B", "builtins", "workflow-specs");
    require("node:fs").mkdirSync(installB, { recursive: true });
    writeFileSync(join(installB, "alpha.yaml"), ALPHA_SPEC); // same content

    // Boot under install B: load specs from install B (simulating upgrade)
    // The loader should detect the stale row from install A and relocate it
    const resultB = loadStarterWorkflowSpecs({ cache, builtinDir: installB });

    // The spec should be relocated, not skipped or re-inserted
    expect(resultB.relocated).toHaveLength(1);
    expect(resultB.relocated[0]?.name).toBe("alpha-spec");
    expect(resultB.relocated[0]?.oldSourcePath).toContain("install-A");
    expect(resultB.relocated[0]?.newSourcePath).toContain("install-B");
    expect(resultB.loaded).toHaveLength(0);
    expect(resultB.skipped).toHaveLength(0);

    // The spec_id should be preserved, but source_path updated to install B
    const rowB = cache.getByNameVersion("alpha-spec", "1");
    expect(rowB).not.toBeNull();
    expect(rowB!.specId).toBe(specIdA);
    expect(rowB!.sourcePath).toContain("install-B");
    expect(rowB!.sourcePath).not.toContain("install-A");
  });

  it("does not relocate operator override rows (workspace-surface reconciliation preserved)", () => {
    // Operator creates an override at a workspace path
    const operatorPath = join(cleanupRoot, "operator-override.yaml");
    writeFileSync(join(cleanupRoot, "operator-override.yaml"), ALPHA_SPEC);
    cache.readThrough(join(cleanupRoot, "operator-override.yaml"));

    // Now load from builtin dir with same (name, version)
    writeFileSync(join(builtinDir, "alpha.yaml"), ALPHA_SPEC);
    const result = loadStarterWorkflowSpecs({ cache, builtinDir });

    // Should be skipped, not relocated (operator override wins)
    expect(result.relocated).toHaveLength(0);
    expect(result.skipped).toHaveLength(1);
    expect(result.skipped[0]?.sourcePathInCache).toContain("operator-override");

    // Cache row should still point to operator path
    const row = cache.getByNameVersion("alpha-spec", "1");
    expect(row?.sourcePath).toContain("operator-override");
  });

  it.each(["error", "retained"] as const)("preserves cached %s rows across install relocation", (status) => {
    migrate(db, [workflowSpecsDiagnosticSchema, workflowSpecJsonSchema]);
    const currentCache = new WorkflowSpecCache(db);
    const installA = join(cleanupRoot, "install-A", "builtins", "workflow-specs");
    const installB = join(cleanupRoot, "install-B", "builtins", "workflow-specs");
    for (const dir of [installA, installB]) {
      require("node:fs").mkdirSync(dir, { recursive: true });
      writeFileSync(join(dir, "alpha.yaml"), ALPHA_SPEC);
    }
    expect(loadStarterWorkflowSpecs({ cache: currentCache, builtinDir: installA }).loaded).toHaveLength(1);
    const original = currentCache.getByNameVersion("alpha-spec", "1")!;
    db.prepare("UPDATE workflow_specs SET status = ?, error_message = ? WHERE spec_id = ?")
      .run(status, status === "error" ? "preserved diagnostic" : null, original.specId);
    const before = db.prepare("SELECT * FROM workflow_specs WHERE spec_id = ?").get(original.specId);

    const result = loadStarterWorkflowSpecs({ cache: currentCache, builtinDir: installB });

    expect(result.relocated).toHaveLength(0);
    expect(result.loaded).toHaveLength(0);
    expect(result.skipped).toHaveLength(1);
    expect(db.prepare("SELECT * FROM workflow_specs WHERE spec_id = ?").get(original.specId)).toEqual(before);
  });

  it("relocates stale builtin with changed content (updates hash and content)", () => {
    // Install A with original content
    const installA = join(cleanupRoot, "install-A", "builtins", "workflow-specs");
    require("node:fs").mkdirSync(installA, { recursive: true });
    const ALPHA_SPEC_V1 = ALPHA_SPEC;
    writeFileSync(join(installA, "alpha.yaml"), ALPHA_SPEC);

    // Boot under A
    const cache = new WorkflowSpecCache(db);
    loadStarterWorkflowSpecs({ cache, builtinDir: installA });
    const rowA = cache.getByNameVersion("alpha-spec", "1");
    const specIdA = rowA!.specId;
    const hashA = rowA!.sourceHash;

    // Install B with CHANGED content (same name/version, different content)
    const installB = join(cleanupRoot, "install-B", "builtins", "workflow-specs");
    require("node:fs").mkdirSync(installB, { recursive: true });
    const ALPHA_SPEC_V2 = `workflow:
  id: alpha-spec
  version: 1
  objective: alpha test v2
  roles:
    a:
      preferred_targets: [a@r]
  steps:
    - id: only
      actor_role: a
      allowed_exits: [handoff]
`;
    writeFileSync(join(installB, "alpha.yaml"), ALPHA_SPEC_V2);

    // Boot under B
    const result = loadStarterWorkflowSpecs({ cache, builtinDir: installB });

    // Should relocate and update content/hash
    expect(result.relocated).toHaveLength(1);
    const rowB = cache.getByNameVersion("alpha-spec", "1");
    expect(rowB!.specId).toBe(rowA.specId); // spec_id preserved
    expect(rowB!.sourcePath).toContain("install-B");
    expect(rowB!.sourceHash).not.toBe(hashA); // hash updated
    expect(rowB!.spec.objective).toBe("alpha test v2"); // content updated
  });
});
