import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "vitest";
import assert from "node:assert/strict";
import { codexRotationContract } from "../src/domain/rotation-native-proof.js";

const profile = "selected-profile";
const argv = ["codex", "-p", profile];

function withHistory(contents: string | Buffer, run: (path: string) => void): void {
  const dir = mkdtempSync(join(tmpdir(), "rotation-native-proof-"));
  const path = join(dir, "history.jsonl");
  try {
    writeFileSync(path, contents, { mode: 0o600 });
    run(path);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function record(type: string, payload: unknown): string {
  return JSON.stringify({ type, payload });
}

test("streams complete latest metadata across chunk boundaries and preserves malformed-line semantics", () => {
  const prefix = '{"type":"turn_context","payload":{"model":"gpt-6-astra","padding":"';
  const paddingBytes = 64 * 1024 - 1 - Buffer.byteLength(prefix);
  assert.ok(paddingBytes > 0);
  const boundaryRecord = prefix + "a".repeat(paddingBytes) + "😀" + '"}}';
  const contents = [
    record("session_meta", { id: "older", model_provider: "old-provider" }),
    "{malformed json is ignored}",
    boundaryRecord,
    record("session_meta", { id: "current-generation", model_provider: "openai" }),
    record("turn_context", {
      model: "gpt-6-luna",
      sandbox_policy: { type: "workspace-write", network_access: true },
      approval_policy: "never",
      effort: "medium",
    }),
  ].join("\n");

  withHistory(contents, (path) => {
    assert.deepEqual(
      codexRotationContract(path, "current-generation", argv, "gpt-6-luna", profile),
      {
        runtime: "codex",
        model: "gpt-6-luna",
        provider: "openai",
        profile,
        permissions: {
          sandbox: { type: "workspace-write", network_access: true },
          approval: "never",
        },
        effort: "medium",
      },
    );
  });
});

test("fails closed with a named error for a single history line above the bound", () => {
  const oversized = Buffer.alloc(16 * 1024 * 1024 + 1, 0x61);
  withHistory(oversized, (path) => {
    assert.throws(
      () => codexRotationContract(path, "current-generation", argv, "gpt-6-luna", profile),
      { message: "Native history line exceeds streaming limit" },
    );
  });
});
