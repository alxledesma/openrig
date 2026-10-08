import { describe, it, expect } from "vitest";
import { assertRotationLaunchPosture } from "../src/domain/rotation-precondition.js";

describe("reserved rotation runtime posture", () => {
  it("preserves Pi trust and refuses a Codex contract or implicit trust upgrade", () => {
    expect(() => assertRotationLaunchPosture("pi", { runtime: "pi", trust: "approve" }, "full_bypass")).not.toThrow();
    expect(() => assertRotationLaunchPosture("pi", { runtime: "pi", trust: "no-approve" }, "floor")).not.toThrow();
    for (const contract of [{ runtime: "pi", trust: "no-approve" }, { runtime: "pi" },
      { runtime: "codex", permissions: { sandbox: { type: "danger-full-access" }, approval: "never" } }]) {
      expect(() => assertRotationLaunchPosture("pi", contract, "full_bypass")).toThrow("no process replaced");
    }
    expect(() => assertRotationLaunchPosture("pi", { runtime: "pi", trust: "approve" }, "floor")).toThrow();
  });
  it("retains Codex sandbox and approval fences without accepting Pi evidence", () => {
    const full = { runtime: "codex", permissions: { sandbox: { type: "danger-full-access" }, approval: "never" } };
    expect(() => assertRotationLaunchPosture("codex", full, "full_bypass")).not.toThrow();
    expect(() => assertRotationLaunchPosture("codex", { ...full, permissions: { ...full.permissions, approval: "on-request" } }, "full_bypass")).toThrow();
    expect(() => assertRotationLaunchPosture("codex", { runtime: "codex", permissions: { sandbox: { type: "workspace-write" } } }, "floor")).not.toThrow();
    expect(() => assertRotationLaunchPosture("codex", { runtime: "pi", trust: "approve" }, "full_bypass")).toThrow();
  });
});
