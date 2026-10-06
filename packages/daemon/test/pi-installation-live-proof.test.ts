// READ-ONLY proof against the REAL installed Pi, exercising only the resolver's
// filesystem provenance. No signal, no inspector attach, no target code, no eval.
// Skips cleanly when the installed Pi is absent, so it never fails a host without it.
import { describe, it, expect } from "vitest";
import { resolvePiInstallationModule } from "../src/domain/pi-installation-module-resolver.js";

describe("pi installation resolver against the real installed Pi", () => {
  it("deterministically resolves the one real cached-chunk exporting both classes", () => {
    const resolved = resolvePiInstallationModule({ executable: "pi", pathEnv: process.env.PATH });
    if (!resolved.ok) {
      // A host without the installed Pi is a legitimate absence, not a resolver defect.
      expect(resolved.reason).toBe("executable_unresolved");
      return;
    }
    // The URL is the real, hashed chunk the evidence pass recorded — reached by walking
    // the installation, never by naming it.
    expect(resolved.value.moduleUrl).toMatch(/\/dist\/bundle\/chunks\/chunk-[A-Z0-9]+\.js$/);
    expect(resolved.value.entryUrl).toMatch(/\/dist\/bundle\/cli\.js$/);
    expect(resolved.value.packageName).toBe("@earendil-works/pi-coding-agent");
    expect(resolved.value.modulesVisited).toBeGreaterThan(1);
  }, 20_000);
});
