import { describe, expect, it } from "vitest";
import { assessNativeResumeProbe } from "../src/domain/native-resume-probe.js";
import { isAttentionRequiredReadinessCode } from "../src/domain/runtime-adapter.js";

// Redacted rendered screen from the one disposable Codex 0.160.0 PTY.
const current = [
  "Update available · 0.160.0 → 0.160.1",
  "  Release notes: https://github.com/openai/codex/releases/latest",
  "› 1. Update now (runs `npm install -g @openai/codex`)",
  "  2. Skip", "  3. Skip until next version", "  enter continue · esc skip",
].join("\n");
const legacy = current.replace("Update available · 0.160.0 → 0.160.1", "✨ Update available! 0.155.1 -> 0.156.1");
const probe = (paneContent: string) => assessNativeResumeProbe({ runtime: "codex", paneCommand: "sh", paneContent });

describe("Codex update menu recognition", () => {
  it.each([current, legacy])("keeps a complete native menu attention-required despite a shell foreground label", menu => {
    expect(probe(menu)).toMatchObject({ status: "inconclusive", code: "update_gate" });
    expect(isAttentionRequiredReadinessCode(probe(menu).code)).toBe(true);
  });
  it.each([current, legacy])("does not let an earlier composer and model footer certify the update menu", menu => {
    expect(probe(`OpenAI Codex (v0.160.0)\n› Earlier prompt\n${menu}\n  gpt-6-luna medium · /work`))
      .toMatchObject({ status: "inconclusive", code: "update_gate" });
  });
  it.each([1, 2, 3])("requires actual option %i rather than an update banner", option => {
    const incomplete = current.split("\n").filter(line => !new RegExp(`^[› ]*${option}\\.`).test(line)).join("\n");
    expect(probe(incomplete)).toMatchObject({ status: "failed", code: "returned_to_shell" });
  });
  it.each([
    "user@host %", "Update available!", "Update available · 0.160.0 → 0.160.1",
    "echo 'Update available!'", "Discuss Update available · 0.160.0 → 0.160.1",
    current.replace("Update available · 0.160.0 → 0.160.1", "Some unrelated available update"),
    current.replace("3. Skip until next version", "3. Delete everything"),
  ])("preserves ordinary shell/text behavior", text => {
    expect(probe(text)).toMatchObject({ status: "failed", code: "returned_to_shell" });
  });
  it("ignores a complete old menu above a later live Codex header", () => {
    expect(probe(`${legacy}\nOpenAI Codex (v0.160.0)\n› Ask Codex to do anything`))
      .toMatchObject({ status: "resumed", code: "active_runtime" });
  });
});
