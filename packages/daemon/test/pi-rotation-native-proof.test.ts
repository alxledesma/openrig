import { afterEach, describe, expect, it } from "vitest";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  piLaunchConfigurationDigest, piRotationContract, piSuccessorMatches,
  type PiRotationNativeProof, type PiRotationNode, type PiRotationReadiness,
} from "../src/domain/pi-rotation-native-proof.js";

const NOW = Date.parse("2026-10-07T23:30:00Z");
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });

function fixture() {
  const root = mkdtempSync(path.join(os.tmpdir(), "pi-rotation-native-proof-")); roots.push(root);
  const agentDir = path.join(root, "agent"), skills = path.join(agentDir, "skills", "review");
  mkdirSync(skills, { recursive: true });
  writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ defaultProvider: "anthropic", defaultModel: "claude-sonnet", defaultThinkingLevel: "high" }));
  writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: [] }));
  writeFileSync(path.join(skills, "SKILL.md"), "skill bytes\n");
  const sessionFile = path.join(root, "session.jsonl");
  const rows = [
    { type: "session", id: "session-one", version: 3 },
    { type: "model_change", id: "e1", provider: "anthropic", modelId: "claude-sonnet" },
    { type: "thinking_level_change", id: "e2", thinkingLevel: "high" },
  ];
  writeFileSync(sessionFile, rows.map(row => JSON.stringify(row) + "\n").join(""));
  const node: PiRotationNode = { runtime: "pi", model: "anthropic/claude-sonnet", generation: "gen-one", sessionStatus: "running", startupStatus: "ready", resumeType: "pi_session_file", resumeToken: sessionFile, launchPosture: "floor" };
  const readiness: PiRotationReadiness = { ready: true, launchId: "launch-one", generation: "gen-one", sessionFile, lastEntryId: "e2", model: { provider: "anthropic", id: "claude-sonnet", contextWindow: 200000 }, thinkingLevel: "high", observedAt: new Date(NOW).toISOString(), failures: [] };
  const proof: PiRotationNativeProof = { state: "present", generation: "gen-one", launchId: "launch-one", fingerprint: "runner-child-start", lastEntryId: "e2", quiescence: { settled: true, observedAt: new Date(NOW - 1000).toISOString() }, verifiedLaunch: { generation: "gen-one", launchId: "launch-one", sessionFile, pid: 4321, startFingerprint: "pid-start-fingerprint", trustFlag: "no-approve" } };
  return { root, agentDir, sessionFile, rows, node, readiness, proof };
}

describe("Pi rotation native contract", () => {
  it("binds a ready session, settings selection, kernel trust and immutable history identity", () => {
    const f = fixture();
    const result = piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.contract.nativeId).toBe(`session-one\n${f.sessionFile}`);
    expect(result.contract.sessionSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(result.contract.trust).toBe("no-approve");
  });

  it("holds a mismatched node model, trust posture or unresolved readiness despite stale defaults", () => {
    const f = fixture();
    f.node.model = "anthropic/other";
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false, hold: "binding-changed" });
    f.node.model = "anthropic/claude-sonnet";
    writeFileSync(path.join(f.agentDir, "settings.json"), JSON.stringify({ defaultProvider: "anthropic", defaultModel: "claude-sonnet", defaultThinkingLevel: "high", modelThinkingLevels: { "anthropic/claude-sonnet": "low" } }));
    const withStaleDefaults = piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW);
    expect(withStaleDefaults.ok).toBe(true);
    if (withStaleDefaults.ok) expect(withStaleDefaults.contract.thinkingLevel).toBe("high");
    f.proof.verifiedLaunch!.trustFlag = "approve";
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false, hold: "binding-changed" });
    f.proof.verifiedLaunch!.trustFlag = "no-approve";
    f.readiness.failures = [{ code: "model_error", observedAt: new Date(NOW).toISOString() }];
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false, hold: "runtime-not-ready" });
  });

  it("uses fresh effective RPC model and thinking selection instead of stale session history or defaults", () => {
    const f = fixture();
    f.readiness.model = { provider: "openrouter", id: "fresh-model", contextWindow: 128000 };
    Object.assign(f.readiness, { thinkingLevel: "xhigh" });
    f.node.model = "openrouter/fresh-model";

    // The immutable history and settings fixture still say Anthropic/Claude + high.
    // The current child-bound RPC selection is authoritative for the contract.
    const result = piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW);
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.contract).toMatchObject({ provider: "openrouter", model: "fresh-model", thinkingLevel: "xhigh" });
  });

  it.each([undefined, "ultra", "HIGH"]) ("holds when current RPC thinking selection is missing or invalid (%s)", thinkingLevel => {
    const f = fixture();
    Object.assign(f.readiness, { thinkingLevel });
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false });
  });

  it("requires exact session and kernel launch bindings plus fresh positive quiescence", () => {
    const f = fixture();
    f.proof.verifiedLaunch!.sessionFile += ".other";
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false, hold: "native-proof-unavailable" });
    f.proof.verifiedLaunch!.sessionFile = f.sessionFile;
    f.proof.quiescence!.observedAt = new Date(NOW - 15_001).toISOString();
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false, hold: "activity-unknown" });
    f.proof.quiescence!.observedAt = new Date(NOW - 1000).toISOString();
    writeFileSync(f.sessionFile, JSON.stringify({ type: "message", id: "not-a-session-header" }) + "\n");
    expect(piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW)).toMatchObject({ ok: false, hold: "compaction-evidence-invalid" });
  });

  it("digests settings, models and skill contents while excluding auth bytes but enforcing auth metadata", () => {
    const f = fixture();
    const initial = piLaunchConfigurationDigest(f.agentDir);
    writeFileSync(path.join(f.agentDir, "auth.json"), "first-secret-material"); chmodSync(path.join(f.agentDir, "auth.json"), 0o600);
    const withAuth = piLaunchConfigurationDigest(f.agentDir);
    writeFileSync(path.join(f.agentDir, "auth.json"), "different-secret-material");
    expect(piLaunchConfigurationDigest(f.agentDir)).toBe(withAuth);
    chmodSync(path.join(f.agentDir, "auth.json"), 0o640);
    expect(() => piLaunchConfigurationDigest(f.agentDir)).toThrow();
    chmodSync(path.join(f.agentDir, "auth.json"), 0o600);
    writeFileSync(path.join(f.agentDir, "settings.json"), JSON.stringify({ defaultProvider: "anthropic", defaultModel: "claude-sonnet", defaultThinkingLevel: "medium" }));
    expect(piLaunchConfigurationDigest(f.agentDir)).not.toBe(initial);
    const changedSettings = piLaunchConfigurationDigest(f.agentDir);
    writeFileSync(path.join(f.agentDir, "models.json"), "{} changed");
    expect(piLaunchConfigurationDigest(f.agentDir)).not.toBe(changedSettings);
    const changedModels = piLaunchConfigurationDigest(f.agentDir);
    writeFileSync(path.join(f.agentDir, "skills", "review", "SKILL.md"), "changed skill bytes\n");
    expect(piLaunchConfigurationDigest(f.agentDir)).not.toBe(changedModels);
  });

  it("accepts only a new generation/session with unchanged pinned contract and launch digest", () => {
    const f = fixture();
    const first = piRotationContract(f.sessionFile, f.readiness, f.proof, f.node, f.agentDir, NOW);
    expect(first.ok).toBe(true);
    if (!first.ok) return;
    const next = { ...first.contract, generation: "gen-two", launchId: "launch-two", nativeId: "session-two\n/session-two.jsonl", sessionFile: "/session-two.jsonl", sessionHeaderId: "session-two" };
    expect(piSuccessorMatches(first.contract, next, "gen-two")).toBe(true);
    expect(piSuccessorMatches(first.contract, { ...next, configurationDigest: "different" }, "gen-two")).toBe(false);
    expect(piSuccessorMatches(first.contract, next, "gen-three")).toBe(false);
  });
});
