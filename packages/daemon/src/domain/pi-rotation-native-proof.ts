import { createHash } from "node:crypto";
import { lstatSync, readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import type { NativeModelWindow } from "./model-window.js";

/** Inputs are observations already obtained by the daemon. This module does not probe or mutate. */
export interface PiRotationNode {
  runtime: string | null;
  model: string | null;
  generation: string | null;
  sessionStatus: string;
  startupStatus: string;
  resumeType: string | null;
  resumeToken: string | null;
  launchPosture: "floor" | "full_bypass" | null;
}
export interface PiRotationReadiness {
  ready: boolean;
  launchId?: string;
  generation?: string;
  sessionFile?: string;
  lastEntryId?: string;
  model: NativeModelWindow | null;
  observedAt: string;
  failures: Array<{ code: string; observedAt: string }>;
}
export interface PiRotationNativeProof {
  state: "present" | "absent";
  generation: string;
  launchId: string | null;
  fingerprint: string;
  lastEntryId?: string | null;
  quiescence?: { settled: boolean | null; observedAt: string | null };
  /** Kernel-verified child process binding; never sourced from sidecar/request fields. */
  verifiedLaunch?: PiVerifiedLaunch;
}
export interface PiVerifiedLaunch {
  generation: string;
  launchId: string;
  sessionFile: string;
  pid: number;
  startFingerprint: string;
  trustFlag: "approve" | "no-approve";
}
export interface PiRotationContract {
  runtime: "pi";
  provider: string;
  model: string;
  thinkingLevel: string;
  trust: "approve" | "no-approve";
  agentDir: string;
  generation: string;
  launchId: string;
  nativeId: string;
  sessionFile: string;
  sessionHeaderId: string;
  sessionSha256: string;
  configurationDigest: string;
}
export type PiRotationResult =
  | { ok: true; contract: PiRotationContract }
  | { ok: false; hold: string };

const sha256 = (data: string | Buffer): string => createHash("sha256").update(data).digest("hex");
const isRecord = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const regularFile = (file: string): boolean => { const st = lstatSync(file); return st.isFile() && !st.isSymbolicLink(); };

function sessionFacts(file: string): { headerId: string; provider: string; model: string; thinkingLevel: string; hash: string } | null {
  try {
    if (!path.isAbsolute(file) || !regularFile(file)) return null;
    const bytes = readFileSync(file);
    const text = bytes.toString("utf8");
    if (!text.endsWith("\n")) return null;
    const rows: unknown[] = [];
    for (const line of text.split("\n").slice(0, -1)) {
      if (!line) return null;
      rows.push(JSON.parse(line));
    }
    const header = rows[0];
    if (!isRecord(header) || header.type !== "session" || typeof header.id !== "string" || !header.id) return null;
    let model: { provider: string; id: string } | null = null;
    let thinking: string | null = null;
    for (const row of rows) {
      if (!isRecord(row)) continue;
      if (row.type === "model_change" && typeof row.provider === "string" && typeof row.modelId === "string") {
        model = { provider: row.provider, id: row.modelId };
      }
      if (row.type === "thinking_level_change" && typeof row.thinkingLevel === "string") thinking = row.thinkingLevel;
    }
    if (!model || !thinking) return null;
    return { headerId: header.id, provider: model.provider, model: model.id, thinkingLevel: thinking, hash: sha256(bytes) };
  } catch { return null; }
}

function configurationDigest(agentDir: string): string {
  if (!path.isAbsolute(agentDir) || lstatSync(agentDir).isSymbolicLink() || !lstatSync(agentDir).isDirectory()) {
    throw new Error("Pi agent directory is not a real directory");
  }
  const settingsPath = path.join(agentDir, "settings.json");
  const modelsPath = path.join(agentDir, "models.json");
  if (!regularFile(settingsPath) || !regularFile(modelsPath)) throw new Error("Pi launch configuration is incomplete");
  const settingsBytes = readFileSync(settingsPath);
  const modelsBytes = readFileSync(modelsPath);
  const settings = JSON.parse(settingsBytes.toString("utf8")) as unknown;
  if (!isRecord(settings)) throw new Error("Pi settings are invalid");
  const skillsRoot = path.join(agentDir, "skills");
  const skills: Array<{ path: string; sha256: string; mode: number }> = [];
  const walk = (dir: string, relative: string): void => {
    let entries;
    try { entries = readdirSync(dir, { withFileTypes: true }); }
    catch (e) { if ((e as NodeJS.ErrnoException).code === "ENOENT" && relative === "") return; throw e; }
    entries.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
    for (const entry of entries) {
      const abs = path.join(dir, entry.name), rel = path.posix.join(relative, entry.name);
      const st = lstatSync(abs);
      if (st.isSymbolicLink()) throw new Error("Pi skills tree contains a symbolic link");
      if (st.isDirectory()) walk(abs, rel);
      else if (st.isFile()) skills.push({ path: rel, sha256: sha256(readFileSync(abs)), mode: st.mode & 0o777 });
      else throw new Error("Pi skills tree contains a non-regular entry");
    }
  };
  walk(skillsRoot, "");
  skills.sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
  const authPath = path.join(agentDir, "auth.json");
  let auth: { present: false } | { present: true; owner: number; mode: number } = { present: false };
  try {
    const st = lstatSync(authPath);
    if (!st.isFile() || st.isSymbolicLink() || (st.mode & 0o777) !== 0o600) throw new Error("Pi auth metadata is unsafe");
    auth = { present: true, owner: st.uid, mode: st.mode & 0o777 };
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  return sha256(JSON.stringify({ settings: sha256(settingsBytes), models: sha256(modelsBytes), skills, auth }));
}

/** Digest only launch-affecting files and safe auth metadata. auth.json bytes are never read. */
export function piLaunchConfigurationDigest(agentDir: string): string { return configurationDigest(agentDir); }

/** Derive a Pi rotation contract from a bound session, sidecar, and native proof. */
export function piRotationContract(
  sessionFile: string,
  readiness: PiRotationReadiness,
  proof: PiRotationNativeProof,
  node: PiRotationNode,
  agentDir: string,
  nowMs = Date.now(),
): PiRotationResult {
  if (node.runtime !== "pi" || !node.generation || node.sessionStatus !== "running" || node.startupStatus !== "ready"
    || node.resumeType !== "pi_session_file" || !node.resumeToken || node.resumeToken !== sessionFile) return { ok: false, hold: "identity-unavailable" };
  if (!readiness.ready || readiness.failures.length !== 0 || readiness.sessionFile !== sessionFile
    || !readiness.launchId || readiness.launchId !== proof.launchId || readiness.generation !== node.generation
    || readiness.model === null || !Number.isFinite(Date.parse(readiness.observedAt))) return { ok: false, hold: "runtime-not-ready" };
  if (proof.state !== "present" || proof.generation !== node.generation || !proof.launchId
    || proof.lastEntryId !== readiness.lastEntryId) return { ok: false, hold: "native-proof-unavailable" };
  const quiescence = proof.quiescence;
  const quiescenceAt = quiescence?.observedAt ? Date.parse(quiescence.observedAt) : NaN;
  if (quiescence?.settled !== true || !Number.isFinite(quiescenceAt) || nowMs < quiescenceAt || nowMs - quiescenceAt > 15_000) {
    return { ok: false, hold: "activity-unknown" };
  }
  const verifiedLaunch = proof.verifiedLaunch;
  if (!verifiedLaunch || !Number.isSafeInteger(verifiedLaunch.pid) || verifiedLaunch.pid <= 0
    || !verifiedLaunch.startFingerprint || verifiedLaunch.generation !== node.generation
    || verifiedLaunch.launchId !== proof.launchId || verifiedLaunch.sessionFile !== sessionFile
    || (verifiedLaunch.trustFlag !== "approve" && verifiedLaunch.trustFlag !== "no-approve")) {
    return { ok: false, hold: "native-proof-unavailable" };
  }
  if (!node.launchPosture) return { ok: false, hold: "binding-changed" };
  const trust = node.launchPosture === "full_bypass" ? "approve" : "no-approve";
  if (verifiedLaunch.trustFlag !== trust) return { ok: false, hold: "binding-changed" };
  const facts = sessionFacts(sessionFile);
  if (!facts) return { ok: false, hold: "compaction-evidence-invalid" };
  const window = readiness.model;
  if (facts.provider !== window.provider || facts.model !== window.id || node.model !== `${window.provider}/${window.id}`) {
    return { ok: false, hold: "binding-changed" };
  }
  let digest: string;
  let settings: Record<string, unknown>;
  try {
    digest = configurationDigest(agentDir);
    settings = JSON.parse(readFileSync(path.join(agentDir, "settings.json"), "utf8")) as Record<string, unknown>;
  } catch { return { ok: false, hold: "binding-changed" }; }
  const provider = settings.defaultProvider, model = settings.defaultModel;
  if (provider !== facts.provider || model !== facts.model) return { ok: false, hold: "binding-changed" };
  const modelLevels = isRecord(settings.modelThinkingLevels) ? settings.modelThinkingLevels : {};
  const perModel = modelLevels[`${facts.provider}/${facts.model}`];
  const configuredThinking = perModel ?? settings.defaultThinkingLevel;
  if (typeof configuredThinking !== "string" || configuredThinking !== facts.thinkingLevel) return { ok: false, hold: "binding-changed" };
  return { ok: true, contract: {
    runtime: "pi", provider: facts.provider, model: facts.model, thinkingLevel: facts.thinkingLevel, trust,
    agentDir, generation: node.generation, launchId: proof.launchId, nativeId: `${facts.headerId}\n${sessionFile}`,
    sessionFile, sessionHeaderId: facts.headerId, sessionSha256: facts.hash, configurationDigest: digest,
  } };
}

/** Require a genuinely fresh native session while preserving the prior pinned contract. */
export function piSuccessorMatches(
  predecessor: PiRotationContract,
  successor: PiRotationContract,
  expectedGeneration: string,
): boolean {
  return predecessor.runtime === "pi" && successor.runtime === "pi"
    && successor.generation === expectedGeneration && successor.generation !== predecessor.generation
    && successor.launchId !== predecessor.launchId && successor.nativeId !== predecessor.nativeId
    && successor.sessionFile !== predecessor.sessionFile && successor.sessionHeaderId !== predecessor.sessionHeaderId
    && successor.provider === predecessor.provider && successor.model === predecessor.model
    && successor.thinkingLevel === predecessor.thinkingLevel && successor.trust === predecessor.trust
    && successor.agentDir === predecessor.agentDir && successor.configurationDigest === predecessor.configurationDigest;
}
