import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync, readdirSync, realpathSync, statSync } from "node:fs";
import path from "node:path";
import type { NativeRecoveryCompletion } from "./native-recovery-continuation-contract.js";
import { validateNativeRecoveryCompletion } from "./native-recovery-completion.js";

const COMPLETION_EVENT = "seat.native_recovery_completed";
const sha256 = (bytes: string | Buffer) => createHash("sha256").update(bytes).digest("hex");
const jsonDigest = (value: unknown) => sha256(JSON.stringify(value));
const isRecord = (value: unknown): value is Record<string, any> => !!value && typeof value === "object" && !Array.isArray(value);
const uuid = (value: string) => /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(value);

interface BindingRow {
  rigId: string; nodeId: string; runtime: string; model: string | null; effort: string | null;
  sessionId: string; sessionName: string; status: string; startupStatus: string;
  resumeType: string; nativeId: string; generation: string;
}

function currentBinding(db: Database.Database, c: NativeRecoveryCompletion): BindingRow | null {
  const row = db.prepare(`SELECT n.rig_id rigId,n.id nodeId,n.runtime,n.model,n.effort,
      s.id sessionId,s.session_name sessionName,s.status,s.startup_status startupStatus,
      s.resume_type resumeType,s.resume_token nativeId
    FROM nodes n JOIN sessions s ON s.node_id=n.id
    WHERE n.id=? ORDER BY s.id DESC LIMIT 1`).get(c.nodeId) as Omit<BindingRow, "generation"> | undefined;
  const tenure = db.prepare("SELECT generation_uuid generation FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal DESC LIMIT 1").get(c.nodeId) as { generation: string } | undefined;
  if (!row || !tenure || row.rigId !== c.rigId || row.nodeId !== c.nodeId || row.sessionId !== c.sessionId ||
      row.sessionName !== c.sessionName || row.runtime !== c.runtime || !row.nativeId || tenure.generation !== c.generation ||
      row.startupStatus !== "ready" || !["running", "detached"].includes(row.status)) return null;
  const resumeType = c.runtime === "pi" ? "pi_session_file" : "codex_id";
  if (row.resumeType !== resumeType || sha256(row.nativeId) !== c.nativeIdentityHash) return null;
  return { ...row, generation: tenure.generation };
}

function privateRegular(file: string): Buffer {
  const before = lstatSync(file);
  if (before.isSymbolicLink() || !before.isFile() || before.size <= 0 || before.size > 256 * 1024 || before.uid !== process.getuid?.() || (before.mode & 0o077) !== 0 || realpathSync(file) !== file)
    throw new Error("private receipt rejected");
  const bytes = readFileSync(file);
  const after = statSync(file);
  if (before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size || before.mtimeMs !== after.mtimeMs || !bytes.length)
    throw new Error("unstable receipt rejected");
  return bytes;
}

function privateDirectory(directory: string): void {
  const st = lstatSync(directory);
  if (st.isSymbolicLink() || !st.isDirectory() || st.uid !== process.getuid?.() || (st.mode & 0o077) !== 0 || realpathSync(directory) !== directory)
    throw new Error("private directory rejected");
}

function rootedFile(root: string, file: string): void {
  if (!path.isAbsolute(root) || path.resolve(root) !== root || !path.isAbsolute(file) || path.resolve(file) !== file)
    throw new Error("noncanonical evidence path");
  privateDirectory(root);
  const canonicalRoot = realpathSync(root);
  if (canonicalRoot !== root) throw new Error("noncanonical evidence root");
  const relative = path.relative(root, file);
  if (!relative || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) throw new Error("evidence escaped root");
  let current = root;
  const parts = relative.split(path.sep);
  for (const part of parts.slice(0, -1)) {
    current = path.join(current, part);
    privateDirectory(current);
  }
  const actual = realpathSync(file);
  if (actual !== file) throw new Error("noncanonical evidence file");
}

function readPrivateJson(file: string, root: string): { value: Record<string, any>; bytes: Buffer } {
  rootedFile(root, file);
  const bytes = privateRegular(file);
  const value: unknown = JSON.parse(bytes.toString("utf8"));
  if (!isRecord(value)) throw new Error("invalid receipt");
  return { value, bytes };
}

function eventRows(db: Database.Database, type: string, nodeId: string): Array<{ seq: number; rig_id: string; node_id: string; payload: string }> {
  return db.prepare("SELECT seq,rig_id,node_id,payload FROM events WHERE type=? AND node_id=? ORDER BY seq")
    .all(type, nodeId) as Array<{ seq: number; rig_id: string; node_id: string; payload: string }>;
}

function parseEventPayload(row: { payload: string }): Record<string, any> {
  const value: unknown = JSON.parse(row.payload);
  if (!isRecord(value)) throw new Error("invalid event");
  return value;
}

function exactOrdinaryPiSource(db: Database.Database, c: NativeRecoveryCompletion, publicationComplete: boolean): boolean {
  const match = /^event:([1-9][0-9]*)$/.exec(c.source.ref);
  if (!match) return false;
  const seq = Number(match[1]);
  if (!Number.isSafeInteger(seq)) return false;
  const source = db.prepare("SELECT seq,rig_id,node_id,type,payload FROM events WHERE seq=?").get(seq) as
    { seq: number; rig_id: string; node_id: string; type: string; payload: string } | undefined;
  if (!source || source.type !== "seat.runner_rehost_completed" || source.node_id !== c.nodeId || source.rig_id !== c.rigId || sha256(source.payload) !== c.source.digest) return false;
  const proof = parseEventPayload(source);
  const stoppedTargetValid = proof.stoppedTargetRecovery === false || (
    proof.stoppedTargetRecovery === true && typeof proof.stoppedTargetAcceptanceReference === "string" && proof.stoppedTargetAcceptanceReference.length > 0 &&
    proof.liveIdleProof === "none" && proof.leafSource === "post_exit_session_file" && typeof proof.stoppedTargetLeaf === "string" &&
    proof.replacementCursorMatchesPostExitLeaf === true && proof.replacementCursorMatchesValidatedLeaf === true &&
    proof.appendedBytesAcceptedBecauseReplacementCursorBindsLeaf === true && proof.possibleUnpersistedTurnLoss === true &&
    isRecord(proof.historyProof) && proof.historyProof.valid === true && proof.historyProof.resultingLeaf === proof.stoppedTargetLeaf &&
    isRecord(proof.preservedSessionSnapshot) && /^[a-f0-9]{64}$/.test(proof.preservedSessionSnapshot.sha256 ?? "") &&
    Number.isSafeInteger(proof.preservedSessionSnapshot.size) && proof.preservedSessionSnapshot.size > 0
  );
  if (proof.type !== source.type || proof.nodeId !== c.nodeId || proof.rigId !== c.rigId || proof.generation !== c.generation ||
      proof.generationUnchanged !== true || proof.sessionFileUnchanged !== true || proof.guardLeftEnabled !== true ||
      proof.continuityCredit !== false || proof.deliveryOrQualificationCredit !== false || !stoppedTargetValid ||
      typeof proof.sessionFile !== "string" || sha256(proof.sessionFile) !== c.nativeIdentityHash || proof.durableModel !== currentBinding(db, c)?.model ||
      proof.launchIdAfter !== c.incarnation.runtimeLaunchId) return false;

  const begin = eventRows(db, "seat.native_recovery_publication_began", c.nodeId)
    .filter(row => row.rig_id === c.rigId && parseEventPayload(row).publicationId === c.recoveryId && parseEventPayload(row).generation === c.generation);
  const completed = eventRows(db, "seat.native_recovery_publication_completed", c.nodeId)
    .filter(row => row.rig_id === c.rigId && parseEventPayload(row).publicationId === c.recoveryId && parseEventPayload(row).generation === c.generation);
  const unknown = eventRows(db, "seat.native_recovery_publication_unknown", c.nodeId)
    .filter(row => row.rig_id === c.rigId && parseEventPayload(row).generation === c.generation && parseEventPayload(row).completionEventSeq === seq);
  return begin.length === 1 && unknown.length === 0 && (publicationComplete ? completed.length === 1 : completed.length === 0);
}

function privateAttemptRoot(root: string, c: NativeRecoveryCompletion, fingerprinted: boolean): { base: string; directory: string } {
  if (!uuid(c.recoveryId)) throw new Error("invalid attempt id");
  if (!path.isAbsolute(root) || path.resolve(root) !== root) throw new Error("invalid evidence root");
  privateDirectory(root);
  const base = path.join(root, jsonDigest([c.nodeId, c.generation]));
  privateDirectory(base);
  if (!fingerprinted) return { base, directory: path.join(base, c.recoveryId) };
  const matches = requireReaddir(base).filter(name => /^[a-f0-9]{64}-[a-f0-9-]{36}$/.test(name) && name.endsWith(`-${c.recoveryId}`));
  if (matches.length !== 1) throw new Error("attempt directory ambiguous");
  return { base, directory: path.join(base, matches[0]!) };
}

function requireReaddir(directory: string): string[] {
  // Kept local to make every root/child validation explicit at the call site.
  return readdirSync(directory);
}

function marker(file: string, root: string, recoveryId: string): Record<string, any> {
  const value = readPrivateJson(file, root).value;
  if (value.attemptId !== recoveryId) throw new Error("marker identity mismatch");
  return value;
}

function exactPrivateSource(c: NativeRecoveryCompletion, options: { publicationComplete: boolean; roots: { piDetached: string; codex: string } }): boolean {
  const pi = c.producer === "pi-detached-resume";
  const root = pi ? options.roots.piDetached : options.roots.codex;
  const { directory } = privateAttemptRoot(root, c, !pi);
  privateDirectory(directory);
  const receiptPath = path.join(directory, pi ? "completed.json" : "completed.json");
  if (c.source.ref !== receiptPath) return false;
  const { value: receipt, bytes } = readPrivateJson(receiptPath, root);
  const originalEvidence = readPrivateJson(path.join(directory, "began.json"), root);
  const original = originalEvidence.value;
  if (sha256(bytes) !== c.source.digest || receipt.ok !== true || receipt.attemptId !== c.recoveryId ||
      receipt.nodeId !== c.nodeId || receipt.sessionName !== c.sessionName || receipt.generation !== c.generation ||
      receipt.generationUnchanged !== true || receipt.custodyPreserved !== true || receipt.guardLeftEnabled !== true ||
      receipt.authorityRepaired !== false || receipt.nativeIdHash !== c.nativeIdentityHash ||
      original.attemptId !== c.recoveryId || original.nativeIdHash !== c.nativeIdentityHash ||
      receipt.receiptPath !== path.join(directory, "began.json")) return false;
  if (pi) {
    let nativeProof: Record<string, any>;
    try { nativeProof = JSON.parse(receipt.nativeFingerprint); } catch { return false; }
    if (receipt.runtime !== "pi" || receipt.sessionId !== c.sessionId || receipt.supervisorLaunchId !== c.incarnation.supervisorLaunchId ||
        original.protocol !== "pi-detached-resume-v1" || !c.incarnation.runtimeLaunchId || nativeProof.launchId !== c.incarnation.runtimeLaunchId ||
        !Array.isArray(nativeProof.pi) || !Number.isSafeInteger(nativeProof.pi[0]) || nativeProof.pi[0] !== c.incarnation.native.pid ||
        !Array.isArray(nativeProof.runner) || !Number.isSafeInteger(nativeProof.runner[1]) || nativeProof.runner[1] !== c.incarnation.supervisor?.pid) return false;
  } else {
    if (receipt.runtime !== "codex" || receipt.supervisorLaunchId !== c.incarnation.supervisorLaunchId ||
        !/^[a-f0-9]{64}$/.test(receipt.nativeFingerprintAfter ?? "")) return false;
    if (c.producer === "codex-rehost" && original.protocol !== "codex-same-generation-rehost-v1") return false;
    if (c.producer === "codex-detached-resume" && original.protocol !== "codex-detached-same-generation-resume-v1") return false;
    if (c.producer === "codex-stopped-recovery") {
      if (!new Set(["codex-same-generation-rehost-v1", "codex-detached-same-generation-resume-v1"]).has(original.protocol) ||
          receipt.recoveryProtocol !== "codex-stopped-recovery-v1" || receipt.beganSha256 !== sha256(originalEvidence.bytes)) return false;
      const unknown = readPrivateJson(path.join(directory, "unknown.json"), root).value;
      const recovery = readPrivateJson(path.join(directory, "recovery-began.json"), root).value;
      const expectedUnknown = original.protocol === "codex-detached-same-generation-resume-v1" ? "codex_rehost_recovery_absence" : "codex_rehost_stop_unknown";
      if ((unknown.attemptId !== c.recoveryId && !(original.protocol === "codex-detached-same-generation-resume-v1" && unknown.attemptId === undefined)) ||
          unknown.effectAttempted !== true || unknown.blindRetryAllowed !== false || unknown.code !== expectedUnknown ||
          recovery.attemptId !== c.recoveryId || recovery.protocol !== "codex-stopped-recovery-v1" || recovery.beganSha256 !== receipt.beganSha256) return false;
    } else if (receipt.recoveryProtocol !== undefined || receipt.beganSha256 !== undefined) return false;
  }
  const began = marker(path.join(directory, "completion-publication-began.json"), root, c.recoveryId);
  const completedPath = path.join(directory, "completion-publication-completed.json");
  const unknownPath = path.join(directory, "completion-publication-unknown.json");
  const completedExists = existsSync(completedPath);
  if (existsSync(unknownPath)) return false;
  if (options.publicationComplete) {
    if (!completedExists) return false;
    marker(completedPath, root, c.recoveryId);
  } else if (completedExists) return false;
  return began.blindRetryAllowed === false;
}

/** Verifies a completion against the current retained occupant and the producer's
 * original durable success evidence. It grants no authority and performs no writes. */
export function nativeRecoverySourceValid(
  db: Database.Database,
  completion: NativeRecoveryCompletion,
  options: { publicationComplete: boolean; roots: { piDetached: string; codex: string } },
): boolean {
  try {
    const c = validateNativeRecoveryCompletion(completion);
    if (!options || typeof options.publicationComplete !== "boolean" || !options.roots ||
        typeof options.roots.piDetached !== "string" || typeof options.roots.codex !== "string") return false;
    if (!currentBinding(db, c)) return false;
    const ordinaryPi = c.producer === "pi-runner-rehost";
    const sourceValid = ordinaryPi ? exactOrdinaryPiSource(db, c, options.publicationComplete) : exactPrivateSource(c, options);
    if (!sourceValid) return false;
    if (!options.publicationComplete) return true;
    const rows = db.prepare(`SELECT rig_id,node_id,payload FROM events WHERE type=? AND node_id=? AND json_extract(payload,'$.completion.recoveryId')=?`)
      .all(COMPLETION_EVENT, c.nodeId, c.recoveryId) as Array<{ rig_id: string; node_id: string; payload: string }>;
    if (rows.length !== 1 || rows[0]!.rig_id !== c.rigId || rows[0]!.node_id !== c.nodeId) return false;
    const recorded = parseEventPayload(rows[0]!).completion;
    return JSON.stringify(recorded) === JSON.stringify(c);
  } catch {
    // Malformed JSON, absent files, permission/path failures, and poisoned rows hold.
    return false;
  }
}
