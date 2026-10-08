import { createHash, randomUUID } from "node:crypto";
import { chmodSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { EventBus } from "../src/domain/event-bus.js";
import { NativeRecoveryCompletionStore } from "../src/domain/native-recovery-completion.js";
import { nativeRecoverySourceValid } from "../src/domain/native-recovery-source-proof.js";
import type { NativeRecoveryCompletion } from "../src/domain/native-recovery-continuation-contract.js";
import type Database from "better-sqlite3";

const hash = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const id = "lead@xv";
const rig = "rig-xv";
const session = "session-lead";
const name = "lead@xv";
const generation = "generation-lead-1";
const nativeId = "/private/native/history.jsonl";
const runtimeLaunchId = "pi-launch-current";
const recoveryId = randomUUID();

let db: Database.Database;
let root: string;
let dirs: string[] = [];
let bus: EventBus;
let store: NativeRecoveryCompletionStore;

function freshDb() {
  const directory = mkdtempSync(path.join(tmpdir(), "native-source-proof-db-"));
  dirs.push(directory);
  const database = createDb(path.join(directory, "db"));
  migrate(database, ALL_MIGRATIONS);
  database.prepare("INSERT INTO rigs(id,name) VALUES(?,?)").run(rig, "xv");
  database.prepare("INSERT INTO nodes(id,rig_id,logical_id,runtime,model,cwd,effort,codex_config_profile) VALUES(?,?,?,?,?,?,?,?)")
    .run(id, rig, "lead", "pi", "openrouter/openai/gpt-6-luna", "/work", "high", null);
  database.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind,native_session_id_at_boot) VALUES(?,?,?,?,?,?)")
    .run("tenure-1", id, 1, generation, "ordinary", nativeId);
  database.prepare("INSERT INTO sessions(id,node_id,session_name,status,startup_status,resume_type,resume_token) VALUES(?,?,?,?,?,?,?)")
    .run(session, id, name, "running", "ready", "pi_session_file", nativeId);
  return database;
}

function baseCompletion(overrides: Partial<NativeRecoveryCompletion> = {}): NativeRecoveryCompletion {
  return {
    schema: "native-recovery-completion.v1", recoveryId, producer: "pi-runner-rehost", rigId: rig, nodeId: id,
    sessionId: session, sessionName: name, generation, runtime: "pi", nativeIdentityHash: hash(nativeId),
    configurationDigest: "c".repeat(64), completedAt: 1000,
    source: { ref: "event:1", digest: "d".repeat(64) },
    incarnation: { key: "key-1", runtimeLaunchId, native: { pid: 222, startFingerprint: "a".repeat(64) } },
    custodyPreserved: true, generationUnchanged: true, ...overrides,
  };
}

function emitOrdinaryPiSource(c: NativeRecoveryCompletion, stoppedTarget = false) {
  bus.emit({ type: "seat.native_recovery_publication_began", rigId: c.rigId, nodeId: c.nodeId,
    sessionName: c.sessionName, generation: c.generation, publicationId: c.recoveryId, blindRetryAllowed: false } as never);
  const source = bus.emit({ type: "seat.runner_rehost_completed", rigId: c.rigId, nodeId: c.nodeId,
    sessionName: c.sessionName, generation: c.generation, generationUnchanged: true, sessionFile: nativeId,
    sessionFileUnchanged: true, durableModel: "openrouter/openai/gpt-6-luna", launchIdAfter: runtimeLaunchId, guardLeftEnabled: true,
    continuityCredit: false, deliveryOrQualificationCredit: false, stoppedTargetRecovery: stoppedTarget,
    ...(stoppedTarget ? { stoppedTargetAcceptanceReference: "acceptance-1", liveIdleProof: "none", leafSource: "post_exit_session_file",
      stoppedTargetLeaf: "leaf-2", replacementCursorMatchesPostExitLeaf: true, replacementCursorMatchesValidatedLeaf: true,
      historyProof: { valid: true, resultingLeaf: "leaf-2" }, preservedSessionSnapshot: { path: "/private/snapshot", sha256: "e".repeat(64), size: 4 },
      appendedBytesAcceptedBecauseReplacementCursorBindsLeaf: true, possibleUnpersistedTurnLoss: true } : {}) } as never);
  return { ...c, source: { ref: `event:${source.seq}`, digest: hash((db.prepare("SELECT payload FROM events WHERE seq=?").get(source.seq) as { payload: string }).payload) } };
}

function completeOrdinaryPublication(c: NativeRecoveryCompletion) {
  bus.emit({ type: "seat.native_recovery_publication_completed", rigId: c.rigId, nodeId: c.nodeId,
    sessionName: c.sessionName, generation: c.generation, publicationId: c.recoveryId } as never);
  store.record(c);
}

function privateRoot(kind: "piDetached" | "codex") {
  const value = mkdtempSync(path.join(tmpdir(), `native-proof-${kind}-`));
  chmodSync(value, 0o700);
  dirs.push(value);
  return realpathSync(value);
}

function privateCompletion(producer: "pi-detached-resume" | "codex-rehost" | "codex-stopped-recovery" | "codex-detached-resume") {
  const isPi = producer === "pi-detached-resume";
  const stopped = producer === "codex-stopped-recovery";
  const detachedCodex = producer === "codex-detached-resume";
  if (!isPi) db.prepare("UPDATE nodes SET runtime='codex',codex_config_profile='test-profile' WHERE id=?").run(id);
  db.prepare("UPDATE sessions SET resume_type=? WHERE id=?").run(isPi ? "pi_session_file" : "codex_id", session);
  const evidenceRoot = privateRoot(isPi ? "piDetached" : "codex");
  const bindingRoot = path.join(evidenceRoot, hash(JSON.stringify([id, generation])));
  const attemptId = randomUUID();
  const attemptDirectory = path.join(bindingRoot, isPi ? attemptId : `${"b".repeat(64)}-${attemptId}`);
  mkdirSync(attemptDirectory, { recursive: true, mode: 0o700 });
  chmodSync(bindingRoot, 0o700);
  chmodSync(attemptDirectory, 0o700);
  const beganPath = path.join(attemptDirectory, "began.json");
  const completedPath = path.join(attemptDirectory, "completed.json");
  const supervisorLaunchId = "supervisor-launch-1";
  const nativeFingerprint = "a".repeat(64);
  const piNativeProof = JSON.stringify({ pane: "%12", runner: [333, 221], pi: [222, 333], launchId: "pi-runner-launch-1", genSources: ["runner", "pi"] });
  const codexNativeProof = "d".repeat(64);
  const backup = { path: path.join(attemptDirectory, "transcript.jsonl"), sha256: "9".repeat(64), size: 123 };
  const originalProtocol = isPi ? "pi-detached-resume-v1" : detachedCodex ? "codex-detached-same-generation-resume-v1" : "codex-same-generation-rehost-v1";
  const originalBytes = Buffer.from(JSON.stringify({ protocol: originalProtocol, attemptId, nativeIdHash: hash(nativeId),
    ...(stopped || detachedCodex ? { nativeFingerprint: "8".repeat(64) } : {}) }) + "\n");
  const beganSha256 = hash(originalBytes);
  const receipt = {
    ok: true, runtime: isPi ? "pi" : "codex", nodeId: id, ...(isPi ? { sessionId: session } : {}), sessionName: name,
    generation, generationUnchanged: true, attemptId, receiptPath: beganPath, backup, nativeIdHash: hash(nativeId),
    ...(!isPi ? { nativeFingerprintBefore: "8".repeat(64) } : {}),
    supervisorLaunchId, ...(isPi ? { nativeFingerprint: piNativeProof } : { nativeFingerprintAfter: codexNativeProof }),
    custodyPreserved: true, guardLeftEnabled: true, authorityRepaired: false,
    ...(stopped ? { recoveryProtocol: "codex-stopped-recovery-v1", beganSha256, custodyAfter: {} } : {}),
  };
  writeFileSync(beganPath, originalBytes, { mode: 0o600 });
  const bytes = Buffer.from(JSON.stringify(receipt) + "\n");
  writeFileSync(completedPath, bytes, { mode: 0o600 });
  if (stopped) {
    writeFileSync(path.join(attemptDirectory, "unknown.json"), JSON.stringify({ attemptId, effectAttempted: true, blindRetryAllowed: false, code: "codex_rehost_stop_unknown" }), { mode: 0o600 });
    writeFileSync(path.join(attemptDirectory, "recovery-began.json"), JSON.stringify({ attemptId, protocol: "codex-stopped-recovery-v1", beganSha256 }), { mode: 0o600 });
  }
  writeFileSync(path.join(attemptDirectory, "completion-publication-began.json"), JSON.stringify({ attemptId, blindRetryAllowed: false }), { mode: 0o600 });
  const c: NativeRecoveryCompletion = {
    schema: "native-recovery-completion.v1", recoveryId: attemptId, producer, rigId: rig, nodeId: id,
    sessionId: session, sessionName: name, generation, runtime: isPi ? "pi" : "codex", nativeIdentityHash: hash(nativeId),
    configurationDigest: "c".repeat(64), completedAt: 1000, source: { ref: completedPath, digest: hash(bytes) },
    incarnation: { key: "key-1", ...(isPi ? { runtimeLaunchId: "pi-runner-launch-1" } : {}), supervisorLaunchId,
      supervisor: { pid: 221, startFingerprint: "b".repeat(64) },
      native: { pid: 222, startFingerprint: nativeFingerprint } }, custodyPreserved: true, generationUnchanged: true,
  };
  return { completion: c, evidenceRoot, attemptDirectory, attemptId };
}

beforeEach(() => {
  dirs = [];
  db = freshDb();
  bus = new EventBus(db);
  store = new NativeRecoveryCompletionStore(bus);
  root = privateRoot("codex");
});
afterEach(() => {
  db.close();
  for (const directory of dirs.reverse()) {
    try { rmSync(directory, { recursive: true, force: true }); } catch { /* best-effort test cleanup */ }
  }
});

describe("native recovery source verifier", () => {
  it("accepts the exact ordinary Pi receipt during its callback, then requires completed publication for continuation", () => {
    const c = emitOrdinaryPiSource(baseCompletion());
    expect(nativeRecoverySourceValid(db, c, { publicationComplete: false, roots: { piDetached: root, codex: root } })).toBe(true);
    expect(nativeRecoverySourceValid(db, c, { publicationComplete: true, roots: { piDetached: root, codex: root } })).toBe(false);
    completeOrdinaryPublication(c);
    expect(nativeRecoverySourceValid(db, c, { publicationComplete: true, roots: { piDetached: root, codex: root } })).toBe(true);
  });

  it("rejects wrong source bytes, a crossed generation, and a publication UNKNOWN", () => {
    const c = emitOrdinaryPiSource(baseCompletion());
    expect(nativeRecoverySourceValid(db, { ...c, source: { ...c.source, digest: "0".repeat(64) } },
      { publicationComplete: false, roots: { piDetached: root, codex: root } })).toBe(false);
    expect(nativeRecoverySourceValid(db, { ...c, generation: "other-generation" },
      { publicationComplete: false, roots: { piDetached: root, codex: root } })).toBe(false);
    bus.emit({ type: "seat.native_recovery_publication_unknown", rigId: c.rigId, nodeId: c.nodeId,
      sessionName: c.sessionName, generation: c.generation, completionEventSeq: Number(c.source.ref.slice(6)), blindRetryAllowed: false } as never);
    expect(nativeRecoverySourceValid(db, c, { publicationComplete: false, roots: { piDetached: root, codex: root } })).toBe(false);
  });

  it("accepts stopped-target Pi recovery only with its completed leaf and preserved-snapshot proof", () => {
    const c = emitOrdinaryPiSource(baseCompletion(), true);
    expect(nativeRecoverySourceValid(db, c, { publicationComplete: false, roots: { piDetached: root, codex: root } })).toBe(true);
    const seq = Number(c.source.ref.slice(6));
    const source = db.prepare("SELECT payload FROM events WHERE seq=?").get(seq) as { payload: string };
    const event = JSON.parse(source.payload);
    event.replacementCursorMatchesValidatedLeaf = false;
    db.prepare("UPDATE events SET payload=? WHERE seq=?").run(JSON.stringify(event), seq);
    const altered = { ...c, source: { ...c.source, digest: hash(JSON.stringify(event)) } };
    expect(nativeRecoverySourceValid(db, altered, { publicationComplete: false, roots: { piDetached: root, codex: root } })).toBe(false);
  });

  it.each(["pi-detached-resume", "codex-rehost", "codex-stopped-recovery", "codex-detached-resume"] as const)("verifies a bounded private %s receipt and rejects symlink or mode violations", producer => {
    const fixture = privateCompletion(producer);
    const roots = { piDetached: root, codex: root };
    roots[producer === "pi-detached-resume" ? "piDetached" : "codex"] = fixture.evidenceRoot;
    const options = { publicationComplete: false, roots };
    expect(nativeRecoverySourceValid(db, fixture.completion, options)).toBe(true);
    expect(nativeRecoverySourceValid(db, fixture.completion, { ...options, publicationComplete: true })).toBe(false);
    writeFileSync(path.join(fixture.attemptDirectory, "completion-publication-completed.json"), JSON.stringify({ attemptId: fixture.attemptId }), { mode: 0o600 });
    expect(nativeRecoverySourceValid(db, fixture.completion, { ...options, publicationComplete: true })).toBe(false);
    store.record(fixture.completion);
    expect(nativeRecoverySourceValid(db, fixture.completion, { ...options, publicationComplete: true })).toBe(true);
    chmodSync(path.join(fixture.attemptDirectory, "completed.json"), 0o644);
    expect(nativeRecoverySourceValid(db, fixture.completion, options)).toBe(false);
  });
});
