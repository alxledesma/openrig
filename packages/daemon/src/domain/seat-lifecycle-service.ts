import type { CodexRehostInput, CodexRehostResult } from "./codex-rehost.js";
import type { NativeProcessLister } from "./native-process-lineage.js";
import type { NativeProcessRow } from "./native-process-lineage.js";
import type Database from "better-sqlite3";
import type { RigRepository } from "./rig-repository.js";
import type { SessionRegistry } from "./session-registry.js";
import type { EventBus } from "./event-bus.js";
import type { TmuxAdapter, SessionProbe } from "../adapters/tmux.js";
import type { NodeInventoryEntry, PersistedEvent } from "./types.js";
import { deriveCanonicalFromEntry, getNodeInventory } from "./node-inventory.js";
import { deriveSessionName, parseSessionName } from "./session-name.js";
import type { NodeLauncher } from "./node-launcher.js";
import type { StartupOrchestrator } from "./startup-orchestrator.js";
import type { RuntimeAdapter, ResolvedStartupFile } from "./runtime-adapter.js";
import type { ProjectionEntry, ProjectionPlan } from "./projection-planner.js";
import type { StartupAction } from "./types.js";
import { resolveStartupProof } from "./startup-resolver.js";
import { reanchorBuiltinStartupFile, reanchorShippedProjectionEntry } from "./builtin-startup-files.js";
import type { OccupantInvalidator } from "./occupant-invalidator.js";
import { rebindAndVerifyPaneIdentity } from "./seat-attention-reconciler.js";
import { observeSolePane } from "./pane-binding-observation.js";
import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { lstatSync, mkdirSync, readFileSync, realpathSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, resolve } from "node:path";
import { promisify } from "node:util";
import { parse as parseToml } from "smol-toml";
import { NativePermissionStore } from "./native-permission-store.js";
import { validateNativePermissionSelection, unresolvedClaudePermissionModes } from "./native-permission-selection.js";
import type { LegacyPiModuleBinding, LegacyPiTargetIdentity, LegacyPiWitnessBinding, LegacyPiWitnessResult } from "./legacy-pi-native-witness.js";
import { classifyNodeInspectorConfiguration, qualifiesDefaultPrivateInspector } from "./pi-installation-module-resolver.js";
import { entryShebang, filePredatesStart } from "./legacy-pi-native-provenance.js";
import { fileURLToPath as fileUrlToPath } from "node:url";
import { pathToFileURL } from "node:url";

/**
 * S5 (OPR.0.5.4.7) — the seat-lifecycle verb surface: set-model, single-seat stop,
 * dead-session clean. One coherent design (KI-5.3-9):
 *
 *   - ONE seat-resolution path shared by all three verbs (the SeatStatusService
 *     findMatches semantics: parseSessionName greedy first-@ rig; canonical-name or
 *     logical-id match; ambiguity returns the match list) — never a per-verb resolver.
 *   - Every mutation is transactional and persists its audit event in the SAME
 *     transaction (node.model_changed / session.stopped / session.cleaned).
 *   - Every refusal names what was actually checked; an indeterminate tmux probe is
 *     a refusal, never a guess (the S1 error bar applied at birth).
 */

const SEAT_LOOKUP_GUIDANCE = "List seats with: rig ps --nodes";

/** Terminal session statuses — rows the clean verb must NOT touch (they already
 *  record an ended tenancy; the vocabulary is shared with seat-handover-service
 *  and the watchdog's TERMINAL_SESSION_STATUSES). */
const TERMINAL_SESSION_STATUSES = new Set(["superseded", "detached", "exited"]);

export interface SeatLifecycleDeps {
  db: Database.Database;
  rigRepo: RigRepository;
  sessionRegistry: SessionRegistry;
  eventBus: EventBus;
  tmuxAdapter: TmuxAdapter;
  listProcesses?: NativeProcessLister;
  nodeLauncher?: NodeLauncher;
  startupOrchestrator?: StartupOrchestrator;
  runtimeAdapters?: Record<string, RuntimeAdapter>;
  occupantInvalidator?: OccupantInvalidator;
  activityOracle?: { declareOccupantSwap(nodeId: string, generation: string): void };
  /** Test seam; production reads the installed Codex profile directory. */
  codexProfileHome?: string;
  /** Test seam; production asks Codex's own loader to accept the named profile. */
  codexProfileProbe?: (profile: string) => Promise<unknown>;
  /** Same-generation Pi runner rehost. Every seam is REQUIRED for the operation
   *  and its absence is a refusal, never a degraded path: the shipped resume
   *  primitive, the shipped pi native prover, the shipped runner-state parser,
   *  the shipped session-file existence check, and the session-file tail reader
   *  used for the idle witness. */
  piResume?: PiRehostResume;
  /** Guarded same-generation continuation for an exactly detached Pi occupant. */
  piDetachedResume?: Pick<import("./pi-detached-resume.js").PiDetachedResume, "run">;
  codexRehost?: { rehost(input: CodexRehostInput): Promise<CodexRehostResult>; recoverStopped?(input: import("./codex-rehost.js").CodexStoppedRecoveryInput): Promise<CodexRehostResult>; resumeDetached?(input: import("./codex-rehost.js").CodexDetachedResumeInput): Promise<CodexRehostResult> };
  piProve?: (session: string) => Promise<PiRehostProof | null>;
  piRunnerState?: (sessionName: string) => PiRehostRunnerState | null;
  piSessionFileExists?: (path: string) => boolean;
  /** Private daemon-owned directory for stopped-target recovery snapshots. */
  piRecoverySnapshotDirectory?: string;
  /** Bounded identity digest of the session file the runner will resume. Optional so a
   *  caller may supply its own reader; the default hashes the real bytes. Rehost
   *  refuses rather than fabricating a prefix it could not read. */
  piSessionFileDigestPrefix?: (sessionFile: string) => string | null;
  /** Authoritative pane ROOT pid for a node, resolved from its registered tmux pane.
   *  Optional seam; the default reads the binding and asks the tmux adapter. */
  paneRootPid?: (nodeId: string) => Promise<number | null>;
  piSessionTailEntryId?: (path: string) => string | null;
  /** SIGTERM delivery to ONE verified runner pid. Never a terminal keystroke. */
  killNativeProcess?: (pid: number) => void;
  /** EXPLICIT legacy native-witness option only. The daemon-owned collector: it
   *  builds the witness from the module binding derived here from the exact census
   *  commands and the bounded session-file tail. Its absence is a refusal on the
   *  legacy path and changes nothing on the ordinary path. */
  legacyPiWitness?: LegacyPiWitnessSeam;
  /** The CURRENT fallback-capable runner entry the same-file resume launches.
   *  The legacy path refuses without it: it must be able to prove that the
   *  replacement is the current runner carrying the missing-entry fallback. */
  currentPiRunnerEntryPath?: string | null;
  /** Daemon-owned resolver seam for the Pi CACHED-CHUNK module exporting
   *  AgentSessionRuntime/AgentSession, tied to the exact child executable and its
   *  launch environment. Deliberately UNWIRED in production: no trusted provenance
   *  exists yet, so its absence refuses the explicit option with a typed blocker
   *  instead of binding the child's entry filename. Never request-supplied. */
  legacyPiCachedModuleUrl?: (targetStartedAtMs?: number) => { modulePath: string; entryPath: string } | null;
  /** Daemon-set of known LEGACY runner script hashes. The live legacy runner's script
   *  must hash to one of these, so an unrelated process is never treated as the legacy
   *  runner. Absent or unmatched refuses: the bridge never guesses which build this is. */
  legacyRunnerHashes?: readonly string[];
  /** Reduced environment provenance for ONE target, from the kernel region: the seat's
   *  generation positive control plus the NODE_OPTIONS classification. Never values. */
  legacyEnvironmentObserver?: (input: { pid: number; generation: string }) => Promise<{
    regionReadable: boolean; occupantGenerationMatches: boolean;
    nodeOptions: "unset" | "present"; reasons: string[];
  }>;
  /** Whether a graph file predates a target's start, so today's bytes are the bytes that
   *  were loaded. Defaults to the shipped provenance check. */
  graphPredatesStart?: (path: string, targetStartedAtMs: number) => boolean;
  rehostPollMs?: number;
  /** Bounded READ-ONLY settling window for the post-proof observation, taken AFTER
   *  the single resume. Observation only: it never repeats the stop or the resume. */
  postProofSettleAttempts?: number;
  postProofSettleGapMs?: number;
  rehostWaitMs?: number;
  /** Up to two EXTRA full strict read-only pre-effect proof observations, spaced
   *  by rehostReobserveGapMs, inside the already-held guard lease. Bounded, never
   *  post-effect, and it does not weaken the prover's own double-sample rules. */
  rehostReobserveAttempts?: number;
  rehostReobserveGapMs?: number;
}

/** Structural view of the shipped PiResumeAdapter: only `resume` is used, and
 *  only with the exact session file. --resume/--fork are never emitted here. */
export interface PiRehostResume {
  resume(tmuxSessionName: string, resumeType: string | null, resumeToken: string | null, cwd: string, model?: string | null, resolvedPosture?: "floor" | "full_bypass", ledgerGeneration?: string): Promise<{ ok: boolean; code?: string; message?: string }>;
}
export interface PiRehostProof { state: "present" | "absent"; generation: string; launchId: string | null; fingerprint: string }
export interface PiRehostRunnerState { ready: boolean; launchId?: string; sessionFile?: string; sessionId?: string; lastEntryId?: string }

export interface RehostAuthorityFact { state: string; leaseUntil: number; epoch: number; ownerGeneration: string; generationMatchesOwner: boolean; expired: boolean; readOnly?: boolean; repairedByThisOperation?: boolean }
/** Shell basenames that may legitimately own a managed pane. tmux is deliberately
 *  absent: it is the shared SERVER, never a pane shell. */
const SHELL_BASENAMES = new Set(["zsh", "bash", "sh", "fish", "dash"]);
/** Shell identity derived exactly as the shipped Pi prover does (see
 *  coordinator-runtime-availability.ts): first token, basename, then the login-shell
 *  leading dash stripped, because ps lists a tmux pane's login shell as `-zsh`. */
function paneShellBasename(command: string | null | undefined): string | null {
  const first = (command ?? "").trim().split(/\s+/)[0] ?? "";
  if (!first) return null;
  return (first.split("/").pop() ?? "").replace(/^-/, "").toLowerCase();
}

export interface RehostPlan {
  generation: string; sessionFile: string; launchId: string; lastEntryId: string | null;
  runnerPid: number; runnerChildPid: number | null; sessionFileSha256Prefix: string;
  runnerStartedAt?: string; childStartedAt?: string; runnerCommand?: string; childCommand?: string;
  model: string | null; cwd: string; posture: "floor" | "full_bypass";
  authority: RehostAuthorityFact | null; sessionId: string | null;
  /** Present ONLY for the explicit legacy native-witness option. Carries the daemon-
   *  built witness facts; it is never accepted from a caller. */
  legacyWitness?: LegacyPiRehostWitnessFacts | null;
  /** The sidecar cursor as OBSERVED, kept beside the carried value so a receipt can
   *  state plainly which cursor was bypassed and which leaf replaced it. */
  sidecarCursorObserved?: string | null;
  stoppedTargetRecovery?: boolean;
  stoppedTargetAcceptanceReference?: string;
  recoverySnapshot?: { path: string; sha256: string; size: number; bytes: Buffer };
  recoveryAppendedBytes?: number;
  recoveryAppendedSha256?: string;
  stoppedTargetLeaf?: string;
  stoppedTargetBytes?: Buffer;
}

/** Full-file proof, private to stopped-target recovery. No conversation append is
 * credited after the old processes have exited. Receipts contain hashes only. */
function stoppedHistoryProof(plan: RehostPlan, sessionName: string) {
  const bytes = readFileSync(plan.sessionFile);
  const stopped = plan.stoppedTargetBytes!;
  const facts = { stopTimeBytes: stopped.length, stopTimeSha256: createHash("sha256").update(stopped).digest("hex"),
    finalBytes: bytes.length, finalSha256: createHash("sha256").update(bytes).digest("hex"),
    startupAppendedBytes: Math.max(0, bytes.length - stopped.length) };
  const refuse = (reason: string) => ({ ...facts, valid: false, reason, resultingLeaf: null as string | null });
  if (bytes.length < stopped.length || !bytes.subarray(0, stopped.length).equals(stopped)) return refuse("stop_time_prefix_changed");
  const parse = (input: Buffer): Array<Record<string, unknown>> => {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(input);
    if (!text.endsWith("\n")) throw new Error("incomplete_jsonl");
    return text.slice(0, -1).split("\n").map(line => {
      const row: unknown = JSON.parse(line);
      if (!row || typeof row !== "object" || Array.isArray(row)) throw new Error("invalid_entry");
      return row as Record<string, unknown>;
    });
  };
  try {
    const historical = parse(stopped);
    const ids = new Set<string>();
    for (const row of historical) {
      if (typeof row.id !== "string" || !row.id || typeof row.type !== "string" || ids.has(row.id)) return refuse("ambiguous_history");
      ids.add(row.id);
    }
    if (historical.at(-1)?.id !== plan.stoppedTargetLeaf) return refuse("stop_time_leaf_mismatch");
    if (bytes.length === stopped.length) return { ...facts, valid: true, reason: "unchanged", resultingLeaf: plan.stoppedTargetLeaf! };
    const suffix = bytes.subarray(stopped.length);
    if (suffix.length > 4096) return refuse("startup_metadata_too_large");
    const rows = parse(suffix);
    if (rows.length !== 1) return refuse("multiple_startup_entries");
    const row = rows[0]!;
    if (Object.keys(row).sort().join(",") !== "id,name,parentId,timestamp,type" || row.type !== "session_info" ||
        typeof row.id !== "string" || !row.id || ids.has(row.id) || row.parentId !== plan.stoppedTargetLeaf ||
        row.name !== sessionName || typeof row.timestamp !== "string" || !Number.isFinite(Date.parse(row.timestamp)))
      return refuse("invalid_startup_metadata");
    return { ...facts, valid: true, reason: "linked_native_session_info", resultingLeaf: row.id };
  } catch { return refuse("malformed_or_truncated_jsonl"); }
}

/** Why the legacy bridge could not even be ATTEMPTED. A closed vocabulary: an
 *  unestablished mapping is a refusal, never a guessed module or a guessed port. */
export const LEGACY_PI_BINDING_BLOCKERS = [
  "witness_seam_unavailable",
  "current_runner_entry_unresolved",
  "runner_entry_unresolved",
  "pi_child_unresolved",
  "pi_child_module_unresolved",
  "inspector_startup_unestablished",
  "legacy_runner_identity_unproven",
  "environment_control_absent",
  "node_options_present",
  "inspector_configuration_unestablished",
  "binding_identity_drift",
  "module_graph_newer_than_target",
] as const;
export type LegacyPiBindingBlocker = (typeof LEGACY_PI_BINDING_BLOCKERS)[number];

/** The daemon-owned legacy Pi native-witness seam. Production constructs the real
 *  collector from the module binding the service derived here from the exact census
 *  commands; nothing here accepts a witness, leaf, path, port or pid from a caller. */
export interface LegacyPiWitnessSeam {
  /** `seat` is the ALREADY VERIFIED seat binding this plan holds for the target. It is carried, not
   *  re-derived and never accepted from a caller, so the audit a witness writes before signalling
   *  names the real rig, node and logical id instead of placeholders. */
  witness(request: { binding: LegacyPiWitnessBinding; modules: LegacyPiModuleBinding; seat: { rigId: string; nodeId: string; logicalId: string } }): Promise<LegacyPiWitnessResult>;
}

/** What one accepted legacy witness proved, plus the binding and modules a FINAL
 *  fresh witness must reproduce before any halt. */
export interface LegacyPiRehostWitnessFacts {
  nativeLeaf: string;
  evidenceId: string;
  rounds: number;
  binding: LegacyPiWitnessBinding;
  modules: LegacyPiModuleBinding;
  /** The already-verified seat identity this witness ran against, so the FINAL witness can reuse
   *  the same identity instead of re-deriving it or inventing one. */
  seat: { rigId: string; nodeId: string; logicalId: string };
}
export interface RehostCustodySnapshot {
  tenantHash: string; resumeTokenHash: string; authorityHash: string; claimHash: string;
  unknownEffects: { count: number; digest: string }; sessionFile: string;
}
export type RehostRunnerResult =
  | CodexRehostResult
  | import("./pi-detached-resume.js").PiDetachedResumeResult
  | {
      ok: true; seat: SeatDescriptor; generation: string; generationUnchanged: true; sessionFile: string;
      launchIdBefore: string; launchIdAfter: string | null; durableModel: string | null;
      unknownEffectsPreserved: { count: number; digest: string };
      authority: (RehostAuthorityFact & { readOnly: true; repairedByThisOperation: false }) | null;
      guardLeftEnabled: true; events: string[];
    }
  | SeatRefusal;

interface ResolvedSeat {
  entry: NodeInventoryEntry;
  nodeId: string;
}

export interface SeatRefusal {
  ok: false;
  code:
    | "seat_ref_required"
    | "seat_not_found"
    | "seat_ambiguous"
    | "missing_model"
    | "missing_reason"
    | "missing_actor"
    | "invalid_cwd"
    | "cwd_selection_conflict"
    | "cwd_guard_unavailable"
    | "invalid_codex_profile"
    | "profile_not_installed"
    | "profile_load_failed"
    | "profile_model_mismatch"
    | "profile_posture_mismatch"
    | "profile_selection_conflict"
    | "profile_guard_unavailable"
    | "runtime_mismatch"
    | "permission_selection_refused"
    | "no_session"
    | "claimed_session"
    | "session_not_live"
    | "session_live"
    | "tmux_probe_failed"
    | "nothing_to_clean"
    | "fresh_required"
    | "unmanaged_session_collision"
    | "startup_context_missing"
    | "startup_context_malformed"
    | "startup_context_runtime_mismatch"
    | "runtime_adapter_missing"
    | "launch_unavailable"
    | "launch_failed"
    | "startup_failed"
    | "attention_required"
    | "runtime_identity_unverified"
    // Same-generation Pi runner rehost. Absence of a required seam, or any
    // unverifiable pre-stop fact, refuses. There is no degraded path.
    | "rehost_requires_pi_runtime"
    | "rehost_unavailable"
    | "rehost_guard_not_enabled"
    | "rehost_precondition_failed"
    | "rehost_receipt_unwritable"
    | "rehost_pane_root_unresolved"
    | "rehost_reservation_active"
    | "rehost_outbox_sending"
    | "rehost_generation_mismatch"
    | "rehost_sidecar_unverified"
    | "rehost_session_file_missing"
    | "rehost_process_identity_unproven"
    | "rehost_process_identity_unknown"
    | "rehost_runner_pid_unresolved"
    | "rehost_not_idle"
    | "rehost_stop_unverified"
    | "rehost_resume_failed"
    | "rehost_custody_drift"
    | "rehost_post_proof_failed"
    | "rehost_post_proof_unstable"
    | "rehost_effect_unknown"
    | "rehost_recovery_acceptance_required"
    | "rehost_recovery_modes_exclusive"
    | "rehost_recovery_snapshot_failed"
    | "rehost_recovery_history_changed"
    | "rehost_recovery_identity_changed"
    // EXPLICIT legacy native-witness option. Both are PRE-EFFECT refusals: they
    // precede every signal, so the daemon-owned mapping or the fresh witness
    // failing leaves the old runner exactly as it was found.
    | "rehost_legacy_witness_unavailable"
    | "rehost_legacy_witness_refused"
    /** B5: a diagnostic signal WAS delivered and the listener close could not be verified. UNKNOWN
     *  outcome, never a pre-effect refusal, and never blind-retryable. */
    | "rehost_legacy_inspector_unverified";
  message: string;
  guidance?: string;
  /** Present on an outcome whose EFFECT already happened: never true, never retryable. */
  blindRetryAllowed?: false;
  observed?: Record<string, unknown>;
  matches?: Array<{ rig_name: string; logical_id: string; current_occupant: string | null }>;
}

export interface SeatDescriptor {
  rigId: string;
  rigName: string;
  logicalId: string;
  nodeId: string;
}

export type SetModelResult =
  | { ok: true; seat: SeatDescriptor; from: string | null; to: string; changed: boolean }
  | SeatRefusal;

export type CodexProfileEffective = { model: string; provider: string; effort: string; approval: string; sandbox: string };
export type SetCodexProfileResult =
  | { ok: true; seat: SeatDescriptor; from: string | null; to: string; changed: boolean; effective: CodexProfileEffective; profileSha256: string; effect: string }
  | SeatRefusal;

const runCodex = promisify(execFile);
const CODEX_PROFILE_NAME = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
const CODEX_EFFORTS = new Set(["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"]);
const CODEX_APPROVALS = new Set(["never", "untrusted", "on-failure", "on-request"]);
const CODEX_SANDBOXES = new Set(["read-only", "workspace-write", "danger-full-access"]);

export function readInstalledCodexProfile(home: string, name: string, requireExplicit = true): { effective: CodexProfileEffective; sha256: string } | SeatRefusal {
  const path = resolve(home, `${name}.config.toml`);
  let bytes: Buffer;
  try {
    if (!lstatSync(path).isFile()) throw new Error("not a regular file");
    bytes = readFileSync(path);
  } catch {
    return { ok: false, code: "profile_not_installed", message: `Named Codex profile '${name}' is not an installed regular file.` };
  }
  try {
    const config = parseToml(bytes.toString("utf8")) as Record<string, unknown>;
    let global: Record<string, unknown> = {};
    if (!requireExplicit) {
      try { global = parseToml(readFileSync(resolve(home, "config.toml"), "utf8")) as Record<string, unknown>; }
      catch { /* Missing or invalid global config leaves required values unresolved below. */ }
    }
    const model = config["model"] ?? global["model"];
    const provider = config["model_provider"] ?? global["model_provider"] ?? (!requireExplicit ? "openai" : undefined);
    const effort = config["model_reasoning_effort"] ?? global["model_reasoning_effort"];
    const approval = config["approval_policy"] ?? global["approval_policy"];
    const sandbox = config["sandbox_mode"] ?? global["sandbox_mode"];
    // The proposed successor must be explicit; the predecessor is compared
    // using Codex's global layering, while refusing any unresolved posture.
    if (typeof model !== "string" || !model.trim() || typeof provider !== "string" || !provider.trim()
      || typeof effort !== "string" || !CODEX_EFFORTS.has(effort)
      || typeof approval !== "string" || !CODEX_APPROVALS.has(approval)
      || typeof sandbox !== "string" || !CODEX_SANDBOXES.has(sandbox)) {
      throw new Error("incomplete or invalid nonsecret native tuple");
    }
    return { effective: { model, provider, effort, approval, sandbox }, sha256: createHash("sha256").update(bytes).digest("hex") };
  } catch {
    return { ok: false, code: "invalid_codex_profile", message: `Named Codex profile '${name}' has invalid TOML or lacks an explicit valid native tuple.` };
  }
}

export type StopSeatResult =
  | { ok: true; seat: SeatDescriptor; sessionName: string; sessionId: string }
  | SeatRefusal;

export type CleanSeatResult =
  | { ok: true; seat: SeatDescriptor; actions: { sessionsExited: string[]; bindingCleared: boolean } }
  | SeatRefusal;

export type LaunchFreshResult =
  | {
      ok: true;
      seat: SeatDescriptor;
      status: "ready";
      sessionName: string;
      sessionId: string;
      generation: string;
      model: string | null;
      effort?: string | null;
      startupPolicyHash: string;
      supersededSessionIds: string[];
    }
  | (SeatRefusal & {
      status?: "attention_required" | "failed";
      sessionName?: string;
      sessionId?: string;
      generation?: string;
    });

interface LatestSessionRow {
  id: string;
  session_name: string;
  status: string;
  origin: string;
}

interface PersistedStartupContextRow {
  projection_entries_json: string;
  resolved_files_json: string;
  startup_actions_json: string;
  runtime: string | null;
}

interface ParsedStartupContext {
  plan: ProjectionPlan;
  resolvedStartupFiles: ResolvedStartupFile[];
  startupActions: StartupAction[];
  runtime: string;
  hash: string;
}

export class SeatLifecycleService {
  private readonly db: Database.Database;
  private readonly rigRepo: RigRepository;
  private readonly sessionRegistry: SessionRegistry;
  private readonly eventBus: EventBus;
  private readonly tmuxAdapter: TmuxAdapter;
  private readonly listProcesses?: NativeProcessLister;
  private readonly nodeLauncher: NodeLauncher | null;
  private readonly startupOrchestrator: StartupOrchestrator | null;
  private readonly runtimeAdapters: Record<string, RuntimeAdapter>;
  private readonly occupantInvalidator: OccupantInvalidator | null;
  private readonly activityOracle: SeatLifecycleDeps["activityOracle"] | null;
  private readonly codexProfileHome: string;
  private readonly codexProfileProbe: (profile: string) => Promise<unknown>;
  private readonly piResume?: PiRehostResume;
  private readonly piDetachedResume?: SeatLifecycleDeps["piDetachedResume"];
  private readonly codexRehost?: SeatLifecycleDeps["codexRehost"];
  private readonly piProve?: (session: string) => Promise<PiRehostProof | null>;
  private readonly piRunnerState?: (sessionName: string) => PiRehostRunnerState | null;
  private readonly piSessionFileExists?: (path: string) => boolean;
  private readonly piRecoverySnapshotDirectory?: string;
  private readonly piSessionFileDigestPrefix?: (sessionFile: string) => string | null;
  private readonly paneRootPid?: (nodeId: string) => Promise<number | null>;
  private readonly piSessionTailEntryId?: (path: string) => string | null;
  private readonly killNativeProcess?: (pid: number) => void;
  private readonly legacyPiWitness?: LegacyPiWitnessSeam;
  private readonly currentPiRunnerEntryPath: string | null;
  private readonly legacyPiCachedModuleUrlResolver?: (targetStartedAtMs?: number) => { modulePath: string; entryPath: string } | null;
  private readonly legacyRunnerHashes?: readonly string[];
  private readonly graphPredatesStart?: (path: string, targetStartedAtMs: number) => boolean;
  private readonly legacyEnvironmentObserver?: (input: { pid: number; generation: string }) => Promise<{
    regionReadable: boolean; occupantGenerationMatches: boolean;
    nodeOptions: "unset" | "present"; reasons: string[];
  }>;
  private readonly rehostPollMs: number;
  private readonly postProofSettleAttempts: number;
  private readonly postProofSettleGapMs: number;
  private readonly rehostReobserveAttempts: number;
  private readonly rehostReobserveGapMs: number;
  private readonly rehostWaitMs: number;

  constructor(deps: SeatLifecycleDeps) {
    if (deps.db !== deps.rigRepo.db) throw new Error("SeatLifecycleService: rigRepo must share the same db handle");
    if (deps.db !== deps.sessionRegistry.db) throw new Error("SeatLifecycleService: sessionRegistry must share the same db handle");
    if (deps.db !== deps.eventBus.db) throw new Error("SeatLifecycleService: eventBus must share the same db handle");
    this.db = deps.db;
    this.rigRepo = deps.rigRepo;
    this.sessionRegistry = deps.sessionRegistry;
    this.eventBus = deps.eventBus;
    this.tmuxAdapter = deps.tmuxAdapter;
    this.listProcesses = deps.listProcesses;
    this.nodeLauncher = deps.nodeLauncher ?? null;
    this.startupOrchestrator = deps.startupOrchestrator ?? null;
    this.runtimeAdapters = deps.runtimeAdapters ?? {};
    this.occupantInvalidator = deps.occupantInvalidator ?? null;
    this.activityOracle = deps.activityOracle ?? null;
    this.codexProfileHome = deps.codexProfileHome ?? process.env["CODEX_HOME"] ?? resolve(homedir(), ".codex");
    this.codexProfileProbe = deps.codexProfileProbe ?? (async (profile) => {
      // argv is fixed and the name is allowlisted; no shell or model turn.
      await runCodex("codex", ["-p", profile, "mcp", "list"], { timeout: 10_000, maxBuffer: 1024 * 1024 });
    });
    this.piResume = deps.piResume;
    this.piDetachedResume = deps.piDetachedResume;
    this.codexRehost = deps.codexRehost;
    this.piProve = deps.piProve;
    this.piRunnerState = deps.piRunnerState;
    this.piSessionFileExists = deps.piSessionFileExists;
    this.piRecoverySnapshotDirectory = deps.piRecoverySnapshotDirectory;
    this.piSessionFileDigestPrefix = deps.piSessionFileDigestPrefix;
    this.paneRootPid = deps.paneRootPid;
    this.piSessionTailEntryId = deps.piSessionTailEntryId;
    this.killNativeProcess = deps.killNativeProcess ?? ((pid) => { process.kill(pid, "SIGTERM"); });
    this.legacyPiWitness = deps.legacyPiWitness;
    this.currentPiRunnerEntryPath = deps.currentPiRunnerEntryPath ?? null;
    this.legacyPiCachedModuleUrlResolver = deps.legacyPiCachedModuleUrl;
    this.legacyRunnerHashes = deps.legacyRunnerHashes;
    this.graphPredatesStart = deps.graphPredatesStart;
    this.legacyEnvironmentObserver = deps.legacyEnvironmentObserver;
    this.rehostPollMs = deps.rehostPollMs ?? 250;
    this.postProofSettleAttempts = Math.max(1, Math.min(5, deps.postProofSettleAttempts ?? 3));
    this.postProofSettleGapMs = Math.max(0, deps.postProofSettleGapMs ?? 250);
    this.rehostWaitMs = deps.rehostWaitMs ?? 20_000;
    this.rehostReobserveAttempts = Math.max(0, Math.min(2, deps.rehostReobserveAttempts ?? 2));
    this.rehostReobserveGapMs = Math.max(0, deps.rehostReobserveGapMs ?? 300);
  }

  /** Audited future-launch directory selection; never sends input or restarts a seat. */
  async setCwd(input: { seatRef: string; cwd: string; reason: string; actor: string }): Promise<
    { ok: true; seat: SeatDescriptor; from: string | null; to: string; changed: boolean; effect: string } | SeatRefusal> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    if (!input.actor?.trim()) return { ok: false, code: "missing_actor", message: "Transport sender identity is required for the directory audit." };
    if (typeof input.cwd !== "string" || !isAbsolute(input.cwd) || input.cwd.includes("\0")) {
      return { ok: false, code: "invalid_cwd", message: "An absolute existing directory is required." };
    }
    const target = this.resolveSeat(input.seatRef);
    if ("code" in target) return target;
    const guard = this.tmuxAdapter.deliveryGuard;
    if (!guard || guard.db !== this.db) return { ok: false, code: "cwd_guard_unavailable", message: "Same-database lifecycle guard is required; no directory changed." };
    const snapshot = JSON.stringify(this.db.prepare("SELECT * FROM nodes WHERE id=?").get(target.nodeId));
    const change = async () => {
      if (!guard.ownsLifecycle(target.nodeId)) return { ok: false as const, code: "cwd_guard_unavailable" as const, message: "Lifecycle lease is required." };
      const resolved = this.resolveSeat(input.seatRef);
      if ("code" in resolved) return resolved;
      if (resolved.nodeId !== target.nodeId || JSON.stringify(this.db.prepare("SELECT * FROM nodes WHERE id=?").get(target.nodeId)) !== snapshot) {
        return { ok: false as const, code: "cwd_selection_conflict" as const, message: "Seat configuration changed while waiting; reconcile and retry." };
      }
      let cwd: string, identity: { dev: number; ino: number };
      try {
        cwd = realpathSync(input.cwd);
        const stat = statSync(cwd);
        if (!stat.isDirectory()) throw new Error("not directory");
        identity = { dev: stat.dev, ino: stat.ino };
      } catch { return { ok: false as const, code: "invalid_cwd" as const, message: "An absolute existing directory is required; no seat changed." }; }
      const seat = this.describe(resolved);
      let persisted: PersistedEvent | null = null;
      const result = this.db.transaction(() => {
        if (JSON.stringify(this.db.prepare("SELECT * FROM nodes WHERE id=?").get(target.nodeId)) !== snapshot) {
          return { ok: false as const, code: "cwd_selection_conflict" as const, message: "Seat configuration changed before commit; no directory changed." };
        }
        try {
          const stat = statSync(cwd);
          if (realpathSync(input.cwd) !== cwd || !stat.isDirectory() || stat.dev !== identity.dev || stat.ino !== identity.ino) throw new Error("changed directory");
        } catch { return { ok: false as const, code: "cwd_selection_conflict" as const, message: "Directory changed before commit; no seat changed." }; }
        const from = resolved.entry.cwd ?? null;
        if (from !== cwd) {
          this.rigRepo.setNodeCwd(target.nodeId, cwd);
          persisted = this.eventBus.persistWithinTransaction({ type: "node.cwd_changed", rigId: seat.rigId, nodeId: seat.nodeId,
            logicalId: seat.logicalId, from, to: cwd, reason: input.reason.trim(), operator: input.actor.trim(), effect: "future_launches_only" });
        }
        return { ok: true as const, seat, from, to: cwd, changed: from !== cwd,
          effect: "Future managed launches only; native process, history, queue, generation, model, profile and permissions are unchanged." };
      })();
      if (persisted) this.eventBus.notifySubscribers(persisted);
      return result;
    };
    return guard.ownsLifecycle(target.nodeId) ? change() : guard.lifecycle([target.nodeId], change);
  }

  async setCodexProfile(input: { seatRef: string; profile: string; reason: string; actor: string }): Promise<SetCodexProfileResult> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    if (!input.actor?.trim()) return { ok: false, code: "missing_actor", message: "A transport-derived seat identity is required for profile pin audit." };
    const profile = input.profile?.trim() ?? "";
    if (!CODEX_PROFILE_NAME.test(profile)) {
      return { ok: false, code: "invalid_codex_profile", message: "A safe named Codex profile is required (--profile)." };
    }
    // Resolve only the node identity before waiting. All mutable seat/profile
    // facts are re-read and validated under the same lifecycle lease used by
    // rotation, handover, restore, and managed launch.
    const target = this.resolveSeat(input.seatRef);
    if ("code" in target) return target;
    const guard = this.tmuxAdapter.deliveryGuard;
    if (!guard || guard.db !== this.db) return { ok: false, code: "profile_guard_unavailable", message: "Seat lifecycle guard is unavailable for this database; no profile pin changed." };
    if (guard.ownsLifecycle(target.nodeId)) return this.setCodexProfileUnderLease(input, target.nodeId);
    return guard.lifecycle([target.nodeId], () => this.setCodexProfileUnderLease(input, target.nodeId));
  }

  private async setCodexProfileUnderLease(
    input: { seatRef: string; profile: string; reason: string; actor: string }, expectedNodeId: string,
  ): Promise<SetCodexProfileResult> {
    const guard = this.tmuxAdapter.deliveryGuard;
    if (!guard || guard.db !== this.db || !guard.ownsLifecycle(expectedNodeId)) {
      return { ok: false, code: "profile_guard_unavailable", message: "Seat lifecycle lease was not held; no profile pin changed." };
    }
    const profile = input.profile.trim();
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;
    if (resolved.nodeId !== expectedNodeId) {
      return { ok: false, code: "profile_selection_conflict", message: "Seat identity changed while waiting for its lifecycle lease; no profile pin changed." };
    }
    if (resolved.entry.runtime !== "codex") {
      return { ok: false, code: "runtime_mismatch", message: "Only an existing Codex seat can select a Codex profile." };
    }
    const installed = readInstalledCodexProfile(this.codexProfileHome, profile);
    if ("code" in installed) return installed;
    if (installed.effective.model !== resolved.entry.model) {
      return { ok: false, code: "profile_model_mismatch", message: "Profile model differs from the persisted seat model; reconcile the model with rig seat set-model first." };
    }
    const priorName = resolved.entry.codexConfigProfile;
    if (!priorName || !CODEX_PROFILE_NAME.test(priorName)) {
      return { ok: false, code: "profile_selection_conflict", message: "Existing Codex profile is not a safe named pin; its provider and launch posture cannot be compared." };
    }
    const prior = readInstalledCodexProfile(this.codexProfileHome, priorName, false);
    if ("code" in prior) {
      return { ok: false, code: "profile_selection_conflict", message: "Existing Codex profile cannot be read; its provider and launch posture cannot be compared." };
    }
    if (prior.effective.provider !== installed.effective.provider || prior.effective.approval !== installed.effective.approval
      || prior.effective.sandbox !== installed.effective.sandbox) {
      return { ok: false, code: "profile_posture_mismatch", message: "New profile would change provider, approval policy or sandbox mode; no seat state changed." };
    }
    try {
      await this.codexProfileProbe(profile);
    } catch {
      // Do not surface CLI stderr: profiles may include private integration data.
      return { ok: false, code: "profile_load_failed", message: `Codex did not load named profile '${profile}'. No seat state changed.` };
    }
    const confirmed = readInstalledCodexProfile(this.codexProfileHome, profile);
    const priorConfirmed = readInstalledCodexProfile(this.codexProfileHome, priorName, false);
    if ("code" in confirmed || confirmed.sha256 !== installed.sha256 || "code" in priorConfirmed
      || priorConfirmed.sha256 !== prior.sha256 || JSON.stringify(priorConfirmed.effective) !== JSON.stringify(prior.effective)) {
      return { ok: false, code: "profile_selection_conflict", message: "Profile file changed during validation; no seat state changed." };
    }
    const seat = this.describe(resolved);
    let persisted: PersistedEvent | null = null;
    const result = this.db.transaction(() => {
      const current = this.db.prepare("SELECT runtime, model, codex_config_profile FROM nodes WHERE id = ?").get(seat.nodeId) as
        | { runtime: string | null; model: string | null; codex_config_profile: string | null }
        | undefined;
      if (!current || current.runtime !== "codex" || current.model !== installed.effective.model || current.codex_config_profile !== priorName) {
        return { ok: false as const, code: "profile_selection_conflict" as const, message: "Seat runtime, model or prior profile changed during validation; no seat state changed." };
      }
      const from = current.codex_config_profile;
      if (from !== profile) {
        this.rigRepo.setNodeCodexConfigProfile(seat.nodeId, profile);
        persisted = this.eventBus.persistWithinTransaction({
          type: "node.codex_profile_changed", rigId: seat.rigId, nodeId: seat.nodeId, logicalId: seat.logicalId,
          from, to: profile, effective: installed.effective, profileSha256: installed.sha256,
          reason: input.reason.trim(), operator: input.actor.trim(), effect: "future_launches_only",
        });
      }
      return { ok: true as const, seat, from, to: profile, changed: from !== profile,
        effective: installed.effective, profileSha256: installed.sha256,
        effect: "Future managed launches only. Native process, history, queue, permissions and work posture are unchanged." };
    })();
    if (persisted) this.eventBus.notifySubscribers(persisted);
    return result;
  }

  async setModel(input: { seatRef: string; model: string; reason: string; operator?: string | null }): Promise<SetModelResult> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    if (!input.model?.trim()) {
      return { ok: false, code: "missing_model", message: "A target model id is required (--model)." };
    }
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;

    const model = input.model.trim();
    const seat = this.describe(resolved);
    const from = resolved.entry.model ?? null;
    if (from === model) {
      // Honest no-op: the persisted value already IS the target; no event is minted.
      return { ok: true, seat, from, to: model, changed: false };
    }

    let persisted: PersistedEvent | null = null;
    const tx = this.db.transaction(() => {
      this.rigRepo.setNodeModel(resolved.nodeId, model);
      persisted = this.eventBus.persistWithinTransaction({
        type: "node.model_changed",
        rigId: seat.rigId,
        nodeId: seat.nodeId,
        logicalId: seat.logicalId,
        from,
        to: model,
        reason: input.reason.trim(),
        operator: input.operator ?? null,
      });
    });
    tx();
    if (persisted) this.eventBus.notifySubscribers(persisted);

    return { ok: true, seat, from, to: model, changed: true };
  }

  async setPermissions(input: { seatRef: string; mode: string; reason: string; actor: string }): Promise<
    | { ok: true; seat: SeatDescriptor; from: unknown; to: unknown; changed: boolean; effect: string }
    | SeatRefusal
  > {
    input = { ...input };
    const required = this.requireReason(input.reason);
    if (required) return required;
    if (!input.actor.trim()) return { ok: false, code: "permission_selection_refused", message: "Sender identity is required for permission audit." };
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;
    const seat = this.describe(resolved);
    const runtime = resolved.entry.runtime ?? "unknown";
    try {
      const dynamic = runtime === "claude-code" && !["floor", "full_bypass", "inherit"].includes(input.mode);
      const managed = this.runtimeAdapters["claude-code"]?.claudeManagedLaunch;
      if (dynamic && !managed) await unresolvedClaudePermissionModes();
      const launch = dynamic ? await managed!.prepare({ nodeId: seat.nodeId, cwd: resolved.entry.cwd ?? undefined }, input.mode) : null;
      const to = input.mode === "inherit" ? null : dynamic ? { runtime: "claude-code" as const, mode: input.mode }
        : validateNativePermissionSelection(runtime, input.mode);
      const store = new NativePermissionStore(this.db);
      let persisted: PersistedEvent | null = null;
      const result = this.db.transaction(() => {
        launch?.assertCurrent();
        const currentRuntime = this.db.prepare("SELECT runtime FROM nodes WHERE id = ?").get(seat.nodeId) as { runtime: string } | undefined;
        if (currentRuntime?.runtime !== runtime) throw new Error("Seat runtime changed while checking native options; selection was not changed.");
        const from = store.read(seat.nodeId);
        const changed = from?.runtime !== to?.runtime || from?.mode !== to?.mode;
        if (changed) {
          store.write(seat.nodeId, to, input.actor.trim(), input.reason.trim());
          persisted = this.eventBus.persistWithinTransaction({ type: "node.permissions_changed", rigId: seat.rigId,
            nodeId: seat.nodeId, from, to, actor: input.actor.trim(), reason: input.reason.trim(), source: "seat_selection", effect: "future_launches_only" });
        }
        return { ok: true as const, seat, from, to, changed,
          effect: "Future managed launches only. The current native process, history, permission rules and work posture are unchanged; no relaunch was requested." };
      })();
      if (persisted) this.eventBus.notifySubscribers(persisted);
      return result;
    } catch (error) {
      return { ok: false, code: "permission_selection_refused", message: (error as Error).message };
    }
  }

  async stopSeat(input: { seatRef: string; reason: string; operator?: string | null }): Promise<StopSeatResult> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;
    const guard = this.tmuxAdapter.deliveryGuard;
    if (guard && !guard.ownsLifecycle(resolved.nodeId)) {
      return guard.lifecycle([resolved.nodeId], () => this.stopSeat(input));
    }
    const seat = this.describe(resolved);

    const session = this.latestSession(resolved.nodeId);
    if (!session) {
      return { ok: false, code: "no_session", message: `Seat "${input.seatRef}" has no session to stop (checked: latest sessions row for node ${seat.logicalId}).` };
    }
    if (session.origin === "claimed") {
      return {
        ok: false,
        code: "claimed_session",
        message: `Session "${session.session_name}" was adopted (origin=claimed), not launched by OpenRig — stop refuses to kill it.`,
        guidance: "Release an adopted session with: rig unclaim",
      };
    }

    // Wave-2 fix round 1 (r1 row 9baac99f): consume the CLASSIFIED probe, never the
    // collapsed hasSession view — a transport blip is INDETERMINATE, not absence
    // (KI-5.3-8 fabricated-absence class, destructive direction).
    const probed = await this.probeLiveness(session.session_name, "stop refuses rather than kill blind");
    if ("code" in probed) return probed;
    if (probed.state === "absent") {
      return {
        ok: false,
        code: "session_not_live",
        message: `Session "${session.session_name}" is absent in tmux (checked: tmux has-session, POSITIVE absence evidence) — there is nothing to stop.`,
        guidance: "A dead seat with stale records is returned to launchable with: rig seat clean",
      };
    }

    return this.stopManagedTmuxSeat(resolved, session, input.reason, input.operator);
  }

  async cleanSeat(input: { seatRef: string; reason: string; operator?: string | null }): Promise<CleanSeatResult> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;
    const guard = this.tmuxAdapter.deliveryGuard;
    if (guard && !guard.ownsLifecycle(resolved.nodeId)) {
      return guard.lifecycle([resolved.nodeId], () => this.cleanSeat(input));
    }
    const seat = this.describe(resolved);

    const binding = this.sessionRegistry.getBindingForNode(resolved.nodeId);
    const nonTerminal = (this.db.prepare(
      "SELECT id, session_name, status, origin FROM sessions WHERE node_id = ? ORDER BY id",
    ).all(resolved.nodeId) as LatestSessionRow[])
      .filter((s) => !TERMINAL_SESSION_STATUSES.has(s.status));

    // Fix r2-F3 (row 30045f39): clean MUTATES every non-terminal session row, so
    // its safety checks must cover exactly that set — probing only the newest row
    // fabricates safety for the others (older-live/newer-dead under canonical-name
    // churn). Every row that would be touched is checked for adopted origin and
    // probed for POSITIVE absence (r1 discipline); the binding's own tmux session
    // is probed too when it names a session no row carries.
    const mutationTargets = nonTerminal;
    for (const row of mutationTargets) {
      if (row.origin === "claimed") {
        return {
          ok: false,
          code: "claimed_session",
          message: `Session "${row.session_name}" was adopted (origin=claimed) — clean refuses to touch adopted state.`,
          guidance: "Release an adopted session with: rig unclaim",
        };
      }
    }
    const probeNames = [...new Set([
      ...mutationTargets.map((s) => s.session_name),
      ...(binding?.tmuxSession ? [binding.tmuxSession] : []),
    ])];
    for (const name of probeNames) {
      const probed = await this.probeLiveness(name, "clean refuses rather than clear state under a possibly-live seat");
      if ("code" in probed) return probed;
      if (probed.state === "present") {
        return {
          ok: false,
          code: "session_live",
          message: `Session "${name}" is alive in tmux (checked: tmux has-session, against EVERY session row clean would mutate) — clean only operates on dead seats.`,
          guidance: "Stop a live seat first with: rig seat stop",
        };
      }
    }
    const session = this.latestSession(resolved.nodeId);

    if (!binding && nonTerminal.length === 0) {
      return {
        ok: false,
        code: "nothing_to_clean",
        message: `Seat "${input.seatRef}" is already clean (checked: no binding row for the node, and no session rows outside terminal statuses ${[...TERMINAL_SESSION_STATUSES].join("/")}).`,
      };
    }

    const sessionsExited: string[] = [];
    let persisted: PersistedEvent | null = null;
    const tx = this.db.transaction(() => {
      for (const row of nonTerminal) {
        this.sessionRegistry.updateStatus(row.id, "exited");
        sessionsExited.push(row.session_name);
      }
      this.sessionRegistry.clearBinding(resolved.nodeId);
      persisted = this.eventBus.persistWithinTransaction({
        type: "session.cleaned",
        rigId: seat.rigId,
        nodeId: seat.nodeId,
        sessionName: session?.session_name ?? null,
        reason: input.reason.trim(),
        operator: input.operator ?? null,
        actions: { sessionsExited, bindingCleared: binding !== null },
      });
    });
    tx();
    this.tmuxAdapter.deliveryGuard?.rebindLifecycle(resolved.nodeId);
    if (persisted) this.eventBus.notifySubscribers(persisted);

    return { ok: true, seat, actions: { sessionsExited, bindingCleared: binding !== null } };
  }

  /** Finish context delivery to the same fresh occupant after its native gate.
   * This never launches a process or replays an uncertain/finished delivery.
   */
  async continueFreshStartup(seatRef: string) {
    const resolved = this.resolveSeat(seatRef);
    if ("code" in resolved) return resolved;
    const guard = this.tmuxAdapter.deliveryGuard;
    return guard
      ? guard.lifecycle([resolved.nodeId], () => this.continueFreshStartupUnchecked(seatRef))
      : this.continueFreshStartupUnchecked(seatRef);
  }

  private async continueFreshStartupUnchecked(seatRef: string) {
    const resolved = this.resolveSeat(seatRef);
    if ("code" in resolved) return resolved;
    const seat = this.describe(resolved);
    const node = this.rigRepo.getRig(seat.rigId)?.nodes.find((n) => n.id === seat.nodeId);
    const session = this.latestSession(seat.nodeId);
    const binding = this.sessionRegistry.getBindingForNode(seat.nodeId);
    if (!node || !session || !binding?.tmuxSession || !this.startupOrchestrator
      || !this.startupOrchestrator.canContinueFresh(node.id, session.id)) {
      return { ok: false as const, code: "continuation_unavailable", message: "No verified pending fresh-context delivery exists for this occupant. Refresh to inspect its actual state." };
    }
    const pane = await observeSolePane(this.tmuxAdapter, binding.tmuxSession);
    if (!pane.ok || pane.pane !== binding.tmuxPane) return { ok: false as const, code: "binding_changed", message: "The managed terminal binding changed; context was not delivered." };
    const adapter = node.runtime ? this.runtimeAdapters[node.runtime] : undefined;
    if (!adapter) return { ok: false as const, code: "runtime_adapter_missing", message: "The configured runtime adapter is unavailable." };
    const ready = await adapter.checkReady({ ...binding, cwd: node.cwd ?? "." });
    if (!ready.ready) return { ok: false as const, code: "attention_required", message: ready.reason ?? "Resolve the native prerequisite first." };
    const startup = this.readStartupContext(node.id, node.cwd ?? ".");
    if (!startup.ok) return startup.refusal;
    if (startup.context.runtime !== node.runtime) return { ok: false as const, code: "startup_context_runtime_mismatch", message: "The saved startup context belongs to a different runtime." };
    // Recheck after the asynchronous native observation; startNode immediately
    // records pending before its first await, consuming the retained permission.
    if (this.latestSession(node.id)?.id !== session.id || this.sessionRegistry.getBindingForNode(node.id)?.tmuxPane !== pane.pane
      || !this.startupOrchestrator.canContinueFresh(node.id, session.id)) return { ok: false as const, code: "continuation_unavailable", message: "Startup changed during the readiness check. Refresh." };
    const result = await this.startupOrchestrator.startNode({
      rigId: seat.rigId, nodeId: node.id, sessionId: session.id,
      binding: { ...binding, cwd: node.cwd ?? ".", model: node.model ?? undefined, effort: node.effort ?? undefined, codexConfigProfile: node.codexConfigProfile ?? undefined },
      adapter, plan: startup.context.plan, resolvedStartupFiles: startup.context.resolvedStartupFiles,
      startupActions: startup.context.startupActions, isRestore: false,
      sessionName: session.session_name, skipHarnessLaunch: true, continueFreshStartup: true, includeDurableObligations: true, allowFreshFallback: false,
    });
    return { ...result, message: result.ok ? "Configured context delivered to the existing fresh conversation." : result.errors.join("; ") };
  }

  /** Deliberately replace exactly one managed seat with a blank native occupant. */
  async launchFresh(input: {
    seatRef: string;
    fresh: boolean;
    reason: string;
    stop?: boolean;
    operator?: string | null;
  }): Promise<LaunchFreshResult> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    if (input.fresh !== true) {
      return {
        ok: false,
        code: "fresh_required",
        message: "Explicit fresh launch requires fresh=true (--fresh); no continuity mode is inferred.",
      };
    }
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;
    const guard = this.tmuxAdapter.deliveryGuard;
    if (guard && !guard.ownsLifecycle(resolved.nodeId)) {
      return guard.lifecycle([resolved.nodeId], () => this.launchFresh(input));
    }
    const seat = this.describe(resolved);
    const rig = this.rigRepo.getRig(seat.rigId);
    const node = rig?.nodes.find((candidate) => candidate.id === seat.nodeId);
    if (!rig || !node) {
      return { ok: false, code: "seat_not_found", message: `Seat "${input.seatRef}" no longer exists.`, guidance: SEAT_LOOKUP_GUIDANCE };
    }
    if (!this.nodeLauncher || !this.startupOrchestrator) {
      return { ok: false, code: "launch_unavailable", message: "Fresh-launch services are unavailable in this daemon." };
    }

    const startup = this.readStartupContext(node.id, node.cwd ?? ".");
    if (!startup.ok) return startup.refusal;
    if (!node.runtime || startup.context.runtime !== node.runtime) {
      return {
        ok: false,
        code: "startup_context_runtime_mismatch",
        message: `Persisted startup context runtime '${startup.context.runtime}' does not match current node runtime '${node.runtime ?? "missing"}'.`,
      };
    }
    const adapter = this.runtimeAdapters[node.runtime];
    if (!adapter) {
      return {
        ok: false,
        code: "runtime_adapter_missing",
        message: `No runtime adapter is available for '${node.runtime}'.`,
      };
    }

    const canonicalSessionName = deriveCanonicalFromEntry(resolved.entry)
      ?? deriveSessionName(seat.rigName, seat.logicalId);
    const retiringRows = this.nonTerminalSessions(node.id);
    if (retiringRows.some((row) => row.origin === "claimed")) {
      return {
        ok: false,
        code: "claimed_session",
        message: `Seat "${input.seatRef}" has an adopted/operator-owned occupant; fresh launch refuses even with --stop.`,
        guidance: "Stop the adopted process yourself, then run rig seat clean before launching fresh.",
      };
    }
    // Detached is terminal for process cleanup, but remains a reboot candidate.
    // A deliberate fresh launch must retire that prior history as well.
    const supersededSessionIds = (this.db.prepare(
      "SELECT id FROM sessions WHERE node_id = ? AND status NOT IN ('superseded', 'exited')",
    ).all(node.id) as Array<{ id: string }>).map((row) => row.id);
    const retiringGeneration = this.sessionRegistry.currentOccupantTenure(node.id)?.generationUuid ?? null;

    let canonicalProbe = await this.probeLiveness(
      canonicalSessionName,
      "fresh launch refuses rather than overwrite a possibly-live canonical session",
    );
    // `rig seat stop` persists an exited row after killing its managed session.
    // When that was the last tmux session, tmux exits too, so the next classified
    // probe sees unavailable transport instead of positive absence. Reopen an
    // empty server only when the persisted state proves this seat's managed
    // occupant was deliberately stopped and no other managed row remains live.
    // The probe below still decides absence; a recreated session or failed
    // transport remains a refusal.
    if ("code" in canonicalProbe) {
      const latest = this.latestSession(node.id);
      const hasCurrentBinding = this.sessionRegistry.getBindingForNode(node.id) !== null;
      const stoppedManagedOccupant = latest !== null
        && latest.session_name === canonicalSessionName
        && latest.status === "exited"
        && latest.origin !== "claimed"
        && !hasCurrentBinding
        && this.nonTerminalSessions(node.id).length === 0;
      if (stoppedManagedOccupant) {
        try {
          const restored = await this.tmuxAdapter.startServer();
          if (restored.ok) {
            canonicalProbe = await this.probeLiveness(
              canonicalSessionName,
              "fresh launch refuses rather than overwrite a possibly-live canonical session",
            );
          }
        } catch {
          // Keep the original classified refusal when the transport cannot be
          // restored; never translate a failed start into absence.
        }
      }
    }
    if ("code" in canonicalProbe) return canonicalProbe;
    if (canonicalProbe.state === "present") {
      const currentSession = this.latestSession(node.id);
      const currentBinding = this.sessionRegistry.getBindingForNode(node.id);
      const currentManaged = currentSession !== null
        && !TERMINAL_SESSION_STATUSES.has(currentSession.status)
        && currentSession.origin !== "claimed"
        && currentSession.session_name === canonicalSessionName
        && currentBinding?.tmuxSession === canonicalSessionName;
      if (!currentManaged) {
        return {
          ok: false,
          code: "unmanaged_session_collision",
          message: `Canonical tmux session "${canonicalSessionName}" exists but is not owned by this seat's current managed rows; refusing to overwrite it.`,
        };
      }
      if (!input.stop) {
        return {
          ok: false,
          code: "session_live",
          message: `Seat "${input.seatRef}" is live; fresh launch refuses without --stop.`,
          guidance: "Re-run with --stop to end exactly this managed occupant, or use rig handover to carry context.",
        };
      }
      const observedPane = await observeSolePane(this.tmuxAdapter, canonicalSessionName);
      if (!observedPane.ok && observedPane.code === "tmux_unavailable") {
        return {
          ok: false,
          code: "tmux_probe_failed",
          message: `${observedPane.detail}; fresh launch refuses rather than kill without live occupant identity.`,
        };
      }
      if (!observedPane.ok || observedPane.pane !== currentBinding?.tmuxPane) {
        return {
          ok: false,
          code: "unmanaged_session_collision",
          message: `Canonical tmux session "${canonicalSessionName}" is live, but its pane does not match this seat's current managed binding; refusing to stop it.`,
        };
      }
      const stopped = await this.stopManagedTmuxSeat(
        resolved,
        currentSession,
        input.reason,
        input.operator,
      );
      if (!stopped.ok) return stopped;
      // Stopping the server's last session ends tmux's server, and every probe
      // below would then be transport_unavailable, never absence. Restore an
      // empty server (no session is invented; a no-op while the server is up)
      // so they get a positive answer. The classified probes still decide.
      await this.tmuxAdapter.startServer();
    }

    // Reuse clean's exhaustive, positive-absence gate for stale/history rows.
    const remaining = this.nonTerminalSessions(node.id);
    const binding = this.sessionRegistry.getBindingForNode(node.id);
    if (remaining.length > 0 || binding !== null) {
      const cleaned = await this.cleanSeat({
        seatRef: canonicalSessionName,
        reason: input.reason,
        operator: input.operator,
      });
      if (!cleaned.ok) return cleaned;
    }

    // The stop/clean composition may have taken time; buy absence again at the
    // mutation boundary. NodeLauncher also refuses duplicate_session and never kills it.
    const finalProbe = await this.probeLiveness(
      canonicalSessionName,
      "fresh launch refuses rather than race a canonical-session collision",
    );
    if ("code" in finalProbe) return finalProbe;
    if (finalProbe.state === "present") {
      return {
        ok: false,
        code: "unmanaged_session_collision",
        message: `Canonical tmux session "${canonicalSessionName}" appeared before launch; refusing to overwrite it.`,
      };
    }

    // Historical rows remain append-only but no longer look current.
    for (const sessionId of supersededSessionIds) this.sessionRegistry.markSuperseded(sessionId);
    this.occupantInvalidator?.invalidateRetiringOccupant({
      retiringSessionName: canonicalSessionName,
      successorSessionName: canonicalSessionName,
      ...(retiringGeneration ? { retiringGeneration } : {}),
    });

    const launch = await this.nodeLauncher.launchNode(seat.rigId, seat.logicalId, {
      sessionName: canonicalSessionName,
      cwd: node.cwd ?? undefined,
      occupantKind: "fresh",
    });
    if (!launch.ok) {
      return { ok: false, code: "launch_failed", message: launch.message };
    }
    const observedGeneration = this.sessionRegistry.currentOccupantTenure(node.id)?.generationUuid ?? null;
    const generation = observedGeneration && observedGeneration !== retiringGeneration
      ? observedGeneration
      : null;
    if (!generation) {
      const compensation = await this.compensateFailedFreshLaunch({
        seat,
        launch,
        supersededSessionIds,
        retiringGeneration,
        newGeneration: null,
        startupPolicyHash: startup.context.hash,
        model: node.model,
        effort: node.effort ?? null,
        reason: input.reason,
        operator: input.operator,
        errors: ["new occupant generation was not persisted"],
      });
      return compensation === "zero"
        ? { ok: false, code: "startup_failed", status: "failed", message: "Fresh launch could not persist a new occupant generation; the new session was rolled back." }
        : { ok: false, code: "attention_required", status: "attention_required", message: "Fresh launch could not persist a new occupant generation and the new process could not be confirmed stopped; the seat requires attention.", sessionName: canonicalSessionName, sessionId: launch.session.id };
    }
    this.activityOracle?.declareOccupantSwap(node.id, generation);

    const launchPosture = this.rigRepo.getNodePolicyProvenance(node.id)?.launchPosture
      ?? this.rigRepo.getRigPolicyProvenance(seat.rigId)?.launchPosture
      ?? "floor";
    const startupResult = await this.startupOrchestrator.startNode({
      rigId: seat.rigId,
      nodeId: node.id,
      sessionId: launch.session.id,
      binding: {
        ...launch.binding,
        cwd: node.cwd ?? ".",
        model: node.model ?? undefined,
        effort: node.effort ?? undefined,
        codexConfigProfile: node.codexConfigProfile ?? undefined,
        launchPosture,
      },
      adapter,
      plan: startup.context.plan,
      resolvedStartupFiles: startup.context.resolvedStartupFiles,
      startupActions: startup.context.startupActions,
      isRestore: false,
      includeDurableObligations: true,
      sessionName: canonicalSessionName,
      allowFreshFallback: false,
    });

    if (!startupResult.ok && startupResult.startupStatus === "failed") {
      const compensation = await this.compensateFailedFreshLaunch({
        seat,
        launch,
        supersededSessionIds,
        retiringGeneration,
        newGeneration: generation,
        startupPolicyHash: startup.context.hash,
        model: node.model,
        effort: node.effort ?? null,
        reason: input.reason,
        operator: input.operator,
        errors: startupResult.errors,
      });
      return compensation === "zero"
        ? {
            ok: false,
            code: "startup_failed",
            status: "failed",
            message: `Fresh startup failed and was rolled back to zero live session/binding: ${startupResult.errors.join("; ")}`,
          }
        : {
            ok: false,
            code: "attention_required",
            status: "attention_required",
            message: `Fresh startup failed and the new process could not be confirmed stopped; the seat requires attention: ${startupResult.errors.join("; ")}`,
            sessionName: canonicalSessionName,
            sessionId: launch.session.id,
            generation,
          };
    }

    const identity = await rebindAndVerifyPaneIdentity({
      db: this.db,
      sessionRegistry: this.sessionRegistry,
      tmux: this.tmuxAdapter,
      nodeId: node.id,
      sessionName: canonicalSessionName,
      runtime: node.runtime,
      expectedResumeToken: this.sessionResumeToken(launch.session.id),
      listProcesses: this.listProcesses,
    });
    const attentionRequired = !startupResult.ok || !identity.ok;
    if (!identity.ok) this.sessionRegistry.updateStartupStatus(launch.session.id, "attention_required");

    let persisted: PersistedEvent | null = null;
    const tx = this.db.transaction(() => {
      this.db.prepare(`
        UPDATE nodes SET
          occupant_lifecycle = 'active',
          continuity_outcome = 'fresh',
          handover_result = NULL,
          previous_occupant = ?,
          handover_at = ?
        WHERE id = ?
      `).run(retiringRows.at(-1)?.session_name ?? null, new Date().toISOString(), node.id);
      const nativeSessionId = this.sessionResumeToken(launch.session.id);
      persisted = this.eventBus.persistWithinTransaction({
        type: "seat.fresh_launched",
        rigId: seat.rigId,
        nodeId: node.id,
        logicalId: seat.logicalId,
        sessionName: canonicalSessionName,
        sessionId: launch.session.id,
        supersededSessionIds,
        retiringGeneration,
        newGeneration: generation,
        nativeSessionId,
        ...(nativeSessionId ? {} : { nativeSessionIdReason: "scrape_miss" }),
        model: node.model,
        effort: node.effort ?? null,
        startupPolicyHash: startup.context.hash,
        reason: input.reason.trim(),
        operator: input.operator ?? null,
        status: attentionRequired ? "attention_required" : "ready",
      });
    });
    tx();
    if (persisted) this.eventBus.notifySubscribers(persisted);

    if (attentionRequired) {
      return {
        ok: false,
        code: startupResult.ok ? "runtime_identity_unverified" : "attention_required",
        status: "attention_required",
        message: startupResult.ok
          ? `Fresh occupant started but runtime identity requires attention: ${identity.ok ? "unknown" : identity.detail}`
          : `Fresh occupant started but startup requires attention: ${startupResult.errors.join("; ")}`,
        sessionName: canonicalSessionName,
        sessionId: launch.session.id,
        generation,
      };
    }

    return {
      ok: true,
      seat,
      status: "ready",
      sessionName: canonicalSessionName,
      sessionId: launch.session.id,
      generation,
      model: node.model,
      effort: node.effort ?? undefined,
      startupPolicyHash: startup.context.hash,
      supersededSessionIds,
    };
  }

  // -- shared internals --

  private async stopManagedTmuxSeat(
    resolved: ResolvedSeat,
    session: LatestSessionRow,
    reason: string,
    operator?: string | null,
  ): Promise<StopSeatResult> {
    const seat = this.describe(resolved);
    const kill = await this.tmuxAdapter.killSession(session.session_name);
    if (kill && !kill.ok && kill.code !== "session_not_found") {
      return {
        ok: false,
        code: "tmux_probe_failed",
        message: `tmux kill-session for "${session.session_name}" failed: ${kill.message ?? kill.code}`,
      };
    }
    let persisted: PersistedEvent | null = null;
    const tx = this.db.transaction(() => {
      this.sessionRegistry.updateStatus(session.id, "exited");
      this.sessionRegistry.clearBinding(resolved.nodeId);
      persisted = this.eventBus.persistWithinTransaction({
        type: "session.stopped",
        rigId: seat.rigId,
        nodeId: seat.nodeId,
        sessionName: session.session_name,
        reason: reason.trim(),
        operator: operator ?? null,
      });
    });
    tx();
    this.tmuxAdapter.deliveryGuard?.rebindLifecycle(resolved.nodeId);
    if (persisted) this.eventBus.notifySubscribers(persisted);
    return { ok: true, seat, sessionName: session.session_name, sessionId: session.id };
  }

  private nonTerminalSessions(nodeId: string): LatestSessionRow[] {
    return (this.db.prepare(
      "SELECT id, session_name, status, origin FROM sessions WHERE node_id = ? ORDER BY created_at, id",
    ).all(nodeId) as LatestSessionRow[]).filter((row) => !TERMINAL_SESSION_STATUSES.has(row.status));
  }

  private sessionResumeToken(sessionId: string): string | null {
    const row = this.db.prepare("SELECT resume_token FROM sessions WHERE id = ?").get(sessionId) as
      | { resume_token: string | null }
      | undefined;
    return row?.resume_token?.trim() || null;
  }

  readStartupContext(
    nodeId: string,
    cwd: string,
  ): { ok: true; context: ParsedStartupContext } | { ok: false; refusal: SeatRefusal } {
    const row = this.db.prepare(
      "SELECT projection_entries_json, resolved_files_json, startup_actions_json, runtime FROM node_startup_context WHERE node_id = ?",
    ).get(nodeId) as PersistedStartupContextRow | undefined;
    if (!row) {
      return {
        ok: false,
        refusal: {
          ok: false,
          code: "startup_context_missing",
          message: `Persisted startup context is missing for node ${nodeId}; fresh launch refuses to invent an empty startup policy.`,
        },
      };
    }

    let rawEntries: unknown;
    let rawFiles: unknown;
    let rawActions: unknown;
    try {
      rawEntries = JSON.parse(row.projection_entries_json);
      rawFiles = JSON.parse(row.resolved_files_json);
      rawActions = JSON.parse(row.startup_actions_json);
    } catch (error) {
      return this.malformedStartupContext(nodeId, `JSON parse failed: ${error instanceof Error ? error.message : String(error)}`);
    }
    if (!Array.isArray(rawEntries) || !Array.isArray(rawFiles) || !Array.isArray(rawActions) || !row.runtime?.trim()) {
      return this.malformedStartupContext(nodeId, "projection entries, resolved files, and startup actions must be arrays and runtime must be non-empty");
    }

    const entries: ProjectionEntry[] = [];
    for (const raw of rawEntries) {
      if (!isRecord(raw)
        || !isProjectionCategory(raw["category"])
        || !hasStrings(raw, ["effectiveId", "sourceSpec", "sourcePath", "resourcePath", "absolutePath"])
        || !isOptionalString(raw["resourceType"])
        || !isOptionalString(raw["target"])
        || !isOptionalOneOf(raw["mergeStrategy"], ["managed_block", "append"] as const)
        || !isOptionalOneOf(raw["pluginType"], ["claude", "codex", "auto"] as const)) {
        return this.malformedStartupContext(nodeId, "projection_entries_json contains an invalid entry");
      }
      // S04 owns the live ambient skill set. Replaying the older catalog
      // selection here could reinstall a skill that work-install removed.
      if (raw["category"] === "skill") continue;
      // #261: shipped-spec resources follow the running install.
      entries.push(reanchorShippedProjectionEntry({
        category: raw["category"],
        effectiveId: raw["effectiveId"],
        sourceSpec: raw["sourceSpec"],
        sourcePath: raw["sourcePath"],
        resourcePath: raw["resourcePath"],
        absolutePath: raw["absolutePath"],
        classification: "safe_projection",
        ...(typeof raw["resourceType"] === "string" ? { resourceType: raw["resourceType"] } : {}),
        ...(typeof raw["mergeStrategy"] === "string" ? { mergeStrategy: raw["mergeStrategy"] as ProjectionEntry["mergeStrategy"] } : {}),
        ...(typeof raw["target"] === "string" ? { target: raw["target"] } : {}),
        ...(typeof raw["pluginType"] === "string" ? { pluginType: raw["pluginType"] as ProjectionEntry["pluginType"] } : {}),
      }));
    }

    const resolvedStartupFiles: ResolvedStartupFile[] = [];
    for (const raw of rawFiles) {
      if (!isRecord(raw)
        || !hasStrings(raw, ["path", "absolutePath", "ownerRoot"])
        || !isOneOf(raw["deliveryHint"], ["auto", "guidance_merge", "skill_install", "send_text"] as const)
        || typeof raw["required"] !== "boolean"
        || !isStringArrayOf(raw["appliesOn"], ["fresh_start", "restore"] as const)
        || !isOptionalOneOf(raw["kind"], ["file"] as const)) {
        return this.malformedStartupContext(nodeId, "resolved_files_json contains an invalid entry");
      }
      // #261: recognized built-in startup files follow the running install.
      resolvedStartupFiles.push(reanchorBuiltinStartupFile({
        path: raw["path"],
        absolutePath: raw["absolutePath"],
        ownerRoot: raw["ownerRoot"],
        deliveryHint: raw["deliveryHint"],
        required: raw["required"],
        appliesOn: raw["appliesOn"],
        ...(raw["kind"] === "file" ? { kind: "file" as const } : {}),
      }));
    }

    const startupActions: StartupAction[] = [];
    for (const raw of rawActions) {
      if (!isRecord(raw)
        || !isOneOf(raw["type"], ["slash_command", "send_text", "startup_proof"] as const)
        || typeof raw["value"] !== "string"
        || !isOneOf(raw["phase"], ["after_files", "after_ready"] as const)
        || !isStringArrayOf(raw["appliesOn"], ["fresh_start", "restore"] as const)
        || typeof raw["idempotent"] !== "boolean"
        || !isOptionalOneOf(raw["builtin"], ["session_identity"] as const)) {
        return this.malformedStartupContext(nodeId, "startup_actions_json contains an invalid entry");
      }
      startupActions.push({
        type: raw["type"],
        value: raw["value"],
        phase: raw["phase"],
        appliesOn: raw["appliesOn"],
        idempotent: raw["idempotent"],
        ...(raw["builtin"] === "session_identity" ? { builtin: "session_identity" as const } : {}),
      });
    }

    try {
      resolveStartupProof(startupActions, "fresh_start");
    } catch (err) {
      return this.malformedStartupContext(nodeId, (err as Error).message);
    }

    const startupFiles = resolvedStartupFiles.map((file) => ({
      kind: file.kind,
      path: file.path,
      deliveryHint: file.deliveryHint,
      required: file.required,
      appliesOn: file.appliesOn,
    }));
    const plan: ProjectionPlan = {
      runtime: row.runtime,
      cwd,
      entries,
      startup: { files: startupFiles, actions: startupActions },
      conflicts: [],
      noOps: [],
      diagnostics: [],
    };
    const hash = createHash("sha256")
      .update(JSON.stringify([row.projection_entries_json, row.resolved_files_json, row.startup_actions_json, row.runtime]))
      .digest("hex");
    return { ok: true, context: { plan, resolvedStartupFiles, startupActions, runtime: row.runtime, hash } };
  }

  private malformedStartupContext(
    nodeId: string,
    detail: string,
  ): { ok: false; refusal: SeatRefusal } {
    return {
      ok: false,
      refusal: {
        ok: false,
        code: "startup_context_malformed",
        message: `Persisted startup context is malformed for node ${nodeId}: ${detail}.`,
      },
    };
  }

  private async compensateFailedFreshLaunch(input: {
    seat: SeatDescriptor;
    launch: Extract<Awaited<ReturnType<NodeLauncher["launchNode"]>>, { ok: true }>;
    supersededSessionIds: string[];
    retiringGeneration: string | null;
    newGeneration: string | null;
    startupPolicyHash: string;
    model: string | null;
    effort?: string | null;
    reason: string;
    operator?: string | null;
    errors: string[];
  }): Promise<"zero" | "attention"> {
    const kill = await this.tmuxAdapter.killSession(input.launch.sessionName);
    const stopped = !kill || kill.ok || kill.code === "session_not_found";
    const errors = stopped
      ? input.errors
      : [...input.errors, `tmux kill-session failed: ${kill.message ?? kill.code}`];
    let persisted: PersistedEvent | null = null;
    const tx = this.db.transaction(() => {
      if (stopped) {
        this.sessionRegistry.updateStatus(input.launch.session.id, "exited");
        this.sessionRegistry.clearBinding(input.seat.nodeId);
        this.db.prepare(
          "UPDATE nodes SET occupant_lifecycle = 'unknown', continuity_outcome = 'failed' WHERE id = ?",
        ).run(input.seat.nodeId);
      } else {
        this.sessionRegistry.updateStartupStatus(input.launch.session.id, "attention_required");
        this.db.prepare(
          "UPDATE nodes SET occupant_lifecycle = 'active', continuity_outcome = 'fresh' WHERE id = ?",
        ).run(input.seat.nodeId);
      }
      persisted = this.eventBus.persistWithinTransaction({
        type: "seat.fresh_launch_failed",
        rigId: input.seat.rigId,
        nodeId: input.seat.nodeId,
        logicalId: input.seat.logicalId,
        sessionName: input.launch.sessionName,
        sessionId: input.launch.session.id,
        supersededSessionIds: input.supersededSessionIds,
        retiringGeneration: input.retiringGeneration,
        newGeneration: input.newGeneration,
        model: input.model,
        effort: input.effort ?? null,
        startupPolicyHash: input.startupPolicyHash,
        reason: input.reason.trim(),
        operator: input.operator ?? null,
        errors,
      });
    });
    tx();
    if (persisted) this.eventBus.notifySubscribers(persisted);
    return stopped ? "zero" : "attention";
  }

  /**
   * The ONE liveness read both mutating verbs share (fix r1, row 9baac99f):
   * the CLASSIFIED probeSession, never the collapsed hasSession view.
   *   present / absent        → returned for the verb to act on (absent is
   *                             POSITIVE tmux evidence, per OPR.0.5.4.2).
   *   transport_unavailable   → an INDETERMINATE refusal: session existence was
   *                             NOT determined, so neither verb may act — and the
   *                             refusal never routes the operator to a
   *                             destructive verb.
   *   unexpected probe throw  → the same indeterminate refusal (fail closed).
   */
  private async probeLiveness(
    sessionName: string,
    refusalConsequence: string,
  ): Promise<{ state: "present" | "absent" } | SeatRefusal> {
    let probe: SessionProbe;
    try {
      probe = await this.tmuxAdapter.probeSession(sessionName);
    } catch (err) {
      return {
        ok: false,
        code: "tmux_probe_failed",
        message: `tmux liveness probe for "${sessionName}" failed (${err instanceof Error ? err.message : String(err)}) — liveness is INDETERMINATE, so ${refusalConsequence}.`,
      };
    }
    if (probe.state === "transport_unavailable") {
      return {
        ok: false,
        code: "tmux_probe_failed",
        message: `tmux transport unavailable probing "${sessionName}" (${probe.cause}) — session existence was NOT determined (checked: classified tmux probe), so ${refusalConsequence}. Retry when the tmux transport is back.`,
      };
    }
    return { state: probe.state };
  }

  private requireReason(reason: string): SeatRefusal | null {
    if (!reason?.trim()) {
      return { ok: false, code: "missing_reason", message: "An audit reason is required (--reason)." };
    }
    return null;
  }

  private describe(resolved: ResolvedSeat): SeatDescriptor {
    return {
      rigId: resolved.entry.rigId,
      rigName: resolved.entry.rigName,
      logicalId: resolved.entry.logicalId,
      nodeId: resolved.nodeId,
    };
  }

  private latestSession(nodeId: string): LatestSessionRow | null {
    const row = this.db.prepare(
      "SELECT id, session_name, status, origin FROM sessions WHERE node_id = ? ORDER BY id DESC LIMIT 1",
    ).get(nodeId) as LatestSessionRow | undefined;
    return row ?? null;
  }

  /** The ONE resolution path, mirroring SeatStatusService.findMatches semantics
   *  (seat-status-service.ts) so every seat verb resolves identically: a canonical
   *  `name@rig` ref scopes to that rig's inventory; a bare ref scans all rigs;
   *  matches are by canonicalSessionName or logicalId; >1 match is a listed
   *  ambiguity, never a pick. */
  private resolveSeat(seatRef: string): ResolvedSeat | SeatRefusal {
    const ref = seatRef?.trim() ?? "";
    if (!ref) {
      return { ok: false, code: "seat_ref_required", message: "seat reference is required", guidance: SEAT_LOOKUP_GUIDANCE };
    }

    const matches = this.findMatches(ref);
    if (matches.length === 0) {
      return {
        ok: false,
        code: "seat_not_found",
        message: `Seat "${ref}" not found (checked: canonical session names and logical ids across ${parseSessionName(ref).kind === "canonical" ? "the named rig" : "all rigs"}).`,
        guidance: SEAT_LOOKUP_GUIDANCE,
      };
    }
    if (matches.length > 1) {
      return {
        ok: false,
        code: "seat_ambiguous",
        message: `Seat "${ref}" matched multiple nodes`,
        guidance: SEAT_LOOKUP_GUIDANCE,
        matches: matches.map((entry) => ({
          rig_name: entry.rigName,
          logical_id: entry.logicalId,
          current_occupant: entry.canonicalSessionName,
        })),
      };
    }

    const entry = matches[0]!;
    const nodeRow = this.db.prepare(
      "SELECT id FROM nodes WHERE rig_id = ? AND logical_id = ?",
    ).get(entry.rigId, entry.logicalId) as { id: string } | undefined;
    if (!nodeRow) {
      return { ok: false, code: "seat_not_found", message: `Seat "${ref}" resolved to a node that no longer exists.`, guidance: SEAT_LOOKUP_GUIDANCE };
    }
    return { entry, nodeId: nodeRow.id };
  }

  private findMatches(ref: string): NodeInventoryEntry[] {
    const parsed = parseSessionName(ref);
    if (parsed.kind === "canonical") {
      const localRef = parsed.member;
      const rigs = this.rigRepo.findUnarchivedRigsByName(parsed.rig);
      return rigs.flatMap((rig) => getNodeInventory(this.db, rig.id).filter((entry) =>
        entry.canonicalSessionName === ref
        || deriveCanonicalFromEntry(entry) === ref
        || entry.logicalId === localRef,
      ));
    }
    return this.rigRepo.listRigs().flatMap((rig) =>
      getNodeInventory(this.db, rig.id).filter((entry) =>
        entry.canonicalSessionName === ref || entry.logicalId === ref,
      ),
    );
  }

  /**
   * SAME-GENERATION Pi runner rehost (OPR.0.4.6.PI1 continuation class).
   *
   * Only the native process incarnation changes: the SAME pane, the SAME
   * persisted session file (opened with --session, never --resume/--fork) and
   * the SAME occupant generation, which travels in the pane environment set at
   * seat creation. It therefore mints no generation, no tenure and no authority,
   * rotates nothing, and calls no invalidator, so tenure, baton, claims,
   * resources, assignments, outbox rows and UNKNOWN effects stay byte-identical.
   * There is no fresh, handover, fork or blank-occupant fallback: any
   * unverifiable fact refuses, and a failed stop or resume writes a failed
   * event and stops, so a blind retry cannot happen.
   */
  async rehostRunner(input: { seatRef: string; reason: string; operator?: string | null; actorGeneration?: string; maintenanceAuthority?: import("./codex-rehost.js").OperatorMaintenanceAuthority; codexStoppedRecovery?: import("./codex-rehost.js").CodexStoppedRecovery; codexDetachedResume?: boolean; piDetachedResume?: boolean; piDetachedRecovery?: { attemptId: string; beganSha256: string }; legacyNativeWitness?: boolean; stoppedTargetRecovery?: boolean; stoppedTargetAcceptanceReference?: string; onPreEffectRefusal?: (refusal: SeatRefusal) => SeatRefusal }): Promise<RehostRunnerResult> {
    const required = this.requireReason(input.reason);
    if (required) return required;
    // EXPLICIT, DEFAULT-OFF. This boolean is the ONLY thing the option adds. No
    // witness, leaf, module path, endpoint, port, pid or cursor may be supplied by a
    // caller, and an absent value is exactly the ordinary rehost.
    const legacyNativeWitness = input.legacyNativeWitness === true;
    const stoppedTargetRecovery = input.stoppedTargetRecovery === true;
    const codexDetachedResume = input.codexDetachedResume === true;
    const piDetachedResume = input.piDetachedResume === true;
    const acceptanceReference = input.stoppedTargetAcceptanceReference?.trim() ?? "";
    if ((legacyNativeWitness && stoppedTargetRecovery)
      || (codexDetachedResume && (legacyNativeWitness || stoppedTargetRecovery || input.codexStoppedRecovery !== undefined
        || input.stoppedTargetAcceptanceReference !== undefined || input.maintenanceAuthority?.legacyCodexProfile !== undefined || piDetachedResume || input.piDetachedRecovery !== undefined)))
      return { ok: false, code: "rehost_recovery_modes_exclusive", message: "Detached Codex resume is mutually exclusive with stopped recovery, legacy profile/witness, and Pi recovery modes." };
    if (piDetachedResume && (codexDetachedResume || legacyNativeWitness || stoppedTargetRecovery || input.codexStoppedRecovery !== undefined
      || input.stoppedTargetAcceptanceReference !== undefined || input.maintenanceAuthority !== undefined || input.piDetachedRecovery !== undefined && (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(input.piDetachedRecovery.attemptId) || !/^[0-9a-f]{64}$/.test(input.piDetachedRecovery.beganSha256))))
      return { ok: false, code: "rehost_recovery_modes_exclusive", message: "Pi detached resume is exclusive with every Codex, legacy, and stopped-target recovery mode and requires a valid optional recovery reference." };
    if (!piDetachedResume && input.piDetachedRecovery !== undefined)
      return { ok: false, code: "rehost_recovery_modes_exclusive", message: "Pi recovery reference requires Pi detached resume." };
    if (stoppedTargetRecovery && !acceptanceReference)
      return { ok: false, code: "rehost_recovery_acceptance_required", message: "Stopped-target recovery requires a nonempty caller acceptance reference acknowledging possible loss of an unpersisted in-flight turn." };
    if (!stoppedTargetRecovery && input.stoppedTargetAcceptanceReference !== undefined)
      return { ok: false, code: "rehost_recovery_modes_exclusive", message: "A stopped-target acceptance reference is only valid with stopped-target recovery." };
    const resolved = this.resolveSeat(input.seatRef);
    if ("code" in resolved) return resolved;
    const seat = this.describe(resolved);
    if (piDetachedResume) {
      if (resolved.entry.runtime !== "pi") return { ok: false, code: "rehost_requires_pi_runtime", message: `Pi detached resume requires runtime 'pi', got '${resolved.entry.runtime ?? "unknown"}'.` };
      if (!this.piDetachedResume) return { ok: false, code: "rehost_unavailable", message: "Verified guarded detached-Pi resume is unavailable; no process or terminal touched." };
      const sessionName = resolved.entry.canonicalSessionName ?? (resolved.entry.logicalId ? `${resolved.entry.logicalId}@${resolved.entry.rigName}` : null);
      if (!sessionName) return { ok: false, code: "rehost_process_identity_unproven", message: "Seat has no canonical session name for Pi detached resume." };
      return this.piDetachedResume.run({ nodeId: resolved.nodeId, sessionName, reason: input.reason,
        operator: input.operator, actorGeneration: input.actorGeneration ?? "",
        ...(input.piDetachedRecovery ? { recovery: input.piDetachedRecovery } : {}) });
    }
    if (resolved.entry.runtime === "codex") {
      if (legacyNativeWitness || stoppedTargetRecovery || input.stoppedTargetAcceptanceReference !== undefined)
        return { ok: false, code: "rehost_recovery_modes_exclusive", message: "Pi recovery modes do not authorize Codex process rehost." };
      if (!this.codexRehost)
        return { ok: false, code: "rehost_unavailable", message: "Verified supervised Codex rehost dependencies are unavailable; no process touched." };
      const sessionName = resolved.entry.canonicalSessionName ?? (resolved.entry.logicalId ? `${resolved.entry.logicalId}@${resolved.entry.rigName}` : null);
      if (!sessionName)
        return { ok: false, code: "rehost_process_identity_unproven", message: "Seat has no canonical session name to rehost." };
      if (codexDetachedResume) {
        if (!this.codexRehost.resumeDetached) return { ok: false, code: "rehost_unavailable", message: "Detached Codex resume is unavailable; no process or terminal touched." };
        return this.codexRehost.resumeDetached({ nodeId: resolved.nodeId, sessionName, reason: input.reason,
          operator: input.operator, maintenanceAuthority: input.maintenanceAuthority,
          actorGeneration: input.actorGeneration ?? "" });
      }
      if (input.codexStoppedRecovery) {
        if (!this.codexRehost.recoverStopped) return {ok:false,code:"rehost_unavailable",message:"Stopped Codex recovery unavailable"};
        return this.codexRehost.recoverStopped({nodeId:resolved.nodeId,sessionName,reason:input.reason,operator:input.operator,actorGeneration:input.actorGeneration??"",maintenanceAuthority:input.maintenanceAuthority,...input.codexStoppedRecovery});
      }
      return this.codexRehost.rehost({ nodeId: resolved.nodeId, sessionName, reason: input.reason, operator: input.operator, maintenanceAuthority: input.maintenanceAuthority });
    }
    if (input.maintenanceAuthority) return {ok:false,code:"rehost_recovery_modes_exclusive",message:"Operator terminal maintenance requires actual Codex runtime"};
    if (input.codexStoppedRecovery) return {ok:false,code:"rehost_recovery_modes_exclusive",message:"Codex stopped recovery requires actual Codex runtime"};
    if (codexDetachedResume) return {ok:false,code:"rehost_recovery_modes_exclusive",message:"Detached Codex resume requires actual Codex runtime"};
    if (resolved.entry.runtime !== "pi")
      return { ok: false, code: "rehost_requires_pi_runtime", message: `Same-generation process rehost is unavailable for runtime '${resolved.entry.runtime ?? "unknown"}'.` };
    // The guard gate runs BEFORE the seam gate: a disarmed guard is a refusal on its own
    // merits and must not be masked by a missing-seam refusal. Nothing is signalled either
    // way, because both refusals precede every effect.
    const guard = this.tmuxAdapter.deliveryGuard;
    const guardPreference = (guard as unknown as { preference?: (id: string) => { desired?: boolean; effective?: boolean } } | undefined)?.preference?.(resolved.nodeId);
    if (!guard)
      return { ok: false, code: "rehost_unavailable", message: "Seat delivery guard unavailable; rehost runs under the guard lifecycle lease and refuses without it." };
    // F3, fail closed: the guard MUST expose its raw preference and BOTH flags must be on.
    // A missing preference is a refusal, never a fallback to the OR semantics of
    // protectionFacts, because a pending activation would otherwise pass.
    if (!guardPreference || guardPreference.desired !== true || guardPreference.effective !== true)
      return { ok: false, code: "rehost_guard_not_enabled", message: "Rehost requires the seat typing guard desired and effective ON (operator-visible quiescence). Enable it first with rig seat set-typing-guard." };
    if (!this.piResume || !this.piProve || !this.piRunnerState || !this.piSessionFileExists || !this.piSessionTailEntryId || !this.listProcesses)
      return { ok: false, code: "rehost_unavailable", message: "Rehost seams are not configured on this daemon; refusing instead of degrading. No runner was touched." };
    // Non-optional after the seam gate above, so the nested callbacks keep the narrowing.
    const piResume = this.piResume!, piProve = this.piProve!, piRunnerState = this.piRunnerState!;
    // `??` binds tighter than `?:`, so the original expression parsed as
    // `(canonicalSessionName ?? logicalId) ? logicalId@rigName : null` and DISCARDED the
    // canonical name, producing a derived name the runner's --session-name never matches.
    const sessionName = resolved.entry.canonicalSessionName ?? (resolved.entry.logicalId ? `${resolved.entry.logicalId}@${resolved.entry.rigName}` : null);
    if (!sessionName)
      return { ok: false, code: "rehost_process_identity_unproven", message: "Seat has no canonical session name to rehost." };

    // R5 behaviour reused unchanged, but under the DEDICATED runner-rehost lease: it
    // requires the typing guard desired AND effective ON (the operator's quiescence is what
    // makes a same-file replacement safe) instead of refusing because the guard is on.
    // It still serializes on the same tail, re-proves the binding, excludes reservations,
    // and takes no human lease. Ordinary lifecycle/input/reconcile paths are untouched.
    const outcome = await guard.runnerRehost<RehostRunnerResult>(resolved.nodeId, async (): Promise<RehostRunnerResult> => {
      // S0: EVERY precondition is re-proven inside the exclusive guard lease. A throw
      // here happens BEFORE any signal, so it is a typed refusal that signals nothing and
      // is never an UNKNOWN: no runner has been touched.
      let plan: RehostPlan | SeatRefusal;
      try {
        plan = await this.rehostPlan(resolved, sessionName, legacyNativeWitness, stoppedTargetRecovery, acceptanceReference || undefined, input.onPreEffectRefusal);
      } catch (error) {
        return { ok: false, code: "rehost_precondition_failed", message: `A precondition could not be evaluated (${(error as Error).message}); no runner was signalled and nothing was touched.`, guidance: "Read the seat state and retry only after the underlying condition is understood." };
      }
      if ("code" in plan) return plan;
      if (stoppedTargetRecovery) {
        try { plan.recoverySnapshot = this.createStoppedTargetSnapshot(plan.sessionFile); }
        catch (error) {
          return { ok: false, code: "rehost_recovery_snapshot_failed", message: `The daemon could not create and verify an owner-only pre-stop session snapshot (${(error as Error).message}); no process was signalled.` };
        }
      }
      const before = this.rehostCustodySnapshot(resolved.nodeId, plan.sessionFile);

      // S1: preserved before-record, appended before anything is stopped.
      // Pre-effect receipt. If it cannot be written, nothing has been signalled, so this
      // is a typed refusal that signals nothing rather than an UNKNOWN.
      try {
      this.appendRehostEvent("seat.runner_rehost_began", seat, input, {
        generation: plan.generation,
        sessionFile: plan.sessionFile,
        sessionFileSha256Prefix: plan.sessionFileSha256Prefix,
        lastEntryId: plan.stoppedTargetRecovery ? null : plan.lastEntryId,
        ...(plan.stoppedTargetRecovery ? { liveSidecarCursorObserved: plan.sidecarCursorObserved ?? null } : {}),
        // The legacy option states BOTH values plainly: the cursor it bypassed and the
        // leaf the daemon proved instead. It never edits the sidecar to match.
        ...(plan.legacyWitness ? {
          legacyNativeWitness: true,
          legacyWitness: { evidenceId: plan.legacyWitness.evidenceId, rounds: plan.legacyWitness.rounds },
          sidecarCursorObserved: plan.sidecarCursorObserved ?? null,
          sidecarCursorCarriedForward: false,
          leafSource: "live_child_session",
        } : { legacyNativeWitness: false }),
        ...(plan.stoppedTargetRecovery ? {
          stoppedTargetRecovery: true,
          stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference,
          liveIdleProof: "none",
          possibleUnpersistedTurnLoss: true,
          preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size },
        } : { stoppedTargetRecovery: false }),
        launchIdBefore: plan.launchId,
        runnerPid: plan.runnerPid,
        runnerChildPid: plan.runnerChildPid,
        durableModel: plan.model,
        cwd: plan.cwd,
        guardProtection: guard.protectionFacts(resolved.nodeId)?.code ?? null,
        authority: plan.authority,
        unknownEffects: before.unknownEffects,
        deliveryOrQualificationCredit: false,
        continuityCredit: false,
        leaseRepairedByThisOperation: false,
      });
      } catch (error) {
        return { ok: false, code: "rehost_receipt_unwritable", message: `The pre-effect receipt could not be written (${(error as Error).message}); ` + (plan.legacyWitness ? "the legacy witness rounds delivered diagnostic signals whose closes were verified; the runner was not stopped and no resume was typed." : "no runner was signalled and nothing was touched."), guidance: "Repair the receipt store, then re-read the seat before any rehost." };
      }

      // Stage and plan facts are declared OUTSIDE the guarded region so its catch can read
      // them, and so no nested try is needed.
      const planFacts = { generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId };
      let stage = "stop";
      let stopVerified = false;
      let signalAttempted = false;
      // R3-B2: ONE guarded region covering everything AFTER the began receipt, so the
      // runner may already be gone. Any throw from here - a failed event write at any
      // stage, a throwing resume, a failing post-proof, proof or custody snapshot - is a
      // typed UNKNOWN with blindRetryAllowed false. It never escapes as an HTTP 500 and is
      // never retried. Everything above this line is a pre-effect refusal.
      try {
      // S2: SIGTERM ONLY the verified runner pid. Never a terminal keystroke: C-c
      // into the pane is input and could abort or steer an in-flight turn.
      try {
        // Capture the pre-stop identity of BOTH processes so pid reuse cannot be
        // mistaken for survival, and so the pane root can be re-proven quiescent.
        const preStopRows = await this.listProcesses!();
        const preStopRunner = preStopRows.find(r => r.pid === plan.runnerPid);
        const preStopChild = plan.runnerChildPid === null ? undefined : preStopRows.find(r => r.pid === plan.runnerChildPid);
        // R3-B1: the pane ROOT is the AUTHORITATIVE pane pid, never derived by climbing
        // the census. Climbing reaches the shared tmux SERVER (ppid 1), whose command is
        // not a shell, so quiescence could never pass on a real pane and every rehost
        // would end as a destructive stop-unverified.
        // R3-B1: the pane ROOT is the AUTHORITATIVE pane pid only. No census climb and no
        // assumed shell parent: climbing reaches the shared tmux SERVER (ppid 1), whose
        // command is not a shell, so quiescence could never pass on a real pane. The
        // runner must provably sit under exactly that root before anything is signalled.
        const rootPid = await this.paneRootPidFor(resolved.nodeId);
        // The root's SHELL shape is proven BEFORE the signal. Without this the whole
        // kill-then-cannot-verify class returns: a real pane whose login shell ps lists
        // as `-zsh` would be signalled and then never recognised as quiescent.
        const rootRow = rootPid === null ? undefined : preStopRows.find(r => r.pid === rootPid);
        const rootIsShell = !!rootRow && SHELL_BASENAMES.has(paneShellBasename(rootRow.command) ?? "");
        if (rootPid === null || !this.ancestryReaches(preStopRows, plan.runnerPid, rootPid) || !rootIsShell)
          return { ok: false, code: "rehost_pane_root_unresolved", message: "The authoritative tmux pane pid for this seat could not be resolved, the verified runner does not sit under exactly that pane root, or that root is not a recognisable shell. " + (plan.legacyWitness ? "The legacy witness rounds delivered diagnostic signals whose closes were verified; the runner was not stopped and no resume was typed." : "No runner was signalled and nothing was touched.") + " No substitute root is assumed.", guidance: "Resolve the real pane root, confirm the runner ancestry, and re-read the seat before any rehost." };
        if (plan.stoppedTargetRecovery) {
          const exactRunner = !!preStopRunner && preStopRunner.startedAt === plan.runnerStartedAt && preStopRunner.command === plan.runnerCommand;
          const exactChild = !!preStopChild && preStopChild.startedAt === plan.childStartedAt && preStopChild.command === plan.childCommand;
          if (!plan.runnerStartedAt || !plan.childStartedAt || !exactRunner || !exactChild)
            return { ok: false, code: "rehost_recovery_identity_changed", message: "The exact runner and child identities changed or could not be established immediately before the stop signal; no process was signalled." };
          try {
            const snapshotBytes = readFileSync(plan.recoverySnapshot!.path);
            const currentBytes = readFileSync(plan.sessionFile);
            if (!snapshotBytes.equals(plan.recoverySnapshot!.bytes) || !currentBytes.equals(plan.recoverySnapshot!.bytes))
              return { ok: false, code: "rehost_recovery_history_changed", message: "The bound session file changed after its pre-stop snapshot; no process was signalled." };
          } catch {
            return { ok: false, code: "rehost_recovery_history_changed", message: "The preserved snapshot or bound session file could not be re-read immediately before the stop signal; no process was signalled." };
          }
        }
      // The EXPLICIT legacy option takes one FINAL fresh witness here, after every
      // other pre-stop check and immediately before the halt. It never stops the old
      // runner and never types a resume, but its rounds DO deliver diagnostic signals:
      // a failure after delivery is therefore the same typed UNKNOWN class as in the
      // plan, with the real pids and an outcome audit; only a failure BEFORE any
      // delivery stays a refusal, and even then the earlier plan rounds signalled.
      if (plan.legacyWitness) {
        let final: Awaited<ReturnType<SeatLifecycleService["rehostFinalWitnessAgrees"]>>;
        try {
          final = await this.rehostFinalWitnessAgrees(plan);
        } catch (error) {
          // H1-F5: an unexpected throw is NOT stop-phase SIGTERM trouble; nothing was halted.
          // Its delivery state cannot be established, so fail closed as a possibly-signalled
          // UNKNOWN without inventing pids. No retry, no kill, no resume.
          return {
            ok: false, code: "rehost_legacy_inspector_unverified",
            message: `The final legacy witness failed unexpectedly (${(error as Error).message}); its delivery state could not be established, so both targets must be treated as possibly signalled. Read the pane and the pid-scoped listeners; the old runner was not stopped and no resume was typed.`,
            blindRetryAllowed: false,
            observed: { outcomeClass: "unknown", deliveredPids: [], reasons: ["witness_inconclusive"], rounds: 0 },
          };
        }
        if (!final.ok) {
          const sig = final.signal;
          if (sig?.delivered === true && sig.closedVerified === true) {
            // H1-F2: the round COMPLETED: signals were delivered AND verifiably closed.
            // Truthful refusal with the real facts; never blind-retried.
            return {
              ok: false, code: "rehost_legacy_witness_refused",
              message: `The FINAL witness round delivered diagnostic signals (${sig.deliveredPids.length} of 2) whose closes were verified, but the sample drifted from the accepted binding (${final.reasons.join(",")}). The old runner was not stopped and no resume was typed; read the pane before any retry.`,
              guidance: "Read the pane; do not retry a legacy rehost while the runner or its child is changing.",
              blindRetryAllowed: false,
              observed: { outcomeClass: "delivered_and_closed", deliveredPids: sig.deliveredPids, auditedBeforeDelivery: sig.auditedBeforeDelivery, reasons: final.reasons, rounds: final.rounds },
            };
          }
          if (sig?.delivered === true) {
            const pids = sig.deliveredPids;
            const closeUnverified = final.reasons.includes("inspector_close_unverified");
            try {
              this.appendLegacyWitnessAudit(resolved, {
                // H1-F3: distinguishable from the plan-phase outcome event.
                stage: "final_pre_stop",
                witnessRefused: true, code: "rehost_legacy_inspector_unverified", reasons: final.reasons,
                signalDelivered: true, blindRetryAllowed: false, binding: plan.legacyWitness.binding, modules: plan.legacyWitness.modules,
                signal: { delivered: true, deliveredPids: pids, auditedBeforeDelivery: sig.auditedBeforeDelivery },
              });
            } catch {
              return {
                ok: false, code: "rehost_legacy_inspector_unverified",
                message: `The FINAL pre-stop witness delivered a diagnostic signal (${pids.length} of 2) and the outcome audit could NOT be written, so its result is uncertain (${final.reasons.join(",")}). Read the pane and the pid-scoped listeners before anything else.`,
                blindRetryAllowed: false,
                observed: { outcomeClass: "unknown", outcomeAuditWritten: false, deliveredPids: pids, auditedBeforeDelivery: sig.auditedBeforeDelivery, closeUnverified, reasons: final.reasons, rounds: final.rounds },
              };
            }
            const who = pids.length === 2 ? "this runner and its pi child" : pids.length === 1 ? "one of this runner's processes" : "no recorded process";
            const closeState = closeUnverified ? "the close could not be verified" : "the outcome after signalling is not fully known";
            return {
              ok: false, code: "rehost_legacy_inspector_unverified",
              message: `The FINAL pre-stop witness delivered a diagnostic signal to ${who} (${pids.length} of 2) and ${closeState} (${final.reasons.join(",")}). The old runner was NOT stopped and no resume was typed; read the pane and the pid-scoped listeners before anything else.`,
              blindRetryAllowed: false,
              // H1-F4: same observed shape as the audit-failed variant.
              observed: { outcomeClass: "unknown", outcomeAuditWritten: true, deliveredPids: pids, auditedBeforeDelivery: sig.auditedBeforeDelivery, closeUnverified, reasons: final.reasons, rounds: final.rounds },
            };
          }
          return { ok: false, code: "rehost_legacy_witness_refused", message: `The final pre-stop witness did not reproduce the accepted binding and leaf (${final.reasons.join(",")}). No NEW signal was delivered by this round, though the legacy witness rounds delivered diagnostic signals whose closes were verified; the old runner was not stopped and no resume was typed.`, guidance: "Read the pane; do not retry a legacy rehost while the runner or its child is changing.", observed: { reasons: final.reasons, rounds: final.rounds } };
        }
      }
        signalAttempted = true;
        this.killNativeProcess?.(plan.runnerPid);
        stopVerified = await this.rehostRunnerExited(rootPid, plan.runnerPid, plan.runnerChildPid, { runnerStartedAt: preStopRunner?.startedAt, childStartedAt: preStopChild?.startedAt });
      } catch (error) {
        this.appendRehostEvent("seat.runner_rehost_failed", seat, input, {
          stage: "stop", generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId,
          ...(plan.stoppedTargetRecovery ? { stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference, liveIdleProof: "none", preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size }, possibleUnpersistedTurnLoss: true } : {}),
          observed: { runnerPid: plan.runnerPid, error: (error as Error).message, runnerGone: false },
          blindRetryAllowed: false, fallbackTaken: "none",
          note: "SIGTERM delivery to the verified runner pid did not complete; the pane may still hold the old runner. A human must read the pane before any further rehost.",
        });
        return { ok: false, code: "rehost_stop_unverified", message: "Runner stop could not be verified; the old runner may still be live. No resume was attempted, no fallback taken, and a blind retry is not permitted.", ...(plan.stoppedTargetRecovery ? { blindRetryAllowed: false, observed: { outcomeClass: "stop_unknown", stoppedTargetRecovery: true, preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size } } } : {}) };
      }
      if (!stopVerified) {
        this.appendRehostEvent("seat.runner_rehost_failed", seat, input, {
          stage: "stop", generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId,
          ...(plan.stoppedTargetRecovery ? { stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference, liveIdleProof: "none", preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size }, possibleUnpersistedTurnLoss: true } : {}),
          observed: { runnerPid: plan.runnerPid, runnerChildPid: plan.runnerChildPid, runnerGone: false, runnerStillListed: true },
          blindRetryAllowed: false, fallbackTaken: "none",
          note: "The runner process was still listed after the bounded wait; stop effect is UNKNOWN. Do not rehost again until the pane is read.",
        });
        return { ok: false, code: "rehost_stop_unverified", message: "Runner exit was not observed within the bounded wait; the stop effect is UNKNOWN. No resume attempted, no fallback taken, no blind retry.", ...(plan.stoppedTargetRecovery ? { blindRetryAllowed: false, observed: { outcomeClass: "stop_unknown", stoppedTargetRecovery: true, preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size } } } : {}) };
      }

      if (plan.stoppedTargetRecovery) {
        const stoppedBytes = readFileSync(plan.sessionFile);
        const originalBytes = plan.recoverySnapshot!.bytes;
        if (stoppedBytes.length < originalBytes.length || !stoppedBytes.subarray(0, originalBytes.length).equals(originalBytes)) {
          this.appendRehostEvent("seat.runner_rehost_failed", seat, input, {
            stage: "post_exit_history", generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId,
            stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference, liveIdleProof: "none",
            preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size },
            observed: { currentSize: stoppedBytes.length, preservedSize: originalBytes.length, preservedPrefixMatches: false },
            blindRetryAllowed: false, fallbackTaken: "none", possibleUnpersistedTurnLoss: true,
            note: "The stopped session history was truncated or changed before the preserved snapshot prefix; no resume was attempted. The recovery snapshot remains available.",
          });
          return { ok: false, code: "rehost_recovery_history_changed", message: "After both old processes exited, the session file was truncated or changed before the preserved snapshot prefix. No resume was attempted; the private recovery snapshot remains available.", blindRetryAllowed: false, observed: { outcomeClass: "stopped_no_resume", oldProcessesExited: true, resumeAttempted: false, preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size } } };
        }
        const leaf = this.piSessionTailEntryId!(plan.sessionFile);
        if (!leaf) {
          this.appendRehostEvent("seat.runner_rehost_failed", seat, input, {
            stage: "post_exit_history", generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId,
            stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference, liveIdleProof: "none",
            preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size },
            observed: { currentSize: stoppedBytes.length, preservedSize: originalBytes.length, tailAvailable: false },
            blindRetryAllowed: false, fallbackTaken: "none", possibleUnpersistedTurnLoss: true,
            note: "No complete post-exit session leaf could be derived; no resume was attempted. The recovery snapshot remains available.",
          });
          return { ok: false, code: "rehost_recovery_history_changed", message: "No complete post-exit session leaf could be derived. No resume was attempted; the private recovery snapshot remains available.", blindRetryAllowed: false, observed: { outcomeClass: "stopped_no_resume", oldProcessesExited: true, resumeAttempted: false, preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size } } };
        }
        plan.stoppedTargetLeaf = leaf;
        plan.stoppedTargetBytes = Buffer.from(stoppedBytes);
        plan.recoveryAppendedBytes = stoppedBytes.length - originalBytes.length;
        plan.recoveryAppendedSha256 = createHash("sha256").update(stoppedBytes.subarray(originalBytes.length)).digest("hex");
      }

      stage = "resume";
      // S3: reopen the SAME file through the shipped resume primitive.
      const resumed = await piResume.resume(sessionName, "pi_session_file", plan.sessionFile, plan.cwd, plan.model, plan.posture, plan.generation);
      if (!resumed.ok) {
        const stoppedTargetFailure = plan.stoppedTargetRecovery ? {
          stoppedTargetRecovery: true,
          stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference,
          liveIdleProof: "none",
          outcomeClass: "stopped_resume_effect_unknown",
          oldProcessesExited: true,
          resumeAttempted: true,
          leafSource: "post_exit_session_file",
          stoppedTargetLeaf: plan.stoppedTargetLeaf,
          preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size },
          appendedBytes: plan.recoveryAppendedBytes ?? 0,
          appendedBytesSha256: plan.recoveryAppendedSha256,
          possibleUnpersistedTurnLoss: true,
        } : {};
        const message = plan.stoppedTargetRecovery
          ? `Both old processes exited and same-file resume was attempted, but the stopped-target outcome is UNKNOWN (${resumed.code ?? "unknown"}). The private recovery snapshot remains available; the typing guard stays ON, no fallback was taken, and a blind retry is not permitted.`
          : `Same-file resume did not complete (${resumed.code ?? "unknown"}). The typing guard stays ON, nothing was flushed or retried, and no fresh, handover or fork fallback was taken.`;
        this.appendRehostEvent("seat.runner_rehost_failed", seat, input, {
          stage: "resume", generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId,
          ...stoppedTargetFailure,
          observed: {
            code: resumed.code ?? null, message: resumed.message ?? null,
            ...(plan.stoppedTargetRecovery ? { oldProcessesExited: true, resumeAttempted: true, outcomeClass: "stopped_resume_effect_unknown" } : { runnerGone: true }),
          },
          blindRetryAllowed: false,
          fallbackTaken: "none",
          note: plan.stoppedTargetRecovery
            ? "Both exact old processes exited before resume was attempted. The resume primitive reported failure, so the post-signal outcome is UNKNOWN; retain the private snapshot and do not retry or fall back."
            : resumed.code === "retry_fresh"
            ? "Session file missing: stop-and-ask. This operation never falls back to fresh, handover or fork."
            : "Resume did not complete. The guard stays ON and held messages stay held; no retry from this invocation.",
        });
        return {
          ok: false,
          code: plan.stoppedTargetRecovery ? "rehost_effect_unknown" : "rehost_resume_failed",
          message,
          ...(plan.stoppedTargetRecovery ? {
            blindRetryAllowed: false,
            guidance: "Read the pane and runner launch id before any further recovery; do not retry this stopped-target rehost.",
            observed: {
              effectApplied: true,
              fallbackTaken: "none",
              ...stoppedTargetFailure,
              resumeCode: resumed.code ?? null,
              resumeMessage: resumed.message ?? null,
            },
          } : {}),
        };
      }

      // S4: post-proof. Process identity, launch scope, file equality, generation,
      // and a READ-ONLY custody comparison against the S1 snapshot.
      //
      // BOUNDED SETTLING WINDOW. A freshly resumed runner publishes its new launch id
      // and ready state asynchronously, so ONE sample taken immediately after resume can
      // transiently disagree with the settled state. We therefore sample the sidecar and
      // the prover within a small bounded window and take the FIRST sample on which the
      // sidecar launch id, the session-file identity and the prover launch id AGREE.
      //
      // This widens OBSERVATION only. It never repeats the stop or the resume, and the
      // fences below are unchanged: wrong generation, wrong session file, a launch id
      // that is not new, or the OLD launch still live all still fail closed.
      type PostSample = { post: PiRehostRunnerState | null; proof: PiRehostProof | null; launchIdAfter: string | null; history: ReturnType<typeof stoppedHistoryProof> | null; ok: boolean };
      const samples: PostSample[] = [];
      let settled: PostSample | null = null;
      for (let attempt = 1; attempt <= this.postProofSettleAttempts; attempt++) {
        if (attempt > 1) await new Promise<void>(resolve => setTimeout(resolve, this.postProofSettleGapMs));
        const post = piRunnerState(sessionName);
        const proof = await piProve(sessionName);
        const launchIdAfter = post?.launchId ?? null;
        const history = plan.stoppedTargetRecovery ? stoppedHistoryProof(plan, sessionName) : null;
        // Agreement = the new launch is published and BOTH readers name the SAME launch
        // id, the session file is the one we resumed, and the prover shows the exact plan
        // generation. A null prover or an unready sidecar is simply not agreement yet.
        const ok =
          !!post && post.ready && post.sessionFile === plan.sessionFile &&
          !!launchIdAfter && launchIdAfter !== plan.launchId &&
          proof?.state === "present" && proof.generation === plan.generation && proof.launchId === launchIdAfter &&
          // The EXPLICIT legacy option additionally requires the replacement to have
          // ACTUALLY refreshed its cursor to the bound leaf, which must still be the
          // bounded file tail. A stale or absent cursor is a mismatch, never credit.
          this.legacyPostCursorAgrees(sessionName, plan) &&
          (!plan.stoppedTargetRecovery || (history?.valid === true && post.lastEntryId === history.resultingLeaf && this.piSessionTailEntryId!(plan.sessionFile) === history.resultingLeaf));
        const sample: PostSample = { post, proof, launchIdAfter, history, ok };
        samples.push(sample);
        // Positive identity contradictions are terminal; later agreement cannot
        // erase a wrong generation/file or a proof of the old launch still present.
        if (history?.valid === false || (post?.sessionFile != null && post.sessionFile !== plan.sessionFile) ||
            (proof != null && proof.generation !== plan.generation) ||
            (proof?.state === "present" && proof.launchId === plan.launchId) ||
            // A cursor that has settled on a DIFFERENT entry than the witnessed leaf is
            // a positive contradiction about history, never a startup transient.
            (!!plan.legacyWitness && post?.ready === true && post.lastEntryId != null && post.lastEntryId !== plan.legacyWitness.nativeLeaf)) break;
        if (ok) { settled = sample; break; }
      }
      const post = settled?.post ?? samples[samples.length - 1]?.post ?? null;
      const proof = settled?.proof ?? samples[samples.length - 1]?.proof ?? null;
      const launchIdAfter = settled?.launchIdAfter ?? samples[samples.length - 1]?.launchIdAfter ?? null;
      // Every disagreement observed inside the window must name the SAME thing. If the
      // disagreement CHANGES across samples it is not a settling startup artifact.
      const disagreementShapes = new Set(samples.filter(s => !s.ok).map(s => JSON.stringify({
        sidecarReady: s.post?.ready ?? null,
        sidecarLaunchId: s.post?.launchId ?? null,
        sidecarSessionFile: s.post?.sessionFile ?? null,
        proofState: s.proof?.state ?? null,
        proofGeneration: s.proof?.generation ?? null,
        proofLaunchId: s.proof?.launchId ?? null,
      })));
      const disagreementStable = disagreementShapes.size <= 1;
      // A GENUINE CONTRADICTION is a disagreement that is itself a real identity
      // violation rather than an unsettled observation. These keep failing closed.
      const genuineContradiction =
        samples.some(s => s.post?.sessionFile != null && s.post.sessionFile !== plan.sessionFile) ||
        samples.some(s => s.proof != null && s.proof.generation !== plan.generation) ||
        samples.some(s => s.proof != null && s.proof.state === "present" && s.proof.launchId === plan.launchId) ||
        (!!post && post.ready && !!launchIdAfter && launchIdAfter === plan.launchId);
      const history = settled?.history ?? samples.at(-1)?.history ?? null;
      const historyContradiction = samples.some(s => s.history?.valid === false);
      const postProof = settled?.ok === true && !genuineContradiction && !historyContradiction;
      const after = this.rehostCustodySnapshot(resolved.nodeId, plan.sessionFile);
      const custodyUnchanged =
        after.tenantHash === before.tenantHash && after.resumeTokenHash === before.resumeTokenHash &&
        after.authorityHash === before.authorityHash && after.claimHash === before.claimHash &&
        after.unknownEffects.digest === before.unknownEffects.digest && after.unknownEffects.count === before.unknownEffects.count;
      if (!postProof || !custodyUnchanged) {
        // A settling disagreement that never became a genuine contradiction gets its own
        // TYPED code and carries the sampled values, so a startup transient is never
        // indistinguishable from a real identity contradiction. The failed receipt is
        // written exactly as before and is preserved; nothing is retried.
        const unstable = !postProof && !genuineContradiction && !historyContradiction && samples.length > 0;
        this.appendRehostEvent("seat.runner_rehost_failed", seat, input, {
          stage: "post_proof", generation: plan.generation, sessionFile: plan.sessionFile, launchIdBefore: plan.launchId, launchIdAfter,
          ...(plan.stoppedTargetRecovery ? {
            stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference,
            liveIdleProof: "none", leafSource: "post_exit_session_file", stoppedTargetLeaf: plan.stoppedTargetLeaf,
            historyProof: history,
            preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size },
            appendedBytes: plan.recoveryAppendedBytes ?? 0, appendedBytesSha256: plan.recoveryAppendedSha256,
            possibleUnpersistedTurnLoss: true,
          } : {}),
          observed: {
            postProof, custodyUnchanged, proofState: proof?.state ?? null, proofGeneration: proof?.generation ?? null,
            proofLaunchId: proof?.launchId ?? null, sidecarReady: post?.ready ?? null, sidecarSessionFile: post?.sessionFile ?? null,
            settlingSamples: samples.length, settlingAttempts: this.postProofSettleAttempts,
            disagreementStable, genuineContradiction,
            samples: samples.map(s => ({ sidecarReady: s.post?.ready ?? null, sidecarLaunchId: s.post?.launchId ?? null, proofState: s.proof?.state ?? null, proofGeneration: s.proof?.generation ?? null, proofLaunchId: s.proof?.launchId ?? null })),
          },
          blindRetryAllowed: false, fallbackTaken: "none",
          note: unstable
            ? "Post-rehost observation did not settle inside the bounded read-only window and is not a proven identity contradiction; reported as unstable, never repaired and never retried."
            : "Post-rehost proof or custody comparison failed; reported, never repaired by this operation.",
        });
        return {
          ok: false,
          code: postProof ? "rehost_custody_drift" : unstable ? "rehost_post_proof_unstable" : "rehost_post_proof_failed",
          message: postProof
            ? "Custody, generation or UNKNOWN-effect comparison failed after resume; reported only, never repaired."
            : unstable
              ? `Post-resume observation did not settle within ${this.postProofSettleAttempts} read-only samples and shows no proven identity contradiction; reported as unstable only, never repaired and never retried.`
              : "Post-resume process proof failed (launch scope, session-file equality or same-generation proof). Reported only, never repaired.",
          observed: {
            ...(plan.stoppedTargetRecovery ? { outcomeClass: "effect_unknown", oldProcessesExited: true, resumeAttempted: true, historyProof: history, preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size } } : {}),
            settlingSamples: samples.length, disagreementStable, genuineContradiction,
          },
          ...(plan.stoppedTargetRecovery ? { blindRetryAllowed: false } : {}),
        };
      }

      // S5: completed receipt. The guard deliberately stays ON. It sits inside the single
      // guarded post-effect region opened at S3, so a failed write here is an UNKNOWN too.
      this.appendRehostEvent("seat.runner_rehost_completed", seat, input, {
        generation: plan.generation,
        generationUnchanged: true,
        sessionFile: plan.sessionFile,
        sessionFileUnchanged: true,
        launchIdBefore: plan.launchId,
        launchIdAfter,
        durableModel: plan.model,
        guardLeftEnabled: true,
        ...(plan.legacyWitness ? {
          legacyNativeWitness: true,
          legacyWitness: { evidenceId: plan.legacyWitness.evidenceId, rounds: plan.legacyWitness.rounds },
          // Post-proof credits the REFRESHED cursor reaching the witnessed leaf; the
          // old UI-UUID cursor is still not a session entry and is not credited.
          cursorRefreshedToLeaf: true,
          historicalProjectionCredit: false,
        } : { legacyNativeWitness: false }),
        ...(plan.stoppedTargetRecovery ? {
          stoppedTargetRecovery: true,
          stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference,
          liveIdleProof: "none",
          stoppedTargetLeaf: plan.stoppedTargetLeaf,
          leafSource: "post_exit_session_file",
          replacementCursorMatchesPostExitLeaf: history?.resultingLeaf === plan.stoppedTargetLeaf,
          replacementCursorMatchesValidatedLeaf: true,
          historyProof: history,
          preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size },
          appendedBytes: plan.recoveryAppendedBytes ?? 0,
          appendedBytesSha256: plan.recoveryAppendedSha256,
          appendedBytesAcceptedBecauseReplacementCursorBindsLeaf: true,
          possibleUnpersistedTurnLoss: true,
        } : { stoppedTargetRecovery: false }),
        authority: plan.authority,
        authorityReadOnly: true,
        leaseRepairedByThisOperation: false,
        unknownEffects: after.unknownEffects,
        deliveryOrQualificationCredit: false,
        continuityCredit: false,
        nextActorIsGenuineHolder: "acknowledge then renew; for an EXPIRED reconciling lease use the single per-epoch reconciliation-recover window, which this operation never touches",
      });
      return {
        ok: true,
        seat,
        generation: plan.generation,
        generationUnchanged: true,
        sessionFile: plan.sessionFile,
        launchIdBefore: plan.launchId,
        launchIdAfter,
        durableModel: plan.model,
        unknownEffectsPreserved: after.unknownEffects,
        authority: plan.authority ? { ...plan.authority, readOnly: true, repairedByThisOperation: false } : null,
        guardLeftEnabled: true,
        ...(plan.stoppedTargetRecovery ? { stoppedTargetRecovery: true, stoppedTargetLeaf: plan.stoppedTargetLeaf, historyProof: history, possibleUnpersistedTurnLoss: true, preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size } } : {}),
        events: ["seat.runner_rehost_began", "seat.runner_rehost_completed"],
      };
      } catch (error) {
        if (plan.stoppedTargetRecovery && !signalAttempted)
          return { ok: false, code: "rehost_precondition_failed", message: `A stopped-target precondition could not be re-established (${(error as Error).message}); no process was signalled and the recovery snapshot remains available.` };
        return {
          ok: false,
          code: "rehost_effect_unknown",
          message: "An effect was already applied for this seat (runner signalled or replaced) and a later step failed, so the rehost outcome is UNKNOWN. Do not retry: read the pane and the runner launch id before any further rehost.",
          blindRetryAllowed: false,
          guidance: "Read the pane and the runner launch id before any further rehost.",
          observed: { stage, effectApplied: true, generation: planFacts.generation, sessionFile: planFacts.sessionFile, launchIdBefore: planFacts.launchIdBefore, guardLeftEnabled: true, fallbackTaken: "none", ...(plan.stoppedTargetRecovery ? { stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: plan.stoppedTargetAcceptanceReference, liveIdleProof: "none", preservedSessionSnapshot: { path: plan.recoverySnapshot!.path, sha256: plan.recoverySnapshot!.sha256, size: plan.recoverySnapshot!.size }, possibleUnpersistedTurnLoss: true } : {}), error: (error as Error).message },
        };
      }
    });
    return outcome;
  }

  /** Create a daemon-owned, owner-only copy of the exact bound session bytes. */
  private createStoppedTargetSnapshot(sessionFile: string): NonNullable<RehostPlan["recoverySnapshot"]> {
    const directory = this.piRecoverySnapshotDirectory;
    if (!directory) throw new Error("private daemon snapshot directory is unavailable");
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const directoryStat = lstatSync(directory);
    if (!directoryStat.isDirectory() || directoryStat.isSymbolicLink()) throw new Error("snapshot directory is not a real directory");
    const directoryMode = directoryStat.mode & 0o777;
    if ((directoryMode & 0o077) !== 0) throw new Error("snapshot directory permissions are not private");
    const bytes = readFileSync(sessionFile);
    const path = resolve(directory, `stopped-target-${randomUUID()}.snapshot`);
    let created = false;
    try {
      writeFileSync(path, bytes, { flag: "wx", mode: 0o600 });
      created = true;
      const snapshotStat = lstatSync(path);
      if (!snapshotStat.isFile() || snapshotStat.isSymbolicLink() || (snapshotStat.mode & 0o777) !== 0o600)
        throw new Error("snapshot file is not a private regular file");
      const copied = readFileSync(path);
      if (!bytes.equals(copied)) throw new Error("snapshot bytes failed exact verification");
      return { path, sha256: createHash("sha256").update(copied).digest("hex"), size: copied.byteLength, bytes: copied };
    } catch (error) {
      if (created) try { unlinkSync(path); } catch { /* preserve the original failure */ }
      throw error;
    }
  }

  /** Every rehost precondition, proven fresh. No partial acceptance. */
  private async rehostPlan(resolved: ResolvedSeat, sessionName: string, legacyNativeWitness: boolean, stoppedTargetRecovery: boolean, stoppedTargetAcceptanceReference?: string, onPreEffectRefusal?: (refusal: SeatRefusal) => SeatRefusal): Promise<RehostPlan | SeatRefusal> {
    const { nodeId } = resolved;
    if (this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state <> 'released' LIMIT 1").get(nodeId))
      return { ok: false, code: "rehost_reservation_active", message: "An unreleased dispatch reservation fences rehost; its cutover disposition must be settled first." };
    const guard = this.tmuxAdapter.deliveryGuard;
    // F3: the architecture gate is desired AND effective ON. protectionFacts reports
    // typing_guard_enabled for either alone, so a pending activation would pass it;
    // Re-proven inside the exclusive guard lease (S0), still fail closed on both flags.
    const recheckPreference = (guard as unknown as { preference?: (id: string) => { desired?: boolean; effective?: boolean } } | undefined)?.preference?.(nodeId);
    if (!recheckPreference || recheckPreference.desired !== true || recheckPreference.effective !== true)
      return { ok: false, code: "rehost_guard_not_enabled", message: "Rehost requires the seat typing guard desired and effective ON (operator-visible quiescence). Enable it first with rig seat set-typing-guard." };
    if (this.db.prepare("SELECT 1 FROM outbox_entries WHERE destination_session=? AND delivery_state='sending' LIMIT 1").get(sessionName))
      return { ok: false, code: "rehost_outbox_sending", message: "An effect is in flight (sending) to this seat; rehost refuses until it resolves. Indeterminate/UNKNOWN rows are allowed and are only read." };

    const tenure = this.db.prepare("SELECT id, generation_uuid FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal DESC LIMIT 1").get(nodeId) as { id: string; generation_uuid: string } | undefined;
    if (!tenure)
      return { ok: false, code: "rehost_generation_mismatch", message: "No occupant tenure exists for this node; rehost never mints one." };
    const generation = tenure.generation_uuid;

    const session = this.db.prepare("SELECT id, status, resume_token, resume_type FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(nodeId) as { id: string; status: string; resume_token: string | null; resume_type: string | null } | undefined;
    if (!session || !session.resume_token?.trim())
      return { ok: false, code: "rehost_sidecar_unverified", message: "Latest session carries no resume token; same-history rehost cannot proceed and never invents one." };
    if ((session.resume_type ?? "") !== "pi_session_file")
      return { ok: false, code: "rehost_sidecar_unverified", message: `Resume token type '${session.resume_type ?? "unknown"}' is not pi_session_file; rehost never converts a token type.` };
    const sessionFile = session.resume_token.trim();

    const authorityRow = this.db.prepare("SELECT state, lease_until, epoch, owner_session, owner_generation FROM coordinator_authority WHERE owner_session=?").get(sessionName) as { state: string; lease_until: number; epoch: number; owner_session: string; owner_generation: string } | undefined;
    const authority = authorityRow
      ? { state: authorityRow.state, leaseUntil: authorityRow.lease_until, epoch: authorityRow.epoch, ownerGeneration: authorityRow.owner_generation, generationMatchesOwner: authorityRow.owner_generation === generation, expired: authorityRow.lease_until <= Date.now() }
      : null;
    if (authorityRow && authorityRow.owner_generation !== generation)
      return { ok: false, code: "rehost_generation_mismatch", message: "Coordinator authority names a different owner generation than the node's latest tenure; rehost refuses rather than acting under drifted authority." };

    const sidecar = this.piRunnerState!(sessionName);
    if (!sidecar || !sidecar.ready || !sidecar.launchId || sidecar.sessionFile !== sessionFile)
      return { ok: false, code: "rehost_sidecar_unverified", message: "Runner sidecar is missing, not ready, or does not name the stored resume token exactly; rehost refuses instead of guessing the live session file." };
    const launchId = sidecar.launchId;
    if (!this.piSessionFileExists!(sessionFile))
      return { ok: false, code: "rehost_session_file_missing", message: "The persisted session file no longer exists; stop-and-ask. Rehost never falls back to a fresh, forked or blank occupant." };

    // F1/F2/F3. The prover yields null for ~14 distinct UNKNOWN causes, so a null is
    // UNKNOWN OBSERVATION, never a positive identity mismatch. Re-observe a bounded
    // number of extra times, read-only and BEFORE any kill, to absorb a transient
    // single-call probe failure. A persistent null still fails closed.
    let proof: PiRehostProof | null = await this.piProve!(sessionName);
    let observations = 1;
    while (!proof && observations <= this.rehostReobserveAttempts) {
      await new Promise<void>(resolve => setTimeout(resolve, this.rehostReobserveGapMs));
      observations++;
      proof = await this.piProve!(sessionName);
    }
    if (!proof || proof.state !== "present" || proof.generation !== generation || proof.launchId !== launchId) {
      const refusal: SeatRefusal = !proof
        ? { ok: false, code: "rehost_process_identity_unknown", message: `Live pi process identity could not be OBSERVED for this seat after ${observations} read-only attempt(s); observation was inconclusive, so rehost refuses before touching any process.`, observed: { observations } }
        : { ok: false, code: "rehost_process_identity_unproven", message: "Live pi process identity was observed but does not match this exact launch id and generation; rehost refuses before touching any process.", observed: { state: proof.state, generation: proof.generation, launchId: proof.launchId, expectedGeneration: generation, expectedLaunchId: launchId, observations } };
      return onPreEffectRefusal ? onPreEffectRefusal(refusal) : refusal;
    }

    // F4/F5: read the sidecar cursor TWICE and require it stable, then read a BOUNDED
    // positional tail. The shared prover double-samples identity but exposes lastEntryId
    // only indirectly, so the cursor is proven stable here. A tail that is overlong or
    // ends in a partial line is refused rather than parsed optimistically.
    const cursorFirst = this.piRunnerState!(sessionName)?.lastEntryId ?? null;
    const tail = stoppedTargetRecovery ? null : this.piSessionTailEntryId!(sessionFile);
    const cursorSecond = stoppedTargetRecovery ? cursorFirst : this.piRunnerState!(sessionName)?.lastEntryId ?? null;

    const rows = await this.listProcesses!();
    const escaped = sessionName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const runners = rows.filter(r => r.command.includes("pi-runner.js") && new RegExp(`--session-name\\s+'?${escaped}'?(\\s|$)`).test(r.command) && new RegExp(`--launch-id\\s+'?${launchId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}'?(\\s|$)`).test(r.command));
    if (runners.length !== 1)
      return { ok: false, code: "rehost_runner_pid_unresolved", message: `Expected exactly one live runner process for this launch id, found ${runners.length}; rehost refuses rather than signalling an ambiguous pid.` };
    const runner = runners[0]!;
    // The runner's pi child is the census row whose parent is the runner pid.
    // SOLE census child: the plan and the receipt both promise exactly one. Zero or several is
    // UNKNOWN about which child the target is, so it refuses and records the observed count.
    const childRows = rows.filter(r => r.ppid === runner.pid && r.pid !== runner.pid);
    if (childRows.length !== 1)
      return {
        ok: false, code: "rehost_legacy_witness_unavailable",
        message: childRows.length === 0
          ? "No pi child of the legacy runner was found in the census, so the witness target cannot be established."
          : `The legacy runner has ${childRows.length} census children, not exactly one, so which process the witness binds is UNKNOWN.`,
        observed: { censusChildCount: childRows.length },
      };
    const child = childRows[0] ?? null;

    // The EXPLICIT legacy native-witness option bypasses ONE gate only: the sidecar
    // cursor equality. That equality is invalid for this old runner, which persists a
    // generic event id (often a UI UUID) into its durable cursor, so the cursor can
    // name no session entry at all. Everything else above — reservation, guard,
    // sending, tenure, session-token type, authority generation, sidecar identity,
    // session-file existence and the shipped prover — still refuses unchanged.
    let legacyWitness: LegacyPiRehostWitnessFacts | null = null;
    if (legacyNativeWitness) {
      const binding = await this.legacyWitnessBinding(runner, child, { launchId, generation, sessionFile });
      if (!binding.ok)
        return { ok: false, code: "rehost_legacy_witness_unavailable", message: `The explicit legacy native-witness option cannot be established for this runner (${binding.blocker}). No module path, endpoint or pid was guessed, no witness was accepted from the caller, no runner was signalled and nothing was touched.`, guidance: "Establish the exact native module mapping and the default private inspector startup configuration, then re-read the seat before any legacy rehost.", observed: { blocker: binding.blocker } };
      // The seat identity comes from the plan's own verified resolution. Nothing extra is read and
      // no caller supplies it.
      const proven = await this.legacyPiWitness!.witness({
        binding: binding.binding, modules: binding.modules,
        seat: { rigId: resolved.entry.rigId, nodeId: resolved.nodeId, logicalId: resolved.entry.logicalId },
      });
      if (!proven.ok) {
        // B5 HONEST WORDING AND CLASS. A refusal AFTER a SIGUSR1 was delivered is not a pre-effect
        // refusal: live processes were signalled and a listener may still be open. When the close is
        // unverified, return a typed UNKNOWN refusal with blindRetryAllowed false, and record both
        // the delivery and its outcome durably so the operator reads the pane and listeners first.
        // C2: the collector reports what ACTUALLY happened to a signal in this run. This is never
        // inferred from the reason list: reasons like inspector_unavailable after a failed attach,
        // inspector_listener_unverified, or a target-identity drift AFTER delivery are all
        // post-signal, and the old string check called them "nothing was touched".
        const delivered = proven.signal?.delivered === true;
        const signalled = delivered;
        if (signalled) {
          // G2: a failed OUTCOME audit after a real delivery must NOT surface as a pre-effect
          // refusal. The intent audit written before delivery still stands, so the signal is not
          // unrecorded, but the outcome is uncertain and the refusal must say so with the real pids.
          try {
            this.appendLegacyWitnessAudit(resolved, {
              witnessRefused: true, code: "rehost_legacy_inspector_unverified", reasons: proven.reasons,
              signalDelivered: true, blindRetryAllowed: false, binding: binding.binding, modules: binding.modules,
              // F3: carry the real facts through to the durable record.
              signal: { delivered: true, deliveredPids: proven.signal?.deliveredPids ?? [], auditedBeforeDelivery: proven.signal?.auditedBeforeDelivery === true },
            });
          } catch {
            const audited = proven.signal?.auditedBeforeDelivery === true;
            return {
              ok: false, code: "rehost_legacy_inspector_unverified",
              message: `A diagnostic signal was delivered (${(proven.signal?.deliveredPids ?? []).length} of 2) and the outcome audit could NOT be written, so its result is uncertain (${proven.reasons.join(",")}). Read the pane and the pid-scoped listeners before anything else.`,
              blindRetryAllowed: false,
              observed: {
                outcomeClass: "unknown", outcomeAuditWritten: false,
                deliveredPids: proven.signal?.deliveredPids ?? [],
                auditedBeforeDelivery: audited,
                closeUnverified: proven.reasons.includes("inspector_close_unverified"),
                reasons: proven.reasons,
              },
            };
          }
          // F4 HONEST WORDING AND CLASS. Name only the pids that were actually signalled, and only
          // claim the close is unverified when a close reason is really present. It stays a typed
          // UNKNOWN, non-retryable outcome: nothing here ever retries automatically.
          const signalledPids = proven.signal?.deliveredPids ?? [];
          const closeUnverified = proven.reasons.includes("inspector_close_unverified");
          const who = signalledPids.length === 2 ? "this runner and its pi child" : signalledPids.length === 1 ? "one of this runner's processes" : "no recorded process";
          const closeState = closeUnverified ? "the close could not be verified" : "the outcome after signalling is not fully known";
          return {
            ok: false, code: "rehost_legacy_inspector_unverified",
            message: `A diagnostic signal was delivered to ${who} (${signalledPids.length} of 2) and ${closeState} (${proven.reasons.join(",")}). Read the pane and the pid-scoped listeners before anything else.`,
            blindRetryAllowed: false,
            observed: { outcomeClass: "unknown", deliveredPids: signalledPids, closeUnverified, reasons: proven.reasons },
          };
        }
        // PRE-EFFECT refusal: nothing was signalled and nothing was touched. `observed` carries the
        // closed reason list and round count only: no pid, port, path or command text escapes.
        return {
          ok: false, code: "rehost_legacy_witness_refused",
          message: `The live legacy runner and its pi child could not be witnessed (${proven.reasons.join(",")}). No runner was signalled and nothing was touched; re-read the pane before retrying.`,
          observed: { reasons: proven.reasons, rounds: proven.rounds },
        };
      }
      // The bounded tail is still required: the leaf is credited only against the very
      // file the plan resumes, and the collector proves the same equality internally.
      if (!tail || tail !== proven.nativeLeaf) {
        return { ok: false, code: "rehost_not_idle", message: "The witnessed native leaf does not equal the bounded session-file tail, so history and file disagree; refuse rather than resume onto a stale file." };
      } else {
        legacyWitness = {
        nativeLeaf: proven.nativeLeaf, evidenceId: proven.evidenceId, rounds: proven.rounds,
        binding: binding.binding, modules: binding.modules,
        seat: { rigId: resolved.entry.rigId, nodeId: resolved.nodeId, logicalId: resolved.entry.logicalId },
      };
      }
    } else if (!stoppedTargetRecovery && (!cursorFirst || cursorFirst !== cursorSecond || !tail || tail !== cursorFirst))
      return { ok: false, code: "rehost_not_idle", message: "Idle witness missing or unstable: the runner's last projected entry could not be confirmed twice-stable against the bounded session-file tail, so a turn may be in flight. Refuse rather than abort-then-proceed." };

    const digestPrefix = this.piSessionFileDigestPrefix
      ? this.piSessionFileDigestPrefix(sessionFile)
      : (() => { try { return createHash("sha256").update(readFileSync(sessionFile)).digest("hex").slice(0, 16); } catch { return null; } })();
    if (!digestPrefix)
      return { ok: false, code: "rehost_session_file_missing", message: "The persisted session file could not be read to bind its identity; rehost refuses instead of resuming an unbound file." };
    // S1 returns the PROVEN plan. Everything below only reads it.
    return {
      generation, sessionFile, launchId,
      // The ordinary path carries the twice-stable sidecar cursor. The legacy option
      // carries the leaf proven equal to the bounded tail; it never edits the sidecar.
      lastEntryId: stoppedTargetRecovery ? null : legacyWitness ? legacyWitness.nativeLeaf : cursorFirst,
      runnerPid: runner.pid, runnerChildPid: child?.pid ?? null, sessionFileSha256Prefix: digestPrefix,
      ...(stoppedTargetRecovery ? { runnerStartedAt: runner.startedAt, childStartedAt: child?.startedAt, runnerCommand: runner.command, childCommand: child?.command } : {}),
      model: resolved.entry.model ?? null, cwd: resolved.entry.cwd ?? "",
      // NodeInventoryEntry carries no posture; the launch posture comes from the
      // existing policy provenance, exactly as the managed start path derives it.
      posture: (this.rigRepo.getNodePolicyProvenance(nodeId)?.launchPosture ?? this.rigRepo.getRigPolicyProvenance(resolved.entry.rigName)?.launchPosture ?? "floor") as "floor" | "full_bypass",
      authority, sessionId: session.id ?? null,
      legacyWitness,
      sidecarCursorObserved: cursorFirst,
      stoppedTargetRecovery,
      ...(stoppedTargetAcceptanceReference ? { stoppedTargetAcceptanceReference } : {}),
    };
  }

  /**
   * The daemon-owned native mapping for ONE verified runner/child pair.
   *
   * Every value is derived from the EXACT census command lines of the two processes
   * this rehost already proved, or refused. There is no caller input, no basename
   * search, no path search, and no port of our own choosing: a mapping that cannot be
   * established returns its closed blocker instead of an approximation.
   */
  private async legacyWitnessBinding(
    runner: NativeProcessRow,
    child: NativeProcessRow | null,
    ids: { launchId: string; generation: string; sessionFile: string },
  ): Promise<{ ok: true; binding: LegacyPiWitnessBinding; modules: LegacyPiModuleBinding } | { ok: false; blocker: LegacyPiBindingBlocker }> {
    if (!this.legacyPiWitness || !this.currentPiRunnerEntryPath)
      return { ok: false, blocker: "witness_seam_unavailable" };
    // The REPLACEMENT must be provably the CURRENT fallback-capable runner entry this
    // daemon launches. The legacy runner is deliberately NOT that entry: it differs by
    // design and is the very thing being replaced. This check qualifies the resume, and
    // never compares the live legacy runner against the current entry.
    if (!isAbsolute(this.currentPiRunnerEntryPath) || !this.pathIsFile(this.currentPiRunnerEntryPath))
      return { ok: false, blocker: "current_runner_entry_unresolved" };
    const runnerEntry = this.runnerEntryFromCommand(runner.command);
    if (!runnerEntry) return { ok: false, blocker: "runner_entry_unresolved" };
    if (!child) return { ok: false, blocker: "pi_child_unresolved" };
    const childStartForGraph = typeof child.startedAt === "string" ? Date.parse(child.startedAt) : Number.NaN;
    const pi = this.legacyPiCachedModuleUrl(Number.isFinite(childStartForGraph) ? childStartForGraph : undefined);
    if (!pi) return { ok: false, blocker: "pi_child_module_unresolved" };
    const childModule = pi.moduleUrl;
    // The inspector endpoint is a property of LAUNCH CONFIGURATION. Absence is never claimed
    // from a command-line scrape: each target's node options come from its own runner tokens or,
    // for the child, from the shebang of its resolved entry, and both must resolve to the
    // built-in private loopback endpoint this bridge speaks. There is NO composed-argv seam: a
    // target with no trustworthy record stays unresolved.
    const endpoint = await this.legacyInspectorStartupEstablished(runner, child, ids);
    if (!endpoint.ok) return { ok: false, blocker: endpoint.blocker };
    const modules: LegacyPiModuleBinding = { runnerModuleUrl: runnerEntry, piModuleUrl: childModule };
    const identity = (row: NativeProcessRow): LegacyPiTargetIdentity => ({ pid: row.pid, ppid: row.ppid, startedAt: row.startedAt ?? "" });
    return {
      ok: true, modules,
      binding: {
        runner: identity(runner), child: identity(child),
        launchId: ids.launchId, generation: ids.generation, sessionFile: ids.sessionFile,
      },
    };
  }

  private pathIsFile(path: string): boolean {
    try { return statSync(path).isFile(); } catch { return false; }
  }

  /** The runner's OWN loaded main module: the exact absolute script token of its
   *  command line, realpathed to the URL the target reports as already loaded. The
   *  legacy runner is NOT compared against the current entry; it is bound to what it
   *  genuinely is. */
  private runnerEntryFromCommand(command: string): string | null {
    for (const token of command.split(/\s+/)) {
      const cleaned = token.replace(/^["']|["'],?$/g, "");
      if (!/^.*\/pi-runner\.(js|mjs)$/.test(cleaned)) continue;
      if (!isAbsolute(cleaned)) continue;
      return this.canonicalModuleUrl(cleaned);
    }
    return null;
  }

  /**
   * The Pi cached-chunk module URL exporting AgentSessionRuntime/AgentSession.
   *
   * The child's argv is NOT a usable source: Pi's bundle entry overwrites its own
   * process title, so the census command is the bare string `pi`. This therefore
   * consumes the daemon-owned installation resolver's answer — the ONE canonical
   * module in the installed package's own static import graph that both required
   * classes are exported from — and refuses anything that is not exactly one
   * canonical file URL. The transport still proves, per target, that the URL
   * arrives already parsed and exporting the required names.
   */
  private legacyPiCachedModuleUrl(targetStartedAtMs?: number): { moduleUrl: string; entryPath: string } | null {
    const resolved = this.legacyPiCachedModuleUrlResolver?.(targetStartedAtMs) ?? null;
    if (!resolved || !isAbsolute(resolved.modulePath) || !isAbsolute(resolved.entryPath)) return null;
    const moduleUrl = this.canonicalModuleUrl(resolved.modulePath);
    return moduleUrl === null ? null : { moduleUrl, entryPath: resolved.entryPath };
  }

  /** Canonical file path from either a module URL or an already-absolute path, or
   *  null when neither converts. Callers hold both forms: derived module bindings are
   *  URLs while daemon-owned configured entries are paths. */
  private fileUrlToPathSafe(value: string): string | null {
    if (!value.startsWith("file://")) return isAbsolute(value) ? value : null;
    try { return fileUrlToPath(value); } catch { return null; }
  }

  /** Canonical file URL for a module the target reports as already loaded. */
  private canonicalModuleUrl(path: string): string | null {
    let resolvedPath: string;
    try { resolvedPath = realpathSync(path); } catch { return null; }
    return pathToFileURL(resolvedPath).href;
  }

  /**
   * Whether BOTH targets will open the BUILT-IN PRIVATE loopback inspector on
   * SIGUSR1 — the only endpoint this bridge's fixed helper speaks.
   *
   * Absence of inspector configuration is a claim about LAUNCH CONFIGURATION, so it
   * is never made from a command-line scrape: `ps` cannot show a target's original
   * argv (Pi overwrites its own process title), and a scrape cannot distinguish an
   * unconfigured launch from a hidden one. Each target must instead present the argv
   * THE DAEMON COMPOSED for it, or a launch record that did, and that trusted argv is
   * classified. No record is an unresolved datum, not a silent pass; anything other
   * than the built-in private loopback default refuses.
   *
   * The verdict is bracketed by a fresh re-read of both process identities, so it can
   * only ever describe the exact two processes this rehost already proved.
   */
  /**
   * Inspector endpoint qualification for the EXISTING live pair.
   *
   * The port a SIGUSR1-opened inspector binds is set by: node flags BEFORE the script,
   * the interpreter shebang, NODE_OPTIONS, and a config file node loads only when
   * explicitly told to. Each is established from live provenance, never assumed:
   *
   *   - RUNNER: its census argv is intact (it does not rewrite its title), so the
   *     node options it was launched with are directly observable and classified.
   *   - CHILD: Pi rewrites its own process title, so its argv is NOT observable and
   *     must never be inferred from it. Its node options are bounded structurally —
   *     the runner builds the child's argv with every flag AFTER the script name, where
   *     node treats them as script arguments — and what remains possible is the
   *     interpreter shebang and NODE_OPTIONS, both read below.
   *   - BOTH: the kernel environment region, with the seat's own occupant generation
   *     as a positive control. Without a readable control an "unset" answer is not an
   *     observation, so the read refuses instead of guessing.
   */
  /** The legacy runner's OWN script must predate the runner's start, on both ctime and mtime.
   *  An unreadable or unstattable script is UNKNOWN, never a pass. */
  private scriptPredatesStart(script: string, targetStartedAtMs: number): boolean {
    // The daemon-configured predicate wins when supplied, so the same injectable seam already used
    // for the Pi graph decides the runner script too. Falling back to a direct stat keeps the real
    // daemon honest: BOTH ctime and mtime must predate the target, and an unreadable script is a
    // refusal, never a pass.
    const injected = this.graphPredatesStart;
    if (injected) return injected(script, targetStartedAtMs);
    try {
      const stat = statSync(script);
      return stat.ctimeMs <= targetStartedAtMs && stat.mtimeMs <= targetStartedAtMs;
    } catch { return false; }
  }

  private async legacyInspectorStartupEstablished(
    runner: NativeProcessRow,
    child: NativeProcessRow,
    ids: { launchId: string; generation: string; sessionFile: string },
  ): Promise<{ ok: true } | { ok: false; blocker: LegacyPiBindingBlocker }> {
    if (!runner.startedAt || !child.startedAt) return { ok: false, blocker: "binding_identity_drift" };
    // The legacy runner must BE the proven legacy build, by content hash of its own
    // script. An unmatched or unreadable hash is UNKNOWN, never "close enough".
    const runnerScript = this.runnerEntryFromCommand(runner.command);
    if (!runnerScript || !this.scriptHashIsKnownLegacy(runnerScript)) return { ok: false, blocker: "legacy_runner_identity_unproven" };
    // TIME BINDING, correct subject: the legacy RUNNER's own script must predate the RUNNER's
    // start. The previous check bound the REPLACEMENT entry (this.currentPiRunnerEntryPath, which
    // is deliberately newer) to the CHILD's start, which refused every real pair after any
    // deployment. The replacement entry is checked for existence and provenance only, never for
    // age against the legacy target.
    const runnerStartMs = Date.parse(runner.startedAt);
    if (!Number.isFinite(runnerStartMs)) return { ok: false, blocker: "binding_identity_drift" };
    if (!runnerScript || !this.scriptPredatesStart(runnerScript, runnerStartMs)) return { ok: false, blocker: "module_graph_newer_than_target" };
    // The FULL resolved Pi graph must predate the CHILD that will load it. resolvePiInstallationModule
    // checks every walked graph file against targetStartedAtMs, which the route's resolver closure
    // now passes, so this is a graph-wide bound rather than two hand-picked files.
    const childStartMs = Date.parse(child.startedAt);
    if (!Number.isFinite(childStartMs)) return { ok: false, blocker: "binding_identity_drift" };
    const pi = this.legacyPiCachedModuleUrl(childStartMs);
    if (!pi) return { ok: false, blocker: "pi_child_module_unresolved" };
    if (!this.graphPredates(pi.moduleUrl, childStartMs) || !this.graphPredates(pi.entryPath, childStartMs))
      return { ok: false, blocker: "module_graph_newer_than_target" };
    // Node options: the runner's are directly observable; the child's can only come
    // from the shebang of its resolved entry.
    const runnerVerdict = classifyNodeInspectorConfiguration(this.nodeOptionsFromCommand(runner.command));
    if (!qualifiesDefaultPrivateInspector(runnerVerdict.configuration)) return { ok: false, blocker: "inspector_configuration_unestablished" };
    // The interpreter line belongs to the child's ENTRY, not to the chunk that carries
    // the classes: the shebang is how the kernel chose its interpreter.
    const shebang = this.entryShebangFor(pi.entryPath);
    if (shebang === null) return { ok: false, blocker: "inspector_configuration_unestablished" };
    const shebangTokens = shebang.split(/\s+/);
    // `#!/usr/bin/env node` resolves the interpreter through PATH; a direct
    // `#!/abs/path/to/node` names it. Either way only a bare interpreter NAME is
    // acceptable: a shebang carrying flags could configure the inspector itself.
    const interpreter = shebangTokens[0]!.endsWith("/env") || shebangTokens[0] === "env" ? shebangTokens[1] : shebangTokens[0];
    const interpreterFlags = shebangTokens[0]!.endsWith("/env") || shebangTokens[0] === "env" ? shebangTokens.slice(2) : shebangTokens.slice(1);
    if (!interpreter || !/(^|\/)(node|bun|deno)$/.test(interpreter)) return { ok: false, blocker: "inspector_configuration_unestablished" };
    if (classifyNodeInspectorConfiguration(interpreterFlags).configuration !== "none") return { ok: false, blocker: "inspector_configuration_unestablished" };
    // Environment: the kernel region for BOTH targets, each with the generation control.
    for (const row of [runner, child]) {
      const verdict = await this.observeLegacyEnvironment(row, ids.generation);
      if (!verdict.ok) return { ok: false, blocker: verdict.blocker };
    }
    return { ok: true };
  }

  /** Node options are the tokens BEFORE the script name; everything after it is a
   *  script argument node never reads as an option. */
  private nodeOptionsFromCommand(command: string): string[] {
    const tokens = command.split(/\s+/).filter(token => token.length > 0);
    const scriptIndex = tokens.findIndex(token => /\/pi-runner\.(js|mjs)$/.test(token));
    return scriptIndex <= 0 ? [] : tokens.slice(0, scriptIndex);
  }

  private scriptHashIsKnownLegacy(moduleUrl: string): boolean {
    const hashes = this.legacyRunnerHashes;
    if (!hashes || hashes.length === 0) return false;
    const digest = this.fileSha256(this.fileUrlToPathSafe(moduleUrl) ?? "");
    return digest !== null && hashes.includes(digest);
  }

  private fileSha256(path: string): string | null {
    try { return createHash("sha256").update(readFileSync(path)).digest("hex"); } catch { return null; }
  }

  private graphPredates(moduleUrl: string, targetStartMs: number): boolean {
    const path = this.fileUrlToPathSafe(moduleUrl);
    if (path === null) return false;
    return this.graphPredatesStart ? this.graphPredatesStart(path, targetStartMs) : filePredatesStart(path, targetStartMs);
  }

  private entryShebangFor(moduleUrl: string): string | null {
    const path = this.fileUrlToPathSafe(moduleUrl);
    return path === null ? null : entryShebang(path);
  }

  private async observeLegacyEnvironment(row: NativeProcessRow, generation: string): Promise<{ ok: true } | { ok: false; blocker: LegacyPiBindingBlocker }> {
    const observer = this.legacyEnvironmentObserver;
    if (!observer) return { ok: false, blocker: "environment_control_absent" };
    let verdict: Awaited<ReturnType<typeof observer>>;
    try { verdict = await observer({ pid: row.pid, generation }); } catch { return { ok: false, blocker: "environment_control_absent" }; }
    if (!verdict.regionReadable) return { ok: false, blocker: "environment_control_absent" };
    if (!verdict.occupantGenerationMatches) return { ok: false, blocker: "environment_control_absent" };
    // ANY present NODE_OPTIONS refuses; the value is never read or compared.
    if (verdict.nodeOptions !== "unset") return { ok: false, blocker: "node_options_present" };
    return { ok: true };
  }

  /** A FINAL fresh witness must reproduce the accepted binding and leaf. Any drift,
   *  changed sample or unverified cleanup refuses BEFORE the halt (no kill, no resume),
   *  but NOT free of consequence: this round itself signals both targets, so a failure
   *  AFTER delivery carries the collector's real signal facts to the caller. */
  private async rehostFinalWitnessAgrees(plan: RehostPlan): Promise<{ ok: true } | { ok: false; reasons: string[]; rounds: number; signal?: { delivered: boolean; deliveredPids: number[]; auditedBeforeDelivery: boolean; closedVerified?: boolean } }> {
    const facts = plan.legacyWitness;
    if (!facts || !this.legacyPiWitness) return { ok: true };
    // The FINAL witness reuses the SAME already-verified seat identity the plan recorded. It is not
    // re-derived here and never comes from a caller.
    const final = await this.legacyPiWitness.witness({
      binding: facts.binding, modules: facts.modules,
      seat: facts.seat,
    });
    if (!final.ok) return { ok: false, reasons: final.reasons, rounds: final.rounds, ...(final.signal ? { signal: final.signal } : {}) };
    // H1-F2: a COMPLETED final round that disagrees still delivered its signals and
    // verified their closes. The acceptance receipt carries no pids (G1 egress rule), so
    // the real targets are named from the SAME verified binding this witness signalled.
    const closed = final.signal ? { signal: {
      delivered: final.signal.delivered,
      deliveredPids: final.signal.delivered ? [facts.binding.runner.pid, facts.binding.child.pid] : [],
      auditedBeforeDelivery: final.signal.auditedBeforeDelivery, closedVerified: true,
    } } : {};
    if (final.nativeLeaf !== facts.nativeLeaf || final.evidenceId !== facts.evidenceId)
      return { ok: false, reasons: ["witness_sample_drift"], rounds: final.rounds, ...closed };
    if (final.rounds < facts.rounds) return { ok: false, reasons: ["witness_sample_drift"], rounds: final.rounds, ...closed };
    return { ok: true };
  }

  /** Post-proof for the legacy option: the replacement's sidecar cursor must have
   *  been REFRESHED to the bound leaf, which must still equal the bounded file tail,
   *  in the same file and generation. A stale cursor is a mismatch, never credit. */
  private legacyPostCursorAgrees(sessionName: string, plan: RehostPlan): boolean {
    const facts = plan.legacyWitness;
    if (!facts) return true;
    const post = this.piRunnerState!(sessionName);
    if (!post || post.lastEntryId !== facts.nativeLeaf) return false;
    return this.piSessionTailEntryId!(plan.sessionFile) === facts.nativeLeaf;
  }
  /** Bounded poll: is the verified runner (and its pi child) gone from the census? */
  /** Every descendant of the pane root, by stable identity. */
 private rehostDescendants(paneRootPid: number, rows: Array<{ pid: number; ppid: number; command: string }>): Array<{ pid: number; command: string }> {
   const children = new Map<number, number[]>();
   for (const r of rows) { if (!children.has(r.ppid)) children.set(r.ppid, []); children.get(r.ppid)!.push(r.pid); }
   const seen = new Set<number>(); const out: Array<{ pid: number; command: string }> = []; const queue = [...(children.get(paneRootPid) ?? [])];
   while (queue.length) {
     const pid = queue.shift()!;
     if (pid === paneRootPid || seen.has(pid)) continue;
     seen.add(pid);
     const row = rows.find(r => r.pid === pid);
     if (row) out.push({ pid, command: row.command });
     queue.push(...(children.get(pid) ?? []));
   }
   return out;
 }
 /** True when the runner's own parent chain in this census reaches exactly paneRootPid.
  *  Nothing is assumed: unproven ancestry is a refusal, never a silent substitute root. */
 private ancestryReaches(rows: Array<{ pid: number; ppid: number }>, startPid: number, paneRootPid: number): boolean {
   const seen = new Set<number>(); let cursor = startPid;
   for (let hop = 0; hop < 32; hop += 1) {
     if (cursor === paneRootPid) return true;
     if (seen.has(cursor)) return false;
     seen.add(cursor);
     const parent = rows.find(r => r.pid === cursor)?.ppid;
     if (!parent || parent <= 1) return false;
     cursor = parent;
   }
   return false;
 }
 /** Resolve the pane ROOT pid for a node. Never climbs to the shared tmux server. */
 private async paneRootPidFor(nodeId: string): Promise<number | null> {
   if (this.paneRootPid) return this.paneRootPid(nodeId);
   const binding = this.db.prepare("SELECT tmux_pane FROM bindings WHERE node_id=?").get(nodeId) as { tmux_pane: string | null } | undefined;
   if (!binding?.tmux_pane) return null;
   if (typeof this.tmuxAdapter.getPanePid !== "function") return null;
   return (await this.tmuxAdapter.getPanePid(binding.tmux_pane).catch(() => null)) ?? null;
 }
 /** A pane is quiescent only when its root is alive as a shell and NOTHING else hangs
  *  off it. Any surviving process means the typed resume could reach a second writer. */
 private async rehostPaneQuiesced(paneRootPid: number, rows: Array<{ pid: number; ppid: number; command: string; startedAt?: string }>): Promise<{ quiesced: boolean; present: Array<{ pid: number; command: string }> }> {
   const root = rows.find(r => r.pid === paneRootPid);
   if (!root) return { quiesced: false, present: [] };
   const base = paneShellBasename(root.command);
   if (!base || !SHELL_BASENAMES.has(base)) return { quiesced: false, present: this.rehostDescendants(paneRootPid, rows) };
   const present = this.rehostDescendants(paneRootPid, rows);
   return { quiesced: present.length === 0, present };
 }
 /**
  * Bounded poll for the verified stop. Identity is by pid PLUS startedAt, never by the
  * runner argv regex: the pi child's argv carries neither `pi-runner.js` nor
  * `--session-name`, so a regex match could never see it, and pid reuse is defeated by
  * the pre-stop startedAt. An orphan child alive BLOCKS: no resume may be typed.
  */
 private async rehostRunnerExited(
   paneRootPid: number,
   runnerPid: number,
   childPid: number | null,
   preStop: { runnerStartedAt?: string; childStartedAt?: string },
 ): Promise<boolean> {
   const deadline = Date.now() + this.rehostWaitMs;
   const sameProcess = (r: { pid: number; startedAt?: string }, was: string | undefined) => r.startedAt !== undefined && was !== undefined && r.startedAt === was;
   for (;;) {
     const rows = await this.listProcesses!();
     const runnerAlive = rows.some(r => r.pid === runnerPid && sameProcess(r, preStop.runnerStartedAt));
     const childAlive = childPid !== null && rows.some(r => r.pid === childPid && sameProcess(r, preStop.childStartedAt));
     const quiesced = await this.rehostPaneQuiesced(paneRootPid, rows);
     // Every one of these must hold; any survivor keeps the stop UNKNOWN.
     if (!runnerAlive && !childAlive && quiesced.quiesced) return true;
     if (Date.now() >= deadline) return false;
     await new Promise<void>(resolve => setTimeout(resolve, this.rehostPollMs));
   }
 }
  /** READ-ONLY preservation facts. Compared before and after; never written. */
  private rehostCustodySnapshot(nodeId: string, sessionFile: string): RehostCustodySnapshot {
    const latestSessionName: string = (this.db.prepare("SELECT session_name FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(nodeId) as { session_name: string } | undefined)?.session_name ?? "";
    const tenures = this.db.prepare("SELECT * FROM occupant_tenures WHERE node_id=? ORDER BY id").all(nodeId);
    // Scoped to claims addressed to this seat, or held by its own generation.
    const claims = this.db.prepare("SELECT qitem_id, claimed_by_generation_uuid, state FROM queue_items WHERE claimed_by_generation_uuid IS NOT NULL AND (destination_session=? OR claimed_by_generation_uuid=?) ORDER BY qitem_id").all(latestSessionName, latestSessionName);
    // Scoped to THIS seat: another rig renewing its lease is not drift here.
    const authority = this.db.prepare("SELECT * FROM coordinator_authority WHERE owner_session=? ORDER BY rig_id").all(latestSessionName);
    const token = (this.db.prepare("SELECT resume_token FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(nodeId) as { resume_token: string | null } | undefined)?.resume_token ?? null;
    const unknown = this.db.prepare("SELECT outbox_id, delivery_state, body FROM outbox_entries WHERE destination_session=? AND delivery_state='indeterminate' ORDER BY outbox_id").all(latestSessionName) as Array<{ outbox_id: string; delivery_state: string; body: string }>;
    const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
    return {
      tenantHash: hash(tenures),
      resumeTokenHash: hash(token),
      authorityHash: hash(authority),
      claimHash: hash(claims),
      unknownEffects: { count: unknown.length, digest: hash(unknown.map((r: { outbox_id: string; delivery_state: string; body: string }) => [r.outbox_id, r.delivery_state, createHash("sha256").update(r.body).digest("hex")])) },
      sessionFile,
    };
  }

  /** B5 durable audit for the legacy-witness path, which runs in the PLAN phase where no SeatDescriptor
   *  or operator input exists. It records only that a diagnostic signal was delivered and what the
   *  outcome was, so a later reader can see the delivery without trusting the refusal wording. */
  private appendLegacyWitnessAudit(resolved: ResolvedSeat, payload: Record<string, unknown>): void {
    const at = new Date().toISOString();
    // F3: the delivery facts this audit exists to preserve, read from the payload the plan passed.
    const facts = payload["signal"] as { delivered?: boolean; deliveredPids?: number[]; auditedBeforeDelivery?: boolean } | undefined;
    const tx = this.db.transaction(() => {
      this.eventBus.persistWithinTransaction({
        type: "seat.runner_rehost_legacy_witness_outcome",
        rigId: resolved.entry.rigId, nodeId: resolved.nodeId, logicalId: resolved.entry.logicalId,
        reason: "legacy_pi_native_witness", operator: null, at,
        witnessRefused: true, code: String(payload["code"] ?? "rehost_legacy_inspector_unverified"),
        reasons: Array.isArray(payload["reasons"]) ? (payload["reasons"] as string[]) : [],
        signalDelivered: true, blindRetryAllowed: false,
        // H1-F3: which witness phase produced this outcome (default: the plan phase).
        stage: typeof payload["stage"] === "string" ? payload["stage"] : "plan_witness",
        // F3: the facts this audit exists to preserve must actually be persisted, not dropped.
        deliveredPids: facts?.deliveredPids ?? [],
        auditedBeforeDelivery: facts?.auditedBeforeDelivery === true,
        binding: payload["binding"] ?? null, modules: payload["modules"] ?? null,
      // H1-F3: the stage marker rides the same untyped-payload convention as every other
      // rehost event (appendRehostEvent passes its payload through `as never`).
      } as never);
    });
    tx();
  }

  private appendRehostEvent(type: string, seat: SeatDescriptor, input: { reason: string; operator?: string | null }, payload: Record<string, unknown>): void {
    const at = new Date().toISOString();
    const tx = this.db.transaction(() => {
      this.eventBus.persistWithinTransaction({ type, rigId: seat.rigId, nodeId: seat.nodeId, logicalId: seat.logicalId, reason: input.reason.trim(), operator: input.operator ?? null, at, ...payload } as never);
    });
    tx();
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasStrings<T extends string>(value: Record<string, unknown>, keys: readonly T[]): value is Record<T, string> & Record<string, unknown> {
  return keys.every((key) => typeof value[key] === "string" && value[key].trim().length > 0);
}

function isProjectionCategory(value: unknown): value is ProjectionEntry["category"] {
  return isOneOf(value, ["skill", "guidance", "subagent", "plugin", "runtime_resource"] as const);
}

function isOptionalString(value: unknown): value is string | undefined {
  return value === undefined || typeof value === "string";
}

function isOneOf<T extends string>(value: unknown, allowed: readonly T[]): value is T {
  return typeof value === "string" && allowed.includes(value as T);
}

function isOptionalOneOf<T extends string>(value: unknown, allowed: readonly T[]): value is T | undefined {
  return value === undefined || isOneOf(value, allowed);
}

function isStringArrayOf<T extends string>(value: unknown, allowed: readonly T[]): value is T[] {
  return Array.isArray(value) && value.every((entry) => isOneOf(entry, allowed));

}
