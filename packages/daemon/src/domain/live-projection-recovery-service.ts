// Live-projection recovery: guarded correction of a FALSELY DETACHED projection
// for the SAME still-live native occupant. Deliberately NOT reconcile-session:
// never mints an occupant tenure, adopts, re-keys, replays startup, sends input,
// launches/kills, or touches authority, claims, resources or outbox custody.
// The only durable writes are the original detached session row's
// status -> running plus one attributable recovery receipt/event.
//
// Identity proof is the same genuine native machinery the daemon already uses:
//  - codex / claude-code: token-bound process-lineage double observation
//    (verifyCodexPaneProcess / verifyClaudePaneProcess, requireResume for
//    codex). A missing resume token can never prove same history -> refuse.
//  - pi: live pane PID bound to the registered binding PLUS the runner's typed
//    sidecar (ready, launchId === the per-launch argv --launch-id, sessionFile
//    === the stored resume token). No clock gate: updatedAt is a state-write
//    stamp, so a healthy idle runner legitimately carries an old one; a live
//    process set contradicting an exited marker stays unknown.
//  - terminal/unknown runtimes: no positive same-native identity proof exists;
//    refuse rather than degrade to pane-name/PID-alone inference.
// Credentials never enter proofs, receipts, or events (tokens appear only as
// digests).

import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import type { CoordinatorAuthorityService } from "./coordinator-authority-service.js";
import type { EventBus } from "./event-bus.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";
import { verifyCodexPaneProcess, verifyClaudePaneProcess, type NativeProcessLister } from "./native-process-lineage.js";
import { classifyPaneRuntimeMatch } from "./seat-identity-reconciler.js";
const digest = (value: unknown): string => createHash("sha256").update(typeof value === "string" ? value : JSON.stringify(value)).digest("hex");

/** Shared Pi proof lives in coordinator-runtime-availability.makePiNativeProver;
 * both this recovery and the coordinator runtime observer consume the SAME
 * prover instance. No clock-freshness heuristic exists here by design. */

class ProjectionDrift extends Error {}

export interface LiveProjectionRecoveryInput {
  operationId: string;
  sessionId: string;
  nodeId: string;
  sessionName: string;
  expectedGeneration: string;
}

export interface LiveProjectionRecoveryOutcome {
  ok: boolean;
  code: string;
  message?: string;
  receipt?: Record<string, unknown>;
}

interface TmuxProbe {
  listSessions(): Promise<Array<{ name: string }>>;
  getPanePid(target: string): Promise<number | null>;
  getPaneCommand(target: string): Promise<string | null>;
}

/** Shared Pi prover injected from coordinator-runtime-availability; the same
 * instance also backs makeCoordinatorRuntimeObserver in startup wiring. */
export interface LiveProjectionRecoveryDeps {
  db: Database.Database;
  tmux: TmuxProbe;
  authority: CoordinatorAuthorityService;
  events: EventBus;
  piProve?: (session: string) => Promise<PiNativeProof | null>;
  listProcesses?: NativeProcessLister;
  now?: () => number;
}

interface SessionRow { id: string; node_id: string; session_name: string; status: string; resume_token: string | null }

type Snapshot = { row: SessionRow; tenureId: string; runtime: string | null; rigId: string; pane: string | null; nativeBoot: string | null };

export class LiveProjectionRecoveryService {
  private readonly db: Database.Database;
  private readonly tmux: TmuxProbe;
  private readonly authority: CoordinatorAuthorityService;
  private readonly events: EventBus;
  private readonly piProve?: (session: string) => Promise<PiNativeProof | null>;
  private readonly listProcesses?: NativeProcessLister;
  private readonly now: () => number;

  constructor(deps: LiveProjectionRecoveryDeps) {
    this.db = deps.db; this.tmux = deps.tmux; this.authority = deps.authority;
    this.events = deps.events; this.piProve = deps.piProve;
    this.listProcesses = deps.listProcesses; this.now = deps.now ?? Date.now;
  }

  async recover(actor: string, generation: string, input: LiveProjectionRecoveryInput): Promise<LiveProjectionRecoveryOutcome> {
    if (!input || typeof input !== "object" || Array.isArray(input)
      || ["operationId", "sessionId", "nodeId", "sessionName", "expectedGeneration"].some(k => typeof (input as unknown as Record<string, unknown>)[k] !== "string" || !(input as unknown as Record<string, string>)[k]?.trim())
      || input.operationId.length > 160) return { ok: false, code: "live_projection_contract_required", message: "Exact operation/session/node/name/generation contract required." };

    const requestHash = digest({ actor, generation, input });
    try {
      const prior = this.db.prepare("SELECT request_hash, receipt FROM live_projection_recovery_operations WHERE operation_id=?").get(`live-projection-recover:${input.operationId}`) as { request_hash: string; receipt: string } | undefined;
      if (prior) {
        if (prior.request_hash !== requestHash) return { ok: false, code: "live_projection_replay_conflict", message: "Operation ID already carries a different recovery payload." };
        return { ok: true, code: "already_recovered", receipt: JSON.parse(prior.receipt) };
      }
    } catch { return { ok: false, code: "recovery_infrastructure_error", message: "Recovery ledger unreadable; nothing was written." }; }

    try { this.authority.assertCurrentOperator(actor, generation); }
    catch { return { ok: false, code: "operator_unauthorized", message: "Genuine current Kernel Operator authorization required." }; }

    let pre: Snapshot;
    try {
      const snap = this.snapshot(input);
      if ("outcome" in snap) return snap.outcome;
      pre = snap;
      const fenced = this.fences(pre);
      if (fenced) return fenced;
    } catch { return { ok: false, code: "recovery_infrastructure_error", message: "Custody schema precondition failed; nothing was written." }; }

    // Fresh native probe AFTER static checks, BEFORE any transaction.
    const probe = await this.probe(pre.row, pre.runtime, pre.pane, pre.nativeBoot);
    if ("outcome" in probe) return probe.outcome;

    const at = new Date(this.now()).toISOString();
    const receipt = {
      kind: "live-projection-recovery", operationId: input.operationId, rigId: pre.rigId,
      sessionId: input.sessionId, nodeId: input.nodeId, sessionName: input.sessionName,
      generation: input.expectedGeneration, actor, actorGeneration: generation,
      sessionRowHash: digest(pre.row), tenureId: pre.tenureId, resumeTokenHash: pre.row.resume_token ? digest(pre.row.resume_token) : null,
      evidence: probe.evidence, preserved: { tenureMinted: false, adopted: false, relaunched: false, inputSent: false, authorityChanged: false, custodyTouched: false },
      statusTransition: { from: "detached", to: "running" }, recoveredAt: at, grantsAuthority: false,
    };

    try {
      this.db.transaction(() => {
        // Full post-probe re-verification inside the write transaction:
        // authority, tenure/binding snapshot, and every fence — row equality
        // alone would miss tenure/binding/fence drift during the async probe.
        try { this.authority.assertCurrentOperator(actor, generation); } catch { throw new ProjectionDrift(); }
        const post = this.snapshot(input);
        if ("outcome" in post) throw new ProjectionDrift();
        if (JSON.stringify(post.row) !== JSON.stringify(pre.row) || post.tenureId !== pre.tenureId || post.pane !== pre.pane) throw new ProjectionDrift();
        if (this.fences(post)) throw new ProjectionDrift();
        const changed = this.db.prepare("UPDATE sessions SET status='running', last_seen_at=? WHERE id=? AND status='detached'").run(at, input.sessionId).changes;
        if (changed !== 1) throw new ProjectionDrift();
        // Dedicated generic ledger: tied to rig/node/session only, never to
        // coordinator-authority enrollment, so unenrolled rigs recover cleanly.
        this.db.prepare("INSERT INTO live_projection_recovery_operations(operation_id,rig_id,node_id,session_id,request_hash,receipt,created_at) VALUES (?,?,?,?,?,?,?)").run(`live-projection-recover:${input.operationId}`, pre.rigId, pre.row.node_id, pre.row.id, requestHash, JSON.stringify(receipt), at);
        this.events.persistWithinTransaction({ type: "session.live_projection_recovered", sessionId: input.sessionId, nodeId: input.nodeId, sessionName: input.sessionName, rigId: pre.rigId, actor, actorGeneration: generation, operationId: input.operationId, at });
      }).immediate();
    } catch (error) {
      if (error instanceof ProjectionDrift) return { ok: false, code: "state_changed_during_probe", message: "Authority, custody, or fencing changed while probing; nothing was written." };
      return { ok: false, code: "recovery_infrastructure_error", message: "Recovery transaction failed on infrastructure grounds; nothing was written." };
    }
    return { ok: true, code: "recovered", receipt };
  }

  /** Quiescence fences matching the coordination engine's own checks: every
   * unreleased reservation (reserved/started/committed), any guarded undelivered
   * effect, and any explicit seat delivery guard (desired or effective).
   * Checkpoint rows carry no lifecycle state column, so none is invented. */
  private fences(s: Snapshot): LiveProjectionRecoveryOutcome | null {
    if (this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state <> 'released' LIMIT 1").get(s.row.node_id))
      return { ok: false, code: "reservation_fencing_active", message: "Unreleased dispatch reservation fences projection recovery." };
    if (this.db.prepare("SELECT 1 FROM outbox_entries WHERE destination_session=? AND guard_binding IS NOT NULL AND delivery_state NOT IN ('delivered','failed','retired') LIMIT 1").get(s.row.session_name))
      return { ok: false, code: "delivery_fencing_active", message: "Guarded undelivered effect fences projection recovery." };
    if (this.db.prepare("SELECT 1 FROM seat_delivery_guards WHERE node_id=? AND (desired=1 OR effective=1) LIMIT 1").get(s.row.node_id))
      return { ok: false, code: "delivery_fencing_active", message: "Explicit seat delivery guard fences projection recovery." };
    return null;
  }

  private snapshot(input: LiveProjectionRecoveryInput): Snapshot | { outcome: LiveProjectionRecoveryOutcome } {
    const row = this.db.prepare("SELECT id,node_id,session_name,status,resume_token FROM sessions WHERE id=?").get(input.sessionId) as SessionRow | undefined;
    if (!row) return { outcome: { ok: false, code: "session_missing", message: "Original session row not found." } };
    if (row.node_id !== input.nodeId || row.session_name !== input.sessionName) return { outcome: { ok: false, code: "identity_mismatch", message: "Session row does not match the expected node and canonical name." } };
    if (row.status !== "detached") return { outcome: { ok: false, code: row.status === "superseded" || row.status === "exited" ? "not_current_occupant" : "not_detached", message: `Row status '${row.status}' is not a falsely-detached current occupant.` } };
    const latest = this.db.prepare("SELECT id,status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(input.nodeId) as { id: string; status: string } | undefined;
    if (!latest || latest.id !== row.id) return { outcome: { ok: false, code: "historical_row", message: "A newer session row supersedes this historical projection." } };
    if (latest.status === "exited" || latest.status === "superseded") return { outcome: { ok: false, code: "not_current_occupant", message: "Latest projection reports an exited/superseded occupant." } };
    const tenure = this.db.prepare("SELECT id,generation_uuid,native_session_id_at_boot FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal DESC LIMIT 1").get(input.nodeId) as { id: string; generation_uuid: string; native_session_id_at_boot: string | null } | undefined;
    if (!tenure) return { outcome: { ok: false, code: "tenure_missing", message: "No occupant tenure exists for the node." } };
    if (tenure.generation_uuid !== input.expectedGeneration) return { outcome: { ok: false, code: "stale_generation", message: "Expected generation is not the node's latest occupant tenure." } };
    const node = this.db.prepare("SELECT runtime,rig_id FROM nodes WHERE id=?").get(input.nodeId) as { runtime: string | null; rig_id: string } | undefined;
    if (!node) return { outcome: { ok: false, code: "node_missing", message: "Persisted node not found." } };
    const binding = this.db.prepare("SELECT tmux_pane FROM bindings WHERE node_id=?").get(input.nodeId) as { tmux_pane: string | null } | undefined;
    return { row, tenureId: tenure.id, runtime: node.runtime ?? null, rigId: node.rig_id, pane: binding?.tmux_pane ?? null, nativeBoot: tenure.native_session_id_at_boot };
  }

  private async probe(row: SessionRow, runtime: string | null, pane: string | null, nativeBoot: string | null): Promise<{ evidence: Record<string, unknown> } | { outcome: LiveProjectionRecoveryOutcome }> {
    if (runtime !== "codex" && runtime !== "claude-code" && runtime !== "pi")
      return { outcome: { ok: false, code: "unsupported_runtime_proof", message: `No positive same-native identity proof exists for runtime '${runtime ?? "unknown"}'; refusing instead of inferring.` } };

    let live: Set<string> | null = null;
    try { live = new Set((await this.tmux.listSessions()).map(s => s.name)); } catch { live = null; }
    if (!live || live.size === 0) return { outcome: { ok: false, code: "probe_uncertain", message: "tmux unreachable; live identity cannot be proven." } };
    if (!live.has(row.session_name)) return { outcome: { ok: false, code: "pane_missing", message: "Managed tmux session is absent; occupant is not live." } };
    if (!pane) return { outcome: { ok: false, code: "binding_absent", message: "Registered binding carries no pane target to prove against." } };
    const pid = await this.tmux.getPanePid(pane);
    if (pid === null) return { outcome: { ok: false, code: "pane_missing", message: "Registered pane PID no longer resolves." } };
    const command = await this.tmux.getPaneCommand(pane);
    if (classifyPaneRuntimeMatch(command, runtime) === "mismatch") return { outcome: { ok: false, code: "identity_mismatch", message: "Pane process contradicts the managed runtime." } };

    if (!row.resume_token) return { outcome: { ok: false, code: "native_proof_unavailable", message: "Stored resume token is missing; same native history cannot be proven." } };

    if (runtime === "codex") {
      const proof = await verifyCodexPaneProcess({ target: pane, tmux: this.tmux, expectedToken: row.resume_token, requireResume: true, ...(this.listProcesses ? { listProcesses: this.listProcesses } : {}) });
      if (!proof) return { outcome: { ok: false, code: "native_proof_failed", message: "Token-bound Codex process-lineage double observation did not confirm the same live resume." } };
      return { evidence: { axis: "codex_process_lineage", pane, pid, command, fingerprint: proof.fingerprint, nativeSessionIdAtBoot: nativeBoot, resumeTokenHash: digest(row.resume_token) } };
    }
    if (runtime === "claude-code") {
      const proof = await verifyClaudePaneProcess({ target: pane, tmux: this.tmux, expectedToken: row.resume_token, ...(this.listProcesses ? { listProcesses: this.listProcesses } : {}) });
      if (!proof) return { outcome: { ok: false, code: "native_proof_failed", message: "Token-bound Claude process-lineage double observation did not confirm the same live resume." } };
      return { evidence: { axis: "claude_process_lineage", pane, pid, command, fingerprint: proof.fingerprint, nativeSessionIdAtBoot: nativeBoot, resumeTokenHash: digest(row.resume_token) } };
    }

    // pi: delegate to the SHARED native prover (double-stable runner+child
    // lineage, argv launch-id === typed sidecar launchId, session-file token
    // binding, genuine occupant-generation environment token equality against
    // the node's LATEST tenure — which snapshot() already required to equal
    // input.expectedGeneration, so generation binding is transitive here). An
    // old sidecar updatedAt on an idle-but-live runner stays valid because
    // identity binds through the per-launch instance id, never a clock.
    if (!this.piProve) return { outcome: { ok: false, code: "probe_uncertain", message: "No Pi native prover configured; refusing instead of weaker inference." } };
    const proof = await this.piProve(row.session_name);
    if (!proof) return { outcome: { ok: false, code: "probe_uncertain", message: "Pi native proof unavailable, stale, or mismatched; nothing was written." } };
    if (proof.state === "absent") return { outcome: { ok: false, code: "not_current_occupant", message: "Pi lineage positively reports the occupant process is gone." } };
    if (proof.launchId === null) return { outcome: { ok: false, code: "native_proof_failed", message: "Pi proof lacks the per-launch instance binding." } };
    return { evidence: { axis: "pi_native_lineage", pane, pid, command, launchId: proof.launchId, fingerprint: proof.fingerprint, resumeTokenHash: digest(row.resume_token), nativeSessionIdAtBoot: nativeBoot } };
  }
}
