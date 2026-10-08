import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import {
  DEFAULT_CONTEXT_REFRESH_POLICY, type ContextRefreshActor, type ContextRefreshAttempt,
  type ContextRefreshCheckpoint, type ContextRefreshDecision, type ContextRefreshGrant,
  type ContextRefreshHold, type ContextRefreshObservation, type ContextRefreshReservationEvidence,
  type ContextRefreshTarget,
} from "./context-refresh-contract.js";
import { contextRefreshCheckpointInstructions } from "./context-refresh-checkpoint-instructions.js";

export interface ContextRefreshCheckpointRequest {
  operationId: string; grantId: string; target: ContextRefreshTarget;
  actor: ContextRefreshActor; qitemId: string; body: string;
}
export interface ContextRefreshCheckpointReceipt {
  operationId: string; qitemId: string; requestDigest: string; receiptDigest: string;
}
export type ContextRefreshAction = "reserve" | "handover" | "release";
export interface ContextRefreshOptions {
  db: Database.Database;
  now?: () => number;
  assertCurrentOperator(actor: ContextRefreshActor): void;
  assertCurrentActor(actor: ContextRefreshActor): void;
  /** Synchronous final check of independently refreshed native ancestry, exact launch/config
   * and current Operator binding. Async callbacks are refused before any effect permit. */
  assertExecutor(grant: ContextRefreshGrant): void;
  currentTarget(nodeId: string): ContextRefreshTarget | null;
  observe(target: ContextRefreshTarget): Promise<ContextRefreshObservation>;
  /** Verified server-side immutable evidence only; API callers cannot provide these facts. */
  checkpoint(target: ContextRefreshTarget): ContextRefreshCheckpoint | null;
  checkpointReceipt(request: ContextRefreshCheckpointRequest): ContextRefreshCheckpointReceipt | null;
  reservationEvidence(attempt: ContextRefreshAttempt): ContextRefreshReservationEvidence | null;
  /** Synchronous existing final native/guard/custody/reservation verifier under its
   * exact lifecycle lease. Refresh async native evidence before entering the service. */
  assertCutoverReady(grant: ContextRefreshGrant, attempt: ContextRefreshAttempt, action: ContextRefreshAction): void;
}
export class ContextRefreshError extends Error {
  constructor(readonly code: string, message: string) { super(message); }
}
const canonical = (v: unknown): string => JSON.stringify(v, (_k, x) => x && typeof x === "object" && !Array.isArray(x)
  ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => a.localeCompare(b))) : x);
export const contextRefreshDigest = (v: unknown): string => createHash("sha256").update(canonical(v)).digest("hex");
const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const text = (v: unknown): v is string => typeof v === "string" && v.trim().length > 0;
const sha = (v: unknown): v is string => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
const actorSame = (a: ContextRefreshActor, b: ContextRefreshActor) => a.session === b.session && a.generation === b.generation;
function fail(code: string, message: string): never { throw new ContextRefreshError(code, message); }
type GrantRow = { grant_json: string; revoked_at: number | null };
type RequestRow = { request_json: string; phase: string; request_digest: string };
type AttemptRow = { attempt_json: string; phase: ContextRefreshAttempt["phase"]; pending_action: ContextRefreshAction | null };

/** Pure daemon ledger/decisions. No method sends input, launches a native process,
 * releases a claim or grants coordinator authority. Effects consume a one-shot permit. */
export class ContextRefreshService {
  private readonly db: Database.Database;
  private readonly clock: () => number;
  constructor(private readonly options: ContextRefreshOptions) { this.db = options.db; this.clock = options.now ?? Date.now; }

  grant(actor: ContextRefreshActor, input: ContextRefreshGrant): ContextRefreshGrant {
    this.operator(actor); this.validateGrant(input);
    if (!actorSame(actor, input.executor)) fail("refresh_executor_mismatch", "Grant must name the genuine granting Operator generation");
    return this.db.transaction(() => {
      const old = this.db.prepare("SELECT * FROM context_refresh_grants WHERE grant_id=?").get(input.grantId) as GrantRow | undefined;
      if (old) {
        if (!same(JSON.parse(old.grant_json), input) || old.revoked_at !== null) fail("refresh_grant_conflict", "Immutable grant cannot be changed or revived");
        return JSON.parse(old.grant_json) as ContextRefreshGrant;
      }
      this.checked(() => this.options.assertExecutor(input));
      for (const target of input.targets) {
        this.exactTarget(target); this.noDebt(target.nodeId);
        if (this.reservationActive(target.nodeId)) fail("refresh_lifecycle_reserved", "Existing lifecycle reservation excludes a new grant");
      }
      this.db.prepare("INSERT INTO context_refresh_grants VALUES(?,?,?,?,NULL)").run(input.grantId, canonical(input), contextRefreshDigest(input), this.now());
      return input;
    }).immediate();
  }
  revoke(actor: ContextRefreshActor, grantId: string): void {
    this.operator(actor); this.retainedGrant(grantId);
    this.db.prepare("UPDATE context_refresh_grants SET revoked_at=? WHERE grant_id=? AND revoked_at IS NULL").run(this.now(), grantId);
  }

  async evaluate(actor: ContextRefreshActor, grantId: string, nodeId: string): Promise<ContextRefreshDecision> {
    const grant = this.forActor(actor, grantId), target = this.target(grant, nodeId);
    const debt = this.debt(nodeId);
    if (debt) {
      const attempt = this.db.prepare("SELECT phase FROM context_refresh_attempts WHERE attempt_id=?").get(debt) as { phase: ContextRefreshAttempt["phase"] } | undefined;
      const request = this.db.prepare("SELECT phase FROM context_refresh_checkpoint_requests WHERE operation_id=?").get(debt) as { phase: string } | undefined;
      // A prepared ledger entry excludes competing work but is not an uncertain effect.
      const prepared = attempt?.phase === "prepared" || request?.phase === "prepared";
      const phase = attempt?.phase === "prepared" ? "checkpoint-ready"
        : request?.phase === "prepared" ? "preparation-needed"
        : attempt && ["reserved", "replacement-started", "committed-awaiting-acceptance"].includes(attempt.phase)
          ? attempt.phase as ContextRefreshDecision["phase"] : "uncertainty-held";
      return this.decision(nodeId, phase, prepared ? "none" : "reconcile", ["attempt-unresolved"], false, false, null, null, debt);
    }
    try { this.live(actor, grantId); } catch (error) {
      if (error instanceof ContextRefreshError && error.code === "refresh_scope_ended") return this.decision(nodeId, "scope-ended", "none", ["scope-ended"]);
      throw error;
    }
    return this.inspect(grant, target, await this.options.observe(target));
  }

  async prepareCheckpointRequest(actor: ContextRefreshActor, grantId: string, nodeId: string, operationId: string): Promise<ContextRefreshCheckpointRequest> {
    this.id(operationId); const grant = this.live(actor, grantId), target = this.target(grant, nodeId);
    const decision = await this.evaluate(actor, grantId, nodeId);
    if (decision.action !== "request-checkpoint") fail("refresh_checkpoint_not_ready", "A fresh prepare threshold and eligible target are required");
    const instructions = contextRefreshCheckpointInstructions({ grantId, nodeId, generation: target.generation });
    const request: ContextRefreshCheckpointRequest = { operationId, grantId, target, actor,
      qitemId: instructions.qitemId, body: instructions.body };
    this.db.transaction(() => {
      this.live(actor, grantId); this.exactTarget(target); this.noDebt(nodeId);
      this.db.prepare("INSERT INTO context_refresh_checkpoint_requests VALUES(?,?,?,?,?,'prepared',?,NULL)")
        .run(operationId, grantId, nodeId, canonical(request), contextRefreshDigest(request), this.now());
      this.event(grantId, nodeId, operationId, "checkpoint-prepared", request);
    }).immediate();
    return request;
  }
  async beginCheckpointRequest(actor: ContextRefreshActor, operationId: string): Promise<{ maySendEffect: boolean; request: ContextRefreshCheckpointRequest }> {
    const row = this.requestRow(operationId), request = JSON.parse(row.request_json) as ContextRefreshCheckpointRequest;
    this.forActor(actor, request.grantId);
    if (row.phase !== "prepared") return { maySendEffect: false, request };
    const grant = this.live(actor, request.grantId), observed = await this.options.observe(request.target);
    const decision = this.inspect(grant, request.target, observed);
    if (decision.action !== "request-checkpoint") fail("refresh_checkpoint_not_ready", "Checkpoint request eligibility changed before effect");
    return this.db.transaction(() => {
      this.live(actor, request.grantId); this.exactTarget(request.target); this.noDebt(request.target.nodeId, undefined, operationId);
      const changed = this.db.prepare("UPDATE context_refresh_checkpoint_requests SET phase='effect-in-flight' WHERE operation_id=? AND phase='prepared'").run(operationId).changes;
      if (changed) this.event(request.grantId, request.target.nodeId, operationId, "checkpoint-effect-in-flight", request);
      return { maySendEffect: changed === 1, request };
    }).immediate();
  }
  cancelCheckpointPreparation(actor: ContextRefreshActor, operationId: string): void {
    const row = this.requestRow(operationId), request = JSON.parse(row.request_json) as ContextRefreshCheckpointRequest;
    this.forActor(actor, request.grantId);
    if (row.phase !== "prepared") fail("refresh_effect_uncertain", "Only a never-sent checkpoint preparation can be cancelled");
    this.db.transaction(() => {
      this.db.prepare("UPDATE context_refresh_checkpoint_requests SET phase='cancelled-before-effect' WHERE operation_id=? AND phase='prepared'").run(operationId);
      this.event(request.grantId, request.target.nodeId, operationId, "checkpoint-cancelled-before-effect", request);
    }).immediate();
  }
  reconcileCheckpoint(actor: ContextRefreshActor, operationId: string): string {
    const row = this.requestRow(operationId), request = JSON.parse(row.request_json) as ContextRefreshCheckpointRequest;
    this.forActor(actor, request.grantId);
    if (["receipt-confirmed", "cancelled-before-effect", "prepared"].includes(row.phase)) return row.phase;
    let receipt: ContextRefreshCheckpointReceipt | null = null;
    try { receipt = this.options.checkpointReceipt(request); } catch { /* unreadable is not absence */ }
    const valid = receipt && receipt.operationId === operationId && receipt.qitemId === request.qitemId
      && receipt.requestDigest === row.request_digest && sha(receipt.receiptDigest);
    const phase = valid ? "receipt-confirmed" : "uncertainty-held";
    this.db.transaction(() => {
      this.db.prepare("UPDATE context_refresh_checkpoint_requests SET phase=?,receipt_digest=? WHERE operation_id=? AND phase IN ('effect-in-flight','uncertainty-held')")
        .run(phase, valid ? receipt!.receiptDigest : null, operationId);
      this.event(request.grantId, request.target.nodeId, operationId, phase, valid ? receipt : { missing: true });
    }).immediate();
    return phase;
  }

  async prepareAttempt(actor: ContextRefreshActor, grantId: string, nodeId: string,
    ids: { attemptId: string; operationId: string; reservationId: string }): Promise<ContextRefreshAttempt> {
    for (const id of Object.values(ids)) this.id(id);
    const grant = this.live(actor, grantId), target = this.target(grant, nodeId);
    const decision = await this.evaluate(actor, grantId, nodeId);
    if (decision.action !== "reserve") fail("refresh_cutover_not_ready", "Exact checkpoint, fresh threshold and prerequisites are required");
    const checkpoint = this.verifiedCheckpoint(target);
    if (!checkpoint) fail("refresh_checkpoint_changed", "Checkpoint is no longer verified");
    const attempt: ContextRefreshAttempt = { ...ids, grantId, target, checkpoint, phase: "prepared" };
    this.db.transaction(() => {
      this.live(actor, grantId); this.exactTarget(target); this.noDebt(nodeId);
      if (!same(this.verifiedCheckpoint(target), checkpoint)) fail("refresh_checkpoint_changed", "Checkpoint changed while preparing");
      this.db.prepare("INSERT INTO context_refresh_attempts VALUES(?,?,?,?,?,?,?,'prepared',NULL,?,NULL)")
        .run(ids.attemptId, grantId, nodeId, canonical(attempt), contextRefreshDigest(attempt), ids.operationId, ids.reservationId, this.now());
      this.event(grantId, nodeId, ids.operationId, "prepared", attempt);
    }).immediate();
    return attempt;
  }
  attempt(attemptId: string): ContextRefreshAttempt {
    const row = this.attemptRow(attemptId); return { ...JSON.parse(row.attempt_json), phase: row.phase } as ContextRefreshAttempt;
  }
  cancelPreparation(actor: ContextRefreshActor, attemptId: string): ContextRefreshAttempt {
    const attempt = this.attempt(attemptId); this.forActor(actor, attempt.grantId);
    return this.db.transaction(() => {
      const row = this.attemptRow(attemptId);
      if (row.phase !== "prepared" || row.pending_action !== null) fail("refresh_effect_uncertain", "An effect may have begun; preparation cannot be cancelled");
      this.db.prepare("UPDATE context_refresh_attempts SET phase='cancelled-before-effect' WHERE attempt_id=?").run(attemptId);
      this.event(attempt.grantId, attempt.target.nodeId, attempt.operationId, "cancelled-before-effect", attempt);
      return this.attempt(attemptId);
    }).immediate();
  }
  beginEffect(actor: ContextRefreshActor, attemptId: string, action: ContextRefreshAction): { maySendEffect: boolean; attempt: ContextRefreshAttempt } {
    const attempt = this.attempt(attemptId); this.forActor(actor, attempt.grantId);
    if (!["reserve", "handover", "release"].includes(action)) fail("refresh_action_invalid", "Unknown refresh action");
    return this.db.transaction(() => {
      const row = this.attemptRow(attemptId), expected = { reserve: "prepared", handover: "reserved", release: "committed-awaiting-acceptance" }[action];
      if (row.pending_action !== null || row.phase !== expected) return { maySendEffect: false, attempt: this.attempt(attemptId) };
      const grant = this.live(actor, attempt.grantId); this.noDebt(attempt.target.nodeId, attemptId);
      if (action !== "release") this.exactTarget(attempt.target);
      if (action === "release" && !this.accepted(grant, attempt, this.evidence(attempt))) fail("refresh_acceptance_required", "Actual successor ACK and current independent acceptance are required");
      this.checked(() => this.options.assertCutoverReady(grant, attempt, action));
      const phase = action === "reserve" ? "effect-in-flight" : action === "handover" ? "replacement-started" : "committed-awaiting-acceptance";
      this.db.prepare("UPDATE context_refresh_attempts SET phase=?,pending_action=? WHERE attempt_id=?").run(phase, action, attemptId);
      this.event(attempt.grantId, attempt.target.nodeId, attempt.operationId, `${action}-effect-in-flight`, attempt);
      return { maySendEffect: true, attempt: this.attempt(attemptId) };
    }).immediate();
  }
  /** Read-only source reconciliation remains possible after scope expiry or native death.
   * Missing/earlier-stage receipts never authorize re-sending the pending action. */
  reconcile(actor: ContextRefreshActor, attemptId: string): ContextRefreshAttempt {
    const attempt = this.attempt(attemptId), grant = this.forActor(actor, attempt.grantId), row = this.attemptRow(attemptId);
    if (["prepared", "refreshed", "cancelled-before-effect"].includes(row.phase)) return attempt;
    const evidence = this.evidence(attempt);
    let phase: ContextRefreshAttempt["phase"] = "uncertainty-held", settled = false;
    if (evidence) {
      if (evidence.state === "released" && evidence.releaseMode === "accepted_successor" && this.accepted(grant, attempt, evidence)
        && evidence.releasedBy && actorSame(evidence.releasedBy, grant.executor)) { phase = "refreshed"; settled = true; }
      else if (evidence.state === "released" && evidence.releaseMode === "cancel_before_replacement"
        && !evidence.successor && evidence.releasedBy && actorSame(evidence.releasedBy, grant.executor)) { phase = "cancelled-before-effect"; settled = true; }
      else if (evidence.state === "committed" && row.pending_action !== "release" && this.successor(attempt, evidence)) { phase = "committed-awaiting-acceptance"; settled = true; }
      else if (evidence.state === "started" && row.pending_action !== "release") { phase = "replacement-started"; }
      else if (evidence.state === "reserved" && (row.pending_action === "reserve" || row.pending_action === null)) { phase = "reserved"; settled = true; }
    }
    this.db.transaction(() => {
      this.db.prepare("UPDATE context_refresh_attempts SET phase=?,pending_action=?,receipt_digest=? WHERE attempt_id=?")
        .run(phase, settled ? null : row.pending_action, evidence?.receiptDigest ?? null, attemptId);
      this.event(attempt.grantId, attempt.target.nodeId, attempt.operationId, phase, evidence ?? { missing: true });
    }).immediate();
    return this.attempt(attemptId);
  }

  private inspect(grant: ContextRefreshGrant, target: ContextRefreshTarget, observation: ContextRefreshObservation): ContextRefreshDecision {
    const now = this.now(), holds = [...observation.holds];
    if (!same(observation.identity, target) || !same(this.options.currentTarget(target.nodeId), target)) holds.push("binding-changed");
    if (target.nodeId === grant.executor.nodeId || target.sessionName === grant.executor.session) holds.push("operator-self-refresh");
    if (!((target.runtime === "codex" && observation.capability === "codex-reserved-fresh")
      || (target.runtime === "pi" && observation.capability === "pi-reserved-fresh"))) holds.push("runtime-unsupported");
    if (!this.fresh(observation.observedAt, 5000) || !observation.native.verified || !text(observation.native.fingerprint)
      || !text(observation.native.launchId) || !this.fresh(observation.native.observedAt, 5000)) holds.push("native-proof-unavailable");
    if (observation.activity.value !== "idle") holds.push(observation.activity.value === "busy" ? "busy" : "activity-unknown");
    if (!this.fresh(observation.activity.observedAt, 5000)) holds.push("activity-stale");
    if (this.reservationActive(target.nodeId)) holds.push("lifecycle-reserved");
    let baseline: number | null = null, delta: number | null = null;
    const c = observation.compactions;
    if (!holds.some(hold => hold !== "usage-unavailable" && hold !== "usage-stale") && c !== null) {
      if (!Number.isSafeInteger(c.count) || c.count < 0 || !Number.isSafeInteger(c.observedAt) || c.observedAt > now || c.observedAt < 0 || !text(c.source) || !text(c.cursor)) holds.push("compaction-evidence-invalid");
      else this.db.transaction(() => {
        const previous = this.db.prepare("SELECT baseline_count,highest_count,source,cursor FROM context_refresh_baselines WHERE node_id=? AND generation=? AND native_id=?")
          .get(target.nodeId, target.generation, target.nativeId) as { baseline_count: number; highest_count: number; source: string; cursor: string } | undefined;
        if (target.runtime === "pi") {
          // Pi native IDs are canonical files. The immutable baseline must also
          // bind their native header; a replaced file cannot inherit its count.
          const cursor = (value: string): { sessionFile: string; sessionHeaderId: string } | null => {
            try { const v = JSON.parse(value); return v?.version === 1 && v.sessionFile === target.nativeId
              && typeof v.sessionHeaderId === "string" && v.sessionHeaderId.length > 0 ? v : null; } catch { return null; }
          };
          const current = cursor(c.cursor), original = previous ? cursor(previous.cursor) : current;
          if (c.source !== "pi_compaction_jsonl" || !current || !original || previous && previous.source !== c.source
            || current.sessionFile !== original.sessionFile || current.sessionHeaderId !== original.sessionHeaderId) {
            holds.push("compaction-evidence-invalid"); return;
          }
        }
        if (previous && c.count < previous.highest_count) { holds.push("compaction-evidence-invalid"); return; }
        if (!previous) this.db.prepare("INSERT INTO context_refresh_baselines VALUES(?,?,?,?,?,?,?,?)")
          .run(target.nodeId, target.generation, target.nativeId, c.count, c.count, now, c.source, c.cursor);
        else this.db.prepare("UPDATE context_refresh_baselines SET highest_count=? WHERE node_id=? AND generation=? AND native_id=?")
          .run(c.count, target.nodeId, target.generation, target.nativeId);
        baseline = previous?.baseline_count ?? c.count; delta = c.count - baseline;
      }).immediate();
    }
    const u = observation.usage, usageValid = u !== null && Number.isFinite(u.usedPercent) && u.usedPercent >= 0 && u.usedPercent <= 100
      && text(u.source) && text(u.cursor) && this.fresh(u.observedAt, grant.policy.maxUsageAgeMs);
    const compactTrigger = delta !== null && delta >= grant.policy.successfulCompactions;
    // Complete successful native compactions are independently sufficient. Unknown
    // usage is not exhaustion; all non-usage readiness/custody holds remain binding.
    if (compactTrigger) for (let i = holds.length - 1; i >= 0; i--) {
      if (holds[i] === "usage-unavailable" || holds[i] === "usage-stale") holds.splice(i, 1);
    }
    if (!usageValid && !compactTrigger) holds.push(u ? "usage-stale" : "usage-unavailable");
    const prepare = compactTrigger || Boolean(usageValid && u!.usedPercent >= grant.policy.preparePercent);
    const rotate = compactTrigger || Boolean(usageValid && u!.usedPercent >= grant.policy.rotatePercent);
    if (holds.length) return this.decision(target.nodeId, "prerequisite-held", "none", [...new Set(holds)], prepare, rotate, baseline, delta);
    if (!prepare) return this.decision(target.nodeId, "watching", "observe", [], false, false, baseline, delta);
    const checkpoint = this.verifiedCheckpoint(target);
    if (checkpoint) return this.decision(target.nodeId, "checkpoint-ready", rotate ? "reserve" : "observe", [], true, rotate, baseline, delta);
    const requested = this.db.prepare("SELECT 1 FROM context_refresh_checkpoint_requests WHERE grant_id=? AND node_id=? AND phase='receipt-confirmed'").get(grant.grantId, target.nodeId);
    return this.decision(target.nodeId, requested ? "checkpoint-requested" : "preparation-needed", requested ? "observe" : "request-checkpoint", ["checkpoint-required"], true, rotate, baseline, delta);
  }
  private verifiedCheckpoint(target: ContextRefreshTarget): ContextRefreshCheckpoint | null {
    const cp = this.options.checkpoint(target);
    return cp && same(cp.target, target) && text(cp.checkpointId) && sha(cp.checkpointHash) && sha(cp.queueDigest)
      && actorSame(cp.authoredBy, { session: target.sessionName, generation: target.generation }) && cp.outstandingEffects === 0 ? cp : null;
  }
  private evidence(attempt: ContextRefreshAttempt): ContextRefreshReservationEvidence | null {
    let e: ContextRefreshReservationEvidence | null = null; try { e = this.options.reservationEvidence(attempt); } catch { return null; }
    return e && e.reservationId === attempt.reservationId && e.operationId === attempt.operationId && e.targetNodeId === attempt.target.nodeId
      && e.predecessorGeneration === attempt.target.generation && e.predecessorNativeId === attempt.target.nativeId
      && e.checkpointHash === attempt.checkpoint.checkpointHash && sha(e.receiptDigest) && e.custodyVerified ? e : null;
  }
  private successor(attempt: ContextRefreshAttempt, evidence: ContextRefreshReservationEvidence): boolean {
    const s = evidence.successor, current = this.options.currentTarget(attempt.target.nodeId);
    return evidence.successorVerified && s !== null && text(s.generation) && text(s.nativeId) && s.generation !== attempt.target.generation
      && s.nativeId !== attempt.target.nativeId && s.configurationDigest === attempt.target.configurationDigest
      && same(current, { ...attempt.target, generation: s.generation, nativeId: s.nativeId });
  }
  private accepted(grant: ContextRefreshGrant, attempt: ContextRefreshAttempt, e: ContextRefreshReservationEvidence | null): boolean {
    if (!e || !this.successor(attempt, e) || !e.successorAck || !e.independentAcceptance
      || !actorSame(e.successorAck, { session: attempt.target.sessionName, generation: e.successor!.generation })
      || !actorSame(e.independentAcceptance, grant.validator)
      || [grant.executor.session, attempt.target.sessionName].includes(e.independentAcceptance.session)) return false;
    try { this.checked(() => this.options.assertCurrentActor(e.successorAck!)); this.checked(() => this.options.assertCurrentActor(e.independentAcceptance!)); return true; } catch { return false; }
  }
  private live(actor: ContextRefreshActor, grantId: string): ContextRefreshGrant {
    const grant = this.forActor(actor, grantId), row = this.retainedGrant(grantId);
    if (row.revoked_at !== null || grant.validUntil <= this.now()) fail("refresh_scope_ended", "Refresh scope expired or was revoked; no new effects");
    this.operator(actor); this.checked(() => this.options.assertExecutor(grant)); return grant;
  }
  private forActor(actor: ContextRefreshActor, grantId: string): ContextRefreshGrant {
    const grant = JSON.parse(this.retainedGrant(grantId).grant_json) as ContextRefreshGrant;
    if (!actorSame(actor, grant.executor)) fail("refresh_actor_mismatch", "Actual retained executor identity required");
    return grant;
  }
  private retainedGrant(grantId: string): GrantRow {
    return this.db.prepare("SELECT grant_json,revoked_at FROM context_refresh_grants WHERE grant_id=?").get(grantId) as GrantRow | undefined
      ?? fail("refresh_grant_missing", "No explicit context refresh grant");
  }
  private target(grant: ContextRefreshGrant, nodeId: string): ContextRefreshTarget {
    return grant.targets.find(t => t.nodeId === nodeId) ?? fail("refresh_target_out_of_scope", "Target is outside the finite grant");
  }
  private exactTarget(target: ContextRefreshTarget): void {
    if (!same(this.options.currentTarget(target.nodeId), target)) fail("refresh_binding_changed", "Exact current target binding/configuration required");
  }
  private operator(actor: ContextRefreshActor): void {
    if (actor?.session !== "operator-agent@kernel" || !text(actor.generation)) fail("refresh_operator_required", "Genuine current Kernel Operator required");
    this.checked(() => this.options.assertCurrentOperator(actor));
  }
  private debt(nodeId: string, ownAttempt?: string, ownRequest?: string): string | null {
    const attempt = this.db.prepare("SELECT attempt_id FROM context_refresh_attempts WHERE node_id=? AND phase NOT IN ('refreshed','cancelled-before-effect') AND attempt_id!=? LIMIT 1").get(nodeId, ownAttempt ?? "") as { attempt_id: string } | undefined;
    if (attempt) return attempt.attempt_id;
    const request = this.db.prepare("SELECT operation_id FROM context_refresh_checkpoint_requests WHERE node_id=? AND phase NOT IN ('receipt-confirmed','cancelled-before-effect') AND operation_id!=? LIMIT 1").get(nodeId, ownRequest ?? "") as { operation_id: string } | undefined;
    if (request) return request.operation_id;
    const native = this.db.prepare("SELECT i.operation_id FROM native_duty_intents i JOIN native_duty_registrations r ON r.registration_id=i.registration_id WHERE r.node_id=? AND i.phase!='receipt-confirmed' LIMIT 1").get(nodeId) as { operation_id: string } | undefined;
    return native?.operation_id ?? null;
  }
  private noDebt(nodeId: string, ownAttempt?: string, ownRequest?: string): void {
    if (this.debt(nodeId, ownAttempt, ownRequest)) fail("refresh_unresolved_effect", "Node-wide unresolved effect/attempt cannot be bypassed by a new grant, generation or registration");
  }
  private reservationActive(nodeId: string): boolean {
    return Boolean(this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state!='released' LIMIT 1").get(nodeId));
  }
  private requestRow(operationId: string): RequestRow {
    return this.db.prepare("SELECT request_json,request_digest,phase FROM context_refresh_checkpoint_requests WHERE operation_id=?").get(operationId) as RequestRow | undefined
      ?? fail("refresh_request_missing", "No retained checkpoint request");
  }
  private attemptRow(attemptId: string): AttemptRow {
    return this.db.prepare("SELECT attempt_json,phase,pending_action FROM context_refresh_attempts WHERE attempt_id=?").get(attemptId) as AttemptRow | undefined
      ?? fail("refresh_attempt_missing", "No retained refresh attempt");
  }
  private event(grantId: string, nodeId: string, operationId: string, phase: string, evidence: unknown): void {
    this.db.prepare("INSERT INTO context_refresh_events(grant_id,node_id,operation_id,phase,evidence_digest,observed_at) VALUES(?,?,?,?,?,?)")
      .run(grantId, nodeId, operationId, phase, contextRefreshDigest(evidence), this.now());
  }
  private decision(nodeId: string, phase: ContextRefreshDecision["phase"], action: ContextRefreshDecision["action"], holds: ContextRefreshHold[] = [],
    prepareThreshold = false, rotateThreshold = false, baselineCompactions: number | null = null,
    compactionsSinceBaseline: number | null = null, attemptId: string | null = null): ContextRefreshDecision {
    return { nodeId, phase, action, holds, prepareThreshold, rotateThreshold, baselineCompactions, compactionsSinceBaseline, attemptId };
  }
  private checked(assertion: () => void): void {
    const result: unknown = assertion();
    if (result !== undefined) {
      if (result && typeof (result as PromiseLike<unknown>).then === "function") void Promise.resolve(result).catch(() => undefined);
      fail("refresh_async_proof", "Final proof callbacks must finish synchronously before an effect permit");
    }
  }
  private fresh(at: number, age: number): boolean { return Number.isSafeInteger(at) && at >= 0 && at <= this.now() && this.now() - at <= age; }
  private id(id: string): void { if (!/^[A-Za-z0-9._-]{1,160}$/.test(id)) fail("refresh_id_invalid", "Bounded immutable operation ID required"); }
  private now(): number { const now = this.clock(); if (!Number.isSafeInteger(now) || now < 0) fail("refresh_clock_invalid", "Valid clock required"); return now; }
  private validateGrant(g: ContextRefreshGrant): void {
    const keys = (value: unknown, fields: string) => value !== null && typeof value === "object" && !Array.isArray(value)
      && Object.keys(value).sort().join(",") === fields.split(",").sort().join(",");
    if (!keys(g,"grantId,kind,executor,targets,policy,policyRevision,validUntil,validator,recoveryOwner")
      || !keys(g.executor,"session,generation,nodeId,launchId,configurationDigest")
      || !keys(g.validator,"session,generation") || !keys(g.recoveryOwner,"session,generation"))
      fail("refresh_grant_invalid", "Exact context refresh grant fields required");
    if (!g || g.kind !== "context-refresh" || !text(g.policyRevision) || !Number.isSafeInteger(g.validUntil) || g.validUntil <= this.now()
      || !same(g.policy, DEFAULT_CONTEXT_REFRESH_POLICY) || !Array.isArray(g.targets) || !g.targets.length) fail("refresh_grant_invalid", "Exact finite context-refresh policy and targets required");
    this.id(g.grantId);
    if (!text(g.executor?.nodeId) || !text(g.executor?.launchId) || !sha(g.executor?.configurationDigest)) fail("refresh_grant_invalid", "Exact native executor binding required");
    for (const actor of [g.validator, g.recoveryOwner]) {
      if (!text(actor?.session) || !text(actor?.generation)) fail("refresh_grant_invalid", "Actual validator/recovery owner required");
      this.checked(() => this.options.assertCurrentActor(actor));
    }
    const nodes = new Set<string>();
    for (const t of g.targets) {
      if (!keys(t,"nodeId,sessionName,generation,runtime,nativeId,configurationDigest") || !text(t.nodeId) || !text(t.sessionName) || !text(t.generation) || !text(t.nativeId) || !sha(t.configurationDigest)
        || !["codex", "pi"].includes(t.runtime) || nodes.has(t.nodeId)) fail("refresh_grant_invalid", "Unique exact current target identities required");
      if (t.nodeId === g.executor.nodeId || t.sessionName === g.executor.session) fail("refresh_self_target", "Operator self-refresh requires an independent stable executor");
      if ([t.sessionName, g.executor.session].includes(g.validator.session)) fail("refresh_validator_not_independent", "Validator must be distinct from target and executor");
      nodes.add(t.nodeId);
    }
  }
}
