import type Database from "better-sqlite3";
import type { CoordinatorAuthorityService } from "./coordinator-authority-service.js";
import type { NativeDutyActor, NativeDutyProof, NativeDutyResumeRequest, NativeDutyScope } from "./native-duty-contract.js";
import { NativeDutyError, NativeDutySupervisionService } from "./native-duty-supervision-service.js";

type Binding = { nodeId: string; session: string; generation: string; runtime: string; rigId: string };
export interface NativeDutyIntegrationOptions {
  db: Database.Database;
  authority: CoordinatorAuthorityService;
  binding: (session: string) => Binding | null;
  observe: (scope: NativeDutyScope, launchId: string, supervisorPid: number) => Promise<NativeDutyProof | null>;
  now?: () => number;
  lifecycleActive?: (nodeId: string) => boolean;
}

/** Connect finite Operator grants to existing work and coordinator authority.
 * No daemon callback sends a coordinator operation or acquires a caller identity.
 * Async native observation completes before a synchronous service boundary.
 */
export class NativeDutyIntegration {
  readonly service: NativeDutySupervisionService;
  private readonly now: () => number;
  private readonly observations = new Map<string, NativeDutyProof | null>();

  constructor(private readonly opts: NativeDutyIntegrationOptions) {
    this.now = opts.now ?? Date.now;
    this.service = new NativeDutySupervisionService({
      db: opts.db, now: this.now,
      approvedScope: (id, candidate) => this.approvedScope(id, candidate),
      assertCurrentOperator: actor => opts.authority.assertCurrentOperator(actor.session, actor.generation),
      assertResumeAuthority: (scope, actor, request) => this.assertResume(scope, actor, request),
      observeNative: (scope, launchId, pid) => {
        const proof = this.observations.get(this.key(scope.scopeId, launchId, pid));
        if (!proof || proof.observedAt > this.now() || this.now() - proof.observedAt > 3000) return null;
        if (!this.approvedScope(scope.scopeId, scope)) return null;
        return proof;
      },
      operationReceipt: (rigId, operationId) => opts.authority.operationReceipt(rigId, operationId),
    });
  }

  private key(scopeId: string, launchId: string, pid: number): string {
    return JSON.stringify([scopeId, launchId, pid]);
  }

  /** Read-only discovery lets a genuine launch wait for its Operator grant and
   * recover a lost registration response without another registration POST.
   * Existing registrations remain discoverable for receipt reconciliation even
   * after their work window closes; discovery never revives them.
   */
  async enrollment(actor: NativeDutyActor, input: { scopeId: string; launchId: string; supervisorPid: number }): Promise<{ state: "waiting" | "ready" | "held"; registrationId?: string }> {
    if (!input.scopeId || input.scopeId.length > 160 || !input.launchId || input.launchId.length > 160
      || !Number.isSafeInteger(input.supervisorPid) || input.supervisorPid <= 1) return { state: "held" };
    const existing = this.opts.db.prepare("SELECT registration_id,session_name,generation,supervisor_pid FROM native_duty_registrations WHERE scope_id=? AND launch_id=?").get(input.scopeId, input.launchId) as { registration_id: string; session_name: string; generation: string; supervisor_pid: number } | undefined;
    if (existing) return existing.session_name === actor.session && existing.generation === actor.generation
      && existing.supervisor_pid === input.supervisorPid ? { state: "ready", registrationId: existing.registration_id } : { state: "held" };
    const binding = this.opts.binding(actor.session);
    if (!binding || binding.generation !== actor.generation) return { state: "held" };
    const grant = this.opts.db.prepare("SELECT scope_json,revoked_at FROM native_duty_grants WHERE scope_id=?").get(input.scopeId) as { scope_json: string; revoked_at: number | null } | undefined;
    if (!grant) return { state: "waiting" };
    let scope: NativeDutyScope;
    try { scope = JSON.parse(grant.scope_json) as NativeDutyScope; } catch { return { state: "held" }; }
    if (grant.revoked_at !== null || scope.sessionName !== actor.session || scope.generation !== actor.generation
      || scope.validUntil <= this.now() || !this.approvedScope(input.scopeId)) return { state: "held" };
    await this.refreshNative(actor, input);
    const proof = this.observations.get(this.key(input.scopeId, input.launchId, input.supervisorPid));
    const matches = proof && proof.nodeId === scope.nodeId && proof.sessionName === scope.sessionName
      && proof.generation === scope.generation && proof.runtime === scope.runtime
      && proof.configurationDigest === scope.configurationDigest && proof.launchId === input.launchId
      && proof.supervisorPid === input.supervisorPid && proof.fingerprint.length > 0
      && Number.isSafeInteger(proof.observedAt) && proof.observedAt <= this.now() && this.now() - proof.observedAt <= 3000
      && proof.nativePresent && proof.supervisorIsNativeAncestor && !proof.lifecycleReserved;
    return { state: matches ? "ready" : "waiting" };
  }

  /** A grant's bounds are explicitly supplied by the genuine Operator. The
   * existing plan supplies the work boundary; no arbitrary timeout is invented.
   * The holder coordinates its rig's workers, so work need not be assigned to
   * the holder itself. Exact acceptance ends the applicable package's work.
   */
  private approvedScope(scopeId: string, candidate?: NativeDutyScope): NativeDutyScope | null {
    let scope = candidate;
    if (!scope) {
      const row = this.opts.db.prepare("SELECT scope_json FROM native_duty_grants WHERE scope_id=?").get(scopeId) as { scope_json: string } | undefined;
      if (!row) return null;
      try { scope = JSON.parse(row.scope_json) as NativeDutyScope; } catch { return null; }
    }
    const recovery = this.opts.authority.coordinationRecovery;
    const binding = this.opts.binding(scope.sessionName);
    const authority = this.opts.authority.get(scope.rigId);
    if (!recovery || !binding || scope.scopeId !== scopeId || binding.nodeId !== scope.nodeId
      || binding.session !== scope.sessionName || binding.generation !== scope.generation
      || binding.runtime !== scope.runtime || binding.rigId !== scope.rigId
      || recovery.configurationDigest(scope.sessionName) !== scope.configurationDigest
      || !authority || authority.owner_session !== scope.sessionName || authority.owner_generation !== scope.generation
      || !["active", "reconciling"].includes(authority.state)
      || !Number.isSafeInteger(scope.validUntil) || scope.validUntil <= this.now()
      || !Number.isSafeInteger(scope.maxLeaseMs) || scope.maxLeaseMs < 1000 || scope.maxLeaseMs > 3600000) return null;
    // An explicit grant is not a bypass of reservation-based custody exclusion.
    if (this.opts.lifecycleActive?.(scope.nodeId)
      || this.opts.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state!='released' LIMIT 1").get(scope.nodeId)) return null;
    const plan = recovery.plan(scope.rigId);
    if (!plan || plan.operatorGeneration !== this.opts.authority.generation("operator-agent@kernel")) return null;
    const boundedWork = recovery.continuationTasks(scope.rigId).some(task => {
      if (task.boundary || task.deadline < scope!.validUntil || task.admission.validUntil < scope!.validUntil
        || task.admission.generation !== this.opts.authority.generation(task.owner)
        || task.admission.configurationDigest !== recovery.configurationDigest(task.owner)
        || !task.admission.qualificationRef || !task.admission.capacityRef || !task.admission.effortRef) return false;
      const restrictions = plan.dispatchRestrictions?.filter(r => r.session === task.owner) ?? [];
      if (restrictions.length && !restrictions.some(r => r.generation === task.admission.generation
        && r.validUntil >= scope!.validUntil && r.packageKeys.includes(task.packageKey))) return false;
      return true;
    });
    return boundedWork ? scope : null;
  }

  private assertResume(scope: NativeDutyScope, actor: NativeDutyActor, request: NativeDutyResumeRequest): void {
    const a = this.opts.authority.get(scope.rigId);
    if (!this.approvedScope(scope.scopeId, scope) || !a || a.owner_session !== actor.session
      || a.owner_generation !== actor.generation || this.opts.authority.generation(actor.session) !== actor.generation
      || a.lease_until <= this.now() || a.epoch !== request.expectedEpoch
      || this.opts.authority.reconciliationDigest(scope.rigId) !== request.expectedObligationsDigest) {
      throw new NativeDutyError("native_duty_authority_changed", "Current live holder, work scope and expected authority must still match");
    }
    const baton = this.opts.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(a.baton_id) as { destination_session: string; state: string; claimed_by_generation_uuid: string | null } | undefined;
    const active = a.state === "active";
    if (!baton || baton.destination_session !== actor.session
      || (active ? baton.state !== "in-progress" || baton.claimed_by_generation_uuid !== actor.generation
        : !["pending", "in-progress"].includes(baton.state) || (baton.claimed_by_generation_uuid !== null && baton.claimed_by_generation_uuid !== actor.generation))) {
      throw new NativeDutyError("native_duty_baton_changed", "Exact canonical baton custody is required");
    }
  }

  /** Read only native proof for the transport-authenticated scope owner. Clear
   * the old cache before observing, so a failed refresh cannot retain success.
   */
  async refreshNative(actor: NativeDutyActor, input: { scopeId?: string; launchId?: string; supervisorPid?: number; registrationId?: string }): Promise<void> {
    let scopeId = input.scopeId, launchId = input.launchId, pid = input.supervisorPid;
    if (input.registrationId) {
      const row = this.opts.db.prepare("SELECT scope_id,launch_id,supervisor_pid FROM native_duty_registrations WHERE registration_id=?").get(input.registrationId) as { scope_id: string; launch_id: string; supervisor_pid: number } | undefined;
      if (!row) return;
      scopeId = row.scope_id; launchId = row.launch_id; pid = row.supervisor_pid;
    }
    if (!scopeId || !launchId || !Number.isSafeInteger(pid)) return;
    const key = this.key(scopeId, launchId, pid!);
    this.observations.delete(key);
    const scope = this.approvedScope(scopeId);
    if (!scope || scope.sessionName !== actor.session || scope.generation !== actor.generation) return;
    try { this.observations.set(key, await this.opts.observe(scope, launchId, pid!)); }
    catch { this.observations.set(key, null); }
  }
}
