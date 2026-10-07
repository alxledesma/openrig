import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, lstatSync, readdirSync } from "node:fs";
import { join } from "node:path";
import type { RigRepository } from "./rig-repository.js";
import type { SessionRegistry } from "./session-registry.js";
import type { SeatDeliveryGuard } from "./seat-delivery-guard.js";
import { resolveConcreteHint, type RuntimeAdapter, type NodeBinding, type ResolvedStartupFile } from "./runtime-adapter.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";
import { rotationLocalAddresses } from "./rotation-local-custody.js";
import { NativePermissionStore } from "./native-permission-store.js";
import { canonical } from "./seat-dispatch-reservation.js";
import { SeatLifecycleService } from "./seat-lifecycle-service.js";
import type { EventBus } from "./event-bus.js";
import type { TmuxAdapter } from "../adapters/tmux.js";

export interface RuntimeMigrationTarget {
  runtime: "codex"; model: string; provider: "openai"; effort: string; codexConfigProfile: string;
  profileSha256?: string;
}
export interface RuntimeMigrationExpected {
  nodeId: string; generation: string; sessionId: string; sessionName: string; pane: string;
  runtime: "pi"; configSha256: string; nativeFingerprintSha256: string;
}
export interface RuntimeMigrationRequest {
  operationId: string; target: RuntimeMigrationTarget; expected?: RuntimeMigrationExpected;
}
export class RuntimeMigrationRefusal extends Error {
  constructor(readonly code: string, message: string) { super(message); }
}
const digest = (v: unknown): string => createHash("sha256").update(canonical(v)).digest("hex");
/** Native identity excludes only the sidecar refresh clock. Cursor changes still invalidate it. */
function nativeIdentity(proof: PiNativeProof): string {
  let fingerprint: unknown;
  try { fingerprint = JSON.parse(proof.fingerprint); } catch { throw new RuntimeMigrationRefusal("runtime_migration_native_unknown", "Structured native identity proof required"); }
  if (!isObject(fingerprint) || !proof.launchId) throw new RuntimeMigrationRefusal("runtime_migration_native_unknown", "Native launch identity required");
  const { sidecarUpdatedAt: _clock, ...identity } = fingerprint;
  return digest({ identity, generation: proof.generation, launchId: proof.launchId, lastEntryId: proof.lastEntryId ?? null });
}
const isObject = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
export function parseRuntimeMigration(value: unknown): RuntimeMigrationRequest {
  if (!isObject(value) || Object.keys(value).some(k => !["operationId","target","expected"].includes(k)) || typeof value.operationId !== "string" || !/^[A-Za-z0-9._-]{1,120}$/.test(value.operationId)
    || !isObject(value.target) || Object.keys(value.target).some(k => !["runtime","provider","model","effort","codexConfigProfile","profileSha256"].includes(k)) || value.target.runtime !== "codex" || value.target.provider !== "openai"
    || !["model", "effort", "codexConfigProfile"].every(k => typeof (value.target as Record<string, unknown>)[k] === "string"
      && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test((value.target as Record<string, string>)[k]!))
    || (value.target.profileSha256 !== undefined && (typeof value.target.profileSha256 !== "string" || !/^[a-f0-9]{64}$/.test(value.target.profileSha256)))) {
    throw new RuntimeMigrationRefusal("runtime_migration_invalid", "Explicit operation ID and Codex OpenAI model/effort/profile required");
  }
  if (value.expected !== undefined && (!isObject(value.expected) ||
    Object.keys(value.expected).sort().join(",") !== "configSha256,generation,nativeFingerprintSha256,nodeId,pane,runtime,sessionId,sessionName" || value.expected.runtime !== "pi" ||
    Object.values(value.expected).some(v => typeof v !== "string" || !v) ||
    ![value.expected.configSha256,value.expected.nativeFingerprintSha256].every(v => /^[a-f0-9]{64}$/.test(String(v))))) {
    throw new RuntimeMigrationRefusal("runtime_migration_invalid", "Exact dry-run predecessor binding required");
  }
  return value as unknown as RuntimeMigrationRequest;
}
export const runtimeMigrationReservationId = (operationId: string): string => `runtime-migration-${operationId}`;

interface Deps {
  db: Database.Database; rigRepo: RigRepository; sessionRegistry: SessionRegistry; eventBus: EventBus;
  tmuxAdapter: TmuxAdapter; runtimeAdapters?: Record<string, RuntimeAdapter>;
  piProve?: (session: string) => Promise<PiNativeProof | null>;
  piSkillRoot?: (session: string) => string;
  fileExists?: (path: string) => boolean;
  readFile?: (path: string) => string;
  sleep?: (ms: number) => Promise<void>;
  predecessorProcessExists?: (pid: number) => boolean;
}
export interface PreparedRuntimeMigration {
  request: RuntimeMigrationRequest & { expected: RuntimeMigrationExpected };
  reservationId: string; binding: NodeBinding; predecessorNativeId: string;
  custodySnapshot: string;
  predecessorPids: number[];
  preLaunchFiles: ResolvedStartupFile[]; postLaunchFiles: ResolvedStartupFile[];
  context: Extract<ReturnType<SeatLifecycleService["readStartupContext"]>, { ok: true }>["context"];
}

/** Explicit zero-custody Pi -> Codex handover. Uses the existing SQL reservation
 * fences, but a distinct protocol that the rotation service cannot execute. */
export class SeatRuntimeMigration {
  constructor(private readonly deps: Deps) {}
  private fail(code: string, message: string): never { throw new RuntimeMigrationRefusal(code, message); }
  private guard(): SeatDeliveryGuard {
    const guard = this.deps.tmuxAdapter.deliveryGuard;
    if (!guard || guard.db !== this.deps.db) this.fail("runtime_migration_guard_unavailable", "Same-database lifecycle guard required");
    return guard;
  }
  actor(actor: string, generation: string): string {
    if (actor !== "operator-agent@kernel" || !generation) this.fail("runtime_migration_actor", "Actual current Kernel Operator required");
    const target = this.guard().target(actor);
    const row = this.deps.db.prepare("SELECT status,startup_status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(target.nodeId) as { status: string; startup_status: string } | undefined;
    if (target.session !== actor || target.occupant !== generation || row?.status !== "running" || row.startup_status !== "ready") {
      this.fail("runtime_migration_actor", "Operator generation must be current, running and ready");
    }
    return target.nodeId;
  }
  inspect(operationId: string, actor: string, generation: string): Record<string, unknown> {
    this.actor(actor,generation);
    const row = this.deps.db.prepare("SELECT * FROM seat_dispatch_reservations WHERE reservation_id=?").get(runtimeMigrationReservationId(operationId)) as Record<string,unknown> | undefined;
    if (!row) this.fail("runtime_migration_not_found","No recorded migration attempt");
    const expected = JSON.parse(String(row.expected_json));
    if (expected.protocol !== "runtime-migration-v1") this.fail("runtime_migration_protocol","Reservation is not a runtime migration");
    const audit = this.deps.db.prepare("SELECT action,created_at FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all(row.reservation_id);
    return {ok:true,operationId,reservationId:row.reservation_id,state:row.state === "released" ? "committed" : "unknown_or_in_progress",
      target:expected.target,predecessor:expected.expected,successorGeneration:row.successor_generation,
      nativeIdentityRecorded:row.successor_native_id !== null,blindRetryAllowed:false,audit,
      recovery:row.state === "released" ? "No further migration action required" : "Exclusion retained. Inspect exact native pane and this attempt; explicit reviewed recovery required. Rotation release is refused."};
  }
  /** Complete rows are hashed privately; queue bodies and UNKNOWN payloads never enter receipts. */
  private custody(nodeId: string, session: string, generation: string): string {
    const db = this.deps.db, addresses = rotationLocalAddresses(db, session);
    const queue = db.prepare("SELECT * FROM queue_items WHERE destination_session IN (?,?) OR claimed_by_generation_uuid=? ORDER BY qitem_id").all(...addresses, generation) as Array<Record<string, unknown>>;
    if (queue.some(q => !["done", "failed", "denied", "handed-off", "canceled", "cancelled"].includes(String(q.state))
      && (q.claimed_by_generation_uuid != null || q.state === "in-progress"))) {
      this.fail("runtime_migration_custody", "Predecessor still holds active claims; genuine holder must dispose custody first");
    }
    const authority = db.prepare("SELECT * FROM coordinator_authority WHERE owner_session IN (?,?) OR owner_generation=? ORDER BY rig_id").all(...addresses, generation);
    if (authority.length) this.fail("runtime_migration_authority", "Predecessor still owns coordinator authority; supported transfer is required");
    const unknown = db.prepare("SELECT * FROM outbox_entries WHERE (sender_session IN (?,?) OR destination_session IN (?,?)) AND delivery_state IN ('pending','sending','indeterminate','retained') ORDER BY outbox_id").all(...addresses, ...addresses);
    const resources = db.prepare(`SELECT * FROM coordinator_resources WHERE (rig_id,package_key) IN (
      SELECT rig_id,package_key FROM coordinator_assignments WHERE destination IN (?,?) UNION
      SELECT rig_id,package_key FROM coordinator_stage_assignments WHERE source IN (?,?) OR destination IN (?,?)) ORDER BY rig_id,resource_key`).all(...addresses,...addresses,...addresses);
    if (resources.length) this.fail("runtime_migration_resources", "Predecessor retains assigned resource custody; supported disposition is required");
    return JSON.stringify({ queueSha256: digest(queue), unknownSha256: digest(unknown), unknownCount: unknown.length, resourcesSha256: digest(resources) });
  }
  /** Preserve the actually installed native skill set, never replay historical selections.
   * Preparing missing target skills belongs to the existing managed loadout flow. */
  private skills(nodeId: string): string {
    const target = this.guard().target(nodeId);
    const root = this.deps.piSkillRoot?.(target.session);
    if (!root) this.fail("runtime_migration_skills_unknown","Current native Pi skill directory must be observable");
    const node = this.deps.db.prepare("SELECT cwd FROM nodes WHERE id=?").get(nodeId) as {cwd:string};
    const destination = join(node.cwd,".agents","skills");
    const files: Array<{path:string;sha256:string;mode:number}> = [];
    const walk = (relative: string) => {
      const source = join(root,relative);
      const stat = lstatSync(source);
      if (stat.isSymbolicLink()) this.fail("runtime_migration_skills_unknown","Symlinked native skills need an explicit target loadout");
      if (stat.isDirectory()) { for (const name of readdirSync(source).sort()) walk(join(relative,name)); return; }
      if (!stat.isFile()) this.fail("runtime_migration_skills_unknown","Unsupported native skill entry");
      const hash = createHash("sha256").update(readFileSync(source)).digest("hex");
      const targetFile = join(destination,relative);
      let matches = false;
      try { const targetStat = lstatSync(targetFile); matches = targetStat.isFile() && (targetStat.mode & 0o777) === (stat.mode & 0o777) && createHash("sha256").update(readFileSync(targetFile)).digest("hex") === hash; } catch {}
      if (!matches) this.fail("runtime_migration_skills_unprepared","Current Pi skill bytes are missing or differ in Codex .agents/skills; prepare the approved current target loadout before migration");
      files.push({path:relative,sha256:hash,mode:stat.mode & 0o777});
    };
    let present = false;
    try { lstatSync(root); present = true; } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    if (present) walk("");
    return digest(files);
  }
  private configuration(nodeId: string): Record<string, unknown> {
    const db = this.deps.db;
    return {
      node: db.prepare("SELECT runtime,model,effort,codex_config_profile,cwd,permission_policy,policy_launch_posture FROM nodes WHERE id=?").get(nodeId),
      startup: db.prepare("SELECT * FROM node_startup_context WHERE node_id=?").get(nodeId),
      permission: new NativePermissionStore(db).read(nodeId),
      installedSkillsSha256: this.skills(nodeId),
      resume: db.prepare("SELECT id,session_name,resume_type,resume_token FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(nodeId),
    };
  }
  private expected(nodeId: string, nativeFingerprintSha256: string): RuntimeMigrationExpected {
    const target = this.guard().target(nodeId);
    const session = this.deps.db.prepare("SELECT id,session_name,status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(nodeId) as { id: string; session_name: string; status: string } | undefined;
    const node = this.deps.db.prepare("SELECT runtime FROM nodes WHERE id=?").get(nodeId) as { runtime: string };
    if (node.runtime !== "pi" || !target.occupant || !target.pane || !session || session.session_name !== target.session || session.status !== "running") {
      this.fail("runtime_migration_predecessor", "Exact current running Pi session, generation and pane required");
    }
    return { nodeId, generation: target.occupant, sessionId: session.id, sessionName: target.session,
      pane: target.pane, runtime: "pi", configSha256: digest(this.configuration(nodeId)), nativeFingerprintSha256 };
  }
  async prepare(nodeId: string, request: RuntimeMigrationRequest, actor: string, generation: string, dryRun: boolean): Promise<PreparedRuntimeMigration> {
    this.actor(actor, generation);
    if (!this.guard().ownsLifecycle(nodeId)) this.fail("runtime_migration_guard_unavailable", "Lifecycle lease required");
    const db = this.deps.db, reservationId = runtimeMigrationReservationId(request.operationId);
    if (db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE reservation_id=? OR (node_id=? AND operation_id=?)").get(reservationId, nodeId, request.operationId)) {
      this.fail("runtime_migration_attempt_exists", "This one-shot attempt already has a durable outcome; inspect it, never replay");
    }
    if (db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state!='released'").get(nodeId)) this.fail("runtime_migration_reserved", "Another cutover reservation remains active");
    const provisional = this.expected(nodeId, "");
    const custodySnapshot = this.custody(nodeId, provisional.sessionName, provisional.generation);
    const proof = await this.deps.piProve?.(provisional.sessionName);
    const observedAt = Date.parse(proof?.quiescence?.observedAt ?? "");
    if (proof?.state !== "present" || proof.generation !== provisional.generation || !proof.launchId
      || proof.quiescence?.settled !== true || !Number.isFinite(observedAt) || Date.now() - observedAt < 0 || Date.now() - observedAt > 15000) {
      this.fail("runtime_migration_native_unknown", "Current native Pi identity and fresh settled proof required before cutover");
    }
    const identity = JSON.parse(proof.fingerprint) as {runner?:number[];pi?:number[]};
    const predecessorPids = [identity.runner?.[0],identity.pi?.[0]];
    if (predecessorPids.some(pid => !Number.isSafeInteger(pid) || (pid ?? 0) <= 1) || new Set(predecessorPids).size !== 2) this.fail("runtime_migration_native_unknown","Exact native runner and child identities required");
    const expected = this.expected(nodeId, nativeIdentity(proof));
    if (expected.configSha256 !== provisional.configSha256 || expected.generation !== provisional.generation) this.fail("runtime_migration_changed", "Predecessor changed during proof");
    if (!dryRun && digest(request.expected) !== digest(expected)) this.fail("runtime_migration_changed", "Exact prepared predecessor binding/configuration required; rerun dry-run");
    const session = db.prepare("SELECT resume_type,resume_token FROM sessions WHERE id=?").get(expected.sessionId) as { resume_type: string; resume_token: string };
    if (session.resume_type !== "pi_session_file" || !session.resume_token || !(this.deps.fileExists ?? existsSync)(session.resume_token)) {
      this.fail("runtime_migration_history", "Retained Pi native session file and resume provenance required");
    }
    const node = db.prepare("SELECT rig_id AS rigId,cwd FROM nodes WHERE id=?").get(nodeId) as { rigId: string; cwd: string | null };
    const posture = this.deps.rigRepo.getNodePolicyProvenance(nodeId)?.launchPosture
      ?? this.deps.rigRepo.getRigPolicyProvenance(node.rigId)?.launchPosture ?? "floor";
    if (new NativePermissionStore(db).read(nodeId)) this.fail("runtime_migration_permissions", "Unexpected native permission selection on Pi predecessor");
    const binding: NodeBinding = { ...this.deps.sessionRegistry.getBindingForNode(nodeId)!, nodeId, cwd: node.cwd ?? "",
      model: request.target.model, effort: request.target.effort, codexConfigProfile: request.target.codexConfigProfile, launchPosture: posture };
    const adapter = this.deps.runtimeAdapters?.codex;
    if (!adapter?.preflightRuntimeMigration) this.fail("runtime_migration_target_unavailable", "Codex adapter migration preflight unavailable");
    const verified = await adapter.preflightRuntimeMigration(binding);
    if (verified.authenticated !== true || verified.effective.provider !== request.target.provider
      || verified.effective.model !== request.target.model || verified.effective.effort !== request.target.effort
      || verified.effective.sandbox !== (posture === "full_bypass" ? "danger-full-access" : "workspace-write")
      || (posture === "full_bypass" && verified.effective.approval !== "never")
      || (!dryRun && verified.profileSha256 !== request.target.profileSha256)) this.fail("runtime_migration_profile", "Target profile or permission posture differs from prepared binding");
    const parsed = new SeatLifecycleService(this.deps).readStartupContext(nodeId, binding.cwd);
    if (!parsed.ok) this.fail(parsed.refusal.code, parsed.refusal.message);
    if (parsed.context.runtime !== "pi" || parsed.context.startupActions.some(a => a.type !== "send_text")
      || parsed.context.plan.entries.some(e => e.category === "runtime_resource" || e.category === "plugin" || e.category === "subagent")) {
      this.fail("runtime_migration_startup_incompatible", "Runtime-specific startup resources/actions require an explicit compatible target policy before migration");
    }
    const preLaunchFiles: ResolvedStartupFile[] = [], postLaunchFiles: ResolvedStartupFile[] = [];
    for (const f of parsed.context.resolvedStartupFiles.filter(f => f.appliesOn.includes("fresh_start"))) {
      let content: string;
      try { content = (this.deps.readFile ?? (p => readFileSync(p,"utf8")))(f.absolutePath); }
      catch { if (f.required) this.fail("runtime_migration_startup_missing", "Required startup file is unreadable before replacement"); else continue; }
      const hint = f.deliveryHint === "auto" ? resolveConcreteHint(f.path,content) : f.deliveryHint;
      (hint === "send_text" ? postLaunchFiles : preLaunchFiles).push({ ...f, deliveryHint: hint });
    }
    const context = { ...parsed.context, runtime: "codex", plan: { ...parsed.context.plan, runtime: "codex" } };
    const prepared = { request: { ...request, target: { ...request.target, profileSha256: verified.profileSha256 }, expected }, reservationId,
      binding, predecessorNativeId: session.resume_token, predecessorPids: predecessorPids as number[], custodySnapshot, context, preLaunchFiles, postLaunchFiles };
    this.recheck(prepared, actor, generation);
    return prepared;
  }
  recheck(p: PreparedRuntimeMigration, actor: string, generation: string): void {
    this.actor(actor, generation);
    const e = p.request.expected;
    if (digest(this.expected(e.nodeId, e.nativeFingerprintSha256)) !== digest(e)
      || this.custody(e.nodeId, e.sessionName, e.generation) !== p.custodySnapshot) this.fail("runtime_migration_changed", "Predecessor configuration, custody or UNKNOWN history changed");
  }
  async verifyBeforeEffects(p: PreparedRuntimeMigration, actor: string, generation: string): Promise<void> {
    const target = await this.deps.runtimeAdapters!.codex!.preflightRuntimeMigration!(p.binding);
    if (target.profileSha256 !== p.request.target.profileSha256) this.fail("runtime_migration_profile_changed", "Target profile changed before process effects");
    const e = p.request.expected, proof = await this.deps.piProve?.(e.sessionName);
    const at = Date.parse(proof?.quiescence?.observedAt ?? "");
    if (proof?.state !== "present" || proof.generation !== e.generation || nativeIdentity(proof) !== e.nativeFingerprintSha256
      || proof.quiescence?.settled !== true || !Number.isFinite(at) || Date.now() < at || Date.now() - at > 15000) {
      this.fail("runtime_migration_native_changed", "Native predecessor changed before process effects");
    }
    this.recheck(p,actor,generation);
  }
  begin(p: PreparedRuntimeMigration, actor: string, generation: string, successorGeneration: string): void {
    this.deps.db.transaction(() => {
      this.recheck(p, actor, generation);
      const e = p.request.expected, at = new Date().toISOString();
      this.deps.db.prepare(`INSERT INTO seat_dispatch_reservations
        (reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,
         request_hash,expected_json,frozen_snapshot,state,performer_session,performer_generation,successor_generation,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,'started',?,?,?,?,?)`).run(p.reservationId,p.request.operationId,e.nodeId,e.sessionName,e.generation,p.predecessorNativeId,
        actor,generation,digest(p.request),JSON.stringify({protocol:"runtime-migration-v1",...p.request}),p.custodySnapshot,actor,generation,successorGeneration,at,at);
      this.audit(p, actor, generation, "runtime_migration_started", { predecessorRuntime: "pi", predecessorSessionId: e.sessionId, target: p.request.target, successorGeneration, authenticated: true });
    }).immediate();
  }
  async project(p: PreparedRuntimeMigration): Promise<void> {
    const exists = this.deps.predecessorProcessExists ?? ((pid: number) => {
      try { process.kill(pid,0); return true; }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; throw error; }
    });
    for (let attempt=0; ; attempt++) {
      if (!p.predecessorPids.some(exists)) break;
      if (attempt >= 14) this.fail("runtime_migration_predecessor_survives","Prior native runner or child remains; exclusion retained and Codex launch refused");
      await (this.deps.sleep ?? (ms => new Promise(r=>setTimeout(r,ms))))(200);
    }
    const adapter = this.deps.runtimeAdapters!.codex!;
    const projected = await adapter.project(p.context.plan, p.binding);
    if (projected.failed.length) this.fail("runtime_migration_projection_failed", "Target startup projection failed; cutover outcome requires reconciliation");
    const delivered = await adapter.deliverStartup(p.preLaunchFiles, p.binding);
    if (delivered.failed.length) this.fail("runtime_migration_projection_failed", "Target startup files failed; cutover outcome requires reconciliation");
  }
  /** Same native delivery operations as fresh Codex startup; no predecessor session row is rewritten. */
  async deliverContext(p: PreparedRuntimeMigration): Promise<void> {
    const actions = p.context.startupActions.filter(a => a.appliesOn.includes("fresh_start"));
    const identity = actions.find(a => a.builtin === "session_identity" || a.value.startsWith("OpenRig session identity:"));
    const send = async (text: string) => {
      if (!(await this.deps.tmuxAdapter.sendText(p.binding.tmuxSession!,text)).ok) this.fail("runtime_migration_context_failed","Successor startup text failed");
      await (this.deps.sleep ?? (ms => new Promise(r => setTimeout(r,ms))))(200);
      if (!(await this.deps.tmuxAdapter.sendKeys(p.binding.tmuxSession!,["Enter"])).ok) this.fail("runtime_migration_context_failed","Successor startup submission failed");
    };
    if (identity) await send(identity.value);
    const delivered = await this.deps.runtimeAdapters!.codex!.deliverStartup(p.postLaunchFiles,p.binding);
    if (delivered.failed.length) this.fail("runtime_migration_context_failed","Successor startup file delivery failed");
    for (const phase of ["after_files","after_ready"]) for (const action of actions) if (action !== identity && action.phase === phase) await send(action.value);
  }
  /** Called inside the handover transaction BEFORE any node/session writes. */
  commitConfiguration(p: PreparedRuntimeMigration, actor: string, generation: string): void {
    if (!this.deps.db.inTransaction) this.fail("runtime_migration_transaction", "Atomic handover transaction required");
    this.recheck(p, actor, generation);
    const t = p.request.target;
    this.deps.db.prepare("UPDATE nodes SET runtime=?,model=?,effort=?,codex_config_profile=? WHERE id=?").run(t.runtime,t.model,t.effort,t.codexConfigProfile,p.request.expected.nodeId);
    this.deps.db.prepare("UPDATE node_startup_context SET runtime=? WHERE node_id=?").run(t.runtime,p.request.expected.nodeId);
  }
  /** No claimant was released, and no authority or UNKNOWN record is rewritten. */
  committed(p: PreparedRuntimeMigration, actor: string, generation: string, successorGeneration: string, nativeId: string): void {
    const e = p.request.expected;
    if (!this.deps.db.inTransaction || this.custody(e.nodeId,e.sessionName,e.generation) !== p.custodySnapshot) this.fail("runtime_migration_custody_changed", "Retained custody/UNKNOWN state changed; transaction refused");
    if (!nativeId || !successorGeneration) this.fail("runtime_migration_successor_unknown", "Verified fresh successor native ID and generation required");
    if (this.deps.db.prepare("UPDATE seat_dispatch_reservations SET state='released',successor_generation=?,successor_native_id=?,release_receipt=?,updated_at=? WHERE reservation_id=? AND state='started'")
      .run(successorGeneration,nativeId,digest({successorGeneration,nativeId,target:p.request.target}),new Date().toISOString(),p.reservationId).changes !== 1) this.fail("runtime_migration_changed", "One-shot migration reservation changed");
    this.audit(p,actor,generation,"runtime_migration_committed",{successorGeneration,predecessorSessionId:e.sessionId,retainedStateSha256:digest(p.custodySnapshot),target:p.request.target});
  }
  unknown(p: PreparedRuntimeMigration, actor: string, generation: string): void {
    this.audit(p,actor,generation,"runtime_migration_unknown",{blindRetryAllowed:false,nextAction:"Inspect retained reservation and actual pane/native identity. Keep exclusion until an independently reviewed explicit recovery reconciles this exact attempt."});
  }
  private audit(p: PreparedRuntimeMigration, actor: string, generation: string, action: string, evidence: unknown): void {
    this.deps.db.prepare("INSERT INTO seat_dispatch_reservation_audit(reservation_id,action,actor_session,actor_generation,evidence_json,created_at) VALUES(?,?,?,?,?,?)")
      .run(p.reservationId,action,actor,generation,JSON.stringify(evidence),new Date().toISOString());
  }
}
