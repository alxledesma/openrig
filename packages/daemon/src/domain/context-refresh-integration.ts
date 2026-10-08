import { AsyncLocalStorage } from "node:async_hooks";
import type Database from "better-sqlite3";
import type { TmuxAdapter } from "../adapters/tmux.js";
import type { QueueRepository } from "./queue-repository.js";
import type { SeatActivityService } from "./seat-activity-service.js";
import type { WhoamiService } from "./whoami-service.js";
import type { SeatDeliveryGuard } from "./seat-delivery-guard.js";
import type { NativeDutyLaunchStore } from "./native-duty-launch.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";
import type { SeatHandoverService } from "./seat-handover-service.js";
import type { SeatDispatchReservationService } from "./seat-dispatch-reservation.js";
import type { ContextRefreshActor, ContextRefreshGrant, ContextRefreshDecision, ContextRefreshAttempt, ContextRefreshObservation, ContextRefreshCheckpoint } from "./context-refresh-contract.js";

export interface ContextRefreshIntegrationDeps {
  db: Database.Database; queue: QueueRepository; guard: SeatDeliveryGuard; tmux: TmuxAdapter;
  whoami: WhoamiService; activity: SeatActivityService; store: NativeDutyLaunchStore;
  rotationRoot: string;
  piRotation?: { agentDir(session: string): string; runnerEntryPath: string; resolvePosture(nodeId: string, rigId: string): "floor" | "full_bypass" };
  configurationDigest(session: string): string | null | undefined;
  piState(session: string): Promise<unknown>;
  piProof(session: string, generation: string): Promise<PiNativeProof | null>;
  handoverFactory(input: { dispatchReservations: SeatDispatchReservationService;
    rotationPrecondition: (seat: string, expected: Record<string, unknown>) => Promise<void> }): SeatHandoverService;
}
export interface ContextRefreshSelection { grantId: string; nodeId: string; operationId?: string }
export interface ContextRefreshStatus {
  grantId: string; nodeId: string;
  checkpointDraft: { draftId: string; phase: "submitted" | "frozen" } | null;
  invocation: { operationId: string; state: "in-flight" | "completed" } | null;
  attempt: ContextRefreshAttempt | null;
  checkpointRequest: { operationId: string; qitemId: string; phase: string } | null;
}
export interface ContextRefreshStepResult extends ContextRefreshStatus {
  operationId: string;
  effect: "none" | "checkpoint" | "reserve" | "handover" | "release";
  decision: ContextRefreshDecision | null;
  hold: string | null;
}
export type ContextRefreshEnrollment =
  | { state: "held"; reason: string }
  | { state: "ready"; grants: ContextRefreshGrant[]; pollMs: number };
export interface ContextRefreshCheckpointPacket {
  current_work: unknown; decisions: unknown; memory: unknown; constraints: unknown;
  standing_duties: unknown; evidence: unknown; next_action: unknown; outstanding_effects: [];
}
export interface ContextRefreshDraftReceipt { draftId: string; grantId: string; nodeId: string; phase: "submitted" }
interface RetainedCheckpointDraft { draftId: string; grantId: string; target: ContextRefreshTarget; authoredBy: ContextRefreshActor; packet: ContextRefreshCheckpointPacket; submittedAt: number }
/** Route facade. Actor is authenticated transport identity, never supplied proof. */
export interface ContextRefreshFacade {
  grant(actor: ContextRefreshActor, grant: ContextRefreshGrant): Promise<ContextRefreshGrant>;
  revoke(actor: ContextRefreshActor, input: { grantId: string }): Promise<void>;
  enrollment(actor: ContextRefreshActor, input: { launchId: string; supervisorPid: number }): Promise<ContextRefreshEnrollment>;
  evaluate(actor: ContextRefreshActor, input: ContextRefreshSelection): Promise<ContextRefreshDecision>;
  observe(actor: ContextRefreshActor, input: ContextRefreshSelection): Promise<ContextRefreshObservation>;
  submitCheckpoint(actor: ContextRefreshActor, input: ContextRefreshSelection & { packet: ContextRefreshCheckpointPacket }): Promise<ContextRefreshDraftReceipt>;
  recordCheckpoint(actor: ContextRefreshActor, input: ContextRefreshSelection & { packet: ContextRefreshCheckpointPacket }): Promise<ContextRefreshDraftReceipt>;
  step(actor: ContextRefreshActor, input: ContextRefreshSelection & { operationId: string }): Promise<ContextRefreshStepResult>;
  reconcile(actor: ContextRefreshActor, input: ContextRefreshSelection): Promise<ContextRefreshStatus>;
  status(actor: ContextRefreshActor, input: ContextRefreshSelection): Promise<ContextRefreshStatus>;
  attest(actor: ContextRefreshActor, input: { attemptId: string; kind: "successor_ack" | "independent_acceptance"; evidenceRef: string }): Promise<ContextRefreshAttempt>;
}

import { createHash, randomUUID } from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync, lstatSync, realpathSync, openSync, closeSync, fsyncSync, constants } from "node:fs";
import { resolve, sep } from "node:path";
import { ContextRefreshService, ContextRefreshError, contextRefreshDigest, type ContextRefreshAction, type ContextRefreshCheckpointRequest } from "./context-refresh-service.js";
import { ContextRefreshObserver, readContextRefreshTranscript, startContextRefreshCatchup, type ContextRefreshBinding } from "./context-refresh-observer.js";
import { SeatDispatchReservationService as Reservations, canonical, type DispatchReservation } from "./seat-dispatch-reservation.js";
import { resolveCodexNativeState, assertCoordinatorRotationSafe } from "./rotation-facts-resolver.js";
import { canonicalPiSessionFile, observePiRotationIdentity, resolvePiRotationNativeState } from "./pi-rotation-facts-resolver.js";
import { parseNativeModelWindow } from "./model-window.js";
import { assertManagedUnattended, assertRotationPrecondition } from "./rotation-precondition.js";
import { rotationActiveQueueRows } from "./rotation-local-custody.js";
import { listNativeProcesses, verifyCodexPaneProcess } from "./native-process-lineage.js";
import { observeNativeDutyLaunch, verifyNativeDutyProcessIdentity } from "./native-duty-launch.js";
import type { NativeDutyProof } from "./native-duty-contract.js";
import type { ContextRefreshTarget, ContextRefreshReservationEvidence } from "./context-refresh-contract.js";

const same = (a: unknown, b: unknown) => canonical(a) === canonical(b);
const hash = (bytes: Buffer | string) => createHash("sha256").update(bytes).digest("hex");
const fresh = (at: number, age = 3000) => Number.isSafeInteger(at) && at <= Date.now() && Date.now() - at <= age;
const refuse = (code: string, message: string): never => { throw new ContextRefreshError(code, message); };
const id = (value: string) => /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,159}$/.test(value);
type CheckpointFile = { checkpoint: ContextRefreshCheckpoint; receipt: Record<string, unknown>; expected: Record<string, unknown> };
type Metadata = { inode: string; offset: number; pending: Buffer; nativeId: string | null; model: string | null; window: number | null; invalid: boolean };

/** A separate bounded model/window cursor: launcher defaults never stand in for
 * actual current native turn metadata. Eviction safely requires a complete scan. */
export class NativeModelReader {
  private readonly entries = new Map<string, Metadata>();
  private readonly bindings = new Map<string,string>();
  read(file: string, nativeId: string, binding = nativeId, current: () => boolean = () => true): { model: string; contextWindow: number } | null {
    const address=`${file}\0${nativeId}`,key = `${address}\0${binding}`;
    this.bindings.delete(address);this.bindings.set(address,key);
    while(this.bindings.size>64)this.bindings.delete(this.bindings.keys().next().value!);
    return this.chunk(file,nativeId,key,true,Infinity,current);
  }
  private chunk(file:string,nativeId:string,key:string,background:boolean,remaining=Infinity,current:()=>boolean=()=>true):{model:string;contextWindow:number}|null {
    let state = this.entries.get(key);
    if (!state) {
      if (this.entries.size >= 64) this.entries.delete(this.entries.keys().next().value!);
      state = { inode: "", offset: 0, pending: Buffer.alloc(0), nativeId: null, model: null, window: null, invalid: false };
    }
    this.entries.delete(key); this.entries.set(key, state);
    const chunk = readContextRefreshTranscript(file, state.offset, Math.min(remaining,8 * 1024 * 1024));
    if (state.inode && (state.inode !== chunk.identity || chunk.size < state.offset)) state.invalid = true;
    state.inode = chunk.identity; state.offset += chunk.bytes.length;
    const bytes = Buffer.concat([state.pending, chunk.bytes]); let start = 0;
    for (let end = bytes.indexOf(10); end >= 0; end = bytes.indexOf(10, start)) {
      if (end - start > 1024 * 1024) { state.invalid = true; break; }
      try {
        const row = JSON.parse(bytes.subarray(start, end).toString("utf8"));
        if (row.type === "session_meta") {
          if (state.nativeId && state.nativeId !== row.payload?.id) state.invalid = true;
          state.nativeId = row.payload?.id ?? null;
        }
        if (row.type === "turn_context" && typeof row.payload?.model === "string") {
          if (state.model !== row.payload.model) state.window = null;
          state.model = row.payload.model;
        }
        const window = row.type === "event_msg" && row.payload?.type === "token_count" ? row.payload.info?.model_context_window : null;
        if (Number.isSafeInteger(window) && window > 0) state.window = window;
      } catch { state.invalid = true; }
      start = end + 1;
    }
    state.pending = bytes.subarray(start);
    if (state.pending.length > 1024 * 1024) { state.invalid = true; state.pending = Buffer.alloc(0); }
    if(background && !state.invalid && state.offset < chunk.size) {
      const captured=state,revision=chunk.revision,size=chunk.size;
      startContextRefreshCatchup(captured,budget=>{
        if(this.entries.get(key)!==captured||this.bindings.get(`${file}\0${nativeId}`)!==key||!current())return{bytes:0,complete:true};
        const stat=readContextRefreshTranscript(file,0,0);
        if(stat.identity!==captured.inode||stat.size<captured.offset){captured.invalid=true;return{bytes:0,complete:true};}
        if(stat.size!==size||stat.revision!==revision)return{bytes:0,complete:true};
        const before=captured.offset;this.chunk(file,nativeId,key,false,budget);
        return{bytes:captured.offset-before,complete:captured.invalid||captured.offset>=size};
      });
    }
    return !state.invalid && state.offset === chunk.size && state.pending.length === 0 && state.nativeId === nativeId && state.model && state.window
      ? { model: state.model, contextWindow: state.window } : null;
  }
}

/** Production composition: observations come from daemon services and kernel OS
 * checks. HTTP callers can author checkpoint prose, never native/custody proof. */
export class ContextRefreshIntegration implements ContextRefreshFacade {
  private readonly grantScope = new AsyncLocalStorage<string>();
  private readonly ledger: ContextRefreshService;
  private readonly observer: ContextRefreshObserver;
  private readonly reservations: SeatDispatchReservationService;
  private readonly modelReader = new NativeModelReader();
  private readonly executorProofs = new Map<string, NativeDutyProof>();
  private readonly receiptProofs = new Map<string, { at: number; evidence: ContextRefreshReservationEvidence; digest: string }>();
  private readonly cutovers = new Map<string, { at: number; action: ContextRefreshAction; digest: string }>();
  constructor(private readonly deps: ContextRefreshIntegrationDeps) {
    this.reservations = new Reservations({ db: deps.db, guard: deps.guard,
      piAgentDir: deps.piRotation ? session => deps.piRotation!.agentDir(session) : undefined,
      verifyPredecessor: (seat, expected) => this.rotationPrecondition(seat, expected),
      observeSuccessor: async seat => { const state = await this.nativeState(seat); return { nativeId: state.usage.sessionId!, runtimeContract: state.runtimeContract }; } });
    this.observer = new ContextRefreshObserver({
      binding: node => this.binding(node),
      bindingCurrent: binding => same(this.currentTarget(binding.nodeId),this.targetOnly(binding)),
      native: async binding => {
        const target = deps.guard.maybeTarget(binding.nodeId);
        if (!target?.pane || !same(this.currentTarget(binding.nodeId), this.targetOnly(binding))) return null;
        if (binding.runtime === "pi") {
          const found = await observePiRotationIdentity(deps,binding.sessionName), proof = found.proof;
          return proof.generation === binding.generation && found.row.resumeToken === binding.nativeId
            ? { ...this.targetOnly(binding), verified: true, observedAt: Date.parse(proof.quiescence?.observedAt ?? ""), launchId: proof.launchId,
              fingerprint: canonical({native:proof.fingerprint,launch:proof.verifiedLaunch}) } : null;
        }
        const process = await verifyCodexPaneProcess({ target: target.pane, tmux: deps.tmux, expectedToken: binding.nativeId });
        if (!process || !await verifyNativeDutyProcessIdentity(process.process.pid, this.publicIdentity(binding))) return null;
        if (!same(this.currentTarget(binding.nodeId), this.targetOnly(binding)) || deps.guard.maybeTarget(binding.nodeId)?.pane !== target.pane) return null;
        return { ...this.targetOnly(binding), verified: true, observedAt: Date.now(), launchId: `native-${process.process.pid}-${hash(process.fingerprint)}`, fingerprint: process.fingerprint };
      },
      activity: async binding => {
        if(binding.runtime === "pi") {
          const proof=await deps.piProof(binding.sessionName,binding.generation),q=proof?.quiescence;
          return proof?.state === "present" && proof.generation===binding.generation && q ? {
            value:q.settled===true?"idle":q.settled===false?"busy":"unknown",observedAt:Date.parse(q.observedAt??"")} : null;
        }
        await deps.activity.pollSeat(binding.sessionName);
        const witness = deps.activity.getRotationActivityWitness(binding.nodeId);
        return witness ? { value: witness.activity === "idle-at-prompt" ? "idle" : witness.activity === "working" ? "busy" : "unknown", observedAt: Date.parse(witness.observedAt) } : null;
      },
      transcriptPath: async binding => this.history(binding.nodeId, binding.sessionName, binding.nativeId),
      piState: binding => deps.piState(binding.sessionName),
      piContract: async binding => (await resolvePiRotationNativeState(deps,binding.sessionName)).runtimeContract,
      piBaseline: binding => (deps.db.prepare("SELECT cursor FROM context_refresh_baselines WHERE node_id=? AND generation=? AND native_id=? AND source='pi_compaction_jsonl'")
        .get(binding.nodeId,binding.generation,binding.nativeId) as {cursor:string}|undefined)?.cursor ?? null,
    });
    this.ledger = new ContextRefreshService({ db: deps.db,
      assertCurrentOperator: actor => this.operator(actor), assertCurrentActor: actor => this.actor(actor),
      assertExecutor: grant => this.assertExecutor(grant), currentTarget: node => this.currentTarget(node),
      observe: target => this.observeTarget(target), checkpoint: target => this.checkpoint(target)?.checkpoint ?? null,
      checkpointReceipt: request => this.checkpointReceipt(request),
      reservationEvidence: attempt => this.cachedEvidence(attempt),
      assertCutoverReady: (grant, attempt, action) => {
        this.assertExecutor(grant);
        const proof = this.cutovers.get(attempt.attemptId);
        if (!deps.guard.ownsLifecycle(attempt.target.nodeId) || !proof || proof.action !== action || !fresh(proof.at)
          || proof.digest !== this.cutoverDigest(attempt)) refuse("refresh_cutover_changed", "Fresh exact owned lifecycle precondition required");
      },
    });
  }
  private async nativeState(seat:string) {
    const target=this.deps.guard.target(seat),row=this.currentTarget(target.nodeId);
    if(!row) return refuse("refresh_native_missing","Current exact native binding required");
    return row.runtime==="pi" ? resolvePiRotationNativeState(this.deps,seat) : resolveCodexNativeState(this.deps,seat);
  }
  private publicIdentity(binding: ContextRefreshTarget) { return { OPENRIG_NODE_ID: binding.nodeId, OPENRIG_SESSION_NAME: binding.sessionName, OPENRIG_OCCUPANT_GENERATION: binding.generation, OPENRIG_RUNTIME: binding.runtime }; }
  private targetOnly(binding: ContextRefreshBinding): ContextRefreshTarget { const { nodeId, sessionName, generation, runtime, nativeId, configurationDigest } = binding; return { nodeId, sessionName, generation, runtime, nativeId, configurationDigest }; }
  private actor(actor: ContextRefreshActor): void {
    const target = this.deps.guard.maybeTarget(actor.session);
    const row = target && this.deps.db.prepare("SELECT status,startup_status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(target.nodeId) as {status:string;startup_status:string}|undefined;
    if (!target || target.session !== actor.session || target.occupant !== actor.generation || row?.status !== "running" || row.startup_status !== "ready") refuse("refresh_actor_stale", "Actual current running ready actor required");
  }
  private operator(actor: ContextRefreshActor): void { this.actor(actor); this.deps.queue.coordinatorAuthority.assertCurrentOperator(actor.session, actor.generation); }
  private retained(grantId: string, actor?: ContextRefreshActor): ContextRefreshGrant {
    const row = this.deps.db.prepare("SELECT grant_json FROM context_refresh_grants WHERE grant_id=?").get(grantId) as {grant_json:string}|undefined;
    if (!row) return refuse("refresh_grant_missing", "Explicit finite refresh grant required");
    const grant = JSON.parse(row.grant_json) as ContextRefreshGrant;
    if (actor && (actor.session !== grant.executor.session || actor.generation !== grant.executor.generation)) refuse("refresh_actor_mismatch", "Exact retained executor required");
    return grant;
  }
  private currentTarget(nodeId: string): ContextRefreshTarget | null {
    const target = this.deps.guard.maybeTarget(nodeId);
    const row = this.deps.db.prepare("SELECT n.runtime,s.resume_type,s.resume_token FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1").get(nodeId) as {runtime:string;resume_type:string;resume_token:string|null}|undefined;
    const configurationDigest = target && this.deps.configurationDigest(target.session);
    return target?.occupant && row?.resume_token && (row.runtime === "pi" && row.resume_type === "pi_session_file" && canonicalPiSessionFile(row.resume_token) || row.runtime === "codex" && row.resume_type === "codex_id") && configurationDigest
      ? {nodeId,sessionName:target.session,generation:target.occupant,runtime:row.runtime,nativeId:row.resume_token,configurationDigest} : null;
  }
  private history(nodeId: string, session: string, nativeId: string): string | null {
    const who = this.deps.whoami.resolve({ nodeId, compact: false });
    const usage = who?.contextUsage as { sessionId?:string; transcriptPath?:string } | undefined;
    return who?.identity.nodeId === nodeId && who.identity.sessionName === session && usage?.sessionId === nativeId && usage.transcriptPath ? usage.transcriptPath : null;
  }
  private async binding(nodeId: string): Promise<ContextRefreshBinding | null> {
    const target = this.currentTarget(nodeId); if (!target) return null;
    if (target.runtime === "pi") {
      const state = await this.deps.piState(target.sessionName) as {model?:unknown} | null, model = parseNativeModelWindow(state?.model);
      return model && same(target,this.currentTarget(nodeId))
        ? {...target,model:`${model.provider}/${model.id}`,contextWindow:model.contextWindow} : null;
    }
    const file = this.history(target.nodeId, target.sessionName, target.nativeId); if (!file) return null;
    const model = this.modelReader.read(file, target.nativeId, JSON.stringify(target), () => same(target,this.currentTarget(nodeId)));
    return model && same(target, this.currentTarget(nodeId)) ? {...target,...model} : null;
  }
  private async observeTarget(target: ContextRefreshTarget): Promise<ContextRefreshObservation> {
    const observation = await this.observer.observe(target.nodeId);
    try { assertCoordinatorRotationSafe(this.deps.db, target.sessionName); } catch { observation.holds.push("authority-transfer-required"); }
    const protection = this.deps.guard.protectionFacts(target.nodeId);
    if (protection) observation.holds.push(protection.code === "seat_dispatch_reserved" ? "lifecycle-reserved" : "typing-guard-enabled");
    if (this.deps.guard.lifecycleActive(target.nodeId) && !this.deps.guard.ownsLifecycle(target.nodeId)) observation.holds.push("lifecycle-reserved");
    if (this.unresolvedEffects(target.sessionName)) observation.holds.push("effects-unresolved");
    observation.holds = [...new Set(observation.holds)]; return observation;
  }
  private unresolvedEffects(session: string): boolean {
    return this.deps.queue.coordinatorAuthority.coordinationRecovery?.hasUnresolvedWorkerEffects(session) ?? true;
  }
  private async executorProof(grant: ContextRefreshGrant, requested?: {launchId:string;supervisorPid:number}): Promise<NativeDutyProof | null> {
    const binding = this.currentTarget(grant.executor.nodeId), latest = this.deps.store.latest(grant.executor.nodeId, grant.executor.generation);
    if (!binding || !latest || latest.intent.launchId !== grant.executor.launchId || binding.sessionName !== grant.executor.session
      || binding.generation !== grant.executor.generation || binding.configurationDigest !== grant.executor.configurationDigest) return null;
    const currentBinding = async (nodeId: string) => {
      const now = this.currentTarget(nodeId), target = this.deps.guard.maybeTarget(nodeId);
      if (!now || !target?.pane || this.deps.guard.protectionFacts(nodeId)
        || this.deps.guard.lifecycleActive(nodeId) && !this.deps.guard.ownsLifecycle(nodeId)) return null;
      return {...now,pane:target.pane,resumeToken:now.nativeId,lifecycleReserved:false};
    };
    let supervisorPid = requested?.supervisorPid;
    if (requested && requested.launchId !== latest.intent.launchId) return null;
    if (supervisorPid === undefined) {
      const target = this.deps.guard.target(binding.nodeId), panePid = target.pane && await this.deps.tmux.getPanePid(target.pane);
      if (!panePid) return null;
      const rows = await listNativeProcesses(), children = new Set([panePid]);
      for (let pass = 0; pass < rows.length; pass++) { let changed = false; for (const row of rows) if (children.has(row.ppid) && !children.has(row.pid)) {children.add(row.pid);changed=true;} if (!changed) break; }
      const matches:number[] = [];
      for (const row of rows) if (children.has(row.pid) && row.executableName === "node"
        && await verifyNativeDutyProcessIdentity(row.pid,this.publicIdentity(binding),[latest.intent.installedNode.path,latest.intent.installedSupervisor.path,"--supervise",latest.intent.configPath])) matches.push(row.pid);
      if (matches.length !== 1) return null; supervisorPid = matches[0]!;
    }
    const proof = await observeNativeDutyLaunch(this.deps.store,{scope:latest.intent,launchId:latest.intent.launchId,supervisorPid},
      {tmux:this.deps.tmux,currentBinding,piProve:(session)=>this.deps.piProof(session,grant.executor.generation)});
    if (!proof || !fresh(proof.observedAt) || !proof.nativePresent || !proof.supervisorIsNativeAncestor || proof.lifecycleReserved
      || proof.nodeId !== binding.nodeId || proof.sessionName !== binding.sessionName || proof.generation !== binding.generation
      || proof.runtime !== binding.runtime || proof.configurationDigest !== binding.configurationDigest || proof.launchId !== grant.executor.launchId) return null;
    return proof;
  }
  private async refreshExecutor(grant: ContextRefreshGrant): Promise<void> {
    this.executorProofs.delete(grant.grantId); const proof = await this.executorProof(grant);
    if (!proof) refuse("refresh_executor_unavailable", "Current genuine Operator supervisor ancestry proof unavailable");
    if(this.executorProofs.size>=64)this.executorProofs.delete(this.executorProofs.keys().next().value!);
    this.executorProofs.set(grant.grantId, proof!);
  }
  private assertExecutor(grant: ContextRefreshGrant): void {
    this.operator(grant.executor); const proof = this.executorProofs.get(grant.grantId), target = this.currentTarget(grant.executor.nodeId);
    if (!this.deps.guard.ownsLifecycle(grant.executor.nodeId) || !proof || !fresh(proof.observedAt) || !target
      || target.generation !== grant.executor.generation || target.sessionName !== grant.executor.session || target.configurationDigest !== grant.executor.configurationDigest
      || this.deps.guard.protectionFacts(grant.executor.nodeId) || this.deps.store.latest(target.nodeId,target.generation)?.intent.launchId !== proof.launchId) refuse("refresh_executor_unavailable", "Fresh exact Operator proof under current lifecycle lease required");
  }
  async grant(actor: ContextRefreshActor, grant: ContextRefreshGrant): Promise<ContextRefreshGrant> {
    this.operator(actor);
    return this.deps.guard.lifecycle([grant.executor.nodeId],async()=>{await this.refreshExecutor(grant);return this.deps.db.transaction(()=>{
      for(const target of grant.targets)this.assertNoInvocationDebt(target.nodeId);
      return this.ledger.grant(actor,grant);
    }).immediate();});
  }
  async revoke(actor: ContextRefreshActor, input: {grantId:string}): Promise<void> { this.ledger.revoke(actor,input.grantId); }
  async enrollment(actor: ContextRefreshActor, input: {launchId:string;supervisorPid:number}): Promise<ContextRefreshEnrollment> {
    this.operator(actor);
    const rows=this.deps.db.prepare("SELECT grant_json FROM context_refresh_grants WHERE revoked_at IS NULL").all() as {grant_json:string}[];
    const grants:ContextRefreshGrant[]=[];
    for(const row of rows){const grant=JSON.parse(row.grant_json) as ContextRefreshGrant;
      if(grant.validUntil>Date.now()&&grant.executor.session===actor.session&&grant.executor.generation===actor.generation&&grant.executor.launchId===input.launchId
        &&await this.executorProof(grant,input))grants.push(grant);}
    return grants.length?{state:"ready",grants,pollMs:Math.min(...grants.map(g=>g.policy.pollMs))}:{state:"held",reason:"No live context-refresh grant with exact current Operator native ancestry"};
  }
  async evaluate(actor: ContextRefreshActor, input: ContextRefreshSelection): Promise<ContextRefreshDecision> {
    const grant=this.retained(input.grantId,actor);
    // Existing effect debt is observable even after expiry or native failure.
    const status=await this.status(actor,input);
    if(status.attempt&&!['refreshed','cancelled-before-effect'].includes(status.attempt.phase)||status.checkpointRequest&&!['receipt-confirmed','cancelled-before-effect'].includes(status.checkpointRequest.phase))return this.ledger.evaluate(actor,input.grantId,input.nodeId);
    return this.deps.guard.lifecycle([grant.executor.nodeId],async()=>{await this.refreshExecutor(grant);return this.grantScope.run(input.grantId,()=>this.ledger.evaluate(actor,input.grantId,input.nodeId));});
  }
  async observe(actor: ContextRefreshActor,input:ContextRefreshSelection):Promise<ContextRefreshObservation>{const grant=this.retained(input.grantId,actor);this.operator(actor);const target=grant.targets.find(t=>t.nodeId===input.nodeId);if(!target)return refuse("refresh_target_out_of_scope","Exact target required");return this.observeTarget(target);}
  private checkpointPath(checkpointId: string): string {
    if (!id(checkpointId)) return refuse("refresh_checkpoint_invalid", "Invalid retained checkpoint identity");
    return resolve(this.deps.rotationRoot,"frozen",`${checkpointId}.json`);
  }
  private privateDirectory(directory: string): void {
    const stat=lstatSync(directory);
    if(!stat.isDirectory()||stat.isSymbolicLink()||realpathSync(directory)!==resolve(directory)||stat.uid!==process.getuid?.()||(stat.mode&0o077)!==0)refuse("refresh_checkpoint_path_unsafe","Private owned canonical checkpoint directory required");
  }
  private readCheckpoint(checkpointId:string,checkpointHash:string):CheckpointFile {
    this.privateDirectory(resolve(this.deps.rotationRoot));this.privateDirectory(resolve(this.deps.rotationRoot,"frozen"));
    const file=this.checkpointPath(checkpointId),stat=lstatSync(file);
    if(!stat.isFile()||stat.isSymbolicLink()||stat.uid!==process.getuid?.()||(stat.mode&0o077)!==0||stat.size>2*1024*1024)refuse("refresh_checkpoint_invalid","Private immutable bounded checkpoint required");
    const bytes=readFileSync(file);if(hash(bytes)!==checkpointHash)refuse("refresh_checkpoint_changed","Retained checkpoint bytes changed");
    return JSON.parse(bytes.toString()) as CheckpointFile;
  }
  private checkpoint(target:ContextRefreshTarget):CheckpointFile|null {
    const grantId=this.grantScope.getStore();if(!grantId)return null;
    const row=this.deps.db.prepare("SELECT operation_id,evidence_digest FROM context_refresh_events WHERE grant_id=? AND node_id=? AND phase='checkpoint-authored' ORDER BY id DESC LIMIT 1").get(grantId,target.nodeId) as {operation_id:string;evidence_digest:string}|undefined;
    if(!row)return null;
    try{const file=this.readCheckpoint(row.operation_id,row.evidence_digest);file.checkpoint.checkpointHash=row.evidence_digest;
      return same(file.checkpoint.target,target)&&file.checkpoint.queueDigest===contextRefreshDigest(rotationActiveQueueRows(this.deps.db,target.sessionName))?file:null;
    }catch{return null;}
  }
  async recordCheckpoint(actor:ContextRefreshActor,input:ContextRefreshSelection&{packet:ContextRefreshCheckpointPacket}):Promise<ContextRefreshDraftReceipt>{return this.submitCheckpoint(actor,input);}
  async submitCheckpoint(actor:ContextRefreshActor,input:ContextRefreshSelection&{packet:ContextRefreshCheckpointPacket}):Promise<ContextRefreshDraftReceipt>{
    this.actor(actor);const grant=this.retained(input.grantId),target=grant.targets.find(t=>t.nodeId===input.nodeId);
    if(!target||actor.session!==target.sessionName||actor.generation!==target.generation)refuse("refresh_checkpoint_author","Only the genuine current target can submit its draft");
    const keys=["current_work","decisions","memory","constraints","standing_duties","evidence","next_action","outstanding_effects"];
    if(!input.packet||!same(Object.keys(input.packet).sort(),keys.sort())||!Array.isArray(input.packet.outstanding_effects)||input.packet.outstanding_effects.length||Buffer.byteLength(JSON.stringify(input.packet))>1024*1024)refuse("refresh_checkpoint_packet_invalid","Exact bounded eight-field packet and reconciled empty effects required");
    return this.deps.guard.lifecycle([target!.nodeId,grant.executor.nodeId],async()=>{
      await this.refreshExecutor(grant);this.assertExecutor(grant);this.actor(actor);this.assertNoInvocationDebt(target!.nodeId);
      const row=this.deps.db.prepare("SELECT revoked_at FROM context_refresh_grants WHERE grant_id=?").get(grant.grantId) as {revoked_at:number|null};
      if(row.revoked_at!==null||grant.validUntil<=Date.now()||!same(this.currentTarget(target!.nodeId),target))refuse("refresh_scope_ended","Exact live grant and target required");
      const prior=this.draft(input.grantId,input.nodeId);
      if(prior){if(!same(prior.packet,input.packet)||!same(prior.authoredBy,actor))refuse("refresh_checkpoint_draft_conflict","This finite grant already retains a different immutable draft");return {draftId:prior.draftId,grantId:prior.grantId,nodeId:prior.target.nodeId,phase:"submitted"};}
      const observed=await this.observeTarget(target!);
      // A native tool call is working by definition. Submission proves authorship,
      // not idleness or cutover readiness; those are checked later by the executor.
      if(!observed.native.verified||!fresh(observed.native.observedAt,5000)||!same(observed.identity,target))refuse("refresh_checkpoint_author","Exact current native author proof required");
      const draft:RetainedCheckpointDraft={draftId:`draft-${randomUUID()}`,grantId:grant.grantId,target:target!,authoredBy:actor,packet:input.packet,submittedAt:Date.now()};
      this.writePrivateEvidence(draft.draftId,draft,(digest)=>this.deps.db.transaction(()=>{
        this.actor(actor);this.assertExecutor(grant);this.assertNoInvocationDebt(target!.nodeId);
        const current=this.deps.db.prepare("SELECT revoked_at FROM context_refresh_grants WHERE grant_id=?").get(grant.grantId) as {revoked_at:number|null};
        if(current.revoked_at!==null||grant.validUntil<=Date.now()||!same(this.currentTarget(target!.nodeId),target))refuse("refresh_scope_ended","Draft grant or target changed; private evidence retained");
        if(this.draft(input.grantId,input.nodeId))refuse("refresh_checkpoint_draft_conflict","Another immutable submission already exists; read its retained receipt");
        this.event(grant.grantId,target!.nodeId,draft.draftId,"checkpoint-draft-submitted",digest);
      }).immediate());
      return {draftId:draft.draftId,grantId:grant.grantId,nodeId:target!.nodeId,phase:"submitted"};
    });
  }
  private writePrivateEvidence(evidenceId:string,value:unknown,record:(digest:string)=>void):void {
    mkdirSync(resolve(this.deps.rotationRoot),{mode:0o700,recursive:true});this.privateDirectory(resolve(this.deps.rotationRoot));
    mkdirSync(resolve(this.deps.rotationRoot,"frozen"),{mode:0o700,recursive:true});this.privateDirectory(resolve(this.deps.rotationRoot,"frozen"));
    const bytes=Buffer.from(canonical(value)+"\n"),fd=openSync(this.checkpointPath(evidenceId),constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);
    try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}
    const dir=openSync(resolve(this.deps.rotationRoot,"frozen"),constants.O_RDONLY);try{fsyncSync(dir);}finally{closeSync(dir);}
    record(hash(bytes));
  }
  private draft(grantId:string,nodeId:string):RetainedCheckpointDraft|null {
    const row=this.deps.db.prepare("SELECT operation_id,evidence_digest FROM context_refresh_events WHERE grant_id=? AND node_id=? AND phase='checkpoint-draft-submitted' ORDER BY id DESC LIMIT 1").get(grantId,nodeId) as {operation_id:string;evidence_digest:string}|undefined;
    if(!row)return null;
    const draft=this.readCheckpoint(row.operation_id,row.evidence_digest) as unknown as RetainedCheckpointDraft;
    const target=this.retained(grantId).targets.find(t=>t.nodeId===nodeId);
    if(draft.draftId!==row.operation_id||draft.grantId!==grantId||!same(draft.target,target)||draft.authoredBy.session!==target?.sessionName||draft.authoredBy.generation!==target.generation)refuse("refresh_checkpoint_changed","Immutable draft attribution mismatch");
    return draft;
  }
  private async promoteCheckpoint(draft:RetainedCheckpointDraft):Promise<ContextRefreshCheckpoint>{
    const actor=draft.authoredBy,input={grantId:draft.grantId,nodeId:draft.target.nodeId,packet:draft.packet};
    if(!same(this.draft(input.grantId,input.nodeId),draft))refuse("refresh_checkpoint_changed","Current exact retained draft required");
    this.actor(actor);const grant=this.retained(input.grantId),target=grant.targets.find(t=>t.nodeId===input.nodeId);
    if(!target||actor.session!==target.sessionName||actor.generation!==target.generation)refuse("refresh_checkpoint_author","Only the genuine current target can author its checkpoint");
    const keys=["current_work","decisions","memory","constraints","standing_duties","evidence","next_action","outstanding_effects"];
    if(!input.packet||!same(Object.keys(input.packet).sort(),keys.sort())||!Array.isArray(input.packet.outstanding_effects)||input.packet.outstanding_effects.length||Buffer.byteLength(JSON.stringify(input.packet))>1024*1024)refuse("refresh_checkpoint_packet_invalid","Exact bounded eight-field packet and reconciled empty effects required");
    return this.deps.guard.lifecycle([target!.nodeId,grant.executor.nodeId],async()=>{
      await this.refreshExecutor(grant);this.assertExecutor(grant);this.actor(actor);
      const retained=this.deps.db.prepare("SELECT revoked_at FROM context_refresh_grants WHERE grant_id=?").get(grant.grantId) as {revoked_at:number|null};
      if(retained.revoked_at!==null||grant.validUntil<=Date.now()||!same(this.currentTarget(target!.nodeId),target))refuse("refresh_scope_ended","Exact live grant and target required");
      const observation=await this.observeTarget(target!);
      if(!observation.native.verified||observation.activity.value!=="idle"||!fresh(observation.activity.observedAt,5000)||observation.holds.some(h=>h!=="usage-unavailable"&&h!=="usage-stale"))refuse("refresh_checkpoint_not_ready","Native idle and all custody prerequisites required");
      const request=this.deps.db.prepare("SELECT request_json FROM context_refresh_checkpoint_requests WHERE grant_id=? AND node_id=? AND phase='receipt-confirmed'").get(grant.grantId,target!.nodeId) as {request_json:string}|undefined;
      if(request){const q=this.deps.queue.getById((JSON.parse(request.request_json) as ContextRefreshCheckpointRequest).qitemId);if(q&&!['done','cancelled','canceled','failed','denied','handed-off'].includes(q.state))refuse("refresh_checkpoint_task_active","Dispose the exact checkpoint request before freezing queue custody");}
      const state=await this.nativeState(target!.sessionName),queue=rotationActiveQueueRows(this.deps.db,target!.sessionName);
      if(state.usage.sessionId!==target!.nativeId||!same(this.currentTarget(target!.nodeId),target))refuse("refresh_binding_changed","Native checkpoint binding changed");
      const checkpointId=`refresh-${randomUUID()}`;
      const checkpoint:ContextRefreshCheckpoint={target:target!,checkpointId,checkpointHash:"",queueDigest:contextRefreshDigest(queue),authoredBy:actor,outstandingEffects:0};
      const receipt={generation:target!.nativeId,at:Date.now(),queue_hash:checkpoint.queueDigest,quiescent:true,unattended_eligible:true,packet:input.packet,snapshot:{who:{identity:{nodeId:target!.nodeId,sessionName:target!.sessionName}}},runtime_contract:state.runtimeContract};
      const expected={protocol:"generation-queue-runtime-idle-v1",generation:target!.nativeId,queue,runtimeContract:state.runtimeContract,checkpointPath:`frozen/${checkpointId}.json`};
      mkdirSync(resolve(this.deps.rotationRoot),{mode:0o700,recursive:true});this.privateDirectory(resolve(this.deps.rotationRoot));mkdirSync(resolve(this.deps.rotationRoot,"frozen"),{mode:0o700,recursive:true});this.privateDirectory(resolve(this.deps.rotationRoot,"frozen"));
      const bytes=Buffer.from(canonical({checkpoint,receipt,expected})+"\n"),file=this.checkpointPath(checkpointId);
      const fd=openSync(file,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|constants.O_NOFOLLOW,0o600);
      try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}
      const dir=openSync(resolve(this.deps.rotationRoot,"frozen"),constants.O_RDONLY);try{fsyncSync(dir);}finally{closeSync(dir);}
      checkpoint.checkpointHash=hash(bytes);
      this.actor(actor);if(!same(this.currentTarget(target!.nodeId),target)||checkpoint.queueDigest!==contextRefreshDigest(rotationActiveQueueRows(this.deps.db,target!.sessionName)))refuse("refresh_checkpoint_changed","Binding or custody changed while freezing; private evidence retained");
      this.deps.db.transaction(()=>{
        this.actor(actor);this.assertExecutor(grant);
        const current=this.deps.db.prepare("SELECT revoked_at FROM context_refresh_grants WHERE grant_id=?").get(grant.grantId) as {revoked_at:number|null};
        if(current.revoked_at!==null||grant.validUntil<=Date.now()||!same(this.draft(input.grantId,input.nodeId),draft)||!same(this.currentTarget(target!.nodeId),target)||checkpoint.queueDigest!==contextRefreshDigest(rotationActiveQueueRows(this.deps.db,target!.sessionName)))refuse("refresh_checkpoint_changed","Exact draft, grant or custody changed; private evidence retained");
        this.event(grant.grantId,target!.nodeId,checkpointId,"checkpoint-authored",checkpoint.checkpointHash);
        this.event(grant.grantId,target!.nodeId,draft.draftId,"checkpoint-draft-frozen",checkpoint.checkpointHash);
      }).immediate();
      return checkpoint;
    });
  }
  private checkpointReceipt(request:ContextRefreshCheckpointRequest){
    const q=this.deps.db.prepare("SELECT * FROM queue_items WHERE qitem_id=?").get(request.qitemId) as Record<string,unknown>|undefined;
    const created=this.deps.db.prepare("SELECT * FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1").get(request.qitemId) as Record<string,unknown>|undefined;
    const grant=this.retained(request.grantId);
    return q&&created&&q.source_session===request.actor.session&&q.destination_session===request.target.sessionName&&q.body===request.body
      &&q.minting_generation_uuid===request.actor.generation&&q.expires_at===new Date(grant.validUntil).toISOString()
      &&created.actor_session===request.actor.session&&created.identity_provenance==="transport:v1"
      ?{operationId:request.operationId,qitemId:request.qitemId,requestDigest:contextRefreshDigest(request),receiptDigest:contextRefreshDigest({id:q.qitem_id,source:q.source_session,destination:q.destination_session,body:q.body,expires:q.expires_at,generation:q.minting_generation_uuid,created})}:null;
  }
  private assertNoInvocationDebt(nodeId:string):void {
    if(this.deps.db.prepare("SELECT 1 FROM context_refresh_events i WHERE i.node_id=? AND i.phase='step-invocation' AND NOT EXISTS(SELECT 1 FROM context_refresh_events c WHERE c.operation_id=i.operation_id AND c.phase='step-completed') LIMIT 1").get(nodeId))refuse("refresh_invocation_unresolved","Retained unfinished invocation excludes new effects and grants; reconcile exact original ID");
  }
  private event(grantId:string,nodeId:string,operationId:string,phase:string,evidenceDigest:string):void{
    this.deps.db.prepare("INSERT INTO context_refresh_events(grant_id,node_id,operation_id,phase,evidence_digest,observed_at) VALUES(?,?,?,?,?,?)").run(grantId,nodeId,operationId,phase,evidenceDigest,Date.now());
  }
  private expected(attempt:ContextRefreshAttempt):Record<string,unknown>{
    const file=this.readCheckpoint(attempt.checkpoint.checkpointId,attempt.checkpoint.checkpointHash);
    if(!same({...file.checkpoint,checkpointHash:attempt.checkpoint.checkpointHash},attempt.checkpoint))refuse("refresh_checkpoint_changed","Exact attempt checkpoint required");
    return {...file.expected,checkpointHash:attempt.checkpoint.checkpointHash,reservationId:attempt.reservationId,operationId:attempt.operationId};
  }
  private async rotationPrecondition(seat:string,expected:Record<string,unknown>):Promise<void>{
    const row=this.deps.db.prepare("SELECT attempt_json FROM context_refresh_attempts WHERE operation_id=? AND reservation_id=?").get(expected.operationId,expected.reservationId) as {attempt_json:string}|undefined;
    if(!row)refuse("refresh_attempt_missing","Exact refresh-owned reservation required");
    const attempt=JSON.parse(row!.attempt_json) as ContextRefreshAttempt,grant=this.retained(attempt.grantId);
    this.assertExecutor(grant);
    const retained=this.deps.db.prepare("SELECT revoked_at FROM context_refresh_grants WHERE grant_id=?").get(grant.grantId) as {revoked_at:number|null};
    if(retained.revoked_at!==null||grant.validUntil<=Date.now()||seat!==attempt.target.sessionName||!same(expected,this.expected(attempt))||!same(this.currentTarget(attempt.target.nodeId),attempt.target)||!this.deps.guard.ownsLifecycle(attempt.target.nodeId))refuse("refresh_binding_changed","Exact live finite refresh target and lifecycle required");
    const file=this.readCheckpoint(attempt.checkpoint.checkpointId,attempt.checkpoint.checkpointHash);
    assertManagedUnattended({automatic_cutover_enabled:true,managed_unattended_seats:grant.targets.map(t=>t.sessionName)},seat,file.receipt);
    assertCoordinatorRotationSafe(this.deps.db,seat);if(this.unresolvedEffects(seat))refuse("refresh_effects_unresolved","Unresolved native/transport effects retained");
    await this.deps.activity.pollSeat(seat);
    const state=await this.nativeState(seat),witness=attempt.target.runtime==="pi" && "proof" in state
      ? {activity:state.proof.quiescence?.settled===true?"idle-at-prompt":"unknown",observedAt:state.proof.quiescence?.observedAt??""}
      : this.deps.activity.getRotationActivityWitness(attempt.target.nodeId);
    assertRotationPrecondition(JSON.parse(canonical(expected)),{generation:state.usage.sessionId??null,queue:JSON.parse(canonical(rotationActiveQueueRows(this.deps.db,seat))),runtimeContract:JSON.parse(canonical(state.runtimeContract)),
      activity:witness?.activity??null,observedAt:witness?Date.parse(witness.observedAt):NaN,checkpointHash:attempt.checkpoint.checkpointHash});
    this.assertExecutor(grant);
  }
  private cutoverDigest(attempt:ContextRefreshAttempt):string{return contextRefreshDigest({target:this.currentTarget(attempt.target.nodeId),
    custody:this.reservations.snapshot(attempt.target.nodeId,attempt.target.sessionName),reservation:this.deps.db.prepare("SELECT * FROM seat_dispatch_reservations WHERE reservation_id=?").get(attempt.reservationId)??null,
    checkpoint:hash(readFileSync(this.checkpointPath(attempt.checkpoint.checkpointId)))});}
  private cachedEvidence(attempt:ContextRefreshAttempt):ContextRefreshReservationEvidence|null{
    const proof=this.receiptProofs.get(attempt.attemptId);if(!proof||!fresh(proof.at)||proof.digest!==this.cutoverDigest(attempt))return null;return proof.evidence;
  }
  private async refreshEvidence(attempt:ContextRefreshAttempt,actor:ContextRefreshActor):Promise<void>{
    this.receiptProofs.delete(attempt.attemptId);
    let r:DispatchReservation;try{r=this.reservations.get(attempt.reservationId);}catch{return;}
    const inspect=async()=>this.deps.guard.lifecycle([r.node_id,this.deps.guard.target(actor.session).nodeId],async()=>{
      const proof=await this.reservations.inspectEvidence(r.reservation_id);r=proof.reservation;
      const rows=this.deps.db.prepare("SELECT * FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all(r.reservation_id) as Array<{id:number;action:string;actor_session:string;actor_generation:string;evidence_json:string}>;
      const acknowledgment=(kind:string)=>{const row=rows.find(row=>{const e=JSON.parse(row.evidence_json);return row.action===kind&&e.operationId===r.operation_id&&e.checkpointHash===attempt.checkpoint.checkpointHash&&e.successorGeneration===r.successor_generation&&e.nativeId===r.successor_native_id;});return row?{session:row.actor_session,generation:row.actor_generation}:null;};
      const released=rows.find(row=>row.action==="released"),release=released?JSON.parse(released.evidence_json):null,current=this.currentTarget(r.node_id);
      const evidence:ContextRefreshReservationEvidence={reservationId:r.reservation_id,operationId:r.operation_id,targetNodeId:r.node_id,predecessorGeneration:r.predecessor_generation,predecessorNativeId:r.predecessor_native_id,
        checkpointHash:String(JSON.parse(r.expected_json).checkpointHash),state:r.state,successor:r.successor_generation&&r.successor_native_id&&current?{generation:r.successor_generation,nativeId:r.successor_native_id,configurationDigest:current.configurationDigest}:null,
        successorVerified:proof.successorVerified,custodyVerified:proof.custodyVerified,successorAck:acknowledgment("successor_ack"),independentAcceptance:acknowledgment("independent_acceptance"),
        releasedBy:released?{session:released.actor_session,generation:released.actor_generation}:null,releaseMode:release?.mode==="accepted_successor"||release?.mode==="cancel_before_replacement"?release.mode:null,receiptDigest:contextRefreshDigest({reservation:r,audit:rows})};
      if(this.receiptProofs.size>=64)this.receiptProofs.delete(this.receiptProofs.keys().next().value!);
      this.receiptProofs.set(attempt.attemptId,{at:Date.now(),evidence,digest:this.cutoverDigest(attempt)});
    },r.state==="released"?undefined:r.reservation_id);
    if(r.state==="started")await this.reservations.withAttemptLock(r.reservation_id,actor.session,actor.generation,inspect);else await inspect();
  }
  async status(actor:ContextRefreshActor,input:ContextRefreshSelection):Promise<ContextRefreshStatus>{
    const grant=this.retained(input.grantId),target=grant.targets.find(t=>t.nodeId===input.nodeId);if(!target)refuse("refresh_target_out_of_scope","Exact target required");
    if(actor.session!==grant.executor.session||actor.generation!==grant.executor.generation){this.actor(actor);if(actor.session!==target!.sessionName)refuse("refresh_actor_mismatch","Only executor or current exact target can read this status");}
    const row=this.deps.db.prepare("SELECT attempt_id FROM context_refresh_attempts WHERE grant_id=? AND node_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1").get(input.grantId,input.nodeId) as {attempt_id:string}|undefined;
    const req=this.deps.db.prepare("SELECT operation_id,request_json,phase FROM context_refresh_checkpoint_requests WHERE grant_id=? AND node_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1").get(input.grantId,input.nodeId) as {operation_id:string;request_json:string;phase:string}|undefined;
    const invoked=input.operationId?this.deps.db.prepare("SELECT phase FROM context_refresh_events WHERE grant_id=? AND node_id=? AND operation_id=? AND phase IN ('step-invocation','step-completed') ORDER BY id DESC LIMIT 1").get(input.grantId,input.nodeId,input.operationId) as {phase:string}|undefined:undefined;
    const draft=this.draft(input.grantId,input.nodeId),frozen=draft&&this.deps.db.prepare("SELECT 1 FROM context_refresh_events WHERE grant_id=? AND node_id=? AND operation_id=? AND phase='checkpoint-draft-frozen'").get(input.grantId,input.nodeId,draft.draftId);
    return{...input,checkpointDraft:draft?{draftId:draft.draftId,phase:frozen?"frozen":"submitted"}:null,invocation:invoked?{operationId:input.operationId!,state:invoked.phase==="step-completed"?"completed":"in-flight"}:null,attempt:row?this.ledger.attempt(row.attempt_id):null,checkpointRequest:req?{operationId:req.operation_id,qitemId:(JSON.parse(req.request_json) as ContextRefreshCheckpointRequest).qitemId,phase:req.phase}:null};
  }
  async reconcile(actor:ContextRefreshActor,input:ContextRefreshSelection):Promise<ContextRefreshStatus>{
    const status=await this.status(actor,input);
    if(input.operationId){
      // A missing receipt does not authorize replay. Fence this exact request
      // before declaring no effect: any delayed step must observe the tombstone.
      // Existing invoked work is never cancelled by absence of process-local state.
      this.retained(input.grantId,actor);this.operator(actor);
      if(!id(input.operationId))refuse("refresh_operation_invalid","Bounded explicit step invocation ID required");
      this.deps.db.transaction(()=>{
        if(this.invocationExists(actor,{...input,operationId:input.operationId!}))return;
        const digest=contextRefreshDigest({actor,input});
        this.event(input.grantId,input.nodeId,input.operationId!,"step-invocation",digest);
        this.event(input.grantId,input.nodeId,input.operationId!,"step-cancelled-before-invocation",digest);
        this.event(input.grantId,input.nodeId,input.operationId!,"step-completed",digest);
      }).immediate();
    }
    if(status.checkpointRequest)this.ledger.reconcileCheckpoint(actor,status.checkpointRequest.operationId);
    if(status.attempt&&!['prepared','refreshed','cancelled-before-effect'].includes(status.attempt.phase)){
      try{await this.refreshEvidence(status.attempt,actor);}catch{this.receiptProofs.delete(status.attempt.attemptId);}
      this.ledger.reconcile(actor,status.attempt.attemptId);
    }
    return this.status(actor,input);
  }
  async attest(actor:ContextRefreshActor,input:{attemptId:string;kind:"successor_ack"|"independent_acceptance";evidenceRef:string}):Promise<ContextRefreshAttempt>{
    this.actor(actor);const attempt=this.ledger.attempt(input.attemptId),grant=this.retained(attempt.grantId);
    if(input.kind==="independent_acceptance"&&!same(actor,grant.validator))refuse("refresh_validator_mismatch","Exact independently named validator required");
    await this.reservations.attest(actor.session,actor.generation,attempt.reservationId,{operationId:attempt.operationId,checkpointHash:attempt.checkpoint.checkpointHash,kind:input.kind,evidenceRef:input.evidenceRef});
    await this.refreshEvidence(attempt,grant.executor);return this.ledger.reconcile(grant.executor,attempt.attemptId);
  }
  private invocationExists(actor:ContextRefreshActor,input:ContextRefreshSelection&{operationId:string}):boolean {
    const rows=this.deps.db.prepare("SELECT grant_id,node_id,phase,evidence_digest FROM context_refresh_events WHERE operation_id=?").all(input.operationId) as Array<{grant_id:string;node_id:string;phase:string;evidence_digest:string}>;
    if(!rows.length)return false;
    const prior=rows.find(row=>row.phase==="step-invocation");
    if(!prior||prior.grant_id!==input.grantId||prior.node_id!==input.nodeId||prior.evidence_digest!==contextRefreshDigest({actor,input}))
      refuse("refresh_operation_conflict","Step invocation ID is immutable");
    return true;
  }
  async step(actor:ContextRefreshActor,input:ContextRefreshSelection&{operationId:string}):Promise<ContextRefreshStepResult>{
    const invocation={owned:false};
    try { return await this.grantScope.run(input.grantId,()=>this.executeStep(actor,input,invocation)); }
    finally {
      const prior=this.deps.db.prepare("SELECT evidence_digest FROM context_refresh_events WHERE operation_id=? AND phase='step-invocation'").get(input.operationId) as {evidence_digest:string}|undefined;
      const done=this.deps.db.prepare("SELECT 1 FROM context_refresh_events WHERE operation_id=? AND phase='step-completed'").get(input.operationId);
      if(invocation.owned&&prior&&!done&&prior.evidence_digest===contextRefreshDigest({actor,input}))this.event(input.grantId,input.nodeId,input.operationId,"step-completed",contextRefreshDigest(await this.status(actor,input)));
    }
  }
  private async executeStep(actor:ContextRefreshActor,input:ContextRefreshSelection&{operationId:string},invocation:{owned:boolean}):Promise<ContextRefreshStepResult>{
    if(!id(input.operationId))refuse("refresh_operation_invalid","Bounded explicit step invocation ID required");
    const grant=this.retained(input.grantId,actor),initial=await this.status(actor,input),existing=initial.attempt;
    this.operator(actor);
    // Accept and durably bind the invocation before a guard can refuse it.
    // No effect occurs until the existing lifecycle/native/ledger gates pass.
    const repeated=this.deps.db.transaction(()=>{
      if(this.invocationExists(actor,input))return true;
      this.assertNoInvocationDebt(input.nodeId);
      this.event(input.grantId,input.nodeId,input.operationId,"step-invocation",contextRefreshDigest({actor,input}));return false;
    }).immediate();
    const result=async(effect:ContextRefreshStepResult["effect"],decision:ContextRefreshDecision|null,hold:string|null):Promise<ContextRefreshStepResult>=>({...await this.status(actor,input),operationId:input.operationId,effect,decision,hold});
    if(repeated)return result("none",null,"invocation-retained-reconcile-only");
    invocation.owned=true;
    const preparedIds={attemptId:`refresh-${randomUUID()}`,operationId:`rotation-${randomUUID()}`,reservationId:`reservation-${randomUUID()}`};
    const reservation=existing?this.deps.db.prepare("SELECT state FROM seat_dispatch_reservations WHERE reservation_id=?").get(existing.reservationId) as {state:string}|undefined:undefined;
    return this.deps.guard.lifecycle([grant.executor.nodeId,input.nodeId],async()=>{
      let status=await this.reconcile(actor,input);
      if(status.attempt?.phase==="uncertainty-held"||status.attempt?.phase==="replacement-started"||status.checkpointRequest?.phase==="uncertainty-held")return result("none",null,"effect-uncertain-reconcile-only");
      await this.refreshExecutor(grant);
      if(status.attempt&&!['refreshed','cancelled-before-effect'].includes(status.attempt.phase))return this.performAttempt(actor,input,status.attempt);
      let decision=await this.ledger.evaluate(actor,input.grantId,input.nodeId);
      const draft=this.draft(input.grantId,input.nodeId);
      if(draft&&decision.prepareThreshold&&!decision.holds.some(hold=>hold!=="checkpoint-required")&&!this.checkpoint(draft.target)){
        await this.promoteCheckpoint(draft);
        decision=await this.ledger.evaluate(actor,input.grantId,input.nodeId);
      }
      if(decision.action==="request-checkpoint"||status.checkpointRequest?.phase==="prepared"){
        const request=status.checkpointRequest?.phase==="prepared"?status.checkpointRequest:await this.ledger.prepareCheckpointRequest(actor,input.grantId,input.nodeId,`checkpoint-${randomUUID()}`);
        const permit=await this.ledger.beginCheckpointRequest(actor,request.operationId);
        if(!permit.maySendEffect)return result("none",decision,"checkpoint-already-begun");
        try{await this.deps.queue.createContextRefreshCheckpoint(request.operationId);}finally{this.ledger.reconcileCheckpoint(actor,request.operationId);}
        return result("checkpoint",decision,null);
      }
      if(decision.action!=="reserve")return result("none",decision,null);
      const attempt=await this.ledger.prepareAttempt(actor,input.grantId,input.nodeId,preparedIds);
      return this.performAttempt(actor,input,attempt);
    },existing&&reservation?.state!=="released"?existing.reservationId:preparedIds.reservationId);
  }
  private async performAttempt(actor:ContextRefreshActor,input:ContextRefreshSelection&{operationId:string},attempt:ContextRefreshAttempt):Promise<ContextRefreshStepResult>{
    const grant=this.retained(input.grantId,actor),action:ContextRefreshAction|null=attempt.phase==="prepared"?"reserve":attempt.phase==="reserved"?"handover":attempt.phase==="committed-awaiting-acceptance"?"release":null;
    const result=async(effect:ContextRefreshStepResult["effect"],hold:string|null):Promise<ContextRefreshStepResult>=>({...await this.status(actor,input),operationId:input.operationId,effect,decision:null,hold});
    if(!action)return result("none","attempt-reconciliation-required");
    const proofAt=Date.now();
    if(action==="release")await this.refreshEvidence(attempt,actor);else await this.rotationPrecondition(attempt.target.sessionName,this.expected(attempt));
    await this.refreshExecutor(grant);
    if(this.cutovers.size>=64)this.cutovers.delete(this.cutovers.keys().next().value!);
    this.cutovers.set(attempt.attemptId,{at:proofAt,action,digest:this.cutoverDigest(attempt)});
    const permit=this.ledger.beginEffect(actor,attempt.attemptId,action);
    if(!permit.maySendEffect)return result("none","attempt-already-begun");
    try{
      if(action==="reserve")await this.reservations.reserve(actor.session,actor.generation,{reservationId:attempt.reservationId,operationId:attempt.operationId,nodeId:attempt.target.nodeId,generation:attempt.target.generation,reason:"Finite automatic context refresh",
        expected:this.expected(attempt),profileSha256:JSON.parse(this.reservations.snapshot(attempt.target.nodeId,attempt.target.sessionName)).profileSha256});
      else if(action==="handover")await this.deps.handoverFactory({dispatchReservations:this.reservations,rotationPrecondition:(seat,expected)=>this.rotationPrecondition(seat,expected)}).handover({seatRef:attempt.target.sessionName,reason:"Finite automatic context refresh",source:"fresh",operator:actor.session,rotationActor:actor.session,rotationActorGeneration:actor.generation,rotationExpected:this.expected(attempt)});
      else await this.reservations.release(actor.session,actor.generation,attempt.reservationId,{operationId:attempt.operationId,reason:"Verified fresh successor and independent acceptance",mode:"accepted_successor"});
    }finally{
      this.cutovers.delete(attempt.attemptId);try{await this.refreshEvidence(attempt,actor);}catch{this.receiptProofs.delete(attempt.attemptId);}this.ledger.reconcile(actor,attempt.attemptId);
    }
    return result(action,null);
  }
}
export function createContextRefreshIntegration(deps:ContextRefreshIntegrationDeps):ContextRefreshFacade{return new ContextRefreshIntegration(deps);}
