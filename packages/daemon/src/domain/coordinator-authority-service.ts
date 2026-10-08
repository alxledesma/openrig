import type { RuntimeAvailability } from "./coordinator-runtime-availability.js";
import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import type { EventBus } from "./event-bus.js";
import { QueueTransitionLog, type NativeQueueCustodyReceipt, type QueueTransition } from "./queue-transition-log.js";

export interface CoordinatorToken { rigId: string; epoch: number; generation: string }
export interface DispatchEnvelope { token: CoordinatorToken; packageKey: string }
export interface PackageContract {
 inputDigest: string; destination: string; bodyHash: string;
 resources: string[]; returnContract: { destination: string; evidenceRequired: string[]; transitions?: Array<{source:string;destination:string;bodyHash:string}> };
 /** Prospective classification, set at Operator registration. Absent means a legacy
  * contract: its frozen bytes are never rewritten, and an unplanned non-backup legacy
  * package still receives its existing materialization duty but never supports a
  * completeness finding. */
 workClass?: "product"|"recovery"|"administrative"|"inquiry";
 /** Scope refs this package was admitted against, bound from a cited proposal at
  *  admission time. Absent means a legacy or operator-registered package that can
  *  never support a frontier completeness finding. */
 scopeCitations?: Array<{ref:string;digest:string}>;
}
export interface HeldHistoryRef {outboxId:string;rowHash:string;custodyHash:string;quarantineHash:string;operationHash:string}
/** Exact, recomputable evidence for one adopted row whose queue custody moved only by ts_updated. */
export interface CustodyAttestationEvidence {outboxId:string;adoptionPostCustodyHash:string;attestedCustodyHash:string;drift:Array<{path:'queue.ts_updated';frozen:string;current:string}>;cause:{queueId:string;transitionId:number;ts:string;state:string;actorSession:string;identityProvenance:'transport:v1'};history:{count:number;digest:string}}
export interface LegacyInventory { heldHistory?:HeldHistoryRef[]; rows: Array<{queueId:string;rowHash:string}>; uncertainEffects: string[]; snapshotDigest:string }
export interface LegacyEnrollment {
 rigId:string;batonId:string;owner:string;ownerGeneration:string;coordinators:string[];leaseMs:number;operationId:string;
 authorizationId:string;inventory:LegacyInventory; heldHistoryRecovery?:{queueId:string;rowHash:string};
 obligations:Array<{queueId:string;kind:"coordination"|"work";evidenceRef:string;packageKey?:string;contract?:PackageContract;resourceScope?:"exclusive"|"read-only"}>;
}
export const legacyProposalDigest=(input:LegacyEnrollment):string=>digest(canonical(input));
export interface Authority {
 rig_id: string; baton_id: string; owner_session: string; owner_generation: string;
 epoch: number; lease_until: number; state: "active" | "reconciling" | "recovery";
 operation_id: string; coordinators: string; recovery_queue_id: string | null;
}
export class CoordinatorFenceError extends Error {
 constructor(readonly code: string, message: string, readonly meta?: Record<string, unknown>) { super(message); }
}
export class AssignmentReplay extends Error { constructor(readonly queueId: string) { super("Existing admitted assignment"); } }
export const digest = (value: string): string => createHash("sha256").update(value).digest("hex");
const reject = (code: string, message: string): never => { throw new CoordinatorFenceError(code, message); };
function canonical(value: unknown): string {
 if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
 if (value !== null && typeof value === "object") return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical((value as Record<string,unknown>)[k])}`).join(",")}}`;
 return JSON.stringify(value);
}

/** Local correctness fence, not protection against hostile local environment/header forgery.
 * All writes use the queue connection. BEGIN IMMEDIATE orders authority transfer and assignment.
 * Enabling is explicit; absent rig registration retains legacy behavior. */
export class CoordinatorAuthorityService {
 constructor(readonly db: Database.Database, private bus?: EventBus, private transitions?: QueueTransitionLog,
   private now: () => number = Date.now) {}
 resilienceRollout?: import('./resilience-rollout-service.js').ResilienceRolloutService;
 runtimeOutcomeAssessment?: import("./runtime-outcome-assessment.js").RuntimeOutcomeAssessment;
 resumeAdministrativeDuties?:(rigId:string,jobId:string)=>Promise<void>;
 outboxAbandonAuthorizationWake?:(source:string|undefined,destination:string,authorizationId:string,proof?:{body:string;ids?:string[]})=>boolean;
 coordinationRecovery?: import("./coordination-recovery-service.js").CoordinationRecoveryService;
 private runtimeObserver?: (session:string)=>Promise<RuntimeAvailability|null>;
 private runtimeEvidence=new Map<string,RuntimeAvailability>();
 private availabilityRuns=new Map<string,number>();
 private operationAvailability=new AsyncLocalStorage<{
  rigId:string;members:string;generations:Map<string,string|null>;observations:Map<string,RuntimeAvailability>;
  observer:((session:string)=>Promise<RuntimeAvailability|null>)|undefined;active:boolean;
 }>();
 /** Server-owned probe-through-decision context. Concurrent operations never share
  * witnesses; an empty/expired context cannot borrow the observation cache. */
 async withFreshRuntimeAvailability<T>(rigId:string,decide:()=>T|Promise<T>):Promise<T> {
  const authority=this.get(rigId),observer=this.runtimeObserver;
  const members=authority?JSON.parse(authority.coordinators) as string[]:[];
  const generations=new Map(members.map(session=>[session,this.generation(session)]));
  const results=observer?await Promise.allSettled(members.map(session=>observer(session))):[];
  const observations=new Map<string,RuntimeAvailability>();
  for(let i=0;i<results.length;i++){
   const result=results[i]!;
   if(result.status==='fulfilled'&&result.value&&result.value.session===members[i]){
    const evidence=result.value;
    observations.set(evidence.session,Object.freeze({...evidence,
     ...(evidence.quiescence?{quiescence:Object.freeze({...evidence.quiescence})}:{})}));
   }
  }
  const batch={rigId,members:authority?.coordinators??'',generations,observations,observer,active:true};
  return this.operationAvailability.run(batch,async()=>{
   try{return await decide();}finally{batch.active=false;}
  });
 }
 private runtimeObservation(session:string):RuntimeAvailability|undefined {
  const batch=this.operationAvailability.getStore();
  if(!batch)return this.runtimeEvidence.get(session); // legacy observation-only/direct callers
  if(!batch.active||batch.observer!==this.runtimeObserver||this.get(batch.rigId)?.coordinators!==batch.members
    ||!batch.generations.has(session)||[...batch.generations].some(([member,generation])=>this.generation(member)!==generation))return undefined;
  return batch.observations.get(session);
 }

 setRuntimeObserver(observer:(session:string)=>Promise<RuntimeAvailability|null>):void {this.runtimeObserver=observer;this.runtimeEvidence.clear();}
 async refreshRuntimeAvailability(rigId:string):Promise<void> {
  const authority=this.get(rigId);if(!authority||!this.runtimeObserver)return;
  const run=(this.availabilityRuns.get(rigId)??0)+1;this.availabilityRuns.set(rigId,run);const members=JSON.parse(authority.coordinators) as string[];
  for(const session of members)this.runtimeEvidence.delete(session);
  // Independent two-seat probes run together to remove serial probe latency.
  // A slow completion still fails the unchanged strict freshness fence.
  const results=await Promise.allSettled(members.map(session=>this.runtimeObserver!(session)));
  const observations=results.flatMap(result=>result.status==='fulfilled'&&result.value?[result.value]:[]);
  if(run!==this.availabilityRuns.get(rigId))return; // delayed probe cannot overwrite a newer observation
  for(const observation of observations)this.runtimeEvidence.set(observation.session,observation);
 }
/** The node that backs a session, for node-scoped guard and reservation fences.
   * A session is registered under its session_name with a generated id, so the logical address is
   * resolved exactly as every other identity read here, through the canonical local lookup: its
   * host qualifier is honoured, never stripped, and an unregistered address stays unregistered. */
  private nodeOf(session:string):string {
   const node=this.local(session);
   if(!node)reject("coordinator_unknown_session","Session is not registered");
   return node!.id;
  }
 private excluded(session:string,generation?:string):boolean {
  const e=this.runtimeObservation(session);return !!e&&e.state==='absent'&&e.session===session&&!!e.fingerprint&&e.observedAt<=this.now()&&this.now()-e.observedAt<=1000&&e.generation===(generation??this.generation(session))&&e.generation===this.generation(session);
 }
 available(): boolean { return !!this.db.prepare("SELECT 1 FROM sqlite_master WHERE name='coordinator_authority'").get(); }
 get(rigId: string): Authority | undefined { return this.db.prepare("SELECT * FROM coordinator_authority WHERE rig_id=?").get(rigId) as Authority | undefined; }
 private local(session: string): { rig_id: string; id: string } | undefined {
   // A host-qualified address is outside this local fence. Never strip its origin.
   const parts = session.split("@");
   if (parts.length !== 2) return undefined;
   const registered=this.db.prepare("SELECT n.rig_id,n.id FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN rigs r ON r.id=n.rig_id WHERE s.session_name=? AND r.name=? ORDER BY s.id DESC LIMIT 1").get(session,parts[1]) as {rig_id:string;id:string}|undefined;
   return registered ?? this.db.prepare("SELECT n.rig_id,n.id FROM nodes n JOIN rigs r ON r.id=n.rig_id WHERE n.logical_id=? AND r.name=?")
     .get(parts[0],parts[1]) as {rig_id:string;id:string}|undefined;
 }
 /** A live acknowledged registered baton is standing authority, not product work.
  * Expired/reconciling/recovery or inconsistent custody remains actionable. */
 isStandingAuthorityMarker(queueId:string):boolean {
  const a=this.db.prepare('SELECT * FROM coordinator_authority WHERE baton_id=?').get(queueId) as Authority|undefined;
  if(!a||a.state!=='active'||a.lease_until<=this.now()||this.generation(a.owner_session)!==a.owner_generation)return false;
  const q=this.db.prepare('SELECT destination_session,state,claimed_at,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId) as any;
  return !!q&&q.destination_session===a.owner_session&&q.state==='in-progress'&&!!q.claimed_at&&q.claimed_by_generation_uuid===a.owner_generation;
 }
 generation(session: string): string | null {
   const node = this.local(session); if (!node) return null;
   const row = this.db.prepare(`SELECT t.generation_uuid FROM sessions s JOIN occupant_tenures t ON t.node_id=s.node_id
     WHERE s.session_name=? AND s.node_id=? ORDER BY t.generation_ordinal DESC LIMIT 1`).get(session,node.id) as {generation_uuid:string}|undefined;
   return row?.generation_uuid ?? null;
 }
 /** Current Operator enrollment assigns coordination roles; names cannot create another occupant. */
 coordinatorMembersValid(rigId:string,owner:string,members:unknown):boolean {
   if(!Array.isArray(members)||members.length!==2||members.some(s=>typeof s!=='string')||new Set(members).size!==2||!members.includes(owner))return false;
   const identities=members.map(session=>{
     const node=this.local(session);if(!node||node.rig_id!==rigId||!this.generation(session))return null;
     return this.db.prepare(`SELECT n.id,n.runtime,s.resume_type,s.resume_token,t.generation_uuid,t.native_session_id_at_boot
       FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN occupant_tenures t ON t.node_id=n.id
       WHERE s.session_name=? AND n.id=? ORDER BY t.generation_ordinal DESC,s.id DESC LIMIT 1`).get(session,node.id) as {id:string;runtime:string|null;resume_type:string|null;resume_token:string|null;generation_uuid:string;native_session_id_at_boot:string|null}|undefined;
   });
   const [a,b]=identities;if(!a||!b||a.id===b.id||a.generation_uuid===b.generation_uuid)return false;
   if(a.runtime===b.runtime){
     if(a.native_session_id_at_boot&&b.native_session_id_at_boot&&a.native_session_id_at_boot===b.native_session_id_at_boot)return false;
     if(a.resume_type&&a.resume_type===b.resume_type&&a.resume_token&&b.resume_token&&a.resume_token===b.resume_token)return false;
   }
   return true;
 }
 private coordinatorMembers(authority:Authority):string[]{
   let members:unknown;try{members=JSON.parse(authority.coordinators);}catch{reject('coordinator_invalid_members','Malformed current coordinator roster requires exact Operator recovery');}
   if(!Array.isArray(members)||members.some(s=>typeof s!=='string'))reject('coordinator_invalid_members','Malformed current coordinator roster requires exact Operator recovery');
   return members as string[];
 }
 private caller(session: string, generation: string): void {
   if (!generation || this.generation(session) !== generation) reject("coordinator_generation_mismatch", "Caller generation is missing, retired, or unknown");
 }
 private operator(session: string, generation: string): void {
   if (session !== "operator-agent@kernel") reject("coordinator_operator_required", "Kernel Operator owns admission and operational recovery");
   this.caller(session,generation);
 }
 private log(rigId: string, operationId: string, kind: string, receipt: unknown, request:unknown={}): void {
   this.db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(rigId,operationId,kind,canonical(receipt),digest(canonical(request)));
 }
 private replay(rigId: string, operationId: string, kind: string,request:unknown={}): unknown {
   const r = this.db.prepare("SELECT kind,receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(rigId,operationId) as {kind:string;receipt:string;request_hash:string}|undefined;
   if (r && (r.kind !== kind || r.request_hash!==digest(canonical(request)))) reject("coordinator_operation_conflict","Operation ID reused for another operation or changed contract");
   return r ? JSON.parse(r.receipt) : undefined;
 }
 enable(actor: string, generation: string, input: {rigId:string;batonId:string;owner:string;ownerGeneration:string;coordinators:string[];leaseMs:number;operationId:string}): Authority {
   return this.db.transaction(() => {
     this.operator(actor,generation);
     const replay = this.replay(input.rigId,input.operationId,"enable",input); if (replay) return replay as Authority;
     if (this.get(input.rigId)) reject("coordinator_already_enabled","Enabled rig cannot silently reset epochs");
     if(!this.coordinatorMembersValid(input.rigId,input.owner,input.coordinators))reject("coordinator_invalid_members","Exactly two current same-rig distinct actual coordinator occupants required");
     this.caller(input.owner,input.ownerGeneration); this.validLease(input.leaseMs);
     const baton = this.db.prepare("SELECT destination_session,state FROM queue_items WHERE qitem_id=?").get(input.batonId) as {destination_session:string;state:string}|undefined;
     if (!baton || baton.destination_session!==input.owner || !["pending","in-progress"].includes(baton.state)) reject("coordinator_baton_mismatch","Canonical live baton must already belong to initial holder");
     const legacy=this.db.prepare("SELECT qitem_id FROM queue_items WHERE qitem_id<>? AND state NOT IN ('done','failed','denied','canceled','cancelled','handed-off')").all(input.batonId) as {qitem_id:string}[];
     for(const q of legacy){const row=this.db.prepare("SELECT source_session,destination_session FROM queue_items WHERE qitem_id=?").get(q.qitem_id) as {source_session:string;destination_session:string};if(this.local(row.source_session)?.rig_id===input.rigId||this.local(row.destination_session)?.rig_id===input.rigId)reject("coordinator_legacy_obligations","Nonempty rig requires separately reconciled migration; no existing custody imported implicitly");}
     const effects=this.db.prepare("SELECT sender_session,destination_session FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired')").all() as {sender_session:string;destination_session:string}[];
     const rigName=(this.db.prepare('SELECT name FROM rigs WHERE id=?').get(input.rigId) as {name:string}).name;
   const touches=(session:string)=>this.local(session)?.rig_id===input.rigId||session.split('@')[1]===rigName;
   if(effects.some(e=>touches(e.sender_session)||touches(e.destination_session)))reject("coordinator_legacy_effects","Uncertain delivery effects require explicit migration before enable");
     this.db.prepare("INSERT INTO coordinator_authority VALUES (?,?,?,?,1,?,'reconciling',?,?,NULL)").run(input.rigId,input.batonId,input.owner,input.ownerGeneration,this.now()+input.leaseMs,input.operationId,JSON.stringify(input.coordinators));
     const r=this.get(input.rigId)!; this.log(input.rigId,input.operationId,"enable",r,input); return r;
   }).immediate();
 }
 /** Read-only immutable inventory. Full private row bytes are hashed, never returned. */
 legacyInventory(rigId:string,authorizationId:string,adoptHeldHistory=false):LegacyInventory {
   const rig=this.db.prepare("SELECT name FROM rigs WHERE id=?").get(rigId) as {name:string}|undefined;
   if(!rig)reject("coordinator_wrong_rig","Known immutable rig required");
   // Include unresolved/host-qualified addresses naming this rig; missing identity
   // must not hide debt merely because a session was deregistered.
   const touches=(value:unknown)=>this.local(String(value))?.rig_id===rigId||String(value).split('@')[1]===rig!.name;
   const rows=this.db.prepare("SELECT * FROM queue_items WHERE state NOT IN ('done','failed','denied','canceled','cancelled','handed-off') AND qitem_id<>? ORDER BY qitem_id").all(authorizationId) as Array<Record<string,unknown>>;
   const owned=rows.filter(q=>touches(q.source_session)||touches(q.destination_session));
   const effects=this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired')").all() as Array<Record<string,unknown>>;
   const heldHistory:HeldHistoryRef[]=[];
   const uncertain=effects.filter(e=>touches(e.sender_session)||touches(e.destination_session)).flatMap(e=>{
     const held=adoptHeldHistory?this.containedHistory(e):null;
     if(held){heldHistory.push(held);return [];}
     return [digest(canonical(e))];
   }).sort();heldHistory.sort((a,b)=>a.outboxId.localeCompare(b.outboxId));
   const inventory={rows:owned.map(q=>({queueId:String(q.qitem_id),rowHash:digest(canonical(q))})),uncertainEffects:uncertain,...(adoptHeldHistory?{heldHistory}:{})};
   return {...inventory,snapshotDigest:digest(canonical(inventory))};
 }
 /** Explicit attributed adoption of existing custody; no queue mutation or wake. */
 migrateLegacy(actor:string,generation:string,input:LegacyEnrollment):Authority {
   return this.db.transaction(()=>{
     this.operator(actor,generation);this.caller(input.owner,input.ownerGeneration);
     const operationRequest={actor,generation,input};
     const replay=this.replay(input.rigId,input.operationId,"legacy-enrollment",operationRequest);if(replay)return replay as Authority;
     if(this.get(input.rigId))reject("coordinator_already_enabled","Migration cannot reset an enabled epoch");
     if(!this.coordinatorMembersValid(input.rigId,input.owner,input.coordinators))reject("coordinator_invalid_members","Exactly two current same-rig distinct actual coordinator occupants required");
     this.validLease(input.leaseMs);
     const auth=this.db.prepare("SELECT * FROM queue_items WHERE qitem_id=?").get(input.authorizationId) as Record<string,unknown>|undefined;
     let receipt:{kind?:string;proposalDigest?:string}|undefined;try{receipt=auth?JSON.parse(String(auth.body)):undefined;}catch{}
     if(!auth||auth.source_session!==input.owner||auth.destination_session!==actor||auth.minting_generation_uuid!==input.ownerGeneration||auth.state!=='in-progress'||auth.claimed_by_generation_uuid!==generation||receipt?.kind!=="coordinator-legacy-enrollment"||receipt.proposalDigest!==legacyProposalDigest(input))reject("coordinator_migration_authorization","Current Lead durable exact proposal authorization required");
     const actual=this.legacyInventory(input.rigId,input.authorizationId,input.inventory.heldHistory!==undefined);
     if(canonical(actual)!==canonical(input.inventory))reject("coordinator_migration_drift","Queue/claim/body/effect inventory changed; reconcile and reauthorize");
     if(actual.uncertainEffects.length)reject("coordinator_legacy_effects","Uncertain delivery effects must be reconciled before import");
     const held=actual.heldHistory??[];
     const recoveryBinding=held.length?this.validateHeldRecovery(actor,generation,input,held):null;
     if(!held.length&&input.heldHistoryRecovery)reject("coordinator_held_history_contract","Recovery must bind actual held debt");
     if(input.obligations.length!==actual.rows.length||new Set(input.obligations.map(o=>o.queueId)).size!==actual.rows.length||actual.rows.some(q=>!input.obligations.some(o=>o.queueId===q.queueId)))reject("coordinator_migration_incomplete","Every exact nonterminal obligation requires explicit attributed classification");
     const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(input.batonId) as {destination_session:string;state:string;claimed_by_generation_uuid:string|null}|undefined;
     if(!baton||baton.destination_session!==input.owner||!['pending','in-progress'].includes(baton.state)||(baton.state==='in-progress'&&baton.claimed_by_generation_uuid!==input.ownerGeneration))reject("coordinator_baton_mismatch","Preserved baton must have current exact custody");
     this.db.prepare("INSERT INTO coordinator_authority VALUES (?,?,?,?,1,?,'reconciling',?,?,NULL)").run(input.rigId,input.batonId,input.owner,input.ownerGeneration,this.now()+input.leaseMs,input.operationId,JSON.stringify(input.coordinators));
     const keys=new Set<string>();
     for(const obligation of input.obligations){
       if(!obligation.evidenceRef)reject("coordinator_migration_contract_unknown","Attributed scope/return evidence required; unknown work needs bounded recovery");
       const q=this.db.prepare("SELECT * FROM queue_items WHERE qitem_id=?").get(obligation.queueId) as Record<string,unknown>;
       const source=String(q.source_session),destination=String(q.destination_session);
       if(q.claimed_by_generation_uuid&&q.claimed_by_generation_uuid!==this.generation(destination))reject("coordinator_generation_mismatch","Existing claim belongs to another occupant; preserve and reconcile before import");
       if(q.state==='in-progress'&&!q.claimed_by_generation_uuid)reject("coordinator_migration_contract_unknown","Working custody lacks authoritative claim generation");
       const coordination=(session:string)=>input.coordinators.includes(session)||(session.endsWith('@kernel')&&!!this.local(session));
       if(obligation.kind==='coordination'){
         if(!coordination(source)||!coordination(destination)||obligation.contract||obligation.packageKey)reject("coordinator_migration_contract_unknown","Worker obligations cannot be disguised as coordination");
         continue;
       }
       if(obligation.kind!=='work'||!obligation.packageKey||!obligation.contract||keys.has(obligation.packageKey)||!['exclusive','read-only'].includes(obligation.resourceScope??''))reject("coordinator_migration_contract_unknown","Known unique package/resource/return contracts required");
       const c=obligation.contract!;const packageKey=obligation.packageKey!;keys.add(packageKey);
       if(c.destination!==destination||c.bodyHash!==digest(String(q.body))||!c.returnContract?.evidenceRequired?.length||(obligation.resourceScope==='exclusive'&&!c.resources.length)||(obligation.resourceScope==='read-only'&&c.resources.length))reject("coordinator_migration_contract_unknown","Contract must bind existing body/custody and known exclusive or read-only scope");
       if(!coordination(c.returnContract.destination))reject("coordinator_migration_contract_unknown","Return must reach a registered coordinator/Kernel");
       this.admit(actor,generation,input.rigId,packageKey,c);
       for(const resource of c.resources){if(this.db.prepare("SELECT 1 FROM coordinator_resources WHERE rig_id=? AND resource_key=?").get(input.rigId,resource))reject("coordinator_resource_conflict","Imported work overlaps exclusive scope");this.db.prepare("INSERT INTO coordinator_resources VALUES (?,?,?)").run(input.rigId,resource,packageKey);}
       this.db.prepare("INSERT INTO coordinator_assignments VALUES (?,?,?,?,?,?,?,?,NULL,?)").run(input.rigId,packageKey,obligation.queueId,destination,c.bodyHash,input.owner,input.ownerGeneration,1,null);
     }
     for(const h of held){
       const row=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(h.outboxId) as Record<string,unknown>;
       const post=this.historyCustody(row);
       this.db.prepare('INSERT INTO coordinator_held_history VALUES (?,?,?,?,?,?,?,?,?,?)').run(input.rigId,h.outboxId,input.operationId,h.rowHash,h.quarantineHash,h.operationHash,h.custodyHash,digest(canonical(post)),input.heldHistoryRecovery!.queueId,JSON.stringify({kind:'coordinator-held-history-adoption.v1',actor,generation,owner:input.owner,ownerGeneration:input.ownerGeneration,pre:h,postCustody:post,recovery:input.heldHistoryRecovery,recoveryBinding,deliveryConclusion:'unknown',originalMutations:0}));
       this.recordAdoptedOutcomeBase(input.rigId,row);
     }
     const result=this.get(input.rigId)!;this.log(input.rigId,input.operationId,"legacy-enrollment",result,operationRequest);return result;
   }).immediate();
 }
 /** Current-epoch adoption appends containment only; never imports custody or resets authority. */
 adoptHeldHistory(actor:string,generation:string,input:{rigId:string;operationId:string;expected:CoordinatorToken;effects:HeldHistoryRef[];recovery:{queueId:string;rowHash:string}}):unknown {
   return this.db.transaction(()=>{
     this.operator(actor,generation);
     if(!input||Object.keys(input).sort().join(',')!=='effects,expected,operationId,recovery,rigId'||typeof input.rigId!=='string'||typeof input.operationId!=='string'||!input.operationId.trim()||!input.expected||Object.keys(input.expected).sort().join(',')!=='epoch,generation,rigId'||input.expected.rigId!==input.rigId||!Number.isSafeInteger(input.expected.epoch)||typeof input.expected.generation!=='string'||!Array.isArray(input.effects)||input.effects.length<1||input.effects.length>2000||new Set(input.effects.map(h=>h?.outboxId)).size!==input.effects.length)reject('coordinator_held_history_contract','Exact bounded current-epoch containment contract required');
     const request={actor,generation,input};const prior=this.replay(input.rigId,input.operationId,'held-history-adoption',request);if(prior)return prior;
     const current=this.get(input.rigId);if(!current)reject('coordinator_not_enabled','Current enabled authority required');
     const a=this.assertOwner(current!.owner_session,input.expected);
     if(!this.coordinatorMembersValid(input.rigId,a.owner_session,this.coordinatorMembers(a)))reject('coordinator_invalid_members','Current registered coordinator occupants required');
     const rows=input.effects.map(ref=>{
       if(!ref||Object.keys(ref).sort().join(',')!=='custodyHash,operationHash,outboxId,quarantineHash,rowHash'||Object.values(ref).some(v=>typeof v!=='string'||!v.trim()))reject('coordinator_held_history_contract','Exact immutable row, quarantine, operation and custody references required');
       const row=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(ref.outboxId) as Record<string,unknown>|undefined;
       const held=row?this.containedHistory(row):null;
       const quarantine=this.db.prepare('SELECT rig_id FROM outbox_historical_quarantines WHERE outbox_id=?').get(ref.outboxId) as {rig_id:string}|undefined;
       if(!row||!held||quarantine?.rig_id!==input.rigId||canonical(held)!==canonical(ref))reject('coordinator_held_history_contract','Immutable same-rig contained row or custody changed; reconcile before adoption');
       if(this.db.prepare('SELECT 1 FROM coordinator_held_history WHERE rig_id=? AND outbox_id=?').get(input.rigId,ref.outboxId))reject('coordinator_held_history_conflict','Already adopted history requires existing recovery binding, not a second adoption');
       return {row:row!,held:held!};
     });
     const recoveryBinding=this.validateHeldRecovery(actor,generation,{rigId:input.rigId,operationId:input.operationId,owner:a.owner_session,ownerGeneration:a.owner_generation,heldHistoryRecovery:input.recovery} as LegacyEnrollment,input.effects);
     for(const queueId of [a.baton_id,input.recovery.queueId]){
       const claim=this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND state='in-progress' AND transition_note='claimed' ORDER BY transition_id DESC LIMIT 1").get(queueId) as {actor_session:string;identity_provenance:string}|undefined;
       if(!claim||claim.actor_session!==(queueId===a.baton_id?a.owner_session:actor)||claim.identity_provenance!=='transport:v1')reject('coordinator_held_history_contract','Actual current native holder and Operator recovery claims required');
     }
     for(const {row,held:h} of rows){const custody=this.historyCustody(row);
       this.db.prepare('INSERT INTO coordinator_held_history VALUES (?,?,?,?,?,?,?,?,?,?)').run(input.rigId,h.outboxId,input.operationId,h.rowHash,h.quarantineHash,h.operationHash,h.custodyHash,h.custodyHash,input.recovery.queueId,JSON.stringify({kind:'coordinator-held-history-adoption.v1',actor,generation,owner:a.owner_session,ownerGeneration:a.owner_generation,epoch:a.epoch,pre:h,postCustody:custody,recovery:input.recovery,recoveryBinding,deliveryConclusion:'unknown',originalMutations:0}));
       this.recordAdoptedOutcomeBase(input.rigId,row);
     }
     const receipt={kind:'coordinator-current-held-history-adoption.v1',actor,generation,expected:input.expected,effects:input.effects,recoveryBinding,deliveryConclusion:'unknown',originalMutations:0};this.log(input.rigId,input.operationId,'held-history-adoption',receipt,request);return receipt;
   }).immediate();
 }
 /** Complete pre-import custody; never overwrite the historical quarantine receipt. */
 private historyCustody(row:Record<string,unknown>):unknown {
   const q=row.audit_pointer?this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(row.audit_pointer):null;
   const assignment=row.audit_pointer?this.db.prepare('SELECT * FROM coordinator_assignments WHERE queue_id=?').all(row.audit_pointer):[];
   const resources=(assignment as {rig_id:string;package_key:string}[]).flatMap(a=>this.db.prepare('SELECT * FROM coordinator_resources WHERE rig_id=? AND package_key=? ORDER BY resource_key').all(a.rig_id,a.package_key));
   return {queue:q??null,assignment,resources};
 }
 private containedHistory(row:Record<string,unknown>):HeldHistoryRef|null {
   if(!['pending','indeterminate'].includes(String(row.delivery_state)))return null;
   const h=this.db.prepare("SELECT * FROM outbox_historical_quarantines WHERE outbox_id=? AND state='held'").get(row.outbox_id) as Record<string,unknown>|undefined;
   if(!h||h.original_hash!==digest(canonical(row)))return null;
   const op=this.db.prepare("SELECT * FROM outbox_historical_operations WHERE rig_id=? AND operation_id=? AND kind='quarantine'").get(h.rig_id,h.operation_id) as Record<string,unknown>|undefined;
   let receipt:Record<string,unknown>|undefined;try{receipt=op?JSON.parse(String(op.receipt)):undefined;}catch{}
   if(!op||!receipt||receipt.kind!=='historical-quarantine'||receipt.rigId!==h.rig_id||receipt.operationId!==h.operation_id||receipt.actor!=='operator-agent@kernel'||typeof receipt.generation!=='string'||typeof receipt.lead!=='string'||typeof receipt.leadGeneration!=='string'||receipt.deliveryConclusion!=='unknown'||receipt.outboxMutations!==0||!Array.isArray(receipt.effects)||!receipt.effects.includes(row.outbox_id)||receipt.admittedUntil!==h.admitted_until)return null;
   return {outboxId:String(row.outbox_id),rowHash:digest(canonical(row)),custodyHash:digest(canonical(this.historyCustody(row))),quarantineHash:digest(canonical(h)),operationHash:digest(canonical(op))};
 }
/** Static containment permits ordinary dispatch; finite recovery still gates takeover. */
  isAdoptedHistoryContained(rigId:string,row:Record<string,unknown>):boolean {
    const base=this.adoptionBase(rigId,row);
    if(!base)return false;
    const live=this.compatibleAdoptedCustody(this.historyCustody(row) as Record<string,unknown>,base.frozen);
    // Exact frozen/attested custody, or a complete immutable native outcome chain for THIS adoption.
    return canonical(live)===canonical(base.frozen)||this.custodyAttested(rigId,String(row.outbox_id),String(base.adopted.post_custody_hash),digest(canonical(live)))||this.adoptedNativeOutcome(rigId,row,base,live);
  }
  /** Every immutable adoption check that does not look at current custody. The frozen reference is the
   *  receipt, hashed at adoption; current bytes are only ever read through it. */
  private adoptionBase(rigId:string,row:Record<string,unknown>):{adopted:Record<string,unknown>;held:HeldHistoryRef;receipt:any;frozen:Record<string,unknown>}|null {
    const adopted=this.db.prepare('SELECT * FROM coordinator_held_history WHERE rig_id=? AND outbox_id=?').get(rigId,row.outbox_id) as Record<string,unknown>|undefined;
    const held=adopted?this.containedHistory(row):null;
    if(!held||adopted!.original_row_hash!==held.rowHash||adopted!.quarantine_hash!==held.quarantineHash||adopted!.quarantine_operation_hash!==held.operationHash)return null;
    let receipt:any;try{receipt=JSON.parse(String(adopted!.receipt));}catch{return null;}
    if(!receipt||receipt.kind!=='coordinator-held-history-adoption.v1'||receipt.actor!=='operator-agent@kernel'||typeof receipt.generation!=='string'||receipt.deliveryConclusion!=='unknown'||receipt.originalMutations!==0||receipt.pre?.outboxId!==row.outbox_id||receipt.pre?.rowHash!==held.rowHash||receipt.pre?.quarantineHash!==held.quarantineHash||receipt.pre?.operationHash!==held.operationHash)return null;
    const original=receipt.postCustody;
    if(original===null||typeof original!=='object'||Array.isArray(original))return null;
    const frozen=original as Record<string,unknown>;
    if(digest(canonical(frozen))!==adopted!.post_custody_hash)return null;
    return {adopted:adopted!,held,receipt,frozen};
  }
  /** Queue columns added after an adoption receipt was frozen. Migrations 090 (reply_to) and 091
   * (human_questions, human_answers) are nullable and purely additive, so an originally absent
   * column that is currently exactly null is the same custody that was adopted. Nothing else about
   * the comparison changes: any non-null value, any originally present column, any other added
   * column, and the whole assignment/resource pair stay compared exactly, and a null or non-object
   * queue is compared as-is. No hash, receipt or global canonical rule is redefined here. */
  private static readonly lateNullableQueueColumns=["reply_to","human_questions","human_answers"];
  private compatibleAdoptedCustody(current:Record<string,unknown>,original:Record<string,unknown>):Record<string,unknown> {
    const frozen=original.queue,live=current.queue;
    if(frozen===null||typeof frozen!=='object'||Array.isArray(frozen)||live===null||typeof live!=='object'||Array.isArray(live))return current;
    const queue={...(live as Record<string,unknown>)};
    for(const column of CoordinatorAuthorityService.lateNullableQueueColumns)
      if(!Object.prototype.hasOwnProperty.call(frozen,column)&&queue[column]===null)delete queue[column];
    return {...current,queue};
  }

 /** Generation-bound evidence is prospective and append-only. Old adoption receipts are never
  * upgraded by a current occupant label; their unproved custody drift remains held. */
 private outcomeEvidenceAvailable():boolean {
   return !!this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_held_history_outcome_bases'").get();
 }
 private nativeCustodyReceipt(transition:QueueTransition):NativeQueueCustodyReceipt|null {
   const row=this.db.prepare('SELECT receipt FROM queue_native_custody_evidence WHERE transition_id=? AND qitem_id=?')
     .get(transition.transitionId,transition.qitemId) as {receipt:string}|undefined;
   let receipt:NativeQueueCustodyReceipt;try{receipt=JSON.parse(row?.receipt??'null');}catch{return null;}
   if(!receipt||receipt.kind!=='queue-native-custody.v1'||typeof receipt.actorGeneration!=='string'||!receipt.actorGeneration||
      canonical(receipt.transition)!==canonical(transition)||!receipt.beforeQueue||!receipt.afterQueue||
      receipt.beforeQueue.qitem_id!==transition.qitemId||receipt.afterQueue.qitem_id!==transition.qitemId||
      receipt.afterQueue.state!==transition.state)return null;
   return receipt;
 }
 private compatibleOutcomeQueue(current:Record<string,unknown>,original:Record<string,unknown>):Record<string,unknown> {
   return this.compatibleAdoptedCustody({queue:current},{queue:original}).queue as Record<string,unknown>;
 }
 /** A complete chain, including same-state notes, is required. Missing generations, intermediate
  * writes, state cycles and non-native actors fail closed; timestamps are not history boundaries. */
 private nativeCustodyProgress(queueId:string,boundary:number,initial:Record<string,unknown>,generation:string):Record<string,unknown>|null {
   const history=(this.transitions??new QueueTransitionLog(this.db)).listForQitemAfter(queueId,boundary,2001);
   if(history.length>2000)return null;
   let queue=initial;
   const mutable=new Set(['state','ts_updated','claimed_at','claimed_by_generation_uuid','closure_required_at','closure_reason','closure_target','handed_off_to','blocked_on']);
   const terminals=['done','failed','denied','canceled','handed-off'];
   for(const transition of history){
     const receipt=this.nativeCustodyReceipt(transition);
     if(!receipt||transition.identityProvenance!=='transport:v1'||transition.actorSession!==initial.destination_session||receipt.actorGeneration!==generation||
        canonical(this.compatibleOutcomeQueue(receipt.beforeQueue,initial))!==canonical(queue))return null;
     const after=this.compatibleOutcomeQueue(receipt.afterQueue,initial),before=queue;
     const changed=Object.keys({...before,...after}).filter(k=>canonical(before[k])!==canonical(after[k]));
     if(changed.some(k=>!mutable.has(k)))return null;
     if(before.state===after.state){
       if(changed.some(k=>k!=='ts_updated'))return null;
     }else if(before.state==='pending'&&after.state==='in-progress'){
       if(transition.transitionNote!=='claimed'||before.claimed_at!==null||before.claimed_by_generation_uuid!==null||
          typeof after.claimed_at!=='string'||after.claimed_by_generation_uuid!==generation)return null;
     }else if(before.state==='in-progress'&&terminals.includes(String(after.state))){
       if(after.claimed_at!==before.claimed_at||after.claimed_by_generation_uuid!==generation)return null;
     }else return null;
     if(after.state!=='pending'&&(after.claimed_by_generation_uuid!==generation||typeof after.claimed_at!=='string'))return null;
     queue=after;
   }
   return queue;
 }
 private recordAdoptedOutcomeBase(rigId:string,row:Record<string,unknown>):void {
   if(!this.db.inTransaction)reject('coordinator_transaction_required','Adopted outcome boundary must be atomic with adoption');
   if(!this.outcomeEvidenceAvailable())return;
   const base=this.adoptionBase(rigId,row),queue=base?.frozen.queue as Record<string,unknown>|null;
   if(!base||!queue||typeof queue.qitem_id!=='string'||typeof queue.destination_session!=='string'||this.local(queue.destination_session)?.rig_id!==rigId)return;
   const generation=this.generation(queue.destination_session);
   if(!generation)return;
   const log=this.transitions??new QueueTransitionLog(this.db),last=log.latestForQitem(queue.qitem_id);
   if(!last||last.state!==queue.state)return;
   if(queue.state==='pending'){
     if(queue.claimed_at!==null||queue.claimed_by_generation_uuid!==null)return;
   }else{
     if(queue.claimed_by_generation_uuid!==generation||typeof queue.claimed_at!=='string')return;
     // Existing claims require their own immutable generation proof, not a session-only receipt.
     const claims=this.db.prepare('SELECT transition_id FROM queue_native_custody_evidence WHERE qitem_id=? ORDER BY transition_id').all(queue.qitem_id) as Array<{transition_id:number}>;
     const claim=claims.find(ref=>{const transition=log.listForQitemAfter(String(queue.qitem_id),ref.transition_id-1,1)[0];
       const receipt=transition?this.nativeCustodyReceipt(transition):null;
       return receipt?.actorGeneration===generation&&transition?.transitionNote==='claimed'&&receipt.afterQueue.claimed_at===queue.claimed_at;});
     const first=claim?log.listForQitemAfter(queue.qitem_id,claim.transition_id-1,1)[0]:undefined;
     const receipt=first?this.nativeCustodyReceipt(first):null;
     const proven=receipt?this.nativeCustodyProgress(queue.qitem_id,first!.transitionId-1,receipt.beforeQueue,generation):null;
     if(!proven||canonical(proven)!==canonical(queue))return;
   }
   const receipt={kind:'coordinator-adopted-custody-base.v1',adoptionReceiptHash:digest(String(base.adopted.receipt)),postCustodyHash:base.adopted.post_custody_hash,
     queueId:queue.qitem_id,destination:queue.destination_session,destinationGeneration:generation,boundaryTransitionId:last.transitionId,
     deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false};
   this.db.prepare('INSERT INTO coordinator_held_history_outcome_bases(rig_id,outbox_id,receipt) VALUES (?,?,?)').run(rigId,row.outbox_id,JSON.stringify(receipt));
 }
 /** Dispose is a worker return, never acceptance. Only its exact immutable operation and typed
  * return authorize the recorded assignment disposition/resource changes. */
 private adoptedDisposition(rigId:string,frozen:any,current:any,queue:Record<string,unknown>,generation:string):boolean {
   if(frozen.disposition_id!==null||typeof current.disposition_id!=='string'||!['done','failed','denied','canceled','handed-off'].includes(String(queue.state)))return false;
   if(canonical({...current,disposition_id:null})!==canonical(frozen))return false;
   const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='disposition'").get(rigId,current.disposition_id) as {receipt:string}|undefined;
   let disposition:any;try{disposition=JSON.parse(op?.receipt??'null');}catch{return false;}
   if(canonical(disposition)!==canonical({packageKey:frozen.package_key,actor:queue.destination_session,generation}))return false;
   const pkg=this.db.prepare('SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(rigId,frozen.package_key) as {contract:string}|undefined;
   const returned=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(current.disposition_id) as any;
   let contract:PackageContract,payload:any;try{contract=JSON.parse(pkg?.contract??'null');payload=JSON.parse(returned?.body??'null');}catch{return false;}
   return !!contract&&!!contract.returnContract&&!!returned&&contract.destination===queue.destination_session&&contract.bodyHash===digest(String(queue.body))&&
     frozen.destination===queue.destination_session&&frozen.body_hash===contract.bodyHash&&returned.source_session===queue.destination_session&&
     returned.destination_session===contract.returnContract.destination&&returned.minting_generation_uuid===generation&&payload?.packageKey===frozen.package_key&&
     payload.inputDigest===contract.inputDigest&&Array.isArray(payload.evidence)&&Array.isArray(contract.returnContract.evidenceRequired)&&
     contract.returnContract.evidenceRequired.every(kind=>payload.evidence.some((e:any)=>e&&e.kind===kind&&typeof e.ref==='string'&&e.ref.length>0));
 }
 private adoptedNativeOutcome(rigId:string,row:Record<string,unknown>,base:NonNullable<ReturnType<CoordinatorAuthorityService['adoptionBase']>>,live:Record<string,unknown>):boolean {
   if(!this.outcomeEvidenceAvailable())return false;
   const stored=this.db.prepare('SELECT receipt FROM coordinator_held_history_outcome_bases WHERE rig_id=? AND outbox_id=?').get(rigId,row.outbox_id) as {receipt:string}|undefined;
   let proof:any;try{proof=JSON.parse(stored?.receipt??'null');}catch{return false;}
   const original=base.frozen.queue as Record<string,unknown>|null,current=live.queue as Record<string,unknown>|null;
   if(!original||!current||!proof||proof.kind!=='coordinator-adopted-custody-base.v1'||proof.adoptionReceiptHash!==digest(String(base.adopted.receipt))||
      proof.postCustodyHash!==base.adopted.post_custody_hash||proof.queueId!==original.qitem_id||proof.destination!==original.destination_session||
      typeof proof.destinationGeneration!=='string'||!proof.destinationGeneration||!Number.isSafeInteger(proof.boundaryTransitionId)||proof.boundaryTransitionId<1||
      proof.deliveryConclusion!=='unknown'||proof.originalMutations!==0||proof.outcomeOnly!==true||proof.grantsAuthority!==false)return false;
   const progressed=this.nativeCustodyProgress(proof.queueId,proof.boundaryTransitionId,original,proof.destinationGeneration);
   if(!progressed||canonical(progressed)!==canonical(current))return false;
   const assignments=base.frozen.assignment as any[],actual=live.assignment as any[],resources=base.frozen.resources as any[];
   if(!Array.isArray(assignments)||!Array.isArray(actual)||!Array.isArray(resources)||assignments.length!==actual.length)return false;
   const disposed=new Set<string>();
   for(let i=0;i<assignments.length;i++){
     if(canonical(assignments[i])===canonical(actual[i]))continue;
     if(assignments[i].rig_id!==rigId||!this.adoptedDisposition(rigId,assignments[i],actual[i],current,proof.destinationGeneration))return false;
     disposed.add(assignments[i].package_key);
   }
   return canonical(live.resources)===canonical(resources.filter(resource=>!disposed.has(resource.package_key)));
 }

 // ------------------------------------------------------------ custody attestation
 // An adopted row's frozen custody includes queue.ts_updated, so a recorded same-state touch of the
 // queue item breaks exact containment although nothing custody-relevant changed. Adoption is
 // immutable and cannot be repeated, so the supported repair is an append-only Operator attestation of
 // the EXACT current custody bytes, bound to a recorded cause. It edits no adoption, quarantine,
 // outbox or queue row and never converts UNKNOWN into delivered.
 private static readonly ISO_MS=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
 /** Transitions of one queue item whose timestamp is at or after `sinceTs`, live and archived, in id order. */
 private custodyTransitions(queueId:string,sinceTs:string):Array<{transition_id:number;ts:string;state:string;actor_session:string|null;identity_provenance:string|null}> {
   const tables=['queue_transitions'];
   if(this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='queue_transitions_archive'").get())tables.push('queue_transitions_archive');
   const rows=tables.flatMap(t=>this.db.prepare(`SELECT transition_id,ts,state,actor_session,identity_provenance FROM ${t} WHERE qitem_id=? AND ts>=?`).all(queueId,sinceTs)) as Array<{transition_id:number;ts:string;state:string;actor_session:string|null;identity_provenance:string|null}>;
   return rows.sort((a,b)=>a.transition_id-b.transition_id);
 }
 /** The exact evidence one adopted row's attestation must carry, derived only from live immutable records. */
 private custodyEvidence(rigId:string,outboxId:string):{ok:true;evidence:CustodyAttestationEvidence;ref:HeldHistoryRef}|{ok:false;code:string;message:string} {
   const no=(code:string,message:string)=>({ok:false as const,code,message});
   const row=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(outboxId) as Record<string,unknown>|undefined;
   if(!row)return no('custody_attestation_unknown_effect','No such effect');
   const base=this.adoptionBase(rigId,row);
   if(!base)return no('custody_attestation_not_adopted','Row is not exactly adopted with intact immutable containment');
   const frozen=base.frozen,live=this.compatibleAdoptedCustody(this.historyCustody(row) as Record<string,unknown>,frozen),liveHash=digest(canonical(live));
   if(canonical(live)===canonical(frozen))return no('custody_attestation_not_needed','Current custody already equals the frozen adoption');
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='held-history-custody-attestation-effect'").get(rigId,'held-custody-attest:'+outboxId+':'+liveHash))return no('custody_attestation_already_attested','These exact current custody bytes are already attested');
   const fq=frozen.queue,lq=live.queue,isObject=(v:unknown):v is Record<string,unknown>=>v!==null&&typeof v==='object'&&!Array.isArray(v);
   if(!isObject(fq)||!isObject(lq))return no('custody_attestation_queue_required','Frozen and current queue custody must both exist');
   const strip=(c:Record<string,unknown>,q:Record<string,unknown>)=>{const queue={...q};delete queue.ts_updated;return {...c,queue};};
   if(canonical(strip(live,lq))!==canonical(strip(frozen,fq)))return no('custody_attestation_not_timestamp_only','Only queue.ts_updated may differ from the frozen custody');
   const was=fq.ts_updated,now=lq.ts_updated;
   if(typeof was!=='string'||typeof now!=='string'||!CoordinatorAuthorityService.ISO_MS.test(was)||!CoordinatorAuthorityService.ISO_MS.test(now)||!(Date.parse(now)>Date.parse(was)))return no('custody_attestation_timestamp_invalid','Current ts_updated must be a later millisecond ISO timestamp than the frozen one');
   const queueId=String(lq.qitem_id),history=this.custodyTransitions(queueId,was);
   if(history.some(t=>t.state!==fq.state))return no('custody_attestation_state_cycle','A transition after the frozen timestamp has a different state; custody was exercised and cannot be attested');
   const causes=history.filter(t=>t.ts===now&&t.state===lq.state&&t.identity_provenance==='transport:v1'&&typeof t.actor_session==='string'&&t.actor_session.length>0);
   if(causes.length===0)return no('custody_attestation_cause_missing','No recorded native transition has exactly the current ts_updated and the current state');
   if(causes.length>1)return no('custody_attestation_cause_ambiguous','More than one recorded native transition has the current ts_updated');
   const c=causes[0]!;
   return {ok:true,ref:base.held,evidence:{outboxId,adoptionPostCustodyHash:String(base.adopted.post_custody_hash),attestedCustodyHash:liveHash,
     drift:[{path:'queue.ts_updated',frozen:was,current:now}],
     cause:{queueId,transitionId:c.transition_id,ts:c.ts,state:c.state,actorSession:String(c.actor_session),identityProvenance:'transport:v1'},
     history:{count:history.length,digest:digest(canonical(history.map(t=>[t.transition_id,t.ts,t.state,t.actor_session,t.identity_provenance])))}}};
 }
 /** Read-only: the exact evidence the Operator would attest, or the typed reason a row cannot be attested. */
 describeHeldHistoryCustody(actor:string,generation:string,input:{rigId:string;outboxIds:string[]}):unknown {
   this.operator(actor,generation);
   if(!input||typeof input!=='object'||Object.keys(input).sort().join(',')!=='outboxIds,rigId'||typeof input.rigId!=='string'||!Array.isArray(input.outboxIds)||input.outboxIds.length<1||input.outboxIds.length>2000||input.outboxIds.some(id=>typeof id!=='string'||!id.trim())||new Set(input.outboxIds).size!==input.outboxIds.length)reject('coordinator_held_history_contract','Exact bounded unique outbox ids required');
   return {rigId:input.rigId,effects:input.outboxIds.map(id=>{const r=this.custodyEvidence(input.rigId,id);return r.ok?{outboxId:id,ok:true,evidence:r.evidence}:{outboxId:id,ok:false,code:r.code,message:r.message};})};
 }
 /** Append-only attestation of exact current custody for already adopted rows; see the block comment above. */
 attestHeldHistoryCustody(actor:string,generation:string,input:{rigId:string;operationId:string;expected:CoordinatorToken;effects:CustodyAttestationEvidence[];recovery:{queueId:string;rowHash:string}}):unknown {
   return this.db.transaction(()=>{
     this.operator(actor,generation);
     if(!input||typeof input!=='object'||Object.keys(input).sort().join(',')!=='effects,expected,operationId,recovery,rigId'||typeof input.rigId!=='string'||typeof input.operationId!=='string'||!input.operationId.trim()||!input.expected||Object.keys(input.expected).sort().join(',')!=='epoch,generation,rigId'||input.expected.rigId!==input.rigId||!Number.isSafeInteger(input.expected.epoch)||typeof input.expected.generation!=='string'||!Array.isArray(input.effects)||input.effects.length<1||input.effects.length>2000||input.effects.some(e=>!e||typeof e!=='object'||typeof e.outboxId!=='string')||new Set(input.effects.map(e=>e.outboxId)).size!==input.effects.length)reject('coordinator_held_history_contract','Exact bounded current-epoch custody attestation contract required');
     const request={actor,generation,input};const prior=this.replay(input.rigId,input.operationId,'held-history-custody-attestation',request);if(prior)return prior;
     const current=this.get(input.rigId);if(!current)reject('coordinator_not_enabled','Current enabled authority required');
     const a=this.assertOwner(current!.owner_session,input.expected);
     if(!this.coordinatorMembersValid(input.rigId,a.owner_session,this.coordinatorMembers(a)))reject('coordinator_invalid_members','Current registered coordinator occupants required');
     // The evidence is recomputed from live immutable records inside this transaction and must equal the request exactly.
     const refs:HeldHistoryRef[]=[];
     input.effects.forEach(entry=>{
       const r=this.custodyEvidence(input.rigId,entry.outboxId);
       if(!r.ok)reject(r.code,r.message);
       if(canonical(r.ok?r.evidence:null)!==canonical(entry))reject('custody_attestation_evidence_drift','Attested evidence differs from the live immutable records; describe again');
       if(r.ok)refs.push(r.ref);
     });
     const recoveryBinding=this.validateHeldRecovery(actor,generation,{rigId:input.rigId,operationId:input.operationId,owner:a.owner_session,ownerGeneration:a.owner_generation,heldHistoryRecovery:input.recovery} as LegacyEnrollment,refs);
     for(const queueId of [a.baton_id,input.recovery.queueId]){
       const claim=this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND state='in-progress' AND transition_note='claimed' ORDER BY transition_id DESC LIMIT 1").get(queueId) as {actor_session:string;identity_provenance:string}|undefined;
       if(!claim||claim.actor_session!==(queueId===a.baton_id?a.owner_session:actor)||claim.identity_provenance!=='transport:v1')reject('coordinator_held_history_contract','Actual current native holder and Operator recovery claims required');
     }
     const receipt={kind:'coordinator-held-history-custody-attestation.v1',actor,generation,epoch:a.epoch,owner:a.owner_session,ownerGeneration:a.owner_generation,expected:input.expected,effects:input.effects,recovery:input.recovery,recoveryBinding,deliveryConclusion:'unknown',originalMutations:0};
     this.log(input.rigId,input.operationId,'held-history-custody-attestation',receipt,request);
     // One direct-lookup marker per exact (row, custody bytes) so containment never scans the operations table.
     for(const e of input.effects){const m={attestationOperationId:input.operationId,outboxId:e.outboxId,adoptionPostCustodyHash:e.adoptionPostCustodyHash,attestedCustodyHash:e.attestedCustodyHash};this.log(input.rigId,'held-custody-attest:'+e.outboxId+':'+e.attestedCustodyHash,'held-history-custody-attestation-effect',m,m);}
     return receipt;
   }).immediate();
 }
 /** True only for an Operator attestation of exactly these current custody bytes for exactly this adoption. */
 private custodyAttested(rigId:string,outboxId:string,adoptionPostCustodyHash:string,attestedCustodyHash:string):boolean {
   const get=(id:string,kind:string)=>this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind=?').get(rigId,id,kind) as {receipt:string}|undefined;
   const marker=get('held-custody-attest:'+outboxId+':'+attestedCustodyHash,'held-history-custody-attestation-effect');
   if(!marker)return false;
   try{
     const m=JSON.parse(marker.receipt);
     if(!m||m.outboxId!==outboxId||m.adoptionPostCustodyHash!==adoptionPostCustodyHash||m.attestedCustodyHash!==attestedCustodyHash||typeof m.attestationOperationId!=='string')return false;
     const parent=get(m.attestationOperationId,'held-history-custody-attestation');if(!parent)return false;
     const r=JSON.parse(parent.receipt);
     return !!r&&r.kind==='coordinator-held-history-custody-attestation.v1'&&r.actor==='operator-agent@kernel'&&r.deliveryConclusion==='unknown'&&r.originalMutations===0&&Array.isArray(r.effects)
       &&r.effects.some((e:any)=>e?.outboxId===outboxId&&e.adoptionPostCustodyHash===adoptionPostCustodyHash&&e.attestedCustodyHash===attestedCustodyHash);
   }catch{return false;}
 }
 /** Exact immutable adopted cohort for a current Lead's administrative authoring duty.
  * This is evidence, never a live recovery binding or delivery conclusion. */
 heldHistoryAuthoringSnapshot(rigId:string):HeldHistoryRef[] {
   const rows=this.db.prepare("SELECT e.* FROM coordinator_held_history h JOIN outbox_entries e ON e.outbox_id=h.outbox_id WHERE h.rig_id=? AND e.delivery_state IN ('pending','indeterminate') ORDER BY e.outbox_id LIMIT 2001").all(rigId) as Record<string,unknown>[];
   if(!rows.length||rows.length>2000)reject('coordinator_held_history_contract','Exact nonempty bounded adopted cohort required');
   return rows.map(row=>{if(!this.isAdoptedHistoryContained(rigId,row))reject('coordinator_held_history_contract','Immutable adopted containment changed');return this.containedHistory(row)!;});
 }
 heldHistoryAuthoringBindingComplete(rigId:string,operationId:string,refs:HeldHistoryRef[],queueId:string,lead:string,leadGeneration:string,operatorGeneration:string):boolean {
   const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='held-history-recovery-binding'").get(rigId,operationId) as {receipt:string}|undefined;if(!row)return false;
   try{const r=JSON.parse(row.receipt),q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(queueId) as any,b=r.binding;this.assertHeldRecoveryCurrent(b);
     return r.kind==='coordinator-held-history-recovery-binding.v1'&&r.actor==='operator-agent@kernel'&&r.generation===operatorGeneration&&r.originalMutations===0&&canonical(r.effects)===canonical(refs.map(h=>h.outboxId))&&canonical(this.heldHistoryAuthoringSnapshot(rigId))===canonical(refs)&&b.queueId===queueId&&b.lead===lead&&b.leadGeneration===leadGeneration&&!!q&&canonical(this.validateHeldRecovery(r.actor,r.generation,{rigId,operationId,owner:lead,ownerGeneration:leadGeneration,heldHistoryRecovery:{queueId,rowHash:digest(canonical(q))}} as LegacyEnrollment,refs))===canonical(b);
   }catch{return false;}
 }
 /** Takeover additionally needs live accountable recovery. */
 private adoptedHistoryContained(rigId:string,row:Record<string,unknown>):boolean {
   const adopted=this.db.prepare('SELECT * FROM coordinator_held_history WHERE rig_id=? AND outbox_id=?').get(rigId,row.outbox_id) as Record<string,unknown>|undefined;
   const held=adopted?this.containedHistory(row):null;
   if(!held||adopted!.original_row_hash!==held.rowHash||adopted!.quarantine_hash!==held.quarantineHash||adopted!.quarantine_operation_hash!==held.operationHash)return false;
   let binding:any;try{binding=JSON.parse(String(adopted!.receipt)).recoveryBinding;
     const renewals=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='held-history-recovery-binding' ORDER BY rowid DESC").all(rigId) as {receipt:string}[];
     for(const renewal of renewals){const r=JSON.parse(renewal.receipt);if(r.effects.includes(row.outbox_id)){binding=r.binding;break;}}
   }catch{reject('coordinator_held_history_recovery_required','Malformed adopted recovery binding; current Lead must issue a finite claimed replacement');}
   this.assertHeldRecoveryCurrent(binding);
   return true;
 }
 private validateHeldRecovery(actor:string,generation:string,input:LegacyEnrollment,held:HeldHistoryRef[]):Record<string,unknown> {
   const ref=input.heldHistoryRecovery;
   if(!ref||Object.keys(ref).sort().join(',')!=='queueId,rowHash'||typeof ref!.queueId!=='string'||typeof ref!.rowHash!=='string')reject('coordinator_held_history_contract','Exact accountable claimed recovery required');
   const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(ref!.queueId) as Record<string,unknown>|undefined;
   let b:Record<string,unknown>|undefined;try{b=q?JSON.parse(String(q.body)):undefined;}catch{}
   const creation=this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1").get(ref!.queueId) as {actor_session:string;identity_provenance:string}|undefined;
   const expected={kind:'coordinator-held-history-recovery.v1',rigId:input.rigId,operationId:input.operationId,owner:actor,generation,lead:input.owner,leadGeneration:input.ownerGeneration,effects:held.map(h=>h.outboxId),action:'reconcile-preserved-unknown-history',returnPath:{session:input.owner,queueId:ref!.queueId}};
   const deadline=b?.deadline;
   if(!q||digest(canonical(q))!==ref!.rowHash||q.source_session!==input.owner||q.destination_session!==actor||q.minting_generation_uuid!==input.ownerGeneration||q.claimed_by_generation_uuid!==generation||q.state!=='in-progress'||creation?.actor_session!==input.owner||creation.identity_provenance!=='transport:v1'||!b||Object.keys(b).sort().join(',')!==[...Object.keys(expected),'deadline'].sort().join(',')||!Number.isSafeInteger(deadline)||(deadline as number)<=this.now()||(deadline as number)>this.now()+1200000||!q.expires_at||!Number.isFinite(Date.parse(String(q.expires_at)))||Date.parse(String(q.expires_at))<=this.now()||Date.parse(String(q.expires_at))>(deadline as number)||Object.entries(expected).some(([k,v])=>canonical(b![k])!==canonical(v)))reject('coordinator_held_history_contract','Finite current Lead-authored actual Operator claimed recovery and exact effects required');
   return {queueId:ref!.queueId,bodyHash:digest(String(q!.body)),actor,generation,lead:input.owner,leadGeneration:input.ownerGeneration,expiresAt:q!.expires_at,deadline};
 }
 private assertHeldRecoveryCurrent(binding:any):void {
   const fail=()=>reject('coordinator_held_history_recovery_required','Adopted history needs current Lead-authored, Operator-claimed finite recovery; use held-history-recovery-bind');
   if(!binding||typeof binding.queueId!=='string')fail();
   const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(binding.queueId) as Record<string,unknown>|undefined;
   if(!q||q.state!=='in-progress'||q.source_session!==(binding.source??binding.lead)||q.destination_session!==binding.actor||(!binding.source&&q.minting_generation_uuid!==binding.leadGeneration)||q.claimed_by_generation_uuid!==binding.generation||this.generation(binding.actor)!==binding.generation||this.generation(binding.lead)!==binding.leadGeneration||digest(String(q.body))!==binding.bodyHash||q.expires_at!==binding.expiresAt||!Number.isSafeInteger(binding.deadline)||binding.deadline<=this.now()||!Number.isFinite(Date.parse(String(q.expires_at)))||Date.parse(String(q.expires_at))<=this.now())fail();
 }
 /** The existing explicit observer grant admits only this fresh unavailable-owner recovery episode. */
 heldRecoveryAdmission(rigId:string,jobId:string):Record<string,unknown> {
   const a=this.get(rigId),plan=this.coordinationRecovery?.plan(rigId);
   const job=this.db.prepare('SELECT * FROM watchdog_jobs WHERE job_id=?').get(jobId) as any;
   if(!a||!plan||!job||job.policy!=='coordinator-continuity'||job.state!=='active'||job.target_session!=='operator-agent@kernel'||job.registered_by_session!=='operator-agent@kernel'||job.registered_by_generation_uuid!==plan.operatorGeneration||this.generation('operator-agent@kernel')!==plan.operatorGeneration||plan.allowUnavailablePeerTransfer!==true||!['active','recovery'].includes(a.state)||a.lease_until>this.now()||!this.hasFreshUnavailableOwner(rigId))reject('coordinator_held_history_recovery_required','Current admitted observer and fresh unavailable owner required');
   const peer=this.coordinatorMembers(a!).find(s=>s!==a!.owner_session),pg=peer?this.generation(peer):null;
   if(!peer||!pg||!this.coordinationRecovery?.canTransferUnavailable(rigId,peer,pg))reject('coordination_unavailable_not_ready','Current finite admissions and fresh eligible Peer required');
   const rows=this.db.prepare("SELECT e.* FROM coordinator_held_history h JOIN outbox_entries e ON e.outbox_id=h.outbox_id WHERE h.rig_id=? AND e.delivery_state IN ('pending','indeterminate') ORDER BY e.outbox_id").all(rigId) as Record<string,unknown>[];
   const effects=rows.map(row=>{const h=this.containedHistory(row),old=this.db.prepare('SELECT * FROM coordinator_held_history WHERE rig_id=? AND outbox_id=?').get(rigId,row.outbox_id) as any;if(!h||old.original_row_hash!==h.rowHash||old.quarantine_hash!==h.quarantineHash||old.quarantine_operation_hash!==h.operationHash)reject('coordinator_held_history_contract','Immutable adopted containment changed');return h!;});
   if(!effects.length)reject('coordinator_held_history_contract','No adopted unresolved debt');
   return {jobId,rigId,epoch:a!.epoch,owner:a!.owner_session,ownerGeneration:a!.owner_generation,planRevision:plan!.revision,planHash:digest(canonical(plan)),peer,peerGeneration:pg,operatorGeneration:plan!.operatorGeneration,effects};
 }
 recordHeldRecoveryAdmission(rigId:string,queueId:string,bodyHash:string,admission:Record<string,unknown>):void {
   this.log(rigId,'held-recovery-notice:'+queueId,'held-history-recovery-notice',{queueId,bodyHash,admission},{queueId,bodyHash,admission});
 }
 private validateUnavailableHeldRecovery(actor:string,generation:string,input:{rigId:string;recovery:{queueId:string;rowHash:string}},refs:HeldHistoryRef[]):Record<string,unknown> {
   const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(input.recovery.queueId) as any;
   let b:any;try{b=JSON.parse(q?.body);}catch{}
   if(!b?.heldHistoryAdmission||typeof b.heldHistoryAdmission.jobId!=='string')reject('coordinator_held_history_contract','Exact supervise-created admission required');
   const admission=this.heldRecoveryAdmission(input.rigId,b.heldHistoryAdmission.jobId);
   const provenance=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='held-history-recovery-notice'").get(input.rigId,'held-recovery-notice:'+input.recovery.queueId) as {receipt:string}|undefined;
   const created=this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1").get(input.recovery.queueId) as any;
   if(!q||digest(canonical(q))!==input.recovery.rowHash||q.source_session!=='watchdog@system'||q.destination_session!==actor||q.state!=='in-progress'||q.claimed_by_generation_uuid!==generation||created?.actor_session!=='watchdog@system'||created.identity_provenance!=='system:operator-authorized-coordination'||b.action!=='recover-unavailable-coordinator'||b.reason!=='coordinator_held_history_recovery_required'||b.recipientGeneration!==generation||b.rigId!==input.rigId||b.epoch!==admission.epoch||canonical(b.heldHistoryAdmission)!==canonical(admission)||canonical(admission.effects)!==canonical(refs)||!Number.isSafeInteger(b.deadline)||b.deadline<=this.now()||b.deadline>this.now()+1200000||!q.expires_at||!Number.isFinite(Date.parse(q.expires_at))||Date.parse(q.expires_at)<=this.now()||Date.parse(q.expires_at)>b.deadline||!provenance||provenance.receipt!==canonical({queueId:input.recovery.queueId,bodyHash:digest(q.body),admission}))reject('coordinator_held_history_contract','Actual current Operator claim on exact finite supervise receipt required');
   return {queueId:q.qitem_id,bodyHash:digest(q.body),actor,generation,lead:admission.owner,leadGeneration:admission.ownerGeneration,source:'watchdog@system',provenance:'system:operator-authorized-coordination',expiresAt:q.expires_at,deadline:b.deadline};
 }
 /** Append a replacement accountability receipt; old adoption and claims remain immutable. */
 bindHeldHistoryRecovery(actor:string,generation:string,input:{rigId:string;operationId:string;effects:HeldHistoryRef[];recovery:{queueId:string;rowHash:string}}):unknown {
   return this.db.transaction(()=>{
     this.operator(actor,generation);if(!input||Object.keys(input).sort().join(',')!=='effects,operationId,recovery,rigId'||typeof input.rigId!=='string'||typeof input.operationId!=='string'||!input.operationId||!Array.isArray(input.effects))reject('coordinator_held_history_contract','Exact typed recovery binding required');const a=this.get(input.rigId);if(!a)reject('coordinator_not_enabled','Adoption authority required');this.caller(a!.owner_session,a!.owner_generation);
     const rows=this.db.prepare("SELECT e.* FROM coordinator_held_history h JOIN outbox_entries e ON e.outbox_id=h.outbox_id WHERE h.rig_id=? AND e.delivery_state IN ('pending','indeterminate') ORDER BY e.outbox_id").all(input.rigId) as Record<string,unknown>[];
     const refs=rows.map(row=>{const h=this.containedHistory(row),old=this.db.prepare('SELECT * FROM coordinator_held_history WHERE rig_id=? AND outbox_id=?').get(input.rigId,row.outbox_id) as any;if(!h||old.original_row_hash!==h.rowHash||old.quarantine_hash!==h.quarantineHash||old.quarantine_operation_hash!==h.operationHash)reject('coordinator_held_history_contract','Immutable adopted containment changed');return h!;});
     if(!refs.length||canonical(refs)!==canonical(input.effects))reject('coordinator_held_history_contract','Exact exhaustive current adopted effect references required');
     if(!input.recovery||Object.keys(input.recovery).sort().join(',')!=='queueId,rowHash'||typeof input.recovery.queueId!=='string'||typeof input.recovery.rowHash!=='string')reject('coordinator_held_history_contract','Exact current recovery reference required');
     const source=(this.db.prepare('SELECT source_session FROM queue_items WHERE qitem_id=?').get(input.recovery.queueId) as {source_session:string}|undefined)?.source_session;
     const binding=source==='watchdog@system'?this.validateUnavailableHeldRecovery(actor,generation,input,refs):this.validateHeldRecovery(actor,generation,{rigId:input.rigId,operationId:input.operationId,owner:a!.owner_session,ownerGeneration:a!.owner_generation,heldHistoryRecovery:input.recovery} as LegacyEnrollment,refs);
     const request={actor,generation,input};const prior=this.replay(input.rigId,input.operationId,'held-history-recovery-binding',request);if(prior){this.assertHeldRecoveryCurrent((prior as any).binding);return prior;}
     const receipt={kind:'coordinator-held-history-recovery-binding.v1',actor,generation,effects:refs.map(h=>h.outboxId),binding,originalMutations:0};this.log(input.rigId,input.operationId,'held-history-recovery-binding',receipt,request);
     // The binding is the fact that completes the authoring and pickup duties: freeze it here, in this transaction.
     this.coordinationRecovery?.captureHeldHistoryBinding(input.rigId,input.operationId);return receipt;
   }).immediate();
 }

 private validLease(ms: number): void { if (!Number.isSafeInteger(ms)||ms<1000||ms>3600000) reject("coordinator_invalid_lease","Lease must be 1 second to 1 hour"); }
 private assertOwner(actor:string, token:CoordinatorToken, allowReconcile=false): Authority {
   const r=this.get(token.rigId);
   if (!r || r.owner_session!==actor || r.owner_generation!==token.generation || r.epoch!==token.epoch) reject("coordinator_retired","Owner, generation, or epoch no longer holds authority");
   this.caller(actor,token.generation);
   if(r!.state==="active"){
     const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(r!.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string}|undefined;
     if(!baton||baton.destination_session!==actor||baton.state!=="in-progress"||baton.claimed_by_generation_uuid!==token.generation)reject("coordinator_baton_mismatch","Active authority requires its exact canonical baton claim");
   }
   if (r!.lease_until<=this.now()) reject("coordinator_lease_expired","Lease expiry does not elect another coordinator");
   if (r!.state!=="active" && !(allowReconcile && r!.state==="reconciling")) reject("coordinator_not_acknowledged","Transferred authority has not reconciled and acknowledged custody");
   return r!;
 }
 assertCurrentOperator(actor:string,generation:string):void {this.operator(actor,generation);}
 assertCurrentOwner(actor:string,token:CoordinatorToken):void {this.assertOwner(actor,token);}
 obligations(rigId:string): unknown[] {
   const localBySession=new Map<string,{rig_id:string;id:string}|undefined>();
   const local=(session:string)=>{
     if(!localBySession.has(session))localBySession.set(session,this.local(session));
     return localBySession.get(session);
   };
   const assignments=this.db.prepare(`SELECT a.package_key,a.queue_id,a.disposition_id,q.state,q.destination_session,q.claimed_at,q.claimed_by_generation_uuid,q.last_nudge_attempt,q.last_nudge_result,a.body_hash FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? ORDER BY a.package_key`).all(rigId);
   const stages=this.db.prepare("SELECT a.*,q.state,q.claimed_by_generation_uuid,q.last_nudge_attempt,q.last_nudge_result FROM coordinator_stage_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? ORDER BY a.queue_id").all(rigId);
   const resources=this.db.prepare("SELECT * FROM coordinator_resources WHERE rig_id=? ORDER BY resource_key").all(rigId);
   const queue=this.db.prepare("SELECT qitem_id,source_session,destination_session,state,claimed_by_generation_uuid,minting_generation_uuid,last_nudge_attempt,last_nudge_result FROM queue_items WHERE state NOT IN ('done','failed','denied','canceled','cancelled','handed-off') ORDER BY qitem_id").all() as Array<Record<string,unknown>>;
   return [{assignments,stages,resources,openQueue:queue.filter(q=>local(String(q.source_session))?.rig_id===rigId||local(String(q.destination_session))?.rig_id===rigId)}];
 }
 /** One explicit, bounded extension for an expired unacknowledged transfer.
  * Never elects a new owner, acknowledges work, or grants retired rights. */
 recoverReconciliation(actor:string,generation:string,input:{token:CoordinatorToken;operationId:string;obligationsDigest:string;windowMs:number}):Authority {
  return this.db.transaction(()=>{
   this.caller(actor,generation);
   if(typeof input.operationId!=="string"||!input.operationId.trim()||input.operationId.length>160)reject("coordinator_invalid_operation","Bounded attributed recovery operation ID required");
   const r=this.get(input.token.rigId);
   if(!r||r.state!=="reconciling"||r.epoch!==input.token.epoch||r.owner_generation!==input.token.generation||this.generation(r.owner_session)!==r.owner_generation)reject("coordinator_reconciliation_mismatch","Exact current unacknowledged owner/epoch required");
   if(actor!==r!.owner_session)this.operator(actor,generation);
   else if(generation!==r!.owner_generation)reject("coordinator_retired","Current recipient generation required");
   const request={actor,generation,input};const replay=this.replay(input.token.rigId,input.operationId,"reconciliation-recover",request);if(replay)return replay as Authority;
   if(r!.lease_until>this.now())reject("coordinator_reconciliation_not_expired","Recovery is only for expired unacknowledged transfer");
   if(!Number.isSafeInteger(input.windowMs)||input.windowMs<10000||input.windowMs>900000)reject("coordinator_invalid_ack_window","Recovery window must be 10 seconds to 15 minutes");
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='reconciliation-recover' AND json_extract(receipt,'$.epoch')=?").get(r!.rig_id,r!.epoch))reject("coordinator_reconciliation_recovery_exhausted","One bounded reconciliation recovery per epoch; no unlimited lease renewal");
   if(input.obligationsDigest!==this.reconciliationDigest(r!.rig_id))reject("coordinator_reconciliation_changed","Read exact current custody before extending acknowledgment window");
   const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(r!.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string|null}|undefined;
   if(!baton||baton.destination_session!==r!.owner_session||!['pending','in-progress'].includes(baton.state)||(baton.claimed_by_generation_uuid!==null&&baton.claimed_by_generation_uuid!==r!.owner_generation))reject("coordinator_baton_mismatch","Exact current recipient baton custody required");
   this.db.prepare("UPDATE coordinator_authority SET lease_until=?,operation_id=? WHERE rig_id=? AND epoch=? AND state='reconciling'").run(this.now()+input.windowMs,input.operationId,r!.rig_id,r!.epoch);
   const out=this.get(r!.rig_id)!;this.log(r!.rig_id,input.operationId,"reconciliation-recover",out,request);return out;
  }).immediate();
 }
 /** Operator opens a new finite reconciliation window for the exact expired
  * active holder. No custody moves, acknowledgment or product admission. */
 recoverExpiredActive(actor:string,generation:string,input:{token:CoordinatorToken;expectedLeaseUntil:number;operationId:string;obligationsDigest:string;windowMs:number}):Authority {
  return this.db.transaction(()=>{
   this.operator(actor,generation);
   if(typeof input.operationId!=="string"||!input.operationId.trim()||input.operationId.length>160)reject("coordinator_invalid_operation","Bounded recovery operation ID required");
   const request={actor,generation,input};const replay=this.replay(input.token.rigId,input.operationId,"active-expiry-recover",request);if(replay)return replay as Authority;
   const r=this.get(input.token.rigId);
   if(!r||r.state!=="active"||r.epoch!==input.token.epoch||r.owner_generation!==input.token.generation||this.generation(r.owner_session)!==r.owner_generation||r.lease_until!==input.expectedLeaseUntil)reject("coordinator_active_recovery_mismatch","Exact expired active holder, generation, epoch and lease required");
   if(r!.lease_until>this.now())reject("coordinator_lease_live","Recovery cannot replace a live lease");
   const e=this.runtimeObservation(r!.owner_session);
   if(!e||e.state!=="present"||e.session!==r!.owner_session||!e.fingerprint||e.generation!==r!.owner_generation||e.observedAt>this.now()||this.now()-e.observedAt>1000)reject("coordinator_live_holder_unproven","Fresh generation-bound native presence required");
   if(!Number.isSafeInteger(input.windowMs)||input.windowMs<10000||input.windowMs>900000)reject("coordinator_invalid_ack_window","Recovery window must be10seconds to15minutes");
   if(input.obligationsDigest!==this.reconciliationDigest(r!.rig_id))reject("coordinator_reconciliation_changed","Exact current custody digest required");
   const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(r!.baton_id) as any;
   if(!baton||baton.destination_session!==r!.owner_session||baton.state!=="in-progress"||baton.claimed_by_generation_uuid!==r!.owner_generation)reject("coordinator_baton_mismatch","Exact original holder baton custody required");
   this.assertRecipientDispatchScope(r!.rig_id,r!.owner_session);
   this.db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=?,operation_id=? WHERE rig_id=?").run(this.now()+input.windowMs,input.operationId,r!.rig_id);
   const out=this.get(r!.rig_id)!;this.log(r!.rig_id,input.operationId,"active-expiry-recover",out,request);return out;
  }).immediate();
 }
 acknowledge(actor:string, token:CoordinatorToken, evidence:{obligationsDigest:string;operationId:string}): Authority {
   return this.db.transaction(() => {
     const r=this.assertOwner(actor,token,true);
     const replay=this.replay(token.rigId,evidence.operationId,"acknowledge",{token,evidence}); if(replay) return replay as Authority;
     if (digest(canonical(this.obligations(token.rigId)))!==evidence.obligationsDigest) reject("coordinator_reconciliation_changed","Reconcile exact assignments, claims and uncertain nudge effects before acknowledgment");
     const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(r.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string|null}|undefined;
     if(!baton||baton.destination_session!==actor||!["pending","in-progress"].includes(baton.state)||(baton.claimed_by_generation_uuid!==null&&baton.claimed_by_generation_uuid!==token.generation))reject("coordinator_baton_mismatch","Cannot acknowledge another incarnation's baton");
     const ts=new Date(this.now()).toISOString();
     this.db.prepare("UPDATE queue_items SET state='in-progress',claimed_at=COALESCE(claimed_at,?),claimed_by_generation_uuid=?,ts_updated=? WHERE qitem_id=?").run(ts,token.generation,ts,r.baton_id);
     this.transitions?.append({qitemId:r.baton_id,state:"in-progress",actorSession:actor,transitionNote:`coordinator epoch ${token.epoch} acknowledged and reconciled`,identityProvenance:"transport:v1"});
     this.bus?.persistWithinTransaction({type:"queue.updated",qitemId:r.baton_id,fromState:baton!.state,toState:"in-progress",closureReason:null,closureTarget:null,actorSession:actor,summary:null});
     this.db.prepare("UPDATE coordinator_authority SET state='active',operation_id=? WHERE rig_id=?").run(evidence.operationId,token.rigId);
     const out=this.get(token.rigId)!; this.log(token.rigId,evidence.operationId,"acknowledge",out,{token,evidence}); return out;
   }).immediate();
 }
 /** Shared native owner continuation. Atomically acknowledges a LIVE reconciling owner and
  *  renews it in ONE transaction, deriving the caller's own token and the CURRENT obligations
  *  digest so an operator Lead never has to invent either.
  *
  *  It removes the observed procedural failure of a hand-assembled digest or a reused
  *  operation id. It NEVER recovers expired authority: a lapsed lease refuses and is only
  *  recoverable through the explicit expiry-recovery path. An already ACTIVE owner may renew
  *  here with equivalent fences. No admissions, qualifications, product acceptance, model
  *  change or UNKNOWN handling occurs, and nothing is retried automatically. */
 resumeOwned(actor:string, callerGeneration:string, input:{rigId:string;leaseMs:number;operationId:string;expectedEpoch:number;expectedObligationsDigest:string}): Authority {
  return this.db.transaction(() => {
   const found=this.get(input.rigId);
   if(!found)reject("coordinator_not_enabled","Rig authority is not enabled");
   // Narrow once after the rejection: every later read is inside this transaction, so the row
   // cannot vanish, and the refusal above still happens before any mutation.
   const row=found!;
   // Immutable authenticated caller identity: only the genuine recorded owner, at the exact
   // recorded generation. A foreign or stale generation refuses before anything else.
   if(row.owner_session!==actor)reject("coordinator_retired","Only the genuine recorded owner may resume its own authority");
   if(row.owner_generation!==callerGeneration)reject("coordinator_generation_mismatch","Immutable caller generation differs from the recorded owner generation");
   this.caller(actor,callerGeneration);
   // Bounded, attributed durable operation ID, validated before any mutation.
   if(typeof input.operationId!=="string"||!input.operationId.trim()||input.operationId.length>160)reject("coordinator_invalid_operation","Bounded attributed operation ID required");
   this.validLease(input.leaseMs);
   // The replay hash covers the authenticated caller plus the ENTIRE submitted contract. Every one
   // of those fields is immutable input to this call, so a changed value under the same operation id
   // is a genuine payload conflict and refuses. What is deliberately NOT hashed is any state the
   // server observes now: the derived token and the freshly computed obligations digest are
   // recomputed after this replay lookup, so a real acknowledgment cannot make an exact replay
   // self-conflict.
   const request={actor,callerGeneration,input};
   const replay=this.replay(input.rigId,input.operationId,"resume-owned",request);if(replay)return replay as Authority;
   // Only these two states may continue. Any unknown or retired state refuses; nothing is implicitly
   // activated.
   if(row.state!=="active"&&row.state!=="reconciling")reject("coordinator_reconciliation_changed",`Authority state ${row.state} cannot be resumed; explicit recovery is required`);
   // The caller's expected read contract must still be current. The CLI derives both from the
   // supported show surface, so the operator never invents them, and a value that went stale in the
   // meantime refuses instead of silently accepting a fresh read.
   if(row.epoch!==input.expectedEpoch)reject("coordinator_cas_lost","Authority epoch advanced since the expected read; re-read and resume again");
   const current=digest(canonical(this.obligations(input.rigId)));
   if(current!==input.expectedObligationsDigest)reject("coordinator_reconciliation_changed","Obligations changed since the expected read; reconcile exact assignments, claims and uncertain nudge effects before resuming");
   // An expired lease NEVER resumes. Resume continues a LIVE window; recovery is separate.
   if(row.lease_until<=this.now())reject("coordinator_lease_expired","An expired lease is recovered only by the explicit expiry-recovery path, never by resume-owned");
   // Derived, never supplied: the caller's own token for this current generation.
   const token:CoordinatorToken={rigId:input.rigId,epoch:row.epoch,generation:callerGeneration};
   let acknowledged=row.state!=="active";
   if(row.state==="active"){
    // An ACTIVE owner renewing here faces the SAME fence as assertOwner/renew: the exact
    // canonical baton must be in-progress and claimed by THIS current native generation.
    // A coordinator-assignment match cannot substitute for the baton claim.
    const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(row.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string}|undefined;
    if(!baton||baton.destination_session!==actor||baton.state!=="in-progress"||baton.claimed_by_generation_uuid!==token.generation)reject("coordinator_baton_mismatch","Active authority requires its exact canonical baton claim");
   }
   if(acknowledged){
    const baton=this.db.prepare("SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(row.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string}|undefined;
    // Same predicate as acknowledge: a freshly transferred or enabled owner may hold a genuinely
    // PENDING, unclaimed (null) baton. A baton already claimed by another generation still refuses.
    if(!baton||baton.destination_session!==actor||!["pending","in-progress"].includes(baton.state)||(baton.claimed_by_generation_uuid!==null&&baton.claimed_by_generation_uuid!==token.generation))reject("coordinator_baton_mismatch","Reconciliation requires its exact canonical baton destined to the caller");
    const ts=new Date(this.now()).toISOString();
    this.db.prepare("UPDATE queue_items SET state='in-progress',claimed_at=COALESCE(claimed_at,?),claimed_by_generation_uuid=?,ts_updated=? WHERE qitem_id=?").run(ts,token.generation,ts,row.baton_id);
    this.transitions?.append({qitemId:row.baton_id,state:"in-progress",actorSession:actor,transitionNote:`coordinator epoch ${token.epoch} acknowledged and reconciled`,identityProvenance:"transport:v1"});
    this.bus?.persistWithinTransaction({type:"queue.updated",qitemId:row.baton_id,fromState:baton!.state,toState:"in-progress",closureReason:null,closureTarget:null,actorSession:actor,summary:null});
    this.db.prepare("UPDATE coordinator_authority SET state='active',operation_id=? WHERE rig_id=?").run(input.operationId,input.rigId);
    acknowledged=true;
   }
   // Renewal happens in the SAME transaction, so any failure here rolls the acknowledgment back.
   this.db.prepare("UPDATE coordinator_authority SET lease_until=?,operation_id=? WHERE rig_id=?").run(this.now()+input.leaseMs,input.operationId,input.rigId);
   const out=this.get(input.rigId)!;
   this.log(input.rigId,input.operationId,"resume-owned",out,request);
   return out;
  }).immediate();
 }
 /** Bounded recovery for ONE expired reconciling window, for the same native holder whose single
  *  reconciliation-recover was already consumed.
  *
  *  It grants a fresh bounded reconciling window at epoch+1 and nothing else: no active state, no
  *  admission, no qualification, no dispatch, no queue/resource/UNKNOWN disposal, and the original
  *  operation and baton are preserved. The genuine holder must still separately resume-owned.
  *
  *  The incident anchor is derived from the STORED spent recovery receipt, never from the current
  *  epoch or from a successor receipt, so a successor cannot regenerate an incident key. Exactly one
  *  successor is allowed per anchor, across epochs, until a genuine acknowledge/resume-owned ends the
  *  incident. */
 recoverExpiredReconciling(actor:string, callerGeneration:string, input:{
  rigId:string; operationId:string; windowMs:number;
  expectedEpoch:number; expectedOwnerGeneration:string; expectedCustodyDigest:string;
  conflictOperationId:string; conflictKind:string;
 }):{authority:Authority;incidentAnchor:string;epoch:number;windowMs:number;leaseUntil:number;operationId:string} {
  return this.db.transaction(() => {
   // Genuine native Operator attribution only; the holder cannot invoke its own escape hatch.
   this.operator(actor,callerGeneration);
   if(typeof input.operationId!=="string"||!input.operationId.trim()||input.operationId.length>160)reject("coordinator_invalid_operation","Bounded attributed operation ID required");
   if(typeof input.conflictOperationId!=="string"||!input.conflictOperationId.trim()||input.conflictOperationId.length>160)reject("coordinator_invalid_operation","The conflicting operation ID must be the real one");
   if(typeof input.conflictKind!=="string"||!input.conflictKind.trim())reject("coordinator_invalid_operation","The conflicting operation kind must be named");
   if(!Number.isSafeInteger(input.windowMs)||input.windowMs<10000||input.windowMs>900000)reject("coordinator_invalid_ack_window","Recovery window must be 10 seconds to 15 minutes");
   const found=this.get(input.rigId);
   if(!found)reject("coordinator_not_enabled","Rig authority is not enabled");
   const row=found!;
   // The exact recorded current owner generation.
   if(row.owner_generation!==input.expectedOwnerGeneration||this.generation(row.owner_session)!==row.owner_generation)reject("coordinator_retired","The exact current owner generation is required");
   // Replay is resolved BEFORE any mutable-state CAS, so an exact replay of a timed-out call returns
   // its durable receipt instead of colliding with the state that same call itself produced.
   const request={actor,callerGeneration,input};
   const replay=this.replay(input.rigId,input.operationId,"expired-window-successor",request);if(replay)return replay as {authority:Authority;incidentAnchor:string;epoch:number;windowMs:number;leaseUntil:number;operationId:string};
   // The EXPIRED reconciling window of the exact recorded epoch.
   if(row.state!=="reconciling")reject("coordinator_reconciliation_changed","Only an expired reconciling window has a supported bounded recovery");
   if(row.epoch!==input.expectedEpoch)reject("coordinator_cas_lost","Authority epoch differs from the expected read");
   if(row.lease_until>this.now())reject("coordinator_reconciliation_not_expired","This recovery is only for an already expired reconciling window");
   // Exact custody: the same obligations the holder read. Nothing is disposed, so historical effect
   // debt is preserved and heldCount is never required to be zero.
   if(this.reconciliationDigest(input.rigId)!==input.expectedCustodyDigest)reject("coordinator_reconciliation_changed","Custody changed since the expected read; reconcile before recovering");
   // A retired or unavailable holder is not a recovery subject.
   if(this.excluded(row.owner_session,row.owner_generation))reject("coordinator_owner_unavailable","The recorded owner is retired or unavailable");
   // Native quiescence is read ONLY from the service's own freshly refreshed runtime evidence. No
   // caller-supplied proof is accepted anywhere in this contract: an authored launch or pid could be
   // fabricated, so it is structurally impossible to pass one in. The route refreshes availability
   // immediately before this mutation.
   const ownerNode=this.nodeOf(row.owner_session);
   const evidence=this.runtimeObservation(row.owner_session);
   // Exact current native occupant, freshly observed: same session, current generation, really present.
   if(!evidence||evidence.session!==row.owner_session||evidence.generation!==row.owner_generation||evidence.state!=="present"||!evidence.fingerprint)reject("coordinator_owner_unobserved","A fresh current native observation of the owner is required");
   // Narrow once after the refusal; the observation is not replaced inside this transaction.
   const observed=evidence!;
   if(observed.observedAt>this.now()||this.now()-observed.observedAt>1000)reject("coordinator_owner_unobserved","The native observation is stale; refresh it before recovering");
   // Positive settled quiescence only. `settled: null`, absent, or false is UNKNOWN and refuses:
   // fresh presence alone is not quiescence.
   const settled=observed.quiescence?.settled;
   const observedAt=observed.quiescence?.observedAt;
   if(settled!==true)reject("coordinator_quiescence_unproven","A positive native settled quiescence observation is required");
   if(typeof observedAt!=="string"||!observedAt.trim()||!Number.isFinite(Date.parse(observedAt))||Date.parse(observedAt)>this.now())reject("coordinator_quiescence_unproven","The quiescence observation must carry a real, non-future observation time");
   // Guard ON: the actual existing delivery guard for the owner's own node, desired AND effective.
   const guard=this.db.prepare("SELECT desired,effective FROM seat_delivery_guards WHERE node_id=?").get(ownerNode) as {desired:number;effective:number}|undefined;
   if(!guard||guard.desired!==1||guard.effective!==1)reject("coordinator_guard_not_enabled","The owner node's delivery guard must be desired AND effective before this recovery");
   // No sending effects and no live reservations may be outstanding. The reservation is bound to the
   // owner NODE and the owner SESSION; comparing node_id to a session name missed real reservations.
   const live=this.db.prepare("SELECT reservation_id FROM seat_dispatch_reservations WHERE (node_id=? OR session_name=?) AND state IN ('reserved','started')").all(ownerNode,row.owner_session) as {reservation_id:string}[];
   if(live.length)reject("coordinator_dispatch_reserved","An outstanding dispatch reservation forbids this recovery");
   // Only an ACTUALLY in-flight send forbids this recovery: OutboxHandler.beginSend writes 'sending'.
   // 'pending' is a known unattempted row, and retained/indeterminate UNKNOWN effects are historical
   // debt that must be preserved and never reclassified, so neither is treated as an active send.
   const sending=this.db.prepare("SELECT outbox_id FROM outbox_entries WHERE delivery_state='sending' AND (sender_session=? OR destination_session=?)").all(row.owner_session,row.owner_session) as {outbox_id:string}[];
   if(sending.length)reject("coordinator_send_in_flight","An in-flight send for the owner forbids this recovery");
   // The ORIGINAL spent recovery receipt must exist for this exact epoch. The incident anchor comes
   // from that stored receipt, not from the current epoch and not from any successor receipt.
   const spent=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='reconciliation-recover' AND json_extract(receipt,'$.epoch')<=? ORDER BY rowid DESC LIMIT 1").get(input.rigId,row.epoch) as {operation_id:string;receipt:string}|undefined;
   if(!spent)reject("coordinator_reconciliation_unrecovered","The original spent recovery receipt for this epoch is required");
   // Narrow once after the refusal; both reads are inside this transaction.
   const spentReceipt=JSON.parse(spent!.receipt) as {epoch:number;owner_generation:string;state:string};
   if(spentReceipt.state!=="reconciling"||spentReceipt.epoch>row.epoch||spentReceipt.owner_generation!==row.owner_generation)reject("coordinator_reconciliation_changed","The stored recovery receipt does not match the current incident");
   const incidentAnchor=`${input.rigId}#${spentReceipt.epoch}#${spentReceipt.owner_generation}`;
   // One successor per incident, across epochs. A successor receipt can never reset the anchor.
   // A genuine successful acknowledgment ends the incident, so no successor window applies at all. It
   // must be STRICTLY later than the stored recovery receipt: the acknowledgment that preceded the
   // incident is not its ending.
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind IN ('acknowledge','resume-owned') AND json_extract(receipt,'$.state')='active' AND json_extract(receipt,'$.epoch')>?").get(input.rigId,spentReceipt.epoch))reject("coordinator_incident_closed","The holder already acknowledged this incident; no successor window applies");
   // Exactly one successor per incident, across epochs. A successor receipt can never reset the anchor.
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='expired-window-successor' AND json_extract(receipt,'$.incidentAnchor')=?").get(input.rigId,incidentAnchor))reject("coordinator_incident_successor_exhausted","This incident already consumed its single bounded successor window");
   // Known backend NO-EFFECT conflict evidence only: a durable, different-kind reservation of the
   // same operation ID. A fabricated failed ACK receipt cannot satisfy this.
   // The conflict must be THIS incident's own spent recovery operation, read from the durable receipt
   // rather than from the caller. coordinator_operations is keyed by (rig, operation id), so the
   // holder reusing that exact id for an acknowledge is what the backend refused, and the durable row
   // for it is the recovery itself. An unrelated operation ID can never stand in.
   if(input.conflictOperationId!==spent!.operation_id)reject("coordinator_conflict_unproven","The conflicting operation ID must be the original incident's own spent recovery operation");
   if(input.conflictKind!=="reconciliation-recover")reject("coordinator_unsupported_conflict","The conflicting kind must be the incident's own durable recovery operation, never an acknowledge or a successor");
   // Same holder, same custody, same baton, epoch plus one. No automatic active state.
   const leaseUntil=this.now()+input.windowMs;
   this.db.prepare("UPDATE coordinator_authority SET epoch=epoch+1,lease_until=?,state='reconciling',operation_id=? WHERE rig_id=? AND epoch=?").run(leaseUntil,input.operationId,input.rigId,row.epoch);
   const authority=this.get(input.rigId)!;
   const receipt={authority,incidentAnchor,epoch:authority.epoch,windowMs:input.windowMs,leaseUntil,operationId:input.operationId};
   this.log(input.rigId,input.operationId,"expired-window-successor",receipt,request);
   return receipt;
  }).immediate();
 }

 transfer(actor:string, callerGeneration:string, input:{expected:CoordinatorToken;oldOwner:string;recipient:string;recipientGeneration:string;operationId:string;leaseMs:number;recoveryEvidenceId?:string}): Authority {
   return this.db.transaction(() => {
     const old=this.get(input.expected.rigId); if (!old) reject("coordinator_not_enabled","Rig is not enabled");
     this.caller(actor,callerGeneration);
     const replay=this.replay(input.expected.rigId,input.operationId,"transfer",input); if(replay) return replay as Authority;
     if (old!.owner_session!==input.oldOwner||old!.epoch!==input.expected.epoch||old!.owner_generation!==input.expected.generation) reject("coordinator_cas_lost","Expected predecessor changed; no transfer effects");
     if (actor===old!.owner_session) {
       if(callerGeneration!==old!.owner_generation) reject("coordinator_retired","Predecessor generation does not match");
       this.assertOwner(actor,input.expected);
       if(input.recipient===input.oldOwner)reject("coordinator_self_transfer_refused","Use explicit expiry recovery, not self-transfer");
     } else {
       this.operator(actor,callerGeneration);
       // Only an attributed, terminal operational recovery obligation permits unavailable-owner transfer.
       const e=input.recoveryEvidenceId && this.db.prepare("SELECT destination_session,state,closure_reason FROM queue_items WHERE qitem_id=?").get(input.recoveryEvidenceId) as {destination_session:string;state:string;closure_reason:string}|undefined;
       if (!this.excluded(old!.owner_session,old!.owner_generation)||!e||input.recoveryEvidenceId!==old!.recovery_queue_id||e.destination_session!==actor||e.state!=="done"||e.closure_reason!=="no-follow-on"||old!.lease_until>this.now()) reject("coordinator_recovery_evidence_required","Unplanned transfer needs expired lease and completed Operator recovery evidence; stale telemetry alone is insufficient");
     }
     this.assertRecipientDispatchScope(old!.rig_id,input.recipient);
     if (!(JSON.parse(old!.coordinators) as string[]).includes(input.recipient) || this.local(input.recipient)?.rig_id!==old!.rig_id) reject("coordinator_ineligible_recipient","Recipient is not a registered local coordinator");
     this.caller(input.recipient,input.recipientGeneration); this.validLease(input.leaseMs);
     const epoch=old!.epoch+1;
     const batonBefore=this.db.prepare("SELECT state FROM queue_items WHERE qitem_id=?").get(old!.baton_id) as {state:string};
     this.db.prepare("UPDATE coordinator_authority SET owner_session=?,owner_generation=?,epoch=?,lease_until=?,state='reconciling',operation_id=?,recovery_queue_id=NULL WHERE rig_id=?").run(input.recipient,input.recipientGeneration,epoch,this.now()+input.leaseMs,input.operationId,old!.rig_id);
     // Only canonical baton projection moves; workers, claims, resources and wake receipts are untouched.
     this.db.prepare("UPDATE queue_items SET destination_session=?,state='pending',claimed_at=NULL,claimed_by_generation_uuid=NULL,ts_updated=? WHERE qitem_id=?").run(input.recipient,new Date(this.now()).toISOString(),old!.baton_id);
     this.transitions?.append({qitemId:old!.baton_id,state:"pending",actorSession:actor,transitionNote:`coordinator transfer epoch ${epoch}; successor acknowledgment required`,identityProvenance:"transport:v1"});
     this.bus?.persistWithinTransaction({type:"queue.updated",qitemId:old!.baton_id,actorSession:actor,fromState:batonBefore.state,toState:"pending",closureReason:null,closureTarget:null,summary:null});
     const out=this.get(old!.rig_id)!; this.log(old!.rig_id,input.operationId,"transfer",out,input); return out;
   }).immediate();
 }
 /** Internal explicit Operator-approved stalled-live transfer. No worker/resource mutation.
  * Caller service holds the same IMMEDIATE transaction and fresh deciding-rung witnesses. */
 transferIdleStalled(jobId:string,input:{rigId:string;expectedEpoch:number;progressDigest:string;planRevision:string;recipient:string;recipientGeneration:string;leaseMs:number}):Authority {
  return this.db.transaction(()=>{
   const r=this.get(input.rigId);if(!r||r.state!=='active'||r.epoch!==input.expectedEpoch)reject('coordinator_cas_lost','Stalled holder changed');
   const job=this.db.prepare('SELECT * FROM watchdog_jobs WHERE job_id=?').get(jobId) as {policy:string;state:string;target_session:string;registered_by_session:string;registered_by_generation_uuid:string}|undefined;
   if(!job||job.policy!=='coordinator-continuity'||job.state!=='active'||job.target_session!=='operator-agent@kernel'||job.registered_by_session!=='operator-agent@kernel')reject('coordination_observer_not_authorized','Explicit current Operator watchdog required');
   this.operator(job!.registered_by_session,job!.registered_by_generation_uuid);
   const planRow=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-plan' ORDER BY rowid DESC LIMIT 1").get(input.rigId) as {receipt:string}|undefined;
   const progressRow=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-progress' ORDER BY rowid DESC LIMIT 1").get(input.rigId) as {receipt:string}|undefined;
   const plan=planRow?JSON.parse(planRow.receipt):null,progress=progressRow?JSON.parse(progressRow.receipt):null;
   if(!plan?.allowIdlePeerTransfer||plan.revision!==input.planRevision||plan.operatorGeneration!==job!.registered_by_generation_uuid||progress?.digest!==input.progressDigest||this.now()-progress.at<plan.stallMs)reject('coordination_stall_unproven','Unchanged progress and opt-in timeout required');
   if(!this.coordinationRecovery?.canTransferIdle(input.rigId,input.recipient,input.recipientGeneration,input.progressDigest))reject('coordination_stall_unproven','Fresh same-observation native idle and unchanged actual progress required');
   if(!(JSON.parse(r!.coordinators) as string[]).includes(input.recipient)||input.recipient===r!.owner_session)reject('coordinator_ineligible_recipient','Exact existing peer required');
   this.caller(r!.owner_session,r!.owner_generation);this.caller(input.recipient,input.recipientGeneration);this.validLease(input.leaseMs);
   // An uncertain transport effect may have already started coordination work.
   const effects=this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired')").all() as Record<string,unknown>[];
   const rigName=(this.db.prepare('SELECT name FROM rigs WHERE id=?').get(input.rigId) as {name:string}).name;
   const touches=(session:string)=>this.local(session)?.rig_id===input.rigId||session.split('@')[1]===rigName;
   if(effects.some(e=>(touches(String(e.sender_session))||touches(String(e.destination_session)))&&!this.adoptedHistoryContained(input.rigId,e)&&!this.coordinationRecovery?.noticeOutcomeContained(input.rigId,e)))reject('coordinator_uncertain_effects','Reconcile uncertain effects before stalled-live takeover');
   const baton=this.db.prepare('SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(r!.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string}|undefined;
   if(!baton||baton.destination_session!==r!.owner_session||baton.state!=='in-progress'||baton.claimed_by_generation_uuid!==r!.owner_generation)reject('coordinator_baton_mismatch','Actual old holder claim required');
   const epoch=r!.epoch+1,ts=new Date(this.now()).toISOString();
   this.db.prepare("UPDATE coordinator_authority SET owner_session=?,owner_generation=?,epoch=?,lease_until=?,state='reconciling',operation_id=?,recovery_queue_id=NULL WHERE rig_id=?").run(input.recipient,input.recipientGeneration,epoch,this.now()+input.leaseMs,'idle-stall:'+epoch,input.rigId);
   const before=this.db.prepare('SELECT state FROM queue_items WHERE qitem_id=?').get(r!.baton_id) as {state:string};
   this.db.prepare("UPDATE queue_items SET destination_session=?,state='pending',claimed_at=NULL,claimed_by_generation_uuid=NULL,ts_updated=? WHERE qitem_id=?").run(input.recipient,ts,r!.baton_id);
   this.transitions?.append({qitemId:r!.baton_id,state:'pending',actorSession:'watchdog@system',transitionNote:'Explicit idle-stall transfer; real Peer reconciliation/acknowledgment required',identityProvenance:'system:operator-authorized-coordination'});
   this.bus?.persistWithinTransaction({type:'queue.updated',qitemId:r!.baton_id,fromState:before.state,toState:'pending',actorSession:'watchdog@system',closureReason:null,closureTarget:null,summary:null});
   this.log(input.rigId,'idle-stall:'+epoch,'idle-stall-transfer',{...input,epoch},input);return this.get(input.rigId)!;
  }).immediate();
 }
 /** Watchdog may exclude only positive fresh native absence, never expiry or unknown alone. */
 hasFreshUnavailableOwner(rigId:string):boolean {const a=this.get(rigId);return !!a&&a.lease_until<=this.now()&&this.excluded(a.owner_session,a.owner_generation);}
 transferObservedUnavailable(jobId:string,input:{rigId:string;expectedEpoch:number;expectedOwner:string;expectedOwnerGeneration:string;planRevision:string;recipient:string;recipientGeneration:string;leaseMs:number}):Authority {
  return this.db.transaction(()=>{
   const a=this.get(input.rigId);if(!a||!['active','recovery'].includes(a.state)||a.epoch!==input.expectedEpoch||a.owner_session!==input.expectedOwner||a.owner_generation!==input.expectedOwnerGeneration)reject('coordinator_cas_lost','Unavailable predecessor changed');
   const job=this.db.prepare('SELECT * FROM watchdog_jobs WHERE job_id=?').get(jobId) as {policy:string;state:string;target_session:string;registered_by_session:string;registered_by_generation_uuid:string}|undefined;
   if(!job||job.policy!=='coordinator-continuity'||job.state!=='active'||job.target_session!=='operator-agent@kernel'||job.registered_by_session!=='operator-agent@kernel')reject('coordination_observer_not_authorized','Explicit current Operator watchdog required');
   this.operator(job!.registered_by_session,job!.registered_by_generation_uuid);
   const plan=this.coordinationRecovery?.plan(input.rigId);
   if(!plan||plan.operatorGeneration!==job!.registered_by_generation_uuid||plan.revision!==input.planRevision||plan.allowUnavailablePeerTransfer!==true)reject('coordination_unavailable_not_admitted','Current exact Operator plan must opt in');
   if(a!.lease_until>this.now()||!this.excluded(a!.owner_session,a!.owner_generation))reject('coordinator_outage_unproven','Expired lease and fresh generation-bound native absence required');
   const members=this.coordinatorMembers(a!);if(!this.coordinatorMembersValid(input.rigId,a!.owner_session,members)||!members.includes(input.recipient)||input.recipient===a!.owner_session)reject('coordinator_ineligible_recipient','Actual distinct current same-rig Peer required');
   this.caller(input.recipient,input.recipientGeneration);this.validLease(input.leaseMs);
   if(!this.coordinationRecovery?.canTransferUnavailable(input.rigId,input.recipient,input.recipientGeneration))reject('coordination_unavailable_not_ready','Fresh idle unoccupied Peer and current nonexpired task admissions required');
   const rigName=(this.db.prepare('SELECT name FROM rigs WHERE id=?').get(input.rigId) as {name:string}).name;
   const touches=(session:string)=>this.local(session)?.rig_id===input.rigId||session.split('@')[1]===rigName;
   const effects=this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired')").all() as Record<string,unknown>[];
   if(effects.some(e=>(touches(String(e.sender_session))||touches(String(e.destination_session)))&&!this.adoptedHistoryContained(input.rigId,e)&&!this.coordinationRecovery?.noticeOutcomeContained(input.rigId,e)))reject('coordinator_uncertain_effects','Uncertain effects require exact recovery before takeover');
   const baton=this.db.prepare('SELECT destination_session,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(a!.baton_id) as {destination_session:string;state:string;claimed_by_generation_uuid:string}|undefined;
   if(!baton||baton.destination_session!==a!.owner_session||baton.state!=='in-progress'||baton.claimed_by_generation_uuid!==a!.owner_generation)reject('coordinator_baton_mismatch','Exact predecessor claimed canonical baton required');
   const epoch=a!.epoch+1,ts=new Date(this.now()).toISOString(),operationId='unavailable-owner:'+epoch;
   this.db.prepare("UPDATE coordinator_authority SET owner_session=?,owner_generation=?,epoch=?,lease_until=?,state='reconciling',operation_id=?,recovery_queue_id=NULL WHERE rig_id=?").run(input.recipient,input.recipientGeneration,epoch,this.now()+input.leaseMs,operationId,input.rigId);
   this.db.prepare("UPDATE queue_items SET destination_session=?,state='pending',claimed_at=NULL,claimed_by_generation_uuid=NULL,ts_updated=? WHERE qitem_id=?").run(input.recipient,ts,a!.baton_id);
   this.transitions?.append({qitemId:a!.baton_id,state:'pending',actorSession:'watchdog@system',transitionNote:'Admitted fresh native unavailable-owner transfer; genuine Peer acknowledgment required',identityProvenance:'system:operator-authorized-coordination'});
   this.bus?.persistWithinTransaction({type:'queue.updated',qitemId:a!.baton_id,fromState:'in-progress',toState:'pending',actorSession:'watchdog@system',closureReason:null,closureTarget:null,summary:null});
   this.log(input.rigId,operationId,'unavailable-owner-transfer',{...input,epoch},input);return this.get(input.rigId)!;
  }).immediate();
 }
 admit(actor:string,generation:string,rigId:string,packageKey:string,contract:PackageContract): void {
   this.db.transaction(() => {
     this.operator(actor,generation); if(!this.get(rigId)) reject("coordinator_not_enabled","Enable rig before admitting packages");
   if(!packageKey||!contract.inputDigest||!contract.bodyHash||!Array.isArray(contract.resources)||new Set(contract.resources).size!==contract.resources.length||contract.resources.some(r=>!r)||!contract.returnContract?.destination||!Array.isArray(contract.returnContract.evidenceRequired)) reject("coordinator_invalid_package","Frozen input, resource and return contracts required");
   if(contract.workClass!==undefined&&!['product','recovery','administrative','inquiry'].includes(contract.workClass)) reject("coordinator_invalid_work_class","Work class must be product, recovery, administrative or inquiry");
   if(contract.scopeCitations!==undefined&&(!Array.isArray(contract.scopeCitations)||!contract.scopeCitations.length||contract.scopeCitations.some(c=>!c||typeof c.ref!=='string'||!c.ref.trim()||typeof c.digest!=='string'||!/^[0-9a-f]{64}$/.test(c.digest)))) reject("coordinator_invalid_scope_citations","Scope citations need exact refs and sha256 digests");
     if(this.local(contract.destination)?.rig_id!==rigId) reject("coordinator_cross_host_refused","Only local worker destinations supported");
     for(const t of contract.returnContract.transitions??[]){if(!t.bodyHash||this.local(t.source)?.rig_id!==rigId||this.local(t.destination)?.rig_id!==rigId)reject("coordinator_invalid_package","Stage transitions require exact local endpoints and immutable body hash");}
     const value=canonical(contract), hash=digest(value);
     const prior=this.db.prepare("SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(rigId,packageKey) as {contract_hash:string}|undefined;
     if(prior){if(prior.contract_hash!==hash)reject("coordinator_package_conflict","A package revision cannot change its admitted contract");return;}
     this.db.prepare("INSERT INTO coordinator_packages VALUES (?,?,?,?,?)").run(rigId,packageKey,value,hash,actor);
   }).immediate();
 }
 /** Runs INSIDE the queue write transaction, before rows/events/wake intents. */
 reserve(source:string,destination:string,body:string,queueId:string,envelope?:DispatchEnvelope,dryRun=false,sourceQueueId:string|null=null): void {
   // Explicit admitted recovery to a coordinator is work, even though ordinary
   // informational coordinator messages intentionally have no dispatch scope.
   const scope=this.scope(source,destination)??(envelope?this.get(envelope.token.rigId)??reject('coordinator_not_enabled','Explicit dispatch requires enrolled authority'):undefined);
   if(!scope) return;
   if(!this.db.inTransaction)reject("coordinator_transaction_required","Assignment fence requires its queue write transaction");
   const coords=JSON.parse(scope.coordinators) as string[];
   if (!coords.includes(source)) {
     // Registered workers may return evidence to coordinator/Operator without product authority.
     if(this.local(source)?.rig_id===scope.rig_id && (coords.includes(destination)||destination==="operator-agent@kernel")) return;
     if(this.local(source)?.rig_id===scope.rig_id && envelope?.token.rigId===scope.rig_id){
       this.caller(source,envelope.token.generation);
       const pkg=this.db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(scope.rig_id,envelope.packageKey) as {contract:string}|undefined;
       const allowed=pkg && (JSON.parse(pkg.contract) as PackageContract).returnContract.transitions?.some(t=>t.source===source&&t.destination===destination&&t.bodyHash===digest(body));
       if(!allowed)reject("coordinator_stage_not_admitted","Worker stage differs from frozen return contract");
       const prior=this.db.prepare("SELECT queue_id,source_queue_id FROM coordinator_stage_assignments WHERE rig_id=? AND package_key=? AND source=? AND destination=? AND body_hash=?").get(scope.rig_id,envelope.packageKey,source,destination,digest(body)) as {queue_id:string;source_queue_id:string|null}|undefined;
       if(prior&&prior.source_queue_id!==sourceQueueId)reject("coordinator_replay_source_conflict","Replay source differs from original committed operation");
       if(prior)throw new AssignmentReplay(prior.queue_id);
       const custody=this.db.prepare(`SELECT 1 FROM queue_items q WHERE q.destination_session=? AND q.claimed_by_generation_uuid=? AND q.state IN ('in-progress','blocked') AND (EXISTS(SELECT 1 FROM coordinator_assignments a WHERE a.queue_id=q.qitem_id AND a.rig_id=? AND a.package_key=?) OR EXISTS(SELECT 1 FROM coordinator_stage_assignments a WHERE a.queue_id=q.qitem_id AND a.rig_id=? AND a.package_key=?))`).get(source,envelope.token.generation,scope.rig_id,envelope.packageKey,scope.rig_id,envelope.packageKey);
       if(!custody)reject("coordinator_stage_custody_required","Worker must hold exact package custody before stage handoff");
       if(!dryRun)this.db.prepare("INSERT INTO coordinator_stage_assignments VALUES (?,?,?,?,?,?,?,?)").run(scope.rig_id,envelope.packageKey,source,destination,digest(body),queueId,envelope.token.generation,sourceQueueId);
       return;
     }
     reject("coordinator_dispatch_required","Fresh worker assignments require its coordinator or an admitted stage transition");
   }
   if(!envelope||envelope.token.rigId!==scope.rig_id)reject("coordinator_envelope_required","Enabled rig requires saved epoch/generation and server-admitted package key");
   this.assertOwner(source,envelope!.token);
   const pkg=this.db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(scope.rig_id,envelope!.packageKey) as {contract:string}|undefined;
   if(!pkg)reject("coordinator_package_not_admitted","Caller cannot mint a package revision");
   const contract=JSON.parse(pkg!.contract) as PackageContract;
   if(contract.destination!==destination||contract.bodyHash!==digest(body))reject("coordinator_package_conflict","Assignment differs from frozen admitted contract");
   this.assertRecipientDispatchScope(scope.rig_id,destination,envelope!.packageKey);
   const prior=this.db.prepare("SELECT queue_id,source_queue_id FROM coordinator_assignments WHERE rig_id=? AND package_key=?").get(scope.rig_id,envelope!.packageKey) as {queue_id:string;source_queue_id:string|null}|undefined;
   if(prior&&prior.source_queue_id!==sourceQueueId)reject("coordinator_replay_source_conflict","Replay source differs from original committed operation");
   if(prior)throw new AssignmentReplay(prior.queue_id);
   for(const resource of contract.resources){
     if(this.db.prepare("SELECT 1 FROM coordinator_resources WHERE rig_id=? AND resource_key=?").get(scope.rig_id,resource))reject("coordinator_resource_conflict","Exclusive resource remains reserved until attributed disposition");
     if(!dryRun)this.db.prepare("INSERT INTO coordinator_resources VALUES (?,?,?)").run(scope.rig_id,resource,envelope!.packageKey);
   }
   if(dryRun)return;
   this.db.prepare("INSERT INTO coordinator_assignments VALUES (?,?,?,?,?,?,?,?,NULL,?)").run(scope.rig_id,envelope!.packageKey,queueId,destination,digest(body),source,envelope!.token.generation,envelope!.token.epoch,sourceQueueId);
 }
 scope(source:string|undefined,destination:string): Authority | undefined {
   if(!this.available())return undefined;
   const from=source && this.local(source), to=this.local(destination);
   const origin=from ? this.get(from.rig_id) : (source ? this.db.prepare("SELECT * FROM coordinator_authority WHERE EXISTS(SELECT 1 FROM json_each(CASE WHEN json_valid(coordinators) THEN CASE WHEN json_type(coordinators)='array' THEN coordinators ELSE '[]' END ELSE '[]' END) WHERE value=?)").get(source) as Authority|undefined : undefined), target=to && this.get(to.rig_id);
   if(origin && this.coordinatorMembers(origin).includes(source!)) {
     // Informational messages to registered coordinators/Kernel remain available, no text classification.
     if(this.coordinatorMembers(origin).includes(destination)||destination.endsWith("@kernel"))return undefined;
     return origin;
   }
   if(target && !this.coordinatorMembers(target).includes(destination))return target;
   return undefined;
 }
 private assertRecipientDispatchScope(rigId:string,recipient:string,packageKey?:string):void {
   const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-plan' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;
   const restriction=row?(JSON.parse(row.receipt) as import('./coordination-recovery-service.js').CoordinationPlan).dispatchRestrictions?.find(r=>r.session===recipient):undefined;
   if(restriction&&(!packageKey||restriction.generation!==this.generation(recipient)||restriction.validUntil<=this.now()||!restriction.packageKeys.includes(packageKey)))reject('coordinator_checkpoint_quiescence','Current checkpoint dispatch scope excludes this assignment or takeover');
 }
 assertRawSend(source:string|undefined,destination:string): void {
   const scope=this.scope(source,destination);
   if(scope && source && this.local(source)?.rig_id===scope.rig_id && !(JSON.parse(scope.coordinators) as string[]).includes(source))return; // direct worker communication is not new durable dispatch
   if(scope)reject("coordinator_raw_dispatch_refused","Raw managed sends cannot bypass admitted durable assignments in enabled rigs");
 }
 /** Only the INTERNAL committed-queue wake seam can bypass the raw-send prohibition. */
 terminalReturnResourcesRetained(rigId:string,packageKey:string,contract:any):boolean {
   const actual=(this.db.prepare('SELECT resource_key FROM coordinator_resources WHERE rig_id=? AND package_key=? ORDER BY resource_key').all(rigId,packageKey) as Array<{resource_key:string}>).map(r=>r.resource_key);
   return Array.isArray(contract?.resources)&&JSON.stringify(actual)===JSON.stringify([...contract.resources].sort());
 }
 registerNativeTerminalReturnControl(actor:string,generation:string,rigId:string,queueId:string,body:string):void {
   if(!this.db.inTransaction)reject('coordinator_transaction_required','Native return duty requires atomic control registration');
   const a=this.get(rigId);if(!a||a.owner_session!==actor||a.owner_generation!==generation||this.generation(actor)!==generation||a.state!=='active'||a.lease_until<=this.now())reject('coordinator_retired','Current genuine holder required');
   const b=JSON.parse(body),plan=this.coordinationRecovery?.plan(rigId);
   const original=this.db.prepare("SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?").get(rigId,b.packageKey,b.originalQueueId) as any;
   const contract=original?JSON.parse(original.contract):null;
   const authorization=b.authorizationId?this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-successor-authorization'").get(rigId,b.authorizationId) as {receipt:string}|undefined:undefined;
   const successor=authorization?JSON.parse(authorization.receipt):null;
   const expectedId='qitem-coordination-terminal-return-'+digest(rigId+':'+b.originalQueueId+':'+b.recipientGeneration+(b.authorizationId?':'+b.authorizationId:'')).slice(0,24);
   if(b.authorizationId&&(!successor||successor.queueId!==queueId||successor.originalQueueId!==b.originalQueueId||successor.packageKey!==b.packageKey||successor.workerGeneration!==b.recipientGeneration||successor.holderGeneration!==generation||successor.operatorGeneration!==this.generation('operator-agent@kernel')||successor.expiresAt!==b.deadline||successor.previousControlId!==b.previousControlId))reject('coordinator_terminal_return_control_required','Exact durable successor authorization required');

   if(!plan||plan.operatorGeneration!==this.generation('operator-agent@kernel')||!original||original.disposition_id||!original.claimed_by_generation_uuid||!['done','failed','denied','canceled','handed-off'].includes(original.state)||original.claimed_by_generation_uuid!==this.generation(original.destination)||digest(original.body)!==original.body_hash||contract.destination!==original.destination||contract.bodyHash!==original.body_hash||b.action!=='record-exact-native-terminal-return'||b.rigId!==rigId||b.recipientGeneration!==original.claimed_by_generation_uuid||b.inputDigest!==contract.inputDigest||JSON.stringify(b.returnContract)!==JSON.stringify(contract.returnContract)||b.grantsAuthority!==false||!Number.isSafeInteger(b.deadline)||b.deadline<=this.now()||b.deadline>this.now()+1200000||queueId!==expectedId||!this.terminalReturnResourcesRetained(rigId,b.packageKey,contract))reject('coordinator_terminal_return_control_required','Exact original live claimant, immutable admitted contract and retained scope required');
   this.assertRecipientDispatchScope(rigId,original.destination,b.packageKey);
   this.log(rigId,queueId,'native-terminal-return-control',{queueId,bodyHash:digest(body),originalQueueId:b.originalQueueId,packageKey:b.packageKey,worker:original.destination,workerGeneration:b.recipientGeneration,holder:actor,holderGeneration:generation,operatorGeneration:plan!.operatorGeneration,expiresAt:b.deadline},{actor,generation,body});
 }
 private validNativeTerminalReturnWake(source:string|undefined,destination:string,queueId:string):boolean {
   if(source!=='watchdog@system')return false;
   const record=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind='native-terminal-return-control'").get(queueId) as {rig_id:string;receipt:string}|undefined;
   if(!record)return false;
   const r=JSON.parse(record.receipt),a=this.get(record.rig_id),q=this.db.prepare('SELECT source_session,destination_session,body,state,expires_at FROM queue_items WHERE qitem_id=?').get(queueId) as any;
   const original=this.db.prepare('SELECT disposition_id,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.queue_id=?').get(record.rig_id,r.originalQueueId) as any;
   if(!q||q.source_session!==source||q.destination_session!==destination||r.worker!==destination||digest(q.body)!==r.bodyHash||!['pending','in-progress','blocked'].includes(q.state)||Date.parse(q.expires_at)!==r.expiresAt||r.expiresAt<=this.now()||r.workerGeneration!==this.generation(destination)||!original||original.disposition_id||original.claimed_by_generation_uuid!==r.workerGeneration||!a||a.state!=='active'||a.lease_until<=this.now()||a.owner_session!==r.holder||a.owner_generation!==r.holderGeneration||this.generation(r.holder)!==r.holderGeneration||r.operatorGeneration!==this.generation('operator-agent@kernel'))return false;
   this.assertRecipientDispatchScope(record.rig_id,destination,r.packageKey);return true;
 }
 assertManagedSend(source:string|undefined,destination:string,queueAssignmentId?:string,proof?:{body:string;ids?:string[]}):void {
   if(queueAssignmentId&&this.outboxAbandonAuthorizationWake?.(source,destination,queueAssignmentId,proof))return;
   if(queueAssignmentId&&this.coordinationRecovery?.isLifecycleControl(queueAssignmentId)){if(this.coordinationRecovery.validLifecycleControlWake(source,destination,queueAssignmentId))return;reject('coordinator_lifecycle_wake_invalid','Exact current finite lifecycle duty proof required');}
   if(queueAssignmentId&&this.coordinationRecovery?.validTerminalReturnContinuationWake(source,destination,queueAssignmentId))return;
   if(queueAssignmentId&&this.validNativeTerminalReturnWake(source,destination,queueAssignmentId))return;
   if(!this.scope(source,destination))return;
   if(queueAssignmentId){
     const a=this.db.prepare(`SELECT a.rig_id,a.destination,a.owner_session,a.body_hash,q.body,q.state FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.queue_id=? UNION ALL SELECT a.rig_id,a.destination,a.source AS owner_session,a.body_hash,q.body,q.state FROM coordinator_stage_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.queue_id=?`).get(queueAssignmentId,queueAssignmentId) as {rig_id:string;destination:string;owner_session:string;body_hash:string;body:string;state:string}|undefined;
     if(a && a.destination===destination && a.owner_session===source && a.body_hash===digest(a.body) && ["pending","in-progress","blocked"].includes(a.state))return;
     const holder=a?this.get(a.rig_id):undefined;
     if(a&&a.destination===destination&&a.body_hash===digest(a.body)&&a.state==='pending'&&holder?.state==='active'&&holder.owner_session===source&&holder.owner_generation===this.generation(source!)&&holder.lease_until>this.now())return;
     reject("coordinator_wake_receipt_invalid","Internal wake does not match immutable committed assignment");
   }
   this.assertRawSend(source,destination);
 }
 renew(actor:string,token:CoordinatorToken,leaseMs:number,operationId:string):Authority {
   return this.db.transaction(()=>{
     this.assertOwner(actor,token);this.validLease(leaseMs);
     const previous=this.replay(token.rigId,operationId,"renew",{token,leaseMs});if(previous)return previous as Authority;
     this.db.prepare("UPDATE coordinator_authority SET lease_until=?,operation_id=? WHERE rig_id=?").run(this.now()+leaseMs,operationId,token.rigId);
     const r=this.get(token.rigId)!;this.log(token.rigId,operationId,"renew",r,{token,leaseMs});return r;
   }).immediate();
 }
  /** P3a read-only durable lookup of ONE recorded operation, keyed exactly by
   *  (rigId, operationId) — the PRIMARY KEY; no history scan, no transaction, no effect,
   *  and nothing here grants authority. A row that cannot be parsed is reported typed,
   *  NEVER as absent: absence must not be inferable into a rejection. Returns null only
   *  for a genuinely unrecorded operation. */
  operationReceipt(rigId: string, operationId: string): { rigId: string; operationId: string; kind: string; requestHash: string; receiptDigest: string; receipt: unknown } | null {
    const row = this.db.prepare("SELECT rig_id,operation_id,kind,receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(rigId, operationId) as { rig_id: string; operation_id: string; kind: string; receipt: string; request_hash: string } | undefined;
    if (!row) return null;
    let receipt: unknown;
    try { receipt = JSON.parse(row.receipt); } catch { throw new CoordinatorFenceError("coordinator_operation_receipt_unreadable", "A durable operation row exists but its receipt cannot be read; reconcile manually and never treat it as absent"); }
    return { rigId: row.rig_id, operationId: row.operation_id, kind: row.kind, requestHash: row.request_hash, receiptDigest: digest(row.receipt), receipt };
  }
 /** Read-only discovery of the frozen return contract; never grants authority or acceptance. */
 returnInstructionsFor(queueId:string) {
   if(!this.available())return null;
   const row=this.db.prepare(`SELECT a.rig_id,a.package_key,a.destination,a.body_hash,q.body,q.destination_session,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.queue_id=?`).get(queueId) as {rig_id:string;package_key:string;destination:string;body_hash:string;body:string;destination_session:string;contract:string}|undefined;
   if(!row)return null;
   let contract:PackageContract;try{contract=JSON.parse(row.contract);}catch{return null;}
   if(digest(row.body)!==row.body_hash||contract.bodyHash!==row.body_hash||contract.destination!==row.destination||row.destination_session!==row.destination||!contract.inputDigest||!contract.returnContract?.destination||!Array.isArray(contract.returnContract.evidenceRequired))return null;
   return {
     rigId:row.rig_id,packageKey:row.package_key,assignmentQueueId:queueId,worker:row.destination,
     returnDestination:contract.returnContract.destination,
     payloadTemplate:{packageKey:row.package_key,inputDigest:contract.inputDigest,evidence:contract.returnContract.evidenceRequired.map(kind=>({kind,ref:null}))},
     steps:["Fill every evidence ref with a real artifact reference; null placeholders are invalid.","Create a NEW native worker-authored return: rig queue create --destination <returnDestination> --body-file <payloadFile> --json","Close the original assignment using its truthful terminal outcome and required closure reason: rig queue update <assignmentQueueId> --state <terminalState> --closure-reason <reason> --json","Submit rig coordinator dispose <contractFile> with {rigId,packageKey,dispositionId}; dispositionId is the NEW return queue ID."],
     grantsAuthority:false,constitutesAcceptance:false
   };
 }
 dispose(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string): void {
   if([rigId,packageKey,dispositionId].some(v=>typeof v!=="string"||!v.trim()))reject("coordinator_dispose_contract_required","Expected {rigId,packageKey,dispositionId}; dispositionId is the new worker-authored typed JSON return queue item, not the original assignment or duty ID. The original worker disposes its own terminal return; coordinator-holder role is not required.");
   this.db.transaction(() => {
     this.caller(actor,generation);
     const a=this.db.prepare(`SELECT a.*,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?`).get(rigId,packageKey) as {destination:string;state:string;claimed_by_generation_uuid:string;disposition_id:string|null}|undefined;
     if(!a||a.destination!==actor||a.claimed_by_generation_uuid!==generation||!["done","failed","denied","canceled","handed-off"].includes(a.state))reject("coordinator_disposition_required","Only exact worker incarnation's terminal return can release reservations");
     if(!dispositionId)reject("coordinator_disposition_required","Disposition receipt required");
     if(a!.disposition_id){if(a!.disposition_id!==dispositionId)reject("coordinator_disposition_conflict","Return already consumed");return;}
     const pkg=this.db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(rigId,packageKey) as {contract:string};
     const contract=JSON.parse(pkg.contract) as PackageContract;
     const receipt=this.db.prepare("SELECT source_session,destination_session,minting_generation_uuid,body FROM queue_items WHERE qitem_id=?").get(dispositionId) as {source_session:string;destination_session:string;minting_generation_uuid:string|null;body:string}|undefined;
     let payload:{packageKey?:string;inputDigest?:string;evidence?:Array<{kind:string;ref:string}>}|undefined;try{payload=receipt?JSON.parse(receipt.body):undefined;}catch{}
     if(!receipt||receipt.source_session!==actor||receipt.destination_session!==contract.returnContract.destination||receipt.minting_generation_uuid!==generation||payload?.packageKey!==packageKey||payload.inputDigest!==contract.inputDigest||!Array.isArray(payload.evidence)||contract.returnContract.evidenceRequired.some(kind=>!payload!.evidence!.some(e=>e.kind===kind&&typeof e.ref==='string'&&e.ref.length>0)))reject("coordinator_return_receipt_required","Exact durable attributed return receipt with every required evidence kind must precede scope release; this is not acceptance");
     if(this.db.prepare("SELECT 1 FROM coordinator_stage_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND q.state NOT IN ('done','failed','denied','canceled','cancelled','handed-off')").get(rigId,packageKey))reject("coordinator_stage_still_open","Open stage custody must be reconciled before scope release");
     this.db.prepare("UPDATE coordinator_assignments SET disposition_id=? WHERE rig_id=? AND package_key=?").run(dispositionId,rigId,packageKey);
     this.db.prepare("DELETE FROM coordinator_resources WHERE rig_id=? AND package_key=?").run(rigId,packageKey);
     this.log(rigId,dispositionId,"disposition",{packageKey,actor,generation});
     this.runtimeOutcomeAssessment?.enqueueDisposed(actor,generation,rigId,packageKey,dispositionId);
   }).immediate();
 }
 /** Read-only exclusion observation. A stale/unknown activity verdict never excludes an owner. */
 observeContinuity(rigId:string): {rigId:string;expectedEpoch:number;evidenceId:string}|null {
   if(!this.available())return null;
   const r=this.get(rigId);
   if(!r||r.lease_until>this.now()||!this.excluded(r.owner_session,r.owner_generation))return null;
   if(r.recovery_queue_id){const q=this.db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(r.recovery_queue_id) as {state:string;claimed_by_generation_uuid:string|null}|undefined;
     if(!q||q.state!=="pending"||q.claimed_by_generation_uuid)return null;}
   return {rigId,expectedEpoch:r.epoch,evidenceId:`runtime-absent:${rigId}:${r.epoch}:${r.owner_generation}`};
 }
 /** Internal watchdog seam: explicit, current Operator registration; no impersonated native caller. */
 recordObservedOutage(rigId:string,expectedEpoch:number,jobId:string,evidenceId:string): {queueId:string;state:string} {
   return this.db.transaction(()=>{
     const job=this.db.prepare("SELECT policy,state,target_session,registered_by_session,registered_by_generation_uuid FROM watchdog_jobs WHERE job_id=?").get(jobId) as {policy:string;state:string;target_session:string;registered_by_session:string;registered_by_generation_uuid:string|null}|undefined;
     if(!job||job.policy!=="coordinator-continuity"||job.state!=="active"||job.target_session!=="operator-agent@kernel"||job.registered_by_session!=="operator-agent@kernel"||!job.registered_by_generation_uuid)reject("coordinator_observer_not_authorized","Current Operator must explicitly register this observer");
     this.operator(job!.registered_by_session,job!.registered_by_generation_uuid!);
     const observation=this.observeContinuity(rigId);
     if(!observation||observation.expectedEpoch!==expectedEpoch||observation.evidenceId!==evidenceId)reject("coordinator_outage_unproven","Exclusion changed since observation");
     return this.intakeOutage("watchdog@system",job!.registered_by_generation_uuid!,rigId,evidenceId);
   }).immediate();
 }
 /** Durable both-down intake. No product dispatch is granted to Operator. */
 recordOutage(actor:string,generation:string,rigId:string,evidenceId:string): {queueId:string;state:string} {
   return this.db.transaction(() => {
     this.operator(actor,generation); return this.intakeOutage(actor,generation,rigId,evidenceId);
   }).immediate();
 }
 private intakeOutage(actor:string,generation:string,rigId:string,evidenceId:string): {queueId:string;state:string} {
     const r=this.get(rigId);
     if(!r)reject("coordinator_not_enabled","Rig not enabled");
     if(r!.recovery_queue_id)return {queueId:r!.recovery_queue_id,state:r!.state};
     if(r!.lease_until>this.now())reject("coordinator_lease_live","Live lease cannot be excluded by a reminder");
     const members=JSON.parse(r!.coordinators) as string[];
     const bothDown=members.every(s=>this.excluded(s));
     if(!this.excluded(r!.owner_session,r!.owner_generation))reject("coordinator_outage_unproven","Owner requires fresh generation-bound native absence; stale or unknown telemetry is insufficient");
     if(!evidenceId)reject("coordinator_recovery_evidence_required","Operational incident evidence required");
     const queueId=`qitem-coordinator-recovery-${digest(`${rigId}:${r!.epoch}`).slice(0,24)}`;
     const ts=new Date(this.now()).toISOString();
     this.db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,priority,body) VALUES (?,?,?,?,?,'pending','urgent',?)")
       .run(queueId,ts,ts,actor,"operator-agent@kernel",`Recover ${bothDown ? "both coordinators" : "unavailable coordinator"} for ${rigId}; epoch ${r!.epoch} excluded. Evidence ${evidenceId}. No product dispatch authority granted. Preserve existing workers and uncertain delivery receipts.`);
     this.transitions?.append({qitemId:queueId,state:"pending",actorSession:actor,transitionNote:"coordinator operational recovery intake",identityProvenance:"transport:v1"});
     this.bus?.persistWithinTransaction({type:"queue.created",qitemId:queueId,sourceSession:actor,destinationSession:"operator-agent@kernel",priority:"urgent",tier:null,summary:null});
     this.db.prepare("UPDATE coordinator_authority SET state='recovery',recovery_queue_id=? WHERE rig_id=?").run(queueId,rigId);
     this.log(rigId,`outage:${r!.epoch}`,"outage",{queueId,evidenceId,actor,generation,bothDown});
     return {queueId,state:"recovery"};
 }
 reconciliationDigest(rigId:string):string {return digest(canonical(this.obligations(rigId)));}
}
