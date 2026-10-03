import type { RuntimeAvailability } from "./coordinator-runtime-availability.js";
import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import type { EventBus } from "./event-bus.js";
import type { QueueTransitionLog } from "./queue-transition-log.js";

export interface CoordinatorToken { rigId: string; epoch: number; generation: string }
export interface DispatchEnvelope { token: CoordinatorToken; packageKey: string }
export interface PackageContract {
 inputDigest: string; destination: string; bodyHash: string;
 resources: string[]; returnContract: { destination: string; evidenceRequired: string[]; transitions?: Array<{source:string;destination:string;bodyHash:string}> };
}
export interface LegacyInventory { rows: Array<{queueId:string;rowHash:string}>; uncertainEffects: string[]; snapshotDigest:string }
export interface LegacyEnrollment {
 rigId:string;batonId:string;owner:string;ownerGeneration:string;coordinators:string[];leaseMs:number;operationId:string;
 authorizationId:string;inventory:LegacyInventory;
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
 coordinationRecovery?: import("./coordination-recovery-service.js").CoordinationRecoveryService;
 private runtimeObserver?: (session:string)=>Promise<RuntimeAvailability|null>;
 private runtimeEvidence=new Map<string,RuntimeAvailability>();
 private availabilityRuns=new Map<string,number>();
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
 private excluded(session:string,generation?:string):boolean {
  const e=this.runtimeEvidence.get(session);return !!e&&e.state==='absent'&&e.session===session&&!!e.fingerprint&&e.observedAt<=this.now()&&this.now()-e.observedAt<=1000&&e.generation===(generation??this.generation(session))&&e.generation===this.generation(session);
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
 legacyInventory(rigId:string,authorizationId:string):LegacyInventory {
   const rig=this.db.prepare("SELECT name FROM rigs WHERE id=?").get(rigId) as {name:string}|undefined;
   if(!rig)reject("coordinator_wrong_rig","Known immutable rig required");
   // Include unresolved/host-qualified addresses naming this rig; missing identity
   // must not hide debt merely because a session was deregistered.
   const touches=(value:unknown)=>this.local(String(value))?.rig_id===rigId||String(value).split('@')[1]===rig!.name;
   const rows=this.db.prepare("SELECT * FROM queue_items WHERE state NOT IN ('done','failed','denied','canceled','cancelled','handed-off') AND qitem_id<>? ORDER BY qitem_id").all(authorizationId) as Array<Record<string,unknown>>;
   const owned=rows.filter(q=>touches(q.source_session)||touches(q.destination_session));
   const effects=this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired')").all() as Array<Record<string,unknown>>;
   const uncertain=effects.filter(e=>touches(e.sender_session)||touches(e.destination_session)).map(e=>digest(canonical(e))).sort();
   const inventory={rows:owned.map(q=>({queueId:String(q.qitem_id),rowHash:digest(canonical(q))})),uncertainEffects:uncertain};
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
     const actual=this.legacyInventory(input.rigId,input.authorizationId);
     if(canonical(actual)!==canonical(input.inventory))reject("coordinator_migration_drift","Queue/claim/body/effect inventory changed; reconcile and reauthorize");
     if(actual.uncertainEffects.length)reject("coordinator_legacy_effects","Uncertain delivery effects must be reconciled before import");
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
     const result=this.get(input.rigId)!;this.log(input.rigId,input.operationId,"legacy-enrollment",result,operationRequest);return result;
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
   const assignments=this.db.prepare(`SELECT a.package_key,a.queue_id,a.disposition_id,q.state,q.destination_session,q.claimed_at,q.claimed_by_generation_uuid,q.last_nudge_attempt,q.last_nudge_result,a.body_hash FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? ORDER BY a.package_key`).all(rigId);
   const stages=this.db.prepare("SELECT a.*,q.state,q.claimed_by_generation_uuid,q.last_nudge_attempt,q.last_nudge_result FROM coordinator_stage_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? ORDER BY a.queue_id").all(rigId);
   const resources=this.db.prepare("SELECT * FROM coordinator_resources WHERE rig_id=? ORDER BY resource_key").all(rigId);
   const queue=this.db.prepare("SELECT qitem_id,source_session,destination_session,state,claimed_by_generation_uuid,minting_generation_uuid,last_nudge_attempt,last_nudge_result FROM queue_items WHERE state NOT IN ('done','failed','denied','canceled','cancelled','handed-off') ORDER BY qitem_id").all() as Array<Record<string,unknown>>;
   return [{assignments,stages,resources,openQueue:queue.filter(q=>this.local(String(q.source_session))?.rig_id===rigId||this.local(String(q.destination_session))?.rig_id===rigId)}];
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
 transfer(actor:string, callerGeneration:string, input:{expected:CoordinatorToken;oldOwner:string;recipient:string;recipientGeneration:string;operationId:string;leaseMs:number;recoveryEvidenceId?:string}): Authority {
   return this.db.transaction(() => {
     const old=this.get(input.expected.rigId); if (!old) reject("coordinator_not_enabled","Rig is not enabled");
     this.caller(actor,callerGeneration);
     const replay=this.replay(input.expected.rigId,input.operationId,"transfer",input); if(replay) return replay as Authority;
     if (old!.owner_session!==input.oldOwner||old!.epoch!==input.expected.epoch||old!.owner_generation!==input.expected.generation) reject("coordinator_cas_lost","Expected predecessor changed; no transfer effects");
     if (actor===old!.owner_session) {
       if(callerGeneration!==old!.owner_generation) reject("coordinator_retired","Predecessor generation does not match");
     } else {
       this.operator(actor,callerGeneration);
       // Only an attributed, terminal operational recovery obligation permits unavailable-owner transfer.
       const e=input.recoveryEvidenceId && this.db.prepare("SELECT destination_session,state,closure_reason FROM queue_items WHERE qitem_id=?").get(input.recoveryEvidenceId) as {destination_session:string;state:string;closure_reason:string}|undefined;
       if (!this.excluded(old!.owner_session,old!.owner_generation)||!e||input.recoveryEvidenceId!==old!.recovery_queue_id||e.destination_session!==actor||e.state!=="done"||e.closure_reason!=="no-follow-on"||old!.lease_until>this.now()) reject("coordinator_recovery_evidence_required","Unplanned transfer needs expired lease and completed Operator recovery evidence; stale telemetry alone is insufficient");
     }
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
   const effects=this.db.prepare("SELECT sender_session,destination_session FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired')").all() as {sender_session:string;destination_session:string}[];
   const rigName=(this.db.prepare('SELECT name FROM rigs WHERE id=?').get(input.rigId) as {name:string}).name;
   const touches=(session:string)=>this.local(session)?.rig_id===input.rigId||session.split('@')[1]===rigName;
   if(effects.some(e=>touches(e.sender_session)||touches(e.destination_session)))reject('coordinator_uncertain_effects','Reconcile uncertain effects before stalled-live takeover');
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
 admit(actor:string,generation:string,rigId:string,packageKey:string,contract:PackageContract): void {
   this.db.transaction(() => {
     this.operator(actor,generation); if(!this.get(rigId)) reject("coordinator_not_enabled","Enable rig before admitting packages");
     if(!packageKey||!contract.inputDigest||!contract.bodyHash||!Array.isArray(contract.resources)||new Set(contract.resources).size!==contract.resources.length||contract.resources.some(r=>!r)||!contract.returnContract?.destination||!Array.isArray(contract.returnContract.evidenceRequired)) reject("coordinator_invalid_package","Frozen input, resource and return contracts required");
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
   const scope=this.scope(source,destination);
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
 assertRawSend(source:string|undefined,destination:string): void {
   const scope=this.scope(source,destination);
   if(scope && source && this.local(source)?.rig_id===scope.rig_id && !(JSON.parse(scope.coordinators) as string[]).includes(source))return; // direct worker communication is not new durable dispatch
   if(scope)reject("coordinator_raw_dispatch_refused","Raw managed sends cannot bypass admitted durable assignments in enabled rigs");
 }
 /** Only the INTERNAL committed-queue wake seam can bypass the raw-send prohibition. */
 assertManagedSend(source:string|undefined,destination:string,queueAssignmentId?:string):void {
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
 dispose(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string): void {
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
