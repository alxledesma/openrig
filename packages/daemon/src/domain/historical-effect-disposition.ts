import type Database from "better-sqlite3";
import {createHash} from "node:crypto";
import {lstatSync,readFileSync} from "node:fs";
import {isAbsolute} from "node:path";
import type {SeatDeliveryGuard} from "./seat-delivery-guard.js";

export class HistoricalEffectError extends Error {constructor(readonly code:string,message:string){super(message);}}
const refuse=(code:string,message:string):never=>{throw new HistoricalEffectError(code,message);};
const canonical=(value:unknown):string=>Array.isArray(value)?`[${value.map(canonical).join(',')}]`:value!==null&&typeof value==='object'?`{${Object.entries(value).sort(([a],[b])=>a<b?-1:a>b?1:0).map(([k,v])=>JSON.stringify(k)+':'+canonical(v)).join(',')}}`:JSON.stringify(value)??'null';
export const historicalDigest=(value:unknown):string=>createHash('sha256').update(canonical(value)).digest('hex');
export interface HistoricalEffectRef {outboxId:string;rowHash:string;custodyHash:string}
export interface HistoricalPlan {rigId:string;leadBatonId:string;leadGeneration:string;operatorGeneration:string;operationId:string;authorizationId:string;expiresAt:number;effects:HistoricalEffectRef[]}
export interface HistoricalStartupBundle {schema:"historical-startup-bundle.v1";cohorts:HistoricalPlan[]}
export interface HistoricalDisposition extends Omit<HistoricalPlan,'effects'> {quarantineOperationId:string;effect:HistoricalEffectRef;action:'withdraw-obsolete-wake'|'custodian-withdraw-unregistered-direct';reason:string;evidenceRef:string}
export type HistoricalSnapshotInput=Pick<HistoricalPlan,'rigId'|'leadBatonId'|'leadGeneration'|'operatorGeneration'> & {outboxIds:string[]};
type Row=Record<string,unknown> & {outbox_id:string;sender_session:string;destination_session:string;body:string;delivery_state:string;audit_pointer:string|null;guard_binding:string|null};

/** Separate ledger: age or an expired admission NEVER unquarantines an ambiguous effect. */
export function historicalQuarantineExists(db:Database.Database):boolean {return !!db.prepare("SELECT 1 FROM sqlite_master WHERE name='outbox_historical_quarantines'").get();}
export function isHistoricalQuarantined(db:Database.Database,id:string):boolean {return historicalQuarantineExists(db)&&!!db.prepare("SELECT 1 FROM outbox_historical_quarantines WHERE outbox_id=? AND state='held'").get(id);}

/** Managed local control, not hostile-local authentication. Original payload/provenance remain unchanged. */
export class HistoricalEffectDispositionService {
 constructor(readonly db:Database.Database,private now:()=>number=Date.now){}
 private local(session:string):{nodeId:string;generation:string;rigId:string}|undefined {
  if(session.split('@').length!==2)return undefined;
  return this.db.prepare(`SELECT s.node_id AS nodeId,t.generation_uuid AS generation,n.rig_id AS rigId
   FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN rigs r ON r.id=n.rig_id JOIN occupant_tenures t ON t.node_id=n.id
   WHERE s.session_name=? AND r.name=? ORDER BY s.id DESC,t.generation_ordinal DESC LIMIT 1`).get(session,session.split('@')[1]) as ReturnType<HistoricalEffectDispositionService['local']>;
 }
 private row(id:string):Row {const r=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(id) as Row|undefined;if(!r)refuse('historical_effect_missing','Exact historical effect required');return r!;}
 private custody(row:Row):unknown {
  const q=row.audit_pointer?this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(row.audit_pointer):undefined;
  const assignment=q?this.db.prepare('SELECT * FROM coordinator_assignments WHERE queue_id=?').all(row.audit_pointer):[];
  const resources=assignment.flatMap(a=>this.db.prepare('SELECT * FROM coordinator_resources WHERE rig_id=? AND package_key=? ORDER BY resource_key').all((a as {rig_id:string}).rig_id,(a as {package_key:string}).package_key));
  return {queue:q??null,assignment,resources};
 }
 inspect(rigId:string,ids:string[]):HistoricalEffectRef[] {return ids.map(id=>{const r=this.row(id);this.scope(rigId,r);return {outboxId:id,rowHash:historicalDigest(r),custodyHash:historicalDigest(this.custody(r))};});}
 snapshot(actor:string,generation:string,input:HistoricalSnapshotInput):HistoricalEffectRef[] {
  if(!input||['rigId','leadBatonId','leadGeneration','operatorGeneration'].some(k=>typeof (input as unknown as Record<string,unknown>)[k]!=='string')||!Array.isArray(input.outboxIds)||input.outboxIds.length<1||input.outboxIds.length>2000||input.outboxIds.some(id=>typeof id!=='string'))refuse('historical_exact_cohort','Explicit bounded snapshot required');
  this.actors(actor,generation,input);return this.inspect(input.rigId,input.outboxIds);
 }
 private scope(rigId:string,r:Row):void {
  const rig=this.db.prepare('SELECT name FROM rigs WHERE id=? AND archived_at IS NULL').get(rigId) as {name:string}|undefined;
  if(!rig||[r.sender_session,r.destination_session].some(s=>s.split('@').length!==2)||![r.sender_session,r.destination_session].some(s=>s.split('@')[1]===rig.name))refuse('historical_effect_scope','Exact active local rig; no host-qualified forwarding');
 }
 private actors(actor:string,generation:string,input:Pick<HistoricalPlan,'rigId'|'leadBatonId'|'leadGeneration'|'operatorGeneration'>):{lead:string;nodes:string[]} {
  const op=this.local(actor),baton=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(input.leadBatonId) as Record<string,unknown>|undefined;
  const lead=baton?this.local(String(baton.destination_session)):undefined;
  if(actor!=='operator-agent@kernel'||!op||op.generation!==generation||input.operatorGeneration!==generation)refuse('historical_operator_required','Genuine current Kernel Operator required');
  if(!baton||!lead||lead.rigId!==input.rigId||lead.generation!==input.leadGeneration||baton.state!=='in-progress'||baton.claimed_by_generation_uuid!==lead.generation)refuse('historical_lead_custody','Current exact Lead baton claim required');
  const authority=this.db.prepare('SELECT owner_session,owner_generation,baton_id FROM coordinator_authority WHERE rig_id=?').get(input.rigId) as {owner_session:string;owner_generation:string;baton_id:string}|undefined;
  if(authority&&(authority.owner_session!==baton!.destination_session||authority.owner_generation!==lead!.generation||authority.baton_id!==input.leadBatonId))refuse('historical_lead_custody','Retired dispatcher cannot authorize effect disposition');
  if(!authority&&(baton!.source_session!=='operator-agent@kernel'||!(this.db.prepare("SELECT 1 FROM nodes WHERE id=? AND logical_id='lead'").get(lead!.nodeId))))refuse('historical_lead_custody','Before enrollment the actual Operator-issued Lead baton is required');
  return {lead:String(baton!.destination_session),nodes:[op!.nodeId,lead!.nodeId]};
 }
 private validateShape(input:Omit<HistoricalPlan,'effects'>):void {
  if(!input||typeof input!=='object'||['rigId','leadBatonId','leadGeneration','operatorGeneration','operationId','authorizationId'].some(k=>typeof (input as unknown as Record<string,unknown>)[k]!=='string'))refuse('historical_contract_required','Explicit typed historical contract required');
 }
 private validateContract(input:Omit<HistoricalPlan,'effects'>):void {
  if(!input.operationId?.trim()||input.operationId.length>160||!input.authorizationId||!Number.isSafeInteger(input.expiresAt)||input.expiresAt<=this.now()||input.expiresAt>this.now()+1200000)refuse('historical_contract_required','Exact operation/authorization and future admission within20minutes required');
 }
 private authorization(actor:string,generation:string,input:Omit<HistoricalPlan,'effects'>,kind:string,requestHash:string,lead:string):void {
  const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(input.authorizationId) as Record<string,unknown>|undefined;
  const creation=this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1").get(input.authorizationId) as {actor_session:string;identity_provenance:string}|undefined;
  let body:Record<string,unknown>|undefined;try{body=q?JSON.parse(String(q.body)):undefined;}catch{}
  if(!q||q.source_session!==lead||q.destination_session!==actor||q.minting_generation_uuid!==input.leadGeneration||q.state!=='in-progress'||q.claimed_by_generation_uuid!==generation||creation?.actor_session!==lead||creation.identity_provenance!=='transport:v1'||!body||Object.keys(body).length!==2||body.kind!==kind||body.requestDigest!==requestHash)refuse('historical_authorization_required','Current Lead exact transport authorization actually claimed by Operator required');
 }
 private replay(rigId:string,op:string,kind:string,requestHash:string):unknown {
  const p=this.db.prepare('SELECT * FROM outbox_historical_operations WHERE rig_id=? AND operation_id=?').get(rigId,op) as {kind:string;request_hash:string;receipt:string}|undefined;
  if(p&&(p.kind!==kind||p.request_hash!==requestHash))refuse('historical_operation_conflict','Operation replay changed actor/request');return p?JSON.parse(p.receipt):undefined;
 }
 private record(rigId:string,op:string,kind:string,requestHash:string,receipt:unknown):void {this.db.prepare('INSERT INTO outbox_historical_operations VALUES (?,?,?,?,?)').run(rigId,op,kind,requestHash,JSON.stringify(receipt));}
 quarantine(actor:string,generation:string,input:HistoricalPlan):unknown {
  this.validateShape(input);
  return this.db.transaction(()=>{
   const {lead}=this.actors(actor,generation,input),hash=historicalDigest({actor,generation,input});const replay=this.replay(input.rigId,input.operationId,'quarantine',hash);if(replay)return replay;
   this.validateContract(input);if(!Array.isArray(input.effects)||input.effects.length<1||input.effects.length>2000||input.effects.some(e=>!e||['outboxId','rowHash','custodyHash'].some(k=>typeof (e as unknown as Record<string,unknown>)[k]!=='string'))||new Set(input.effects.map(e=>e.outboxId)).size!==input.effects.length)refuse('historical_exact_cohort','One bounded enumerated effect cohort required');
   this.authorization(actor,generation,input,'outbox-historical-quarantine-authorization',hash,lead);
   for(const e of input.effects){const r=this.row(e.outboxId);this.scope(input.rigId,r);if(!['pending','indeterminate'].includes(r.delivery_state)||historicalDigest(r)!==e.rowHash||historicalDigest(this.custody(r))!==e.custodyHash)refuse('historical_effect_drift','Exact unresolved row/custody changed; no quarantine');if(isHistoricalQuarantined(this.db,e.outboxId))refuse('historical_quarantine_conflict','Effect already has an attributed quarantine');
    this.db.prepare("INSERT INTO outbox_historical_quarantines VALUES (?,?,?,?,?,?,'held',?)").run(e.outboxId,input.rigId,e.rowHash,input.operationId,input.authorizationId,input.expiresAt,new Date(this.now()).toISOString());}
   const receipt={kind:'historical-quarantine',rigId:input.rigId,operationId:input.operationId,actor,generation,lead,leadGeneration:input.leadGeneration,effects:input.effects.map(e=>e.outboxId),admittedUntil:input.expiresAt,deliveryConclusion:'unknown',outboxMutations:0};this.record(input.rigId,input.operationId,'quarantine',hash,receipt);return receipt;
  }).immediate();
 }
 async dispose(actor:string,generation:string,input:HistoricalDisposition,guard:SeatDeliveryGuard|undefined):Promise<unknown> {
  this.validateShape(input);
  if(typeof input.quarantineOperationId!=='string'||!input.quarantineOperationId.trim()||!input.effect||['outboxId','rowHash','custodyHash'].some(k=>typeof (input.effect as unknown as Record<string,unknown>)[k]!=='string')||typeof input.reason!=='string'||typeof input.evidenceRef!=='string')refuse('historical_contract_required','Exact typed effect/reason/evidence required');
  if(!guard||guard.db!==this.db)refuse('historical_lifecycle_required','Same-database lifecycle guard required');
  const actors=this.actors(actor,generation,input),r=this.row(input.effect.outboxId),nodes=[...actors.nodes];
  const endpoints=[r.sender_session,r.destination_session].map(session=>({session,identity:this.local(session)??null}));
  for(const e of endpoints){if(e.identity)nodes.push(e.identity.nodeId);}
  if(r.guard_binding){try{const g=JSON.parse(r.guard_binding);if(typeof g.nodeId==='string'&&this.db.prepare('SELECT 1 FROM nodes WHERE id=?').get(g.nodeId))nodes.push(g.nodeId);}catch{refuse('historical_guard_invalid','Historical guard binding malformed');}}
  return guard!.lifecycle([...new Set(nodes)],async()=>this.db.transaction(()=>{
   const currentActors=this.actors(actor,generation,input);if(currentActors.lead!==actors.lead||historicalDigest(currentActors)!==historicalDigest(actors)||endpoints.some(e=>historicalDigest(this.local(e.session)??null)!==historicalDigest(e.identity))||nodes.some(n=>!guard!.ownsLifecycle(n)))refuse('historical_identity_drift','Current identity/lifecycle changed');
   const hash=historicalDigest({actor,generation,input}),replay=this.replay(input.rigId,input.operationId,'dispose',hash);if(replay)return replay;
   const quarantine=this.db.prepare("SELECT * FROM outbox_historical_quarantines WHERE outbox_id=?").get(input.effect.outboxId) as {rig_id:string;operation_id:string;original_hash:string;state:string}|undefined;
   if(!quarantine||quarantine.state!=='held'||quarantine.rig_id!==input.rigId||quarantine.operation_id!==input.quarantineOperationId||quarantine.original_hash!==input.effect.rowHash)refuse('historical_quarantine_required','Disposition requires exact prior historical quarantine membership');
   this.validateContract(input);if(!input.reason?.trim()||!input.evidenceRef?.trim())refuse('historical_contract_required','Attributed reason/evidence required');
   this.authorization(actor,generation,input,'outbox-historical-disposition-authorization',hash,actors.lead);
   const entry=this.row(input.effect.outboxId);this.scope(input.rigId,entry);
   if(!['pending','indeterminate'].includes(entry.delivery_state)||historicalDigest(entry)!==input.effect.rowHash||historicalDigest(this.custody(entry))!==input.effect.custodyHash)refuse('historical_effect_drift','Exact unresolved row/custody changed');
   const wake=entry.outbox_id.startsWith('wake-intent-');
   if(input.action==='withdraw-obsolete-wake'){
    if(!wake||!entry.audit_pointer||!this.db.prepare('SELECT 1 FROM queue_items WHERE qitem_id=?').get(entry.audit_pointer))refuse('historical_wake_custody_required','Exact executable wake with preserved queue custody required');
   }else if(input.action==='custodian-withdraw-unregistered-direct'){
    if(wake||entry.guard_binding||(this.local(entry.sender_session)&&this.local(entry.destination_session)))refuse('historical_custodian_case_required','Only unguarded nonwake with missing local endpoint; registered direct uses existing actualsender path');
   }else refuse('historical_action_required','Explicit bounded historical action required');
   const changed=this.db.prepare("UPDATE outbox_entries SET delivery_state='retired',retired_at=?,retired_by=?,retirement_reason=? WHERE outbox_id=? AND delivery_state=?").run(new Date(this.now()).toISOString(),actor,`Historical intention withdrawn; delivery unknown: ${input.reason}`,entry.outbox_id,entry.delivery_state);if(changed.changes!==1)refuse('historical_effect_drift','Concurrent disposition won');
   this.db.prepare("UPDATE outbox_historical_quarantines SET state='disposed' WHERE outbox_id=?").run(entry.outbox_id);
   const receipt={kind:'historical-disposition',rigId:input.rigId,operationId:input.operationId,action:input.action,outboxId:entry.outbox_id,actor,generation,lead:actors.lead,leadGeneration:input.leadGeneration,authorizationId:input.authorizationId,reason:input.reason,evidenceRef:input.evidenceRef,originalState:entry.delivery_state,originalRowHash:input.effect.rowHash,custodyHash:input.effect.custodyHash,originalProvenance:entry.identity_provenance??null,originalSender:entry.sender_session,originalDestination:entry.destination_session,originalAuditPointer:entry.audit_pointer,originalGuardHash:entry.guard_binding?historicalDigest(entry.guard_binding):null,deliveryConclusion:'unknown',queueMutations:0};this.record(input.rigId,input.operationId,'dispose',hash,receipt);return receipt;
  }).immediate());
 }
}
/** Startup uses the same exact authorized cohort before any recovery sends. */
export function applyHistoricalStartupRecovery(db:Database.Database,mode:string|undefined,manifestPath:string|undefined):unknown {
 const selected=mode??'deliver';if(selected!=='deliver'&&selected!=='observe')refuse('historical_startup_mode','Wake recovery mode must be deliver or observe');
 if(selected==='deliver'){if(manifestPath)refuse('historical_startup_manifest','Manifest requires observe mode');return null;}
 if(!manifestPath||!isAbsolute(manifestPath))refuse('historical_startup_manifest','Observe requires an absolute exact-cohort manifest');
 const st=lstatSync(manifestPath!);if(!st.isFile()||st.isSymbolicLink()||st.size>1048576)refuse('historical_startup_manifest','Regular manifest at most1MiB required');
 let parsed:unknown;try{parsed=JSON.parse(readFileSync(manifestPath!,'utf8'));}catch{refuse('historical_startup_manifest','Invalid cohort manifest');}
 const record=parsed!==null&&typeof parsed==='object'&&!Array.isArray(parsed)?parsed as Record<string,unknown>:null;
 if(!record)refuse('historical_startup_manifest','Typed cohort or versioned bundle required');
 const service=new HistoricalEffectDispositionService(db);
 if(record!.schema===undefined&&!Object.hasOwn(record!,'cohorts')){const p=record as unknown as HistoricalPlan;return service.quarantine('operator-agent@kernel',p.operatorGeneration,p);}
 if(record!.schema!=='historical-startup-bundle.v1'||Object.keys(record!).sort().join(',')!=='cohorts,schema'||!Array.isArray(record!.cohorts)||record!.cohorts.length<1||record!.cohorts.length>32)refuse('historical_startup_manifest','Exact versioned bundle requires1..32 independently authorized cohorts');
 const cohorts=record!.cohorts as HistoricalPlan[],effectIds=new Set<string>(),operations=new Set<string>(),authorizations=new Set<string>();let total=0;
 for(const p of cohorts){
  if(!p||typeof p!=='object'||Array.isArray(p)||['rigId','leadBatonId','leadGeneration','operatorGeneration','operationId','authorizationId'].some(k=>typeof (p as unknown as Record<string,unknown>)[k]!=='string')||!Number.isSafeInteger(p.expiresAt)||!Array.isArray(p.effects)||p.effects.length<1||p.effects.length>2000)refuse('historical_startup_manifest','Every bundle cohort retains exact typed bounded contract');
  if(p.operatorGeneration!==cohorts[0]!.operatorGeneration||operations.has(p.operationId)||authorizations.has(p.authorizationId))refuse('historical_startup_manifest','One current Operator and distinct operation/authorization identities required');
  operations.add(p.operationId);authorizations.add(p.authorizationId);total+=p.effects.length;
  for(const e of p.effects){if(!e||typeof e!=='object'||Array.isArray(e)||['outboxId','rowHash','custodyHash'].some(k=>typeof (e as unknown as Record<string,unknown>)[k]!=='string')||effectIds.has(e.outboxId))refuse('historical_startup_manifest','Every effect must belong to exactly one enumerated cohort');effectIds.add(e.outboxId);}
 }
 if(total>32000)refuse('historical_startup_manifest','Bundle total exceeds32000 enumerated effects');
 // Nested quarantine savepoints remain inside this single all-or-nothing admission.
 // No startup drain/transport exists before this function returns successfully.
 return db.transaction(()=>({schema:'historical-startup-bundle.v1',cohortCount:cohorts.length,effectCount:total,outboxMutations:0,deliveryConclusion:'unknown',receipts:cohorts.map(p=>service.quarantine('operator-agent@kernel',p.operatorGeneration,p))})).immediate();
}
