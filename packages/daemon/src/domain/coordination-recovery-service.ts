import { rotationLocalAddresses } from "./rotation-local-custody.js";
import type { QueueRepository } from "./queue-repository.js";
import { CoordinatorFenceError, digest, type CoordinatorToken } from "./coordinator-authority-service.js";
import type { ActivityEvidence, ArbitratedSeatState } from "./activity-taxonomy.js";

export interface CoordinationActivity { generation:string; identityVerified:boolean; identityObservedAt?:string|null; state:ArbitratedSeatState; witness:ActivityEvidence|null }
export interface CoordinationTask {
 key:string; packageKey:string; owner:string; action:string; deadline:number; body:string; recoveryFor?:string;
 predecessors:Array<{queueId:string;dispositionId:string}>;
 admission:{generation:string;configurationDigest:string;qualificationRef:string;capacityRef:string;effortRef:string;validUntil:number};
 /** Owner boundary affects this slice only. Recovery work is a separate admitted task. */
 boundary?:"owner-access"|"owner-credential"|"owner-material"|"owner-irreversible";
}
export interface CoordinationPlan { rigId:string; revision:string; operatorGeneration:string; stallMs:number; allowIdlePeerTransfer:boolean; allowUnavailablePeerTransfer?:boolean; acknowledgmentWindowMs?:number; refreshDispatchIdentity?:boolean; dispatchRestrictions?:Array<{session:string;generation:string;packageKeys:string[];validUntil:number;evidenceRef:string;checkpointDisposition?:'release-listed-packages'}>; tasks:CoordinationTask[] }
export interface CoordinationResult { key:string; state:string; queueId?:string; reason?:string; deadline:number; activityEvidence?:Record<string,unknown> }
const successfulReturn=(state:string,disposition:string|null):boolean=>!!disposition&&['done','handed-off'].includes(state);
/** Only the exact migration091 refusal is normalized, never arbitrary SQL failures. */
function heldDispatchCode(error:unknown):string|undefined {
 const e=error as {code?:string;message?:string};
 if(e.code==='SQLITE_CONSTRAINT_TRIGGER'&&e.message==='seat_dispatch_reserved')return 'seat_dispatch_reserved';
 return ['coordinator_resource_conflict','seat_dispatch_reserved'].includes(e.code??'')?e.code:undefined;
}
const fail=(code:string,message:string):never=>{throw new CoordinatorFenceError(code,message);};
/** Same-observation proof; public display/old hook timestamps cannot manufacture idle.
 * lastSwap records an observed handover, not an initial-generation admission.
 * After restart it is null: independently verified current identity/generation plus
 * fresh deciding evidence remain required. A present swap must match the managed
 * generation UUID and the witness cannot predate its invalidation watermark. */
export function coordinationIdle(sample:CoordinationActivity|null,generation:string,now:number):boolean {
 if(!sample||!sample.identityVerified||sample.generation!==generation||(sample.state.lastSwap!==null&&sample.state.lastSwap.generation!==generation)||sample.state.needsInput.count!==0)return false;
 const w=sample.witness,at=Date.parse(w?.observedAt??"");
 return !!w&&w.sessionName.length>0&&w.seatNodeId===sample.state.seatNodeId&&w.activity==="idle-at-prompt"&&sample.state.activity===w.activity&&sample.state.decidedBy===w.rung&&Number.isFinite(at)&&at<=now&&now-at<=3000&&(!sample.state.lastSwap||at>=Date.parse(sample.state.lastSwap.at));
}
/** Durable plans use the existing append-only operation store, with queue/resource
 * mutations in one SQLite transaction. Reconciliation never manufactures worker claims. */
export class CoordinationRecoveryService {
 constructor(private repo:QueueRepository,private activity:(session:string)=>CoordinationActivity|null,private now:()=>number=Date.now,private refreshIdentity?:(sessions:readonly string[])=>Promise<void>){}
 authorizeTerminalReturnSuccessor(actor:string,generation:string,input:{rigId:string;intakeQueueId:string;previousControlId:string;previousBodyHash:string;workerGeneration:string;holderGeneration:string;deadline:number;operationId:string}):{queueId:string} {
  return this.db.transaction(()=>{
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation)fail('coordination_operator_required','Current native Operator required');
   const id='native-return-successor:'+input.operationId,requestHash=digest(JSON.stringify({actor,generation,input}));
   if(!input.operationId||input.operationId.length>160)fail('coordination_return_successor_required','Bounded operation ID required');
   const saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-successor-authorization'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==requestHash)fail('coordination_return_successor_conflict','Frozen successor authorization differs');return {queueId:JSON.parse(saved.receipt).queueId};}
   const claimGeneration=(id:string)=>(this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(id) as {claimed_by_generation_uuid:string|null}|undefined)?.claimed_by_generation_uuid;
   const intake=this.repo.getById(input.intakeQueueId),previous=this.repo.getById(input.previousControlId),a=this.authority.get(input.rigId),plan=this.plan(input.rigId);
   const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(input.rigId,input.previousControlId) as {receipt:string}|undefined;
   const r=row?JSON.parse(row.receipt):null,b=previous?JSON.parse(previous.body):null,h=intake?JSON.parse(intake.body):null;
   const creation=this.db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(input.intakeQueueId) as any;
   const expectedIntake=h?'qitem-coordination-task-hold-'+digest(JSON.stringify({rigId:input.rigId,revision:h.planRevision,operatorGeneration:generation,packageKey:h.packageKey,reason:'terminal-return-duty-exhausted',queueId:input.previousControlId})).slice(0,24):null;
   if(!intake||intake.sourceSession!=='watchdog@system'||!creation||creation.state!=='pending'||creation.actor_session!=='watchdog@system'||creation.identity_provenance!=='system:operator-authorized-coordination'||(input.intakeQueueId!==expectedIntake&&!this.refreshedIntakeAuthorized(input.rigId,input.intakeQueueId,input.previousControlId,generation,h?.packageKey))||h.recipientGeneration!==generation||h.grantsAuthority!==false||h.deadline!==Date.parse(intake.expiresAt??'')||!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-plan' AND json_extract(receipt,'$.operatorGeneration')=?").get(input.rigId,'coordination-plan:'+h.planRevision,generation))fail('coordination_return_successor_required','Exact native exhaustion intake provenance required');
   if(!intake||intake.destinationSession!==actor||claimGeneration(input.intakeQueueId)!==generation||!['in-progress','blocked'].includes(intake.state)||!intake.expiresAt||Date.parse(intake.expiresAt)<=this.now()||h.action!=='resolve-exact-coordination-task-hold'||h.reason!=='terminal-return-duty-exhausted'||h.rigId!==input.rigId||h.retainedQueueId!==input.previousControlId||!r||!previous||!['done','failed','denied','canceled','handed-off'].includes(previous.state)||digest(previous.body)!==input.previousBodyHash||r.bodyHash!==input.previousBodyHash||r.workerGeneration!==input.workerGeneration||claimGeneration(input.previousControlId)!==input.workerGeneration||this.authority.generation(r.worker)!==input.workerGeneration||h.packageKey!==r.packageKey||!a||a.state!=='active'||a.lease_until<=this.now()||a.owner_generation!==input.holderGeneration||this.authority.generation(a.owner_session!)!==input.holderGeneration||!plan||plan.operatorGeneration!==generation||!Number.isSafeInteger(input.deadline)||input.deadline<=this.now()||input.deadline>this.now()+1200000)fail('coordination_return_successor_required','Exact claimed exhaustion intake, immutable prior native duty, current holder and finite authorization required');
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-successor-authorization' AND json_extract(receipt,'$.previousControlId')=?").get(input.rigId,input.previousControlId))fail('coordination_return_successor_conflict','This prior duty already has its one authorized successor');
   if(this.workerEffectDebt(r.worker)||this.db.prepare("SELECT 1 FROM outbox_entries WHERE audit_pointer=? AND delivery_state IN ('pending','sending','indeterminate')").get(input.previousControlId))fail('coordination_return_successor_unknown_effect','Reconcile unknown effects before authorizing a successor');
   const queueId='qitem-coordination-terminal-return-'+digest(input.rigId+':'+b.originalQueueId+':'+input.workerGeneration+':'+id).slice(0,24);
   const receipt={queueId,originalQueueId:b.originalQueueId,packageKey:r.packageKey,workerGeneration:input.workerGeneration,holderGeneration:input.holderGeneration,operatorGeneration:generation,previousControlId:input.previousControlId,previousBodyHash:input.previousBodyHash,intakeQueueId:input.intakeQueueId,intakeBodyHash:digest(intake!.body),expiresAt:input.deadline};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-successor-authorization',JSON.stringify(receipt),requestHash);
   this.repo.createNativeTerminalReturnDuty(a!.owner_session!,a!.owner_generation!,input.rigId,{qitemId:queueId,sourceSession:'watchdog@system',destinationSession:r.worker,expiresAt:new Date(input.deadline).toISOString(),body:JSON.stringify({...b,authorizationId:id,previousControlId:input.previousControlId,deadline:input.deadline,disposeContract:{rigId:input.rigId,packageKey:r.packageKey,dispositionId:'<new worker-authored typed-return queue item ID>'},required:'Claim this successor under your genuine identity; preserve prior reports. Create your durable JSON return to the original returnContract.destination with {packageKey,inputDigest,evidence:[{kind,ref}]} covering every original evidenceRequired kind, then invoke supported coordinator dispose with original packageKey and that return queue ID. Only then close this duty. No product writes, reruns, acceptance or historical reopening.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
   this.repo.stageWakeIntent(queueId,'watchdog@system',r.worker,'system:operator-authorized-coordination',true,input.workerGeneration);
   return {queueId};
  })();
 }
 /** A correction notice continues the exact claimed control; it creates no queue
  * assignment and cannot change its custody, original return or resource locks. */
 private terminalReturnContinuationContext(rigId:string,controlQueueId:string,bodyHash:string,workerGeneration:string,deadline:number,excludeEffect?:string,retirement=false) {
  const op=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,controlQueueId) as {receipt:string}|undefined;
  const control=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(controlQueueId) as any;
  const r=op?JSON.parse(op.receipt):null,b=control?JSON.parse(control.body):null,a=this.authority.get(rigId),plan=this.plan(rigId);
  const original=r?this.db.prepare("SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?").get(rigId,r.packageKey,r.originalQueueId) as any:null;
  const contract=original?JSON.parse(original.contract):null;
  if(!r||!control||r.bodyHash!==bodyHash||digest(control.body)!==bodyHash||control.source_session!=='watchdog@system'||control.destination_session!==r.worker||!['in-progress','blocked'].includes(control.state)||!workerGeneration||control.claimed_by_generation_uuid!==workerGeneration||r.workerGeneration!==workerGeneration||this.authority.generation(r.worker)!==workerGeneration||Date.parse(control.expires_at)!==r.expiresAt||!Number.isSafeInteger(deadline)||deadline<=this.now()||(!retirement&&(deadline>r.expiresAt||r.expiresAt<=this.now()))||(retirement&&r.expiresAt>this.now())||b.action!=='record-exact-native-terminal-return'||b.rigId!==rigId||b.originalQueueId!==r.originalQueueId||b.packageKey!==r.packageKey||b.recipientGeneration!==workerGeneration||!original||original.disposition_id||original.destination!==r.worker||original.claimed_by_generation_uuid!==workerGeneration||!['done','failed','denied','canceled','handed-off'].includes(original.state)||digest(original.body)!==original.body_hash||contract.destination!==r.worker||contract.bodyHash!==original.body_hash||b.inputDigest!==contract.inputDigest||JSON.stringify(b.returnContract)!==JSON.stringify(contract.returnContract)||!this.authority.terminalReturnResourcesRetained(rigId,r.packageKey,contract)||!a||a.state!=='active'||a.lease_until<=this.now()||this.authority.generation(a.owner_session)!==a.owner_generation||!plan||this.authority.generation('operator-agent@kernel')!==plan.operatorGeneration)fail('coordination_return_continuation_required','Exact immutable active claimed unexpired native return duty, original claimant and current live authority required; expired duties must use supported exhaustion recovery');
  const restriction=plan!.dispatchRestrictions?.find(scope=>scope.session===r.worker);
  if(restriction&&(restriction.generation!==workerGeneration||restriction.validUntil<=this.now()||!restriction.packageKeys.includes(r.packageKey)))fail('coordination_return_continuation_protected','Checkpoint scope excludes this native return continuation');
  for(const session of [r.worker,a!.owner_session,'operator-agent@kernel'])if(this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(session,session))fail('coordination_return_continuation_protected','Native seat reservation excludes this continuation');
  if(this.workerEffectDebt(r.worker,excludeEffect))fail('coordination_return_continuation_unknown_effect','Reconcile unknown effects before continuing native return custody');
  if(retirement){const observation=this.activity(r.worker),at=Date.parse(observation?.identityObservedAt??'');if(!Number.isFinite(at)||at>this.now()||this.now()-at>3000||!coordinationIdle(observation,workerGeneration,this.now()))fail('coordination_return_retirement_idle_required','Retirement requires fresh verified native idle; active or unknown workers remain protected');}
  return {r,control,receiptHash:digest(op!.receipt),a:a!,plan:plan!,contract};
 }
 private validContinuationReturn(queueId:string,worker:string,workerGeneration:string,packageKey:string,contract:any):boolean {
  const row=this.db.prepare('SELECT source_session,destination_session,minting_generation_uuid,body FROM queue_items WHERE qitem_id=?').get(queueId) as any;
  let body:any;try{body=row?JSON.parse(row.body):null;}catch{return false;}
  return !!row&&row.source_session===worker&&row.destination_session===contract.returnContract.destination&&row.minting_generation_uuid===workerGeneration&&body?.packageKey===packageKey&&body.inputDigest===contract.inputDigest&&Array.isArray(body.evidence)&&contract.returnContract.evidenceRequired.every((kind:string)=>body.evidence.some((e:any)=>e&&e.kind===kind&&typeof e.ref==='string'&&e.ref.length>0));
 }
 continueTerminalReturn(actor:string,generation:string,input:{rigId:string;controlQueueId:string;controlBodyHash:string;workerGeneration:string;deadline:number;dispositionId?:string}):{queueId:string;outboxId:string;deadline:number} {
  return this.db.transaction(()=>{
   const a=this.authority.get(input.rigId);
   if(!generation||this.authority.generation(actor)!==generation||!(actor==='operator-agent@kernel'||(a?.owner_session===actor&&a.owner_generation===generation)))fail('coordination_return_continuation_required','Genuine current Operator or live holder required');
   const id='native-return-continuation:'+input.controlQueueId,requestHash=digest(JSON.stringify({actor,generation,input}));
   const prior=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-continuation'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(prior){if(prior.request_hash!==requestHash)fail('coordination_return_continuation_conflict','This duty already has its one frozen correction notice');const r=JSON.parse(prior.receipt);return {queueId:r.controlQueueId,outboxId:r.outboxId,deadline:r.deadline};}
   const context=this.terminalReturnContinuationContext(input.rigId,input.controlQueueId,input.controlBodyHash,input.workerGeneration,input.deadline);
   if(input.deadline>this.now()+1200000)fail('coordination_return_continuation_required','Correction notice must expire within twenty minutes and the existing duty expiry');
   const {r,contract}=context;
   let dispositionId=input.dispositionId;
   if(dispositionId&&!this.validContinuationReturn(dispositionId,r.worker,input.workerGeneration,r.packageKey,contract))fail('coordination_return_continuation_required','Referenced return must be the original worker incarnation\'s genuine typed return under its admitted contract');
   if(!dispositionId){const returns=this.db.prepare('SELECT qitem_id FROM queue_items WHERE source_session=? AND destination_session=? AND minting_generation_uuid=? AND json_valid(body) ORDER BY rowid DESC').all(r.worker,contract.returnContract.destination,input.workerGeneration) as Array<{qitem_id:string}>;dispositionId=returns.find(row=>this.validContinuationReturn(row.qitem_id,r.worker,input.workerGeneration,r.packageKey,contract))?.qitem_id;}
   const outboxId=this.repo.stageNativeTerminalReturnContinuation({controlQueueId:input.controlQueueId,worker:r.worker,workerGeneration:input.workerGeneration,proofId:id,body:JSON.stringify({action:'continue-exact-native-terminal-return',controlQueueId:input.controlQueueId,originalQueueId:r.originalQueueId,deadline:input.deadline,grantsAuthority:false,disposeContract:{rigId:input.rigId,packageKey:r.packageKey,dispositionId:dispositionId??'<new worker-authored typed-return queue item ID>'},returnContract:contract.returnContract,required:'Continue the same already-claimed duty; do not claim new work or repeat product work. Reuse your genuine retained typed return if identified here. Otherwise author your JSON {packageKey,inputDigest,evidence:[{kind,ref}]} return to returnContract.destination. Submit exactly {rigId,packageKey,dispositionId} to rig coordinator dispose using that actual new return queue ID. Successful duty closure requires the resulting original disposition. Preserve original evidence, unknown effects, checkpoint scope and locks; report a supported refusal. This correction grants no acceptance or automatic disposal.'})});
   const effect=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(outboxId) as {body:string};
   const receipt={controlQueueId:input.controlQueueId,controlBodyHash:input.controlBodyHash,controlReceiptHash:context.receiptHash,worker:r.worker,workerGeneration:input.workerGeneration,actor,actorGeneration:generation,holder:context.a.owner_session,holderGeneration:context.a.owner_generation,epoch:context.a.epoch,operatorGeneration:context.plan.operatorGeneration,deadline:input.deadline,outboxId,outboxBodyHash:digest(effect.body),...(dispositionId?{dispositionId}:{})};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-continuation',JSON.stringify(receipt),requestHash);
   return {queueId:input.controlQueueId,outboxId,deadline:input.deadline};
  }).immediate();
 }
 private exactAccepted(rigId:string,queueId:string,dispositionId:string):boolean {return !!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(rigId,queueId,dispositionId);}
 private acceptedTaskHistory(rigId:string,t:CoordinationTask):boolean {
  const r=this.db.prepare("SELECT a.queue_id,a.disposition_id,a.body_hash,a.destination,q.body,q.state,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=?").get(rigId,t.packageKey) as any;
  const c=r?JSON.parse(r.contract):null;return !!r&&r.destination===t.owner&&r.body_hash===digest(t.body)&&digest(r.body)===r.body_hash&&c.destination===t.owner&&c.bodyHash===r.body_hash&&successfulReturn(r.state,r.disposition_id)&&this.exactAccepted(rigId,r.queue_id,r.disposition_id);
 }
 private dormantRecoveryHistory(rigId:string,t:CoordinationTask,prior:CoordinationPlan):boolean {
  const target=prior.tasks.find(parent=>parent.key===t.recoveryFor);if(!target||!this.acceptedTaskHistory(rigId,target)||this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,target.packageKey))return false;
  const id='qitem-coordination-'+digest(rigId+':'+t.packageKey).slice(0,24);
  return !this.db.prepare('SELECT 1 FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey)&&!this.db.prepare('SELECT 1 FROM coordinator_stage_assignments WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey)&&!this.db.prepare('SELECT 1 FROM coordinator_resources WHERE rig_id=? AND package_key=?').get(rigId,t.packageKey)&&!this.db.prepare("SELECT 1 FROM queue_items WHERE qitem_id=? AND state IN ('pending','in-progress','blocked')").get(id);
 }
 private lifecycleRecipientReady(rigId:string,recipient:string,packageKey:string,excludeEffect?:string):boolean {
  const plan=this.plan(rigId),scope=plan?.dispatchRestrictions?.find(r=>r.session===recipient);
  return !!plan&&(!scope||(scope.generation===this.authority.generation(recipient)&&scope.validUntil>this.now()&&scope.packageKeys.includes(packageKey)))&&!this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN (SELECT node_id FROM sessions WHERE session_name=?))").get(recipient,recipient)&&!this.workerEffectDebt(recipient,excludeEffect);
 }
 private lifecycleDuty(rigId:string,kind:'acceptance'|'recovery'|'materialization',packageKey:string,recipient:string,recipientGeneration:string,semanticKey:string,details:Record<string,unknown>):CoordinationResult {
  const a=this.authority.get(rigId)!,plan=this.plan(rigId)!;
  const queueId='qitem-coordination-lifecycle-'+digest(rigId+':'+kind+':'+semanticKey+':'+recipientGeneration+':'+a.epoch+':'+plan.revision).slice(0,24),existing=this.repo.getById(queueId);
  const deadline=existing?.expiresAt?Date.parse(existing.expiresAt):this.now()+1200000;
  if(existing&&this.lifecycleControlCompleted(queueId))return {key:kind+':'+packageKey,state:kind==='materialization'?'materialized':'owned-recovery',queueId,deadline};
  if(existing){return {key:kind+':'+packageKey,state:['pending','in-progress','blocked'].includes(existing.state)&&deadline>this.now()?'pending-native-'+kind:'held',queueId,...(!['pending','in-progress','blocked'].includes(existing.state)||deadline<=this.now()?{reason:'lifecycle-duty-exhausted'}:{}),deadline};}
  if(!this.lifecycleRecipientReady(rigId,recipient,packageKey))return {key:kind+':'+packageKey,state:'held',reason:'lifecycle-recipient-protected',deadline};
  let body=JSON.stringify({action:kind==='acceptance'?'accept-exact-return-or-own-recovery':kind==='recovery'?'own-exact-failed-return-recovery':'materialize-exact-admitted-frontier',rigId,packageKey,recipientGeneration,deadline,grantsAuthority:false,...details,required:kind==='acceptance'?'Claim this finite duty under current holder identity. Inspect the exact typed disposed return and required technical evidence. Use supported coordination-accept only when all classifier, qualification and independent review gates actually pass. An incomplete or unverified outcome requires distinct admitted, configured and genuinely picked-up recovery; record its exact active custody through coordination-lifecycle-recovery. Prose is not acceptance or owned recovery. Original acceptance alone releases the existing authorized frontier; never invent work or waive a gate.':'Claim this finite Operator intake. Inspect the immutable admitted contract, retained accepted predecessor references and current plan. Materialize this exact package into the existing plan with actual current qualification, capacity, effort, native generation/configuration and recovery evidence through coordination-plan. Preserve unchanged accepted history and dormant backup bytes. If scope or a protected gate prevents this, park the concrete boundary; this notice grants no admission, qualification, dispatch or acceptance.'});
  if(kind==='recovery'){const b=JSON.parse(body);b.required='Claim this finite recovery-only duty under the current native holder identity. Preserve the exact failed/denied/canceled original and its genuine typed disposition. Technical acceptance is forbidden for this original failure. Coordinate with the genuine current Operator to materialize a distinct admitted current recovery in the existing plan if absent; do not invent or reopen work. After actual worker pickup, record its exact active custody and evidence through coordination-lifecycle-recovery. Successful duty closure requires that verified distinct owned recovery; prose, a pending ticket and a failed-return acceptance attempt are not completion. Unknown effects, quiescence, current admission and qualifications remain protected.';body=JSON.stringify(b);}
  this.db.transaction(()=>{this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:recipient,expiresAt:new Date(deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:true});
   const receipt={kind,queueId,packageKey,recipient,recipientGeneration,bodyHash:digest(body),deadline,holder:a.owner_session,holderGeneration:a.owner_generation,epoch:a.epoch,operatorGeneration:plan.operatorGeneration,planRevision:plan.revision,...details};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'coordinator-lifecycle-control',JSON.stringify(receipt),digest(body));this.repo.stageCoordinatorLifecycleWake(queueId,recipient,recipientGeneration);
  })();
  return {key:kind+':'+packageKey,state:'pending-native-'+kind,queueId,deadline};
 }
 private centralLifecyclePass(rigId:string):CoordinationResult[] {
  const a=this.authority.get(rigId)!,plan=this.plan(rigId)!,result:CoordinationResult[]=[];
  const returned=this.db.prepare("SELECT a.*,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.disposition_id IS NOT NULL AND q.state IN ('done','handed-off','failed','denied','canceled')").all(rigId) as any[];
  for(const row of returned){
   const kind=successfulReturn(row.state,row.disposition_id)?'acceptance':'recovery';
   if(kind==='acceptance'&&this.exactAccepted(rigId,row.queue_id,row.disposition_id))continue;
   const contract=JSON.parse(row.contract);if(row.body_hash!==digest(row.body)||contract.destination!==row.destination||contract.bodyHash!==row.body_hash||!this.validContinuationReturn(row.disposition_id,row.destination,row.claimed_by_generation_uuid,row.package_key,contract)){result.push({key:kind+':'+row.package_key,state:'held',queueId:row.queue_id,reason:'lifecycle-return-contract-drift',deadline:this.now()+1200000});continue;}
   const returnedBody=(this.repo.getById(row.disposition_id)!).body;
   result.push(this.lifecycleDuty(rigId,kind,row.package_key,a.owner_session,a.owner_generation,row.queue_id+':'+row.disposition_id,{originalQueueId:row.queue_id,dispositionId:row.disposition_id,returnBodyHash:digest(returnedBody),assignmentBodyHash:row.body_hash,contractHash:digest(row.contract),...(kind==='acceptance'?{acceptContract:{rigId,packageKey:row.package_key,dispositionId:row.disposition_id,evidenceRef:'<actual technical acceptance evidence>'}}:{terminalState:row.state,recoveryContract:{rigId,dutyQueueId:'<this native recovery-only duty queue ID>',recoveryPackageKey:'<distinct admitted configured recovery package>',recoveryQueueId:'<actual currently claimed recovery assignment queue ID>',evidenceRef:'<actual recovery pickup evidence>'}})}));
  }
  const unplanned=this.db.prepare('SELECT package_key,contract,contract_hash FROM coordinator_packages p WHERE p.rig_id=? AND NOT EXISTS (SELECT 1 FROM coordinator_assignments a WHERE a.rig_id=p.rig_id AND a.package_key=p.package_key)').all(rigId) as any[];
  const accepted=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' ORDER BY operation_id").all(rigId) as Array<{receipt:string}>;
  const acceptedPredecessors=accepted.map(row=>{const r=JSON.parse(row.receipt);return {queueId:r.queueId,dispositionId:r.dispositionId,evidenceRef:r.evidenceRef};});
  for(const row of unplanned){if(plan.tasks.some(t=>t.packageKey===row.package_key))continue;
   result.push(this.lifecycleDuty(rigId,'materialization',row.package_key,'operator-agent@kernel',plan.operatorGeneration,row.package_key+':'+row.contract_hash,{contractHash:row.contract_hash,contract:JSON.parse(row.contract),acceptedPredecessors}));
  }
  return result;
 }
 private lifecycleControl(queueId:string):{rigId:string;receipt:any}|null {const row=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind='coordinator-lifecycle-control'").get(queueId) as {rig_id:string;receipt:string}|undefined;return row?{rigId:row.rig_id,receipt:JSON.parse(row.receipt)}:null;}
 isLifecycleControl(queueId:string):boolean {return this.lifecycleControl(queueId)!==null;}
 lifecycleControlCompleted(queueId:string):boolean {
  const op=this.lifecycleControl(queueId);if(!op)return false;const r=op.receipt;
  if(r.kind==='materialization'){const plan=this.plan(op.rigId),pkg=this.db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(op.rigId,r.packageKey) as any;return !!plan&&plan.operatorGeneration===this.authority.generation('operator-agent@kernel')&&pkg?.contract_hash===r.contractHash&&plan.tasks.some(t=>t.packageKey===r.packageKey&&this.admittedNow(t));}
  if(r.kind==='acceptance'&&this.exactAccepted(op.rigId,r.originalQueueId,r.dispositionId))return true;
  const recovery=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordinator-lifecycle-recovery'").get(op.rigId,'lifecycle-recovery:'+queueId) as {receipt:string}|undefined;
  return !!recovery&&this.validOwnedLifecycleRecovery(op.rigId,r,JSON.parse(recovery.receipt));
 }
 private validOwnedLifecycleRecovery(rigId:string,duty:any,recovery:any):boolean {
  const plan=this.plan(rigId),target=plan?.tasks.find(t=>t.packageKey===duty.packageKey),task=plan?.tasks.find(t=>t.packageKey===recovery.recoveryPackageKey),a=this.authority.get(rigId);
  const row=this.db.prepare('SELECT a.destination,a.body_hash,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?').get(rigId,recovery.recoveryPackageKey,recovery.recoveryQueueId) as any;
  return !!a&&a.state==='active'&&a.lease_until>this.now()&&a.epoch===recovery.epoch&&a.owner_session===recovery.actor&&a.owner_generation===recovery.generation&&this.authority.generation(recovery.actor)===recovery.generation&&!!plan&&plan.operatorGeneration===this.authority.generation('operator-agent@kernel')&&!!target&&!!task&&task.recoveryFor===target.key&&task.packageKey!==duty.packageKey&&this.admittedNow(task)&&!!row&&row.destination===task.owner&&row.state==='in-progress'&&row.claimed_by_generation_uuid===recovery.workerGeneration&&this.authority.generation(task.owner)===recovery.workerGeneration&&row.body_hash===digest(row.body)&&digest(task.body)===row.body_hash;
 }
 recordLifecycleRecovery(actor:string,generation:string,input:{rigId:string;dutyQueueId:string;recoveryPackageKey:string;recoveryQueueId:string;evidenceRef:string}):void {
  this.db.transaction(()=>{
   const op=this.lifecycleControl(input.dutyQueueId),q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(input.dutyQueueId) as any;
   const row=this.db.prepare('SELECT q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?').get(input.rigId,input.recoveryPackageKey,input.recoveryQueueId) as any;
   const receipt={actor,generation,epoch:this.authority.get(input.rigId)?.epoch,recoveryPackageKey:input.recoveryPackageKey,recoveryQueueId:input.recoveryQueueId,workerGeneration:row?.claimed_by_generation_uuid,evidenceRef:input.evidenceRef};
   if(!op||op.rigId!==input.rigId||!['acceptance','recovery'].includes(op.receipt.kind)||!this.validLifecycleControlWake('watchdog@system',actor,input.dutyQueueId)||!q||digest(q.body)!==op.receipt.bodyHash||q.destination_session!==actor||q.claimed_by_generation_uuid!==generation||!['in-progress','blocked'].includes(q.state)||Date.parse(q.expires_at)<=this.now()||typeof input.evidenceRef!=='string'||!input.evidenceRef.trim()||!this.validOwnedLifecycleRecovery(input.rigId,op.receipt,receipt))fail('coordination_lifecycle_recovery_required','Exact current holder claim and distinct admitted genuinely active recovery required; prose cannot close acceptance duty');
   const id='lifecycle-recovery:'+input.dutyQueueId,prior=this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(input.rigId,id) as {receipt:string}|undefined;
   if(prior){if(prior.receipt!==JSON.stringify(receipt))fail('coordination_lifecycle_recovery_conflict','Frozen recovery disposition differs');return;}
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'coordinator-lifecycle-recovery',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
  }).immediate();
 }
 validLifecycleControlWake(source:string|undefined,destination:string,queueId:string):boolean {
  if(source!=='watchdog@system')return false;const op=this.lifecycleControl(queueId);if(!op)return false;const r=op.receipt,a=this.authority.get(op.rigId),plan=this.plan(op.rigId),q=this.repo.getById(queueId);
  if(!q||q.sourceSession!==source||q.destinationSession!==destination||r.recipient!==destination||r.recipientGeneration!==this.authority.generation(destination)||!['pending','in-progress','blocked'].includes(q.state)||q.expiresAt===null||Date.parse(q.expiresAt)!==r.deadline||r.deadline<=this.now()||digest(q.body)!==r.bodyHash||!a||a.state!=='active'||a.lease_until<=this.now()||a.epoch!==r.epoch||a.owner_session!==r.holder||a.owner_generation!==r.holderGeneration||this.authority.generation(r.holder)!==r.holderGeneration||!plan||plan.revision!==r.planRevision||plan.operatorGeneration!==r.operatorGeneration||this.authority.generation('operator-agent@kernel')!==r.operatorGeneration||!this.lifecycleRecipientReady(op.rigId,destination,r.packageKey,'wake-intent-'+queueId)||this.lifecycleControlCompleted(queueId))return false;
  if(r.kind==='materialization'){const pkg=this.db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(op.rigId,r.packageKey) as any;return pkg?.contract_hash===r.contractHash&&(!plan.tasks.some(t=>t.packageKey===r.packageKey))&&r.acceptedPredecessors.every((ref:any)=>this.exactAccepted(op.rigId,ref.queueId,ref.dispositionId));}
  const original=this.db.prepare('SELECT a.*,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(op.rigId,r.originalQueueId,r.packageKey) as any;
  const returned=this.repo.getById(r.dispositionId);return !!original&&(r.kind==='recovery'?['failed','denied','canceled'].includes(original.state)&&original.state===r.terminalState:successfulReturn(original.state,original.disposition_id))&&original.disposition_id===r.dispositionId&&original.body_hash===r.assignmentBodyHash&&digest(original.body)===r.assignmentBodyHash&&digest(original.contract)===r.contractHash&&!!returned&&digest(returned.body)===r.returnBodyHash&&this.validContinuationReturn(r.dispositionId,original.destination,original.claimed_by_generation_uuid,r.packageKey,JSON.parse(original.contract));
 }
 private currentReturnObserver(jobId:string,operatorGeneration:string):boolean {
  return !!this.db.prepare("SELECT 1 FROM watchdog_jobs WHERE job_id=? AND policy='coordinator-continuity' AND state='active' AND target_session='operator-agent@kernel' AND registered_by_session='operator-agent@kernel' AND registered_by_generation_uuid=?").get(jobId,operatorGeneration);
 }
 private exhaustionIntakeProvenance(rigId:string,queueId:string,controlQueueId:string,packageKey:string):boolean {
  const q=this.repo.getById(queueId),h=q?JSON.parse(q.body):null,creation=this.db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(queueId) as any;
  const expected=h?'qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:h.planRevision,operatorGeneration:h.recipientGeneration,packageKey,reason:'terminal-return-duty-exhausted',queueId:controlQueueId})).slice(0,24):null;
  return !!q&&q.sourceSession==='watchdog@system'&&q.destinationSession==='operator-agent@kernel'&&!!creation&&creation.state==='pending'&&creation.actor_session==='watchdog@system'&&creation.identity_provenance==='system:operator-authorized-coordination'&&queueId===expected&&h.action==='resolve-exact-coordination-task-hold'&&h.reason==='terminal-return-duty-exhausted'&&h.rigId===rigId&&h.packageKey===packageKey&&h.retainedQueueId===controlQueueId&&h.grantsAuthority===false&&Number.isSafeInteger(h.deadline)&&h.deadline===Date.parse(q.expiresAt??'')&&!!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-plan' AND json_extract(receipt,'$.operatorGeneration')=?").get(rigId,'coordination-plan:'+h.planRevision,h.recipientGeneration);
 }
 private refreshedIntakeAuthorized(rigId:string,queueId:string,controlQueueId:string,operatorGeneration:string,packageKey:string):boolean {
  const q=this.repo.getById(queueId),h=q?JSON.parse(q.body):null;
  const saved=h?.authorizationId?this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-intake-authorization'").get(rigId,h.authorizationId) as {receipt:string}|undefined:undefined;
  if(!saved)return false;const r=JSON.parse(saved.receipt),a=this.authority.get(rigId),plan=this.plan(rigId),prior=this.repo.getById(r.previousIntakeQueueId),control=this.repo.getById(controlQueueId);
  const record=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,controlQueueId) as {receipt:string}|undefined;
  const original=this.db.prepare('SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(rigId,r.originalQueueId,packageKey) as any;
  return !!q&&queueId===r.queueId&&h.authorizationId===r.authorizationId&&h.rigId===rigId&&h.packageKey===packageKey&&h.retainedQueueId===controlQueueId&&h.recipientGeneration===operatorGeneration&&h.grantsAuthority===false&&h.deadline===r.deadline&&digest(q.body)===r.intakeBodyHash&&!!prior&&digest(prior.body)===r.previousIntakeBodyHash&&this.exhaustionIntakeProvenance(rigId,r.previousIntakeQueueId,controlQueueId,packageKey)&&!!control&&digest(control.body)===r.controlBodyHash&&!!record&&digest(record.receipt)===r.controlReceiptHash&&!!original&&!original.disposition_id&&digest(JSON.stringify(original))===r.originalHash&&original.claimed_by_generation_uuid===r.workerGeneration&&this.authority.generation(original.destination)===r.workerGeneration&&!!a&&a.state==='active'&&a.lease_until>this.now()&&a.epoch===r.epoch&&a.owner_session===r.holder&&a.owner_generation===r.holderGeneration&&this.authority.generation(r.holder)===r.holderGeneration&&!!plan&&plan.revision===r.planRevision&&plan.operatorGeneration===operatorGeneration&&this.authority.generation('operator-agent@kernel')===operatorGeneration&&r.operatorGeneration===operatorGeneration&&this.currentReturnObserver(r.observerJobId,operatorGeneration)&&this.lifecycleRecipientReady(rigId,'operator-agent@kernel',packageKey)&&this.lifecycleRecipientReady(rigId,r.holder,packageKey);
 }
 refreshTerminalReturnIntake(actor:string,generation:string,input:{rigId:string;previousIntakeQueueId:string;controlQueueId:string;controlBodyHash:string;workerGeneration:string;holderGeneration:string;observerJobId:string;deadline:number;operationId:string}):{queueId:string;deadline:number} {
  return this.db.transaction(()=>{
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation)fail('coordination_operator_required','Genuine current Operator required for administrative exhaustion intake');
   if(!input.operationId||input.operationId.length>160)fail('coordination_return_intake_required','Bounded operation ID required');
   const id='native-return-intake:'+input.operationId,requestHash=digest(JSON.stringify({actor,generation,input})),saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-intake-authorization'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==requestHash)fail('coordination_return_intake_conflict','Frozen administrative intake authorization differs');const r=JSON.parse(saved.receipt);return {queueId:r.queueId,deadline:r.deadline};}
   if(!Number.isSafeInteger(input.deadline)||input.deadline<=this.now()||input.deadline>this.now()+1200000)fail('coordination_return_intake_required','New administrative intake must expire within twenty minutes');
   const c=this.terminalReturnContinuationContext(input.rigId,input.controlQueueId,input.controlBodyHash,input.workerGeneration,input.deadline,undefined,true),previous=this.repo.getById(input.previousIntakeQueueId);
   const claimed=this.db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(input.previousIntakeQueueId) as any;
   if(c.a.owner_generation!==input.holderGeneration||!this.currentReturnObserver(input.observerJobId,generation)||!previous||!previous.expiresAt||Date.parse(previous.expiresAt)>this.now()||!this.exhaustionIntakeProvenance(input.rigId,input.previousIntakeQueueId,input.controlQueueId,c.r.packageKey)||claimed?.claimed_by_generation_uuid!==JSON.parse(previous!.body).recipientGeneration)fail('coordination_return_intake_required','Exact expired genuinely claimed runtime intake, registered current Operator observer and live holder required');
   if(this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-intake-authorization' AND json_extract(receipt,'$.previousIntakeQueueId')=?").get(input.rigId,input.previousIntakeQueueId))fail('coordination_return_intake_conflict','Expired intake already has its one finite authorized successor');
   if(this.workerEffectDebt(actor)||this.workerEffectDebt(c.a.owner_session))fail('coordination_return_intake_unknown_effect','Reconcile current Operator and holder unknown effects before administrative intake');
   if(!this.lifecycleRecipientReady(input.rigId,actor,c.r.packageKey)||!this.lifecycleRecipientReady(input.rigId,c.a.owner_session,c.r.packageKey))fail('coordination_return_intake_protected','Operator checkpoint scope excludes administrative intake');
   const queueId='qitem-coordination-task-hold-'+digest(input.rigId+':'+input.previousIntakeQueueId+':'+id).slice(0,24),h=JSON.parse(previous!.body),body=JSON.stringify({...h,authorizationId:id,previousIntakeQueueId:input.previousIntakeQueueId,planRevision:c.plan.revision,recipientGeneration:generation,deadline:input.deadline,nextAction:'Genuinely claim this finite administrative intake. The registered current Operator observer can stage only failure/cancellation of the expired native return duty by its original worker. This grants no product dispatch, disposal, acceptance, lock release, admission or plan rewrite.'});
   const original=this.db.prepare('SELECT a.destination,a.body_hash,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.queue_id=? AND a.package_key=?').get(input.rigId,c.r.originalQueueId,c.r.packageKey);
   const receipt={authorizationId:id,queueId,previousIntakeQueueId:input.previousIntakeQueueId,previousIntakeBodyHash:digest(previous!.body),controlQueueId:input.controlQueueId,controlBodyHash:input.controlBodyHash,controlReceiptHash:c.receiptHash,originalQueueId:c.r.originalQueueId,originalHash:digest(JSON.stringify(original)),packageKey:c.r.packageKey,workerGeneration:input.workerGeneration,holder:c.a.owner_session,holderGeneration:c.a.owner_generation,epoch:c.a.epoch,operatorGeneration:generation,observerJobId:input.observerJobId,planRevision:c.plan.revision,deadline:input.deadline,intakeBodyHash:digest(body)};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-intake-authorization',JSON.stringify(receipt),requestHash);
   this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:actor,expiresAt:new Date(input.deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false});return {queueId,deadline:input.deadline};
  }).immediate();
 }
 private exactRetirementIntake(rigId:string,intakeQueueId:string,controlQueueId:string,operatorGeneration:string,packageKey:string):string {
  const q=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(intakeQueueId) as any;
  const h=q?JSON.parse(q.body):null,creation=this.db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id LIMIT 1').get(intakeQueueId) as any;
  const expected=h?'qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:h.planRevision,operatorGeneration,packageKey,reason:'terminal-return-duty-exhausted',queueId:controlQueueId})).slice(0,24):null;
  if(!q||q.source_session!=='watchdog@system'||q.destination_session!=='operator-agent@kernel'||q.claimed_by_generation_uuid!==operatorGeneration||!['in-progress','blocked'].includes(q.state)||!creation||creation.state!=='pending'||creation.actor_session!=='watchdog@system'||creation.identity_provenance!=='system:operator-authorized-coordination'||(intakeQueueId!==expected&&!this.refreshedIntakeAuthorized(rigId,intakeQueueId,controlQueueId,operatorGeneration,packageKey))||h.action!=='resolve-exact-coordination-task-hold'||h.reason!=='terminal-return-duty-exhausted'||h.rigId!==rigId||h.packageKey!==packageKey||h.retainedQueueId!==controlQueueId||h.recipientGeneration!==operatorGeneration||h.grantsAuthority!==false||!Number.isSafeInteger(h.deadline)||h.deadline!==Date.parse(q.expires_at)||h.deadline<=this.now()||!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='coordination-plan' AND json_extract(receipt,'$.operatorGeneration')=?").get(rigId,'coordination-plan:'+h.planRevision,operatorGeneration))fail('coordination_return_retirement_required','Current Operator must genuinely claim the exact finite runtime exhaustion intake before retirement');
  return digest(q.body);
 }
 retireExpiredTerminalReturn(actor:string,generation:string,input:{rigId:string;intakeQueueId:string;controlQueueId:string;controlBodyHash:string;workerGeneration:string;deadline:number}):{queueId:string;outboxId:string;deadline:number} {
  return this.db.transaction(()=>{
   if(actor!=='operator-agent@kernel'||!generation||this.authority.generation(actor)!==generation)fail('coordination_operator_required','Genuine current Operator authorizes expired-control retirement');
   const id='native-return-retirement:'+input.controlQueueId,requestHash=digest(JSON.stringify({actor,generation,input}));
   const saved=this.db.prepare("SELECT receipt,request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-retirement'").get(input.rigId,id) as {receipt:string;request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==requestHash)fail('coordination_return_retirement_conflict','This expired duty already has its one finite retirement notice');const r=JSON.parse(saved.receipt);return {queueId:r.controlQueueId,outboxId:r.outboxId,deadline:r.deadline};}
   if(input.deadline>this.now()+1200000)fail('coordination_return_retirement_required','Retirement notice must expire within twenty minutes');
   const c=this.terminalReturnContinuationContext(input.rigId,input.controlQueueId,input.controlBodyHash,input.workerGeneration,input.deadline,undefined,true);
   const intakeBodyHash=this.exactRetirementIntake(input.rigId,input.intakeQueueId,input.controlQueueId,generation,c.r.packageKey);
   const outboxId=this.repo.stageNativeTerminalReturnContinuation({controlQueueId:input.controlQueueId,worker:c.r.worker,workerGeneration:input.workerGeneration,proofId:id,body:JSON.stringify({action:'retire-expired-native-terminal-return',controlQueueId:input.controlQueueId,originalQueueId:c.r.originalQueueId,deadline:input.deadline,grantsAuthority:false,allowedClosureStates:['failed','canceled'],required:'This is a finite failure-only retirement notice. Your original duty has expired; do not execute expired work, dispose product scope, redo work or release locks. Under your own genuine native identity, record failed or canceled for ONLY controlQueueId through the supported queue update with an honest expiry reason. Preserve the original product claim, all typed returns, evidence and effects. Current Operator can then use the existing exact successor API. No automatic cancellation, acceptance or expiry extension is granted.'})});
   const effect=this.db.prepare('SELECT body FROM outbox_entries WHERE outbox_id=?').get(outboxId) as {body:string};
   const receipt={retirement:true,controlQueueId:input.controlQueueId,controlBodyHash:input.controlBodyHash,controlReceiptHash:c.receiptHash,intakeQueueId:input.intakeQueueId,intakeBodyHash,worker:c.r.worker,workerGeneration:input.workerGeneration,actor,actorGeneration:generation,holder:c.a.owner_session,holderGeneration:c.a.owner_generation,epoch:c.a.epoch,operatorGeneration:c.plan.operatorGeneration,deadline:input.deadline,outboxId,outboxBodyHash:digest(effect.body)};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'native-terminal-return-retirement',JSON.stringify(receipt),requestHash);
   return {queueId:input.controlQueueId,outboxId,deadline:input.deadline};
  }).immediate();
 }
 private retireForAuthorizedObserver(rigId:string,operatorGeneration:string,result:CoordinationResult[]):CoordinationResult[] {
  for(const held of result){
   if(held.reason!=='terminal-return-duty-exhausted'||!held.queueId)continue;
   const control=this.repo.getById(held.queueId);if(!control||!['in-progress','blocked'].includes(control.state)||!control.expiresAt||Date.parse(control.expiresAt)>this.now())continue;
   const record=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-control'").get(rigId,held.queueId) as {receipt:string}|undefined;if(!record)continue;const r=JSON.parse(record.receipt);
   const plan=this.plan(rigId)!;let intakeQueueId='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:plan.revision,operatorGeneration,packageKey:r.packageKey,reason:'terminal-return-duty-exhausted',queueId:held.queueId})).slice(0,24);
   const refreshed=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='native-terminal-return-intake-authorization' AND json_extract(receipt,'$.controlQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,held.queueId) as {receipt:string}|undefined;if(refreshed)intakeQueueId=JSON.parse(refreshed.receipt).queueId;
   const saved=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-retirement'").get(rigId,'native-return-retirement:'+held.queueId) as {receipt:string}|undefined;
   if(saved){const receipt=JSON.parse(saved.receipt);held.reason=receipt.deadline>this.now()?'native-retirement-notice-staged':'native-retirement-notice-exhausted';continue;}
   try{const staged=this.retireExpiredTerminalReturn('operator-agent@kernel',operatorGeneration,{rigId,intakeQueueId,controlQueueId:held.queueId,controlBodyHash:digest(control.body),workerGeneration:r.workerGeneration,deadline:this.now()+1200000});held.reason='native-retirement-notice-staged';held.deadline=staged.deadline;}catch(error){if(!(error instanceof CoordinatorFenceError))throw error;held.reason=error.code;}
  }
  return result;
 }
 /** Read-only proof shared by outbox selection and the final managed-send seam. */
 validTerminalReturnContinuationWake(source:string|undefined,destination:string,proofId:string):boolean {
  if(source!=='watchdog@system')return false;
  const op=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind IN ('native-terminal-return-continuation','native-terminal-return-retirement')").get(proofId) as {rig_id:string;receipt:string}|undefined;
  if(!op)return false;const r=JSON.parse(op.receipt);
  try{
   const c=this.terminalReturnContinuationContext(op.rig_id,r.controlQueueId,r.controlBodyHash,r.workerGeneration,r.deadline,r.outboxId,r.retirement===true);
   const effect=this.db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(r.outboxId) as any;
   if(r.retirement&&this.exactRetirementIntake(op.rig_id,r.intakeQueueId,r.controlQueueId,r.operatorGeneration,c.r.packageKey)!==r.intakeBodyHash)return false;
   return destination===r.worker&&c.receiptHash===r.controlReceiptHash&&c.a.epoch===r.epoch&&c.a.owner_session===r.holder&&c.a.owner_generation===r.holderGeneration&&c.plan.operatorGeneration===r.operatorGeneration&&this.authority.generation(r.actor)===r.actorGeneration&&(r.actor==='operator-agent@kernel'||r.actor===c.a.owner_session)&&!!effect&&effect.sender_session===source&&effect.destination_session===destination&&effect.audit_pointer===r.controlQueueId&&['pending','sending'].includes(effect.delivery_state)&&digest(effect.body)===r.outboxBodyHash&&(!r.dispositionId||this.validContinuationReturn(r.dispositionId,r.worker,r.workerGeneration,c.r.packageKey,c.contract));
  }catch(error){if(error instanceof CoordinatorFenceError)return false;throw error;}
 }
 async probeWorker(actor:string,generation:string,input:{rigId:string;worker:string}):Promise<{worker:string;generation:string;configurationDigest:string|null;observedAt:number;identityVerified:boolean;idle:boolean;observation:CoordinationActivity|null;grantsAuthority:false}> {
  const authorize=()=>{
   const a=this.authority.get(input.rigId),current=this.authority.generation(actor);
   if(!generation||current!==generation||!(actor==='operator-agent@kernel'||(a?.state==='active'&&a.owner_session===actor&&a.owner_generation===generation&&a.lease_until>this.now())))fail('coordination_worker_probe_required','Genuine current Operator or active holder required');
   if(!this.db.prepare('SELECT 1 FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.session_name=? AND n.rig_id=?').get(input.worker,input.rigId)||!this.authority.generation(input.worker))fail('coordination_worker_probe_required','Existing native worker in exact rig required');
  };
  authorize();const before=this.authority.generation(input.worker)!;
  await this.refreshIdentity?.([input.worker]);authorize();
  if(this.authority.generation(input.worker)!==before)fail('coordination_worker_probe_changed','Worker changed during observation');
  const observation=this.activity(input.worker),now=this.now(),identityAt=Date.parse(observation?.identityObservedAt??'');
  const identityVerified=observation?.identityVerified===true&&observation.generation===before&&Number.isFinite(identityAt)&&identityAt<=now&&now-identityAt<=3000;
  return {worker:input.worker,generation:before,configurationDigest:this.configurationDigest(input.worker),observedAt:now,identityVerified,idle:identityVerified&&coordinationIdle(observation,before,now),observation,grantsAuthority:false};
 }
 async refreshActivity(rigId:string):Promise<void> {const plan=this.plan(rigId);if(plan?.refreshDispatchIdentity===true){const coordinators=JSON.parse(this.authority.get(rigId)?.coordinators??'[]') as string[];const retained=this.db.prepare("SELECT DISTINCT a.destination FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.disposition_id IS NULL AND q.state IN ('done','failed','denied','canceled','handed-off')").all(rigId) as Array<{destination:string}>;await this.refreshIdentity?.([...new Set([...coordinators,...plan.tasks.map(t=>t.owner),...retained.map(r=>r.destination)])]);}}
 private get authority(){return this.repo.coordinatorAuthority;}
 private get db(){return this.authority.db;}
 configure(actor:string,generation:string,plan:CoordinationPlan):CoordinationPlan {
  return this.db.transaction(()=>{
   if(actor!=="operator-agent@kernel"||this.authority.generation(actor)!==generation||plan.operatorGeneration!==generation)fail("coordination_operator_required","Current genuine Operator configures recovery");
   if(!this.authority.get(plan.rigId))fail("coordinator_not_enabled","Explicit legacy enrollment/admission required");
   if(!Number.isSafeInteger(plan.stallMs)||plan.stallMs<10000||plan.stallMs>3600000||typeof plan.allowIdlePeerTransfer!=="boolean"||!plan.revision||!plan.tasks.length||new Set(plan.tasks.map(t=>t.key)).size!==plan.tasks.length||new Set(plan.tasks.map(t=>t.packageKey)).size!==plan.tasks.length)fail("coordination_invalid_plan","Unique immutable tasks/packages required");
   if(plan.acknowledgmentWindowMs!==undefined&&(!Number.isSafeInteger(plan.acknowledgmentWindowMs)||plan.acknowledgmentWindowMs<10000||plan.acknowledgmentWindowMs>900000))fail("coordination_invalid_ack_window","Acknowledgment window must be 10 seconds to 15 minutes");
   if(plan.allowUnavailablePeerTransfer!==undefined&&typeof plan.allowUnavailablePeerTransfer!=='boolean')fail('coordination_invalid_unavailable_optin','Unavailable-owner transfer requires strict explicit boolean');
   if(plan.refreshDispatchIdentity!==undefined&&typeof plan.refreshDispatchIdentity!=='boolean')fail('coordination_invalid_identity_refresh','Identity refresh requires strict explicit boolean');
   if(plan.dispatchRestrictions!==undefined){
    if(!Array.isArray(plan.dispatchRestrictions)||new Set(plan.dispatchRestrictions.map(r=>r.session)).size!==plan.dispatchRestrictions.length)fail('coordination_invalid_dispatch_scope','Unique explicit dispatch restrictions required');
    for(const r of plan.dispatchRestrictions){
     if(r.checkpointDisposition!==undefined&&r.checkpointDisposition!=='release-listed-packages')fail('coordination_invalid_checkpoint_disposition','Explicit listed-package checkpoint disposition required');
     if(!r.session||r.generation!==this.authority.generation(r.session)||!Array.isArray(r.packageKeys)||!r.packageKeys.length||new Set(r.packageKeys).size!==r.packageKeys.length||r.packageKeys.some(key=>!plan.tasks.some(t=>t.owner===r.session&&t.packageKey===key))||!Number.isFinite(r.validUntil)||r.validUntil<=this.now()||typeof r.evidenceRef!=='string'||!r.evidenceRef.trim())fail('coordination_invalid_dispatch_scope','Exact current owner, admitted packages, future expiry and evidence required');
    }
   }
   const prior=this.plan(plan.rigId);
   const stable=(t:CoordinationTask)=>JSON.stringify({...t,admission:undefined,deadline:undefined});
   const keys=new Set(plan.tasks.map(t=>t.key));
   for(const t of plan.tasks){
    const old=prior?.tasks.find(previous=>previous.key===t.key),historical=!!old&&(this.acceptedTaskHistory(plan.rigId,old)||this.dormantRecoveryHistory(plan.rigId,old,prior!));
    if(historical&&JSON.stringify(old)!==JSON.stringify(t))fail('coordination_history_rewrite_refused','Accepted task and dormant backup history must retain full task, admission and deadline bytes');
    if(!t.key||!t.action.trim()||!Number.isFinite(t.deadline)||(t.deadline<=this.now()&&!prior?.tasks.some(old=>old.key===t.key&&stable(old)===stable(t)))||!t.body||!Array.isArray(t.predecessors))fail("coordination_invalid_task","Concrete action, future deadline, predecessors and exact body required");
    const ad=t.admission;
    if(!historical&&(!ad||ad.generation!==this.authority.generation(t.owner)||ad.configurationDigest!==this.configurationDigest(t.owner)||!ad.qualificationRef||!ad.capacityRef||!ad.effortRef||!Number.isFinite(ad.validUntil)||ad.validUntil<=this.now()))fail('coordination_current_admission_required','Exact current generation/configuration, qualification/capacity/effort evidence and expiry required');
    if(t.boundary&&!['owner-access','owner-credential','owner-material','owner-irreversible'].includes(t.boundary))fail("coordination_invalid_boundary","Unknown boundary");
    const row=this.db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?").get(plan.rigId,t.packageKey) as {contract:string}|undefined;
    if(!row)fail("coordination_package_not_admitted","Every task including recovery needs explicit admission");
    const c=JSON.parse(row!.contract);
    if(c.destination!==t.owner||c.bodyHash!==digest(t.body)||!c.returnContract?.evidenceRequired?.length)fail("coordination_package_mismatch","Exact owner/body and attributed return contract required");
    for(const pred of t.predecessors)if(!pred.queueId||!pred.dispositionId)fail("coordination_invalid_predecessor","Exact disposition receipt required");
    if(t.recoveryFor&&(!keys.has(t.recoveryFor)||t.recoveryFor===t.key))fail("coordination_invalid_recovery","Recovery must name another exact plan task");
    if(!t.recoveryFor&&!t.boundary&&!plan.tasks.some(r=>r.recoveryFor===t.key))fail("coordination_recovery_required","Every ordinary task needs a distinct admitted recovery task with concrete owner/action/deadline");
   }
   const byQueue=new Map(plan.tasks.map(t=>['qitem-coordination-'+digest(plan.rigId+':'+t.packageKey).slice(0,24),t]));
   const visiting=new Set<string>(),visited=new Set<string>();
   const visit=(t:CoordinationTask):void=>{if(visiting.has(t.key))fail('coordination_dependency_cycle','Recovery/dependency graph cannot cycle');if(visited.has(t.key))return;visiting.add(t.key);for(const pred of t.predecessors){const parent=byQueue.get(pred.queueId);if(parent)visit(parent);}if(t.recoveryFor)visit(plan.tasks.find(other=>other.key===t.recoveryFor)!);visiting.delete(t.key);visited.add(t.key);};
   for(const t of plan.tasks)visit(t);
   const reaches=(t:CoordinationTask,target:string,seen=new Set<string>()):boolean=>{
    if(t.key===target)return true;if(seen.has(t.key))return false;seen.add(t.key);
    return t.predecessors.some(pred=>{const parent=byQueue.get(pred.queueId);return !!parent&&reaches(parent,target,seen);});
   };
   for(const t of plan.tasks)if(t.recoveryFor&&reaches(t,t.recoveryFor))fail('coordination_recovery_deadlock','Recovery cannot depend directly or transitively on its blocked task');
   const id=`coordination-plan:${plan.revision}`;
   const old=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(plan.rigId,id) as {receipt:string}|undefined;
   if(old){if(old.receipt!==JSON.stringify(plan))fail("coordination_plan_conflict","Frozen revision cannot change");return JSON.parse(old.receipt);}
   // Do not replace unresolved contracts with a new plan and silently orphan work.
   if(prior&&prior.tasks.some(t=>!plan.tasks.some(n=>n.key===t.key&&stable(n)===stable(t))))fail("coordination_plan_obligation_lost","Retain all existing tasks unchanged in successor revision");
   this.db.prepare("INSERT INTO coordinator_operations VALUES (?,?,?,?,?)").run(plan.rigId,id,"coordination-plan",JSON.stringify(plan),digest(JSON.stringify({actor,generation,plan})));
   for(const r of plan.dispatchRestrictions??[]){
    if(r.checkpointDisposition!=='release-listed-packages')continue;
    const queueId='qitem-coordination-scope-'+digest(plan.rigId+':'+generation+':'+JSON.stringify(r)).slice(0,24);
    if(!this.repo.getById(queueId)){this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:r.session,expiresAt:new Date(r.validUntil).toISOString(),body:JSON.stringify({action:'checkpoint-scope-disposition',operator:actor,operatorGeneration:generation,rigId:plan.rigId,recipientGeneration:r.generation,packageKeys:r.packageKeys,evidenceRef:r.evidenceRef,validUntil:r.validUntil,instruction:'Current genuine Operator releases post-checkpoint quiescence ONLY for the listed already-admitted packages under the cited disposition. Rederive actual native identity/generation and read the disposition; claim and close this control notice honestly, then consume the existing matching assignment when present. Preserve quiescence for all other work, rotation and baton takeover. No duplicate assignment, source edit, historical-effect replay or acceptance waiver. This notice is not worker pickup or technical acceptance.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
     this.repo.stageWakeIntent(queueId,actor,r.session,'system:operator-authorized-coordination',true,r.generation);
    }
   }
   this.recordProgress(plan.rigId);
   return plan;
  }).immediate();
 }
 plan(rigId:string):CoordinationPlan|null {
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-plan' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;
  return rows?JSON.parse(rows.receipt):null;
 }
 reconcile(actor:string,generation:string,rigId:string):CoordinationResult[] {
  return this.db.transaction(()=>{
   const a=this.authority.get(rigId),plan=this.plan(rigId);
   if(!a||!plan)fail("coordination_plan_required","Explicit current recovery plan required");
   if(this.authority.generation("operator-agent@kernel")!==plan!.operatorGeneration)fail("coordination_operator_retired","Reauthorize plan after Operator generation change");
   if(actor!==a!.owner_session||generation!==a!.owner_generation||this.authority.generation(actor)!==generation||a!.state!=="active"||a!.lease_until<=this.now())fail("coordinator_retired","Only reconciled current holder may dispatch");
   const token:CoordinatorToken={rigId,epoch:a!.epoch,generation};
   const lifecycle=this.centralLifecyclePass(rigId),result:CoordinationResult[]=[];
   this.recordProgress(rigId);
   for(const t of plan!.tasks){
    const dispatchHold=this.dispatchScopeHold(plan!,t);
    const assigned=this.db.prepare("SELECT a.queue_id,a.disposition_id,q.state,q.claimed_by_generation_uuid,q.destination_session FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,t.packageKey) as {queue_id:string;disposition_id:string|null;state:string;claimed_by_generation_uuid:string|null;destination_session:string}|undefined;
    if(assigned){
     if(!dispatchHold&&!t.boundary&&this.predecessorsReady(rigId,t)&&assigned.state==='pending'&&!assigned.claimed_by_generation_uuid&&!assigned.disposition_id&&this.admittedNow(t)&&!this.workerEffectDebt(t.owner)&&coordinationIdle(this.activity(t.owner),this.authority.generation(t.owner)??'',this.now()))this.repo.stageCoordinatorAssignmentWake({rigId,epoch:a!.epoch,generation,actor,queueId:assigned.queue_id,recipient:t.owner,recipientGeneration:t.admission.generation,now:this.now()});
     const picked=assigned.state==='in-progress'&&assigned.claimed_by_generation_uuid===this.authority.generation(t.owner)&&assigned.destination_session===t.owner;
     const semanticRecovery=this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,t.packageKey)??false;
     const accepted=successfulReturn(assigned.state,assigned.disposition_id)&&!!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(rigId,assigned.queue_id,assigned.disposition_id);
     const state=accepted?'accepted':semanticRecovery?'recovery-required:semantic-incomplete':successfulReturn(assigned.state,assigned.disposition_id)?'returned-awaiting-acceptance':picked?'picked-up':assigned.state==='pending'?'pending-pickup':`recovery-required:${assigned.state}`;
     result.push({key:t.key,state,queueId:assigned.queue_id,deadline:t.deadline,...(!assigned.disposition_id&&this.now()>t.deadline?{reason:'deadline-exceeded: concrete recovery owner/action remains '+t.owner+' / '+t.action}:{})});continue;
    }
    if(dispatchHold){result.push({key:t.key,state:'held',reason:dispatchHold,deadline:t.deadline});continue;}
    if(t.recoveryFor){
     const target=plan!.tasks.find(other=>other.key===t.recoveryFor)!;
     const targetAssignment=this.db.prepare("SELECT q.state,a.disposition_id FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,target.packageKey) as {state:string;disposition_id:string|null}|undefined;
     const targetGen=this.authority.generation(target.owner);
     // An undispatched dependent is not a failed obligation. Ordinary owner
     // custody/busy work is scheduling, not evidence that this package failed.
     const targetReady=this.predecessorsReady(rigId,target);
     const occupied=!!this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked')").get(...rotationLocalAddresses(this.db,target.owner));
     const observed=this.activity(target.owner);
     const busyAt=Date.parse(observed?.witness?.observedAt??'');
     const knownBusy=observed?.identityVerified&&observed.generation===targetGen&&observed.state.activity==='working'&&observed.witness?.activity==='working'&&observed.witness.rung===observed.state.decidedBy&&observed.witness.seatNodeId===observed.state.seatNodeId&&Number.isFinite(busyAt)&&busyAt<=this.now()&&this.now()-busyAt<=3000;
     const semanticRecovery=this.authority.runtimeOutcomeAssessment?.requiresRecovery(rigId,target.packageKey)??false;
     const needsRecovery=!target.boundary&&(!targetAssignment||!successfulReturn(targetAssignment.state,targetAssignment.disposition_id)||semanticRecovery)&&((semanticRecovery&&!!targetAssignment)||(!!targetAssignment&&(['blocked','failed','denied','canceled'].includes(targetAssignment.state)||this.now()>target.deadline))||(!targetAssignment&&targetReady&&!occupied&&!knownBusy&&(this.now()>target.deadline||this.workerEffectDebt(target.owner)||!this.admittedNow(target)||!targetGen||!coordinationIdle(observed,targetGen,this.now()))));
     if(!needsRecovery){result.push({key:t.key,state:'held',reason:'recovery-not-needed',deadline:t.deadline});continue;}
    }
    if(t.boundary){result.push({key:t.key,state:'held',reason:t.boundary,deadline:t.deadline});continue;}
    const ready=this.predecessorsReady(rigId,t);
    if(!ready){result.push({key:t.key,state:'held',reason:'predecessor-disposition',deadline:t.deadline});continue;}
    if(this.workerEffectDebt(t.owner)){result.push({key:t.key,state:'held',reason:'uncertain-worker-effect',deadline:t.deadline});continue;}
    if(!this.admittedNow(t)){result.push({key:t.key,state:'held',reason:'current-admission-required',deadline:t.deadline});continue;}
    const gen=this.authority.generation(t.owner);
    const sample=this.activity(t.owner),observedNow=this.now();
    if(!gen||!coordinationIdle(sample,gen,observedNow)){
     const age=(at:string|null|undefined)=>{const ms=Date.parse(at??'');return Number.isFinite(ms)?observedNow-ms:null;};
     result.push({key:t.key,state:'held',reason:'fresh-activity-required',deadline:t.deadline,activityEvidence:{expectedGeneration:gen,generation:sample?.generation??null,identityVerified:sample?.identityVerified??false,identityAgeMs:age(sample?.identityObservedAt),activity:sample?.state.activity??null,decidedBy:sample?.state.decidedBy??null,needsInputCount:sample?.state.needsInput.count??null,witnessActivity:sample?.witness?.activity??null,witnessRung:sample?.witness?.rung??null,witnessAgeMs:age(sample?.witness?.observedAt),witnessSeatMatches:!!sample?.witness&&sample.witness.seatNodeId===sample.state.seatNodeId,swapGeneration:sample?.state.lastSwap?.generation??null,witnessPredatesSwap:!!sample?.witness&&!!sample.state.lastSwap&&Date.parse(sample.witness.observedAt)<Date.parse(sample.state.lastSwap.at)}});continue;
    }
    // An unrelated queue claim is an exclusive worker obligation, even while idle.
    if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked')").get(...rotationLocalAddresses(this.db,t.owner))){result.push({key:t.key,state:'held',reason:'existing-worker-custody',deadline:t.deadline});continue;}
    const queueId=`qitem-coordination-${digest(rigId+':'+t.packageKey).slice(0,24)}`;
    // A retained pre-ledger row is history, not a new assignment. Never recreate
    // it or manufacture ownership; keep this slice accountable and continue others.
    const retained=this.repo.getById(queueId);
    if(retained){result.push({key:t.key,state:'held',queueId,reason:retained.destinationSession===t.owner&&digest(retained.body)===digest(t.body)?'existing-queue-without-assignment':'deterministic-queue-conflict',deadline:t.deadline});continue;}
    try {
     this.db.transaction(()=>this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:t.owner,body:t.body,dispatch:{token,packageKey:t.packageKey},identityProvenance:'system:operator-authorized-coordination',nudge:true}))();
    } catch(error) {
     const code=heldDispatchCode(error);
     if(!code)throw error;
     result.push({key:t.key,state:'held',reason:code,deadline:t.deadline});continue;
    }
    result.push({key:t.key,state:'pending-pickup',queueId,deadline:t.deadline});
   }
   // A terminal UI state is not an attributed return. Detect retained scope even
   // when that completed assignment is absent from the latest dispatch plan.
   const missingReturns=this.db.prepare("SELECT a.package_key,a.queue_id,a.destination,a.body_hash,q.body,q.ts_updated,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.disposition_id IS NULL AND q.state IN ('done','failed','denied','canceled','handed-off')").all(rigId) as Array<{package_key:string;queue_id:string;destination:string;body_hash:string;body:string;ts_updated:string;claimed_by_generation_uuid:string|null;contract:string}>;
   for(const missing of missingReturns){
    const current=this.authority.generation(missing.destination),contract=JSON.parse(missing.contract);
    const restriction=plan!.dispatchRestrictions?.find(r=>r.session===missing.destination);
    const scopeHeld=restriction&&(restriction.generation!==current||restriction.validUntil<=this.now()||!restriction.packageKeys.includes(missing.package_key));
    const reason=!current||current!==missing.claimed_by_generation_uuid?'terminal-return-incarnation-changed':digest(missing.body)!==missing.body_hash||contract.destination!==missing.destination||contract.bodyHash!==missing.body_hash||!this.authority.terminalReturnResourcesRetained(rigId,missing.package_key,contract)?'terminal-return-contract-drift':scopeHeld?'checkpoint-quiescence':this.workerEffectDebt(missing.destination)?'uncertain-worker-effect':null;
    if(reason){result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId:missing.queue_id,reason,deadline:Date.parse(missing.ts_updated)+1200000});continue;}
    let queueId='qitem-coordination-terminal-return-'+digest(rigId+':'+missing.queue_id+':'+current).slice(0,24);const deadline=this.now()+1200000;
    const latest=this.db.prepare("SELECT q.qitem_id,q.body,o.receipt FROM coordinator_operations o JOIN queue_items q ON q.qitem_id=o.operation_id WHERE o.rig_id=? AND o.kind='native-terminal-return-control' AND json_extract(o.receipt,'$.originalQueueId')=? AND json_extract(o.receipt,'$.workerGeneration')=? ORDER BY q.ts_created DESC,q.rowid DESC LIMIT 1").get(rigId,missing.queue_id,current) as {qitem_id:string;body:string;receipt:string}|undefined;
    if(latest&&JSON.parse(latest.receipt).bodyHash===digest(latest.body))queueId=latest.qitem_id;
    const existing=this.repo.getById(queueId);if(existing&&(!['pending','in-progress','blocked'].includes(existing.state)||!existing.expiresAt||Date.parse(existing.expiresAt)<=this.now())){result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId,reason:'terminal-return-duty-exhausted',deadline:existing.expiresAt?Date.parse(existing.expiresAt):Date.parse(missing.ts_updated)+1200000});continue;}
    if(existing&&['in-progress','blocked'].includes(existing.state)&&!JSON.parse(existing.body).disposeContract&&!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=? AND kind='native-terminal-return-continuation'").get(rigId,'native-return-continuation:'+queueId)){
     try{this.continueTerminalReturn(actor,generation,{rigId,controlQueueId:queueId,controlBodyHash:digest(existing.body),workerGeneration:current!,deadline:Math.min(Date.parse(existing.expiresAt!),this.now()+1200000)});}catch(error){if(!(error instanceof CoordinatorFenceError))throw error;result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId,reason:error.code,deadline:Date.parse(existing.expiresAt!)});continue;}
    }
    try{
     this.db.transaction(()=>{if(!this.repo.getById(queueId)){this.repo.createNativeTerminalReturnDuty(actor,generation,rigId,{qitemId:queueId,sourceSession:'watchdog@system',destinationSession:missing.destination,expiresAt:new Date(deadline).toISOString(),body:JSON.stringify({action:'record-exact-native-terminal-return',rigId,packageKey:missing.package_key,originalQueueId:missing.queue_id,recipientGeneration:current,inputDigest:contract.inputDigest,returnContract:contract.returnContract,disposeContract:{rigId,packageKey:missing.package_key,dispositionId:'<new worker-authored typed-return queue item ID>'},deadline,grantsAuthority:false,required:'Claim this bounded return duty under your genuine current native identity. Reuse retained evidence from your own original assignment; author its exact typed durable return and use supported coordinator disposition. Close this duty with the actual receipt. Do not reopen or redo work, fabricate evidence, release locks directly, accept work, merge, deploy, or claim another incarnation’s results. Preserve uncertainty and explicit scope limits; report a concrete supported API refusal to the current Lead.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});this.repo.stageWakeIntent(queueId,'watchdog@system',missing.destination,'system:operator-authorized-coordination',true,current!);}})();
     result.push({key:'terminal-return:'+missing.package_key,state:'pending-native-terminal-return',queueId,deadline:Date.parse(this.repo.getById(queueId)!.expiresAt!)});
    }catch(error){const code=heldDispatchCode(error);if(!code)throw error;result.push({key:'terminal-return:'+missing.package_key,state:'held',queueId:missing.queue_id,reason:'terminal-return-'+code,deadline:Date.parse(missing.ts_updated)+1200000});}
   }
   // Configuration/effect holds need real recovery custody, not only a diagnostic
   // string. Control intake grants no dispatch, acceptance or history authority.
   for(const held of result){
    if(held.state!=='held'||!['current-admission-required','uncertain-worker-effect','existing-queue-without-assignment','deterministic-queue-conflict','terminal-return-incarnation-changed','terminal-return-contract-drift','terminal-return-duty-exhausted','terminal-return-seat_dispatch_reserved','terminal-return-coordinator_resource_conflict'].includes(held.reason??''))continue;
    const missing=missingReturns.find(m=>'terminal-return:'+m.package_key===held.key);
    const task=plan!.tasks.find(t=>t.key===held.key)??(missing?{packageKey:missing.package_key,owner:missing.destination}:undefined);if(!task)continue;
    const queueId='qitem-coordination-task-hold-'+digest(JSON.stringify({rigId,revision:plan!.revision,operatorGeneration:plan!.operatorGeneration,packageKey:task.packageKey,reason:held.reason,queueId:held.queueId??null})).slice(0,24);
    try {this.db.transaction(()=>{if(!this.repo.getById(queueId)){
     const deadline=this.now()+1200000;
     this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',expiresAt:new Date(deadline).toISOString(),body:JSON.stringify({action:'resolve-exact-coordination-task-hold',rigId,planRevision:plan!.revision,packageKey:task.packageKey,taskOwner:task.owner,reason:held.reason,retainedQueueId:held.queueId??null,recipientGeneration:plan!.operatorGeneration,deadline,grantsAuthority:false,returnPath:a!.owner_session,...(held.reason==='terminal-return-duty-exhausted'?{nextAction:'For an expired still-claimed control: claim this intake genuinely; the registered Operator observer can stage finite failure-only retirement when native idle and effects are proven. The original worker records failed/canceled; then use coordination-return-successor. Never extend expiry or cancel on the worker behalf.'}:{}),required:'Claim this bounded recovery and inspect the exact current task, native identity and custody. Repair an expired admission only from current qualified evidence. Reconcile uncertain effects through supported disposition without assuming delivery or retrying unknown effects. For a retained pre-ledger row, preserve it and have the current Lead define a distinct admitted follow-up contract when needed; never forge an assignment or reopen terminal history. Return supported resolution evidence or a named protected boundary. Reconcile eligible independent product frontier afterward; this control item grants no acceptance, product qualification, checkpoint release or dispatch authority.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
     this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,plan!.operatorGeneration);
    }})();}catch(error){
     const code=heldDispatchCode(error);if(!code)throw error;
     const receipt={rigId,packageKey:task.packageKey,reason:code,owner:'operator-agent@kernel',action:'Resolve native Operator reservation before exact recovery intake',grantsAuthority:false};
     this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-intake-hold:'+digest(queueId+':'+code),'coordination-intake-hold',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
    }
   }
   result.push(...lifecycle);
   // Observation ages are diagnostics, not new work or a new reconciliation state.
   const stableResult=result.map(({activityEvidence,...state})=>state);
   const operationId=`coordination-reconcile:${digest(JSON.stringify({revision:plan!.revision,epoch:a!.epoch,result:stableResult}))}`;
   this.db.prepare("INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)").run(rigId,operationId,'coordination-reconcile',JSON.stringify(result),digest(JSON.stringify({actor,generation})));
   this.recordProgress(rigId);
   return result;
  }).immediate();
 }
 configurationDigest(session:string):string|null {
  const row=this.db.prepare('SELECT n.id,n.runtime,n.model,n.profile,n.codex_config_profile,n.cwd FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE s.session_name=? ORDER BY s.id DESC LIMIT 1').get(session);
  return row?digest(JSON.stringify(row)):null;
 }
 private dispatchScopeHold(plan:CoordinationPlan,t:CoordinationTask):string|null {
  const r=plan.dispatchRestrictions?.find(r=>r.session===t.owner);if(!r)return null;
  if(r.generation!==this.authority.generation(t.owner))return 'dispatch-scope-generation';
  if(r.validUntil<=this.now())return 'dispatch-scope-expired';
  return r.packageKeys.includes(t.packageKey)?null:'checkpoint-quiescence';
 }
 private workerEffectDebt(session:string,excludeEffect?:string):boolean {
  const addresses=rotationLocalAddresses(this.db,session);
  const rig=this.db.prepare('SELECT n.rig_id FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE s.session_name=? ORDER BY s.id DESC LIMIT 1').get(session) as {rig_id:string}|undefined;
  const effects=this.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state NOT IN ('delivered','failed','retired') AND (sender_session IN (?,?) OR destination_session IN (?,?))").all(...addresses,...addresses) as Record<string,unknown>[];
  return effects.some(row=>row.outbox_id!==excludeEffect&&(!rig||!this.authority.isAdoptedHistoryContained(rig.rig_id,row)));
 }
 private admittedNow(t:CoordinationTask):boolean {
  return t.admission.validUntil>this.now()&&t.admission.generation===this.authority.generation(t.owner)&&t.admission.configurationDigest===this.configurationDigest(t.owner);
 }
 /** Returns require coordinator disposition; worker completion alone is not acceptance. */
 accept(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string,evidenceRef:string):void {
  this.db.transaction(()=>{
   const a=this.authority.get(rigId);
   if(!a||a.owner_session!==actor||a.owner_generation!==generation||a.state!=='active'||this.authority.generation(actor)!==generation||a.lease_until<=this.now())fail('coordinator_retired','Current holder must accept exact return');
   this.authority.runtimeOutcomeAssessment?.assertAcceptance(rigId,packageKey);
   if(!evidenceRef)fail('coordination_acceptance_required','Attributed technical disposition evidence required');
   const row=this.db.prepare("SELECT a.queue_id,a.disposition_id,q.state FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?").get(rigId,packageKey) as {queue_id:string;disposition_id:string|null;state:string}|undefined;
   if(!row||row.disposition_id!==dispositionId||!['done','handed-off'].includes(row.state))fail('coordination_return_required','Exact successful attributed released return required');
   const receipt={queueId:row!.queue_id,dispositionId,actor,generation,evidenceRef};
   const id='coordination-accept:'+packageKey;
   const prior=this.db.prepare('SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(rigId,id) as {receipt:string}|undefined;
   if(prior){if(JSON.parse(prior.receipt).dispositionId!==dispositionId)fail('coordination_acceptance_conflict','Accepted result cannot change');return;}
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,id,'coordination-accept',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
   this.recordProgress(rigId);
   const plan=this.plan(rigId);if(plan&&plan.operatorGeneration===this.authority.generation('operator-agent@kernel'))this.reconcile(actor,generation,rigId);
  }).immediate();
 }
 private progressDigest(rigId:string):string {
  // Message text, wake consumption, repeated notes and nudge timestamps do not count.
  const rows=this.db.prepare("SELECT a.package_key,a.queue_id,a.disposition_id,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? ORDER BY a.package_key").all(rigId);
  const accepted=this.db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' ORDER BY operation_id").all(rigId);
  return digest(JSON.stringify({rows,accepted}));
 }
 private recordProgress(rigId:string):{digest:string;at:number} {
  const value=this.progressDigest(rigId);
  const last=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-progress' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;
  const prior=last?JSON.parse(last.receipt) as {digest:string;at:number}:null;
  if(prior?.digest===value)return prior;
  const next={digest:value,at:this.now()};
  this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-progress:'+digest(JSON.stringify(next)+JSON.stringify(this.db.prepare('SELECT count(*) n FROM coordinator_operations').get())), 'coordination-progress',JSON.stringify(next),value);
  return next;
 }
 canTransferIdle(rigId:string,recipient:string,recipientGeneration:string,progressDigest:string):boolean {
  const a=this.authority.get(rigId);
  if(this.plan(rigId)?.dispatchRestrictions?.some(r=>r.session===recipient))return false;
  if(!a||this.progressDigest(rigId)!==progressDigest||this.authority.generation(recipient)!==recipientGeneration)return false;
  if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked') AND qitem_id<>?").get(...rotationLocalAddresses(this.db,recipient),a.baton_id))return false;
  return coordinationIdle(this.activity(recipient),recipientGeneration,this.now())&&coordinationIdle(this.activity(a.owner_session),a.owner_generation,this.now());
 }
 canTransferUnavailable(rigId:string,recipient:string,recipientGeneration:string):boolean {
  if(this.plan(rigId)?.dispatchRestrictions?.some(r=>r.session===recipient))return false;
  const a=this.authority.get(rigId),plan=this.plan(rigId);if(!a||!plan||plan.allowUnavailablePeerTransfer!==true||this.authority.generation(recipient)!==recipientGeneration)return false;
  if(plan.tasks.some(t=>t.admission.generation!==this.authority.generation(t.owner)||t.admission.configurationDigest!==this.configurationDigest(t.owner)||!Number.isFinite(t.admission.validUntil)||t.admission.validUntil<=this.now()))return false;
  if(this.db.prepare("SELECT 1 FROM queue_items WHERE destination_session IN (?,?) AND state IN ('pending','in-progress','blocked') AND qitem_id<>?").get(...rotationLocalAddresses(this.db,recipient),a.baton_id))return false;
  return coordinationIdle(this.activity(recipient),recipientGeneration,this.now());
 }
 private stageCoordinatorRecovery(rigId:string,epoch:number,operatorGeneration:string,action:string,reason:string,deadline:number,heldHistoryAdmission?:Record<string,unknown>):string {
  const recoveryKey=digest((heldHistoryAdmission?JSON.stringify(heldHistoryAdmission):'')+rigId+':'+epoch+':'+operatorGeneration+':'+action+':'+reason);
  const previous=this.db.prepare("SELECT qitem_id,state,expires_at FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.recoveryKey')=? ORDER BY rowid DESC LIMIT 1").get(recoveryKey) as {qitem_id:string;state:string;expires_at:string|null}|undefined;
  const queueId=previous&&['pending','in-progress','blocked'].includes(previous.state)&&(!heldHistoryAdmission||(!!previous.expires_at&&Date.parse(previous.expires_at)>this.now()))?previous.qitem_id:'qitem-coordination-recovery-'+digest(recoveryKey+':'+(previous?.qitem_id??'initial')).slice(0,24);
  if(!this.repo.getById(queueId))this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',expiresAt:heldHistoryAdmission?new Date(deadline).toISOString():undefined,body:JSON.stringify({...(heldHistoryAdmission?{heldHistoryAdmission}:{}),action,reason,recoveryKey,previousQueueId:previous?.qitem_id??null,rigId,epoch,recipientGeneration:operatorGeneration,deadline,nextAction:action==='restore-current-held-history-binding'?'Have the actual current Lead author a finite held-history recovery task; current Operator must genuinely claim it and use supported held-history-recovery-bind with exact retained hashes. Preserve UNKNOWN effects, existing worker custody and all checkpoint limits. This notice is not a recovery admission, binding, takeover or acceptance. Return exact proof or a concrete protected boundary.':'Revalidate exact current native holder/Peer, plan and baton custody. A living holder may perform supported voluntary transfer; admit fresh idle or positive-absence recovery only when proven. Repair expired admissions or uncertain effects through their existing supported paths. Preserve workers and return a concrete protected boundary when evidence is unknown; do not fabricate extension or acknowledgment.',returnPath:{queueId,actor:'operator-agent@kernel',required:'Claim exact recovery item and return supported evidence or concrete protected boundary. Do not declare pickup/ACK or native absence from a role label.'}}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
  if(heldHistoryAdmission){const opId='held-recovery-notice:'+queueId;if(!this.db.prepare('SELECT 1 FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(rigId,opId))this.authority.recordHeldRecoveryAdmission(rigId,queueId,digest(this.repo.getById(queueId)!.body),heldHistoryAdmission);}
  this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,operatorGeneration);return queueId;
 }
 /** Separately admitted feedback preserves the exact existing parent custody. */
 continueCustody(actor:string,generation:string,input:{rigId:string;epoch:number;parentPackageKey:string;parentQueueId:string;workerGeneration:string;feedbackPackageKey:string;body:string}):{queueId:string} {
  return this.db.transaction(()=>{

   const a=this.authority.get(input.rigId);
   if(!a||a.owner_session!==actor||a.owner_generation!==generation||a.epoch!==input.epoch||this.authority.generation(actor)!==generation||a.state!=='active'||a.lease_until<=this.now())fail('coordinator_retired','Only active current coordinator may continue custody');
   this.authority.assertCurrentOwner(actor,{rigId:input.rigId,epoch:input.epoch,generation});
   const parent=this.db.prepare("SELECT a.destination,a.body_hash,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.queue_id=?").get(input.rigId,input.parentPackageKey,input.parentQueueId) as {destination:string;body_hash:string;body:string;state:string;claimed_by_generation_uuid:string}|undefined;
   if(!parent||!['blocked','in-progress'].includes(parent.state)||parent.body_hash!==digest(parent.body)||parent.claimed_by_generation_uuid!==input.workerGeneration||this.authority.generation(parent.destination)!==input.workerGeneration)fail('coordination_parent_custody_required','Exact current claimed parent obligation required');
   const row=this.db.prepare('SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?').get(input.rigId,input.feedbackPackageKey) as {contract:string}|undefined;
   const c=row?JSON.parse(row.contract):null;let packet;try{packet=JSON.parse(input.body);}catch{fail('coordination_invalid_feedback','Typed feedback required');}
   if(!c||c.destination!==parent!.destination||c.bodyHash!==digest(input.body)||c.resources.length!==0||c.returnContract.destination!==actor||!c.returnContract.evidenceRequired.length||packet.action!=='reconcile-existing-custody'||packet.parentQueueId!==input.parentQueueId||packet.parentPackageKey!==input.parentPackageKey||typeof packet.instruction!=='string'||!packet.instruction.trim()||Object.keys(packet).some(k=>!['action','parentQueueId','parentPackageKey','instruction'].includes(k)))fail('coordination_feedback_not_admitted','Exact separately Operator-admitted feedback and current return path required');
   const queueId='qitem-coordination-feedback-'+digest(input.rigId+':'+input.feedbackPackageKey).slice(0,24);
   const existing=this.db.prepare('SELECT queue_id FROM coordinator_assignments WHERE rig_id=? AND package_key=?').get(input.rigId,input.feedbackPackageKey) as {queue_id:string}|undefined;
   if(existing)return {queueId:existing.queue_id};
   this.repo.createWithinTransaction({qitemId:queueId,sourceSession:actor,destinationSession:parent!.destination,body:input.body,dispatch:{token:{rigId:input.rigId,epoch:input.epoch,generation},packageKey:input.feedbackPackageKey},identityProvenance:'transport:v1',nudge:true});return {queueId};
  }).immediate();
 }
 private predecessorsReady(rigId:string,t:CoordinationTask):boolean {return t.predecessors.every(p=>!!this.db.prepare("SELECT 1 FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.queue_id=? AND a.disposition_id=? AND q.state IN ('done','handed-off') AND EXISTS (SELECT 1 FROM coordinator_operations o WHERE o.rig_id=a.rig_id AND o.kind='coordination-accept' AND json_extract(o.receipt,'$.queueId')=a.queue_id AND json_extract(o.receipt,'$.dispositionId')=a.disposition_id)").get(rigId,p.queueId,p.dispositionId));}
 async deliverCommitted():Promise<void>{await this.repo.drainPendingWakeIntents();}
 /** Existing Operator-registered coordinator watchdog is the only unattended actor.
  * It creates real queue intents through the same path, never acknowledgment. */
 supervise(rigId:string,jobId:string):CoordinationResult[]|null {
  return this.db.transaction(()=>{
   const job=this.db.prepare('SELECT * FROM watchdog_jobs WHERE job_id=?').get(jobId) as {policy:string;state:string;registered_by_session:string;registered_by_generation_uuid:string;target_session:string}|undefined;
   const plan=this.plan(rigId),a=this.authority.get(rigId);
   if(!plan||!a)return null;
   if(!job||job.policy!=='coordinator-continuity'||job.state!=='active'||job.target_session!=='operator-agent@kernel'||job.registered_by_session!=='operator-agent@kernel'||job.registered_by_generation_uuid!==plan.operatorGeneration||this.authority.generation('operator-agent@kernel')!==plan.operatorGeneration)fail('coordination_observer_not_authorized','Current Operator job and plan required');
   const progress=this.recordProgress(rigId);
   if((a.state==='active'||a.state==='recovery')&&a.lease_until<=this.now()&&plan.allowUnavailablePeerTransfer===true&&this.authority.hasFreshUnavailableOwner(rigId)){
    const peer=(JSON.parse(a.coordinators) as string[]).find(s=>s!==a.owner_session),peerGeneration=peer?this.authority.generation(peer):null;
    try{
     if(!peer||!peerGeneration)fail('coordinator_ineligible_recipient','Actual current Peer required');
     return this.db.transaction(()=>{const transferred=this.authority.transferObservedUnavailable(jobId,{rigId,expectedEpoch:a.epoch,expectedOwner:a.owner_session,expectedOwnerGeneration:a.owner_generation,planRevision:plan.revision,recipient:peer!,recipientGeneration:peerGeneration!,leaseMs:plan.acknowledgmentWindowMs??300000});
     const queueId='qitem-coordination-peer-'+digest(rigId+':'+transferred.epoch).slice(0,24);
     this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:peer!,body:JSON.stringify({action:'reconcile-transferred-baton',rigId,epoch:transferred.epoch,batonId:transferred.baton_id,deadline:transferred.lease_until,recipientGeneration:peerGeneration,required:'Read actual obligations and native identity; claim notice, then genuine Peer acknowledgment claims canonical baton. No product dispatch until acknowledgment.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
     this.repo.stageWakeIntent(queueId,'watchdog@system',peer!,'system:operator-authorized-coordination',true,peerGeneration!);
     return [{key:'coordinator',state:'pending-peer-acknowledgment',queueId,reason:'fresh-native-unavailable-owner',deadline:transferred.lease_until}];})();
    }catch(error){const code=heldDispatchCode(error)??(error instanceof CoordinatorFenceError?error.code:undefined);if(!code)throw error;const deadline=this.now()+(code==='coordinator_held_history_recovery_required'?Math.min(plan.stallMs,1200000):plan.stallMs);const queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'recover-unavailable-coordinator',code,deadline,code==='coordinator_held_history_recovery_required'?this.authority.heldRecoveryAdmission(rigId,jobId):undefined);return [{key:'coordinator',state:'held',queueId,reason:code,deadline}];}
   }
   if(a.state==='active'&&this.now()-progress.at>=plan.stallMs&&plan.allowIdlePeerTransfer&&!this.authority.hasFreshUnavailableOwner(rigId)){
    const peers=(JSON.parse(a.coordinators) as string[]).filter(s=>s!==a.owner_session),peer=peers[0],peerGeneration=peer?this.authority.generation(peer):null;
    // No live working process or unknown telemetry is overridden. Transfer keeps
    // old worker claims/resources intact and requires genuine successor pickup.
    if(peer&&peerGeneration&&coordinationIdle(this.activity(peer),peerGeneration,this.now())&&coordinationIdle(this.activity(a.owner_session),a.owner_generation,this.now())) {
     try {
      return this.db.transaction(()=>{
       const transferred=this.authority.transferIdleStalled(jobId,{rigId,expectedEpoch:a.epoch,progressDigest:progress.digest,planRevision:plan.revision,recipient:peer,recipientGeneration:peerGeneration,leaseMs:plan.acknowledgmentWindowMs??300000});
       const queueId='qitem-coordination-peer-'+digest(rigId+':'+transferred.epoch).slice(0,24);
       this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:peer,body:JSON.stringify({action:'reconcile-transferred-baton',rigId,epoch:transferred.epoch,batonId:transferred.baton_id,deadline:transferred.lease_until,recipientGeneration:peerGeneration,required:'Read exact obligations and native current identity; acknowledge authority through supported coordinator API, then reconcile admitted frontier. This notice is not acknowledgment.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
       this.repo.stageWakeIntent(queueId,'watchdog@system',peer,'system:operator-authorized-coordination',true,peerGeneration);
       return [{key:'coordinator',state:'pending-peer-acknowledgment',queueId,deadline:transferred.lease_until}];
      })();
     } catch(error) {
      const code=(error as {code?:string}).code,hold=heldDispatchCode(error)??(['coordination_stall_unproven','coordinator_uncertain_effects','coordinator_held_history_recovery_required'].includes(code??'')?code:undefined);
      if(!hold)throw error;
      // A safe takeover refusal is not a global work gate. Re-read authority before
      // fallback; never use a stale cached holder/epoch or imply successor pickup.
      const current=this.authority.get(rigId);
      if(!current||current.owner_session!==a.owner_session||current.owner_generation!==a.owner_generation||current.epoch!==a.epoch||current.state!=='active'||this.authority.generation(current.owner_session)!==current.owner_generation)throw error;
      if(current.lease_until<=this.now()){const deadline=this.now()+plan.stallMs,queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'recover-expired-idle-transfer',hold,deadline);return [{key:'coordinator',state:'held',queueId,reason:hold,deadline}];}
      const receipt={rigId,epoch:a.epoch,peer,reason:hold,owner:'operator-agent@kernel',action:'Reconcile exact Peer custody/transport/lifecycle fence and retry only after eligibility is proven',deadline:this.now()+plan.stallMs};
      this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'coordination-takeover-hold:'+digest(JSON.stringify({revision:plan.revision,epoch:a.epoch,hold})), 'coordination-takeover-hold',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
      const queueId=hold==='coordinator_held_history_recovery_required'?this.stageCoordinatorRecovery(rigId,current.epoch,plan.operatorGeneration,'restore-current-held-history-binding',hold,receipt.deadline):undefined;
      return [...this.reconcile(current.owner_session,current.owner_generation,rigId),{key:'coordinator',state:'held',...(queueId?{queueId}:{}),reason:hold,deadline:receipt.deadline}];
     }
    }
   }
   if(a.state==='reconciling'&&a.lease_until<=this.now()){
    const queueId='qitem-coordination-ack-recovery-'+digest(rigId+':'+a.epoch).slice(0,24);
    const existing=this.repo.getById(queueId);
    if(!existing)this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',body:JSON.stringify({action:'recover-expired-reconciliation',rigId,epoch:a.epoch,recipient:a.owner_session,recipientGeneration:a.owner_generation,batonId:a.baton_id,required:'Inspect current native recipient and exact obligations; invoke reconciliation-recover once with current token and custody digest, then require genuine recipient acknowledge. If exhausted or identity uncertain, return concrete supported recovery boundary; do not fabricate lease or acknowledgment.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});
    this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,plan.operatorGeneration);
    return [{key:'coordinator',state:'pending-reconciliation-recovery',queueId,reason:'expired-unacknowledged-transfer',deadline:a.lease_until+(plan.acknowledgmentWindowMs??300000)}];
   }
   if(a.state!=='active'||a.lease_until<=this.now()){const deadline=this.now()+plan.stallMs,reason='current-holder-acknowledgment-or-lease';const queueId=this.stageCoordinatorRecovery(rigId,a.epoch,plan.operatorGeneration,'reconcile-current-coordinator-lease',reason,deadline);return [{key:'coordinator',state:'recovery-required',queueId,reason,deadline}];}
   return this.retireForAuthorizedObserver(rigId,plan.operatorGeneration,this.reconcile(a.owner_session,a.owner_generation,rigId));
  }).immediate();
 }

}
