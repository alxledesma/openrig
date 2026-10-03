import {assess} from './decision-assessment/assess.mjs';
import {validateConfig} from './decision-assessment/config.mjs';
import {digest,CoordinatorFenceError} from './coordinator-authority-service.js';
import type {QueueRepository} from './queue-repository.js';
export interface OutcomePolicy {
 rigId:string;revision:string;mode:'observe'|'enforce';operatorGeneration:string;
 dataClass:'public'|'private';allowPaid:boolean;allowUnqualifiedNegativeAdvice?:boolean;adapterConfig:Record<string,unknown>;
 qualification:{ref:string;providerConfigDigest:string;validUntil:number};
}
type Job={rigId:string;packageKey:string;queueId:string;dispositionId:string;worker:string;generation:string;policyRevision:string;inputDigest:string;configurationDigest:string|null;state:string;createdAt:number;startedAt?:number};
const reject=(code:string,message:string):never=>{throw new CoordinatorFenceError(code,message);};
/** One harness-neutral durable terminal-return consumer. Models classify only;
 * queue/identity/admission/current-holder checks remain deterministic authority. */
export class RuntimeOutcomeAssessment {
 private draining=new Map<string,Promise<void>>();
 constructor(private repo:QueueRepository,private dependencies:Record<string,unknown>={},private now:()=>number=Date.now){}
 private get authority(){return this.repo.coordinatorAuthority;}
 private get db(){return this.authority.db;}
 policy(rigId:string):OutcomePolicy|null {const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='runtime-outcome-policy' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;return row?JSON.parse(row.receipt):null;}
 configure(actor:string,generation:string,p:OutcomePolicy):void {
  this.db.transaction(()=>{
   this.authority.assertCurrentOperator(actor,generation);
   if(!p||!this.authority.get(p.rigId)||!this.authority.coordinationRecovery?.plan(p.rigId)||p.operatorGeneration!==generation||!p.revision||!['observe','enforce'].includes(p.mode)||!['public','private'].includes(p.dataClass)||typeof p.allowPaid!=='boolean'||(p.allowUnqualifiedNegativeAdvice!==undefined&&typeof p.allowUnqualifiedNegativeAdvice!=='boolean'))reject('runtime_outcome_invalid_policy','Explicit enrolled rig/plan/current Operator and strict opt-ins required');
   const previous=this.policy(p.rigId);
   if(previous?.mode==='enforce'&&previous.revision!==p.revision){for(const row of this.db.prepare("SELECT DISTINCT json_extract(receipt,'$.packageKey') packageKey FROM coordinator_operations WHERE rig_id=? AND kind IN ('runtime-outcome-pending','runtime-outcome-running','runtime-outcome-recovery')").all(p.rigId) as Array<{packageKey:string}>){this.assertAcceptance(p.rigId,row.packageKey);}}
   validateConfig(p.adapterConfig);
   const primary=String(p.adapterConfig.primary),providers=p.adapterConfig.providers as Record<string,{timeoutMs:number;allowedData:string[]}>;
   if(providers[primary]!.timeoutMs>3000||!providers[primary]!.allowedData.includes(p.dataClass))reject('runtime_outcome_invalid_policy','Bounded 3-second assessment and explicit data policy required');
   if(!p.qualification?.ref||p.qualification.providerConfigDigest!==digest(JSON.stringify(p.adapterConfig))||!Number.isFinite(p.qualification.validUntil)||p.qualification.validUntil<=this.now())reject('runtime_outcome_qualification_required','Dated exact provider configuration qualification required');
   const existing=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?").get(p.rigId,'runtime-outcome-policy:'+p.revision) as {receipt:string}|undefined;
   if(existing){if(existing.receipt!==JSON.stringify(p))reject('runtime_outcome_policy_conflict','Revision cannot change');return;}
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(p.rigId,'runtime-outcome-policy:'+p.revision,'runtime-outcome-policy',JSON.stringify(p),digest(JSON.stringify({actor,generation,p})));
  }).immediate();
 }
 private current(p:OutcomePolicy):boolean {return p.operatorGeneration===this.authority.generation('operator-agent@kernel')&&p.qualification.validUntil>this.now()&&p.qualification.providerConfigDigest===digest(JSON.stringify(p.adapterConfig));}
 enqueueDisposed(actor:string,generation:string,rigId:string,packageKey:string,dispositionId:string):void {
  const p=this.policy(rigId);if(!p)return;
  const a=this.db.prepare('SELECT a.queue_id,a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?').get(rigId,packageKey) as {queue_id:string;disposition_id:string;body:string;state:string;claimed_by_generation_uuid:string}|undefined;
  const returned=this.db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(dispositionId) as {body:string}|undefined;
  if(!a||a.disposition_id!==dispositionId||a.claimed_by_generation_uuid!==generation||!returned)reject('runtime_outcome_return_required','Exact validated terminal return required');
  const job:Job={rigId,packageKey,queueId:a!.queue_id,dispositionId,worker:actor,generation,policyRevision:p.revision,configurationDigest:this.authority.coordinationRecovery?.configurationDigest(actor)??null,inputDigest:digest(JSON.stringify({body:a!.body,returned:returned!.body,state:a!.state})),state:JSON.stringify({assignment:a!.body,return:JSON.parse(returned!.body),terminalState:a!.state}),createdAt:this.now()};
  this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'runtime-outcome-job:'+dispositionId,'runtime-outcome-pending',JSON.stringify(job),digest(JSON.stringify(job)));
 }
 requiresRecovery(rigId:string,packageKey:string):boolean {
  const p=this.policy(rigId);if(p?.mode!=='enforce')return false;
  return !!this.db.prepare("SELECT 1 FROM coordinator_operations o JOIN coordinator_assignments a ON a.rig_id=o.rig_id AND a.package_key=json_extract(o.receipt,'$.packageKey') WHERE o.rig_id=? AND o.kind='runtime-outcome-recovery' AND a.package_key=? AND a.disposition_id=json_extract(o.receipt,'$.dispositionId') AND json_extract(o.receipt,'$.policyRevision')=?").get(rigId,packageKey,p.revision);
 }
 assertAcceptance(rigId:string,packageKey:string):void {
  const p=this.policy(rigId);if(p?.mode!=='enforce')return;
  if(this.db.prepare("SELECT 1 FROM coordinator_operations o JOIN coordinator_assignments a ON a.rig_id=o.rig_id AND a.package_key=json_extract(o.receipt,'$.packageKey') WHERE o.rig_id=? AND o.kind IN ('runtime-outcome-pending','runtime-outcome-running') AND a.package_key=? AND a.disposition_id=json_extract(o.receipt,'$.dispositionId') AND json_extract(o.receipt,'$.policyRevision')=?").get(rigId,packageKey,p.revision))reject('runtime_outcome_pending','Durable outcome assessment not yet resolved');
  const plan=this.authority.coordinationRecovery?.plan(rigId),target=plan?.tasks.find(t=>t.packageKey===packageKey);
  const acceptedRepair=!!target&&!!plan?.tasks.some(t=>t.recoveryFor===target.key&&!!this.db.prepare("SELECT 1 FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=? AND a.disposition_id IS NOT NULL AND q.state IN ('done','handed-off') AND EXISTS(SELECT 1 FROM coordinator_operations o WHERE o.rig_id=a.rig_id AND o.kind='coordination-accept' AND json_extract(o.receipt,'$.queueId')=a.queue_id AND json_extract(o.receipt,'$.dispositionId')=a.disposition_id)").get(rigId,t.packageKey));
  if(this.requiresRecovery(rigId,packageKey)&&!acceptedRepair)reject('runtime_outcome_recovery_required','Incomplete/unverified return requires authorized recovery; model cannot grant acceptance');
 }
 drain(rigId:string):Promise<void> {const existing=this.draining.get(rigId);if(existing)return existing;const promise=this.consume(rigId).finally(()=>{this.draining.delete(rigId);});this.draining.set(rigId,promise);return promise;}
 private async consume(rigId:string):Promise<void> {
  const rows=this.db.prepare("SELECT operation_id,kind,receipt FROM coordinator_operations WHERE rig_id=? AND kind IN ('runtime-outcome-pending','runtime-outcome-running') ORDER BY rowid").all(rigId) as Array<{operation_id:string;kind:string;receipt:string}>;
  for(const row of rows){
   const job=JSON.parse(row.receipt) as Job,p=this.policy(rigId);if(!p)continue;
   if(row.kind==='runtime-outcome-running'&&this.now()-(job.startedAt??job.createdAt)<15000)continue;
   if(row.kind==='runtime-outcome-pending'){job.startedAt=this.now();if(!this.db.prepare("UPDATE coordinator_operations SET kind='runtime-outcome-running',receipt=? WHERE rig_id=? AND operation_id=? AND kind='runtime-outcome-pending'").run(JSON.stringify(job),rigId,row.operation_id).changes)continue;}
   let classification='unknown',reason='deterministic-fallback',status='unavailable',negativeAdvice=false;let provenance:Record<string,unknown>={};
   const a=this.db.prepare('SELECT a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?').get(rigId,job.packageKey) as {disposition_id:string;body:string;state:string;claimed_by_generation_uuid:string}|undefined;
   const returned=this.db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(job.dispositionId) as {body:string}|undefined;
   const ownerBefore=this.authority.get(rigId);const before=digest(JSON.stringify({a,returned,policy:p.revision,generation:this.authority.generation(job.worker),epoch:ownerBefore?.epoch,ownerGeneration:ownerBefore?.owner_generation}));
   const configurationBefore=this.authority.coordinationRecovery?.configurationDigest(job.worker)??null;
   const valid=configurationBefore===job.configurationDigest&&a?.disposition_id===job.dispositionId&&a.claimed_by_generation_uuid===job.generation&&this.authority.generation(job.worker)===job.generation&&!!returned&&digest(JSON.stringify({body:a.body,returned:returned.body,state:a.state}))===job.inputDigest&&this.current(p)&&p.revision===job.policyRevision;
   if(valid&&row.kind==='runtime-outcome-pending'){
    const receipt=await assess({schema:'assessment.v1',rubricId:'runtime-explicit-unfinished-v1',dataClass:p.dataClass,state:job.state,context:{qitemId:job.queueId,operationId:row.operation_id,generation:job.generation,currentGeneration:job.generation,stateRevision:before,currentStateRevision:before,authorized:true,deterministicEvidenceSufficient:false},questions:{outcome:{type:'choice',instructions:'Does the attributed return explicitly say required assigned work remains unfinished? Answer yes only for explicit remaining required work, no if absent, unknown if ambiguous. Never infer completion or override a protected boundary.',criteria:{yes:'Explicit required work remains unfinished',no:'No explicit remaining required work statement',unknown:'Ambiguous or insufficient evidence'}}}},p.adapterConfig,{...this.dependencies,allowPaid:p.allowPaid});
    status=String(receipt.status);reason=String(receipt.reason);
    const choice=(receipt.answers as Record<string,{choice:string}>|undefined)?.outcome?.choice;
    provenance={providerId:receipt.providerId,model:receipt.model,rubricId:receipt.rubricId,rubricDigest:receipt.rubricDigest,inputCoverage:receipt.inputCoverage,calibrationId:receipt.calibrationId};
    if(receipt.status==='assessed')classification=choice==='yes'?'incomplete':choice==='no'?'no-explicit-unfinished':'unknown';
    negativeAdvice=p.allowUnqualifiedNegativeAdvice===true&&receipt.status==='abstained'&&['uncalibrated','coverage_unverified','low_probability'].includes(String(receipt.reason))&&receipt.inputCoverage!=='truncated'&&choice==='yes';
    if(negativeAdvice)classification='incomplete';
   }
   this.db.transaction(()=>{
    const latest=this.policy(rigId),fresh=this.db.prepare('SELECT a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?').get(rigId,job.packageKey);
    const freshReturn=this.db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(job.dispositionId);const ownerNow=this.authority.get(rigId);
    const unchanged=(this.authority.coordinationRecovery?.configurationDigest(job.worker)??null)===configurationBefore&&latest?.revision===p.revision&&this.current(p)&&JSON.stringify(fresh)===JSON.stringify(a)&&JSON.stringify(freshReturn)===JSON.stringify(returned)&&this.authority.generation(job.worker)===job.generation&&ownerNow?.epoch===ownerBefore?.epoch&&ownerNow?.owner_generation===ownerBefore?.owner_generation;
    if(!unchanged){negativeAdvice=false;classification='unknown';reason='state-changed';status='abstained';}
    const plan=this.authority.coordinationRecovery?.plan(rigId),target=plan?.tasks.find(t=>t.packageKey===job.packageKey);
    const hasAuthorizedRecovery=!!target&&!!plan?.tasks.some(t=>t.recoveryFor===target.key);
    const required=unchanged&&p.mode==='enforce'&&(classification==='incomplete'||!a||!['done','handed-off'].includes(a.state));
    const recoveryGap=required&&!hasAuthorizedRecovery?{owner:'operator-agent@kernel',action:'Admit and configure a distinct concrete recovery task for this exact incomplete return',deadline:this.now()+60000}:null;
    const result={rigId,packageKey:job.packageKey,queueId:job.queueId,dispositionId:job.dispositionId,policyRevision:p.revision,inputDigest:job.inputDigest,status,reason,classification,required,recoveryGap,negativeAdvice,provenance,grantsAuthority:false,observedAt:this.now()};
    if(!this.db.prepare("UPDATE coordinator_operations SET kind='runtime-outcome-finished',receipt=? WHERE rig_id=? AND operation_id=? AND kind='runtime-outcome-running'").run(JSON.stringify(result),rigId,row.operation_id).changes)return;
    if(required)this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'runtime-outcome-recovery:'+job.dispositionId,'runtime-outcome-recovery',JSON.stringify(result),digest(JSON.stringify(result)));
    if(recoveryGap){const queueId='qitem-outcome-recovery-'+digest(rigId+':'+job.dispositionId+':'+p.revision+':'+p.operatorGeneration).slice(0,24);if(!this.repo.getById(queueId))this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',body:JSON.stringify({action:recoveryGap.action,deadline:recoveryGap.deadline,rigId,packageKey:job.packageKey,dispositionId:job.dispositionId,policyRevision:p.revision,recipientGeneration:p.operatorGeneration,returnPath:'current coordinator holder '+ownerNow?.owner_session,required:'Inspect exact return; admit a bounded recovery package or return a concrete protected boundary. No product dispatch/qualification/acceptance authority granted by model.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,p.operatorGeneration);}
   }).immediate();
   const owner=this.authority.get(rigId);
   if(owner?.state==='active'&&owner.lease_until>this.now()&&this.authority.generation(owner.owner_session)===owner.owner_generation){this.authority.coordinationRecovery?.reconcile(owner.owner_session,owner.owner_generation,rigId);await this.authority.coordinationRecovery?.deliverCommitted();}
  }
 }
}
