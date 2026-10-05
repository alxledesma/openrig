import {assess} from './decision-assessment/assess.mjs';
import {validateConfig} from './decision-assessment/config.mjs';
import {digest,CoordinatorFenceError} from './coordinator-authority-service.js';
import type {QueueRepository} from './queue-repository.js';
export interface OutcomePolicy {
 rigId:string;revision:string;mode:'observe'|'enforce';operatorGeneration:string;
 dataClass:'public'|'private';allowPaid:boolean;allowUnqualifiedNegativeAdvice?:boolean;adapterConfig:Record<string,unknown>;
 qualification:{ref:string;providerConfigDigest:string;validUntil:number};
}
export interface QualificationRefresh {rigId:string;dutyQueueId:string;operationId:string;policyRevision:string;policyDigest:string;qualifiedAt:number;qualification:OutcomePolicy['qualification']}
export interface RecoveryBinding {rigId:string;operationId:string;originalPackageKey:string;originalDispositionId:string;originalContractHash:string;recoveryPackageKey:string;recoveryQueueId:string;recoveryContractHash:string;evidenceRef:string;deadline:number}
type Job={rigId:string;packageKey:string;queueId:string;dispositionId:string;worker:string;generation:string;policyRevision:string;inputDigest:string;configurationDigest:string|null;state:string;createdAt:number;startedAt?:number};
/** Fixed-size allowlist of normalized adapter evidence; never raw provider payloads. */
export function sanitizedAssessmentEvidence(receipt:Record<string,unknown>,configDigest:string):Record<string,unknown> {
 const obj=(v:unknown):Record<string,unknown>|null=>v!==null&&typeof v==='object'&&!Array.isArray(v)?v as Record<string,unknown>:null;
 const text=(v:unknown)=>typeof v==='string'&&v.length<=256?v:null;
 const hash=(v:unknown)=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v)?v:null;
 const probability=(v:unknown)=>typeof v==='number'&&Number.isFinite(v)&&v>=0&&v<=1;
 const count=(v:unknown)=>typeof v==='number'&&Number.isSafeInteger(v)&&v>=0;
 const answer=obj(obj(receipt.answers)?.outcome), probabilities=obj(answer?.probabilities);
 let outcome:Record<string,unknown>|null=null;
 if(answer?.type==='choice'&&['yes','no','unknown'].includes(String(answer.choice))&&probabilities&&Object.keys(probabilities).sort().join(',')==='no,unknown,yes'&&Object.values(probabilities).every(probability)&&Math.abs(Number(probabilities.yes)+Number(probabilities.no)+Number(probabilities.unknown)-1)<0.0001&&probability(answer.topProbability)&&probability(answer.margin)){
  outcome={type:'choice',choice:answer.choice,probabilities:{yes:probabilities.yes,no:probabilities.no,unknown:probabilities.unknown},topProbability:answer.topProbability,margin:answer.margin};
  for(const key of ['providerConfidence','providerAnswerConfidence'])if(probability(answer[key]))outcome[key]=answer[key];
 }
 const rawUsage=obj(receipt.usage),usage:Record<string,number>={};
 for(const key of ['input_tokens','output_tokens'])if(count(rawUsage?.[key]))usage[key]=rawUsage![key] as number;
 if(typeof rawUsage?.cost==='number'&&Number.isFinite(rawUsage.cost)&&rawUsage.cost>=0&&rawUsage.cost<=Number.MAX_SAFE_INTEGER)usage.cost=rawUsage.cost;
 return {schema:'runtime-assessment-evidence.v1',providerConfigDigest:hash(configDigest),assessmentInputDigest:hash(receipt.inputDigest),rubricId:receipt.rubricId==='runtime-explicit-unfinished-v1'?receipt.rubricId:null,rubricDigest:hash(receipt.rubricDigest),providerId:text(receipt.providerId),requestedModel:text(receipt.requestedModel),model:text(receipt.model),calibrationId:text(receipt.calibrationId),inputCoverage:['unverified','truncated','reported_complete'].includes(String(receipt.inputCoverage))?receipt.inputCoverage:null,outcome,usage,latencyMs:count(receipt.latencyMs)?receipt.latencyMs:null,grantsAuthority:false};
}
const reject=(code:string,message:string):never=>{throw new CoordinatorFenceError(code,message);};
/** One harness-neutral durable terminal-return consumer. Models classify only;
 * queue/identity/admission/current-holder checks remain deterministic authority. */
export class RuntimeOutcomeAssessment {
 private draining=new Map<string,Promise<void>>();
 constructor(private repo:QueueRepository,private dependencies:Record<string,unknown>={},private now:()=>number=Date.now){}
 private get authority(){return this.repo.coordinatorAuthority;}
 private get db(){return this.authority.db;}
 /** The logical policy is immutable; qualification is a separate dated receipt,
  * so renewal cannot hide unresolved jobs/recovery by changing their revision. */
 private storedPolicy(rigId:string):OutcomePolicy|null {const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='runtime-outcome-policy' ORDER BY rowid DESC LIMIT 1").get(rigId) as {receipt:string}|undefined;return row?JSON.parse(row.receipt):null;}
 policy(rigId:string):OutcomePolicy|null {
  const p=this.storedPolicy(rigId);if(!p)return null;
  const row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='runtime-outcome-qualification' AND json_extract(receipt,'$.policyRevision')=? ORDER BY rowid DESC LIMIT 1").get(rigId,p.revision) as {receipt:string}|undefined;
  const r=row?JSON.parse(row.receipt):null;
  return r&&r.policyDigest===digest(JSON.stringify(p))&&r.operatorGeneration===p.operatorGeneration?{...p,qualification:r.qualification}:p;
 }
 refreshQualification(actor:string,generation:string,input:QualificationRefresh):void {
  this.db.transaction(()=>{
   this.authority.assertCurrentOperator(actor,generation);if(!input||typeof input.rigId!=='string')reject('runtime_outcome_refresh_required','Exact qualification refresh contract required');const p=this.storedPolicy(input.rigId)!;
   if(!input||Object.keys(input).sort().join(',')!=='dutyQueueId,operationId,policyDigest,policyRevision,qualification,qualifiedAt,rigId'||!p||!this.authority.coordinationRecovery?.plan(input.rigId)||p.operatorGeneration!==generation||input.policyRevision!==p.revision||input.policyDigest!==digest(JSON.stringify(p))||!input.operationId?.trim()||!input.dutyQueueId?.trim())reject('runtime_outcome_refresh_required','Exact unchanged logical policy and current genuine Operator required');
   const id='runtime-outcome-qualification:'+input.operationId,request=digest(JSON.stringify({actor,generation,input})),saved=this.db.prepare('SELECT request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(input.rigId,id) as {request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==request)reject('runtime_outcome_refresh_conflict','Qualification replay differs');return;}
   const current=this.policy(input.rigId)!;
   if(!this.qualificationDutyExact(input,p,generation)||!this.qualificationBoundaryMatches(input.rigId,{policyRevision:input.policyRevision,policyDigest:input.policyDigest,providerConfigDigest:current.qualification.providerConfigDigest,qualificationRef:current.qualification.ref,qualificationValidUntil:current.qualification.validUntil,recipient:'operator-agent@kernel',recipientGeneration:generation})||!this.authority.coordinationRecovery?.lifecycleControlActAllowed(input.dutyQueueId,actor,generation,'transport:v1'))reject('runtime_outcome_refresh_required','Exact active expired-qualification duty must be natively claimed by the current Operator before refresh');
   const q=input.qualification;
   if(!q||Object.keys(q).sort().join(',')!=='providerConfigDigest,ref,validUntil'||typeof q.ref!=='string'||!q.ref.trim()||q.ref===current.qualification.ref||q.providerConfigDigest!==p.qualification.providerConfigDigest||q.providerConfigDigest!==digest(JSON.stringify(p.adapterConfig))||!Number.isSafeInteger(input.qualifiedAt)||input.qualifiedAt<this.now()-180000||input.qualifiedAt>this.now()||!Number.isSafeInteger(q.validUntil)||q.validUntil<=this.now()||q.validUntil>input.qualifiedAt+3600000)reject('runtime_outcome_qualification_required','Fresh dated exact provider proof and finite qualification of at most one hour required');
   const receipt={policyRevision:p.revision,policyDigest:input.policyDigest,operatorGeneration:generation,dutyQueueId:input.dutyQueueId,qualifiedAt:input.qualifiedAt,qualification:q,grantsAuthority:false};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'runtime-outcome-qualification',JSON.stringify(receipt),request);
  }).immediate();
 }
 /** Only the exact persisted expired-qualification duty for this unchanged policy,
   * provider configuration and qualification reference authorizes a refresh; no other
   * claimed, act-eligible Operator duty can stand in for it. */
 private qualificationDutyExact(input:QualificationRefresh,p:OutcomePolicy,generation:string):boolean {
  const current=this.policy(input.rigId);if(!current)return false;
  const r=this.authority.coordinationRecovery?.lifecycleControlReceipt(input.dutyQueueId),policyDigest=digest(JSON.stringify(p));
  return !!r&&r.kind==='outcome-qualification-refresh'&&r.recipient==='operator-agent@kernel'&&r.recipientGeneration===generation&&r.operatorGeneration===generation&&r.packageKey==='outcome-qualification:'+p.revision&&r.policyRevision===p.revision&&r.policyRevision===input.policyRevision&&r.policyDigest===policyDigest&&r.policyDigest===input.policyDigest&&r.providerConfigDigest===p.qualification.providerConfigDigest&&r.qualificationRef===current.qualification.ref&&Number.isSafeInteger(r.deadline)&&r.deadline>this.now()&&r.semanticKey===digest(JSON.stringify({policyRevision:p.revision,policyDigest,providerConfigDigest:p.qualification.providerConfigDigest,qualificationRef:current.qualification.ref,qualificationValidUntil:current.qualification.validUntil}));
 }
 qualificationBoundaryMatches(rigId:string,input:any):boolean {
  const stored=this.storedPolicy(rigId),current=this.policy(rigId),generation=this.authority.generation('operator-agent@kernel'),plan=this.authority.coordinationRecovery?.plan(rigId);
  return !!stored&&!!current&&!!plan&&!!generation&&stored.operatorGeneration===generation&&plan.operatorGeneration===generation&&input?.policyRevision===stored.revision&&input.policyDigest===digest(JSON.stringify(stored))&&input.providerConfigDigest===digest(JSON.stringify(stored.adapterConfig))&&input.providerConfigDigest===stored.qualification.providerConfigDigest&&current.revision===stored.revision&&current.operatorGeneration===generation&&current.qualification.ref===input.qualificationRef&&current.qualification.providerConfigDigest===input.providerConfigDigest&&current.qualification.validUntil===input.qualificationValidUntil&&current.qualification.validUntil<=this.now()&&(!input.recipient||input.recipient==='operator-agent@kernel')&&(!input.recipientGeneration||input.recipientGeneration===generation)&&(!input.operatorGeneration||input.operatorGeneration===generation);
 }
/** Monotone receipt postcondition: proof freshness is enforced once at apply time,
   * and only this exact bound receipt keeps the completion satisfied afterwards. */
 qualificationRefreshCompleted(rigId:string,duty:any):boolean {
  const stored=this.storedPolicy(rigId),row=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='runtime-outcome-qualification' AND json_extract(receipt,'$.policyRevision')=? AND json_extract(receipt,'$.dutyQueueId')=? ORDER BY rowid DESC LIMIT 1").get(rigId,duty.policyRevision,duty.queueId) as {receipt:string}|undefined;
  if(!stored||!row||stored.revision!==duty.policyRevision||digest(JSON.stringify(stored))!==duty.policyDigest||stored.operatorGeneration!==duty.recipientGeneration||stored.qualification.providerConfigDigest!==duty.providerConfigDigest)return false;
  let receipt:any;try{receipt=JSON.parse(row.receipt);}catch{return false;}const q=receipt.qualification;
  return receipt.policyRevision===duty.policyRevision&&receipt.policyDigest===duty.policyDigest&&receipt.operatorGeneration===duty.recipientGeneration&&receipt.dutyQueueId===duty.queueId&&receipt.grantsAuthority===false&&Number.isSafeInteger(receipt.qualifiedAt)&&receipt.qualifiedAt<=this.now()&&q?.ref!==duty.qualificationRef&&typeof q?.ref==='string'&&q.ref.trim().length>0&&q.providerConfigDigest===duty.providerConfigDigest&&Number.isSafeInteger(q.validUntil)&&q.validUntil>receipt.qualifiedAt&&q.validUntil<=receipt.qualifiedAt+3600000;
 }
 private bindingAssignment(rigId:string,packageKey:string):any|null {
  const row=this.db.prepare('SELECT a.*,q.body,q.state,q.claimed_by_generation_uuid,p.contract FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id JOIN coordinator_packages p ON p.rig_id=a.rig_id AND p.package_key=a.package_key WHERE a.rig_id=? AND a.package_key=?').get(rigId,packageKey) as any;
  if(!row)return null;const c=JSON.parse(row.contract);
  if(!row.claimed_by_generation_uuid||digest(row.body)!==row.body_hash||c.destination!==row.destination||c.bodyHash!==row.body_hash)return null;
  if(row.disposition_id){const returned=this.repo.getById(row.disposition_id);let b:any;try{b=returned?JSON.parse(returned.body):null;}catch{return null;}
   if(!returned||returned.sourceSession!==row.destination||returned.destinationSession!==c.returnContract.destination||(this.db.prepare('SELECT minting_generation_uuid FROM queue_items WHERE qitem_id=?').get(row.disposition_id) as {minting_generation_uuid:string}).minting_generation_uuid!==row.claimed_by_generation_uuid||b?.packageKey!==packageKey||b.inputDigest!==c.inputDigest||!Array.isArray(b.evidence)||c.returnContract.evidenceRequired.some((kind:string)=>!b.evidence.some((e:any)=>e.kind===kind&&typeof e.ref==='string'&&e.ref.trim())))return null;
  }
  return row;
 }
 private closureTargetMatches(rigId:string,original:any,target:any):boolean {
  return !!target&&Object.keys(target).sort().join(',')==='contractHash,dispositionId,packageKey,queueId,rigId'&&target.rigId===rigId&&target.packageKey===original.package_key&&target.queueId===original.queue_id&&target.dispositionId===original.disposition_id&&target.contractHash===digest(original.contract);
 }
 /** Explicit current holder technical attribution, never inferred from review prose. */
 bindRecovery(actor:string,generation:string,input:RecoveryBinding):void {
  this.db.transaction(()=>{
   if(!input||typeof input.rigId!=='string')reject('runtime_outcome_binding_required','Exact recovery binding contract required');
   const a=this.authority.get(input.rigId)!,plan=this.authority.coordinationRecovery?.plan(input.rigId)!,p=this.policy(input.rigId);
   if(!input||Object.keys(input).sort().join(',')!=='deadline,evidenceRef,operationId,originalContractHash,originalDispositionId,originalPackageKey,recoveryContractHash,recoveryPackageKey,recoveryQueueId,rigId'||!a||a.state!=='active'||a.owner_session!==actor||a.owner_generation!==generation||this.authority.generation(actor)!==generation||a.lease_until<=this.now()||!plan||plan.operatorGeneration!==this.authority.generation('operator-agent@kernel')||p?.operatorGeneration!==plan.operatorGeneration)reject('runtime_outcome_binding_required','Current live holder and exact current Operator/plan required');
   for(const session of [actor,'operator-agent@kernel'])if(plan.dispatchRestrictions?.some(r=>r.session===session)||this.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE state!='released' AND (session_name=? OR node_id IN(SELECT node_id FROM sessions WHERE session_name=?))").get(session,session)||this.db.prepare('SELECT 1 FROM seat_delivery_guards WHERE node_id IN(SELECT node_id FROM sessions WHERE session_name=?) AND (desired=1 OR effective=1)').get(session))reject('runtime_outcome_binding_protected','Current holder/Operator reservation, typing or quiescence remains protected');
   const original=this.bindingAssignment(input.rigId,input.originalPackageKey),repair=this.bindingAssignment(input.rigId,input.recoveryPackageKey);
   if(!original||original.disposition_id!==input.originalDispositionId||!['done','handed-off','failed','denied','canceled'].includes(original.state)||!repair||repair.queue_id!==input.recoveryQueueId||input.originalPackageKey===input.recoveryPackageKey||digest(original.contract)!==input.originalContractHash||digest(repair.contract)!==input.recoveryContractHash||!this.requiresRecovery(input.rigId,input.originalPackageKey)||!input.operationId?.trim()||typeof input.evidenceRef!=='string'||!input.evidenceRef.trim())reject('runtime_outcome_binding_required','Exact required historical original and distinct genuine admitted recovery with explicit technical attribution required');
   let recoveryBody:any;try{recoveryBody=JSON.parse(repair.body);}catch{}
   if(!this.closureTargetMatches(input.rigId,original,recoveryBody?.closureTarget)||(repair.disposition_id&&!this.closureTargetMatches(input.rigId,original,JSON.parse(this.repo.getById(repair.disposition_id)!.body).closureTarget)))reject('runtime_outcome_binding_required','Immutable recovery assignment and typed result must explicitly name the exact original closureTarget; separate review is not recovery');
   if(!repair.disposition_id){const task=plan.tasks.find(t=>t.packageKey===input.recoveryPackageKey);
    if(!['in-progress','blocked'].includes(repair.state)||repair.claimed_by_generation_uuid!==this.authority.generation(repair.destination)||!task||task.owner!==repair.destination||digest(task.body)!==repair.body_hash||task.admission.generation!==repair.claimed_by_generation_uuid||task.admission.configurationDigest!==this.authority.coordinationRecovery!.configurationDigest(repair.destination)||task.admission.validUntil<=this.now()||task.deadline<=this.now())reject('runtime_outcome_binding_required','Active recovery requires exact current configured admission and actual native worker custody');
   }else if(!['done','handed-off'].includes(repair.state))reject('runtime_outcome_binding_required','Terminal recovery must be a genuinely successful typed disposed result');
   const id='runtime-outcome-recovery-binding:'+input.operationId,request=digest(JSON.stringify({actor,generation,input})),saved=this.db.prepare('SELECT request_hash FROM coordinator_operations WHERE rig_id=? AND operation_id=?').get(input.rigId,id) as {request_hash:string}|undefined;
   if(saved){if(saved.request_hash!==request)reject('runtime_outcome_binding_conflict','Frozen recovery attribution replay differs');return;}
   if(!Number.isSafeInteger(input.deadline)||input.deadline<=this.now()||input.deadline>this.now()+1200000)reject('runtime_outcome_binding_expired','Explicit recovery binding must expire within twenty minutes');
   const receipt={...input,actor,generation,holderEpoch:a.epoch,operatorGeneration:plan.operatorGeneration,policyRevision:p!.revision,planRevision:plan.revision,originalQueueId:original.queue_id,originalBodyHash:original.body_hash,recoveryBodyHash:repair.body_hash,grantsAuthority:false};
   this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(input.rigId,id,'runtime-outcome-recovery-binding',JSON.stringify(receipt),request);
  }).immediate();
 }
 private acceptedBoundRecovery(rigId:string,packageKey:string):boolean {
  const p=this.policy(rigId),original=this.bindingAssignment(rigId,packageKey);if(!p||!original)return false;
  const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='runtime-outcome-recovery-binding' AND json_extract(receipt,'$.originalPackageKey')=? AND json_extract(receipt,'$.policyRevision')=?").all(rigId,packageKey,p.revision) as Array<{receipt:string}>;
  return rows.some(row=>{const r=JSON.parse(row.receipt),repair=this.bindingAssignment(rigId,r.recoveryPackageKey);const a=this.authority.get(rigId),plan=this.authority.coordinationRecovery?.plan(rigId);return a?.state==='active'&&a.lease_until>this.now()&&a.owner_session===r.actor&&a.owner_generation===r.generation&&a.epoch===r.holderEpoch&&this.authority.generation(r.actor)===r.generation&&this.authority.generation('operator-agent@kernel')===r.operatorGeneration&&plan?.revision===r.planRevision&&r.deadline>this.now()&&r.originalQueueId===original.queue_id&&r.originalDispositionId===original.disposition_id&&r.originalBodyHash===original.body_hash&&r.originalContractHash===digest(original.contract)&&repair&&r.recoveryQueueId===repair.queue_id&&r.recoveryBodyHash===repair.body_hash&&r.recoveryContractHash===digest(repair.contract)&&['done','handed-off'].includes(repair.state)&&!!repair.disposition_id&&this.closureTargetMatches(rigId,original,JSON.parse(this.repo.getById(repair.disposition_id)!.body).closureTarget)&&!!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(rigId,repair.queue_id,repair.disposition_id);});
 }
 recoveryDutyClosureAllowed(queueId:string):boolean {
  const row=this.db.prepare("SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind='runtime-outcome-binding-duty'").get(queueId) as {rig_id:string;receipt:string}|undefined;if(!row)return true;
  const r=JSON.parse(row.receipt),q=this.repo.getById(queueId),original=this.bindingAssignment(row.rig_id,r.packageKey);
  return !!q&&digest(q.body)===r.bodyHash&&!!original&&original.queue_id===r.originalQueueId&&original.disposition_id===r.dispositionId&&r.operatorGeneration===this.authority.generation('operator-agent@kernel')&&(this.acceptedBoundRecovery(row.rig_id,r.packageKey)||!!this.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id=? AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=? AND json_extract(receipt,'$.dispositionId')=?").get(row.rig_id,original.queue_id,original.disposition_id));
 }
 /** Existing registered observer owns a finite actionable duty; no new scheduler. */
 stageRecoveryBoundary(rigId:string):string[] {
  return this.db.transaction(()=>{
   const p=this.policy(rigId),plan=this.authority.coordinationRecovery?.plan(rigId),a=this.authority.get(rigId);if(!p||p.mode!=='enforce'||!plan||!a||p.operatorGeneration!==this.authority.generation('operator-agent@kernel')||plan.operatorGeneration!==p.operatorGeneration)return [];
   const rows=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='runtime-outcome-recovery' AND json_extract(receipt,'$.policyRevision')=?").all(rigId,p.revision) as Array<{receipt:string}>;const queueIds:string[]=[];
   for(const row of rows){const r=JSON.parse(row.receipt);if(!this.requiresRecovery(rigId,r.packageKey)||this.acceptedBoundRecovery(rigId,r.packageKey))continue;const original=this.bindingAssignment(rigId,r.packageKey);if(!original||original.disposition_id!==r.dispositionId)continue;
    const target=plan.tasks.find(t=>t.packageKey===r.packageKey);if(target&&plan.tasks.some(t=>t.recoveryFor===target.key))continue;
    const queueId='qitem-outcome-binding-'+digest(rigId+':'+r.dispositionId+':'+p.revision+':'+p.operatorGeneration+':'+a.epoch).slice(0,24);queueIds.push(queueId);
    if(this.repo.getById(queueId))continue;const deadline=this.now()+1200000;
    this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',identityProvenance:'system:operator-authorized-coordination',expiresAt:new Date(deadline).toISOString(),nudge:true,body:JSON.stringify({action:'materialize-exact-outcome-recovery-binding',rigId,policyRevision:p.revision,recipientGeneration:p.operatorGeneration,deadline,grantsAuthority:false,originalPackageKey:r.packageKey,originalQueueId:original.queue_id,originalDispositionId:r.dispositionId,originalContractHash:digest(original.contract),holder:a.owner_session,holderGeneration:a.owner_generation,recoveryAssignmentContract:{closureTarget:{rigId,packageKey:r.packageKey,queueId:original.queue_id,dispositionId:r.dispositionId,contractHash:digest(original.contract)},action:'<current holder explicit technical recovery scope>',evidenceInputs:['<actual retained independent review/evidence>']},bindingContract:{rigId,operationId:'<new immutable binding operation>',originalPackageKey:r.packageKey,originalDispositionId:r.dispositionId,originalContractHash:digest(original.contract),recoveryPackageKey:'<distinct actual admitted recovery>',recoveryQueueId:'<actual recovery worker assignment>',recoveryContractHash:'<immutable admitted recovery contract hash>',evidenceRef:'<current holder explicit technical attribution>',deadline},required:'Claim this finite Operator materialization duty. Obtain genuine current holder explicit technical attribution of a distinct admitted recovery assignment/result to this exact required original. Never infer independent review equals recovery. Admit a distinct actual recovery whose immutable assignment body and typed return both carry recoveryAssignmentContract.closureTarget, preserving original history. Use outcome-recovery-bind under current holder identity; active recovery needs fresh configured admission and actual custody, terminal history needs genuine successful typed disposition. Actual holder coordination-accept of recovery remains required before original acceptance. Preserve logical policy, UNKNOWN, original contracts and prior queues. Reuse supported qualification-refresh only with unchanged policy/provider permissions and fresh dated proof. Missing technical attribution/admission is a precise protected hold; do not fabricate work or extend an expired duty.'})});
    const receipt={queueId,packageKey:r.packageKey,originalQueueId:original.queue_id,dispositionId:r.dispositionId,operatorGeneration:p.operatorGeneration,deadline,bodyHash:digest(this.repo.getById(queueId)!.body)};
    this.db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,queueId,'runtime-outcome-binding-duty',JSON.stringify(receipt),digest(JSON.stringify(receipt)));
    this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,p.operatorGeneration);
   }return queueIds;
  }).immediate();
 }
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
 /** Expiry is a native recovery obligation, never permission to extend a grant. */
 stagePolicyBoundary(rigId:string):string|null {
  const p=this.policy(rigId),generation=this.authority.generation('operator-agent@kernel');
  if(!p||this.current(p)||!generation||generation!==p.operatorGeneration||!this.authority.get(rigId))return null;
  return this.authority.coordinationRecovery?.stageOutcomeQualificationDuty(rigId,{policyRevision:p.revision,policyDigest:digest(JSON.stringify(this.storedPolicy(rigId))),providerConfigDigest:p.qualification.providerConfigDigest,qualificationRef:p.qualification.ref,qualificationValidUntil:p.qualification.validUntil})??null;
 }
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
  if(this.requiresRecovery(rigId,packageKey)&&!acceptedRepair&&!this.acceptedBoundRecovery(rigId,packageKey))reject('runtime_outcome_recovery_required','Incomplete/unverified return requires authorized recovery; model cannot grant acceptance');
 }
 drain(rigId:string):Promise<void> {const existing=this.draining.get(rigId);if(existing)return existing;const promise=this.consume(rigId).finally(()=>{this.draining.delete(rigId);});this.draining.set(rigId,promise);return promise;}
 private async consume(rigId:string):Promise<void> {
  const rows=this.db.prepare("SELECT operation_id,kind,receipt FROM coordinator_operations WHERE rig_id=? AND kind IN ('runtime-outcome-pending','runtime-outcome-running') ORDER BY rowid").all(rigId) as Array<{operation_id:string;kind:string;receipt:string}>;
  for(const row of rows){
   const job=JSON.parse(row.receipt) as Job,p=this.policy(rigId);if(!p)continue;
   if(row.kind==='runtime-outcome-running'&&this.now()-(job.startedAt??job.createdAt)<15000)continue;
   if(row.kind==='runtime-outcome-pending'){job.startedAt=this.now();if(!this.db.prepare("UPDATE coordinator_operations SET kind='runtime-outcome-running',receipt=? WHERE rig_id=? AND operation_id=? AND kind='runtime-outcome-pending'").run(JSON.stringify(job),rigId,row.operation_id).changes)continue;}
   let classification='unknown',reason='deterministic-fallback',status='unavailable',negativeAdvice=false;let provenance:Record<string,unknown>={};let assessmentEvidence:Record<string,unknown>|null=null;
   const a=this.db.prepare('SELECT a.disposition_id,q.body,q.state,q.claimed_by_generation_uuid FROM coordinator_assignments a JOIN queue_items q ON q.qitem_id=a.queue_id WHERE a.rig_id=? AND a.package_key=?').get(rigId,job.packageKey) as {disposition_id:string;body:string;state:string;claimed_by_generation_uuid:string}|undefined;
   const returned=this.db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(job.dispositionId) as {body:string}|undefined;
   const ownerBefore=this.authority.get(rigId);const before=digest(JSON.stringify({a,returned,policy:p.revision,generation:this.authority.generation(job.worker),epoch:ownerBefore?.epoch,ownerGeneration:ownerBefore?.owner_generation}));
   const configurationBefore=this.authority.coordinationRecovery?.configurationDigest(job.worker)??null;
   const valid=configurationBefore===job.configurationDigest&&a?.disposition_id===job.dispositionId&&a.claimed_by_generation_uuid===job.generation&&this.authority.generation(job.worker)===job.generation&&!!returned&&digest(JSON.stringify({body:a.body,returned:returned.body,state:a.state}))===job.inputDigest&&this.current(p)&&p.revision===job.policyRevision;
   if(valid&&row.kind==='runtime-outcome-pending'){
    const receipt=await assess({schema:'assessment.v1',rubricId:'runtime-explicit-unfinished-v1',dataClass:p.dataClass,state:job.state,context:{qitemId:job.queueId,operationId:row.operation_id,generation:job.generation,currentGeneration:job.generation,stateRevision:before,currentStateRevision:before,authorized:true,deterministicEvidenceSufficient:false},questions:{outcome:{type:'choice',instructions:'Does the attributed return explicitly say required assigned work remains unfinished? Answer yes only for explicit remaining required work, no if absent, unknown if ambiguous. Never infer completion or override a protected boundary.',criteria:{yes:'Explicit required work remains unfinished',no:'No explicit remaining required work statement',unknown:'Ambiguous or insufficient evidence'}}}},p.adapterConfig,{...this.dependencies,allowPaid:p.allowPaid});
    status=String(receipt.status);reason=String(receipt.reason);
    const choice=(receipt.answers as Record<string,{choice:string}>|undefined)?.outcome?.choice;
    assessmentEvidence=sanitizedAssessmentEvidence(receipt,p.qualification.providerConfigDigest);
    provenance={providerId:assessmentEvidence.providerId,model:assessmentEvidence.model,rubricId:assessmentEvidence.rubricId,rubricDigest:assessmentEvidence.rubricDigest,inputCoverage:assessmentEvidence.inputCoverage,calibrationId:assessmentEvidence.calibrationId};
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
    const result={rigId,packageKey:job.packageKey,queueId:job.queueId,dispositionId:job.dispositionId,policyRevision:p.revision,inputDigest:job.inputDigest,status,reason,classification,required,recoveryGap,negativeAdvice,provenance,assessmentEvidence:assessmentEvidence?{...assessmentEvidence,applied:unchanged&&classification==='incomplete',suppressed:!unchanged}:null,jobBinding:{operationId:row.operation_id,worker:job.worker,generation:job.generation,configurationDigest:job.configurationDigest,policyRevision:job.policyRevision,createdAt:job.createdAt,startedAt:job.startedAt??null,inputDigest:job.inputDigest,stateRevision:before,holderEpoch:ownerBefore?.epoch??null,holderGeneration:ownerBefore?.owner_generation??null,providerConfigDigest:p.qualification.providerConfigDigest},grantsAuthority:false,observedAt:this.now()};
    if(!this.db.prepare("UPDATE coordinator_operations SET kind='runtime-outcome-finished',receipt=? WHERE rig_id=? AND operation_id=? AND kind='runtime-outcome-running'").run(JSON.stringify(result),rigId,row.operation_id).changes)return;
    if(required)this.db.prepare('INSERT OR IGNORE INTO coordinator_operations VALUES (?,?,?,?,?)').run(rigId,'runtime-outcome-recovery:'+job.dispositionId,'runtime-outcome-recovery',JSON.stringify(result),digest(JSON.stringify(result)));
    if(recoveryGap){const queueId='qitem-outcome-recovery-'+digest(rigId+':'+job.dispositionId+':'+p.revision+':'+p.operatorGeneration).slice(0,24);if(!this.repo.getById(queueId))this.repo.createWithinTransaction({qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',body:JSON.stringify({action:recoveryGap.action,deadline:recoveryGap.deadline,rigId,packageKey:job.packageKey,dispositionId:job.dispositionId,policyRevision:p.revision,recipientGeneration:p.operatorGeneration,returnPath:'current coordinator holder '+ownerNow?.owner_session,required:'Inspect exact return; admit a bounded recovery package or return a concrete protected boundary. No product dispatch/qualification/acceptance authority granted by model.'}),identityProvenance:'system:operator-authorized-coordination',nudge:true});this.repo.stageWakeIntent(queueId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,p.operatorGeneration);}
   }).immediate();
   const owner=this.authority.get(rigId);
   if(owner?.state==='active'&&owner.lease_until>this.now()&&this.authority.generation(owner.owner_session)===owner.owner_generation){this.authority.coordinationRecovery?.reconcile(owner.owner_session,owner.owner_generation,rigId);await this.authority.coordinationRecovery?.deliverCommitted();}
  }
 }
}
