import {createHash} from "node:crypto";
import {CoordinatorFenceError} from "./coordinator-authority-service.js";
import type {QueueRepository} from "./queue-repository.js";
import type {SeatDeliveryGuard} from "./seat-delivery-guard.js";
import {sourceFacts,stageRefusalRecovery} from "./queue-wake-ladder.js";
const OP="operator-agent@kernel",hash=(s:string)=>createHash("sha256").update(s).digest("hex");
export interface RecoveryInspectionRequest {qitemId:string;sourceFactsHash:string;operationId:string;authorizationId:string;}
function fail(code:string,msg:string):never {throw new CoordinatorFenceError(code,msg);}
export async function inspectQueueRecovery(repo:QueueRepository,guard:SeatDeliveryGuard|undefined,actor:string,generation:string,input:RecoveryInspectionRequest,clock=()=>new Date()) {
 if(!input||Object.keys(input).sort().join(',')!=="authorizationId,operationId,qitemId,sourceFactsHash"||typeof input.operationId!=="string"||!/^[A-Za-z0-9._-]{1,160}$/.test(input.operationId)||typeof input.sourceFactsHash!=="string"||!/^[a-f0-9]{64}$/.test(input.sourceFactsHash)||![input.qitemId,input.authorizationId].every(x=>typeof x==='string'&&x.length>0&&x.length<=200))fail('inspection_contract_invalid','Exact obligation/hash/op/auth required');
 repo.coordinatorAuthority.assertCurrentOperator(actor,generation);
 if(actor!==OP||!guard||guard.db!==repo.coordinatorAuthority.db)fail('inspection_control_unavailable','Current Operator/shared guard required');
 const db=repo.coordinatorAuthority.db,initial=repo.getById(input.qitemId);if(!initial)fail('inspection_source_missing','Exact existing obligation required');
 const requestHash=hash(JSON.stringify([actor,generation,input.qitemId,input.sourceFactsHash,input.operationId,input.authorizationId]));
 const result=await guard.inspectProtection(initial.destinationSession,(target,protection)=>db.transaction(()=>{
  repo.coordinatorAuthority.assertCurrentOperator(actor,generation);
  const now=clock();
  const liveProtection=guard.protectionFacts(target.nodeId);
  if(JSON.stringify(liveProtection)!==JSON.stringify(protection))fail("inspection_protection_changed","Protection changed while acquiring atomic commitment");
  const current=repo.getById(input.qitemId);
  if(!current||!['pending','in-progress','blocked'].includes(current.state)||hash(sourceFacts(current))!==input.sourceFactsHash||sourceFacts(current)!==sourceFacts(initial))fail('inspection_source_changed','Exact current nonterminal facts required');
  const actual=guard.target(current.destinationSession);if(JSON.stringify(actual)!==JSON.stringify(target))fail('inspection_target_changed','Target changed');
  const auth=repo.getById(input.authorizationId);const raw=db.prepare('SELECT claimed_by_generation_uuid,minting_generation_uuid FROM queue_items WHERE qitem_id=?').get(input.authorizationId) as {claimed_by_generation_uuid:string|null;minting_generation_uuid:string|null}|undefined;
  let body:Record<string,unknown>|null=null;try{body=JSON.parse(auth?.body??'null');}catch{}
  if(!auth||auth.state!=='in-progress'||auth.destinationSession!==actor||!auth.claimedAt||raw?.claimed_by_generation_uuid!==generation||!body||Array.isArray(body)||Object.keys(body).sort().join(',')!=='expiresAt,kind,operationId,operatorGeneration,purpose,qitemId,sourceFactsHash'||body.kind!=='queue-recovery-inspection-authorization.v1'||body.purpose!=='inspect-protected-obligation'||body.qitemId!==input.qitemId||body.sourceFactsHash!==input.sourceFactsHash||body.operationId!==input.operationId||body.operatorGeneration!==generation||typeof body.expiresAt!=='number'||!Number.isSafeInteger(body.expiresAt)||!Number.isFinite(now.getTime())||!Number.isFinite(Date.parse(auth.tsCreated))||Date.parse(auth.tsCreated)>now.getTime()||body.expiresAt<=now.getTime()||body.expiresAt>Date.parse(auth.tsCreated)+1200000||(auth.expiresAt!==null&&(!Number.isFinite(Date.parse(auth.expiresAt))||Date.parse(auth.expiresAt)<=now.getTime())))fail('inspection_authorization_required','Exact finite current Operator-owned transport claim required');
  const ts=db.prepare('SELECT state,actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id').all(auth.qitemId) as {state:string;actor_session:string;identity_provenance:string|null}[];
  if(!ts.some(t=>t.state==='in-progress'&&t.actor_session===actor&&t.identity_provenance==='transport:v1'))fail('inspection_authorization_provenance','Genuine Operator claim required');
  const created=ts.find(t=>t.state==='pending'),human=['owner@local','operator-human@kernel','owner-human@kernel'].includes(auth.sourceSession),authorGen=repo.coordinatorAuthority.generation(auth.sourceSession);
  if(!created||created.actor_session!==auth.sourceSession||(human?created.identity_provenance!=='transport:v1':!authorGen||authorGen!==raw?.minting_generation_uuid||created.identity_provenance!=='transport:v1'))fail('inspection_authorization_provenance','Current attributed author required');
  const existing=db.prepare("SELECT qitem_id,body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.inspection.operationId')=?").all(input.operationId) as {qitem_id:string;body:string}[];
  if(existing.length){if(existing.length!==1)fail('inspection_replay_conflict','Ambiguous operation');const parent=repo.getById(existing[0]!.qitem_id)!;const parsed=JSON.parse(existing[0]!.body),prior=parsed.inspection;
   const intent=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+parent.qitemId) as {sender_session:string;destination_session:string;audit_pointer:string;identity_provenance:string;tags:string|null}|undefined;
   let intentTags:unknown=null;try{intentTags=JSON.parse(intent?.tags??'null');}catch{}
   if(parent.sourceSession!=='watchdog@system'||parent.destinationSession!==OP||!parent.tags?.includes('wake-ladder-accountability')||parsed.action!=='reconcile-refused-wake-ladder'||parsed.recipientGeneration!==generation||parsed.original?.factsHash!==input.sourceFactsHash||!intent||intent.sender_session!=='watchdog@system'||intent.destination_session!==OP||intent.audit_pointer!==parent.qitemId||intent.identity_provenance!=='system:operator-authorized-coordination'||!Array.isArray(intentTags)||!intentTags.includes('queue:recipient-generation:'+generation))fail('inspection_replay_conflict','Canonical attributed control and bound intent required');
   if(prior?.requestHash!==requestHash||prior?.authorizationId!==input.authorizationId||!protection||prior.protectionCode!==protection.code||prior.protectionHash!==hash(protection.fingerprint)||prior.targetHash!==hash(JSON.stringify(target)))fail('inspection_replay_conflict','Changed operation');return {disposition:'existing-control' as const,recoveryQueueId:existing[0]!.qitem_id,protectionCode:prior.protectionCode,created:false};}
  if(!protection)return {disposition:'unprotected' as const,recoveryQueueId:null,protectionCode:null,created:false};
  const inspection={operationId:input.operationId,authorizationId:input.authorizationId,sourceFactsHash:input.sourceFactsHash,requestHash,actor,generation,protectionCode:protection.code,protectionHash:hash(protection.fingerprint),targetHash:hash(JSON.stringify(target))};
  const staged=stageRefusalRecovery({db,queueRepo:repo},{row:current,kind:'operator-protection-inspection',evidenceAt:auth.tsCreated,route:current.destinationSession},protection.code,now,inspection);
  return {disposition:'created-control' as const,recoveryQueueId:staged.qitemId,protectionCode:protection.code,created:true};
 }).immediate());
 let deliveryAttempted=false,deliveryError:string|null=null;
 if(result.created&&result.recoveryQueueId){deliveryAttempted=true;try{await repo.deliverWakeForSuccessor(result.recoveryQueueId,OP,true,'watchdog@system');}catch{deliveryError='inspection_control_delivery_failed';}}
 return {...result,operationId:input.operationId,originalMutations:0 as const,deliveryAttempted,deliveryError,pickup:'unverified' as const};
}
