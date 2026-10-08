import { createHash, randomUUID } from "node:crypto";
import type Database from "better-sqlite3";
import type { NativeDutyActor, NativeDutyGrant, NativeDutyIntent, NativeDutyOperationRequest, NativeDutyPrepareRequest, NativeDutyProof, NativeDutyScope, NativeDutyStatus, NativeDutyResumeRequest } from "./native-duty-contract.js";
import { nativeDutyLeaseMs } from "./native-duty-contract.js";

type GrantRow = { scope_id:string; scope_digest:string; scope_json:string; granted_by_session:string; granted_by_generation:string; granted_at:number; revoked_at:number|null };
type RegistrationRow = { registration_id:string; scope_id:string; launch_id:string; supervisor_pid:number; node_id:string; session_name:string; generation:string; runtime:"codex"|"pi"; configuration_digest:string; proof_fingerprint:string; phase:NativeDutyStatus["phase"]; last_heartbeat_at:number; observer_deadline:number; reason:string|null; created_at:number };
type IntentRow = { registration_id:string; operation_id:string; request_json:string; request_hash:string; body_digest:string; prepared_at:number; phase:NativeDutyIntent["phase"] };
export interface NativeDutyOperationReceipt { rigId:string; operationId:string; kind:string; requestHash:string; receiptDigest:string; receipt:unknown }
export class NativeDutyError extends Error { constructor(readonly code:string, message:string, readonly status=409) { super(message); } }
export interface NativeDutySupervisionOptions {
  db:Database.Database; now?:()=>number;
  approvedScope:(scopeId:string,candidate?:NativeDutyScope)=>NativeDutyScope|null;
  /** Temporary native custody exclusion; never invalidates a durable grant. */
  temporarilyExcluded?:(scope:NativeDutyScope)=>boolean;
  assertCurrentOperator:(actor:NativeDutyActor)=>void;
  observeNative:(scope:NativeDutyScope,launchId:string,supervisorPid:number)=>NativeDutyProof|null;
  assertResumeAuthority:(scope:NativeDutyScope,actor:NativeDutyActor,request:NativeDutyResumeRequest)=>void;
  operationReceipt:(rigId:string,operationId:string)=>NativeDutyOperationReceipt|null;
}
function canonical(v:unknown):string {
  if(Array.isArray(v))return "["+v.map(canonical).join(",")+"]";
  if(v!==null&&typeof v==="object"){const o=v as Record<string,unknown>;return "{"+Object.keys(o).sort().map(k=>JSON.stringify(k)+":"+canonical(o[k])).join(",")+"}";}
  return JSON.stringify(v);
}
const digest=(s:string)=>createHash("sha256").update(s).digest("hex");
function fail(code:string,message:string,status=409):never{throw new NativeDutyError(code,message,status);}
const same=(a:unknown,b:unknown)=>canonical(a)===canonical(b);
function exactKeys(v:unknown,keys:string[]):v is Record<string,unknown>{return v!==null&&typeof v==="object"&&!Array.isArray(v)&&same(Object.keys(v as object).sort(),[...keys].sort());}
const text=(v:unknown):v is string=>typeof v==="string"&&v.trim().length>0;
const scopeKeys=["scopeId","nodeId","sessionName","generation","runtime","rigId","configurationDigest","validUntil","maxLeaseMs","kind"];
const resumeKeys=["rigId","operationId","leaseMs","expectedEpoch","expectedObligationsDigest"];

/** Durable opt-in supervision for one already-authorized native holder. It never sends coordinator effects. */
export class NativeDutySupervisionService {
  private readonly db:Database.Database; private readonly now:()=>number;
  constructor(private readonly opts:NativeDutySupervisionOptions){this.db=opts.db;this.now=opts.now??Date.now;}

  grant(actor:NativeDutyActor,input:unknown):NativeDutyGrant {
    this.requireActor(actor);this.opts.assertCurrentOperator(actor);const scope=this.validateScope(input);
    const approved=this.opts.approvedScope(scope.scopeId,scope);if(!approved||!same(scope,approved))fail("native_duty_scope_not_approved","Scope must exactly match current approved work",403);
    this.assertNotTemporarilyExcluded(scope);
    const scopeDigest=digest(canonical(scope)),at=this.timestamp();
    const row=this.db.transaction(()=>{const old=this.grantRow(scope.scopeId);if(old){if(old.scope_digest!==scopeDigest||old.granted_by_session!==actor.session||old.granted_by_generation!==actor.generation)fail("native_duty_grant_conflict","Scope ID is already bound to another immutable grant");if(old.revoked_at!==null)fail("native_duty_grant_revoked","A revoked scope ID cannot be regranted");return old;}
      this.assertNoUnresolvedNodeIntent(scope.nodeId);
      this.db.prepare("INSERT INTO native_duty_grants(scope_id,scope_digest,scope_json,granted_by_session,granted_by_generation,granted_at,revoked_at) VALUES (?,?,?,?,?,?,NULL)").run(scope.scopeId,scopeDigest,canonical(scope),actor.session,actor.generation,at);return this.grantRow(scope.scopeId)!;}).immediate();
    return this.toGrant(row);
  }

  revoke(actor:NativeDutyActor,scopeId:string):NativeDutyGrant {
    this.requireActor(actor);this.opts.assertCurrentOperator(actor);if(!text(scopeId))fail("native_duty_scope_required","A scope ID is required",400);
    const row=this.db.transaction(()=>{const grant=this.grantRow(scopeId);if(!grant)fail("native_duty_grant_missing","No durable grant exists for this scope",404);if(grant.revoked_at===null){this.db.prepare("UPDATE native_duty_grants SET revoked_at=? WHERE scope_id=? AND revoked_at IS NULL").run(this.timestamp(),scopeId);this.db.prepare("UPDATE native_duty_registrations SET phase='stopped',reason='scope-revoked' WHERE scope_id=? AND phase!='stopped'").run(scopeId);}return this.grantRow(scopeId)!;}).immediate();
    return this.toGrant(row);
  }

  register(actor:NativeDutyActor,input:unknown):NativeDutyStatus {
    this.requireActor(actor);if(!exactKeys(input,["scopeId","launchId","supervisorPid"])||!text(input.scopeId)||!text(input.launchId)||!Number.isSafeInteger(input.supervisorPid)||Number(input.supervisorPid)<=0)fail("native_duty_register_request_invalid","Registration requires only scopeId, launchId and supervisorPid",400);
    const scopeId=String(input.scopeId),launchId=String(input.launchId),pid=Number(input.supervisorPid),grant=this.requireLiveGrant(scopeId),scope=JSON.parse(grant.scope_json) as NativeDutyScope;
    if(actor.session!==scope.sessionName||actor.generation!==scope.generation)fail("native_duty_actor_scope_mismatch","Authenticated native session and generation must match the granted scope",403);
    const old=this.db.prepare("SELECT * FROM native_duty_registrations WHERE scope_id=? AND launch_id=?").get(scopeId,launchId) as RegistrationRow|undefined;
    if(old){if(old.supervisor_pid!==pid||old.generation!==actor.generation||old.session_name!==actor.session)fail("native_duty_registration_conflict","Launch ID is already immutably bound to another native process");if(old.phase==="stopped"||old.phase==="held")fail("native_duty_registration_inactive","Held or stopped registrations cannot be reactivated");this.registrationProof(old,scope);return this.status(old.registration_id);}
    return this.db.transaction(()=>{
      this.assertNoUnresolvedNodeIntent(scope.nodeId);
      const proof=this.assertNativeProof(scope,launchId,pid),now=this.timestamp(),id=randomUUID();
      this.db.prepare("INSERT INTO native_duty_registrations(registration_id,scope_id,launch_id,supervisor_pid,node_id,session_name,generation,runtime,configuration_digest,proof_fingerprint,phase,last_heartbeat_at,observer_deadline,reason,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,'watching',?,?,NULL,?)").run(id,scopeId,launchId,pid,proof.nodeId,proof.sessionName,proof.generation,proof.runtime,proof.configurationDigest,proof.fingerprint,now,scope.validUntil,now);
      return this.status(id);
    }).immediate();
  }

  heartbeat(actor:NativeDutyActor,registrationId:string):NativeDutyStatus {
    const {row,scope}=this.liveRegistration(actor,registrationId),proof=this.registrationProof(row,scope),at=this.timestamp();
    this.db.prepare("UPDATE native_duty_registrations SET last_heartbeat_at=?,observer_deadline=?,proof_fingerprint=?,reason=NULL WHERE registration_id=? AND phase='watching'").run(at,scope.validUntil,proof.fingerprint,registrationId);return this.status(registrationId);
  }

  prepare(actor:NativeDutyActor,input:NativeDutyPrepareRequest):NativeDutyIntent {
    if(!exactKeys(input,["registrationId","request"])||!text(input.registrationId)||!exactKeys(input.request,resumeKeys)||!text(input.request.rigId)||!text(input.request.operationId)||input.request.operationId.length>160||!Number.isSafeInteger(input.request.leaseMs)||input.request.leaseMs<1000||input.request.leaseMs>3600000||!Number.isSafeInteger(input.request.expectedEpoch)||input.request.expectedEpoch<1||typeof input.request.expectedObligationsDigest!=="string"||!/^[a-f0-9]{64}$/.test(input.request.expectedObligationsDigest))fail("native_duty_prepare_request_invalid","Prepare requires the exact bounded resume-owned body: lease 1s..1h, epoch >=1, operation ID <=160 characters and SHA-256 obligations digest",400);
    const registrationId=String(input.registrationId),request=input.request as NativeDutyResumeRequest;
    const retained=this.registrationForActor(actor,registrationId);
    const prior=this.intent(registrationId,request.operationId);
    if(prior){if(!same(JSON.parse(prior.request_json),request))fail("native_duty_intent_conflict","Operation ID is already bound to another immutable request");return this.toIntent(prior);}
    this.assertNoUnresolvedNodeIntent(retained.node_id);
    const {row,scope}=this.liveRegistration(actor,registrationId);if(request.rigId!==scope.rigId)fail("native_duty_rig_mismatch","Resume request rig must match approved scope",403);
    const maxLease=nativeDutyLeaseMs(scope,this.timestamp());if(maxLease<=0||request.leaseMs>maxLease||request.leaseMs>3600000)fail("native_duty_lease_out_of_scope","Lease must fit inside the approved remaining scope window and resume-owned limit",403);
    this.registrationProof(row,scope);this.opts.assertResumeAuthority(scope,actor,request);
    const requestHash=digest(canonical({actor:actor.session,callerGeneration:actor.generation,input:request})),bodyDigest=digest(canonical(request)),preparedAt=this.timestamp();
    try{this.db.transaction(()=>{
      // Serialize node-wide exclusion with insertion; registration-local uniqueness is insufficient.
      this.assertNoUnresolvedNodeIntent(retained.node_id);
      this.db.prepare("INSERT INTO native_duty_intents(registration_id,operation_id,request_json,request_hash,body_digest,prepared_at,phase) VALUES (?,?,?,?,?,?,'prepared')").run(registrationId,request.operationId,canonical(request),requestHash,bodyDigest,preparedAt);
    }).immediate();}
    catch(error){if((error as {code?:string})?.code==="SQLITE_CONSTRAINT_UNIQUE")fail("native_duty_unresolved_intent","An earlier native effect intent remains unresolved");throw error;}
    return this.toIntent(this.intent(registrationId,request.operationId)!);
  }

  /** Write-ahead boundary: only the first prepared-to-in-flight transition may send once. */
  markInFlight(actor:NativeDutyActor,input:NativeDutyOperationRequest):{intent:NativeDutyIntent;maySendEffect:boolean} {
    this.validateOperation(input);const {row,scope}=this.liveRegistration(actor,input.registrationId);this.registrationProof(row,scope);
    const intent=this.intent(input.registrationId,input.operationId);if(!intent)fail("native_duty_intent_missing","Prepare must record the exact request before any effect");
    if(intent.phase!=="prepared")return {intent:this.toIntent(intent),maySendEffect:false};
    return this.db.transaction(()=>{
      const current=this.intent(input.registrationId,input.operationId)!;
      if(current.phase!=="prepared")return {intent:this.toIntent(current),maySendEffect:false};
      this.assertNoUnresolvedNodeIntent(row.node_id,input);
      const changed=this.db.prepare("UPDATE native_duty_intents SET phase='effect-in-flight' WHERE registration_id=? AND operation_id=? AND phase='prepared'").run(input.registrationId,input.operationId).changes;
      return {intent:this.toIntent(this.intent(input.registrationId,input.operationId)!),maySendEffect:changed===1};
    }).immediate();
  }

  reconcile(actor:NativeDutyActor,input:NativeDutyOperationRequest):NativeDutyIntent {
    this.validateOperation(input);const row=this.registration(input.registrationId);if(!row||row.session_name!==actor.session||row.generation!==actor.generation)fail("native_duty_registration_actor_mismatch","Authenticated actor does not match retained registration",403);
    const intent=this.intent(input.registrationId,input.operationId);if(!intent)fail("native_duty_intent_missing","No retained intent exists for this operation",404);if(intent.phase==="receipt-confirmed")return this.toIntent(intent);
    const scope=JSON.parse(this.grantRow(row.scope_id)!.scope_json) as NativeDutyScope;let receipt:NativeDutyOperationReceipt|null=null;try{receipt=this.opts.operationReceipt(scope.rigId,input.operationId);}catch{/* unreadable is uncertainty, never absence */}
    let valid=false;if(receipt){const request=JSON.parse(intent.request_json) as NativeDutyResumeRequest,authority=receipt.receipt as Record<string,unknown>|null,expectedHash=digest(canonical({actor:row.session_name,callerGeneration:row.generation,input:request}));
      valid=receipt.rigId===scope.rigId&&receipt.operationId===input.operationId&&receipt.kind==="resume-owned"&&receipt.requestHash===expectedHash&&text(receipt.receiptDigest)&&authority!==null&&typeof authority==="object"&&authority.rig_id===scope.rigId&&authority.owner_session===row.session_name&&authority.owner_generation===row.generation&&authority.state==="active"&&Number.isSafeInteger(authority.epoch)&&Number(authority.epoch)===request.expectedEpoch&&Number.isSafeInteger(authority.lease_until)&&Number(authority.lease_until)>0;
      if(valid&&digest(canonical(receipt.receipt))!==receipt.receiptDigest)valid=false;
    }
    const phase=valid?"receipt-confirmed":"uncertainty-held";this.db.prepare("UPDATE native_duty_intents SET phase=? WHERE registration_id=? AND operation_id=? AND phase IN ('prepared','effect-in-flight','uncertainty-held')").run(phase,input.registrationId,input.operationId);
    if(!valid)this.db.prepare("UPDATE native_duty_registrations SET phase='held',reason='operation-receipt-unconfirmed' WHERE registration_id=? AND phase IN ('watching','awaiting-native-proof')").run(input.registrationId);
    return this.toIntent(this.intent(input.registrationId,input.operationId)!);
  }

  stop(actor:NativeDutyActor,input:{registrationId:string;reason:string}):NativeDutyStatus {
    if(!exactKeys(input,["registrationId","reason"])||!text(input.registrationId)||!text(input.reason))fail("native_duty_stop_request_invalid","Stop requires only registrationId and reason",400);
    const row=this.registration(input.registrationId);if(!row||row.session_name!==actor.session||row.generation!==actor.generation)fail("native_duty_registration_actor_mismatch","Authenticated actor does not match retained registration",403);
    this.db.prepare("UPDATE native_duty_registrations SET phase='stopped',reason=? WHERE registration_id=? AND phase!='stopped'").run(input.reason.slice(0,240),input.registrationId);return this.status(input.registrationId);
  }

  status(registrationId:string):NativeDutyStatus {
    const row=this.registration(registrationId);if(!row)fail("native_duty_registration_missing","Registration not found",404);const grant=this.grantRow(row.scope_id);if(!grant)fail("native_duty_grant_missing","Registration grant is missing");
    const intent=this.latestIntent(registrationId);return {registrationId,scope:JSON.parse(grant.scope_json),scopeDigest:grant.scope_digest,launchId:row.launch_id,phase:row.phase,lastHeartbeatAt:row.last_heartbeat_at,observerDeadline:row.observer_deadline,reason:row.reason,intent:intent?this.toIntent(intent):null};
  }

  /** Retained intent debt belongs to the logical node, even after revocation or a new generation. */
  private assertNoUnresolvedNodeIntent(nodeId:string,own?:NativeDutyOperationRequest):void {
    const sql="SELECT i.operation_id FROM native_duty_intents i JOIN native_duty_registrations r ON r.registration_id=i.registration_id WHERE r.node_id=? AND i.phase!='receipt-confirmed'";
    const unresolved=own
      ? this.db.prepare(sql+" AND NOT (i.registration_id=? AND i.operation_id=?) LIMIT 1").get(nodeId,own.registrationId,own.operationId)
      : this.db.prepare(sql+" LIMIT 1").get(nodeId);
    if(unresolved)fail("native_duty_unresolved_intent","An earlier native effect intent for this node remains unresolved");
  }

  private liveRegistration(actor:NativeDutyActor,id:string):{row:RegistrationRow;scope:NativeDutyScope} {
    const row=this.registrationForActor(actor,id);if(row.phase!=="watching")fail("native_duty_registration_inactive","Only a watching registration can start new effects");
    const grant=this.requireLiveGrant(row.scope_id),scope=JSON.parse(grant.scope_json) as NativeDutyScope,approved=this.opts.approvedScope(scope.scopeId);if(!approved||!same(scope,approved)){this.db.prepare("UPDATE native_duty_registrations SET phase='stopped',reason='approved-scope-changed' WHERE registration_id=? AND phase!='stopped'").run(id);fail("native_duty_scope_changed","Approved scope changed; registration is stopped");}return {row,scope};
  }
  private registrationForActor(actor:NativeDutyActor,id:string):RegistrationRow {
    this.requireActor(actor);const row=this.registration(id);if(!row)fail("native_duty_registration_missing","Registration not found",404);if(row.session_name!==actor.session||row.generation!==actor.generation)fail("native_duty_registration_actor_mismatch","Authenticated actor does not match retained registration generation",403);return row;
  }
  private requireLiveGrant(id:string):GrantRow {
    const grant=this.grantRow(id);if(!grant)fail("native_duty_grant_missing","No opt-in grant exists for this scope",403);if(grant.revoked_at!==null){this.stopScope(id,"scope-revoked");fail("native_duty_grant_revoked","Scope grant has been revoked",403);}const scope=JSON.parse(grant.scope_json) as NativeDutyScope;if(this.timestamp()>=scope.validUntil){this.stopScope(id,"scope-expired");fail("native_duty_scope_expired","Approved scope window has expired",403);}
    try{this.opts.assertCurrentOperator({session:grant.granted_by_session,generation:grant.granted_by_generation});}catch{this.stopScope(id,"grantor-generation-retired");fail("native_duty_grantor_retired","Operator generation that granted this scope is no longer current",403);}const approved=this.opts.approvedScope(id);if(!approved||!same(scope,approved)){this.stopScope(id,"approved-scope-changed");fail("native_duty_scope_changed","Current approved scope no longer matches stored grant",403);}this.assertNotTemporarilyExcluded(scope);return grant;
  }
  private assertNotTemporarilyExcluded(scope:NativeDutyScope):void {
    if(this.opts.temporarilyExcluded?.(scope))fail("native_duty_temporary_exclusion","Native lifecycle or reservation currently excludes continuation");
  }
  private stopScope(id:string,reason:string):void{this.db.prepare("UPDATE native_duty_registrations SET phase='stopped',reason=? WHERE scope_id=? AND phase!='stopped'").run(reason,id);}
  private assertNativeProof(scope:NativeDutyScope,launchId:string,pid:number):NativeDutyProof {
    let proof:NativeDutyProof|null;try{proof=this.opts.observeNative(scope,launchId,pid);}catch{proof=null;}if(!proof)fail("native_duty_proof_unavailable","Current native process proof is unavailable; no effect is permitted");
    const ok=proof.nativePresent&&proof.supervisorIsNativeAncestor&&proof.nodeId===scope.nodeId&&proof.sessionName===scope.sessionName&&proof.generation===scope.generation&&proof.runtime===scope.runtime&&proof.launchId===launchId&&proof.supervisorPid===pid&&proof.configurationDigest===scope.configurationDigest&&text(proof.fingerprint)&&Number.isSafeInteger(proof.observedAt)&&proof.observedAt<=this.timestamp();if(!ok)fail("native_duty_proof_mismatch","Independent native identity, configuration or lifecycle proof does not match grant",403);if(proof.lifecycleReserved)fail("native_duty_temporary_exclusion","Native proof observes a temporary lifecycle exclusion");return proof;
  }
  private registrationProof(row:RegistrationRow,scope:NativeDutyScope):NativeDutyProof {
    try{return this.assertNativeProof(scope,row.launch_id,row.supervisor_pid);}
    catch(error){
      const code=error instanceof NativeDutyError?error.code:"native-duty-proof-unavailable";
      if(code==="native_duty_temporary_exclusion")throw error; // No durable phase change for an observed custody hold.
      this.db.prepare("UPDATE native_duty_registrations SET phase='held',reason=? WHERE registration_id=? AND phase='watching'").run(code,row.registration_id);
      throw error;
    }
  }
  private validateScope(input:unknown):NativeDutyScope {
    if(!exactKeys(input,scopeKeys)||!text(input.scopeId)||!text(input.nodeId)||!text(input.sessionName)||!text(input.generation)||!text(input.rigId)||!text(input.configurationDigest)||!["codex","pi"].includes(String(input.runtime))||input.kind!=="holder-continuation"||!Number.isSafeInteger(input.validUntil)||!Number.isSafeInteger(input.maxLeaseMs)||Number(input.maxLeaseMs)<=0)fail("native_duty_scope_invalid","Grant requires exact approved holder-continuation scope fields",400);return input as unknown as NativeDutyScope;
  }
  private validateOperation(input:NativeDutyOperationRequest):void{if(!exactKeys(input,["registrationId","operationId"])||!text(input.registrationId)||!text(input.operationId))fail("native_duty_operation_request_invalid","Operation requires only registrationId and operationId",400);}
  private requireActor(actor:NativeDutyActor):void{if(!text(actor?.session)||!text(actor?.generation))fail("native_duty_actor_required","Authenticated transport session and generation are required",403);}
  private timestamp():number{const n=this.now();if(!Number.isSafeInteger(n)||n<0)fail("native_duty_clock_invalid","Clock returned an invalid timestamp",500);return n;}
  private grantRow(id:string):GrantRow|undefined{return this.db.prepare("SELECT * FROM native_duty_grants WHERE scope_id=?").get(id) as GrantRow|undefined;}
  private registration(id:string):RegistrationRow|undefined{return this.db.prepare("SELECT * FROM native_duty_registrations WHERE registration_id=?").get(id) as RegistrationRow|undefined;}
  private intent(reg:string,op:string):IntentRow|undefined{return this.db.prepare("SELECT * FROM native_duty_intents WHERE registration_id=? AND operation_id=?").get(reg,op) as IntentRow|undefined;}
  private latestIntent(reg:string):IntentRow|undefined{return this.db.prepare("SELECT * FROM native_duty_intents WHERE registration_id=? ORDER BY prepared_at DESC LIMIT 1").get(reg) as IntentRow|undefined;}
  private toGrant(r:GrantRow):NativeDutyGrant{return {scope:JSON.parse(r.scope_json),scopeDigest:r.scope_digest,grantedBy:{session:r.granted_by_session,generation:r.granted_by_generation},grantedAt:r.granted_at,revokedAt:r.revoked_at};}
  private toIntent(r:IntentRow):NativeDutyIntent{return {operationId:r.operation_id,request:JSON.parse(r.request_json),bodyDigest:r.body_digest,preparedAt:r.prepared_at,phase:r.phase};}
}
