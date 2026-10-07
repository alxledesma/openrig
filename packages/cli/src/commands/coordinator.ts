import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { Command } from "commander";
import { DaemonClient, terminalAuthHeaders } from "../client.js";

const DEFAULT_READ_TIMEOUT_MS = 30_000;
const MAX_QUALIFICATION_DUTY_MS = 20 * 60_000;
const SHA256_HEX = /^[a-f0-9]{64}$/;
const SHA256_CONTRACT = /^sha256:[a-f0-9]{64}$/;
function readTimeout(value:string):number|undefined {
 if(!/^\d+$/.test(value))return undefined;
 const ms=Number(value);
 return Number.isSafeInteger(ms)&&ms>=1_000&&ms<=60_000?ms:undefined;
}

function isRecord(value:unknown):value is Record<string,unknown> {
 return typeof value==="object"&&value!==null&&!Array.isArray(value);
}
function hasExactKeys(value:Record<string,unknown>,keys:readonly string[]):boolean {
 const actual=Object.keys(value);
 return actual.length===keys.length&&keys.every(key=>Object.hasOwn(value,key));
}
function boundedText(value:unknown,max=256):value is string {
 return typeof value==="string"&&value.trim().length>0&&value.length<=max;
}
function boundedDutyDeadline(value:unknown):boolean {
 const now=Date.now();
 return Number.isSafeInteger(value)&&(value as number)>now&&(value as number)<=now+MAX_QUALIFICATION_DUTY_MS;
}

type RecoveryKind = "reconciliation" | "expired";
type RecoveryEnvelope = {
 schema:"coordinator-recovery-envelope.v1"; kind:RecoveryKind; operation:string;
 endpoint:string; rigId:string; request:Record<string,unknown>;
};
const recoveryOperation = (kind:RecoveryKind) => kind==="reconciliation"?"reconciliation-recover":"expired-window-recover";
const validWindow = (value:unknown):value is number => Number.isSafeInteger(value)&&Number(value)>=10000&&Number(value)<=900000;
const validEpoch = (value:unknown):value is number => Number.isSafeInteger(value)&&Number(value)>=1;
function recoveryEnvelopeValid(value:unknown,rigId:string,kind:RecoveryKind):value is RecoveryEnvelope {
 if(!isRecord(value)||!hasExactKeys(value,["schema","kind","operation","endpoint","rigId","request"])
   ||value.schema!=="coordinator-recovery-envelope.v1"||value.rigId!==rigId||value.kind!==kind
   ||value.operation!==recoveryOperation(kind)||value.endpoint!==`/api/coordinator/${recoveryOperation(kind)}`||!isRecord(value.request))return false;
 const r=value.request;
 if(!boundedText(r.operationId,160)||!validWindow(r.windowMs))return false;
 if(kind==="reconciliation")return hasExactKeys(r,["token","operationId","obligationsDigest","windowMs"])
   &&isRecord(r.token)&&hasExactKeys(r.token,["rigId","epoch","generation"])&&r.token.rigId===rigId
   &&validEpoch(r.token.epoch)&&boundedText(r.token.generation)&&typeof r.obligationsDigest==="string"&&SHA256_HEX.test(r.obligationsDigest);
 return hasExactKeys(r,["rigId","operationId","windowMs","expectedEpoch","expectedOwnerGeneration","expectedCustodyDigest","conflictOperationId","conflictKind"])
   &&r.rigId===rigId&&validEpoch(r.expectedEpoch)&&boundedText(r.expectedOwnerGeneration)
   &&typeof r.expectedCustodyDigest==="string"&&SHA256_HEX.test(r.expectedCustodyDigest)
   &&boundedText(r.conflictOperationId,160)&&r.conflictKind==="reconciliation-recover";
}

/** A prepared request is intent, not a grant. Only the daemon checks native custody and readiness. */
async function recoverWindow(rigId:string,opts:{kind:string;windowMs:string;readTimeoutMs:string;requestTimeoutMs:string;conflictOperationId?:string;replayContract?:string}):Promise<void> {
 const reject=(message:string)=>{process.stderr.write(`${message}; nothing was sent\n`);process.exitCode=1;};
 const kind=opts.kind,readMs=readTimeout(opts.readTimeoutMs),requestMs=readTimeout(opts.requestTimeoutMs);
 if(!boundedText(rigId)||(kind!=="reconciliation"&&kind!=="expired")||readMs===undefined||requestMs===undefined){reject("rigId, explicit kind and integer timeouts 1000-60000 are required");return;}
 if(opts.replayContract&&(opts.conflictOperationId!==undefined||opts.windowMs!=="900000")){reject("replay cannot reconstruct window or conflict fields");return;}
 const windowMs=Number(opts.windowMs);
 if(!opts.replayContract&&(!/^\d+$/.test(opts.windowMs)||!validWindow(windowMs)
   ||(kind==="expired"&&!boundedText(opts.conflictOperationId,160))||(kind==="reconciliation"&&opts.conflictOperationId!==undefined))){reject("window must be 10000-900000; expired requires conflict-operation-id, reconciliation forbids it");return;}
 const headers=terminalAuthHeaders();
 if(!headers.Authorization){reject("genuine terminal authentication is required");return;}
 const client=new DaemonClient();let envelope:RecoveryEnvelope;
 if(opts.replayContract){
  let value:unknown;
  try{const stat=fs.statSync(opts.replayContract);if(!stat.isFile()||stat.size>16384)throw new Error();value=JSON.parse(fs.readFileSync(opts.replayContract,"utf8"));}
  catch{reject("prepared envelope is unreadable or oversized");return;}
  if(!recoveryEnvelopeValid(value,rigId,kind)){reject("prepared envelope does not match the exact rig, kind, endpoint and bounded contract");return;}
  envelope=value; // No GET, regenerated operation ID, mutable-state reconstruction or automatic retry.
 }else{
  let shown;
  try{shown=await client.get(`/api/coordinator/${encodeURIComponent(rigId)}`,{headers,timeoutMs:readMs});}
  catch{reject("initial authority read failed");return;}
  const current=shown.data;
  if(shown.status<200||shown.status>=300||!isRecord(current)||!isRecord(current.authority)){reject("authority read failed or is incomplete");return;}
  const a=current.authority;
  if(a.rig_id!==rigId||a.state!=="reconciling"||!validEpoch(a.epoch)||!boundedText(a.owner_generation)||!boundedText(a.owner_session)
    ||!Number.isSafeInteger(a.lease_until)||Number(a.lease_until)>Date.now()||typeof current.obligationsDigest!=="string"||!SHA256_HEX.test(current.obligationsDigest)){
   reject("exact expired reconciling authority and custody digest are required");return;
  }
  const operationId=randomUUID();
  const request=kind==="reconciliation"
   ?{token:{rigId,epoch:a.epoch,generation:a.owner_generation},operationId,obligationsDigest:current.obligationsDigest,windowMs}
   :{rigId,operationId,windowMs,expectedEpoch:a.epoch,expectedOwnerGeneration:a.owner_generation,expectedCustodyDigest:current.obligationsDigest,conflictOperationId:opts.conflictOperationId!,conflictKind:"reconciliation-recover"};
  envelope={schema:"coordinator-recovery-envelope.v1",kind,operation:recoveryOperation(kind),endpoint:`/api/coordinator/${recoveryOperation(kind)}`,rigId,request};
 }
 let preparedPath:string,receiptPath:string;
 const readbackEndpoint=`/api/coordinator/${encodeURIComponent(rigId)}/operations/${encodeURIComponent(String(envelope.request.operationId))}`;
 const guidance="Read the original operation receipt first. A missing receipt or replay refusal does not prove the original effect absent. No automatic retry; the holder must resume-owned separately.";
 try{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),"openrig-recover-window-"));fs.chmodSync(dir,0o700);
  preparedPath=path.join(dir,"prepared-request.json");receiptPath=path.join(dir,"submission-receipt.json");
  fs.writeFileSync(preparedPath,`${JSON.stringify(envelope,null,2)}\n`,{mode:0o600});
  // A crash during the single POST leaves UNKNOWN durably, never an invented no-effect failure.
  fs.writeFileSync(receiptPath,`${JSON.stringify({status:"UNKNOWN",phase:"prepared-before-post",operationId:envelope.request.operationId,preparedContract:preparedPath,replayedFrom:opts.replayContract??null,readbackEndpoint,guidance},null,2)}\n`,{mode:0o600});
 }catch{reject("could not persist private intent and receipt");return;}
 process.stderr.write(`prepared request: ${preparedPath}\nreceipt: ${receiptPath}\noperation id: ${envelope.request.operationId}\nread-only operation endpoint: ${readbackEndpoint}\n${guidance}\n`);
 let result:Record<string,unknown>;
 try{
  const response=await client.post(envelope.endpoint,envelope.request,{headers,timeoutMs:requestMs});
  result={status:response.status>=200&&response.status<300?"COMMITTED":response.status>=400&&response.status<500?"REFUSED":"UNKNOWN",httpStatus:response.status,response:response.data};
 }catch{result={status:"UNKNOWN",phase:"post-response-unavailable"};}
 const receipt={...result,operationId:envelope.request.operationId,preparedContract:preparedPath,replayedFrom:opts.replayContract??null,readbackEndpoint,guidance};
 try{fs.writeFileSync(receiptPath,`${JSON.stringify(receipt,null,2)}\n`,{mode:0o600});}
 catch{process.stderr.write("receipt update failed; original private UNKNOWN receipt and request are retained\n");process.exitCode=1;}
 console.log(JSON.stringify({...receipt,receiptPath},null,2));if(result.status!=="COMMITTED")process.exitCode=1;
}
/** Local shape bounds prevent malformed or overbroad assessment contracts from reaching the API.
 * The daemon remains authoritative for identity, custody, wake effects, plan and lease fences. */
function qualificationDutyInputError(operation:string,value:unknown):string|undefined {
 if(!isRecord(value))return "contract must be a JSON object";
 if(operation==="qualification-assessment-stage"){
  const keys=["rigId","worker","workerGeneration","configurationDigest","deadline","contract"] as const;
  if(!hasExactKeys(value,keys))return "stage contract must contain only rigId, worker, workerGeneration, configurationDigest, deadline, and contract";
  const c=value.contract;
  if(!boundedText(value.rigId)||!boundedText(value.worker)||!boundedText(value.workerGeneration)
    ||typeof value.configurationDigest!=="string"||!SHA256_HEX.test(value.configurationDigest)||!boundedDutyDeadline(value.deadline)||!isRecord(c)
    ||!hasExactKeys(c,["schema","artifactRef","artifactSha256","taskDigest","scope","productAuthority"])
    ||c.schema!=="qualification-assessment-contract.v1"||!boundedText(c.artifactRef,2048)
    ||typeof c.artifactSha256!=="string"||!SHA256_CONTRACT.test(c.artifactSha256)
    ||typeof c.taskDigest!=="string"||!SHA256_CONTRACT.test(c.taskDigest)
    ||c.scope!=="qualification-only"||c.productAuthority!==false)
   return "stage requires the exact native Worker binding and qualification-only artifact contract; nothing was sent";
 }
 if(operation==="qualification-assessment-retirement-stage"){
  const legacyKeys=["rigId","targetQueueId","targetBodyHash","sweepFindingQueueId","sweepFindingBodyHash","deadline"] as const;
  const accountabilityKeys=["rigId","targetQueueId","targetBodyHash","evidenceKind","accountabilityControlQueueId","accountabilityControlBodyHash","deadline"] as const;
  const common=boundedText(value.rigId)&&boundedText(value.targetQueueId)&&typeof value.targetBodyHash==="string"&&SHA256_HEX.test(value.targetBodyHash)&&boundedDutyDeadline(value.deadline);
  const legacy=hasExactKeys(value,legacyKeys)&&common&&boundedText(value.sweepFindingQueueId)&&typeof value.sweepFindingBodyHash==="string"&&SHA256_HEX.test(value.sweepFindingBodyHash);
  const accountability=hasExactKeys(value,accountabilityKeys)&&common&&value.evidenceKind==="operator-accountability"&&boundedText(value.accountabilityControlQueueId)&&typeof value.accountabilityControlBodyHash==="string"&&SHA256_HEX.test(value.accountabilityControlBodyHash);
  if(!legacy&&!accountability)return "retirement stage requires an exact failed-wake finding or claimed Operator accountability evidence, target hashes, and a deadline within twenty minutes; nothing was sent";
 }
 if(operation==="qualification-assessment-uncertainty-dispose"){
  const keys=["rigId","rows","deadline"] as const;
  if(!hasExactKeys(value,keys)||!boundedText(value.rigId)||!boundedDutyDeadline(value.deadline)||!Array.isArray(value.rows)||value.rows.length<1||value.rows.length>4
    ||value.rows.some(row=>!isRecord(row)||!hasExactKeys(row,["targetQueueId","targetBodyHash","sweepFindingQueueId","sweepFindingBodyHash"])
      ||!boundedText(row.targetQueueId)||!boundedText(row.sweepFindingQueueId)
      ||typeof row.targetBodyHash!=="string"||!SHA256_HEX.test(row.targetBodyHash)
      ||typeof row.sweepFindingBodyHash!=="string"||!SHA256_HEX.test(row.sweepFindingBodyHash)))
   return "uncertainty disposition requires one to four exact expired legacy target/sweep pairs and a deadline within twenty minutes; it never retries or asserts failed delivery";
 }
 if(operation==="qualification-assessment-return"){
  if(!hasExactKeys(value,["rigId","dutyQueueId","returnQueueId"])||!boundedText(value.rigId)
    ||!boundedText(value.dutyQueueId)||!boundedText(value.returnQueueId))
   return "return requires only rigId, dutyQueueId, and the Worker-authored returnQueueId; nothing was sent";
 }
 if(operation==="qualification-assessment-review"){
  if(!hasExactKeys(value,["rigId","dutyQueueId","finding","evidenceRef"])||!boundedText(value.rigId)
    ||!boundedText(value.dutyQueueId)||!boundedText(value.evidenceRef,2048)
    ||typeof value.finding!=="string"||!["evidence-sufficient","evidence-insufficient","inconclusive"].includes(value.finding))
   return "review requires the exact duty, a bounded evidence reference, and a supported finding; no qualification is inferred";
 }
 return undefined;
}

/** Exact JSON contracts keep authority and evidence explicit; no implied automatic takeover. */
export function coordinatorCommand():Command {
 const cmd=new Command("coordinator").description("Rig-scoped durable coordinator authority and admitted packages");
 cmd.command("recover-window <rigId>")
  .description("Derive ONE explicit expired reconciling-window recovery under genuine native auth; persist intent before a single POST. Read operation receipt before any deliberate exact replay.")
  .requiredOption("--kind <kind>","Explicit reconciliation or expired recovery; never auto-selected")
  .option("--window-ms <ms>","Bounded acknowledgment window (10000-900000)","900000")
  .option("--conflict-operation-id <id>","Original spent reconciliation recovery operation; required only for expired kind")
  .option("--replay-contract <file>","Exact prepared envelope; no authority read, new operation ID or automatic resend")
  .option("--read-timeout-ms <ms>","Initial authority read timeout (1000-60000)",String(DEFAULT_READ_TIMEOUT_MS))
  .option("--request-timeout-ms <ms>","Single POST timeout (1000-60000)",String(DEFAULT_READ_TIMEOUT_MS))
  .action(recoverWindow);
 cmd.command("show <rigId>")
  .option("--read-timeout-ms <ms>","Read timeout in milliseconds (1000-60000)",String(DEFAULT_READ_TIMEOUT_MS))
  .action(async(rigId:string,opts:{readTimeoutMs:string})=>{
   const timeoutMs=readTimeout(opts.readTimeoutMs);
   if(timeoutMs===undefined){process.stderr.write("read-timeout-ms must be an integer 1000-60000\n");process.exitCode=1;return;}
   const res=await new DaemonClient().get(`/api/coordinator/${encodeURIComponent(rigId)}`, { headers: terminalAuthHeaders(), timeoutMs });
   console.log(JSON.stringify(res.data,null,2));if(res.status>=400)process.exitCode=1;
 });
 // resume-owned takes a rigId, not a contract file: the daemon derives the caller's token
 // and current obligations digest, so the native Lead supplies no digest and no epoch.
 cmd.command("expired-window-recover <contractFile>")
  .description("Operator-attributed bounded recovery for ONE already expired reconciling window, at epoch plus one, with no admission, qualification or dispatch. Requires the original spent recovery receipt, a durable different-kind operation-ID conflict, exact custody, a delivery guard that is desired AND effective, and the service's own freshly refreshed positive native quiescence observation. The body carries no proof: the daemon reads its own native evidence. The holder must still resume-owned separately.")
  .action(async(file:string)=>{
   const contract=JSON.parse(fs.readFileSync(file,"utf8"));
   const client=new DaemonClient(),headers=terminalAuthHeaders();
   // Prepare the exact request at owner-only permissions before the single POST, and announce the
   // path and id first. Nothing retries: an unknown outcome is resolved with --replay-contract.
   const dir=fs.mkdtempSync(path.join(os.tmpdir(),"openrig-expired-window-"));
   fs.chmodSync(dir,0o700);
   const preparedPath=path.join(dir,"prepared-request.json");
   fs.writeFileSync(preparedPath,`${JSON.stringify(contract,null,2)}\n`,{mode:0o600});
   process.stderr.write(`prepared request: ${preparedPath}\n`);
   const res=await client.post("/api/coordinator/expired-window-recover",contract,{ headers });
   const receipt:unknown=res.data;
   console.log(JSON.stringify({...(receipt&&typeof receipt==="object"?receipt:{}),preparedContract:preparedPath},null,2));if(res.status>=400)process.exitCode=1;
  });
 cmd.command("resume-owned <rigId>")
  .description("Continue your OWN live coordinator authority: atomically acknowledge a reconciling owner and renew it. Reads the current authority once to derive the expected epoch and obligations digest, so you supply neither. Never recovers an expired lease.")
  .option("--operation-id <id>","Stable durable receipt id. Omitted by default, which generates a fresh id so an operator never reuses one.")
  .option("--replay-contract <file>","Submit a previously PREPARED request file exactly as prepared, under genuine native auth. Performs no read, generates no new id and retries nothing. Use after a timeout or unknown outcome.")
  .option("--lease-ms <ms>","Renewed lease in milliseconds (1000-3600000)","1200000")
  .option("--read-timeout-ms <ms>","Initial authority read timeout in milliseconds (1000-60000)",String(DEFAULT_READ_TIMEOUT_MS))
  .action(async(rigId:string,opts:{operationId?:string;replayContract?:string;leaseMs:string;readTimeoutMs:string})=>{
   const client=new DaemonClient(),headers=terminalAuthHeaders();
   const show=(b:unknown)=>{console.log(JSON.stringify(b,null,2));process.exitCode=1;};
   const timeoutMs=readTimeout(opts.readTimeoutMs);
   if(timeoutMs===undefined){process.stderr.write("read-timeout-ms must be an integer 1000-60000\n");process.exitCode=1;return;}
   if(opts.replayContract){
    // Controlled replay: submit the prepared request byte-for-byte. A fresh read here would build a
    // DIFFERENT CAS contract, which is unsafe reconstruction of an unknown effect.
    let prepared:Record<string,unknown>;
    try{prepared=JSON.parse(fs.readFileSync(opts.replayContract,"utf8"));}
    catch{process.stderr.write("replay contract file is unreadable; nothing was sent\n");process.exitCode=1;return;}
    if(prepared.rigId!==rigId||typeof prepared.operationId!=="string"||!prepared.operationId.trim()
      ||typeof prepared.expectedEpoch!=="number"||!Number.isSafeInteger(prepared.expectedEpoch)
      ||typeof prepared.expectedObligationsDigest!=="string"||!prepared.expectedObligationsDigest
      ||!Number.isSafeInteger(Number(prepared.leaseMs))){process.stderr.write("replay contract file does not hold a complete prepared request; nothing was sent\n");process.exitCode=1;return;}
    process.stderr.write(`replaying prepared request ${opts.replayContract} as operation ${prepared.operationId}\n`);
    const res=await client.post(`/api/coordinator/resume-owned`,prepared,{ headers });
    const receipt:unknown=res.data;
    console.log(JSON.stringify({...(receipt&&typeof receipt==="object"?receipt:{}),operationId:prepared.operationId,operationIdSource:"replayed",replayedFrom:opts.replayContract},null,2));if(res.status>=400)process.exitCode=1;
    return;
   }
   const leaseMs=Number(opts.leaseMs);
   if(!Number.isSafeInteger(leaseMs)||leaseMs<1000||leaseMs>3600000){process.stderr.write("leaseMs must be an integer 1000-3600000\n");process.exitCode=1;return;}
   const generated=!opts.operationId;
   // A fresh generated id removes the reused-operation-id failure at its source. There is NO
   // automatic retry: a failed or timed-out call is retried deliberately with --replay-contract.
   const operationId=opts.operationId??randomUUID();
   // Read the CURRENT supported authority once and derive the expected contract from it, so the
   // operator never invents either field. Exactly one read and one POST: no polling and no retry.
   let shown;
   try{shown=await client.get(`/api/coordinator/${encodeURIComponent(rigId)}`,{ headers, timeoutMs });}
   catch(error){
    const detail=error instanceof Error?error.message:String(error);
    show({status:"PRE_EFFECT_READ_FAILED",phase:"initial_authority_read",postAttempts:0,preparedRequest:false,
     diagnostic:detail,continuation:"No write was attempted and no request was prepared. After confirming the daemon is responsive, start a fresh resume-owned command to read current authority and derive a new request."});
    return;
   }
   if(shown.status>=400){show(shown.data);return;}
   const current=shown.data as {authority?:{epoch?:unknown};obligationsDigest?:unknown};
   const expectedEpoch=Number(current.authority?.epoch);
   const expectedObligationsDigest=typeof current.obligationsDigest==="string"?current.obligationsDigest:"";
   if(!Number.isSafeInteger(expectedEpoch)||!expectedObligationsDigest){process.stderr.write("authority read did not supply a current epoch and obligations digest; nothing was sent\n");process.exitCode=1;return;}
   // Persist the EXACT request before touching the network, so a lost response can never lose the
   // original contract. Owner-only permissions, in a fresh private directory, and it holds no
   // credentials: the request carries a rig, a lease and a digest.
   const request={rigId,leaseMs,operationId,expectedEpoch,expectedObligationsDigest};
   let preparedPath:string;
   try{
    const dir=fs.mkdtempSync(path.join(os.tmpdir(),"openrig-resume-owned-"));
    fs.chmodSync(dir,0o700);
    preparedPath=path.join(dir,"prepared-request.json");
    fs.writeFileSync(preparedPath,`${JSON.stringify(request,null,2)}\n`,{mode:0o600});
   }catch{process.stderr.write("could not persist the prepared request; nothing was sent\n");process.exitCode=1;return;}
   process.stderr.write(`prepared request: ${preparedPath}\noperation id: ${operationId}\nretry after an unknown outcome with: rig coordinator resume-owned ${rigId} --replay-contract ${preparedPath}\n`);
   const res=await client.post(`/api/coordinator/resume-owned`,request,{ headers });
   const receipt:unknown=res.data;
   console.log(JSON.stringify({...(receipt&&typeof receipt==="object"?receipt:{}),operationId,operationIdSource:generated?"generated":"supplied",preparedContract:preparedPath,expectedEpoch,expectedObligationsDigest},null,2));if(res.status>=400)process.exitCode=1;
  });

for(const op of ["active-expiry-recover","held-history-adopt","held-history-recovery-bind","held-history-custody-evidence","held-history-custody-attest","outbox-abandon-evidence","outbox-abandon-continue","outcome-qualification-refresh","outcome-recovery-bind","outbox-abandon-authorize","outbox-abandon-notify","enable","transfer","acknowledge","renew","admit","dispose","recover","legacy-inventory","migrate-legacy","diagnostic-wake-dispose","coordination-plan","coordination-reconcile","coordination-accept","coordination-continue-custody","outcome-configure","resilience-materialize","reconciliation-recover","coordination-worker-probe","coordination-return-successor","coordination-return-continue","coordination-return-retire","coordination-return-intake-refresh","coordination-lifecycle-recovery","coordination-frontier-plan","coordination-frontier-admit","coordination-frontier-confirm","coordination-frontier-boundary","coordination-frontier-legacy-classify","coordination-frontier-legacy-revoke","qualification-assessment-stage","qualification-assessment-retirement-stage","qualification-assessment-uncertainty-dispose","qualification-assessment-return","qualification-assessment-review"]){
   cmd.command(`${op} <contractFile>`).description(op==="dispose"?'Submit {"rigId":"...","packageKey":"original admitted package key","dispositionId":"new worker-authored JSON return queue ID"}. The original worker may dispose its own terminal return; holder role is not required.':"Submit exact frozen JSON contract; caller identity/generation derive from seat environment")
    .action(async(file:string)=>{
     let contract:Record<string,unknown>;
     try{contract=JSON.parse(fs.readFileSync(file,"utf8"));}
     catch{process.stderr.write("contract file is unreadable or invalid JSON; nothing was sent\n");process.exitCode=1;return;}
     if(!isRecord(contract)){process.stderr.write("contract must be a JSON object; nothing was sent\n");process.exitCode=1;return;}
     const invalid=qualificationDutyInputError(op,contract);
     if(invalid){process.stderr.write(`${invalid}\n`);process.exitCode=1;return;}
     if(op==="dispose"&&["rigId","packageKey","dispositionId"].some(k=>typeof contract?.[k]!=="string"||!contract[k].trim()))throw new Error("dispose requires JSON {rigId,packageKey,dispositionId}; dispositionId is the NEW worker-authored typed-return queue item, not the original assignment or duty ID");
     const res=await new DaemonClient().post(`/api/coordinator/${op}`,contract, { headers: terminalAuthHeaders() });
     console.log(JSON.stringify(res.data,null,2));if(res.status>=400)process.exitCode=1;
    });
 }
 return cmd;
}
