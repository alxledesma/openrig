import type Database from "better-sqlite3";
import { createHash, randomUUID } from "node:crypto";
import { closeSync, constants, existsSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import path from "node:path";
import type { CodexResumeAdapter } from "../adapters/codex-resume.js";
import type { SeatDeliveryGuard } from "./seat-delivery-guard.js";
import type { ActivityEvidence } from "./activity-taxonomy.js";
import { listNativeProcesses, observeCodexPaneProcess, type NativeProcessRow } from "./native-process-lineage.js";
import { verifyNativeDutyProcessIdentity } from "./native-duty-launch.js";
import { rotationLocalAddresses } from "./rotation-local-custody.js";
import { forEachJsonlLine } from "./rotation-native-proof.js";

export interface CodexRehostBinding {
  nodeId:string;sessionId:string;sessionName:string;generation:string;runtime:"codex";nativeId:string;
  cwd:string;model:string;effort:string|null;codexConfigProfile:string;
}
export interface CodexRehostNativeState {
  nodeId:string;sessionName:string;nativeId:string;transcriptPath:string;
  runtimeContract:{runtime:string;model:string;provider:string;profile:string;effort:string|null;permissions:{sandbox:unknown;approval:string}};
}
export interface CodexRehostPreflight {
  posture:"floor"|"full_bypass";
  effective:{model:string;provider:string;effort:string|null;approval:string;sandbox:string};
  evidenceDigest:string;
}
export type CodexRehostResult = {
  ok:true;runtime:"codex";nodeId:string;sessionName:string;generation:string;generationUnchanged:true;
  nativeIdHash:string;attemptId:string;receiptPath:string;backup:{path:string;sha256:string;size:number};
  nativeFingerprintBefore:string;nativeFingerprintAfter:string;supervisorLaunchId:string;
  custodyPreserved:true;guardLeftEnabled:true;authorityRepaired:false;
} | {ok:false;code:string;message:string;blindRetryAllowed:false;effectAttempted:boolean;receiptPath?:string};
export interface CodexRehostInput {nodeId:string;sessionName:string;reason:string;operator?:string|null}
export interface CodexStoppedRecovery {attemptId:string;beganSha256:string}
export interface CodexStoppedRecoveryInput extends CodexRehostInput, CodexStoppedRecovery {actorGeneration:string}
export function parseCodexStoppedRecovery(value:unknown):CodexStoppedRecovery {
  if(!value||typeof value!=="object"||Array.isArray(value)||Object.keys(value).sort().join(',')!=="attemptId,beganSha256")throw new Error("Exact attemptId and beganSha256 required; caller native facts are not accepted");
  const v=value as CodexStoppedRecovery;
  if(!/^[a-f0-9-]{36}$/.test(v.attemptId)||!/^[a-f0-9]{64}$/.test(v.beganSha256))throw new Error("Invalid immutable stopped-rehost receipt reference");
  return v;
}
export interface CodexRehostOptions {
  db:Database.Database;guard:SeatDeliveryGuard;tmux:{getPanePid(pane:string):Promise<number|null>};
  resume:Pick<CodexResumeAdapter,"resume">;snapshotRoot:string;
  nativeState:(session:string)=>Promise<CodexRehostNativeState>;
  /** Must refresh the real seat observation before returning its deciding witness. */
  activityWitness:(nodeId:string,pane:string)=>Promise<ActivityEvidence|null>;
  /** Exact launch dependencies, effective profile/posture/model/effort; no native effect. */
  preflightSupervisedLaunch:(binding:CodexRehostBinding,native:CodexRehostNativeState)=>Promise<CodexRehostPreflight>;
  /** Must independently prove the new installed supervisor under this exact owned rehost lease. No enrollment/grant. */
  observeSupervisedReplacement:(binding:CodexRehostBinding)=>Promise<{launchId:string;fingerprint:string}|null>;
  /** Recovery-only retained transcript resolution. Never used as living native proof. */
  stoppedNativeState?:(binding:CodexRehostBinding)=>Promise<CodexRehostNativeState>;
  /** Fail-closed global kernel census; bare pane alone cannot exclude a reparented old process. */
  proveStoppedIdentityAbsent?:(binding:CodexRehostBinding,panePid:number)=>Promise<boolean>;
  listProcesses?:()=>Promise<NativeProcessRow[]>;
  verifyProcessIdentity?:typeof verifyNativeDutyProcessIdentity;
  signal?:(pid:number)=>void;now?:()=>number;sleep?:(ms:number)=>Promise<void>;waitMs?:number;pollMs?:number;
}
class Refusal extends Error {constructor(readonly code:string,message:string){super(message);}}
function reject(code:string,message:string):never {throw new Refusal(code,message);}
const hash=(value:string|Buffer)=>createHash("sha256").update(value).digest("hex");
const digest=(value:unknown)=>hash(JSON.stringify(value));
// Delayed held deliveries and session telemetry are allowed before recovery.
// Identity and authority custody must still equal the original stop receipt;
// every table is then frozen against the fresh in-recovery baseline.
const STOP_CUSTODY_KEYS=['node','bindings','tenures','permissions','authority','assignments','staged','resources'] as const;
function originalStopCustodyMatches(original:unknown,current:Record<string,unknown>):boolean {
  if(!original||typeof original!=='object'||Array.isArray(original))return false;
  const recorded=original as Record<string,unknown>;
  return STOP_CUSTODY_KEYS.every(key=>recorded[key]!==undefined&&digest(recorded[key])===digest(current[key]));
}
function privatePath(file:string,directory:boolean){const s=lstatSync(file);if(s.isSymbolicLink()||(directory?!s.isDirectory():!s.isFile())||s.uid!==process.getuid?.()||(s.mode&0o077))reject("codex_rehost_private_store","Private owner-only nonsymlink evidence path required");}
function writeDurable(file:string,bytes:string|Buffer){const fd=openSync(file,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY,0o600);try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}const dir=openSync(path.dirname(file),constants.O_RDONLY);try{fsyncSync(dir);}finally{closeSync(dir);}}
function descendants(rows:NativeProcessRow[],root:number){const ids=new Set([root]);for(let i=0;i<rows.length;i++){let change=false;for(const r of rows)if(ids.has(r.ppid)&&!ids.has(r.pid)){ids.add(r.pid);change=true;}if(!change)break;}return rows.filter(r=>ids.has(r.pid));}

/** Replace only a proven native process incarnation. Never change session/tenure/custody,
 * never repeat an uncertain effect, and never accept caller-native evidence or a fresh fallback. */
export class CodexSameGenerationRehost {
  private readonly now:()=>number;private readonly sleep:(ms:number)=>Promise<void>;
  private readonly census:()=>Promise<NativeProcessRow[]>;private readonly verify:typeof verifyNativeDutyProcessIdentity;
  constructor(private readonly deps:CodexRehostOptions){this.now=deps.now??Date.now;this.sleep=deps.sleep??(ms=>new Promise(r=>setTimeout(r,ms)));this.census=deps.listProcesses??listNativeProcesses;this.verify=deps.verifyProcessIdentity??verifyNativeDutyProcessIdentity;}
  async rehost(input:CodexRehostInput):Promise<CodexRehostResult>{
    let effectAttempted=false,receiptPath:string|undefined;
    try {
      if(!input.reason?.trim())reject("codex_rehost_reason_required","An accountable reason is required");
      if(!this.deps.preflightSupervisedLaunch||!this.deps.observeSupervisedReplacement||!this.deps.nativeState||!this.deps.activityWitness||!this.deps.resume)
        reject("codex_rehost_unavailable","Every supervised resume, native and activity proof seam is required");
      return await this.deps.guard.runnerRehost(input.nodeId,async()=>{
        const binding=this.binding(input),pane=this.pane(input.nodeId),before=this.custody(binding);
        this.gates(binding);this.assertNoUnresolved(binding);
        const native=await this.native(binding),preflight=await this.deps.preflightSupervisedLaunch(binding,native);this.preflightMatches(binding,native,preflight);
        const first=await this.prove(binding,pane,false);await this.idle(binding,pane);
        const initial=this.history(native.transcriptPath,binding.nativeId);
        await this.sleep(100);
        const second=await this.prove(binding,pane,false);await this.idle(binding,pane);
        if(first.fingerprint!==second.fingerprint||!initial.equals(this.history(native.transcriptPath,binding.nativeId)))reject("codex_rehost_unstable","Native incarnation or transcript changed during deciding idle proof");
        this.unchanged(input,binding,before);this.gates(binding);
        const evidenceNative=await this.native(binding),finalPreflight=await this.deps.preflightSupervisedLaunch(binding,evidenceNative);this.preflightMatches(binding,evidenceNative,finalPreflight);
        if(digest(native)!==digest(evidenceNative)||digest(preflight)!==digest(finalPreflight))reject("codex_rehost_configuration_changed","Native profile/model/effort/posture or launch dependencies changed before stop");
        const attemptId=randomUUID(),directory=this.attemptDirectory(binding,second.fingerprint,attemptId);
        const backupPath=path.join(directory,"transcript.jsonl"),backup={path:backupPath,sha256:hash(initial),size:initial.length};
        writeDurable(backupPath,initial);privatePath(backupPath,false);if(!readFileSync(backupPath).equals(initial))reject("codex_rehost_snapshot_failed","Private full transcript backup could not be verified");
        const final=await this.prove(binding,pane,false);await this.idle(binding,pane);this.unchanged(input,binding,before);this.gates(binding);
        if(final.fingerprint!==second.fingerprint||!this.history(native.transcriptPath,binding.nativeId).equals(initial))reject("codex_rehost_unstable","Native/history changed immediately before the durable stop boundary");
        receiptPath=path.join(directory,"began.json");
        writeDurable(receiptPath,JSON.stringify({protocol:"codex-same-generation-rehost-v1",attemptId,at:this.now(),reason:input.reason,actor:input.operator??null,
          bindingDigest:digest(binding),nativeIdHash:hash(binding.nativeId),nativeFingerprint:final.fingerprint,backup,custody:before,preflightDigest:preflight.evidenceDigest,
          nativeEvidence:{pane,panePid:final.panePid,processes:final.processes.map(({pid,ppid,startedAt})=>({pid,ppid,startedAt}))}})+"\n");
        effectAttempted=true; // Durable write-ahead intent: any following uncertainty blocks replay.
        try {
          (this.deps.signal??(pid=>process.kill(pid,"SIGTERM")))(final.pid);
          await this.absent(pane,final.panePid,final.processes);
          this.unchanged(input,binding,before);this.gates(binding);
          const stopped=this.history(native.transcriptPath,binding.nativeId);this.prefix(stopped,initial);
          const afterStopPreflight=await this.deps.preflightSupervisedLaunch(binding,native);this.preflightMatches(binding,native,afterStopPreflight);
          if(digest(afterStopPreflight)!==digest(preflight))reject("codex_rehost_configuration_changed","Launch dependencies changed after stop; no resume attempted");
          const resumed=await this.deps.resume.resume(binding.sessionName,"codex_id",binding.nativeId,binding.cwd,binding.codexConfigProfile,preflight.posture,binding.model,binding.effort,binding.generation);
          if(!resumed.ok)reject("codex_rehost_resume_unknown","Exact native resume was not confirmed; no fresh fallback or retry is permitted");
          const replacement=await this.replacement(binding,pane,final.pid);
          this.prefix(this.history(native.transcriptPath,binding.nativeId),stopped);this.unchanged(input,binding,before);this.gates(binding);
          const result:Extract<CodexRehostResult,{ok:true}>={ok:true,runtime:"codex",nodeId:binding.nodeId,sessionName:binding.sessionName,generation:binding.generation,generationUnchanged:true,
            nativeIdHash:hash(binding.nativeId),attemptId,receiptPath,backup,nativeFingerprintBefore:final.fingerprint,nativeFingerprintAfter:replacement.native.fingerprint,
            supervisorLaunchId:replacement.supervision.launchId,custodyPreserved:true,guardLeftEnabled:true,authorityRepaired:false};
          writeDurable(path.join(directory,"completed.json"),JSON.stringify({...result,at:this.now(),custodyAfter:this.custody(binding)})+"\n");return result;
        }catch(error){try{writeDurable(path.join(directory,"unknown.json"),JSON.stringify({attemptId,at:this.now(),effectAttempted:true,code:error instanceof Refusal?error.code:"codex_rehost_effect_unknown",blindRetryAllowed:false})+"\n");}catch{}throw error;}
      });
    }catch(error){return {ok:false,code:error instanceof Refusal?error.code:effectAttempted?"codex_rehost_effect_unknown":"codex_rehost_precondition_failed",
      message:error instanceof Refusal?error.message:effectAttempted?"Native effect outcome is unknown; retained receipt and history require explicit recovery. No retry or fallback.":"A required native, activity, custody or launch precondition could not be proven; no process signal was attempted.",blindRetryAllowed:false,effectAttempted,...(receiptPath?{receiptPath}:{})};}
  }
  /** Continue only a recorded stop-timeout, whose original code path never resumed.
   * The original UNKNOWN is immutable. A separate exclusive write-ahead marker
   * makes every uncertain recovery resume non-replayable, including lost replies. */
  async recoverStopped(input:CodexStoppedRecoveryInput):Promise<CodexRehostResult>{
    let effectAttempted=false,receiptPath:string|undefined;
    try {
      parseCodexStoppedRecovery({attemptId:input.attemptId,beganSha256:input.beganSha256});
      if(!input.reason?.trim()||!this.deps.stoppedNativeState||!this.deps.proveStoppedIdentityAbsent)reject("codex_rehost_recovery_unavailable","Accountable reason and stopped recovery proof dependencies required");
      return await this.deps.guard.runnerRehost(input.nodeId,async()=>{
        this.recoveryActor(input);
        const b=this.binding(input),pane=this.pane(b.nodeId),before=this.custody(b),root=this.nodeDirectory(b);
        this.gates(b);
        const candidates=readdirSync(root).filter(id=>id.endsWith('-'+input.attemptId));
        if(candidates.length!==1)reject("codex_rehost_recovery_receipt","Exactly one retained original attempt is required");
        const directory=path.join(root,candidates[0]!);privatePath(directory,true);
        receiptPath=path.join(directory,'began.json');privatePath(receiptPath,false);
        const raw=readFileSync(receiptPath);if(hash(raw)!==input.beganSha256)reject("codex_rehost_recovery_receipt","Original write-ahead receipt digest differs");
        const began=JSON.parse(raw.toString('utf8'));
        privatePath(path.join(directory,'unknown.json'),false);const unknown=JSON.parse(readFileSync(path.join(directory,'unknown.json'),'utf8'));
        if(began.protocol!=="codex-same-generation-rehost-v1"||began.attemptId!==input.attemptId||began.actor!=="operator-agent@kernel"
          ||unknown.attemptId!==input.attemptId||unknown.code!=="codex_rehost_stop_unknown"||unknown.effectAttempted!==true||unknown.blindRetryAllowed!==false
          ||began.bindingDigest!==digest(b)||began.nativeIdHash!==hash(b.nativeId)||!originalStopCustodyMatches(began.custody,before)
          ||!/^[a-f0-9]{64}$/.test(began.nativeFingerprint)||!/^[a-f0-9]{64}$/.test(began.preflightDigest))reject("codex_rehost_recovery_receipt","Original stop-only UNKNOWN, binding and custody must match exactly");
        for(const id of readdirSync(root)){
          const other=path.join(root,id);privatePath(other,true);
          if(other===directory){if(existsSync(path.join(other,'completed.json'))||existsSync(path.join(other,'recovery-began.json')))reject("codex_rehost_recovery_replay","This attempt has a completion or an uncertain recovery intent; no replay");}
          else if(this.hasBegan(other)){privatePath(path.join(other,'completed.json'),false);const r=JSON.parse(readFileSync(path.join(other,'completed.json'),'utf8'));if(r.ok!==true||r.nodeId!==b.nodeId||r.generation!==b.generation)reject("codex_rehost_unresolved_attempt","Another unresolved attempt excludes recovery");}
        }
        const backupPath=path.join(directory,'transcript.jsonl');privatePath(backupPath,false);
        const backup=this.history(backupPath,b.nativeId);
        if(began.backup?.path!==backupPath||began.backup.sha256!==hash(backup)||began.backup.size!==backup.length)reject("codex_rehost_recovery_history","Original full backup failed immutable verification");
        const native=await this.deps.stoppedNativeState!(b);this.nativeMatches(b,native);
        const stopped=this.history(native.transcriptPath,b.nativeId);this.prefix(stopped,backup);
        const preflight=await this.deps.preflightSupervisedLaunch(b,native);this.preflightMatches(b,native,preflight);
        if(preflight.evidenceDigest!==began.preflightDigest)reject("codex_rehost_configuration_changed","Recovery launch/profile digest differs from original preflight");
        const first=await this.stoppedProof(b,pane,began.nativeEvidence);await this.sleep(100);
        const second=await this.stoppedProof(b,pane,began.nativeEvidence);
        if(digest(first)!==digest(second)||!stopped.equals(this.history(native.transcriptPath,b.nativeId)))reject("codex_rehost_recovery_unstable","Bare pane or retained history changed during absence proof");
        const finalPreflight=await this.deps.preflightSupervisedLaunch(b,native);this.preflightMatches(b,native,finalPreflight);
        if(digest(finalPreflight)!==digest(preflight))reject("codex_rehost_configuration_changed","Recovery launch dependencies changed before resume");
        this.recoveryActor(input);this.unchanged(input,b,before);this.gates(b);
        // Recheck absence after launch preflight awaits, immediately before the write-ahead boundary.
        if(digest(await this.stoppedProof(b,pane,began.nativeEvidence))!==digest(second))reject("codex_rehost_recovery_unstable","Bare pane changed before recovery intent");
        this.recoveryActor(input);this.unchanged(input,b,before);this.gates(b);
        if(!stopped.equals(this.history(native.transcriptPath,b.nativeId))||hash(readFileSync(receiptPath!))!==input.beganSha256)reject('codex_rehost_recovery_unstable','Retained history or original receipt changed before recovery intent');
        writeDurable(path.join(directory,'recovery-began.json'),JSON.stringify({protocol:'codex-stopped-recovery-v1',attemptId:input.attemptId,beganSha256:input.beganSha256,at:this.now(),actor:input.operator,actorGeneration:input.actorGeneration,reason:input.reason,bindingDigest:digest(b),originalCustody:began.custody,recoveryBaselineCustody:before,preflightDigest:preflight.evidenceDigest,stoppedHistorySha256:hash(stopped),absence:second})+'\n');
        effectAttempted=true;
        try {
          const resumed=await this.deps.resume.resume(b.sessionName,'codex_id',b.nativeId,b.cwd,b.codexConfigProfile,preflight.posture,b.model,b.effort,b.generation);
          if(!resumed.ok)reject("codex_rehost_resume_unknown","Recovery resume outcome unknown; no replay or fresh fallback");
          const replacement=await this.replacement(b,pane,-1);
          this.prefix(this.history(native.transcriptPath,b.nativeId),stopped);this.unchanged(input,b,before);this.gates(b);
          const result:Extract<CodexRehostResult,{ok:true}>={ok:true,runtime:'codex',nodeId:b.nodeId,sessionName:b.sessionName,generation:b.generation,generationUnchanged:true,nativeIdHash:hash(b.nativeId),attemptId:input.attemptId,receiptPath:receiptPath!,backup:began.backup,nativeFingerprintBefore:began.nativeFingerprint,nativeFingerprintAfter:replacement.native.fingerprint,supervisorLaunchId:replacement.supervision.launchId,custodyPreserved:true,guardLeftEnabled:true,authorityRepaired:false};
          writeDurable(path.join(directory,'completed.json'),JSON.stringify({...result,at:this.now(),recoveryProtocol:'codex-stopped-recovery-v1',beganSha256:input.beganSha256,custodyAfter:this.custody(b)})+'\n');return result;
        }catch(error){try{writeDurable(path.join(directory,'recovery-unknown.json'),JSON.stringify({attemptId:input.attemptId,at:this.now(),effectAttempted:true,code:error instanceof Refusal?error.code:'codex_rehost_recovery_unknown',blindRetryAllowed:false})+'\n');}catch{}throw error;}
      });
    }catch(error){return {ok:false,code:error instanceof Refusal?error.code:effectAttempted?'codex_rehost_recovery_unknown':'codex_rehost_recovery_unproven',message:error instanceof Refusal?error.message:'Stopped recovery proof unavailable; original evidence retained. No blind retry.',effectAttempted,blindRetryAllowed:false,...(receiptPath?{receiptPath}:{})};}
  }
  private recoveryActor(input:CodexStoppedRecoveryInput){
    const t=this.deps.guard.target('operator-agent@kernel'),s=this.deps.db.prepare('SELECT status,startup_status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1').get(t.nodeId) as {status:string;startup_status:string}|undefined;
    if(input.operator!=='operator-agent@kernel'||!input.actorGeneration||t.session!==input.operator||t.occupant!==input.actorGeneration||s?.status!=='running'||s.startup_status!=='ready')reject('codex_rehost_recovery_actor','Current running ready Operator transport identity and generation required');
  }
  private async stoppedProof(b:CodexRehostBinding,pane:string,original?:{pane:string;panePid:number;processes:Array<{pid:number;startedAt:string}>}){
    const panePid=await this.deps.tmux.getPanePid(pane),rows=await this.census(),root=rows.find(r=>r.pid===panePid);
    if(!panePid||!root?.startedAt||!['zsh','bash','sh','fish','dash'].includes(root.executableName??'')||descendants(rows,panePid).length!==1
      ||(original&&(original.pane!==pane||original.panePid!==panePid||original.processes.some(old=>rows.some(r=>r.pid===old.pid&&r.startedAt===old.startedAt))))
      ||!await this.deps.proveStoppedIdentityAbsent!(b,panePid)||await this.deps.tmux.getPanePid(pane)!==panePid)reject('codex_rehost_recovery_absence','Stable bare bound pane and global old native identity absence are required');
    return {pane,panePid,startedAt:root.startedAt,ppid:root.ppid,executableName:root.executableName};
  }
  private binding(input:Pick<CodexRehostInput,"nodeId"|"sessionName">):CodexRehostBinding {
    const row=this.deps.db.prepare("SELECT n.id nodeId,n.runtime,n.cwd,n.model,n.effort,n.codex_config_profile codexConfigProfile,s.id sessionId,s.session_name sessionName,s.resume_type resumeType,s.resume_token nativeId,s.status,s.startup_status startupStatus FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1").get(input.nodeId) as Record<string,string|null>|undefined;
    const tenure=this.deps.db.prepare("SELECT generation_uuid FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal DESC LIMIT 1").get(input.nodeId) as {generation_uuid:string}|undefined;
    if(!row||row.runtime!=="codex"||row.sessionName!==input.sessionName||row.resumeType!=="codex_id"||!row.nativeId||!tenure?.generation_uuid||row.status!=="running"||row.startupStatus!=="ready")reject("codex_rehost_binding_unproven","Current running Codex binding and exact saved codex_id/current tenure are required; no fresh or last-thread fallback");
    if(!row.cwd||!path.isAbsolute(row.cwd)||!row.model||!row.codexConfigProfile)reject("codex_rehost_configuration_unproven","Persisted absolute cwd, model and named Codex profile are required before any process effect");
    return {nodeId:row.nodeId!,sessionId:row.sessionId!,sessionName:row.sessionName!,generation:tenure.generation_uuid,runtime:"codex",nativeId:row.nativeId,cwd:row.cwd,model:row.model,effort:row.effort??null,codexConfigProfile:row.codexConfigProfile};
  }
  private pane(nodeId:string):string {const row=this.deps.db.prepare("SELECT tmux_pane FROM bindings WHERE node_id=?").get(nodeId) as {tmux_pane:string|null}|undefined;if(!row?.tmux_pane)reject("codex_rehost_binding_unproven","Exact managed pane binding required");return row.tmux_pane;}
  private gates(binding:CodexRehostBinding){
    const target=this.deps.guard.target(binding.nodeId),pref=this.deps.guard.preference(binding.nodeId);
    if(target.occupant!==binding.generation||target.session!==binding.sessionName||pref.desired!==true||pref.effective!==true)reject("codex_rehost_guard_required","Exact current binding and desired/effective typing guard ON are required");
    if(this.deps.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state!='released'").get(binding.nodeId))reject("codex_rehost_reservation_active","An unresolved lifecycle reservation excludes process rehost");
    const addresses=rotationLocalAddresses(this.deps.db,binding.sessionName);
    if(this.deps.db.prepare("SELECT 1 FROM outbox_entries WHERE delivery_state='sending' AND (sender_session IN (?,?) OR destination_session IN (?,?)) LIMIT 1").get(...addresses,...addresses))reject("codex_rehost_sending","An in-flight sending effect excludes process rehost; UNKNOWN remains retained");
    if(this.deps.db.prepare("SELECT 1 FROM coordinator_authority WHERE owner_session IN (?,?) AND owner_generation!=? LIMIT 1").get(...addresses,binding.generation))reject("codex_rehost_generation_mismatch","Retained authority names a different occupant generation");
  }
  private nativeMatches(binding:CodexRehostBinding,n:CodexRehostNativeState){if(n.nodeId!==binding.nodeId||n.sessionName!==binding.sessionName||n.nativeId!==binding.nativeId||n.runtimeContract.runtime!=="codex"||n.runtimeContract.model!==binding.model||n.runtimeContract.profile!==binding.codexConfigProfile||(binding.effort!==null&&n.runtimeContract.effort!==binding.effort))reject("codex_rehost_native_configuration_mismatch","Native thread/model/profile/effort differs from the persistent continuation binding");}
  private async native(binding:CodexRehostBinding){const n=await this.deps.nativeState(binding.sessionName);this.nativeMatches(binding,n);return n;}
  private preflightMatches(binding:CodexRehostBinding,n:CodexRehostNativeState,p:CodexRehostPreflight){
    const sandbox=n.runtimeContract.permissions.sandbox,kind=typeof sandbox==="string"?sandbox:(sandbox as {type?:unknown}|null)?.type;
    if(!p||!['floor','full_bypass'].includes(p.posture)||!/^[a-f0-9]{64}$/.test(p.evidenceDigest)||p.effective.model!==binding.model||p.effective.provider!==n.runtimeContract.provider||p.effective.effort!==n.runtimeContract.effort||p.effective.approval!==n.runtimeContract.permissions.approval||p.effective.sandbox!==kind||(p.posture==='full_bypass'&&(kind!=='danger-full-access'||p.effective.approval!=='never')))
      reject("codex_rehost_launch_mismatch","Supervised successor profile/model/provider/effort/posture must preserve the actual native runtime contract before stop");
  }
  private async idle(b:CodexRehostBinding,pane:string){const w=await this.deps.activityWitness(b.nodeId,pane),at=w?Date.parse(w.observedAt):NaN;if(!w||w.seatNodeId!==b.nodeId||w.sessionName!==b.sessionName||!['idle','idle-at-prompt'].includes(w.activity??'')||!Number.isFinite(at)||at>this.now()||this.now()-at>5000||(w.needsInput?.count??0)>0)reject("codex_rehost_not_idle","Fresh real deciding idle/activity witness for this exact seat is required");}
  private async prove(b:CodexRehostBinding,pane:string,requireResume:boolean){
    const panePid=await this.deps.tmux.getPanePid(pane);if(!panePid)reject("codex_rehost_native_unproven","Managed pane root is unavailable");
    const rows=await this.census(),selected=await observeCodexPaneProcess({target:pane,tmux:{getPanePid:async()=>panePid},listProcesses:async()=>rows,expectedToken:b.nativeId,requireResume});
    const identity={OPENRIG_NODE_ID:b.nodeId,OPENRIG_SESSION_NAME:b.sessionName,OPENRIG_OCCUPANT_GENERATION:b.generation,OPENRIG_RUNTIME:'codex'};
    if(!selected||!await this.verify(selected.process.pid,identity)||await this.deps.tmux.getPanePid(pane)!==panePid)reject("codex_rehost_native_unproven","Exact native ancestry, foreground process, thread and kernel seat identity are required");
    return {pid:selected.process.pid,panePid,fingerprint:hash(selected.fingerprint),processes:descendants(rows,panePid).filter(r=>r.pid!==panePid)};
  }
  private history(file:string,nativeId:string):Buffer {
    if(!path.isAbsolute(file)||realpathSync(file)!==file)reject("codex_rehost_history_unproven","Exact daemon-observed nonsymlink transcript path required");
    const before=lstatSync(file);if(!before.isFile()||before.isSymbolicLink())reject("codex_rehost_history_unproven","Transcript must be a regular native file");
    const bytes=readFileSync(file),after=lstatSync(file);if(before.ino!==after.ino||before.size!==after.size||before.mtimeMs!==after.mtimeMs||!bytes.length||bytes.at(-1)!==10)reject("codex_rehost_history_unstable","Complete stable native transcript required");
    let matching=0,pendingText:string|undefined,whitespaceOnlyLine=false;
    const decoder=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true});
    const parseLine=(text:string)=>{const row=JSON.parse(text);if(row.type==='session_meta'){if((row.payload?.id??row.payload?.session_id)!==nativeId)reject("codex_rehost_history_mismatch","Transcript native session ID differs");matching++;}};
    let firstLine=true;
    forEachJsonlLine(bytes,line=>{
      let text=decoder.decode(line);
      if(firstLine){if(text.startsWith('\uFEFF'))text=text.slice(1);firstLine=false;}
      if(!text.trim()){whitespaceOnlyLine=true;return;}
      if(whitespaceOnlyLine)JSON.parse(''); // Internal blank records remain strict JSON failures; only trimEnd's terminal whitespace is ignored.
      if(pendingText!==undefined)parseLine(pendingText);
      pendingText=text;whitespaceOnlyLine=false;
    });
    if(pendingText===undefined){JSON.parse('');return bytes;}
    parseLine(pendingText.trimEnd());
    if(matching!==1)reject("codex_rehost_history_mismatch","Exactly one matching native transcript identity is required");return bytes;
  }
  private prefix(bytes:Buffer,prefix:Buffer){if(bytes.length<prefix.length||!bytes.subarray(0,prefix.length).equals(prefix))reject("codex_rehost_history_changed","Native transcript prefix changed or was truncated; private full backup retained");}
  private custody(b:CodexRehostBinding){const db=this.deps.db,a=rotationLocalAddresses(db,b.sessionName);const rows={
    node:db.prepare('SELECT * FROM nodes WHERE id=?').all(b.nodeId),sessions:db.prepare('SELECT * FROM sessions WHERE node_id=? ORDER BY id').all(b.nodeId),bindings:db.prepare('SELECT * FROM bindings WHERE node_id=?').all(b.nodeId),tenures:db.prepare('SELECT * FROM occupant_tenures WHERE node_id=? ORDER BY id').all(b.nodeId),permissions:db.prepare('SELECT * FROM node_permission_selections WHERE node_id=?').all(b.nodeId),
    queue:db.prepare('SELECT * FROM queue_items WHERE destination_session IN (?,?) OR claimed_by_generation_uuid=? ORDER BY qitem_id').all(...a,b.generation),authority:db.prepare('SELECT * FROM coordinator_authority WHERE owner_session IN (?,?) OR owner_generation=? ORDER BY rig_id').all(...a,b.generation),
    assignments:db.prepare('SELECT * FROM coordinator_assignments WHERE destination IN (?,?) ORDER BY rig_id,package_key').all(...a),staged:db.prepare('SELECT * FROM coordinator_stage_assignments WHERE source IN (?,?) OR destination IN (?,?) ORDER BY rig_id,package_key').all(...a,...a),
    resources:db.prepare('SELECT * FROM coordinator_resources WHERE (rig_id,package_key) IN (SELECT rig_id,package_key FROM coordinator_assignments WHERE destination IN (?,?) UNION SELECT rig_id,package_key FROM coordinator_stage_assignments WHERE source IN (?,?) OR destination IN (?,?)) ORDER BY rig_id,resource_key').all(...a,...a,...a),
    outbox:db.prepare('SELECT * FROM outbox_entries WHERE sender_session IN (?,?) OR destination_session IN (?,?) ORDER BY outbox_id').all(...a,...a)};
    return Object.fromEntries(Object.entries(rows).map(([k,v])=>[k,{count:v.length,sha256:digest(v)}]));
  }
  private unchanged(input:CodexRehostInput,b:CodexRehostBinding,before:unknown){if(digest(this.binding(input))!==digest(b)||digest(this.custody(b))!==digest(before))reject("codex_rehost_custody_changed","Exact node/session/generation, authority, claims, resources or retained effects changed; nothing is silently repaired");}
  private nodeDirectory(b:CodexRehostBinding){if(!path.isAbsolute(this.deps.snapshotRoot))reject("codex_rehost_private_store","Absolute private snapshot root required");mkdirSync(this.deps.snapshotRoot,{recursive:true,mode:0o700});privatePath(this.deps.snapshotRoot,true);const directory=path.join(realpathSync(this.deps.snapshotRoot),digest([b.nodeId,b.generation]));mkdirSync(directory,{recursive:true,mode:0o700});privatePath(directory,true);return directory;}
  /** Only the durable write-ahead marker signifies an effect may have begun.
   * A retained backup without it is a pre-effect refusal, never retry debt. */
  private hasBegan(attempt:string):boolean {
    try { privatePath(path.join(attempt,"began.json"),false);return true; }
    catch(error) { if((error as NodeJS.ErrnoException).code==="ENOENT")return false;throw error; }
  }
  private assertNoUnresolved(b:CodexRehostBinding){const dir=this.nodeDirectory(b);for(const id of readdirSync(dir)){const attempt=path.join(dir,id);privatePath(attempt,true);if(!this.hasBegan(attempt))continue;try{privatePath(path.join(attempt,'completed.json'),false);const receipt=JSON.parse(readFileSync(path.join(attempt,'completed.json'),'utf8'));if(receipt.ok!==true||receipt.generation!==b.generation||receipt.nodeId!==b.nodeId)throw new Error();}catch{reject("codex_rehost_unresolved_attempt","An earlier rehost attempt lacks a confirmed completion; no retry is permitted");}}}
  private attemptDirectory(b:CodexRehostBinding,fingerprint:string,attemptId:string){
    const root=this.nodeDirectory(b);
    // Recognize both legacy fingerprint-only and uniquely named attempt directories.
    // Completed effects cannot replay their incarnation; pre-effect evidence remains
    // immutable in its own directory while a safe retry receives a distinct one.
    for(const id of readdirSync(root))if((id===fingerprint||id.startsWith(fingerprint+"-"))&&this.hasBegan(path.join(root,id)))
      reject("codex_rehost_attempt_replay","This exact native incarnation already has a rehost attempt; no same-attempt replay");
    const directory=path.join(root,`${fingerprint}-${attemptId}`);
    mkdirSync(directory,{mode:0o700});privatePath(directory,true);return directory;
  }
  private async absent(pane:string,panePid:number,old:NativeProcessRow[]){const deadline=this.now()+(this.deps.waitMs??5000);for(let attempt=0;attempt<50;attempt++){const rows=await this.census(),root=rows.find(r=>r.pid===panePid);if(await this.deps.tmux.getPanePid(pane)!==panePid)reject("codex_rehost_stop_unknown","Pane root changed after stop");if(root&&['zsh','bash','sh','fish','dash'].includes(root.executableName??'')&&descendants(rows,panePid).length===1&&!old.some(was=>rows.some(r=>r.pid===was.pid)))return;if(this.now()>=deadline)break;await this.sleep(this.deps.pollMs??100);}reject("codex_rehost_stop_unknown","Exact old process tree absence and bare shell were not proven; no resume attempted");}
  private async replacement(b:CodexRehostBinding,pane:string,oldPid:number){for(let attempt=0;attempt<20;attempt++){try{const first=await this.prove(b,pane,true),second=await this.prove(b,pane,true);if(first.pid===oldPid||first.fingerprint!==second.fingerprint)throw new Error();const supervision=await this.deps.observeSupervisedReplacement(b);if(supervision?.launchId&&supervision.fingerprint)return {native:second,supervision};}catch{}await this.sleep(this.deps.pollMs??250);}reject("codex_rehost_replacement_unknown","Exact resumed thread and installed supervisor proof did not settle; no retry or fallback");}
}
