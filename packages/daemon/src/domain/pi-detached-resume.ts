import { recoveryReceiptDigest, type NativeRecoveryCompletionPublisher } from "./native-recovery-completion.js";
import { createHash, randomUUID } from "node:crypto";
import { closeSync, constants, existsSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import path from "node:path";
import { listNativeProcesses } from "./native-process-lineage.js";
import { rotationLocalAddresses } from "./rotation-local-custody.js";
import type Database from "better-sqlite3";
import { DeliveryGuardError, type SeatDeliveryGuard } from "./seat-delivery-guard.js";
import type { NativeProcessRow } from "./native-process-lineage.js";
export interface PiDetachedBinding { nodeId:string;sessionId:string;sessionName:string;generation:string;runtime:"pi";nativeId:string;cwd:string;model:string;effort:string|null;profile:string|null;launchPosture:string|null }
export interface PiDetachedInput {nodeId:string;sessionName:string;reason:string;operator:string|null|undefined;actorGeneration:string;recovery?:{attemptId:string;beganSha256:string;originalRunnerEntryPath?:string}}
export interface PiDetachedPreflight {digest:string;posture:"floor"|"full_bypass"}
export type PiDetachedResumeResult = {ok:true;runtime:"pi";nodeId:string;sessionName:string;generation:string;generationUnchanged:true;sessionId:string;nativeIdHash:string;attemptId:string;receiptPath:string;backup:{path:string;sha256:string;size:number};supervisorLaunchId:string;nativeFingerprint:string;custodyPreserved:true;guardLeftEnabled:true;authorityRepaired:false} | {ok:false;code:string;message:string;effectAttempted:boolean;blindRetryAllowed:false;receiptPath?:string};
export interface PiDetachedResumeOptions {recordNativeRecoveryCompletion?:NativeRecoveryCompletionPublisher;db:Database.Database;guard:SeatDeliveryGuard;snapshotRoot:string;
 tmux:{getPanePid(pane:string):Promise<number|null>};
 validateHistory(binding:PiDetachedBinding,bytes:Buffer):{nativeIdentity:string;lastEntryId:string};
 preflight(binding:PiDetachedBinding,detached:boolean):Promise<PiDetachedPreflight>;
 runnerEntryPath?:string;
 preflightAtOriginalRunner?(binding:PiDetachedBinding,detached:boolean,originalRunnerEntryPath:string):Promise<PiDetachedPreflight>;
 terminalAbsent(binding:PiDetachedBinding):Promise<boolean>;
 proveNativeAbsent(binding:PiDetachedBinding,panePid:number):Promise<boolean>;
 createTerminal(binding:PiDetachedBinding):Promise<{pane:string}>;
 resume(binding:PiDetachedBinding,preflight:PiDetachedPreflight):Promise<{ok:boolean}>;
 observeReplacement(binding:PiDetachedBinding):Promise<{supervisorLaunchId:string;nativeFingerprint:string;nativeIdentity:string;sessionFile:string;generation:string}|null>;
 listProcesses?:()=>Promise<NativeProcessRow[]>;now?:()=>number;sleep?:(ms:number)=>Promise<void>;waitMs?:number;pollMs?:number;
}
const hash=(value:string|Buffer)=>createHash("sha256").update(value).digest("hex");
const digest=(value:unknown)=>hash(JSON.stringify(value));
class Hold extends Error {constructor(readonly code:string,message:string){super(message);}}
function hold(code:string,message:string):never {throw new Hold(code,message);}
function validRunnerPath(value:unknown):value is string {return typeof value==="string"&&path.isAbsolute(value)&&path.normalize(value)===value&&!/[\0\r\n]/.test(value);}
function privatePath(file:string,directory=false){const st=lstatSync(file);if(st.isSymbolicLink()||(directory?!st.isDirectory():!st.isFile())||st.uid!==process.getuid?.()||(st.mode&0o077))hold("pi_detached_private_store","Private owner-only evidence path required");}
function durable(file:string,value:unknown,raw=false){const fd=openSync(file,constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY,0o600);try{writeFileSync(fd,raw?value as Buffer:JSON.stringify(value)+"\n");fsyncSync(fd);}finally{closeSync(fd);}const dir=openSync(path.dirname(file),constants.O_RDONLY);try{fsyncSync(dir);}finally{closeSync(dir);}}
function readReceipt(file:string){privatePath(file);return JSON.parse(readFileSync(file,"utf8"));}
const protectedKeys=["node","sessions","bindings","tenures","permissions","authority","assignments","staged","resources","claims"];

/** One exact retained Pi occupant, with separately durable terminal and resume
 * boundaries. Neither a missing terminal nor a sidecar is native absence proof.
 * Explicit continuation proves either the original bound bare terminal or
 * complete pre-terminal absence, always with NO prior native resume intent. */
export class PiDetachedResume {
 private readonly now:()=>number;private readonly sleep:(ms:number)=>Promise<void>;private readonly census:()=>Promise<NativeProcessRow[]>;
 constructor(private readonly deps:PiDetachedResumeOptions){this.now=deps.now??Date.now;this.sleep=deps.sleep??(ms=>new Promise(r=>setTimeout(r,ms)));this.census=deps.listProcesses??listNativeProcesses;}
 async run(input:PiDetachedInput):Promise<PiDetachedResumeResult>{
  let effectAttempted=false,receiptPath:string|undefined,directory:string|undefined;
  try{
   if(!input.reason?.trim())hold("pi_detached_reason_required","Accountable recovery reason required");
   if(input.recovery&&(!["attemptId,beganSha256","attemptId,beganSha256,originalRunnerEntryPath"].includes(Object.keys(input.recovery).sort().join(','))||!/^[a-f0-9-]{36}$/.test(input.recovery.attemptId)||!/^[a-f0-9]{64}$/.test(input.recovery.beganSha256)))hold("pi_detached_receipt_invalid","Exact original attemptId and beganSha256 required");
   if(input.recovery?.originalRunnerEntryPath!==undefined&&!validRunnerPath(input.recovery.originalRunnerEntryPath))hold("pi_detached_receipt_invalid","Absolute original runner path required");
   this.actor(input);
   return await this.deps.guard.runnerRehost(input.nodeId,async()=>{
    this.actor(input);const b=this.binding(input),before=this.custody(b),oldPane=this.pane(b.nodeId);this.gates(b);
    let expectedPane=oldPane;
    let expectedStatus=(this.deps.db.prepare('SELECT status FROM sessions WHERE id=?').get(b.sessionId) as {status:string}).status;
    const unchanged=()=>{this.actor(input);const status=(this.deps.db.prepare('SELECT status FROM sessions WHERE id=?').get(b.sessionId) as {status:string}|undefined)?.status;if(status!==expectedStatus||this.pane(b.nodeId)!==expectedPane||digest(this.binding(input))!==digest(b)||digest(this.custody(b))!==digest(before))hold("pi_detached_custody_changed","Retained identity, claims, authority or effects changed");this.gates(b);};
    const state=(this.deps.db.prepare('SELECT status FROM sessions WHERE id=?').get(b.sessionId) as {status:string}).status;
    if(!input.recovery&&state!=="detached")hold("pi_detached_binding","Expected exact detached occupant or its original partial continuation");
    const root=this.root(b);let attemptId:string,backup:{path:string;sha256:string;size:number},began:any,terminal:any;
    let history=this.history(b),preflight:PiDetachedPreflight,originalRunnerEntryPath:string|undefined;
    if(input.recovery){
     attemptId=input.recovery.attemptId;directory=path.join(root,attemptId);privatePath(directory,true);receiptPath=path.join(directory,"began.json");privatePath(receiptPath);
     if(hash(readFileSync(receiptPath))!==input.recovery.beganSha256)hold("pi_detached_receipt_invalid","Original receipt digest differs");
     began=readReceipt(receiptPath);const unknown=readReceipt(path.join(directory,"unknown.json"));
     const hasBound=existsSync(path.join(directory,"terminal-bound.json")),hasCreated=existsSync(path.join(directory,"terminal-created.json"));
     if(began.protocol!=="pi-detached-resume-v1"||began.attemptId!==attemptId||began.bindingDigest!==digest(b)||began.actor!=="operator-agent@kernel"
       ||unknown.effectAttempted!==true||unknown.blindRetryAllowed!==false
       ||protectedKeys.some(k=>!began.custody?.[k]||digest(began.custody[k])!==digest(before[k]))
       ||!this.unknownPreserved(began.unknownEffects,b))hold("pi_detached_receipt_invalid","Original retained identity and custody must match");
     if(hasBound&&hasCreated){
      terminal=readReceipt(path.join(directory,"terminal-bound.json"));const created=readReceipt(path.join(directory,"terminal-created.json"));
      if(state!=="running"||terminal.attemptId!==attemptId||digest(terminal)!==digest(created)||terminal.pane!==oldPane)hold("pi_detached_receipt_invalid","Original bound terminal must match");
     }else if(hasBound||hasCreated||state!=="detached"||began.oldPane!==oldPane||digest(began.custody)!==digest(before)
       ||existsSync(path.join(directory,"terminal-recovery-began.json")))hold("pi_detached_receipt_invalid","Pre-terminal continuation requires untouched custody and no prior terminal-recovery intent");
     this.noUnresolved(root,b,directory);backup=began.backup;
     if(backup?.path!==path.join(directory,"transcript.jsonl"))hold("pi_detached_history","Exact original private backup required");
     privatePath(backup.path);const bytes=readFileSync(backup.path);this.deps.validateHistory(b,bytes);
     if(hash(bytes)!==backup.sha256||bytes.length!==backup.size)hold("pi_detached_history","Original backup verification failed");this.prefix(history.bytes,bytes);
     if(history.facts.nativeIdentity!==began.nativeIdentity)hold("pi_detached_history","Retained native header identity changed");
     if(!terminal&&!history.bytes.equals(bytes))hold("pi_detached_history_changed","Pre-terminal continuation requires the unchanged original full history");
     if(began.runnerEntryPath!==undefined&&!validRunnerPath(began.runnerEntryPath))hold("pi_detached_receipt_invalid","Invalid recorded runner provenance");
     if(began.runnerEntryPath!==undefined&&input.recovery.originalRunnerEntryPath!==undefined&&began.runnerEntryPath!==input.recovery.originalRunnerEntryPath)hold("pi_detached_receipt_invalid","Explicit runner provenance contradicts original receipt");
     originalRunnerEntryPath=began.runnerEntryPath??input.recovery.originalRunnerEntryPath;
     preflight=await this.deps.preflight(b,!terminal);
     if(digest(preflight)!==began.preflightDigest){
      // Release directories may move while the actual launch contract stays
      // identical. Reconstruct the original commitment, never waive it.
      if(!originalRunnerEntryPath||!this.deps.preflightAtOriginalRunner)hold("pi_detached_configuration_changed","Launch contract changed since original intent");
      let original:PiDetachedPreflight;
      try{original=await this.deps.preflightAtOriginalRunner(b,!terminal,originalRunnerEntryPath);}catch{hold("pi_detached_configuration_changed","Original runner contract cannot be reproduced");}
      if(digest(original)!==began.preflightDigest)hold("pi_detached_configuration_changed","Original launch contract differs beyond runner location");
     }
    }else{
     this.noUnresolved(root,b);preflight=await this.deps.preflight(b,true);
     await this.absent(b);await this.sleep(100);await this.absent(b);unchanged();
     const deciding=this.history(b);if(!deciding.bytes.equals(history.bytes))hold("pi_detached_history_changed","Retained history changed before terminal creation");
     const final=await this.deps.preflight(b,true);if(digest(final)!==digest(preflight))hold("pi_detached_configuration_changed","Launch contract changed before intent");
     await this.absent(b);unchanged();
     attemptId=randomUUID();directory=path.join(root,attemptId);mkdirSync(directory,{mode:0o700});backup={path:path.join(directory,"transcript.jsonl"),sha256:hash(history.bytes),size:history.bytes.length};
     durable(backup.path,history.bytes,true);if(!readFileSync(backup.path).equals(history.bytes))hold("pi_detached_history","Private full history backup failed");
     receiptPath=path.join(directory,"began.json");began={protocol:"pi-detached-resume-v1",attemptId,at:this.now(),actor:input.operator,actorGeneration:input.actorGeneration,reason:input.reason,bindingDigest:digest(b),nativeIdentity:history.facts.nativeIdentity,nativeIdHash:hash(b.nativeId),backup,custody:before,unknownEffects:this.unknownRows(b),preflightDigest:digest(preflight),oldPane,...(this.deps.runnerEntryPath?{runnerEntryPath:this.deps.runnerEntryPath}:{})};
     durable(receiptPath,began);effectAttempted=true;
    }
    if(!terminal){
     if(input.recovery){
      await this.absent(b);await this.sleep(100);await this.absent(b);unchanged();
      const final=await this.deps.preflight(b,true);if(digest(final)!==digest(preflight))hold("pi_detached_configuration_changed","Launch contract changed before terminal continuation");
      if(!this.history(b).bytes.equals(history.bytes))hold("pi_detached_history_changed","Retained history changed before terminal continuation");
      await this.absent(b);unchanged();
      if(hash(readFileSync(receiptPath!))!==input.recovery.beganSha256)hold("pi_detached_receipt_invalid","Original receipt changed before terminal continuation");
      this.noUnresolved(root,b,directory);
      durable(path.join(directory!,"terminal-recovery-began.json"),{attemptId,at:this.now(),actor:input.operator,actorGeneration:input.actorGeneration,beganSha256:input.recovery.beganSha256,bindingDigest:digest(b),preflightDigest:digest(preflight),...(originalRunnerEntryPath?{originalRunnerEntryPath}:{})});effectAttempted=true;
     }
     const created=await this.deps.createTerminal(b);if(!/^%[0-9]+$/.test(created.pane))hold("pi_detached_terminal_unknown","Created pane is not exact");
     const pid=await this.deps.tmux.getPanePid(created.pane),row=(await this.census()).find(x=>x.pid===pid);
     if(!pid||!row?.startedAt)hold("pi_detached_terminal_unknown","Created shell incarnation unavailable");
     terminal={attemptId,pane:created.pane,panePid:pid,startedAt:row.startedAt};durable(path.join(directory,"terminal-created.json"),terminal);unchanged();
     this.deps.db.transaction(()=>{
      unchanged();
      const bound=this.deps.db.prepare("UPDATE bindings SET tmux_pane=?,tmux_window='0',updated_at=datetime('now') WHERE node_id=? AND tmux_session=? AND tmux_pane IS ?").run(created.pane,b.nodeId,b.sessionName,oldPane);
      const running=this.deps.db.prepare("UPDATE sessions SET status='running' WHERE id=? AND node_id=? AND session_name=? AND status='detached' AND resume_type='pi_session_file' AND resume_token=?").run(b.sessionId,b.nodeId,b.sessionName,b.nativeId);
      if(bound.changes!==1||running.changes!==1)hold("pi_detached_binding_changed","Exact physical binding compare-and-swap failed");
      durable(path.join(directory!,"terminal-bound.json"),terminal);
     }).immediate();
     expectedPane=created.pane;expectedStatus="running";this.deps.guard.rebindRunnerRehost(b.nodeId);
     const normal=await this.deps.preflight(b,false);if(digest(normal)!==digest(preflight))hold("pi_detached_configuration_changed","Created terminal launch contract differs");
    }
    // Actual absence must be proved again in the exact bound terminal. The
    // durable resume intent, not an error name, determines continuation safety.
    await this.bare(b,terminal);await this.sleep(100);await this.bare(b,terminal);unchanged();
    const final=await this.deps.preflight(b,false);if(digest(final)!==digest(preflight))hold("pi_detached_configuration_changed","Launch contract changed before resume");
    await this.bare(b,terminal);unchanged();
    const stable=this.history(b);if(!stable.bytes.equals(history.bytes))hold("pi_detached_history_changed","Retained history changed before resume");
    if(input.recovery&&hash(readFileSync(receiptPath!))!==input.recovery.beganSha256)hold("pi_detached_receipt_invalid","Original receipt changed before resume");
    if(digest(readReceipt(path.join(directory!,"terminal-bound.json")))!==digest(terminal))hold("pi_detached_receipt_invalid","Bound terminal receipt changed");
    durable(path.join(directory!,"resume-began.json"),{attemptId,at:this.now(),actor:input.operator,actorGeneration:input.actorGeneration,reason:input.reason,bindingDigest:digest(b),beganSha256:hash(readFileSync(receiptPath!)),preflightDigest:digest(preflight),historySha256:hash(history.bytes),recovery:!!input.recovery,...(originalRunnerEntryPath?{originalRunnerEntryPath}:{})});effectAttempted=true;
    if(!(await this.deps.resume(b,preflight)).ok)hold("pi_detached_resume_unknown","Native resume was not confirmed; no replay or fresh fallback");
    const proof=await this.replacement(b,history.facts.nativeIdentity);const after=this.history(b);this.prefix(after.bytes,history.bytes);unchanged();
    if(after.facts.nativeIdentity!==history.facts.nativeIdentity)hold("pi_detached_history_changed","Resumed native identity changed");
    const result:Extract<PiDetachedResumeResult,{ok:true}>={ok:true,runtime:"pi",nodeId:b.nodeId,sessionName:b.sessionName,generation:b.generation,generationUnchanged:true,sessionId:b.sessionId,nativeIdHash:hash(b.nativeId),attemptId,receiptPath:receiptPath!,backup,supervisorLaunchId:proof.supervisorLaunchId,nativeFingerprint:proof.nativeFingerprint,custodyPreserved:true,guardLeftEnabled:true,authorityRepaired:false};
    if(this.deps.recordNativeRecoveryCompletion)durable(path.join(directory!,"completion-publication-began.json"),{attemptId,at:this.now(),blindRetryAllowed:false});
    const completedPath=path.join(directory!,"completed.json");
    durable(completedPath,{...result,at:this.now(),custodyAfter:this.custody(b)});
    if(this.deps.recordNativeRecoveryCompletion){try{await this.deps.recordNativeRecoveryCompletion({producer:"pi-detached-resume",recoveryId:attemptId,rigId:(this.deps.db.prepare("SELECT rig_id FROM nodes WHERE id=?").get(b.nodeId) as {rig_id:string}).rig_id,nodeId:b.nodeId,sessionId:b.sessionId,sessionName:b.sessionName,generation:b.generation,runtime:"pi",nativeIdentityHash:result.nativeIdHash,source:{ref:completedPath,digest:recoveryReceiptDigest(readFileSync(completedPath))},supervisorLaunchId:proof.supervisorLaunchId,nativeFingerprint:proof.nativeFingerprint});durable(path.join(directory!,"completion-publication-completed.json"),{attemptId,at:this.now()});}catch(error){durable(path.join(directory!,"completion-publication-unknown.json"),{attemptId,at:this.now(),blindRetryAllowed:false});throw error;}}
    return result;
   });
  }catch(error){
   const code=error instanceof Hold||error instanceof DeliveryGuardError?error.code:effectAttempted?"pi_detached_effect_unknown":"pi_detached_precondition_failed";
   if(effectAttempted&&directory){try{durable(path.join(directory,input.recovery?"recovery-unknown.json":"unknown.json"),{at:this.now(),code,effectAttempted:true,blindRetryAllowed:false});}catch{/* Original evidence remains immutable. */}}
   return {ok:false,code,message:error instanceof Hold||error instanceof DeliveryGuardError?error.message:"Required retained-native proof unavailable; original evidence is preserved",effectAttempted,blindRetryAllowed:false,...(receiptPath?{receiptPath}:{})};
  }
 }
 private actor(input:PiDetachedInput){const t=this.deps.guard.target("operator-agent@kernel"),s=this.deps.db.prepare('SELECT status,startup_status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1').get(t.nodeId) as {status:string;startup_status:string}|undefined;if(input.nodeId===t.nodeId||input.operator!=="operator-agent@kernel"||!input.actorGeneration||t.occupant!==input.actorGeneration||t.session!==input.operator||s?.status!=="running"||s.startup_status!=="ready")hold("pi_detached_actor","Current genuine ready Operator generation required; no self-repair");}
 private binding(input:PiDetachedInput):PiDetachedBinding{const r=this.deps.db.prepare("SELECT n.id nodeId,n.runtime,n.cwd,n.model,n.effort,n.profile,n.policy_launch_posture launchPosture,s.id sessionId,s.session_name sessionName,s.status,s.startup_status startupStatus,s.resume_type resumeType,s.resume_token nativeId FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1").get(input.nodeId) as Record<string,string|null>|undefined;const t=this.deps.db.prepare('SELECT generation_uuid FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal DESC LIMIT 1').get(input.nodeId) as {generation_uuid:string}|undefined;
  if(!r||r.runtime!=="pi"||r.sessionName!==input.sessionName||!["detached","running"].includes(r.status??"")||r.startupStatus!=="ready"||r.resumeType!=="pi_session_file"||!r.nativeId||!path.isAbsolute(r.nativeId)||!r.cwd||!path.isAbsolute(r.cwd)||!r.model||!t?.generation_uuid)hold("pi_detached_binding","Exact retained ready Pi session, file, model and generation required");
  return {nodeId:r.nodeId!,sessionId:r.sessionId!,sessionName:r.sessionName!,generation:t.generation_uuid,runtime:"pi",nativeId:r.nativeId,cwd:r.cwd,model:r.model,effort:r.effort??null,profile:r.profile??null,launchPosture:r.launchPosture??null};}
 private pane(nodeId:string){const r=this.deps.db.prepare('SELECT tmux_pane FROM bindings WHERE node_id=?').get(nodeId) as {tmux_pane:string|null}|undefined;if(!r?.tmux_pane)hold("pi_detached_binding","Retained physical binding required");return r.tmux_pane;}
 private gates(b:PiDetachedBinding){const t=this.deps.guard.target(b.nodeId),p=this.deps.guard.preference(b.nodeId);if(t.session!==b.sessionName||t.occupant!==b.generation||p.desired!==true||p.effective!==true)hold("pi_detached_guard","Exact generation and guard desired/effective ON required");const a=rotationLocalAddresses(this.deps.db,b.sessionName);if(this.deps.db.prepare("SELECT 1 FROM seat_dispatch_reservations WHERE node_id=? AND state!='released'").get(b.nodeId))hold("pi_detached_reservation","Unresolved reservation excludes native recovery");if(this.deps.db.prepare("SELECT 1 FROM outbox_entries WHERE delivery_state='sending' AND (sender_session IN (?,?) OR destination_session IN (?,?))").get(...a,...a))hold("pi_detached_sending","In-flight sending effect excludes recovery");if(this.deps.db.prepare('SELECT 1 FROM coordinator_authority WHERE owner_session IN (?,?) AND owner_generation!=?').get(...a,b.generation))hold("pi_detached_authority","Retained authority names another generation");}
 private root(b:PiDetachedBinding){const root=this.deps.snapshotRoot;if(!path.isAbsolute(root))hold("pi_detached_private_store","Absolute snapshot root required");mkdirSync(root,{recursive:true,mode:0o700});privatePath(root,true);const node=path.join(realpathSync(root),digest([b.nodeId,b.generation]));mkdirSync(node,{recursive:true,mode:0o700});privatePath(node,true);return node;}
 private noUnresolved(root:string,b:PiDetachedBinding,recoveryDir?:string){for(const id of readdirSync(root)){const dir=path.join(root,id);privatePath(dir,true);if(existsSync(path.join(dir,"completion-publication-unknown.json"))||(existsSync(path.join(dir,"completion-publication-began.json"))&&!existsSync(path.join(dir,"completion-publication-completed.json"))))hold("pi_detached_completion_unresolved","Successful native replacement completion publication unresolved; no native retry");if(!existsSync(path.join(dir,"began.json")))continue;if(dir===recoveryDir){if(existsSync(path.join(dir,"completed.json"))||existsSync(path.join(dir,"resume-began.json")))hold("pi_detached_replay","Prior resume intent or completion forbids replay");}else{if(!existsSync(path.join(dir,"completed.json")))hold("pi_detached_unresolved","Unresolved original native effect must be reconciled first");const completed=readReceipt(path.join(dir,"completed.json"));if(completed.ok!==true||completed.nodeId!==b.nodeId||completed.generation!==b.generation)hold("pi_detached_unresolved","Original completion does not match current node/generation");}}}
 private history(b:PiDetachedBinding){if(realpathSync(b.nativeId)!==b.nativeId)hold("pi_detached_history","Canonical nonsymlink retained native path required");const before=lstatSync(b.nativeId);if(!before.isFile()||before.isSymbolicLink())hold("pi_detached_history","Regular retained session file required");const bytes=readFileSync(b.nativeId),after=lstatSync(b.nativeId);if(!bytes.length||bytes.at(-1)!==10||before.ino!==after.ino||before.size!==after.size||before.mtimeMs!==after.mtimeMs)hold("pi_detached_history_changed","Complete stable retained native history required");const facts=this.deps.validateHistory(b,bytes);if(!facts.nativeIdentity||!facts.lastEntryId)hold("pi_detached_history","Native header and final entry identity required");return {bytes,facts};}
 private prefix(bytes:Buffer,prior:Buffer){if(bytes.length<prior.length||!bytes.subarray(0,prior.length).equals(prior))hold("pi_detached_history_changed","Retained native history was rewritten or truncated");}
 private async absent(b:PiDetachedBinding){if(!await this.deps.terminalAbsent(b)||!await this.deps.proveNativeAbsent(b,0))hold("pi_detached_absence","Exact terminal and global native identity absence required");}
 private async bare(b:PiDetachedBinding,t:{pane:string;panePid:number;startedAt:string}){const pid=await this.deps.tmux.getPanePid(t.pane),rows=await this.census(),r=rows.find(x=>x.pid===pid);const ids=new Set([pid]);for(let i=0;i<rows.length;i++){let changed=false;for(const row of rows)if(ids.has(row.ppid)&&!ids.has(row.pid)){ids.add(row.pid);changed=true;}if(!changed)break;}if(this.pane(b.nodeId)!==t.pane||pid!==t.panePid||r?.startedAt!==t.startedAt||!['sh','bash','zsh','fish','dash'].includes(r?.executableName??'')||ids.size!==1||!await this.deps.proveNativeAbsent(b,pid!)||await this.deps.tmux.getPanePid(t.pane)!==pid)hold("pi_detached_absence","Stable bare bound pane and independent global native absence required");}
 private async replacement(b:PiDetachedBinding,nativeIdentity:string){const poll=Math.max(1,this.deps.pollMs??200),attempts=Math.max(1,Math.ceil((this.deps.waitMs??20000)/poll));for(let n=0;n<attempts;n++){const p=await this.deps.observeReplacement(b);if(p){if(p.generation!==b.generation||p.sessionFile!==b.nativeId||p.nativeIdentity!==nativeIdentity||!p.supervisorLaunchId||!p.nativeFingerprint)hold("pi_detached_replacement_mismatch","Replacement native identity contradicts retained occupant");return p;}if(n+1<attempts)await this.sleep(poll);}hold("pi_detached_replacement_unknown","Exact native and managed supervisor proof unavailable; no retry");}
 private unknownRows(b:PiDetachedBinding){const a=rotationLocalAddresses(this.deps.db,b.sessionName);return (this.deps.db.prepare("SELECT * FROM outbox_entries WHERE delivery_state='indeterminate' AND (sender_session IN (?,?) OR destination_session IN (?,?)) ORDER BY outbox_id").all(...a,...a) as Array<Record<string,unknown>>).map(r=>({id:r.outbox_id,sha256:digest(r)}));}
 private unknownPreserved(old:unknown,b:PiDetachedBinding){if(!Array.isArray(old))return false;const rows=new Map(this.unknownRows(b).map(x=>[x.id,x.sha256]));return old.every(x=>x&&typeof x.id==='string'&&rows.get(x.id)===x.sha256);}
 private custody(b:PiDetachedBinding):Record<string,{count:number;sha256:string}>{const db=this.deps.db,a=rotationLocalAddresses(db,b.sessionName);const omit=(r:unknown,keys:string[])=>Object.fromEntries(Object.entries(r as Record<string,unknown>).filter(([k])=>!keys.includes(k)));const rows={
  node:db.prepare('SELECT * FROM nodes WHERE id=?').all(b.nodeId),sessions:db.prepare('SELECT * FROM sessions WHERE node_id=? ORDER BY id').all(b.nodeId).map(r=>omit(r,['status','last_seen_at','startup_completed_at','resume_last_verified','resume_last_probe_status'])),bindings:db.prepare('SELECT * FROM bindings WHERE node_id=?').all(b.nodeId).map(r=>omit(r,['tmux_pane','tmux_window','updated_at'])),tenures:db.prepare('SELECT * FROM occupant_tenures WHERE node_id=? ORDER BY id').all(b.nodeId),permissions:db.prepare('SELECT * FROM node_permission_selections WHERE node_id=?').all(b.nodeId),
  queue:db.prepare('SELECT * FROM queue_items WHERE destination_session IN (?,?) OR claimed_by_generation_uuid=? ORDER BY qitem_id').all(...a,b.generation),claims:db.prepare("SELECT * FROM queue_items WHERE claimed_by_generation_uuid=? AND state='in-progress' ORDER BY qitem_id").all(b.generation),authority:db.prepare('SELECT * FROM coordinator_authority WHERE owner_session IN (?,?) OR owner_generation=? ORDER BY rig_id').all(...a,b.generation),assignments:db.prepare('SELECT * FROM coordinator_assignments WHERE destination IN (?,?) ORDER BY rig_id,package_key').all(...a),staged:db.prepare('SELECT * FROM coordinator_stage_assignments WHERE source IN (?,?) OR destination IN (?,?) ORDER BY rig_id,package_key').all(...a,...a),resources:db.prepare('SELECT * FROM coordinator_resources WHERE (rig_id,package_key) IN (SELECT rig_id,package_key FROM coordinator_assignments WHERE destination IN (?,?) UNION SELECT rig_id,package_key FROM coordinator_stage_assignments WHERE source IN (?,?) OR destination IN (?,?)) ORDER BY rig_id,resource_key').all(...a,...a,...a),outbox:db.prepare('SELECT * FROM outbox_entries WHERE sender_session IN (?,?) OR destination_session IN (?,?) ORDER BY outbox_id').all(...a,...a)};return Object.fromEntries(Object.entries(rows).map(([k,v])=>[k,{count:v.length,sha256:digest(v)}]));}
}
