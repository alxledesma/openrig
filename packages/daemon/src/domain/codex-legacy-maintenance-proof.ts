import type Database from 'better-sqlite3';
import {readFileSync} from 'node:fs';
import type {WhoamiService} from './whoami-service.js';
import type {SeatDeliveryGuard} from './seat-delivery-guard.js';
import type {CodexRehostNativeState} from './codex-rehost.js';
import {forEachJsonlLine} from './rotation-native-proof.js';
import {listNativeProcesses,observeCodexPaneProcess} from './native-process-lineage.js';
import {verifyNativeDutyProcessIdentity} from './native-duty-launch.js';
import {proveCodexNativeThread} from './codex-native-thread-proof.js';

/** Only the maintenance service calls this under its exact rehost capability.
 * The current native contract is observed independently of stale saved model/
 * profile pins. A future profile name is a selection, never old-launch proof. */
export async function observeLegacyCodexMaintenance(deps:{db:Database.Database;whoami:WhoamiService;guard:SeatDeliveryGuard;tmux:{getPanePid(pane:string):Promise<number|null>}},
 session:string,profile:string,seams:{processes?:typeof listNativeProcesses;verify?:typeof verifyNativeDutyProcessIdentity;thread?:typeof proveCodexNativeThread}={}):Promise<CodexRehostNativeState>{
 const hold=():never=>{throw new Error('Exact live legacy Operator native contract unavailable');};
 if(session!=='operator-agent@kernel'||!/^[a-zA-Z0-9_-]+$/.test(profile))return hold();
 const target=deps.guard.target(session);
 if(!deps.guard.ownsRunnerRehost(target.nodeId)||!target.pane||!target.occupant)return hold();
 const read=()=>deps.db.prepare(`SELECT n.id,n.runtime,n.model,n.effort,n.codex_config_profile,n.cwd,s.id sessionId,s.session_name,s.resume_type,s.resume_token,s.status,s.startup_status
   FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1`).get(target.nodeId) as Record<string,string|null>|undefined;
 const before=read(),who=deps.whoami.resolve({nodeId:target.nodeId,compact:false}),usage=who?.contextUsage as {sessionId?:string;transcriptPath?:string}|undefined;
 if(!before||before.runtime!=='codex'||before.codex_config_profile!==null||before.session_name!==session||before.resume_type!=='codex_id'||before.status!=='running'||before.startup_status!=='ready'
   ||who?.identity.nodeId!==target.nodeId||who.identity.runtime!=='codex'||!usage?.transcriptPath||usage.sessionId!==before.resume_token)return hold();
 const identity={OPENRIG_NODE_ID:target.nodeId,OPENRIG_SESSION_NAME:session,OPENRIG_OCCUPANT_GENERATION:target.occupant,OPENRIG_RUNTIME:'codex'};
 const observe=()=>observeCodexPaneProcess({target:target.pane!,tmux:deps.tmux,listProcesses:seams.processes??listNativeProcesses,expectedToken:before.resume_token,requireResume:false});
 const first=await observe();if(!first||!await(seams.verify??verifyNativeDutyProcessIdentity)(first.process.pid,identity))return hold();
 const revalidateThread=await(seams.thread??proveCodexNativeThread)(first.process.pid,usage.transcriptPath,first.process.command);
 let meta:Record<string,unknown>|undefined,turn:Record<string,unknown>|undefined,count=0;
 forEachJsonlLine(readFileSync(usage.transcriptPath),line=>{if(!line.toString().trim())return;const row=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(line));if(row.type==='session_meta'){meta=row.payload;count++;}if(row.type==='turn_context')turn=row.payload;});
 const provider=turn?.model_provider??meta?.model_provider;
 if(count!==1||(meta?.id??meta?.session_id)!==before.resume_token||typeof turn?.model!=='string'||typeof provider!=='string'||typeof turn?.effort!=='string'||typeof turn.approval_policy!=='string')return hold();
 // Legacy bootstrap is narrow: the existing launcher can prove these exact
 // sandbox modes, but cannot reproduce arbitrary per-turn writable-root or
 // network exceptions. Keep the full object in evidence and refuse extras.
 const sandbox=turn.sandbox_policy;
 if(!sandbox||typeof sandbox!=='object'||Array.isArray(sandbox)||Object.keys(sandbox).join(',')!=='type'
   ||!['danger-full-access','workspace-write'].includes((sandbox as {type:string}).type))return hold();
 const second=await observe();await revalidateThread();
 if(!second||second.fingerprint!==first.fingerprint||!await(seams.verify??verifyNativeDutyProcessIdentity)(second.process.pid,identity)
   ||JSON.stringify(read())!==JSON.stringify(before)||JSON.stringify(deps.guard.target(session))!==JSON.stringify(target))return hold();
 const args=first.process.command.match(/"[^"]*"|'[^']*'|\S+/g)?.map(s=>s.replace(/^['"]|['"]$/g,''))??[];
 const index=args.findIndex(x=>x==='-p'||x==='--profile');
 return {nodeId:target.nodeId,sessionName:session,nativeId:before.resume_token!,transcriptPath:usage.transcriptPath,
   legacyLaunch:{observedProfile:index>=0?args[index+1]??null:null},
   runtimeContract:{runtime:'codex',model:turn.model as string,provider,profile,effort:turn.effort as string,permissions:{sandbox,approval:turn.approval_policy as string}}};
}
