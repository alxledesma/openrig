import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { CodexRuntimeAdapter } from "../adapters/codex-runtime-adapter.js";
import type { CodexResumeAdapter } from "../adapters/codex-resume.js";
import type { TmuxAdapter } from "../adapters/tmux.js";
import type { SeatActivityService } from "./seat-activity-service.js";
import type { WhoamiService } from "./whoami-service.js";
import type { SeatDeliveryGuard } from "./seat-delivery-guard.js";
import type { Binding } from "./types.js";
import type { CodexDaemonSupportDetector } from "./codex-daemon-support.js";
import { CodexSameGenerationRehost, type CodexRehostNativeState, type CodexRehostBinding } from "./codex-rehost.js";
import { forEachJsonlLine } from "./rotation-native-proof.js";
import { NativeDutyLaunchStore, observeNativeDutyLaunch, verifyNativeDutyProcessIdentity } from "./native-duty-launch.js";
import { resolveCodexNativeState } from "./rotation-facts-resolver.js";
import { listNativeProcesses } from "./native-process-lineage.js";
import { SeatLaunchEnvironment, structuredNativeExecutable } from "./seat-launch-environment.js";

// No argv/environment values cross this subprocess boundary. Unlike a positive
// identity predicate, an unreadable process is UNKNOWN, never negative evidence.
const STOPPED_CENSUS_PY = String.raw`import ctypes,json,os,struct,subprocess,sys
def executable_path(kernel,native):
 if not kernel or not os.path.isabs(kernel) or any(ord(c)<32 for c in kernel): raise ValueError()
 if native is not None:
  if not os.path.isabs(native) or any(ord(c)<32 for c in native): raise ValueError()
  # macOS Cryptex aliases can disagree for unrelated applications. Identity env
  # is still checked below; only a possibly Codex executable needs equivalence.
  if 'codex' in [os.path.basename(kernel),os.path.basename(native)] and native!=kernel and not os.path.samefile(native,kernel): raise ValueError()
 if os.path.basename(kernel)=='codex': return kernel
 return native if native is not None else kernel
def argv_env(pid):
 if sys.platform=='darwin':
  libc=ctypes.CDLL(None); mib=(ctypes.c_int*3)(1,49,pid); n=ctypes.c_size_t(0)
  if libc.sysctl(mib,3,None,ctypes.byref(n),None,0)!=0 or not 0<n.value<=8388608: raise ValueError()
  b=ctypes.create_string_buffer(n.value)
  if libc.sysctl(mib,3,b,ctypes.byref(n),None,0)!=0: raise ValueError()
  raw=b.raw[:n.value]; argc=struct.unpack_from('i',raw)[0]
  if not 0<argc<=65536: raise ValueError()
  end=raw.index(b'\0',4); kernel_executable=raw[4:end].decode('utf8'); at=end+1
  while at<len(raw) and raw[at]==0: at+=1
  argv=[]
  for _ in range(argc):
   end=raw.index(b'\0',at); argv.append(raw[at:end].decode('utf8')); at=end+1
  env=raw[at:].split(b'\0')
  lib=ctypes.CDLL('/usr/lib/libproc.dylib'); buf=ctypes.create_string_buffer(4096)
  result=lib.proc_pidpath(pid,buf,len(buf))
  executable=executable_path(kernel_executable,buf.value.decode('utf8') if result>0 else None)
 elif sys.platform.startswith('linux'):
  argv=open('/proc/'+str(pid)+'/cmdline','rb').read().rstrip(b'\0').decode('utf8').split('\0')
  env=open('/proc/'+str(pid)+'/environ','rb').read().split(b'\0'); executable=os.readlink('/proc/'+str(pid)+'/exe')
 else: raise ValueError()
 return argv,env,executable
def native_thread(argv):
 values={'-a','--ask-for-approval','-c','--config','-m','--model','-p','--profile','-s','--sandbox','-C','--cd','-i','--image','--add-dir','--enable','--disable','--local-provider','--remote','--remote-auth-token-env'}
 flags={'--no-daemon','--yolo','--no-alt-screen','--full-auto','--dangerously-bypass-approvals-and-sandbox','--oss','--search','--strict-config','--approve-for-me','--worktree','--dangerously-bypass-hook-trust','--help','-h','--version','-V'}
 args=argv[1:]; i=0; resumed=False
 while i<len(args):
  a=args[i]
  if a in ['-i','--image'] or a.startswith('--image='): raise ValueError() # variadic image arguments are ambiguous for exclusion
  if a in values:
   if i+1>=len(args) or args[i+1].startswith('-'): raise ValueError()
   i+=2; continue
  if '=' in a and a.split('=',1)[0] in values:
   if not a.split('=',1)[1]: raise ValueError()
   i+=1; continue
  if a in flags: i+=1; continue
  if a.startswith('-'): raise ValueError()
  if resumed: return a
  if a=='resume': resumed=True; i+=1; continue
  # These named commands cannot themselves be the old interactive resume.
  if a in ['app-server','exec-server','mcp-server','login','logout','completion','features','debug','sandbox']: return None
  if a=='exec': i+=1; continue
  raise ValueError()
 if resumed: raise ValueError()
 return None
ok=False
try:
 expected=json.loads(sys.argv[1]); pane=int(sys.argv[2]); native=sys.argv[3]
 rows=subprocess.check_output(['/bin/ps','-axo','pid=,uid=,stat='],env={'PATH':'/usr/bin:/bin','LC_ALL':'C'}).decode().splitlines()
 for row in rows:
  fields=row.split()
  if len(fields)!=3: raise ValueError()
  pid,uid,state=int(fields[0]),int(fields[1]),fields[2]
  if uid!=os.getuid() or pid in [pane,os.getpid()] or state.startswith('Z'): continue
  try: argv,env,executable=argv_env(pid)
  except:
   # A disappeared process cannot survive this sample; every other read failure holds.
   try: os.kill(pid,0)
   except ProcessLookupError: continue
   raise
  identity={}
  for k in expected:
   prefix=(k+'=').encode(); values=[x[len(prefix):] for x in env if x.startswith(prefix)]
   if len(values)>1: raise ValueError()
   identity[k]=values[0].decode('utf8') if values else None
  # Broader same-seat exclusion also catches stale-generation wrappers. The
  # independently proven bare pane root alone is exempt from inherited tmux env.
  if identity['OPENRIG_NODE_ID']==expected['OPENRIG_NODE_ID'] or identity['OPENRIG_SESSION_NAME']==expected['OPENRIG_SESSION_NAME']: raise ValueError()
  if os.path.basename(executable)=='codex' and native_thread(argv)==native: raise ValueError()
 ok=True
except: pass
print('1' if ok else '0')`;

export async function proveStoppedCodexIdentityAbsent(binding:CodexRehostBinding,panePid:number):Promise<boolean>{
  if(!Number.isSafeInteger(panePid)||panePid<=1)return false;
  try {const result=await promisify(execFile)('python3',['-c',STOPPED_CENSUS_PY,JSON.stringify({OPENRIG_NODE_ID:binding.nodeId,OPENRIG_SESSION_NAME:binding.sessionName}),String(panePid),binding.nativeId],{timeout:5000,maxBuffer:128,encoding:'utf8'});return result.stdout.trim()==='1';}catch{return false;}
}

/** Retained native records are contract evidence, not a claim of live process
 * identity. The recovery service validates the complete strict transcript and
 * backup prefix and reproduces the original private preflight digest separately. */
export function stoppedCodexContract(file:string,binding:CodexRehostBinding):CodexRehostNativeState['runtimeContract']{
  let meta:Record<string,unknown>|undefined,turn:Record<string,unknown>|undefined;
  forEachJsonlLine(file,bytes=>{const row=JSON.parse(new TextDecoder('utf8',{fatal:true}).decode(bytes));if(row.type==='session_meta')meta=row.payload;if(row.type==='turn_context')turn=row.payload;});
  const provider=turn?.model_provider??meta?.model_provider;
  if((meta?.id??meta?.session_id)!==binding.nativeId||turn?.model!==binding.model||typeof provider!=='string'||!turn.sandbox_policy||typeof turn.approval_policy!=='string'||(turn.effort!==undefined&&turn.effort!==null&&typeof turn.effort!=='string'))throw new Error('Retained exact native contract unavailable');
  return {runtime:'codex',model:binding.model,provider,profile:binding.codexConfigProfile,effort:turn.effort as string|null??null,permissions:{sandbox:turn.sandbox_policy,approval:turn.approval_policy}};
}

/** Production composition only. Every effect remains inside the guarded rehost
 * service; this factory cannot grant or renew coordinator authority. */
export function createCodexRehostIntegration(deps: {
  db: Database.Database; guard: SeatDeliveryGuard; tmux: TmuxAdapter;
  whoami: WhoamiService; activity: SeatActivityService;
  adapter: CodexRuntimeAdapter; resume: CodexResumeAdapter;
  launchEnvironment: SeatLaunchEnvironment; store?: NativeDutyLaunchStore;
  launchPath: string; snapshotRoot: string; detectDaemonSupport: CodexDaemonSupportDetector;
  configurationDigest(session: string): string | null | undefined;
}) {
  const currentBinding = async (nodeId: string) => {
    if (!deps.guard.ownsRunnerRehost(nodeId)) return null;
    const target = deps.guard.maybeTarget(nodeId);
    if (!target?.occupant || !target.pane) return null;
    // Only this exact rehost lease and its required guard are exempted for
    // read-only process proof. Durable reservations still refuse the proof.
    if (deps.guard.protectionFacts(nodeId)?.code === "seat_dispatch_reserved") return null;
    const row = deps.db.prepare("SELECT n.runtime,s.resume_token FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1")
      .get(nodeId) as { runtime: string; resume_token: string | null } | undefined;
    const configurationDigest = deps.configurationDigest(target.session);
    if (row?.runtime !== "codex" || !row.resume_token || !configurationDigest) return null;
    return { nodeId, sessionName: target.session, generation: target.occupant,
      runtime: "codex" as const, configurationDigest, pane: target.pane,
      resumeToken: row.resume_token, lifecycleReserved: false };
  };
  return new CodexSameGenerationRehost({
    db: deps.db, guard: deps.guard, tmux: deps.tmux, resume: deps.resume, snapshotRoot: deps.snapshotRoot,
    nativeState: async session => {
      const state = await resolveCodexNativeState(deps, session);
      const contract = state.runtimeContract as CodexRehostNativeState["runtimeContract"];
      if (!state.usage.sessionId || !state.usage.transcriptPath || contract.runtime !== "codex") throw new Error("Exact Codex native history unavailable");
      return { nodeId: state.who.identity.nodeId, sessionName: session, nativeId: state.usage.sessionId,
        transcriptPath: state.usage.transcriptPath, runtimeContract: contract };
    },
    stoppedNativeState: async binding => {
      if(!deps.guard.ownsRunnerRehost(binding.nodeId))throw new Error('Owned recovery lease required');
      const who=deps.whoami.resolve({sessionName:binding.sessionName,compact:false});
      const usage=who?.contextUsage as {sessionId?:string;transcriptPath?:string}|undefined;
      if(who?.identity.nodeId!==binding.nodeId||who.identity.runtime!=='codex'||usage?.sessionId!==binding.nativeId||!usage.transcriptPath)throw new Error('Saved exact native history unavailable');
      return {nodeId:binding.nodeId,sessionName:binding.sessionName,nativeId:binding.nativeId,transcriptPath:usage.transcriptPath,runtimeContract:stoppedCodexContract(usage.transcriptPath,binding)};
    },
    proveStoppedIdentityAbsent: proveStoppedCodexIdentityAbsent,
    activityWitness: async (nodeId, pane) => {
      if (!deps.guard.ownsRunnerRehost(nodeId)) return null;
      const target = deps.guard.maybeTarget(nodeId);
      if (!target || target.pane !== pane) return null;
      await deps.activity.pollSeat(target.session);
      return deps.activity.getRotationActivityWitness(nodeId);
    },
    preflightSupervisedLaunch: async (binding, native) => {
      if (!deps.store || !deps.guard.ownsRunnerRehost(binding.nodeId)
        || !await deps.launchEnvironment.usesNativeDuty(binding.sessionName, binding.nodeId)) throw new Error("Supervised Codex rehost is not enabled");
      deps.store.assertReady();
      const current = await currentBinding(binding.nodeId);
      if (!current || current.generation !== binding.generation || current.sessionName !== binding.sessionName
        || current.resumeToken !== binding.nativeId) throw new Error("Codex rehost binding changed");
      const stored = deps.db.prepare(`SELECT id,node_id AS nodeId,attachment_type AS attachmentType,
        tmux_session AS tmuxSession,tmux_window AS tmuxWindow,tmux_pane AS tmuxPane,
        external_session_name AS externalSessionName,cmux_workspace AS cmuxWorkspace,
        cmux_surface AS cmuxSurface,updated_at AS updatedAt FROM bindings WHERE node_id=?`)
        .get(binding.nodeId) as Binding | undefined;
      const node = deps.db.prepare("SELECT policy_launch_posture FROM nodes WHERE id=?")
        .get(binding.nodeId) as { policy_launch_posture: string | null } | undefined;
      const sandbox = native.runtimeContract.permissions.sandbox;
      const sandboxType = sandbox && typeof sandbox === "object" ? (sandbox as { type?: unknown }).type : sandbox;
      if (sandboxType !== "workspace-write" && sandboxType !== "danger-full-access") throw new Error("Codex rehost cannot preserve this sandbox posture");
      const posture = sandboxType === "danger-full-access" ? "full_bypass" : "floor";
      if (!stored || stored.tmuxPane !== current.pane || (node?.policy_launch_posture && node.policy_launch_posture !== posture)) throw new Error("Codex rehost persisted posture or pane mismatch");
      const verified = await deps.adapter.preflightRuntimeMigration({ ...stored, cwd: binding.cwd,
        model: binding.model, effort: binding.effort ?? native.runtimeContract.effort ?? undefined, codexConfigProfile: binding.codexConfigProfile,
        launchPosture: posture });
      if (verified.effective.sandbox !== sandboxType || verified.effective.approval !== native.runtimeContract.permissions.approval
        || verified.effective.provider !== native.runtimeContract.provider) throw new Error("Codex rehost would change native permissions or provider");
      const daemon = await deps.detectDaemonSupport(binding.cwd);
      if (daemon.kind !== "supported") throw new Error("Supervised Codex rehost requires proven --no-daemon support");
      const harness = structuredNativeExecutable("codex", [], deps.launchPath, binding.cwd);
      // tmux retains the predecessor's generation/runtime after a managed
      // handover. Structured launch overrides those from the proven binding;
      // only the stable node/session address is inherited from tmux. The rehost
      // service separately proves current native kernel identity before effects.
      for (const [key, expected] of Object.entries({ OPENRIG_NODE_ID: binding.nodeId,
        OPENRIG_SESSION_NAME: binding.sessionName })) {
        if (await deps.tmux.getSessionEnv(binding.sessionName, key) !== expected) throw new Error("Codex rehost native launch environment mismatch");
      }
      return { posture, effective: verified.effective, evidenceDigest: createHash("sha256").update(JSON.stringify({
        configurationDigest: current.configurationDigest, profileSha256: verified.profileSha256,
        effective: verified.effective, posture, harness, daemon: daemon.kind,
      })).digest("hex") };
    },
    observeSupervisedReplacement: async binding => {
      if (!deps.store) return null;
      const current = await currentBinding(binding.nodeId);
      const latest = deps.store.latest(binding.nodeId, binding.generation);
      if (!current || !latest || current.resumeToken !== binding.nativeId || current.sessionName !== binding.sessionName
        || latest.intent.configurationDigest !== current.configurationDigest) return null;
      const panePid = await deps.tmux.getPanePid(current.pane);
      if (!panePid) return null;
      const rows = await listNativeProcesses(), descendants = new Set([panePid]);
      for (let i = 0; i < rows.length; i++) {
        let changed = false;
        for (const row of rows) if (descendants.has(row.ppid) && !descendants.has(row.pid)) { descendants.add(row.pid); changed = true; }
        if (!changed) break;
      }
      const identity = { OPENRIG_NODE_ID: binding.nodeId, OPENRIG_SESSION_NAME: binding.sessionName,
        OPENRIG_OCCUPANT_GENERATION: binding.generation, OPENRIG_RUNTIME: "codex" };
      const supervisors: number[] = [];
      for (const row of rows) if (descendants.has(row.pid) && row.executableName === "node"
        && await verifyNativeDutyProcessIdentity(row.pid, identity,
          [latest.intent.installedNode.path, latest.intent.installedSupervisor.path, "--supervise", latest.intent.configPath])) supervisors.push(row.pid);
      if (supervisors.length !== 1) return null;
      const proof = await observeNativeDutyLaunch(deps.store, { scope: latest.intent,
        launchId: latest.intent.launchId, supervisorPid: supervisors[0]! }, { currentBinding, tmux: deps.tmux });
      return proof ? { launchId: proof.launchId, fingerprint: proof.fingerprint } : null;
    },
  });
}
