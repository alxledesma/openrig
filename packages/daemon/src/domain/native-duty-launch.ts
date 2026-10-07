import { createHash, randomUUID } from "node:crypto";
import { accessSync, closeSync, constants, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { NativeDutyLaunchConfig } from "../adapters/native-duty-supervisor.js";
import type { NativeDutyProof, NativeDutyScope } from "./native-duty-contract.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";
import { listNativeProcesses, observeCodexPaneProcess, type NativeProcessRow } from "./native-process-lineage.js";

const SCHEMA = "openrig.native-duty-launch/v1";
const identifier = (v: unknown): v is string => typeof v === "string" && /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,159}$/.test(v);
const sha = (v: unknown): v is string => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
const hash = (bytes: string | Buffer): string => createHash("sha256").update(bytes).digest("hex");
const canonical = (v: unknown): string => JSON.stringify(v, (_k, value) => value && typeof value === "object" && !Array.isArray(value)
  ? Object.fromEntries(Object.entries(value).sort(([a],[b]) => a.localeCompare(b))) : value);
const equal = (a: unknown,b: unknown): boolean => canonical(a) === canonical(b);
const ownKeys = (v: unknown, keys: string[]): boolean => !!v && typeof v === "object" && !Array.isArray(v)
  && equal(Object.keys(v).sort(),[...keys].sort());
const absolute = (v: unknown): v is string => typeof v === "string" && path.isAbsolute(v) && !v.includes("\0") && path.normalize(v) === v;
const uid = (): number => { const value=process.getuid?.(); if(value === undefined) throw new Error("native-duty-owner-unavailable"); return value; };

export interface NativeDutyLaunchIdentity {
  scopeId: string; launchId: string; nodeId: string; sessionName: string; generation: string;
  runtime: "codex" | "pi"; configurationDigest: string;
}
interface InstalledFile { path: string; sha256: string; size: number; device: number; inode: number; modifiedAt: number; changedAt: number; mode: number }
export interface NativeDutyLaunchIntent extends NativeDutyLaunchIdentity {
  schema: typeof SCHEMA; createdAt: number; configPath: string; configSha256: string;
  installedNode: InstalledFile; installedSupervisor: InstalledFile;
}
export interface PreparedNativeDutyLaunch {
  intent: NativeDutyLaunchIntent;
  config: NativeDutyLaunchConfig;
  intentPath: string;
  /** Structured argv only. The caller retains its existing private environment channel. */
  launch: { executable: string; args: string[]; cwd: string; stdio: "inherit"; inheritEnvironment: true };
  publicEnvironment: Record<"OPENRIG_NODE_ID" | "OPENRIG_SESSION_NAME" | "OPENRIG_OCCUPANT_GENERATION" | "OPENRIG_RUNTIME", string>;
}
export interface NativeDutyLaunchInput extends Omit<NativeDutyLaunchIdentity,"launchId"> {
  launchId?: string;
  /** Already composed native argv. Credentials must remain in inherited env, never argv/config. */
  harness: NativeDutyLaunchConfig["harness"];
  pollMs: number;
}
function publicIdentity(i: NativeDutyLaunchIdentity): PreparedNativeDutyLaunch["publicEnvironment"] {
  return {OPENRIG_NODE_ID:i.nodeId,OPENRIG_SESSION_NAME:i.sessionName,OPENRIG_OCCUPANT_GENERATION:i.generation,OPENRIG_RUNTIME:i.runtime};
}
function privatePath(file: string, directory: boolean): void {
  const stat=lstatSync(file);
  if(stat.isSymbolicLink() || (directory ? !stat.isDirectory() : !stat.isFile()) || stat.uid !== uid() || (stat.mode & 0o077) !== 0)
    throw new Error("native-duty-private-owned-path-required");
}
function fileStamp(file: string): Omit<InstalledFile,"sha256"> {
  const stat=lstatSync(file);
  if(!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o022)!==0) throw new Error("native-duty-installed-file-untrusted");
  return {path:file,size:stat.size,device:stat.dev,inode:stat.ino,modifiedAt:stat.mtimeMs,changedAt:stat.ctimeMs,mode:stat.mode};
}
function installedFile(file: string): InstalledFile {
  const resolved=realpathSync(file),before=fileStamp(resolved),digest=hash(readFileSync(resolved));
  if(!equal(before,fileStamp(resolved))) throw new Error("native-duty-installed-file-changed");
  return {...before,sha256:digest};
}
function unchangedInstalled(file: InstalledFile): boolean {
  const {sha256: _digest,...stamp}=file;
  return equal(stamp,fileStamp(file.path));
}
function syncDirectory(directory: string): void { const fd=openSync(directory,constants.O_RDONLY);try{fsyncSync(fd);}finally{closeSync(fd);} }
function writeExclusive(file: string, bytes: string): void {
  const fd=openSync(file,constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL,0o600);
  try{writeFileSync(fd,bytes,"utf8");fsyncSync(fd);}finally{closeSync(fd);}
}
function validIdentity(i: NativeDutyLaunchIdentity): boolean {
  return [i.scopeId,i.launchId,i.nodeId,i.sessionName,i.generation].every(identifier)
    && (i.runtime === "codex" || i.runtime === "pi") && sha(i.configurationDigest);
}
function validConfig(c: NativeDutyLaunchConfig): boolean {
  return ownKeys(c,["scopeId","launchId","journalDir","harness","pollMs"]) && identifier(c.scopeId) && identifier(c.launchId)
    && absolute(c.journalDir) && ownKeys(c.harness,["executable","args","cwd"]) && absolute(c.harness.executable) && absolute(c.harness.cwd)
    && Array.isArray(c.harness.args) && c.harness.args.every(arg=>typeof arg === "string" && !arg.includes("\0"))
    && Number.isSafeInteger(c.pollMs) && c.pollMs>=1000 && c.pollMs<=60000;
}

/** Private, immutable opt-in launch intents. No grant, process or effect is created here.
 * Node bytes are hashed once per store instance; subsequent reads require unchanged inode,
 * size, mtime and ctime. A replacement install requires a new store/launch, never silent adoption. */
export class NativeDutyLaunchStore {
  readonly root: string;
  private readonly node: InstalledFile;
  private readonly supervisor: InstalledFile;
  constructor(input:{root:string;nodeExecutable:string;supervisorEntry:string;now?:()=>number}) {
    if(!absolute(input.root) || !absolute(input.nodeExecutable) || !absolute(input.supervisorEntry)) throw new Error("native-duty-absolute-path-required");
    mkdirSync(input.root,{recursive:true,mode:0o700});privatePath(input.root,true);this.root=realpathSync(input.root);
    this.node=installedFile(input.nodeExecutable);accessSync(this.node.path,constants.X_OK);this.supervisor=installedFile(input.supervisorEntry);
    this.now=input.now ?? Date.now;
  }
  private readonly now:()=>number;
  prepare(input:NativeDutyLaunchInput):PreparedNativeDutyLaunch {
    const identity:NativeDutyLaunchIdentity={scopeId:input.scopeId,launchId:input.launchId ?? randomUUID(),nodeId:input.nodeId,
      sessionName:input.sessionName,generation:input.generation,runtime:input.runtime,configurationDigest:input.configurationDigest};
    if(!validIdentity(identity) || !unchangedInstalled(this.node) || !unchangedInstalled(this.supervisor)) throw new Error("native-duty-launch-invalid");
    privatePath(this.root,true);
    const directory=path.join(this.root,identity.launchId),configPath=path.join(directory,"supervisor-config.json"),intentPath=path.join(directory,"launch-intent.json");
    const config:NativeDutyLaunchConfig={scopeId:identity.scopeId,launchId:identity.launchId,journalDir:path.join(directory,"journal"),
      harness:{executable:input.harness.executable,args:[...input.harness.args],cwd:input.harness.cwd},pollMs:input.pollMs};
    if(!validConfig(config)) throw new Error("native-duty-launch-invalid");
    const createdAt=this.now();if(!Number.isSafeInteger(createdAt)||createdAt<0)throw new Error("native-duty-clock-invalid");
    mkdirSync(directory,{mode:0o700}); // no replay, overwrite, or resume of a partial prior intent
    mkdirSync(config.journalDir,{mode:0o700});
    const configBytes=canonical(config)+"\n";
    const intent:NativeDutyLaunchIntent={schema:SCHEMA,...identity,createdAt,configPath,configSha256:hash(configBytes),installedNode:this.node,installedSupervisor:this.supervisor};
    writeExclusive(configPath,configBytes);syncDirectory(config.journalDir);
    writeExclusive(intentPath,canonical(intent)+"\n");syncDirectory(directory);syncDirectory(this.root);
    return {intent,config,intentPath,launch:{executable:this.node.path,args:[this.supervisor.path,"--supervise",configPath],cwd:config.harness.cwd,stdio:"inherit",inheritEnvironment:true},publicEnvironment:publicIdentity(identity)};
  }
  read(scopeId:string,launchId:string):{intent:NativeDutyLaunchIntent;config:NativeDutyLaunchConfig}|null {
    if(!identifier(scopeId)||!identifier(launchId))return null;
    const value=this.readLaunch(launchId);return value?.intent.scopeId===scopeId?value:null;
  }
  /** Latest immutable intent for exactly this node/generation. It does not assert a live launch. */
  latest(nodeId:string,generation:string):{intent:NativeDutyLaunchIntent;config:NativeDutyLaunchConfig}|null {
    if(!identifier(nodeId)||!identifier(generation))return null;
    privatePath(this.root,true);
    const children=readdirSync(this.root);
    if(children.some(id=>!identifier(id)))return null;
    const all=children.map(id=>this.readLaunch(id));
    if(all.some(value=>!value))return null; // incomplete/altered intent is uncertainty, never an older fallback
    const candidates=all.filter((v):v is NonNullable<typeof v>=>!!v&&v.intent.nodeId===nodeId&&v.intent.generation===generation)
      .sort((a,b)=>b.intent.createdAt-a.intent.createdAt||b.intent.launchId.localeCompare(a.intent.launchId));
    // Equal-time competing intents are ambiguous, not an arbitrary latest launch.
    return candidates[0] && candidates[1]?.intent.createdAt!==candidates[0].intent.createdAt?candidates[0]:null;
  }
  private readLaunch(launchId:string):{intent:NativeDutyLaunchIntent;config:NativeDutyLaunchConfig}|null {
    try {
      privatePath(this.root,true);const directory=path.join(this.root,launchId),intentPath=path.join(directory,"launch-intent.json"),configPath=path.join(directory,"supervisor-config.json");
      privatePath(directory,true);privatePath(intentPath,false);privatePath(configPath,false);
      const intent=JSON.parse(readFileSync(intentPath,"utf8")) as NativeDutyLaunchIntent;
      if(!ownKeys(intent,["schema","scopeId","launchId","nodeId","sessionName","generation","runtime","configurationDigest","createdAt","configPath","configSha256","installedNode","installedSupervisor"])
        || intent.schema!==SCHEMA || !validIdentity(intent) || intent.launchId!==launchId || intent.configPath!==configPath || !sha(intent.configSha256)
        || !Number.isSafeInteger(intent.createdAt) || intent.createdAt<0 || !equal(intent.installedNode,this.node) || !equal(intent.installedSupervisor,this.supervisor)
        || !unchangedInstalled(this.node) || !unchangedInstalled(this.supervisor))return null;
      const bytes=readFileSync(configPath),config=JSON.parse(bytes.toString("utf8")) as NativeDutyLaunchConfig;
      if(hash(bytes)!==intent.configSha256 || !validConfig(config) || config.launchId!==launchId || config.scopeId!==intent.scopeId || config.journalDir!==path.join(directory,"journal"))return null;
      privatePath(config.journalDir,true);return {intent,config};
    } catch{return null;}
  }
}

export interface NativeDutyManagedBinding {
  nodeId:string;sessionName:string;generation:string;runtime:"codex"|"pi";configurationDigest:string;
  pane:string;resumeToken?:string|null;lifecycleReserved:boolean;
}
export interface NativeDutyLaunchObserverDeps {
  /** Trusted daemon state, including in-process lifecycle holds and durable reservations. */
  currentBinding:(nodeId:string)=>Promise<NativeDutyManagedBinding|null>;
  tmux:{getPanePid(pane:string):Promise<number|null>};
  listProcesses?:()=>Promise<NativeProcessRow[]>;
  piProve?:(session:string)=>Promise<PiNativeProof|null>;
  /** Kernel observation seam, never a client-supplied assertion. Default emits only a boolean. */
  verifyProcessIdentity?:(pid:number,expected:PreparedNativeDutyLaunch["publicEnvironment"],argv?:string[])=>Promise<boolean>;
  now?:()=>number;
}

// Observe argv/env inside the child process and emit ONLY one boolean. No environment
// value, raw command line or exception can cross this subprocess boundary.
const PROCESS_IDENTITY_PY = `import ctypes,json,os,struct,sys
ok=False
try:
 p=int(sys.argv[1]); expected=json.loads(sys.argv[2]); wanted=json.loads(sys.argv[3])
 if sys.platform=='darwin':
  libc=ctypes.CDLL(None); mib=(ctypes.c_int*3)(1,49,p); n=ctypes.c_size_t(0)
  if libc.sysctl(mib,3,None,ctypes.byref(n),None,0)!=0 or n.value>8388608: raise ValueError()
  b=ctypes.create_string_buffer(n.value)
  if libc.sysctl(mib,3,b,ctypes.byref(n),None,0)!=0: raise ValueError()
  raw=b.raw[:n.value]; argc=struct.unpack_from('i',raw,0)[0]
  if argc<1 or argc>65536: raise ValueError()
  at=raw.index(b'\\0',4)+1
  while at<len(raw) and raw[at]==0: at+=1
  argv=[]
  for _ in range(argc):
   end=raw.index(b'\\0',at); argv.append(raw[at:end].decode('utf-8')); at=end+1
  env=raw[at:].split(b'\\0')
  libproc=ctypes.CDLL('/usr/lib/libproc.dylib'); executable=ctypes.create_string_buffer(4096)
  if libproc.proc_pidpath(p,executable,len(executable))<=0: raise ValueError()
  actual_executable=executable.value.decode('utf-8')
 elif sys.platform.startswith('linux'):
  argv=open('/proc/'+str(p)+'/cmdline','rb').read().rstrip(b'\\0').decode('utf-8').split('\\0')
  env=open('/proc/'+str(p)+'/environ','rb').read().split(b'\\0')
  actual_executable=os.readlink('/proc/'+str(p)+'/exe')
 else: raise ValueError()
 identity={}
 for key in expected:
  prefix=(key+'=').encode(); values=[s[len(prefix):].decode('utf-8') for s in env if s.startswith(prefix)]
  if len(values)!=1: raise ValueError()
  identity[key]=values[0]
 ok=identity==expected and (wanted is None or (argv==wanted and os.path.samefile(actual_executable,wanted[0])))
except: pass
print('1' if ok else '0')`;
export async function verifyNativeDutyProcessIdentity(pid:number,expected:PreparedNativeDutyLaunch["publicEnvironment"],argv?:string[]):Promise<boolean> {
  if(!Number.isSafeInteger(pid)||pid<=1 || !ownKeys(expected,["OPENRIG_NODE_ID","OPENRIG_SESSION_NAME","OPENRIG_OCCUPANT_GENERATION","OPENRIG_RUNTIME"]) || Object.values(expected).some(value=>typeof value!=="string"))return false;
  try {const result=await promisify(execFile)("python3",["-c",PROCESS_IDENTITY_PY,String(pid),JSON.stringify(expected),JSON.stringify(argv??null)],{timeout:2000,maxBuffer:128,encoding:"utf8"});return result.stdout.trim()==="1";}catch{return false;}
}
function ancestry(rows:NativeProcessRow[],child:number,ancestor:number):NativeProcessRow[]|null {
  const byPid=new Map(rows.map(row=>[row.pid,row]));if(byPid.size!==rows.length)return null;
  const chain:NativeProcessRow[]=[],seen=new Set<number>();let row=byPid.get(child);
  while(row&&!seen.has(row.pid)&&row.startedAt){seen.add(row.pid);chain.push(row);if(row.pid===ancestor)return chain;row=byPid.get(row.ppid);}return null;
}

/** Two fresh independent OS samples. A PID claim or launch nonce alone proves nothing. */
export async function observeNativeDutyLaunch(store:NativeDutyLaunchStore,input:{scope:NativeDutyScope;launchId:string;supervisorPid:number},deps:NativeDutyLaunchObserverDeps):Promise<NativeDutyProof|null> {
  try {
    const {scope,launchId,supervisorPid}=input;if(!Number.isSafeInteger(supervisorPid)||supervisorPid<=1)return null;
    const read=store.read(scope.scopeId,launchId);if(!read)return null;
    const {intent,config}=read;
    if(!["nodeId","sessionName","generation","runtime","configurationDigest"].every(k=>(intent as unknown as Record<string,unknown>)[k]===(scope as unknown as Record<string,unknown>)[k]))return null;
    const binding=await deps.currentBinding(scope.nodeId);
    if(!binding || binding.lifecycleReserved || !binding.pane || !["nodeId","sessionName","generation","runtime","configurationDigest"].every(k=>(binding as unknown as Record<string,unknown>)[k]===(scope as unknown as Record<string,unknown>)[k]))return null;
    const census=deps.listProcesses??listNativeProcesses,verify=deps.verifyProcessIdentity??verifyNativeDutyProcessIdentity;
    const sample=async():Promise<string|null>=>{
      if(!equal(await deps.currentBinding(scope.nodeId),binding)||!equal(store.read(scope.scopeId,launchId),read))return null;
      const panePid=await deps.tmux.getPanePid(binding.pane);if(!panePid)return null;
      const rows=await census();const supervisor=rows.find(row=>row.pid===supervisorPid);
      if(!supervisor || supervisor.executableName!==path.basename(intent.installedNode.path) || !ancestry(rows,supervisorPid,panePid))return null;
      if(!await verify(supervisorPid,publicIdentity(intent),[intent.installedNode.path,intent.installedSupervisor.path,"--supervise",intent.configPath]))return null;
      let native:NativeProcessRow|undefined;
      if(scope.runtime==="codex"){
        const observed=await observeCodexPaneProcess({target:binding.pane,tmux:{getPanePid:async()=>panePid},listProcesses:async()=>rows,expectedToken:binding.resumeToken});
        native=observed?.process;
      }else{
        const pi=await deps.piProve?.(binding.sessionName);if(pi?.state!=="present"||pi.generation!==binding.generation)return null;
        const fingerprint=JSON.parse(pi.fingerprint) as {pi?:number[];runner?:number[]};
        native=rows.find(row=>row.pid===fingerprint.pi?.[0]);
        if(!native||!fingerprint.runner?.[0]||!ancestry(rows,native.pid,fingerprint.runner[0]))return null;
      }
      if(!native||native.pid===supervisorPid)return null;
      const chain=ancestry(rows,native.pid,panePid),underSupervisor=ancestry(rows,native.pid,supervisorPid);
      if(!chain||!underSupervisor||underSupervisor.length<2)return null;
      const harness=underSupervisor[underSupervisor.length-2]!;
      if(!await verify(harness.pid,publicIdentity(intent),[config.harness.executable,...config.harness.args]) || !await verify(native.pid,publicIdentity(intent)))return null;
      if(await deps.tmux.getPanePid(binding.pane)!==panePid || !equal(await deps.currentBinding(scope.nodeId),binding) || !equal(store.read(scope.scopeId,launchId),read))return null;
      return hash(canonical({pane:binding.pane,panePid,configSha256:intent.configSha256,configurationDigest:scope.configurationDigest,
        chain:chain.map(row=>[row.pid,row.ppid,row.startedAt,row.pgid,row.tpgid,row.executableName]),supervisorPid,launchId}));
    };
    const first=await sample();if(!first)return null;const second=await sample();if(!second||second!==first)return null;
    const observedAt=(deps.now??Date.now)();if(!Number.isSafeInteger(observedAt)||observedAt<0)return null;
    return {nodeId:scope.nodeId,sessionName:scope.sessionName,generation:scope.generation,runtime:scope.runtime,launchId,supervisorPid,
      configurationDigest:scope.configurationDigest,fingerprint:second,observedAt,nativePresent:true,supervisorIsNativeAncestor:true,lifecycleReserved:false};
  }catch{return null;}
}
