import { beforeEach, afterEach, it, expect, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { NativeDutyLaunchStore, observeNativeDutyLaunch, type PreparedNativeDutyLaunch, type NativeDutyManagedBinding, type NativeDutyLaunchObserverDeps } from "../src/domain/native-duty-launch.js";
import type { NativeDutyScope } from "../src/domain/native-duty-contract.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";
let dir:string,node:string,entry:string,root:string,store:NativeDutyLaunchStore,prepared:PreparedNativeDutyLaunch,scope:NativeDutyScope,binding:NativeDutyManagedBinding;
let rows:NativeProcessRow[],argv:Map<number,string[]>,env:Map<number,Record<string,string>>,deps:NativeDutyLaunchObserverDeps;
const meta={scopeId:"approved-later",launchId:"launch-1",nodeId:"node-1",sessionName:"peer@rig",generation:"reserved-g2",runtime:"codex" as const,configurationDigest:"a".repeat(64)};
function makeStore(){return new NativeDutyLaunchStore({root,nodeExecutable:node,supervisorEntry:entry,now:()=>1000});}
function row(pid:number,ppid:number,executableName:string,command:string):NativeProcessRow{return {pid,ppid,executableName,command,pgid:20,tpgid:20,startedAt:"Wed Oct 7 12:00:00 2026"};}
beforeEach(()=>{
 dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),"native-duty-launch-")));root=path.join(dir,"intents");node=path.join(dir,"node");entry=path.join(dir,"native-duty-supervisor.js");
 vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN","fixture-secret-never-write");
 fs.writeFileSync(node,"fixture installed Node",{mode:0o700});fs.writeFileSync(entry,"fixture installed supervisor",{mode:0o600});store=makeStore();
 prepared=store.prepare({...meta,harness:{executable:node,args:[path.join(dir,"codex.js"),"-c",'model_reasoning_effort="high"',"-C",path.join(dir,"cwd with spaces")],cwd:dir},pollMs:1000});
 scope={...meta,rigId:"rig",validUntil:90000,maxLeaseMs:30000,kind:"holder-continuation"};
 binding={nodeId:meta.nodeId,sessionName:meta.sessionName,generation:meta.generation,runtime:"codex",configurationDigest:meta.configurationDigest,pane:"%1",resumeToken:"fresh-native-thread",lifecycleReserved:false};
 rows=[row(10,1,"zsh","/bin/zsh"),row(20,10,"node",`${node} ${entry} --supervise ${prepared.intent.configPath}`),row(30,20,"node",`${node} codex.js`),row(40,30,"codex","/native/codex -m gpt-6-luna"),row(50,20,"node",`${node} ${entry} --helper ${prepared.intent.configPath} 20`)];
 argv=new Map([[20,[node,entry,"--supervise",prepared.intent.configPath]],[30,[prepared.config.harness.executable,...prepared.config.harness.args]],[50,[node,entry,"--helper",prepared.intent.configPath,"20"]]]);
 env=new Map(rows.map(r=>[r.pid,{...prepared.publicEnvironment}]));
 deps={currentBinding:vi.fn(async()=>({...binding})),tmux:{getPanePid:vi.fn(async()=>10)},listProcesses:vi.fn(async()=>structuredClone(rows)),now:()=>2000,
  verifyProcessIdentity:vi.fn(async(pid,expected,wanted)=>JSON.stringify(env.get(pid))===JSON.stringify(expected)&&(!wanted||JSON.stringify(argv.get(pid))===JSON.stringify(wanted)))};
});
afterEach(()=>{fs.rmSync(dir,{recursive:true,force:true});vi.unstubAllEnvs();});
const observe=()=>observeNativeDutyLaunch(store,{scope,launchId:meta.launchId,supervisorPid:20},deps);
it("creates an immutable private opt-in descriptor with unchanged harness argv/cwd and inherited stdio/auth, without an effect grant",()=>{
 expect(prepared.launch).toEqual({executable:node,args:[entry,"--supervise",prepared.intent.configPath],cwd:dir,stdio:"inherit",inheritEnvironment:true});
 expect(prepared.config.harness.args).toEqual([path.join(dir,"codex.js"),"-c",'model_reasoning_effort="high"',"-C",path.join(dir,"cwd with spaces")]);
 expect(prepared.publicEnvironment).toEqual({OPENRIG_NODE_ID:meta.nodeId,OPENRIG_SESSION_NAME:meta.sessionName,OPENRIG_OCCUPANT_GENERATION:meta.generation,OPENRIG_RUNTIME:"codex"});
 expect(fs.statSync(root).mode&0o777).toBe(0o700);expect(fs.statSync(prepared.intentPath).mode&0o777).toBe(0o600);expect(fs.statSync(prepared.intent.configPath).mode&0o777).toBe(0o600);expect(fs.statSync(prepared.config.journalDir).mode&0o777).toBe(0o700);
 expect(fs.readFileSync(prepared.intent.configPath,"utf8")).not.toContain("fixture-secret");expect(store.read(meta.scopeId,meta.launchId)?.intent).toEqual(prepared.intent);expect(store.latest(meta.nodeId,meta.generation)?.intent).toEqual(prepared.intent);
 expect(store.read("different-scope",meta.launchId)).toBeNull();expect(store.latest(meta.nodeId,"old-g1")).toBeNull();
 expect(()=>store.prepare({...meta,harness:prepared.config.harness,pollMs:1000})).toThrow();
});
it("proves actual installed supervisor ancestry, current identity and fresh Codex launch across two independent censuses",async()=>{
 const proof=await observe();expect(proof).toMatchObject({nodeId:meta.nodeId,generation:meta.generation,launchId:meta.launchId,supervisorPid:20,configurationDigest:meta.configurationDigest,observedAt:2000,nativePresent:true,supervisorIsNativeAncestor:true,lifecycleReserved:false});expect(proof?.fingerprint).toMatch(/^[a-f0-9]{64}$/);expect(deps.listProcesses).toHaveBeenCalledTimes(2);
 expect(deps.verifyProcessIdentity).toHaveBeenCalledWith(20,prepared.publicEnvironment,[node,entry,"--supervise",prepared.intent.configPath]);expect(deps.verifyProcessIdentity).toHaveBeenCalledWith(30,prepared.publicEnvironment,[node,...prepared.config.harness.args]);expect(deps.verifyProcessIdentity).toHaveBeenCalledWith(40,prepared.publicEnvironment);
});
it.each(["wrong-supervisor-argv","wrong-harness-argv","wrong-native-env","not-an-ancestor","duplicate-native","old-generation","config-drift","reservation","pid-reuse"])("refuses %s instead of trusting launch ID or reported PID",async kind=>{
 if(kind==="wrong-supervisor-argv")argv.set(20,[node,path.join(dir,"forged-supervisor.js"),"--supervise",prepared.intent.configPath]);
 if(kind==="wrong-harness-argv")argv.set(30,[node,"different-codex.js"]);
 if(kind==="wrong-native-env")env.set(40,{...prepared.publicEnvironment,OPENRIG_OCCUPANT_GENERATION:"old-g1"});
 if(kind==="not-an-ancestor")rows.find(r=>r.pid===30)!.ppid=10;
 if(kind==="duplicate-native")rows.push(row(41,30,"codex","/native/codex -m gpt-6-luna"));
 if(kind==="old-generation")binding.generation="old-g1";
 if(kind==="config-drift")binding.configurationDigest="b".repeat(64);
 if(kind==="reservation")binding.lifecycleReserved=true;
 if(kind==="pid-reuse"){let count=0;deps.listProcesses=async()=>{const copy=structuredClone(rows);if(++count===2)copy.find(r=>r.pid===20)!.startedAt="Wed Oct 7 12:00:01 2026";return copy;};}
 expect(await observe()).toBeNull();
});
it("keeps the parent intent observational while a reserved successor generation is not yet current",async()=>{
 binding.generation="old-g1";expect(await observe()).toBeNull();expect(store.read(meta.scopeId,meta.launchId)).not.toBeNull();binding.generation=meta.generation;expect(await observe()).not.toBeNull();
 // Grant timing is absent from the launch API. The service, not this proof, owns finite effect authorization.
});
it.each(["config-bytes","config-mode","config-symlink","install-bytes","partial-intent"])("rejects changed/private-path evidence: %s",async kind=>{
 if(kind==="config-bytes")fs.appendFileSync(prepared.intent.configPath," ");
 if(kind==="config-mode")fs.chmodSync(prepared.intent.configPath,0o644);
 if(kind==="config-symlink"){const copy=path.join(dir,"config-copy");fs.renameSync(prepared.intent.configPath,copy);fs.symlinkSync(copy,prepared.intent.configPath);}
 if(kind==="install-bytes")fs.appendFileSync(entry," altered");
 if(kind==="partial-intent")fs.mkdirSync(path.join(root,"partial"),{mode:0o700});
 if(kind==="partial-intent")expect(store.latest(meta.nodeId,meta.generation)).toBeNull();else{expect(store.read(meta.scopeId,meta.launchId)).toBeNull();expect(await observe()).toBeNull();}
});
it("rejects a symlinked or public launch root",()=>{
 const link=path.join(dir,"root-link");fs.symlinkSync(root,link);expect(()=>new NativeDutyLaunchStore({root:link,nodeExecutable:node,supervisorEntry:entry})).toThrow("private-owned");fs.chmodSync(root,0o755);expect(()=>makeStore()).toThrow("private-owned");
});
it("accepts Pi only with the independent native runner/child proof on the supervised chain",async()=>{
 const second=store.prepare({...meta,scopeId:"pi-scope",launchId:"pi-launch",runtime:"pi",harness:{executable:node,args:[path.join(dir,"pi-runner.js"),"--session-name",meta.sessionName],cwd:dir},pollMs:1000});
 binding.runtime="pi";scope={...scope,scopeId:"pi-scope",runtime:"pi"};rows.find(r=>r.pid===40)!.executableName="pi";rows.find(r=>r.pid===40)!.command="pi";
 argv.set(20,[node,entry,"--supervise",second.intent.configPath]);argv.set(30,[node,...second.config.harness.args]);for(const pid of env.keys())env.set(pid,{...second.publicEnvironment});
 deps.piProve=vi.fn(async()=>({state:"present",generation:meta.generation,launchId:"real-pi-launch",fingerprint:JSON.stringify({runner:[30,20],pi:[40,30]})}));
 const request={scope,launchId:"pi-launch",supervisorPid:20};expect(await observeNativeDutyLaunch(store,request,deps)).toMatchObject({runtime:"pi",nativePresent:true});
 deps.piProve=async()=>({state:"present",generation:"old-g1",launchId:"real-pi-launch",fingerprint:JSON.stringify({runner:[30,20],pi:[40,30]})});expect(await observeNativeDutyLaunch(store,request,deps)).toBeNull();
});
