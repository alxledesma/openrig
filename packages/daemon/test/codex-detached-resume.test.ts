import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, realpathSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { CodexSameGenerationRehost, createOperatorMaintenanceAuthority, type CodexRehostOptions } from "../src/domain/codex-rehost.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";

const lead="lead@xv",generation="lead-g1",nativeId="native-thread-exact",now=Date.UTC(2026,9,8,12);
const input={nodeId:lead,sessionName:lead,reason:"Resume the same detached native thread under its retained identity",operator:"operator-agent@kernel"};
const custodyTables=["nodes","occupant_tenures","queue_items","coordinator_authority","coordinator_assignments","coordinator_stage_assignments","coordinator_resources","outbox_entries"];

describe("detached Codex runner resume",()=>{
 let db:Database.Database,dir:string,file:string,guard:SeatDeliveryGuard,base:CodexRehostOptions;
 let processes:NativeProcessRow[],incarnation:number,panePid:number|null,paneOrdinal:number,resume:ReturnType<typeof vi.fn>,create:ReturnType<typeof vi.fn>,terminalAbsent:ReturnType<typeof vi.fn>,identityAbsent:ReturnType<typeof vi.fn>,supervisor:ReturnType<typeof vi.fn>;
 const tree=(n:number):NativeProcessRow[]=>[
  {pid:10,ppid:1,command:"/bin/zsh",executableName:"zsh",startedAt:"root",pgid:10,tpgid:n},
  {pid:n,ppid:10,command:"/usr/bin/node codex-wrapper.js",executableName:"node",startedAt:"wrapper-"+n,pgid:n,tpgid:n},
  {pid:n+1,ppid:n,command:`/usr/bin/codex --no-daemon -p exact -m gpt-6-luna resume ${nativeId}`,executableName:"codex",startedAt:"native-"+n,pgid:n,tpgid:n},
 ];
 const resumedTree=(n:number):NativeProcessRow[]=>{
  const root=panePid??30;
  return [
  {pid:root,ppid:1,command:"/bin/zsh",executableName:"zsh",startedAt:"new-pane-shell-"+root,pgid:root,tpgid:n},
  {pid:n,ppid:root,command:"/usr/bin/node codex-wrapper.js",executableName:"node",startedAt:"wrapper-"+n,pgid:n,tpgid:n},
  {pid:n+1,ppid:n,command:`/usr/bin/codex --no-daemon -p exact -m gpt-6-luna resume ${nativeId}`,executableName:"codex",startedAt:"native-"+n,pgid:n,tpgid:n},
  ];
 };
 const barePane=(pid:number)=>{panePid=pid;processes=[tree(10)[0]!,{pid,ppid:1,command:"/bin/zsh",executableName:"zsh",startedAt:"new-shell-"+pid,pgid:pid,tpgid:pid}];};
 const evidenceFiles=()=>{try{return readdirSync(base.snapshotRoot,{recursive:true}).map(String);}catch{return [] as string[];}};
 const makeService=(overrides:Record<string,unknown>={})=>new CodexSameGenerationRehost({...base,...overrides} as unknown as CodexRehostOptions);
 const resumeDetached=(service:CodexSameGenerationRehost,request:Record<string,unknown>={})=>(service as unknown as {resumeDetached(v:unknown):Promise<any>}).resumeDetached({...input,actorGeneration:"operator-agent-g1",...request});
 const custody=()=>JSON.stringify(Object.fromEntries(custodyTables.map(t=>[t,db.prepare("SELECT * FROM "+t).all()])));
 beforeEach(async()=>{
  dir=realpathSync(mkdtempSync(path.join(tmpdir(),"codex-detached-resume-")));file=path.join(dir,"native.jsonl");
  writeFileSync(file,Buffer.from(JSON.stringify({type:"session_meta",payload:{id:nativeId}})+"\n"+JSON.stringify({type:"turn_context",payload:{model:"gpt-6-luna"}})+"\n"),{mode:0o600});
  db=createDb();seed(db);new EventBus(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,lead);
  db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token=? WHERE node_id=?").run(nativeId,lead);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding',?,?,'%1')").run(lead,lead);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed' WHERE node_id='operator-agent@kernel'").run();
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('operator-binding','operator-agent@kernel','operator-agent@kernel','%9')").run();
  guard=new SeatDeliveryGuard(db,name=>resolveGuardTarget(db,name));await guard.set(lead,true,"operator","detached resume fixture");
  processes=[tree(20)[0]!];incarnation=20;panePid=null;paneOrdinal=1;
  resume=vi.fn(async()=>{incarnation+=20;processes=resumedTree(incarnation);return {ok:true as const};});
  terminalAbsent=vi.fn(async()=>true);identityAbsent=vi.fn(async()=>true);
  create=vi.fn(async()=>{const pane=`%${++paneOrdinal+1}`;barePane(paneOrdinal===2?30:50);return {pane};});
  supervisor=vi.fn(async()=>({launchId:"supervised-"+incarnation,fingerprint:"independent-os-proof"}));
  base={db,guard,tmux:{getPanePid:async(pane:string)=>pane==="%1"?null:panePid},snapshotRoot:path.join(dir,"private-resume"),resume:{resume},
   nativeState:async()=>({nodeId:lead,sessionName:lead,nativeId,transcriptPath:file,runtimeContract:{runtime:"codex",model:"gpt-6-luna",provider:"openai",profile:"exact",effort:"high",permissions:{sandbox:{type:"workspace-write"},approval:"never"}}}),
   activityWitness:async()=>({seatNodeId:lead,sessionName:lead,rung:"window-sampling",sourceId:"fresh-detached-observation",seq:1,observedAt:new Date(now).toISOString(),activity:"idle-at-prompt"}),
   preflightSupervisedLaunch:async()=>({posture:"floor",effective:{model:"gpt-6-luna",provider:"openai",effort:"high",approval:"never",sandbox:"workspace-write"},evidenceDigest:"a".repeat(64)}),
   observeSupervisedReplacement:supervisor,
   listProcesses:async()=>processes,verifyProcessIdentity:async(_pid,identity)=>identity.OPENRIG_NODE_ID===lead&&identity.OPENRIG_SESSION_NAME===lead&&identity.OPENRIG_OCCUPANT_GENERATION===generation&&identity.OPENRIG_RUNTIME==="codex",
   now:()=>now,sleep:async()=>{},waitMs:1,pollMs:1,
   stoppedNativeState:async(binding:any)=>({nodeId:binding.nodeId,sessionName:binding.sessionName,nativeId,transcriptPath:file,runtimeContract:{runtime:"codex",model:"gpt-6-luna",provider:"openai",profile:"exact",effort:"high",permissions:{sandbox:{type:"workspace-write"},approval:"never"}}}),
   detachedTerminalAbsent:terminalAbsent,proveDetachedIdentityAbsent:identityAbsent,createDetachedTerminal:create,
   proveStoppedIdentityAbsent:async()=>true
  } as unknown as CodexRehostOptions;
 });
 afterEach(()=>{db?.close();rmSync(dir,{recursive:true,force:true});});

 it("resumes the exact detached thread, preserving identity, custody and history under the guard",async()=>{
  expect(guard.preference(lead)).toMatchObject({desired:true,effective:true});
  const originalTranscript=readFileSync(file),sessionBefore=db.prepare("SELECT id,node_id,status,startup_status,resume_type,resume_token FROM sessions WHERE node_id=?").get(lead) as any;
  const bindingBefore=db.prepare("SELECT * FROM bindings WHERE node_id=?").get(lead) as any;
  new OutboxHandler(db).record({outboxId:"unknown-detached",senderSession:lead,destinationSession:"peer@xv",body:"Preserve this UNKNOWN effect"});db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate'").run();
  const withUnknown=custody(),createDetached=vi.fn(async(_binding:any)=>{expect(guard.ownsRunnerRehost(lead)).toBe(true);expect(evidenceFiles().some(p=>p.endsWith("began.json"))).toBe(true);barePane(30);return {pane:"%2"};});
  const preflight=vi.fn(async(_binding:any,_native:any,_detached?:true)=>({posture:"floor" as const,effective:{model:"gpt-6-luna",provider:"openai",effort:"high",approval:"never",sandbox:"workspace-write"},evidenceDigest:"a".repeat(64)}));
  const svc=makeService({createDetachedTerminal:createDetached,preflightSupervisedLaunch:preflight});
  const result=await resumeDetached(svc);
  expect(result,JSON.stringify(result)).toMatchObject({ok:true,runtime:"codex",nodeId:lead,sessionName:lead,generation,generationUnchanged:true,custodyPreserved:true,guardLeftEnabled:true,authorityRepaired:false});
  expect(custody()).toBe(withUnknown);expect(db.prepare("SELECT id,node_id,status,startup_status,resume_type,resume_token FROM sessions WHERE node_id=?").get(lead)).toMatchObject({id:sessionBefore.id,node_id:lead,status:"running",startup_status:"ready",resume_type:"codex_id",resume_token:nativeId});
  expect(db.prepare("SELECT * FROM bindings WHERE node_id=?").get(lead)).toMatchObject({id:bindingBefore.id,node_id:lead,tmux_session:lead,tmux_pane:"%2",tmux_window:"0"});
  expect(resume).toHaveBeenCalledTimes(1);expect(createDetached).toHaveBeenCalledTimes(1);expect(preflight).toHaveBeenCalledWith(expect.anything(),expect.anything(),true);expect(supervisor).toHaveBeenCalledTimes(1);expect(terminalAbsent).toHaveBeenCalled();expect(identityAbsent).toHaveBeenCalled();
  expect(readFileSync(result.backup.path)).toEqual(originalTranscript);
  expect(readFileSync(file)).toEqual(originalTranscript);
  expect(guard.preference(lead)).toMatchObject({desired:true,effective:true});expect(guard.ownsRunnerRehost(lead)).toBe(false);
 });

 it.each(["living-process","present-canonical-terminal","active-reservation","sending-effect"] as const)("refuses %s before pane creation or resume",async kind=>{
  if(kind==="living-process")identityAbsent.mockResolvedValue(false);
  if(kind==="present-canonical-terminal")terminalAbsent.mockResolvedValue(false);
  if(kind==="active-reservation")db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r','op',?,?,'lead-g1','native','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved','now','now')").run(lead,lead);
  if(kind==="sending-effect"){new OutboxHandler(db).record({outboxId:"sending",senderSession:lead,destinationSession:"peer@xv",body:"in flight"});db.prepare("UPDATE outbox_entries SET delivery_state='sending'").run();}
  const result=await resumeDetached(makeService());
  expect(result).toMatchObject({ok:false,effectAttempted:false});expect(create).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();expect(evidenceFiles().some(p=>p.endsWith("began.json"))).toBe(false);
 });

 it.each(["generation","native","history","profile","input"] as const)("holds %s drift before durable intent or native effects",async kind=>{
  let nativeState=base.nativeState;
  if(kind==="native")nativeState=async()=>({...await base.nativeState(lead),nativeId:"different-thread"});
  if(kind==="history")writeFileSync(file,"partial native history");
  if(kind==="profile")nativeState=async()=>({...await base.nativeState(lead),runtimeContract:{...((await base.nativeState(lead)).runtimeContract),profile:"different"}});
  const result=await resumeDetached(makeService({stoppedNativeState:nativeState}),{...(kind==="input"?{sessionName:"other@xv"}:{}),...(kind==="generation"?{actorGeneration:"operator-old"}:{})});
  expect(result).toMatchObject({ok:false,effectAttempted:false});expect(create).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();expect(evidenceFiles().some(p=>p.endsWith("began.json"))).toBe(false);
 });

 it("refuses missing post-creation absence proof before creating a terminal",async()=>{
  const result=await resumeDetached(makeService({proveStoppedIdentityAbsent:undefined}));
  expect(result).toMatchObject({ok:false,code:"codex_detached_resume_unavailable",effectAttempted:false});
  expect(create).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
 });

 it("refuses target generation drift during absence proof before durable intent",async()=>{
  identityAbsent.mockImplementation(async()=>{
   db.prepare("UPDATE occupant_tenures SET generation_uuid='replacement-generation' WHERE node_id=?").run(lead);
   return true;
  });
  const result=await resumeDetached(makeService());
  expect(result).toMatchObject({ok:false,effectAttempted:false});
  expect(create).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
  expect(evidenceFiles().some(p=>p.endsWith("began.json"))).toBe(false);
 });

 it.each(["creation-unknown","resume-unknown"] as const)("records %s once and fences every second invocation",async kind=>{
  if(kind==="creation-unknown")create.mockImplementation(async()=>{throw new Error("terminal creation outcome unknown");});
  if(kind==="resume-unknown")resume.mockImplementation(async()=>({ok:false as const,code:"resume_outcome_unknown"}));
  const svc=makeService(),first=await resumeDetached(svc);
  expect(first,JSON.stringify(first)).toMatchObject({ok:false,effectAttempted:true,blindRetryAllowed:false});expect(evidenceFiles().some(p=>p.endsWith("began.json"))).toBe(true);
  const creates=create.mock.calls.length,resumes=resume.mock.calls.length;
  expect(await resumeDetached(svc)).toMatchObject({ok:false,effectAttempted:false});
  expect(create).toHaveBeenCalledTimes(creates);expect(resume).toHaveBeenCalledTimes(resumes);
 });

 it("does not replay a completed historical attempt but permits a new attempt after a later terminal loss",async()=>{
  const svc=makeService(),first=await resumeDetached(svc);expect(first,JSON.stringify(first)).toMatchObject({ok:true});
  const firstAttempt=first.attemptId;db.prepare("UPDATE sessions SET status='detached' WHERE node_id=?").run(lead);processes=[tree(10)[0]!];panePid=null;
  const second=await resumeDetached(svc);expect(second,JSON.stringify(second)).toMatchObject({ok:true,generation,generationUnchanged:true});expect(second.attemptId).not.toBe(firstAttempt);expect(create).toHaveBeenCalledTimes(2);expect(resume).toHaveBeenCalledTimes(2);
 });

 it("requires local maintenance authority for Operator recovery and refuses self-repair",async()=>{
  const operator="operator-agent@kernel";db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,operator);
  db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token='operator-native' WHERE node_id=?").run(operator);
  const request={nodeId:operator,sessionName:operator,reason:input.reason,operator};
  const result=await resumeDetached(makeService(),{...request,actorGeneration:"operator-agent-g1"});
  expect(result).toMatchObject({ok:false,effectAttempted:false});expect(create).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
  const operatorTranscript=path.join(dir,"operator.jsonl");writeFileSync(operatorTranscript,JSON.stringify({type:"session_meta",payload:{id:"operator-native"}})+"\n",{mode:0o600});
  const operatorResume=vi.fn(async()=>{
   processes=[
    {pid:30,ppid:1,command:"/bin/zsh",executableName:"zsh",startedAt:"operator-shell",pgid:30,tpgid:41},
    {pid:40,ppid:30,command:"/usr/bin/node codex-wrapper.js",executableName:"node",startedAt:"operator-wrapper",pgid:41,tpgid:41},
    {pid:41,ppid:40,command:"/usr/bin/codex --no-daemon -p exact -m gpt-6-luna resume operator-native",executableName:"codex",startedAt:"operator-native-process",pgid:41,tpgid:41},
   ];return {ok:true as const};
  });
  const localAuthority=createOperatorMaintenanceAuthority({nodeId:operator,generation:"operator-agent-g1",detachedResume:true,enableGuard:true});
  const locallyAuthorized=makeService({resume:{resume:operatorResume},stoppedNativeState:async(binding:any)=>({nodeId:binding.nodeId,sessionName:binding.sessionName,nativeId:"operator-native",transcriptPath:operatorTranscript,runtimeContract:{runtime:"codex",model:"gpt-6-luna",provider:"openai",profile:"exact",effort:"high",permissions:{sandbox:{type:"workspace-write"},approval:"never"}}}),
   verifyProcessIdentity:async(_pid,identity)=>identity.OPENRIG_NODE_ID===operator&&identity.OPENRIG_SESSION_NAME===operator&&identity.OPENRIG_OCCUPANT_GENERATION==="operator-agent-g1"&&identity.OPENRIG_RUNTIME==="codex"});
  expect(await resumeDetached(locallyAuthorized,{...request,maintenanceAuthority:localAuthority})).toMatchObject({ok:true,nodeId:operator,generation:"operator-agent-g1",authorityRepaired:false});
  expect(operatorResume).toHaveBeenCalledTimes(1);expect(guard.preference(operator)).toMatchObject({desired:true,effective:true});
 });
 it.each(["continue","native-present","wrong-phase","custody-drift","session-drift","recovery-intent"] as const)("recovers only actual pre-native detached failure under current peer Operator: %s",async kind=>{
  const absence=vi.fn(async()=>false),svc=makeService({proveStoppedIdentityAbsent:absence});
  const first=await resumeDetached(svc);expect(first,JSON.stringify(first)).toMatchObject({ok:false,code:"codex_rehost_recovery_absence",effectAttempted:true,blindRetryAllowed:false});expect(resume).not.toHaveBeenCalled();expect(create).toHaveBeenCalledTimes(1);
  const beganPath=evidenceFiles().find(p=>p.endsWith("began.json"))!,directory=path.dirname(path.join(base.snapshotRoot,beganPath)),beganBytes=readFileSync(path.join(directory,"began.json")),unknownBytes=readFileSync(path.join(directory,"unknown.json"));
  const request={...input,actorGeneration:"operator-agent-g1",attemptId:JSON.parse(beganBytes.toString()).attemptId,beganSha256:createHash("sha256").update(beganBytes).digest("hex")};
  absence.mockResolvedValue(true);if(kind==="native-present")processes=resumedTree(40);if(kind==="wrong-phase")writeFileSync(path.join(directory,"unknown.json"),JSON.stringify({code:"codex_rehost_resume_unknown",effectAttempted:true,blindRetryAllowed:false}));
  if(kind==="custody-drift")db.prepare("UPDATE nodes SET role='changed-custody' WHERE id=?").run(lead);
  if(kind==="session-drift")db.prepare("UPDATE sessions SET origin='changed-origin' WHERE node_id=?").run(lead);
  if(kind==="recovery-intent")writeFileSync(path.join(directory,"recovery-began.json"),JSON.stringify({attemptId:request.attemptId}),{mode:0o600});
  const protectedBefore=custody(),originalHistory=readFileSync(file),unknownBefore=readFileSync(path.join(directory,"unknown.json"));const recovered=await svc.recoverStopped(request);
  if(kind==="continue"){expect(recovered,JSON.stringify(recovered)).toMatchObject({ok:true,generation,generationUnchanged:true,custodyPreserved:true,authorityRepaired:false});expect(resume).toHaveBeenCalledTimes(1);expect(await svc.recoverStopped(request)).toMatchObject({ok:false,effectAttempted:false});expect(readFileSync(path.join(directory,"unknown.json"))).toEqual(unknownBytes);}else{expect(recovered,JSON.stringify(recovered)).toMatchObject({ok:false,effectAttempted:false});expect(resume).not.toHaveBeenCalled();}
  expect(create).toHaveBeenCalledTimes(1);expect(custody()).toBe(protectedBefore);expect(readFileSync(path.join(directory,"began.json"))).toEqual(beganBytes);expect(readFileSync(path.join(directory,"unknown.json"))).toEqual(unknownBefore);expect(readFileSync(file)).toEqual(originalHistory);expect(guard.preference(lead)).toMatchObject({desired:true,effective:true});
 });
 it("continues actual local maintenance pre-native failure without new terminal or generation",async()=>{
  const operator="operator-agent@kernel";db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,operator);
  db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token='operator-native' WHERE node_id=?").run(operator);
  const request={nodeId:operator,sessionName:operator,reason:input.reason,operator};
  const result=await resumeDetached(makeService(),{...request,actorGeneration:"operator-agent-g1"});
  expect(result).toMatchObject({ok:false,effectAttempted:false});expect(create).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
  const operatorTranscript=path.join(dir,"operator.jsonl");writeFileSync(operatorTranscript,JSON.stringify({type:"session_meta",payload:{id:"operator-native"}})+"\n",{mode:0o600});
  const operatorResume=vi.fn(async()=>{
   processes=[
    {pid:30,ppid:1,command:"/bin/zsh",executableName:"zsh",startedAt:"operator-shell",pgid:30,tpgid:41},
    {pid:40,ppid:30,command:"/usr/bin/node codex-wrapper.js",executableName:"node",startedAt:"operator-wrapper",pgid:41,tpgid:41},
    {pid:41,ppid:40,command:"/usr/bin/codex --no-daemon -p exact -m gpt-6-luna resume operator-native",executableName:"codex",startedAt:"operator-native-process",pgid:41,tpgid:41},
   ];return {ok:true as const};
  });
  const localAuthority=createOperatorMaintenanceAuthority({nodeId:operator,generation:"operator-agent-g1",detachedResume:true,enableGuard:true});
  const absence=vi.fn(async()=>false);const locallyAuthorized=makeService({proveStoppedIdentityAbsent:absence,resume:{resume:operatorResume},stoppedNativeState:async(binding:any)=>({nodeId:binding.nodeId,sessionName:binding.sessionName,nativeId:"operator-native",transcriptPath:operatorTranscript,runtimeContract:{runtime:"codex",model:"gpt-6-luna",provider:"openai",profile:"exact",effort:"high",permissions:{sandbox:{type:"workspace-write"},approval:"never"}}}),
   verifyProcessIdentity:async(_pid,identity)=>identity.OPENRIG_NODE_ID===operator&&identity.OPENRIG_SESSION_NAME===operator&&identity.OPENRIG_OCCUPANT_GENERATION==="operator-agent-g1"&&identity.OPENRIG_RUNTIME==="codex"});
  const failed=await resumeDetached(locallyAuthorized,{...request,maintenanceAuthority:localAuthority});expect(failed,JSON.stringify(failed)).toMatchObject({ok:false,code:"codex_rehost_recovery_absence",effectAttempted:true});expect(operatorResume).not.toHaveBeenCalled();
  const beganPath=evidenceFiles().find(p=>p.endsWith("began.json"))!,directory=path.dirname(path.join(base.snapshotRoot,beganPath)),beganBytes=readFileSync(path.join(directory,"began.json")),unknownBytes=readFileSync(path.join(directory,"unknown.json"));const recovery={attemptId:JSON.parse(beganBytes.toString()).attemptId,beganSha256:createHash("sha256").update(beganBytes).digest("hex")};
  absence.mockResolvedValue(true);const recoveryAuthority=createOperatorMaintenanceAuthority({nodeId:operator,generation:"operator-agent-g1",recovery});const creates=create.mock.calls.length;
  const recovered=await locallyAuthorized.recoverStopped({...request,...recovery,actorGeneration:"",maintenanceAuthority:recoveryAuthority});expect(recovered,JSON.stringify(recovered)).toMatchObject({ok:true,nodeId:operator,generation:"operator-agent-g1",authorityRepaired:false});expect(create).toHaveBeenCalledTimes(creates);expect(readFileSync(path.join(directory,"began.json"))).toEqual(beganBytes);expect(readFileSync(path.join(directory,"unknown.json"))).toEqual(unknownBytes);
  expect(await locallyAuthorized.recoverStopped({...request,...recovery,actorGeneration:"",maintenanceAuthority:recoveryAuthority})).toMatchObject({ok:false,effectAttempted:false});
  expect(operatorResume).toHaveBeenCalledTimes(1);expect(guard.preference(operator)).toMatchObject({desired:true,effective:true});
 });
});
