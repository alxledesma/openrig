import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { CoordinationRecoveryService, type CoordinationActivity, type CoordinationTask } from "../src/domain/coordination-recovery-service.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { NativeDutyLaunchStore, observeNativeDutyLaunch, type PreparedNativeDutyLaunch } from "../src/domain/native-duty-launch.js";
import { NativeRecoveryCompletionStore, recoveryReceiptDigest, type NativeRecoveryProducerEvidence } from "../src/domain/native-recovery-completion.js";
import { createNativeRecoveryContinuation } from "../src/domain/native-recovery-continuation.js";
import type { NativeRecoveryObservation } from "../src/domain/native-recovery-continuation-contract.js";
import { observeCodexPaneProcess, type NativeProcessRow } from "../src/domain/native-process-lineage.js";

// Only OS census and the argv/identity subprocess boundary are mocked. The
// launch store, event store, source verifier, runtime and delivery guard are real.
// No provider/runtime executable, terminal input or process is started.
const kernel = vi.hoisted(()=>({
  rows: [] as NativeProcessRow[],
  argv: new Map<number,string[]>(),
  identities: new Map<number,Record<string,string>>(),
  beforeCensus: null as null|(()=>Promise<void>),
  census: vi.fn(),
  identity: vi.fn(),
}));
vi.mock("../src/domain/native-process-lineage.js",async original=>({
  ...await original<typeof import("../src/domain/native-process-lineage.js")>(),
  listNativeProcesses:()=>kernel.census(),
}));
vi.mock("node:child_process",async original=>{
  const actual=await original<typeof import("node:child_process")>();
  const exec=vi.fn();
  Object.defineProperty(exec,Symbol.for("nodejs.util.promisify.custom"),{value:(...args:unknown[])=>kernel.identity(...args)});
  return {...actual,execFile:exec};
});
const sha=(value:string)=>createHash("sha256").update(value).digest("hex");
const nodeId="builder@xv",sessionName=nodeId,generation="builder-g1";
let configurationDigest:string,launchSerial:number;
const runnerLaunchId="pi-native-recovered-1",supervisorLaunchId="managed-recovered-1";
let db:Database.Database,bus:EventBus,guard:SeatDeliveryGuard,store:NativeDutyLaunchStore,completionStore:NativeRecoveryCompletionStore;
let dir:string,nativeId:string,prepared:PreparedNativeDutyLaunch;
let runtime:ReturnType<typeof createNativeRecoveryContinuation>;
let enabled:boolean,launchTime:number,clockNow:number;
let evidence:NativeRecoveryProducerEvidence;
const piProof=vi.fn();
function row(pid:number,ppid:number,executableName:string):NativeProcessRow {
 return {pid,ppid,executableName,command:executableName,pgid:30,tpgid:30,startedAt:new Date(Date.UTC(2026,9,7,12,0,pid/10)).toISOString()};
}
function constructRuntime(legacyReceipts?:{ref:string;digest:string}[]){return createNativeRecoveryContinuation({legacyReceipts,db,eventBus:bus,guard,store,tmux:{getPanePid:async()=>10},piProve:piProof,
 configurationDigest:()=>configurationDigest,roots:{piDetached:path.join(dir,"pi-private"),codex:path.join(dir,"codex-private")},enabled:()=>enabled,now:()=>clockNow});}
function configure(kind:"pi"|"codex"="pi"){
 nativeId=kind==='pi'?path.join(dir,'retained-pi.jsonl'):'retained-codex-thread';
 db.prepare("UPDATE nodes SET runtime=? WHERE id=?").run(kind,nodeId);
 db.prepare("UPDATE sessions SET status='running',startup_status='ready',resume_type=?,resume_token=? WHERE node_id=?").run(kind==='pi'?'pi_session_file':'codex_id',nativeId,nodeId);
 const node=path.join(dir,'node'),entry=path.join(dir,'native-duty-supervisor.js');
 const harnessArgs=kind==='pi'?[path.join(dir,'pi-runner.js'),'--session-name',sessionName,'--session',nativeId,'--launch-id',runnerLaunchId]:[path.join(dir,'codex.js'),'resume',nativeId];
 prepared=store.prepare({scopeId:'recovery-scope-'+kind,launchId:supervisorLaunchId+"-"+kind+"-"+(++launchSerial),nodeId,sessionName,generation,runtime:kind,configurationDigest,harness:{executable:node,args:harnessArgs,cwd:dir},pollMs:1000});
 kernel.rows=[row(10,1,'zsh'),row(20,10,'node'),row(30,20,'node'),row(40,30,kind==='pi'?'pi':'codex')];
 kernel.rows[3]!.command=kind==='pi'?'pi':`/native/codex -m gpt-6-luna resume ${nativeId}`;
 kernel.argv=new Map([[20,[node,entry,'--supervise',prepared.intent.configPath]],[30,[node,...harnessArgs]]]);
 kernel.identities=new Map(kernel.rows.filter(r=>r.pid!==10).map(r=>[r.pid,{...prepared.publicEnvironment}]));
 piProof.mockResolvedValue({state:'present',generation,launchId:runnerLaunchId,fingerprint:JSON.stringify({pane:'%1',runner:[30,20],pi:[40,30],launchId:runnerLaunchId})});
}
function eventProof(){
 const recoveryId=`pi-runner-rehost:${nodeId}:${generation}:${runnerLaunchId}`;
 bus.emit({type:'seat.native_recovery_publication_began',rigId:'xv',nodeId,generation,publicationId:recoveryId,blindRetryAllowed:false} as never);
 const completed=bus.emit({type:'seat.runner_rehost_completed',rigId:'xv',nodeId,generation,generationUnchanged:true,sessionFile:nativeId,sessionFileUnchanged:true,launchIdBefore:'pi-native-old',launchIdAfter:runnerLaunchId,durableModel:(db.prepare('SELECT model FROM nodes WHERE id=?').get(nodeId) as {model:string|null}).model,guardLeftEnabled:true,stoppedTargetRecovery:false,continuityCredit:false,deliveryOrQualificationCredit:false} as never);
 const payload=(db.prepare('SELECT payload FROM events WHERE seq=?').get(completed.seq) as {payload:string}).payload;
 evidence={producer:'pi-runner-rehost',recoveryId,rigId:'xv',nodeId,sessionId:nodeId,sessionName,generation,runtime:'pi',nativeIdentityHash:sha(nativeId),runtimeLaunchId:runnerLaunchId,source:{ref:`event:${completed.seq}`,digest:sha(payload)}};
}
function completePublication(){
 if(evidence.producer==='pi-runner-rehost')bus.emit({type:'seat.native_recovery_publication_completed',rigId:'xv',nodeId,generation,publicationId:evidence.recoveryId} as never);
 else fs.writeFileSync(path.join(path.dirname(evidence.source.ref),'completion-publication-completed.json'),JSON.stringify({attemptId:evidence.recoveryId,at:2001}),{mode:0o600});
}
async function publishOrdinary(complete=true){
 eventProof();await guard.runnerRehost(nodeId,()=>runtime.recordNativeRecoveryCompletion(evidence));
 if(complete)completePublication();
 await guard.set(nodeId,false,'test-fixture','allow bounded test operation');
}
async function observation():Promise<NativeRecoveryObservation>{
 const observed=await runtime.observeRecoveredIncarnation(sessionName);expect(observed).not.toBeNull();return observed!;
}
async function privateProof(kind:'pi'|'codex'){
 if(kind==='codex'){launchTime=2000;configure('codex');}
 const recoveryId='11111111-1111-4111-8111-111111111111';
 const root=path.join(dir,kind==='pi'?'pi-private':'codex-private');
 const nodeRoot=path.join(root,sha(JSON.stringify([nodeId,generation])));
 const directory=path.join(nodeRoot,kind==='pi'?recoveryId:'f'.repeat(64)+'-'+recoveryId);
 fs.mkdirSync(directory,{recursive:true,mode:0o700});
 const source=path.join(directory,'completed.json');
 // Receipt fields exactly follow the real producer, with distinct whole native
 // proof fingerprint and kernel-start fingerprint. Never make them equal.
 const proof=await observeNativeDutyLaunch(store,{scope:prepared.intent,launchId:prepared.intent.launchId,supervisorPid:20},{currentBinding:async()=>({nodeId,sessionName,generation,runtime:kind,configurationDigest,pane:'%1',resumeToken:nativeId,lifecycleReserved:false}),tmux:{getPanePid:async()=>10},piProve:piProof});
 expect(proof).not.toBeNull();
 const nativeProcess=kind==='codex'?await observeCodexPaneProcess({target:'%1',tmux:{getPanePid:async()=>10},listProcesses:async()=>structuredClone(kernel.rows),expectedToken:nativeId,requireResume:true}):null;
 if(kind==='codex')expect(nativeProcess).not.toBeNull();
 const nativeFingerprint=kind==='pi'?(await piProof()).fingerprint:sha(nativeProcess!.fingerprint);
 const receipt={ok:true,runtime:kind,nodeId,sessionId:nodeId,sessionName,generation,generationUnchanged:true,nativeIdHash:sha(nativeId),attemptId:recoveryId,receiptPath:path.join(directory,'began.json'),supervisorLaunchId:prepared.intent.launchId,nativeFingerprint,...(kind==='codex'?{nativeFingerprintAfter:nativeFingerprint}:{}),custodyPreserved:true,guardLeftEnabled:true,authorityRepaired:false};
 fs.writeFileSync(source,JSON.stringify(receipt)+'\n',{mode:0o600});
 fs.writeFileSync(path.join(directory,'began.json'),JSON.stringify({protocol:kind==='pi'?'pi-detached-resume-v1':'codex-same-generation-rehost-v1',attemptId:recoveryId,nativeIdHash:sha(nativeId)}),{mode:0o600});
 fs.writeFileSync(path.join(directory,'completion-publication-began.json'),JSON.stringify({attemptId:recoveryId,blindRetryAllowed:false}),{mode:0o600});
 evidence={producer:kind==='pi'?'pi-detached-resume':'codex-rehost',recoveryId,rigId:'xv',nodeId,sessionId:nodeId,sessionName,generation,runtime:kind,nativeIdentityHash:sha(nativeId),supervisorLaunchId:prepared.intent.launchId,nativeFingerprint,source:{ref:source,digest:recoveryReceiptDigest(fs.readFileSync(source))}};
}
async function legacyReceipt(){
 await privateProof('pi');
 // Construct an OLD-protocol fixture in a private disposable directory. This
 // removes only the marker created by our fixture; no real receipt is edited.
 fs.unlinkSync(path.join(path.dirname(evidence.source.ref),'completion-publication-began.json'));
 const receipt=JSON.parse(fs.readFileSync(evidence.source.ref,'utf8'));receipt.at=clockNow-1000;
 fs.writeFileSync(evidence.source.ref,JSON.stringify(receipt)+'\n',{mode:0o600});
 return {ref:evidence.source.ref,digest:recoveryReceiptDigest(fs.readFileSync(evidence.source.ref))};
}
beforeEach(async()=>{
 vi.clearAllMocks();kernel.beforeCensus=null;
 kernel.census.mockImplementation(async()=>{await kernel.beforeCensus?.();return structuredClone(kernel.rows);});
 kernel.identity.mockImplementation(async(command:string,args:string[])=>{
  if(command!=='python3'||args[0]!=='-c'||args.length!==5)throw Error('Unexpected OS subprocess request');
  const pid=Number(args[2]),expected=JSON.parse(args[3]!),wanted=JSON.parse(args[4]!);
  const ok=JSON.stringify(kernel.identities.get(pid))===JSON.stringify(expected)&&(wanted===null||JSON.stringify(kernel.argv.get(pid))===JSON.stringify(wanted));
  return {stdout:ok?'1\n':'0\n',stderr:''};
 });
 dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'native-recovery-runtime-')));
 fs.writeFileSync(path.join(dir,'node'),'installed fixture Node',{mode:0o700});fs.writeFileSync(path.join(dir,'native-duty-supervisor.js'),'installed fixture supervisor',{mode:0o600});
 db=createDb();seed(db);bus=new EventBus(db);completionStore=new NativeRecoveryCompletionStore(bus);
 db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host','now','now')").run();
 db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('runtime-binding',?,?,'%1')").run(nodeId,sessionName);
 guard=new SeatDeliveryGuard(db,name=>resolveGuardTarget(db,name));await guard.set(nodeId,true,'test-fixture','recovery proof requires guard');
 store=new NativeDutyLaunchStore({root:path.join(dir,'launches'),nodeExecutable:path.join(dir,'node'),supervisorEntry:path.join(dir,'native-duty-supervisor.js'),now:()=>launchTime});
 enabled=true;launchTime=1000;launchSerial=0;configurationDigest="a".repeat(64);clockNow=Date.UTC(2026,9,7,12,5);configure();runtime=constructRuntime();
});
afterEach(()=>{db?.close();fs.rmSync(dir,{recursive:true,force:true});});

describe('native recovery continuation runtime with real storage and guard',()=>{
 it('records producer proof, requires completed publication, observes exact incarnation and performs one guarded send',async()=>{
  await publishOrdinary(false);expect(completionStore.latest(nodeId,generation)).not.toBeNull();expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();
  completePublication();const current=await observation();expect(current.completion.incarnation).toMatchObject({runtimeLaunchId:runnerLaunchId,supervisorLaunchId:prepared.intent.launchId,native:{pid:40},supervisor:{pid:20}});expect(current.completion.incarnation.native.startFingerprint).toMatch(/^[a-f0-9]{64}$/);
  const send=vi.fn(async()=> 'sent');expect(await runtime.withRecoveredIncarnation(current,send)).toEqual({state:'performed',value:'sent'});expect(send).toHaveBeenCalledTimes(1);
 });
 it.each(['incomplete','unknown'])('%s publication grants no observation or send',async kind=>{
  await publishOrdinary(false);const saved=completionStore.latest(nodeId,generation)!;
  if(kind==='unknown'){const seq=Number(evidence.source.ref.slice(6));bus.emit({type:'seat.native_recovery_publication_unknown',rigId:'xv',nodeId,generation,completionEventSeq:seq,blindRetryAllowed:false} as never);completePublication();}
  expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();const send=vi.fn();expect(await runtime.withRecoveredIncarnation({completion:saved,observedAt:2000},send)).toMatchObject({state:'held',reason:'recovery-publication-unproved'});expect(send).not.toHaveBeenCalled();
 });
 it('guard holds an existing observation without a send and permits it after explicit clear',async()=>{
  await publishOrdinary();const current=await observation();await guard.set(nodeId,true,'test-fixture','hold');const send=vi.fn(async()=> 'sent');
  expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();expect(await runtime.withRecoveredIncarnation(current,send)).toEqual({state:'held',reason:'typing_guard_enabled'});expect(send).not.toHaveBeenCalled();
  await guard.set(nodeId,false,'test-fixture','clear');expect(await runtime.withRecoveredIncarnation(current,send)).toMatchObject({state:'performed'});expect(send).toHaveBeenCalledTimes(1);
 });
 it.each([40,20])('PID %s reused with new start invalidates the recorded incarnation',async pid=>{
  await publishOrdinary();const current=await observation();kernel.rows.find(r=>r.pid===pid)!.startedAt='Wed Oct 7 12:10:00 2026';
  expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();const send=vi.fn();expect(await runtime.withRecoveredIncarnation(current,send)).toEqual({state:'invalid',reason:'recovered-incarnation-changed'});expect(send).not.toHaveBeenCalled();
 });
 it('start drift between independent native samples is held',async()=>{
  await publishOrdinary();const current=await observation();let count=0;kernel.beforeCensus=async()=>{if(++count===3)kernel.rows.find(r=>r.pid===40)!.startedAt='Wed Oct 7 12:10:00 2026';};
  const send=vi.fn();expect(await runtime.withRecoveredIncarnation(current,send)).toEqual({state:'held',reason:'native-recovery-observation-unavailable'});expect(send).not.toHaveBeenCalled();
 });
 it('binding change while asynchronous census is pending grants no observation',async()=>{
  await publishOrdinary();let entered!:()=>void,release!:()=>void;const started=new Promise<void>(r=>entered=r),pending=new Promise<void>(r=>release=r);let first=true;
  kernel.beforeCensus=async()=>{if(first){first=false;entered();await pending;}};const read=runtime.observeRecoveredIncarnation(sessionName);await started;
  db.prepare("UPDATE occupant_tenures SET generation_uuid='replacement-g2' WHERE node_id=?").run(nodeId);release();expect(await read).toBeNull();
 });
 it('send errors propagate exactly once; adapter never retries the effect',async()=>{
  await publishOrdinary();const current=await observation(),error=Error('ambiguous transport outcome');const send=vi.fn(async()=>{throw error;});
  await expect(runtime.withRecoveredIncarnation(current,send)).rejects.toBe(error);expect(send).toHaveBeenCalledTimes(1);expect(completionStore.latest(nodeId,generation)).toEqual(current.completion);
 });
 it('unopted recovery leaves legacy producer evidence intact and creates no shared completion',async()=>{
  enabled=false;eventProof();await guard.runnerRehost(nodeId,()=>runtime.recordNativeRecoveryCompletion(evidence));completePublication();expect(completionStore.latest(nodeId,generation)).toBeNull();expect((db.prepare("SELECT count(*) n FROM events WHERE type='seat.runner_rehost_completed'").get() as {n:number}).n).toBe(1);await guard.set(nodeId,false,'test-fixture','clear');expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();
 });
 it.each(['pi','codex'] as const)('%s private producer receipt binds the actual proved replacement without equating proof/start fingerprints',async kind=>{
  await privateProof(kind);await guard.runnerRehost(nodeId,()=>runtime.recordNativeRecoveryCompletion(evidence));completePublication();await guard.set(nodeId,false,'test-fixture','clear');const current=await observation();
  expect(current.completion.incarnation.native.startFingerprint).not.toBe(evidence.nativeFingerprint);expect(current.completion.source).toEqual(evidence.source);const send=vi.fn(async()=> 'sent');expect(await runtime.withRecoveredIncarnation(current,send)).toMatchObject({state:'performed'});expect(send).toHaveBeenCalledTimes(1);
 });
 it('legacy explicit pin corroborates current original Pi process under guard OFF and appends only normalized evidence',async()=>{
  const pin=await legacyReceipt(),original=fs.readFileSync(pin.ref);runtime=constructRuntime([pin]);
  await guard.set(nodeId,false,'test-fixture','legacy read-only corroboration');const current=await observation();
  expect(current.completion.source).toEqual(pin);expect(current.completion.completedAt).toBe(clockNow-1000);expect(current.completion.incarnation.runtimeLaunchId).toBe(runnerLaunchId);
  expect(fs.readFileSync(pin.ref)).toEqual(original);const rows=()=>db.prepare("SELECT seq FROM events WHERE type='seat.native_recovery_completed'").all();expect(rows()).toHaveLength(1);
  expect((await observation()).completion).toEqual(current.completion);expect(rows()).toHaveLength(1);
  const send=vi.fn(async()=> 'sent');expect(await runtime.withRecoveredIncarnation(current,send)).toMatchObject({state:'performed'});expect(send).toHaveBeenCalledTimes(1);
 });
 it.each(['no-pin','wrong-pin','new-native-start','wrong-native-fingerprint','wrong-binding','guard-on'])('legacy %s grants no normalized completion',async change=>{
  const pin=await legacyReceipt();runtime=constructRuntime(change==='no-pin'?[]:[change==='wrong-pin'?{...pin,digest:'0'.repeat(64)}:pin]);
  if(change!=='guard-on')await guard.set(nodeId,false,'test-fixture','bounded legacy test');
  if(change==='new-native-start')kernel.rows.find(r=>r.pid===40)!.startedAt=new Date(clockNow).toISOString();
  if(change==='wrong-native-fingerprint')piProof.mockResolvedValue({...await piProof(),fingerprint:JSON.stringify({pane:'%changed',runner:[30,20],pi:[40,30],launchId:runnerLaunchId})});
  if(change==='wrong-binding')db.prepare("UPDATE sessions SET resume_token=? WHERE node_id=?").run(path.join(dir,'different-history.jsonl'),nodeId);
  expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();expect(completionStore.latest(nodeId,generation)).toBeNull();
 });

 it('combined actual recovery store/runtime/queue drain resumes the same claimed assignment once',async()=>{
  clockNow=Date.now();vi.useFakeTimers({toFake:['Date']});vi.setSystemTime(clockNow);
  try{
   const repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s),nativeRecoveryContinuation:runtime});repo.attachOutbox(new OutboxHandler(db));
   for(const session of ['lead@xv','peer@xv','reviewer@xv','architect@xv'])db.prepare('INSERT INTO bindings(node_id,tmux_session) VALUES (?,?)').run(session,session);
   await repo.create({qitemId:'runtime-baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
   repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'runtime-baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:120000,operationId:'runtime-enable'});
   repo.coordinatorAuthority.acknowledge('lead@xv',{rigId:'xv',epoch:1,generation:'lead-g1'},{operationId:'runtime-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
   const activity=(session:string):CoordinationActivity=>{const g=repo.coordinatorAuthority.generation(session)!;return {generation:g,identityVerified:true,identityObservedAt:new Date(clockNow).toISOString(),state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clockNow).toISOString(),rungs:[],lastSwap:{generation:g,at:new Date(clockNow).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'fixture-observation',seq:1,observedAt:new Date(clockNow).toISOString(),activity:'idle-at-prompt'}};};
   const svc=new CoordinationRecoveryService(repo,activity,()=>clockNow,undefined,undefined,undefined,runtime);repo.coordinatorAuthority.coordinationRecovery=svc;
   configurationDigest=svc.configurationDigest(sessionName)!;launchTime=2000;configure('pi');
   const task=(key:string,owner:string,recoveryFor?:string):CoordinationTask=>({key,packageKey:key,owner,action:'Complete '+key,deadline:clockNow+60000,body:'body:'+key,predecessors:[],...(recoveryFor?{recoveryFor}:{}),admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'fixture/qualification/'+key,capacityRef:'fixture/capacity/'+key,effortRef:'fixture/effort/'+key,validUntil:clockNow+120000}});
   const product=task('runtime-product',sessionName),repair=task('runtime-repair','architect@xv','runtime-product');
   for(const task of [product,repair])repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',task.packageKey,{inputDigest:sha(task.body),destination:task.owner,bodyHash:sha(task.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
   svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'runtime-combined-r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks:[product,repair]});
   await guard.set(nodeId,false,'test-fixture','materialize admitted pending fixture');const assignment=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='runtime-product')!,queueId=assignment.queueId!;expect(queueId).toBeTruthy();
   db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE audit_pointer=?").run(queueId);repo.claim({qitemId:queueId,destinationSession:sessionName,identityProvenance:'transport:v1'});
   await guard.set(nodeId,true,'test-fixture','supported recovery fixture');await publishOrdinary();
   const before=(db.prepare('SELECT state,body,destination_session,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId));const send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});
   const result=await svc.reconcilePrepared('lead@xv','lead-g1','xv');expect(result.some(r=>r.state==='claimed-recovery-continuation-staged'&&r.queueId===queueId)).toBe(true);
   await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(send.mock.calls[0]![0]).toBe(sessionName);expect(send.mock.calls[0]![1]).toContain('already-claimed assignment '+queueId);
   await svc.reconcilePrepared('lead@xv','lead-g1','xv');await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(db.prepare('SELECT state,body,destination_session,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual(before);
   expect(db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").all(queueId)).toHaveLength(1);
  }finally{vi.useRealTimers();}
 });

 it('candidate search stays inside the exact pane subtree before querying actor argv',async()=>{
  await publishOrdinary();kernel.rows.push(row(60,1,'node'));kernel.identities.set(60,{...prepared.publicEnvironment});kernel.argv.set(60,[prepared.intent.installedNode.path,prepared.intent.installedSupervisor.path,'--supervise',prepared.intent.configPath]);kernel.identity.mockClear();
  expect(await runtime.observeRecoveredIncarnation(sessionName)).not.toBeNull();expect(kernel.identity.mock.calls.some(call=>call[1]?.[2]==='60')).toBe(false);
 });
 it('explicit optout holds an already recorded observation without transport',async()=>{
  await publishOrdinary();const current=await observation();enabled=false;expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();const send=vi.fn();expect(await runtime.withRecoveredIncarnation(current,send)).toMatchObject({state:'held',reason:'native-recovery-observation-unavailable'});expect(send).not.toHaveBeenCalled();
 });
 it('absent optional native launch store refuses producer credit',async()=>{
  runtime=createNativeRecoveryContinuation({db,eventBus:bus,guard,store:undefined,tmux:{getPanePid:async()=>10},piProve:piProof,configurationDigest:()=>configurationDigest,roots:{piDetached:path.join(dir,'pi-private'),codex:path.join(dir,'codex-private')},now:()=>clockNow});eventProof();
  await expect(guard.runnerRehost(nodeId,()=>runtime.recordNativeRecoveryCompletion(evidence))).rejects.toThrow('Native recovery kernel incarnation unavailable');expect(completionStore.latest(nodeId,generation)).toBeNull();
 });

});
