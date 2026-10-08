import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";import os from "node:os";import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SeatDeliveryGuard, DeliveryGuardError, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { NativeDutyLaunchStore, type PreparedNativeDutyLaunch } from "../src/domain/native-duty-launch.js";
import { NativeRecoveryCompletionStore } from "../src/domain/native-recovery-completion.js";
import { createNativeRecoveryContinuation } from "../src/domain/native-recovery-continuation.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";
import type { PiNativeProof } from "../src/domain/coordinator-runtime-availability.js";
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
const nodeId="builder@xv",sessionName=nodeId,generation="builder-g1";
let configurationDigest:string,launchSerial:number;
const runnerLaunchId="pi-native-recovered-1",supervisorLaunchId="managed-recovered-1";
let db:Database.Database,bus:EventBus,guard:SeatDeliveryGuard,store:NativeDutyLaunchStore,completionStore:NativeRecoveryCompletionStore;
let dir:string,nativeId:string,prepared:PreparedNativeDutyLaunch;
let runtime:ReturnType<typeof createNativeRecoveryContinuation>;
let enabled:boolean,launchTime:number,clockNow:number;
const piProof=vi.fn();let proof:PiNativeProof;
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
 guard=new SeatDeliveryGuard(db,name=>resolveGuardTarget(db,name));await guard.set(nodeId,false,'test-fixture','allow fixture observation');
 store=new NativeDutyLaunchStore({root:path.join(dir,'launches'),nodeExecutable:path.join(dir,'node'),supervisorEntry:path.join(dir,'native-duty-supervisor.js'),now:()=>launchTime});
 enabled=true;launchTime=1000;launchSerial=0;configurationDigest="a".repeat(64);clockNow=Date.UTC(2026,9,7,12,5);configure();
 proof={state:'present',generation,launchId:runnerLaunchId,lastEntryId:'native-cursor-1',
 fingerprint:JSON.stringify({pane:'%1',runner:[30,20],pi:[40,30],launchId:runnerLaunchId}),
 quiescence:{settled:true,observedAt:new Date(clockNow).toISOString()}};
 piProof.mockImplementation(async()=>structuredClone(proof));runtime=constructRuntime();
});
afterEach(()=>{db?.close();fs.rmSync(dir,{recursive:true,force:true});});


async function observation(){const o=await runtime.observeSettledClaimant(sessionName);expect(o).not.toBeNull();return o!;}
describe('ordinary native settled claimant with real launch storage and serialized guard',()=>{
 it('observes and sends with no recovery receipt; quiet freshness polling does not change turn identity',async()=>{
  expect(completionStore.latest(nodeId,generation)).toBeNull();expect(await runtime.observeRecoveredIncarnation(sessionName)).toBeNull();
  const o=await observation();expect(o).toMatchObject({schema:'native-settled-observation.v1',sessionName,generation,lastEntryId:'native-cursor-1',incarnation:{runtimeLaunchId:runnerLaunchId,native:{pid:40}}});
  clockNow+=1000;proof.quiescence!.observedAt=new Date(clockNow).toISOString();const later=await observation();
  expect(later.incarnation).toEqual(o.incarnation);expect(later.lastEntryId).toBe(o.lastEntryId);expect(later.quiescenceObservedAt).not.toBe(o.quiescenceObservedAt);
  const send=vi.fn(async()=> 'sent');expect(await runtime.withSettledClaimant(o,send)).toEqual({state:'performed',value:'sent'});expect(send).toHaveBeenCalledTimes(1);
  expect(completionStore.latest(nodeId,generation)).toBeNull();
 });
 it.each(['busy','unknown','stale','future','empty-cursor','wrong-generation','wrong-launch','wrong-fingerprint'] as const)('%s proof denies observation and send',async kind=>{
  const o=await observation();
  if(kind==='busy')proof.quiescence!.settled=false;
  if(kind==='unknown')proof.quiescence=undefined;
  if(kind==='stale')proof.quiescence!.observedAt=new Date(clockNow-3001).toISOString();
  if(kind==='future')proof.quiescence!.observedAt=new Date(clockNow+1).toISOString();
  if(kind==='empty-cursor')proof.lastEntryId='';
  if(kind==='wrong-generation')proof.generation='wrong';
  if(kind==='wrong-launch')proof.launchId='wrong';
  if(kind==='wrong-fingerprint')proof.fingerprint='{}';
  expect(await runtime.observeSettledClaimant(sessionName)).toBeNull();const send=vi.fn();expect(await runtime.withSettledClaimant(o,send)).toMatchObject({state:'held'});expect(send).not.toHaveBeenCalled();
 });
 it.each(['native-start','supervisor-start','configuration','generation'] as const)('%s changed since recorded observation never sends',async kind=>{
  const o=await observation();
  if(kind==='native-start')kernel.rows.find(r=>r.pid===40)!.startedAt='2026-10-07T12:04:00.000Z';
  if(kind==='supervisor-start')kernel.rows.find(r=>r.pid===20)!.startedAt='2026-10-07T12:04:00.000Z';
  if(kind==='configuration')configurationDigest='b'.repeat(64);
  if(kind==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='replacement' WHERE node_id=?").run(nodeId);
  const send=vi.fn();expect(await runtime.withSettledClaimant(o,send)).toMatchObject({state:'invalid'});expect(send).not.toHaveBeenCalled();
 });
 it('a later settled cursor in the same incarnation refreshes the pending obligation proof',async()=>{
  const original=await observation();proof.lastEntryId='native-cursor-2';
  const send=vi.fn(async(current:NativeSettledObservation)=>current.lastEntryId);
  expect(await runtime.withSettledClaimant(original,send)).toEqual({state:'performed',value:'native-cursor-2'});
  expect(send).toHaveBeenCalledTimes(1);expect(original.lastEntryId).toBe('native-cursor-1');
 });
 it('changed cursor across async kernel observation and final binding change hold',async()=>{
  let once=true;kernel.beforeCensus=async()=>{if(once){once=false;proof.lastEntryId='next-cursor';}};
  expect(await runtime.observeSettledClaimant(sessionName)).toBeNull();kernel.beforeCensus=null;
  let calls=0;piProof.mockImplementation(async()=>{if(++calls===4)configurationDigest='b'.repeat(64);return structuredClone(proof);});
  expect(await runtime.observeSettledClaimant(sessionName)).toBeNull();
 });
 it('reobserves only after guard serialization; busy proof while waiting holds without callback',async()=>{
  const o=await observation();let entered!:()=>void,release!:()=>void;const started=new Promise<void>(r=>entered=r),pending=new Promise<void>(r=>release=r);
  const blocker=guard.operation(sessionName,async()=>{entered();await pending;});await started;
  const send=vi.fn();piProof.mockClear();const attempt=runtime.withSettledClaimant(o,send);await Promise.resolve();expect(piProof).not.toHaveBeenCalled();
  proof.quiescence!.settled=false;release();await blocker;expect(await attempt).toMatchObject({state:'held'});expect(send).not.toHaveBeenCalled();
 });
 it('typing protection holds and post-callback errors, including guard errors, propagate UNKNOWN',async()=>{
  const o=await observation();await guard.set(nodeId,true,'fixture','hold');const send=vi.fn();expect(await runtime.withSettledClaimant(o,send)).toMatchObject({state:'held',reason:'typing_guard_enabled'});expect(send).not.toHaveBeenCalled();
  await guard.set(nodeId,false,'fixture','clear');const error=new DeliveryGuardError('typing_guard_enabled','after possible effect');
  const failed=vi.fn(async()=>{throw error;});await expect(runtime.withSettledClaimant(o,failed)).rejects.toBe(error);expect(failed).toHaveBeenCalledTimes(1);
 });
 it('wrong inherited native identity and unsupported Codex never infer settled',async()=>{
  kernel.identities.get(40)!.OPENRIG_OCCUPANT_GENERATION='wrong';expect(await runtime.observeSettledClaimant(sessionName)).toBeNull();
  configure('codex');expect(await runtime.observeSettledClaimant(sessionName)).toBeNull();
 });
});
