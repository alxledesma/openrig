import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, realpathSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed, token } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { CodexSameGenerationRehost, type CodexRehostOptions } from "../src/domain/codex-rehost.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { digest } from "../src/domain/coordinator-authority-service.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { SeatLifecycleService } from "../src/domain/seat-lifecycle-service.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";

const seat="lead@xv",generation="lead-g1",nativeId="native-thread-exact",now=Date.UTC(2026,9,7,23);
const input={nodeId:seat,sessionName:seat,reason:"Install supervised parent while preserving the adopted holder",operator:"operator-agent@kernel"};
const tables=['nodes','sessions','bindings','occupant_tenures','queue_items','coordinator_authority','coordinator_assignments','coordinator_stage_assignments','coordinator_resources','outbox_entries'];
describe("same-generation Codex process rehost",()=>{
 let db:Database.Database,dir:string,file:string,original:Buffer,guard:SeatDeliveryGuard,options:CodexRehostOptions,service:CodexSameGenerationRehost;
 let processes:NativeProcessRow[],incarnation:number,signal:ReturnType<typeof vi.fn>,resume:ReturnType<typeof vi.fn>,events:EventBus;
 const tree=(n:number):NativeProcessRow[]=>[
  {pid:10,ppid:1,command:'/bin/zsh',executableName:'zsh',startedAt:'root',pgid:10,tpgid:n},
  {pid:n,ppid:10,command:'/usr/bin/node codex-wrapper.js',executableName:'node',startedAt:'wrapper-'+n,pgid:n,tpgid:n},
  {pid:n+1,ppid:n,command:`/usr/bin/codex --no-daemon -p exact -m gpt-6-luna resume ${nativeId}`,executableName:'codex',startedAt:'native-'+n,pgid:n,tpgid:n},
 ];
 const snapshot=()=>JSON.stringify(Object.fromEntries(tables.map(t=>[t,db.prepare('SELECT * FROM '+t).all()])));
 const receipts=()=>readdirSync(options.snapshotRoot,{recursive:true}).map(String);
 beforeEach(async()=>{
  dir=realpathSync(mkdtempSync(path.join(tmpdir(),'codex-rehost-test-')));file=path.join(dir,'native.jsonl');
  original=Buffer.from(JSON.stringify({type:'session_meta',payload:{id:nativeId}})+'\n'+JSON.stringify({type:'turn_context',payload:{model:'gpt-6-luna'}})+'\n');writeFileSync(file,original,{mode:0o600});
  db=createDb();seed(db);events=new EventBus(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,seat);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token=? WHERE node_id=?").run(nativeId,seat);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding',?,?,'%1')").run(seat,seat);
  guard=new SeatDeliveryGuard(db,name=>resolveGuardTarget(db,name));await guard.set(seat,true,'operator','isolated rehost test');
  incarnation=20;processes=tree(incarnation);
  signal=vi.fn(()=>{expect(guard.ownsRunnerRehost(seat)).toBe(true);expect(receipts().some(p=>p.endsWith('began.json'))).toBe(true);processes=[tree(20)[0]!];});
  resume=vi.fn(async()=>{incarnation+=20;processes=tree(incarnation);return {ok:true as const};});
  options={db,guard,tmux:{getPanePid:async()=>10},snapshotRoot:path.join(dir,'private-rehost'),resume:{resume},
   nativeState:async()=>({nodeId:seat,sessionName:seat,nativeId,transcriptPath:file,runtimeContract:{runtime:'codex',model:'gpt-6-luna',provider:'openai',profile:'exact',effort:'high',permissions:{sandbox:{type:'workspace-write'},approval:'never'}}}),
   activityWitness:async()=>({seatNodeId:seat,sessionName:seat,rung:'window-sampling',sourceId:'actual-refreshed-pane',seq:1,observedAt:new Date(now).toISOString(),activity:'idle-at-prompt'}),
   preflightSupervisedLaunch:async()=>({posture:'floor',effective:{model:'gpt-6-luna',provider:'openai',effort:'high',approval:'never',sandbox:'workspace-write'},evidenceDigest:'a'.repeat(64)}),
   observeSupervisedReplacement:async()=>{expect(guard.ownsRunnerRehost(seat)).toBe(true);return {launchId:'supervised-'+incarnation,fingerprint:'independent-os-proof'};},
   listProcesses:async()=>processes,verifyProcessIdentity:async(_pid,identity)=>identity.OPENRIG_NODE_ID===seat&&identity.OPENRIG_SESSION_NAME===seat&&identity.OPENRIG_OCCUPANT_GENERATION===generation&&identity.OPENRIG_RUNTIME==='codex',signal,now:()=>now,sleep:async()=>{},waitMs:1,pollMs:1};
  service=new CodexSameGenerationRehost(options);
 });
 afterEach(()=>{db?.close();rmSync(dir,{recursive:true,force:true});});

 it("preserves real claimed custody, resources and UNKNOWN while resuming exact thread under owned lease",async()=>{
  const repo=new QueueRepository(db,events,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:seat,body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:seat,ownerGeneration:generation,coordinators:[seat,'peer@xv'],leaseMs:60000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge(seat,token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','owned',{inputDigest:'input',destination:seat,bodyHash:digest('retained claim'),resources:['file:x'],returnContract:{destination:'peer@xv',evidenceRequired:['report']}});
  await repo.create({qitemId:'work',sourceSession:seat,destinationSession:seat,body:'retained claim',dispatch:{token,packageKey:'owned'},nudge:false});
  repo.claim({qitemId:'work',destinationSession:seat,identityProvenance:'transport:v1'});
  new OutboxHandler(db).record({outboxId:"unknown",senderSession:seat,destinationSession:"peer@xv",body:"ambiguous effect"});
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate'").run();
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources").get()).toEqual({n:1});
  const before=snapshot(),result=await service.rehost(input);
  expect(result).toMatchObject({ok:true,runtime:'codex',generation,generationUnchanged:true,custodyPreserved:true,authorityRepaired:false});
  if(!result.ok)throw Error(JSON.stringify(result));
  expect(snapshot()).toBe(before);expect(signal).toHaveBeenCalledExactlyOnceWith(21);
  expect(resume).toHaveBeenCalledExactlyOnceWith(seat,'codex_id',nativeId,dir,'exact','floor','gpt-6-luna','high',generation);
  expect(readFileSync(result.backup.path)).toEqual(original);expect(statSync(result.backup.path).mode&0o777).toBe(0o600);
  expect(receipts().some(p=>p.endsWith('completed.json'))).toBe(true);expect(guard.preference(seat)).toMatchObject({desired:true,effective:true});expect(guard.ownsRunnerRehost(seat)).toBe(false);
 });

 it.each(['guard-off','last-thread','runtime','profile','model','effort','kernel','busy','stale','sending','reservation','history','missing-supervision'])("refuses %s before native effects",async(kind)=>{
  if(kind==='guard-off')await guard.set(seat,false,'operator','test');
  if(kind==='last-thread')db.prepare("UPDATE sessions SET resume_type='codex_last' WHERE node_id=?").run(seat);
  if(kind==='runtime')db.prepare("UPDATE nodes SET runtime='pi' WHERE id=?").run(seat);
  if(['profile','model','effort'].includes(kind)){const n=await options.nativeState(seat);(n.runtimeContract as any)[kind]='different';options.nativeState=async()=>n;}
  if(kind==='kernel')options.verifyProcessIdentity=async()=>false;
  if(kind==='busy'||kind==='stale'){const w=(await options.activityWitness(seat,'%1'))!;if(kind==='busy')w.activity='working';else w.observedAt=new Date(now-5001).toISOString();options.activityWitness=async()=>w;}
  if(kind==='sending'){new OutboxHandler(db).record({outboxId:'sending',senderSession:seat,destinationSession:'peer@xv',body:'in-flight'});db.prepare("UPDATE outbox_entries SET delivery_state='sending'").run();}
  if(kind==='reservation')db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('r','op',?,?,'lead-g1','native','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved','now','now')").run(seat,seat);
  if(kind==='history')writeFileSync(file,'partial native write');
  if(kind==='missing-supervision')options.observeSupervisedReplacement=undefined as never;
  const result=await new CodexSameGenerationRehost(options).rehost(input);expect(result).toMatchObject({ok:false,effectAttempted:false});expect(signal).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();expect(guard.ownsRunnerRehost(seat)).toBe(false);
 });

 it.each(['idle','native-proof'])("retains pre-effect evidence and safely retries after final %s refusal",async(kind)=>{
  const originalWitness=options.activityWitness,originalVerify=options.verifyProcessIdentity!;
  let observations=0;
  if(kind==='idle')options.activityWitness=async(...args)=>{
   const witness=(await originalWitness(...args))!;
   return ++observations===3?{...witness,activity:'working'}:witness;
  };
  else options.verifyProcessIdentity=async(...args)=>++observations===3?false:originalVerify(...args);
  service=new CodexSameGenerationRehost(options);
  expect(await service.rehost(input)).toMatchObject({ok:false,effectAttempted:false,code:kind==='idle'?'codex_rehost_not_idle':'codex_rehost_native_unproven'});
  expect(signal).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
  const before=receipts();expect(before.filter(p=>p.endsWith('transcript.jsonl'))).toHaveLength(1);
  expect(before.some(p=>p.endsWith('began.json'))).toBe(false);
  const backupPath=path.join(options.snapshotRoot,before.find(p=>p.endsWith('transcript.jsonl'))!),retained=readFileSync(backupPath);
  expect(retained).toEqual(original);
  expect(await service.rehost(input)).toMatchObject({ok:true,generation});
  expect(signal).toHaveBeenCalledExactlyOnceWith(21);expect(resume).toHaveBeenCalledTimes(1);
  expect(readFileSync(backupPath)).toEqual(retained);expect(receipts().filter(p=>p.endsWith('transcript.jsonl'))).toHaveLength(2);
  expect(receipts().filter(p=>p.endsWith('began.json'))).toHaveLength(1);
 });

 it.each(['stop-unproven','resume-failed','custody-drift','history-truncated','supervision-missing'])("retains durable UNKNOWN and prohibits replay after %s",async(kind)=>{
  if(kind==='stop-unproven')signal.mockImplementation(()=>{});
  if(kind==='resume-failed')resume.mockImplementation(async()=>({ok:false,code:'retry_fresh'}));
  if(kind==='custody-drift')resume.mockImplementation(async()=>{processes=tree(40);db.prepare("UPDATE nodes SET title='changed custody' WHERE id=?").run(seat);return {ok:true};});
  if(kind==='history-truncated')resume.mockImplementation(async()=>{processes=tree(40);writeFileSync(file,JSON.stringify({type:'session_meta',payload:{id:nativeId}})+'\n');return {ok:true};});
  if(kind==='supervision-missing')options.observeSupervisedReplacement=async()=>null;
  service=new CodexSameGenerationRehost(options);const first=await service.rehost(input);expect(first).toMatchObject({ok:false,effectAttempted:true,blindRetryAllowed:false});expect(signal).toHaveBeenCalledTimes(1);expect(resume).toHaveBeenCalledTimes(kind==='stop-unproven'?0:1);expect(receipts().some(p=>p.endsWith('unknown.json'))).toBe(true);
  const again=await service.rehost(input);expect(again).toMatchObject({ok:false,code:'codex_rehost_unresolved_attempt',effectAttempted:false});expect(signal).toHaveBeenCalledTimes(1);expect(guard.ownsRunnerRehost(seat)).toBe(false);
 });

 it("permits a distinct future incarnation after success but rejects same-incarnation replay",async()=>{
  expect(await service.rehost(input)).toMatchObject({ok:true});expect(await service.rehost(input)).toMatchObject({ok:true});expect(signal).toHaveBeenCalledTimes(2);
  processes=tree(20);expect(await service.rehost(input)).toMatchObject({ok:false,code:'codex_rehost_attempt_replay',effectAttempted:false});expect(signal).toHaveBeenCalledTimes(2);
 });

 it("dispatches actual Codex runtime, excludes Pi recovery flags and preserves general adopted stop refusal",async()=>{
  const delegation=vi.fn((i:typeof input)=>service.rehost(i));
  const lifecycle=new SeatLifecycleService({db,rigRepo:new RigRepository(db),sessionRegistry:new SessionRegistry(db),eventBus:events,tmuxAdapter:{deliveryGuard:guard} as unknown as TmuxAdapter,codexRehost:{rehost:delegation}});
  expect(await lifecycle.rehostRunner({seatRef:seat,reason:'supervise',legacyNativeWitness:true})).toMatchObject({ok:false,code:'rehost_recovery_modes_exclusive'});expect(delegation).not.toHaveBeenCalled();
  await guard.set(seat,false,'operator','exercise unchanged general stop fence');
  expect(await lifecycle.stopSeat({seatRef:seat,reason:'ordinary stop'})).toMatchObject({ok:false,code:'claimed_session'});expect(signal).not.toHaveBeenCalled();
  await guard.set(seat,true,'operator','restore rehost guard');
  expect(await lifecycle.rehostRunner({seatRef:seat,reason:'supervise'})).toMatchObject({ok:true,runtime:'codex'});expect(delegation).toHaveBeenCalledTimes(1);
 });
});
