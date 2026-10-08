import { afterEach,beforeEach,describe,expect,it,vi } from 'vitest';
import { mkdtempSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type Database from 'better-sqlite3';
import { createDb } from '../src/db/connection.js';
import { EventBus } from '../src/domain/event-bus.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { CoordinationRecoveryService,type CoordinationActivity,type CoordinationTask } from '../src/domain/coordination-recovery-service.js';
import { digest } from '../src/domain/coordinator-authority-service.js';
import { makeCoordinatorContinuityPolicy } from '../src/domain/policies/coordinator-continuity.js';
import type { NativeRecoveryContinuationRuntime,NativeSettledObservation } from '../src/domain/native-recovery-continuation-contract.js';
import { seed,token } from './helpers/coordinator-fixture.js';

describe('owned in-progress outcome protocol through the registered coordinator policy',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,bus:EventBus,clock:number;
 let observations:Map<string,CoordinationActivity>,runtime:NativeRecoveryContinuationRuntime,settled:NativeSettledObservation|null;
 let refreshIdentityCalls:string[][],refreshActivityCalls:string[],failBuilderRefresh:boolean,builderRefreshBusy:boolean;
 const makeActivity=(session:string):CoordinationActivity=>{const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,identityObservedAt:new Date(clock).toISOString(),state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};};
 const task=(key:string,owner:string):CoordinationTask=>({key,packageKey:key,owner,action:'Complete '+key+' with bounded return evidence',deadline:clock+60000,body:'body:'+key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'fixture/qualification/'+key,capacityRef:'fixture/capacity/'+key,effortRef:'fixture/effort/'+key,validUntil:clock+120000}});
 const admitted=(t:CoordinationTask)=>repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.body),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 const configure=()=>{const main=task('product','builder@xv'),repair={...task('repair','architect@xv'),recoveryFor:'product'};admitted(main);admitted(repair);svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'owned-outcome-r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks:[main,repair]});return main;};
 const settledObservation=():NativeSettledObservation=>({schema:'native-settled-observation.v1',rigId:'xv',nodeId:'builder@xv',sessionId:'builder@xv',sessionName:'builder@xv',generation:'builder-g1',runtime:'pi',nativeIdentityHash:digest('builder-native'),configurationDigest:svc.configurationDigest('builder@xv')!,incarnation:{key:'builder-incarnation-1',runtimeLaunchId:'builder-launch-1',supervisorLaunchId:'builder-supervisor-1',native:{pid:8101,startFingerprint:'kernel-start-builder-8101'},supervisor:{pid:8100,startFingerprint:'kernel-start-supervisor-8100'}},lastEntryId:'native-entry-1',quiescenceObservedAt:clock,observedAt:clock});
 const emitActivity=(generation='builder-g1',eventAt=new Date(clock).toISOString())=>bus.emit({type:'agent.activity',rigId:'xv',nodeId:'builder@xv',sessionName:'builder@xv',runtime:'pi',activity:{state:'idle',reason:'native-runtime-idle',evidenceSource:'runtime_hook',sampledAt:eventAt,eventAt,evidence:null,generation}});
 const job={jobId:'continuity-observer',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any;

 beforeEach(async()=>{
  vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'owned-outcome-protocol-'));db=createDb(join(dir,'db'));seed(db);
  for(const session of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])db.prepare('INSERT INTO bindings(node_id,tmux_session) VALUES (?,?)').run(session,session);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  for(const node of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES (?,0,0,'fixture','off',?)").run(node,new Date(clock).toISOString());
  bus=new EventBus(db);observations=new Map();
  refreshIdentityCalls=[];refreshActivityCalls=[];failBuilderRefresh=false;builderRefreshBusy=false;
  const refreshIdentity=async(sessions:readonly string[])=>{
   refreshIdentityCalls.push([...sessions]);
   for(const session of sessions){const old=observations.get(session);if(old)observations.set(session,{...old,identityObservedAt:new Date(clock).toISOString()});}
  };
  const refreshWorkerActivity=async(session:string)=>{
   refreshActivityCalls.push(session);
   if(session==='builder@xv'&&failBuilderRefresh)throw new Error('fixture-refresh-failed');
   const old=observations.get(session);if(!old)return;
   const fresh=makeActivity(session);
   observations.set(session,builderRefreshBusy&&session==='builder@xv'
    ?{...fresh,state:{...fresh.state,activity:'busy'},witness:{...fresh.witness,activity:'busy'}}
    :{...fresh,identityObservedAt:old.identityObservedAt});
  };
  runtime={observeRecoveredIncarnation:vi.fn(async()=>null),withRecoveredIncarnation:vi.fn(async(_observation,send)=>({state:'performed' as const,value:await send()})),observeSettledClaimant:vi.fn(async()=>settled),withSettledClaimant:vi.fn(async(_observation,send)=>({state:'performed' as const,value:await send(settled!)}))};
  repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s),nativeRecoveryContinuation:runtime});repo.attachOutbox(new OutboxHandler(db));
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:120000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  svc=new CoordinationRecoveryService(repo,s=>observations.get(s)??null,()=>clock,refreshIdentity,refreshWorkerActivity,undefined,runtime);repo.coordinatorAuthority.coordinationRecovery=svc;
  for(const session of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])observations.set(session,makeActivity(session));
  configure();settled=settledObservation();
  db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid,target_generation_uuid) VALUES ('continuity-observer','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1','operator-agent-g1')").run(new Date(clock).toISOString());
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});

 async function claimOriginal(options:{activity?:'fresh'|'missing'|'old'|'wrong-generation';native?:boolean}={}){
  const assignment=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='product')!,queueId=assignment.queueId!;
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE audit_pointer=?").run(queueId);
  repo.claim({qitemId:queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  const row=db.prepare('SELECT qitem_id,body,state,claimed_at,claimed_by_generation_uuid,destination_session FROM queue_items WHERE qitem_id=?').get(queueId) as {qitem_id:string;body:string;state:string;claimed_at:string;claimed_by_generation_uuid:string;destination_session:string};
  const transition=db.prepare("SELECT transition_id,ts,actor_session FROM queue_transitions WHERE qitem_id=? AND state='in-progress' ORDER BY transition_id DESC LIMIT 1").get(queueId) as {transition_id:number;ts:string;actor_session:string};
  clock+=2000;vi.setSystemTime(clock);
  if(options.activity==='fresh'||options.activity===undefined)emitActivity();
  if(options.activity==='wrong-generation')emitActivity('builder-g2');
  if(options.activity==='old')emitActivity('builder-g1',new Date(Date.parse(transition.ts)-1).toISOString());
  settled=options.native===false?null:settledObservation();
  return {queueId,row,transition};
 }

 it('actual registered policy tick delivers one owned-outcome notice after fresh idle evidence, without taking over or accepting the claim',async()=>{
  const {queueId,row,transition}=await claimOriginal();expect(transition.actor_session).toBe('builder@xv');
  const sent:string[]=[];repo.attachTransport({send:async(destination,body)=>{sent.push(destination+':'+body);return {ok:true,verified:true};}});
  const beforeItems=db.prepare('SELECT COUNT(*) AS n FROM queue_items').get() as {n:number};
  const policy=makeCoordinatorContinuityPolicy(repo.coordinatorAuthority);
  const first=await policy.evaluate(job);
  const intents=()=>db.prepare("SELECT outbox_id,delivery_state,tags,body FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:owned-outcome-protocol%' ORDER BY outbox_id").all(queueId) as Array<{outbox_id:string;delivery_state:string;tags:string;body:string}>;
  expect(JSON.stringify(first)).toContain('owned-outcome-protocol-staged');
  expect(intents()).toHaveLength(1);expect(intents()[0]?.delivery_state).toBe('delivered');expect(sent).toHaveLength(1);
  expect(intents()[0]?.body.toLowerCase()).toMatch(/return\s*instructions/);expect(intents()[0]?.body.toLowerCase()).toMatch(/retained evidence/);expect(intents()[0]?.body.toLowerCase()).toMatch(/typed return/);expect(intents()[0]?.body.toLowerCase()).toMatch(/unfinished|blocked/);
  expect(repo.getById(queueId)).toMatchObject({state:'in-progress',body:row.body,destinationSession:'builder@xv'});
  expect(db.prepare('SELECT claimed_at,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_at:row.claimed_at,claimed_by_generation_uuid:'builder-g1'});
  expect((db.prepare('SELECT COUNT(*) AS n FROM queue_items').get() as {n:number}).n).toBe(beforeItems.n);
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept' AND json_extract(receipt,'$.queueId')=?").get(queueId)).toBeUndefined();
  expect(runtime.observeRecoveredIncarnation).toHaveBeenCalledWith('builder@xv');
  expect(runtime.observeSettledClaimant).toHaveBeenCalledWith('builder@xv');
  expect(runtime.withSettledClaimant).toHaveBeenCalledTimes(1);

  clock+=1000;vi.setSystemTime(clock);settled={...settled!,lastEntryId:'native-entry-2',observedAt:clock,quiescenceObservedAt:clock};emitActivity();
  await policy.evaluate(job);
  expect(intents()).toHaveLength(1);expect(sent).toHaveLength(1);
  expect(repo.getById(queueId)).toMatchObject({state:'in-progress',body:row.body,destinationSession:'builder@xv'});
  expect(db.prepare('SELECT claimed_at,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_at:row.claimed_at,claimed_by_generation_uuid:'builder-g1'});
 });

 it('refreshes a stale claimed Builder through the registered policy before staging the one outcome notice',async()=>{
  const {queueId,row}=await claimOriginal();clock+=4001;vi.setSystemTime(clock);
  settled=null;
  runtime.observeSettledClaimant=vi.fn(async(session:string)=>{settled=settledObservation();return session==='builder@xv'?settled:null;});
  const sent:string[]=[];repo.attachTransport({send:async(destination,body)=>{sent.push(destination+':'+body);return {ok:true,verified:true};}});
  const result=await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate(job);
  const intents=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:owned-outcome-protocol%' ORDER BY outbox_id").all(queueId) as Array<{outbox_id:string;delivery_state:string}>;
  expect(refreshActivityCalls).toContain('builder@xv');
  expect(refreshIdentityCalls.some(sessions=>sessions.length===1&&sessions[0]==='builder@xv')).toBe(true);
  expect(JSON.stringify(result)).toContain('owned-outcome-protocol-staged');
  expect(intents).toHaveLength(1);expect(intents[0]?.delivery_state).toBe('delivered');expect(sent).toHaveLength(1);
  expect(repo.getById(queueId)).toMatchObject({state:'in-progress',body:row.body,destinationSession:'builder@xv'});
  expect(db.prepare('SELECT claimed_at,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_at:row.claimed_at,claimed_by_generation_uuid:'builder-g1'});
 });

 it.each([
  ['missing post-claim activity','missing'],
  ['old activity timestamp','old'],
  ['wrong activity generation','wrong-generation'],
  ['missing settled-native proof','native-missing'],
  ['claim generation changed','changed-claim'],
  ['typed return already exists','typed-return'],
  ['unknown worker effect exists','unknown-effect'],
  ['worker delivery guard is on','guard-on'],
  ['current admission is expired','expired-admission'],
 ] as const)('holds without staging an outcome notice when %s',async(_label,scenario)=>{
  const options=scenario==='missing'||scenario==='old'||scenario==='wrong-generation'?{activity:scenario as 'missing'|'old'|'wrong-generation'}:scenario==='native-missing'?{activity:'fresh' as const,native:false}:{};
  const {queueId,row}=await claimOriginal(options);
  if(scenario==='changed-claim')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g2' WHERE qitem_id=?").run(queueId);
  if(scenario==='typed-return')db.prepare('UPDATE queue_items SET reply_to=? WHERE qitem_id=?').run('typed-return',queueId);
  if(scenario==='unknown-effect'){
   const effect=new OutboxHandler(db).record({senderSession:'builder@xv',destinationSession:'lead@xv',body:'fixture unresolved worker effect'});
   db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(effect.outboxId);
  }
  if(scenario==='guard-on')db.prepare("UPDATE seat_delivery_guards SET desired=1,effective=1 WHERE node_id='builder@xv'").run();
  if(scenario==='expired-admission'){
   const plan=svc.plan('xv')!,current=plan.tasks.find(t=>t.key==='product')!;
   current.admission.validUntil=clock+1000;svc.configure('operator-agent@kernel','operator-agent-g1',{...plan,revision:'short-owned-outcome-admission'});
   clock+=2000;vi.setSystemTime(clock);settled=settledObservation();emitActivity();
  }
  const policy=makeCoordinatorContinuityPolicy(repo.coordinatorAuthority);await policy.evaluate(job);
  expect(db.prepare("SELECT 1 FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:owned-outcome-protocol%' LIMIT 1").get(queueId)).toBeUndefined();
  const after=repo.getById(queueId)!;expect(after.body).toBe(row.body);expect(after.state).toBe('in-progress');
  if(scenario!=='changed-claim')expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_by_generation_uuid:'builder-g1'});
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept' AND json_extract(receipt,'$.queueId')=?").get(queueId)).toBeUndefined();
 });

 async function stageHeldIntent(){
  const {queueId}=await claimOriginal(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});
  runtime.withSettledClaimant=vi.fn(async()=>({state:'held' as const,reason:'fixture-native-guard-held'}));
  await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate(job);
  const intent=()=>db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:owned-outcome-protocol%' LIMIT 1").get(queueId) as {outbox_id:string;delivery_state:string}|undefined;
  expect(intent()?.delivery_state).toBe('pending');expect(send).not.toHaveBeenCalled();
  return {queueId,intent,send};
 }

 it('keeps a native-guard hold pending and drains the same intent once when the guard clears',async()=>{
  const {queueId,intent,send}=await stageHeldIntent(),pending=intent()!;
  runtime.withSettledClaimant=vi.fn(async(_observation,sendOnce)=>({state:'performed' as const,value:await sendOnce(settled!)}));
  await repo.drainPendingWakeIntents();await repo.drainPendingWakeIntents();
  expect(intent()).toEqual({...pending,delivery_state:'delivered'});expect(send).toHaveBeenCalledTimes(1);expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('refreshes stale claimant activity and reobserves the exact native worker before draining a pending intent',async()=>{
  const {queueId,intent,send}=await stageHeldIntent(),pending=intent()!;clock+=4001;vi.setSystemTime(clock);
  runtime.observeSettledClaimant=vi.fn(async(session:string)=>{settled=session==='builder@xv'?settledObservation():null;return settled;});
  runtime.withSettledClaimant=vi.fn(async(_observation,sendOnce)=>{
   const current=await runtime.observeSettledClaimant!('builder@xv');
   return current?{state:'performed' as const,value:await sendOnce(current)}:{state:'held' as const,reason:'fixture-observation-missing'};
  });
  await repo.drainPendingWakeIntents();
  expect(refreshActivityCalls.filter(session=>session==='builder@xv').length).toBeGreaterThan(1);
  expect(refreshIdentityCalls.filter(sessions=>sessions.length===1&&sessions[0]==='builder@xv').length).toBeGreaterThan(1);
  expect(runtime.observeSettledClaimant).toHaveBeenCalledWith('builder@xv');
  expect(runtime.observeSettledClaimant).toHaveBeenCalledTimes(1);
  expect(intent()).toEqual({...pending,delivery_state:'delivered'});expect(send).toHaveBeenCalledTimes(1);
  expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it.each(['refresh-failed','newly-busy','generation-changed','unknown-effect'] as const)(
  'does not send or retire the original claim when refreshed delivery is held by %s',async(reason)=>{
   const {queueId,intent,send}=await stageHeldIntent();clock+=4001;vi.setSystemTime(clock);
   if(reason==='refresh-failed')failBuilderRefresh=true;
   if(reason==='newly-busy')builderRefreshBusy=true;
   if(reason==='generation-changed'){
    db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
    settled=settledObservation();
   }
   if(reason==='unknown-effect'){
    const effect=new OutboxHandler(db).record({senderSession:'builder@xv',destinationSession:'lead@xv',body:'fixture unresolved worker effect'});
    db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(effect.outboxId);
   }
   const beforeClaim=repo.getById(queueId)!;
   if(reason!=='refresh-failed')runtime.withSettledClaimant=vi.fn(async(_observation,sendOnce)=>({state:'performed' as const,value:await sendOnce(settledObservation())}));
   await repo.drainPendingWakeIntents();
   expect(send).not.toHaveBeenCalled();
   expect(repo.getById(queueId)).toMatchObject({state:'in-progress',body:beforeClaim.body,destinationSession:'builder@xv'});
   expect(db.prepare('SELECT claimed_at,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_at:beforeClaim.claimedAt,claimed_by_generation_uuid:'builder-g1'});
   expect(intent()?.delivery_state).toBe(reason==='generation-changed'?'failed':'pending');
  }
 );

 it('delivers the same pending obligation after a later native turn settles, without rewriting the receipt',async()=>{
  const {queueId,intent,send}=await stageHeldIntent(),original=intent()!;
  const saved=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=?").get(original.outbox_id);
  clock+=1000;vi.setSystemTime(clock);emitActivity();settled={...settled!,lastEntryId:'native-entry-changed',observedAt:clock,quiescenceObservedAt:clock};
  runtime.withSettledClaimant=vi.fn(async(_observation,sendOnce)=>({state:'performed' as const,value:await sendOnce(settled!)}));
  await repo.drainPendingWakeIntents();await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate(job);
  expect(intent()).toEqual({...original,delivery_state:'delivered'});expect(send).toHaveBeenCalledTimes(1);
  expect(db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=?").get(original.outbox_id)).toEqual(saved);
  expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it.each(['claim','authority','typed-return'] as const)('invalidates a staged intent when %s changes before the final send CAS',async(change)=>{
  const {queueId,intent,send}=await stageHeldIntent();
  if(change==='claim')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g2' WHERE qitem_id=?").run(queueId);
  if(change==='authority')db.prepare("UPDATE coordinator_authority SET epoch=epoch+1 WHERE rig_id='xv'").run();
  if(change==='typed-return')db.prepare('UPDATE queue_items SET reply_to=? WHERE qitem_id=?').run('new-typed-return',queueId);
  runtime.withSettledClaimant=vi.fn(async(_observation,sendOnce)=>({state:'performed' as const,value:await sendOnce(settled!)}));
  await repo.drainPendingWakeIntents();
  expect(send).not.toHaveBeenCalled();expect(intent()?.delivery_state).toBe('failed');expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('records an ambiguous post-send result UNKNOWN and never replays it on another drain',async()=>{
  const {queueId,intent,send}=await stageHeldIntent();
  runtime.withSettledClaimant=vi.fn(async(_observation,sendOnce)=>{await sendOnce(settled!);throw new Error('native guard completion became uncertain');});
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(intent()?.delivery_state).toBe('indeterminate');
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(intent()?.delivery_state).toBe('indeterminate');expect(repo.getById(queueId)?.state).toBe('in-progress');
 });
});
