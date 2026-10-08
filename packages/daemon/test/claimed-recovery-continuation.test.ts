import { afterEach,beforeEach,describe,expect,it,vi } from 'vitest';
import { mkdtempSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type Database from 'better-sqlite3';
import { createDb } from '../src/db/connection.js';
import { EventBus } from '../src/domain/event-bus.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { CoordinationRecoveryService,type CoordinationActivity,type CoordinationPlan,type CoordinationTask } from '../src/domain/coordination-recovery-service.js';
import { digest } from '../src/domain/coordinator-authority-service.js';
import { makeCoordinatorContinuityPolicy } from '../src/domain/policies/coordinator-continuity.js';
import type { NativeRecoveryCompletion,NativeRecoveryObservation,NativeRecoveryContinuationRuntime } from '../src/domain/native-recovery-continuation-contract.js';
import { seed,token } from './helpers/coordinator-fixture.js';

describe('retained claimed native-recovery continuation',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number;
 let observations:Map<string,CoordinationActivity>,nativeObservation:NativeRecoveryObservation|null,runtime:NativeRecoveryContinuationRuntime;
 const makeActivity=(session:string):CoordinationActivity=>{const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,identityObservedAt:new Date(clock).toISOString(),state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};};
 const task=(key:string,owner:string,more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Complete '+key+' with bounded return evidence',deadline:clock+60000,body:'body:'+key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'fixture/qualification/'+key,capacityRef:'fixture/capacity/'+key,effortRef:'fixture/effort/'+key,validUntil:clock+120000},...more});
 const refresh=()=>{for(const session of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])observations.set(session,makeActivity(session));};
 const admitted=(t:CoordinationTask)=>repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.body),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 const configure=(productDeadline=clock+60000)=>{const main=task('product','builder@xv',{deadline:productDeadline}),repair=task('repair','architect@xv',{deadline:productDeadline,recoveryFor:'product'});admitted(main);admitted(repair);svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'continuation-r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks:[main,repair]});return main;};
 const completion=(configurationDigest:string):NativeRecoveryCompletion=>({schema:'native-recovery-completion.v1',recoveryId:'recovery-1',producer:'pi-detached-resume',rigId:'xv',nodeId:'builder@xv',sessionId:'builder@xv',sessionName:'builder@xv',generation:'builder-g1',runtime:'pi',nativeIdentityHash:digest('native-identity'),configurationDigest,completedAt:clock-100,source:{ref:'fixture/private-completion',digest:digest('receipt')},incarnation:{key:'native-launch-incarnation-1',runtimeLaunchId:'pi-launch-1',supervisorLaunchId:'supervisor-launch-1',native:{pid:8101,startFingerprint:'kernel-start-8101'},supervisor:{pid:8100,startFingerprint:'kernel-start-8100'}},custodyPreserved:true,generationUnchanged:true});

 beforeEach(async()=>{
  vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'claimed-native-continuation-'));db=createDb(join(dir,'db'));seed(db);
  for(const session of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])db.prepare('INSERT INTO bindings(node_id,tmux_session) VALUES (?,?)').run(session,session);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  for(const node of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES (?,0,0,'fixture','off',?)").run(node,new Date(clock).toISOString());
  const bus=new EventBus(db);
  runtime={observeRecoveredIncarnation:vi.fn(async session=>session==='builder@xv'?nativeObservation:null),withRecoveredIncarnation:vi.fn(async (_observation,send)=>({state:'performed' as const,value:await send()}))};
  repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s),nativeRecoveryContinuation:runtime});repo.attachOutbox(new OutboxHandler(db));
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:120000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  observations=new Map();svc=new CoordinationRecoveryService(repo,s=>observations.get(s)??null,()=>clock,undefined,undefined,undefined,runtime);repo.coordinatorAuthority.coordinationRecovery=svc;refresh();nativeObservation=null;
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});

 async function claimedAssignment(productDeadline?:number){
  const main=configure(productDeadline),assignment=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='product')!,queueId=assignment.queueId!;
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE audit_pointer=?").run(queueId);
  repo.claim({qitemId:queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  nativeObservation={completion:completion(svc.configurationDigest('builder@xv')!),observedAt:clock};refresh();
  return {main,queueId};
 }

 it('stages and drains exactly one notice for the same claimed row and native incarnation',async()=>{
  const {queueId}=await claimedAssignment();const sent:string[]=[];
  repo.attachTransport({send:async(destination,text)=>{sent.push(destination+':'+text);return {ok:true,verified:true};}});
  const first=await svc.reconcilePrepared('lead@xv','lead-g1','xv');expect(first.some(x=>x.state==='claimed-recovery-continuation-staged'&&x.queueId===queueId)).toBe(true);
  const intents=()=>db.prepare("SELECT outbox_id,delivery_state,tags FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").all(queueId) as Array<{outbox_id:string;delivery_state:string;tags:string}>;
  expect(intents()).toHaveLength(1);expect(repo.getById(queueId)?.state).toBe('in-progress');expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_by_generation_uuid:'builder-g1'});
  await repo.drainPendingWakeIntents();expect(sent).toHaveLength(1);expect(sent[0]).toContain('already-claimed assignment '+queueId);expect(runtime.withRecoveredIncarnation).toHaveBeenCalledTimes(1);expect(repo.getById(queueId)?.state).toBe('in-progress');
  await svc.reconcilePrepared('lead@xv','lead-g1','xv');expect(intents()).toHaveLength(1);await repo.drainPendingWakeIntents();expect(sent).toHaveLength(1);
 });

 it('actual registered coordinator-continuity ticks stage and deliver one immutable notice',async()=>{
  const {queueId}=await claimedAssignment(),sent:string[]=[];
  repo.attachTransport({send:async(destination,text)=>{sent.push(destination+':'+text);return {ok:true,verified:true};}});
  db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('continuity-observer','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')").run(new Date(clock).toISOString());
  const job={jobId:'continuity-observer',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any;
  const before=db.prepare('SELECT qitem_id,body,state,claimed_by_generation_uuid,destination_session FROM queue_items WHERE qitem_id=?').get(queueId);
  const policy=makeCoordinatorContinuityPolicy(repo.coordinatorAuthority);
  await policy.evaluate(job);
  const intents=()=>db.prepare("SELECT outbox_id,delivery_state,tags,body FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").all(queueId) as Array<{outbox_id:string;delivery_state:string;tags:string;body:string}>;
  expect(intents()).toHaveLength(1);expect(intents()[0]?.delivery_state).toBe('delivered');expect(sent).toHaveLength(1);
  expect(repo.getById(queueId)?.state).toBe('in-progress');
  expect(db.prepare('SELECT qitem_id,body,state,claimed_by_generation_uuid,destination_session FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual(before);
  expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual({claimed_by_generation_uuid:'builder-g1'});
  await policy.evaluate(job);
  expect(intents()).toHaveLength(1);expect(intents()[0]?.delivery_state).toBe('delivered');expect(sent).toHaveLength(1);
  expect(db.prepare('SELECT qitem_id,body,state,claimed_by_generation_uuid,destination_session FROM queue_items WHERE qitem_id=?').get(queueId)).toEqual(before);
  expect(runtime.observeRecoveredIncarnation).toHaveBeenCalledWith('builder@xv');
 });

 it('default absent completion observer disables claimed continuation',async()=>{
  const {queueId}=await claimedAssignment();
  svc=new CoordinationRecoveryService(repo,s=>observations.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;
  await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  expect(db.prepare("SELECT 1 FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toBeUndefined();
 });

 it.each(['expired-holder','changed-configuration','typed-return','unknown-effect','guard-on'])('%s before staging holds without creating a continuation intent',async(change)=>{
  const {queueId}=await claimedAssignment();
  if(change==='expired-holder')db.prepare('UPDATE coordinator_authority SET lease_until=? WHERE rig_id=?').run(clock-1,'xv');
  if(change==='changed-configuration')db.prepare("UPDATE nodes SET model='changed' WHERE id='builder@xv'").run();
  if(change==='typed-return')db.prepare('UPDATE queue_items SET reply_to=? WHERE qitem_id=?').run('typed-return',queueId);
  if(change==='unknown-effect'){const outbox=new OutboxHandler(db);const row=outbox.record({senderSession:'builder@xv',destinationSession:'lead@xv',body:'fixture unknown effect'});db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(row.outboxId);}
  if(change==='guard-on')db.prepare("UPDATE seat_delivery_guards SET desired=1,effective=1 WHERE node_id='builder@xv'").run();
  if(change==='expired-holder')await expect(svc.reconcilePrepared('lead@xv','lead-g1','xv')).rejects.toThrow('Only reconciled current holder');
  else await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  expect(db.prepare("SELECT 1 FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toBeUndefined();
 });

 it('changed claim is invalidated before transport and never reclaims the original row',async()=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g2' WHERE qitem_id=?").run(queueId);
  await repo.drainPendingWakeIntents();expect(send).not.toHaveBeenCalled();expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toEqual({delivery_state:'failed'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it.each([
  ['holder epoch changes','invalid'],
  ['typed return appears','invalid'],
  ['configuration changes','invalid'],
  ['typing guard turns on','retained'],
  ['competing effect becomes unknown','retained'],
 ] as const)('%s after staging is settled without transport',async(change,expected)=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  if(change==='holder epoch changes')db.prepare("UPDATE coordinator_authority SET epoch=epoch+1 WHERE rig_id='xv'").run();
  if(change==='typed return appears')db.prepare('UPDATE queue_items SET reply_to=? WHERE qitem_id=?').run('typed-return',queueId);
  if(change==='configuration changes')db.prepare("UPDATE nodes SET model='changed' WHERE id='builder@xv'").run();
  if(change==='typing guard turns on')db.prepare("UPDATE seat_delivery_guards SET desired=1,effective=1 WHERE node_id='builder@xv'").run();
  if(change==='competing effect becomes unknown'){const outbox=new OutboxHandler(db);const row=outbox.record({senderSession:'builder@xv',destinationSession:'lead@xv',body:'fixture unknown effect'});db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(row.outboxId);}
  await repo.drainPendingWakeIntents();expect(send).not.toHaveBeenCalled();expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toEqual({delivery_state:expected==='retained'?'pending':'failed'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('keeps a transient guard hold pending and later sends the same immutable intent once',async()=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  const getIntent=()=>db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId) as {outbox_id:string;delivery_state:string};
  const original=getIntent();expect(original.delivery_state).toBe('pending');
  runtime.withRecoveredIncarnation=vi.fn(async()=>({state:'held' as const,reason:'native-busy-or-stale'}));
  await repo.drainPendingWakeIntents();expect(send).not.toHaveBeenCalled();expect(getIntent()).toEqual(original);
  runtime.withRecoveredIncarnation=vi.fn(async(_observation,guardedSend)=>({state:'performed' as const,value:await guardedSend()}));
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(getIntent()).toEqual({outbox_id:original.outbox_id,delivery_state:'delivered'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('uses a fresh native guard when the retained completion observation is older than three seconds',async()=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  clock+=4000;refresh();
  runtime.withRecoveredIncarnation=vi.fn(async(_observation,guardedSend)=>({state:'performed' as const,value:await guardedSend()}));
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toEqual({delivery_state:'delivered'});
 });

 it('holds expired admission then delivers the same immutable intent after an authorized refresh',async()=>{
  const {queueId}=await claimedAssignment();
  const plan=svc.plan('xv')!;const product=plan.tasks.find(t=>t.key==='product')!;
  product.deadline=clock+2000;product.admission.validUntil=clock+2000;
  svc.configure('operator-agent@kernel','operator-agent-g1',{...plan,revision:'short-current-admission'});
  const send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});
  await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  const intent=()=>db.prepare("SELECT outbox_id,delivery_state,tags,body FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId) as {outbox_id:string;delivery_state:string;tags:string;body:string};
  const original=intent();expect(original.delivery_state).toBe('pending');
  clock+=3000;refresh();await repo.drainPendingWakeIntents();expect(send).not.toHaveBeenCalled();expect(intent()).toEqual(original);
  const refreshed=svc.plan('xv')!,current=refreshed.tasks.find(t=>t.key==='product')!;
  current.deadline=clock+60000;current.admission={...current.admission,validUntil:clock+60000,capacityRef:'fixture/current-capacity-after-refresh'};
  svc.configure('operator-agent@kernel','operator-agent-g1',{...refreshed,revision:'renewed-current-admission'});
  refresh();await repo.drainPendingWakeIntents();await repo.drainPendingWakeIntents();
  expect(send).toHaveBeenCalledTimes(1);expect(intent()).toEqual({...original,delivery_state:'delivered'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('stages the real admission-refresh duty around its own pending continuation and then delivers that intent once',async()=>{
  const {main,queueId}=await claimedAssignment(clock+600000),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});
  const first=await svc.reconcilePrepared('lead@xv','lead-g1','xv');expect(first.some(x=>x.state==='claimed-recovery-continuation-staged'&&x.queueId===queueId)).toBe(true);
  const intent=()=>db.prepare("SELECT outbox_id,delivery_state,tags,body FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId) as {outbox_id:string;delivery_state:string;tags:string;body:string};
  const original=intent();expect(original.delivery_state).toBe('pending');
  runtime.withRecoveredIncarnation=vi.fn(async()=>({state:'held' as const,reason:'native-busy-or-stale'}));
  await repo.drainPendingWakeIntents();expect(intent()).toEqual(original);expect(send).not.toHaveBeenCalled();

  repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-before-admission-expiry');
  clock=main.admission.validUntil+1;vi.setSystemTime(clock);refresh();
  svc.reconcile('lead@xv','lead-g1','xv');
  const controls=db.prepare("SELECT operation_id,receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='product' ORDER BY rowid DESC").all() as Array<{operation_id:string;receipt:string}>;
  expect(controls).toHaveLength(1);const staged=JSON.parse(controls[0].receipt);
  const dutyBody=JSON.parse(repo.getById(controls[0].operation_id)!.body);
  expect(dutyBody.admissionRefreshContract).toMatchObject({taskKey:'product',owner:'builder@xv',ownerGeneration:staged.ownerGeneration,configurationDigest:staged.liveConfigurationDigest});
  expect(intent()).toEqual(original);expect(repo.getById(queueId)?.state).toBe('in-progress');

  repo.claim({qitemId:controls[0].operation_id,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const plan=svc.plan('xv')!,current=plan.tasks.find(t=>t.key==='product')!;
  const refreshed={...current,admission:{generation:staged.ownerGeneration,configurationDigest:staged.liveConfigurationDigest,qualificationRef:'fixture/qualification/product-after-refresh',capacityRef:'fixture/capacity/product-after-refresh',effortRef:'fixture/effort/product-after-refresh',validUntil:clock+300000}};
  svc.configure('operator-agent@kernel','operator-agent-g1',{...plan,revision:'product-admission-refreshed',tasks:plan.tasks.map(t=>t.key==='product'?refreshed:t)});
  expect((svc as any).admissionRefreshCompleted('xv',staged)).toBe(true);
  repo.update({qitemId:controls[0].operation_id,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(svc.lifecycleControlCompleted(controls[0].operation_id)).toBe(true);

  runtime.withRecoveredIncarnation=vi.fn(async(_observation,guardedSend)=>({state:'performed' as const,value:await guardedSend()}));
  await repo.drainPendingWakeIntents();await repo.drainPendingWakeIntents();
  expect(send.mock.calls.filter(([,body])=>String(body).includes('already-claimed assignment '+queueId))).toHaveLength(1);
  expect(intent()).toEqual({...original,delivery_state:'delivered'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it.each(['sending','indeterminate'] as const)('does not exempt a %s continuation from expired-admission effect debt',async(state)=>{
  const {main,queueId}=await claimedAssignment(clock+600000);await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  const row=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId) as {outbox_id:string;delivery_state:string};
  db.prepare('UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?').run(state,row.outbox_id);
  repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-before-admission-expiry-'+state);
  clock=main.admission.validUntil+1;vi.setSystemTime(clock);refresh();
  svc.reconcile('lead@xv','lead-g1','xv');
  expect(db.prepare("SELECT operation_id FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='product'").get()).toBeUndefined();
  expect(db.prepare('SELECT outbox_id,delivery_state FROM outbox_entries WHERE outbox_id=?').get(row.outbox_id)).toEqual({outbox_id:row.outbox_id,delivery_state:state});
 });

 it('keeps unrelated pending worker-effect debt blocking the admission-refresh duty',async()=>{
  const {main,queueId}=await claimedAssignment(clock+600000);await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  const continuation=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId) as {outbox_id:string;delivery_state:string};
  db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('unrelated-pending-debt','watchdog@system','builder@xv','unrelated pending effect',?,'pending')").run(new Date(clock).toISOString());
  repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-before-admission-expiry-unrelated');
  clock=main.admission.validUntil+1;vi.setSystemTime(clock);refresh();
  svc.reconcile('lead@xv','lead-g1','xv');
  expect(db.prepare("SELECT operation_id FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='product'").get()).toBeUndefined();
  const pending=db.prepare('SELECT outbox_id,delivery_state FROM outbox_entries WHERE outbox_id IN (?,?)').all(continuation.outbox_id,'unrelated-pending-debt') as Array<{outbox_id:string;delivery_state:string}>;
  expect(pending).toHaveLength(2);expect(pending).toContainEqual({outbox_id:continuation.outbox_id,delivery_state:'pending'});expect(pending).toContainEqual({outbox_id:'unrelated-pending-debt',delivery_state:'pending'});
 });

 it('holds an expired same-holder lease and resumes only after that authority is active again',async()=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});
  await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  const read=()=>db.prepare("SELECT outbox_id,delivery_state,tags FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId);
  const original=read();db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock-1);
  await repo.drainPendingWakeIntents();expect(send).not.toHaveBeenCalled();expect(read()).toEqual(original);
  // Simulate the supported owner's restored lease; the transport grants none.
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock+60000);
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);
 });

 it('unrelated plan revision does not invalidate an unchanged claimed task',async()=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});
  await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'unrelated-plan-revision'});
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);
 });

 it('binds retained session history by exact row and current occupant instead of requiring a unique historical name',async()=>{
  const {queueId}=await claimedAssignment();
  db.prepare("INSERT INTO sessions (id,node_id,session_name,status) VALUES ('builder-history-2','builder@xv','builder@xv','stopped'),('builder-history-3','builder@xv','builder@xv','stopped')").run();
  const sent=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send:sent});
  const result=await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  expect(result.some(x=>x.state==='claimed-recovery-continuation-staged'&&x.queueId===queueId)).toBe(true);
  await repo.drainPendingWakeIntents();expect(sent).toHaveBeenCalledTimes(1);
 });

 it.each(['claim','authority','return'] as const)('revalidates %s after asynchronous native observation before claiming or sending',async(change)=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  const original=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId) as {outbox_id:string;delivery_state:string};
  runtime.withRecoveredIncarnation=vi.fn(async(_observation,guardedSend)=>{
    if(change==='claim')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g2' WHERE qitem_id=?").run(queueId);
    if(change==='authority')db.prepare("UPDATE coordinator_authority SET epoch=epoch+1 WHERE rig_id='xv'").run();
    if(change==='return')db.prepare('UPDATE queue_items SET reply_to=? WHERE qitem_id=?').run('typed-return',queueId);
    return {state:'performed' as const,value:await guardedSend()};
  });
  await repo.drainPendingWakeIntents();expect(send).not.toHaveBeenCalled();expect(db.prepare('SELECT outbox_id,delivery_state FROM outbox_entries WHERE outbox_id=?').get(original.outbox_id)).toEqual({outbox_id:original.outbox_id,delivery_state:'failed'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('allows only one concurrent drain to claim and send the immutable intent',async()=>{
  const {queueId}=await claimedAssignment();let arrivals=0,release!:()=>void;const bothGuards=new Promise<void>(resolve=>{release=resolve;});
  const sendStarted=vi.fn();let releaseTransport!:()=>void;const transportGate=new Promise<void>(resolve=>{releaseTransport=resolve;});
  repo.attachTransport({send:async()=>{sendStarted();await transportGate;return {ok:true,verified:true};}});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  runtime.withRecoveredIncarnation=vi.fn(async(_observation,guardedSend)=>{arrivals++;if(arrivals===2)release();await bothGuards;return {state:'performed' as const,value:await guardedSend()};});
  const first=repo.drainPendingWakeIntents(),second=repo.drainPendingWakeIntents();
  await vi.waitFor(()=>expect(arrivals).toBe(2));await vi.waitFor(()=>expect(sendStarted).toHaveBeenCalledTimes(1));releaseTransport();await Promise.all([first,second]);
  expect(sendStarted).toHaveBeenCalledTimes(1);expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toEqual({delivery_state:'delivered'});expect(repo.getById(queueId)?.state).toBe('in-progress');
 });

 it('a throwing final guard after transport is recorded UNKNOWN and never replayed',async()=>{
  const {queueId}=await claimedAssignment(),send=vi.fn(async()=>({ok:true,verified:true}));repo.attachTransport({send});await svc.reconcilePrepared('lead@xv','lead-g1','xv');
  runtime.withRecoveredIncarnation=vi.fn(async(_observation,guardedSend)=>{await guardedSend();throw new Error('guard completion uncertain');});
  await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE audit_pointer=? AND tags LIKE '%queue:claimed-native-recovery-continuation%'").get(queueId)).toEqual({delivery_state:'indeterminate'});await repo.drainPendingWakeIntents();expect(send).toHaveBeenCalledTimes(1);
 });
});
