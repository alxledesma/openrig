import {makeCoordinatorContinuityPolicy} from '../src/domain/policies/coordinator-continuity.js';
import type {RuntimeAvailability} from '../src/domain/coordinator-runtime-availability.js';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {resolve} from 'node:path';
import { describe,it,expect,beforeEach,afterEach,vi } from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,LIFECYCLE_INTAKE_RENEWAL_REASONS,coordinationIdle,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {SeatActivityService} from '../src/domain/seat-activity-service.js';
import {RuntimeOutcomeAssessment} from '../src/domain/runtime-outcome-assessment.js';
import {digest} from '../src/domain/coordinator-authority-service.js';import {seed,token} from './helpers/coordinator-fixture.js';
describe('durable coordination recovery',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 /** Captures a typed refusal. CoordinatorFenceError exposes `code` as a property, so a code
 * assertion cannot be expressed with toThrow(string), which matches only the message. */
 function refusal(fn:()=>unknown):{code?:string;message:string}{try{fn();}catch(e){return {code:(e as {code?:string}).code,message:(e as Error).message};}throw new Error('expected a typed refusal, but the call succeeded');}
 const plan=(tasks:CoordinationTask[],overrides:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,...overrides,tasks});
function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
  function configure(tasks:CoordinationTask[],resources:Record<string,string[]>={},planOverrides:Partial<CoordinationPlan>={}){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:resources[t.key]??[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,planOverrides));}
 function refresh(){for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));}
 function job(){db.prepare(`INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')`).run(new Date(clock).toISOString());}
 it('scope-only attachment preserves expired admissions without granting dispatch or replaying releases',()=>{
  const tasks=[task('scope-main'),task('scope-recovery','architect@xv',{recoveryFor:'scope-main'})];
  const original=configure(tasks);clock+=70000;vi.setSystemTime(clock);
  const before=db.prepare('SELECT * FROM outbox_entries').all();
  const next={...original,revision:'scope-connect',scopeSources:[{ref:'mission.md',digest:'a'.repeat(64)}],frontierPlanning:{stabilizationObservations:2}};
  expect(svc.configure('operator-agent@kernel','operator-agent-g1',next).tasks).toEqual(original.tasks);
  expect((svc as any).admittedNow(next.tasks[0])).toBe(false);
  expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(before);
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...next,revision:'changed-deadline',tasks:next.tasks.map(t=>({...t,deadline:clock+20000}))})).toThrow('Exact current');
 });
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'coordination-'));db=createDb(join(dir,'db'));seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;});
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});
 const normal=()=>[task('product'),task('repair','architect@xv',{recoveryFor:'product'})];
 async function finishTyped(packageKey:string,owner:string,queueId:string,returnId:string){
  repo.claim({qitemId:queueId,destinationSession:owner,identityProvenance:'transport:v1'});repo.update({qitemId:queueId,actorSession:owner,state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:returnId,sourceSession:owner,destinationSession:'lead@xv',body:JSON.stringify({packageKey,inputDigest:digest(packageKey),evidence:[{kind:'report',ref:'actual/'+packageKey+'.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose(owner,repo.coordinatorAuthority.generation(owner)!,'xv',packageKey,returnId);db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
 }
 it('central lifecycle follows an outside-plan typed return through genuine acceptance and actual next worker pickup',async()=>{
  configure([task('next','reviewer@xv',{predecessors:[{queueId:'outside-product',dispositionId:'outside-return'}]}),task('next-repair','architect@xv',{recoveryFor:'next'})]);
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','outside',{inputDigest:digest('outside'),destination:'builder@xv',bodyHash:digest('outside'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  await repo.create({qitemId:'outside-product',sourceSession:'lead@xv',destinationSession:'builder@xv',body:'outside',dispatch:{token,packageKey:'outside'},nudge:false});await finishTyped('outside','builder@xv','outside-product','outside-return');
  const first=svc.reconcile('lead@xv','lead-g1','xv'),duty=first.find(r=>r.key==='acceptance:outside')!;expect(duty.state).toBe('pending-native-acceptance');expect(first.find(r=>r.key==='next')?.reason).toBe('predecessor-disposition');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:outside')?.queueId).toBe(duty.queueId);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control'").get()).toEqual({n:1});
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','lead@xv',duty.queueId)).not.toThrow();repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  expect(()=>repo.update({qitemId:duty.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow('Exact native acceptance');
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();svc.accept('lead@xv','lead-g1','xv','outside','outside-return','actual/outside-acceptance.md');
  expect(repo.getById(duty.queueId!)?.state).toBe('done');
  const next=db.prepare("SELECT queue_id FROM coordinator_assignments WHERE package_key='next'").get() as any;expect(next).toBeTruthy();repo.claim({qitemId:next.queue_id,destinationSession:'reviewer@xv',identityProvenance:'transport:v1'});expect(repo.getById(next.queue_id)?.state).toBe('in-progress');
 });
 it('central lifecycle requires distinct genuinely owned recovery for classifier-incomplete return',async()=>{
  configure(normal());const original=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',original,'incomplete-return');
  const assessment=new RuntimeOutcomeAssessment(repo,{},()=>clock);repo.coordinatorAuthority.runtimeOutcomeAssessment=assessment;
  const policy={rigId:'xv',revision:'fixture-enforce',mode:'enforce'};db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv','fixture-policy','runtime-outcome-policy',JSON.stringify(policy),'fixture');
  db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv','fixture-incomplete','runtime-outcome-recovery',JSON.stringify({packageKey:'product',dispositionId:'incomplete-return',policyRevision:policy.revision}),'fixture');
  const results=svc.reconcile('lead@xv','lead-g1','xv'),duty=results.find(r=>r.key==='acceptance:product')!,repair=results.find(r=>r.key==='repair')!;db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();expect(svc.dutyFacts(duty.queueId!)).toMatchObject({claim:true});repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  expect(()=>svc.accept('lead@xv','lead-g1','xv','product','incomplete-return','prose.md')).toThrow('Incomplete/unverified');expect(()=>repo.update({qitemId:duty.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow('Exact native acceptance');
  const input={rigId:'xv',dutyQueueId:duty.queueId!,recoveryPackageKey:'repair',recoveryQueueId:repair.queueId!,evidenceRef:'actual/recovery-pickup.json'};
  expect(()=>svc.recordLifecycleRecovery('lead@xv','lead-g1',input)).toThrow('genuinely active recovery');repo.claim({qitemId:repair.queueId!,destinationSession:'architect@xv',identityProvenance:'transport:v1'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();svc.recordLifecycleRecovery('lead@xv','lead-g1',input);
  repo.update({qitemId:duty.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});expect(repo.getById(repair.queueId!)?.state).toBe('in-progress');expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();
 });
 it.each(['failed','denied','canceled'] as const)('central lifecycle keeps outside-plan disposed %s accountable through genuine recovery-only ownership',async(state)=>{
  const retained=configure([task('independent','reviewer@xv',{boundary:'owner-material'}),task('independent-repair','peer@xv',{recoveryFor:'independent'})]);
  const original=task('outside'),repair=task('outside-repair','architect@xv',{recoveryFor:'outside'});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','outside',{inputDigest:digest('outside'),destination:original.owner,bodyHash:digest(original.body),resources:['failed-source.ts'],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  await repo.create({qitemId:'outside-failure',sourceSession:'lead@xv',destinationSession:original.owner,body:original.body,dispatch:{token,packageKey:original.packageKey},nudge:false});repo.claim({qitemId:'outside-failure',destinationSession:original.owner,identityProvenance:'transport:v1'});repo.update({qitemId:'outside-failure',actorSession:original.owner,state});
  await repo.create({qitemId:'outside-failure-return',sourceSession:original.owner,destinationSession:'lead@xv',body:JSON.stringify({packageKey:original.packageKey,inputDigest:digest('outside'),evidence:[{kind:'report',ref:'actual/failed-report.md'}]}),nudge:false});repo.coordinatorAuthority.dispose(original.owner,'builder-g1','xv',original.packageKey,'outside-failure-return');
  const first=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='recovery:outside')!;expect(first.state).toBe('pending-native-recovery');expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='recovery:outside')?.queueId).toBe(first.queueId);
  const packet=JSON.parse(repo.getById(first.queueId!)!.body);expect(packet.action).toBe('own-exact-failed-return-recovery');expect(packet.acceptContract).toBeUndefined();expect(packet.terminalState).toBe(state);expect(packet.originalQueueId).toBe('outside-failure');expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','lead@xv',first.queueId)).not.toThrow();
  repo.claim({qitemId:first.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});expect(()=>repo.update({qitemId:first.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow('Exact native acceptance');expect(()=>svc.accept('lead@xv','lead-g1','xv',original.packageKey,'outside-failure-return','prose.md')).toThrow('Exact successful');
  repo.update({qitemId:first.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'canceled'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',repair.packageKey,{inputDigest:digest(repair.key),destination:repair.owner,bodyHash:digest(repair.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  svc.configure('operator-agent@kernel','operator-agent-g1',{...retained,revision:'actual-failure-recovery-r2',tasks:[...retained.tasks,original,repair]});
  const ready=svc.reconcile('lead@xv','lead-g1','xv'),duty=ready.find(r=>r.key==='recovery:outside')!,assignment=ready.find(r=>r.key===repair.key)!;expect(duty.queueId).not.toBe(first.queueId);expect(assignment.state).toBe('pending-pickup');db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const input={rigId:'xv',dutyQueueId:duty.queueId!,recoveryPackageKey:repair.packageKey,recoveryQueueId:assignment.queueId!,evidenceRef:'actual/failed-recovery-custody.json'};expect(()=>svc.recordLifecycleRecovery('lead@xv','lead-g1',input)).toThrow('genuinely active recovery');repo.claim({qitemId:assignment.queueId!,destinationSession:repair.owner,identityProvenance:'transport:v1'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();svc.recordLifecycleRecovery('lead@xv','lead-g1',input);repo.update({qitemId:duty.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='recovery:outside')).toMatchObject({state:'owned-recovery',queueId:duty.queueId});expect(repo.getById('outside-failure')?.state).toBe(state);expect(repo.getById(assignment.queueId!)?.state).toBe('in-progress');expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE queue_id='outside-failure'").get()).toEqual({disposition_id:'outside-failure-return'});expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();expect(db.prepare("SELECT 1 FROM coordinator_resources WHERE package_key='outside'").get()).toBeUndefined();
 });
 it.each(['expiry','generation','epoch','plan'] as const)('central lifecycle delivery rejects %s without granting acceptance',async(kind)=>{
  configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',q,'guarded-return');const duty=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;
  if(kind==='expiry'){clock=duty.deadline+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+60000);}else if(kind==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='lead-g2' WHERE node_id='lead@xv'").run();else if(kind==='epoch')db.prepare('UPDATE coordinator_authority SET epoch=epoch+1').run();else svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'guarded-plan-r2'});
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','lead@xv',duty.queueId)).toThrow('finite lifecycle duty');expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();
 });
 it('central lifecycle records a genuine return duty for a zero-resource completed assignment',()=>{
  configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;repo.claim({qitemId:q,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:q,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const r=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')!;expect(r.state).toBe('pending-native-terminal-return');expect(JSON.parse(repo.getById(r.queueId!)!.body).originalQueueId).toBe(q);expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE queue_id=?").get(q)).toEqual({disposition_id:null});expect(db.prepare('SELECT count(*) n FROM coordinator_resources').get()).toEqual({n:0});
 });
 it('central lifecycle materializes a current admitted frontier while preserving exact accepted parent and dormant backup history',async()=>{
  const historical=configure(normal()),q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',q,'historical-return');svc.accept('lead@xv','lead-g1','xv','product','historical-return','actual/accepted-parent.md');
  clock+=60001;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+60000);
  const next=task('new-frontier','reviewer@xv',{predecessors:[{queueId:q,dispositionId:'historical-return'}]}),backup=task('z-new-backup','peer@xv',{recoveryFor:next.key});
  for(const t of [next,backup])repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  const observed=svc.reconcile('lead@xv','lead-g1','xv'),intake=observed.find(r=>r.key==='materialization:new-frontier')!;expect(intake.state).toBe('pending-native-materialization');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='materialization:new-frontier')?.queueId).toBe(intake.queueId);const packet=JSON.parse(repo.getById(intake.queueId!)!.body);expect(packet.acceptedPredecessors).toContainEqual({queueId:q,dispositionId:'historical-return',evidenceRef:'actual/accepted-parent.md'});
  // The newly accountable backup hold creates a real notice; it must settle before pickup.
  expect(svc.dutyFacts(intake.queueId!).claim).toBe(false);
  repo.attachTransport({send:async(session,_text,opts)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,session,opts?.queueAssignmentId);return {ok:true,verified:true};}});
  await svc.deliverCommitted();
  expect(svc.dutyFacts(intake.queueId!).claim).toBe(true);
  repo.claim({qitemId:intake.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});expect(()=>repo.update({qitemId:intake.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow('Exact native acceptance');
  const proposal={...historical,revision:'materialized-r2',tasks:[...historical.tasks,next,backup]};
  for(const patch of [{body:'rewritten'},{admission:{...historical.tasks[0].admission,validUntil:clock+60000}},{predecessors:[{queueId:q,dispositionId:'changed'}]},{deadline:clock+20000}])expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...proposal,tasks:[{...historical.tasks[0],...patch},historical.tasks[1],next,backup]})).toThrow('history must retain');
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...proposal,tasks:[...historical.tasks,{...next,admission:{...next.admission,validUntil:clock-1}},backup]})).toThrow('current generation/configuration');
  svc.configure('operator-agent@kernel','operator-agent-g1',proposal);expect(svc.plan('xv')!.tasks.slice(0,2)).toEqual(historical.tasks);repo.update({qitemId:intake.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();const ready=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key===next.key)!;expect(ready.state).toBe('pending-pickup');repo.claim({qitemId:ready.queueId!,destinationSession:next.owner,identityProvenance:'transport:v1'});
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key IN ('product','repair')").get()).toEqual({n:1});expect(repo.getById(q)?.state).toBe('done');
 });
 it('central lifecycle history exception rejects unaccepted parents and active backup scope',async()=>{
  const old=configure(normal()),q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',q,'not-yet-accepted');
  clock+=60001;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+60000);
  // AMENDMENT1 (1a) the parent is UNACCEPTED, so a CHANGED stale copy still gets no history
  // exception: the admission gate, not byte retention, is what refuses it.
  const changedUnacceptedParent={...old,revision:'unaccepted-changed',tasks:old.tasks.map(t=>t.packageKey==='product'?{...t,deadline:t.deadline+1}:t)};
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',changedUnacceptedParent)).toThrow('current generation/configuration');
  // AMENDMENT1 (1b) the byte-identical copy is now the intended retained contract: kept with
  // its tasks unchanged, and still inert - no new queue, assignment or outbox row.
  const qBefore=db.prepare("SELECT count(*) n FROM queue_items").get(),aBefore=db.prepare("SELECT count(*) n FROM coordinator_assignments").get(),oBefore=db.prepare("SELECT count(*) n FROM outbox_entries").get();
  const retained=svc.configure('operator-agent@kernel','operator-agent-g1',{...old,revision:'unaccepted-identical'});
  expect(retained.tasks).toEqual(old.tasks);
  // The RETENTION itself adds no queue, assignment or outbox row. The acceptance-duty queue row
  // staged afterwards comes from the following reconcile, this scenario's normal mechanism.
  expect(db.prepare("SELECT count(*) n FROM queue_items").get()).toEqual(qBefore);
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual(aBefore);
  expect(db.prepare("SELECT count(*) n FROM outbox_entries").get()).toEqual(oBefore);
  const afterRetain=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='product')!;
  expect(afterRetain).toMatchObject({state:'returned-awaiting-acceptance'});
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual(aBefore);
  // The outbox count is NOT asserted here: reconcile stages the acceptance duty's own wake,
  // which is this scenario's normal mechanism and not retention activity.
  svc.accept('lead@xv','lead-g1','xv','product','not-yet-accepted','actual/accepted.md');
  db.prepare("INSERT INTO coordinator_resources VALUES ('xv','retained-backup-resource','repair')").run();
  // AMENDMENT1 (2a) the parent is now ACCEPTED, and accepted history is immutable: a CHANGED
  // stale copy is refused as a rewrite, again not merely as stale admission.
  const changedAcceptedParent={...old,revision:'accepted-changed',tasks:old.tasks.map(t=>t.packageKey==='product'?{...t,deadline:t.deadline+1}:t)};
  const rewriteRefusal=refusal(()=>svc.configure('operator-agent@kernel','operator-agent-g1',changedAcceptedParent));
  expect(rewriteRefusal.code).toBe('coordination_history_rewrite_refused');
  expect(rewriteRefusal.message).toBe('Accepted task and dormant backup history must retain full task, admission and deadline bytes');
  // AMENDMENT1 (2b) the live coordinator_resources trace on the backup denies dormant-history
  // status, so a CHANGED stale backup copy is still refused on generation/configuration.
  const changedBackup={...old,revision:'active-backup-changed',tasks:old.tasks.map(t=>t.packageKey==='repair'?{...t,deadline:t.deadline+1}:t)};
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',changedBackup)).toThrow('current generation/configuration');
  // AMENDMENT1 (2c) byte-identical retention still succeeds, and the live resource row is not
  // modified or released by the retention.
  const resourceBefore=db.prepare("SELECT * FROM coordinator_resources").all();
  const retainedBackup=svc.configure('operator-agent@kernel','operator-agent-g1',{...old,revision:'active-backup-identical'});
  expect(retainedBackup.tasks).toEqual(old.tasks);
  expect(db.prepare("SELECT * FROM coordinator_resources").all()).toEqual(resourceBefore);
 });
 it.each([['pending','done'],['indeterminate','done'],['indeterminate','failed']] as const)('first lifecycle protected by %s debt after %s routes one intake and preserves uncertainty',async(state,terminal)=>{
 configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',q,'first-protected-return');
 if(terminal==='failed')db.prepare("UPDATE queue_items SET state='failed' WHERE qitem_id=?").run(q);
 db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('first-debt','watchdog@system','lead@xv','unresolved',?,?)").run(new Date(clock).toISOString(),state);
 const debt=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='first-debt'").get();
 const held=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key===(terminal==='done'?'acceptance:product':'recovery:product'))!;
 expect(held).toMatchObject({state:'held',reason:'lifecycle-recipient-protected',subject:{packageKey:'product',owner:'lead@xv'}});expect(held.queueId).toBeUndefined();
 const intakes=()=>db.prepare("SELECT body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.reason')='lifecycle-recipient-protected'").all() as {body:string}[];
 expect(intakes()).toHaveLength(1);expect(JSON.parse(intakes()[0].body)).toMatchObject({packageKey:'product',recipientGeneration:'operator-agent-g1',grantsAuthority:false});
 svc.reconcile('lead@xv','lead-g1','xv');expect(intakes()).toHaveLength(1);expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='first-debt'").get()).toEqual(debt);expect(repo.getById(q)?.state).toBe(terminal);
 });
 it('first return-contract drift routes an accountable intake without changing original custody',async()=>{
 configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',q,'drift-return');
 db.prepare("UPDATE queue_items SET body='drifted' WHERE qitem_id='drift-return'").run();const before=repo.getById(q);
 const held=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;
 expect(held).toMatchObject({state:'held',queueId:q,reason:'lifecycle-return-contract-drift',subject:{packageKey:'product',owner:'lead@xv'}});
 const intakes=()=>db.prepare("SELECT body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.reason')='lifecycle-return-contract-drift'").all();
 expect(intakes()).toHaveLength(1);svc.reconcile('lead@xv','lead-g1','xv');expect(intakes()).toHaveLength(1);expect(repo.getById(q)).toEqual(before);
 });
 it('shared duty facets plan revision cannot duplicate live acceptance and expired custody requires native retirement',async()=>{configure(normal());const product=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',product,'shared-return');const first=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'same-subject-new-plan'});expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')?.queueId).toBe(first.queueId);repo.claim({qitemId:first.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});clock=first.deadline+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);const later=svc.reconcile('lead@xv','lead-g1','xv');expect(later.find(r=>r.key==='acceptance:product')).toMatchObject({state:'held',queueId:first.queueId,reason:'lifecycle-duty-exhausted'});expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement'").get()).toMatchObject({n:1});expect(repo.getById(first.queueId!)?.state).toBe('in-progress');});
 it('expired unclaimed acceptance with an UNKNOWN wake stages failure-only retirement, routes accountable intake and contains the notice only by receipt',async()=>{
  configure(normal());const product=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',product,'unknown-wake-return');const duty=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!.queueId!;const originalExpiry=repo.getById(duty)!.expiresAt;
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+duty);const unknown=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+duty) as any;clock=Date.parse(repo.getById(duty)!.expiresAt!)+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);
  const held=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;expect(held).toMatchObject({state:'held',queueId:duty,reason:'lifecycle-recipient-protected'});expect(repo.getById(duty)?.claimedAt).toBeNull();
  const retirement=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(duty) as {receipt:string}).receipt);expect(retirement).toMatchObject({targetQueueId:duty,recipient:'lead@xv',recipientGeneration:'lead-g1',targetBodyHash:digest(repo.getById(duty)!.body),planRevision:'r1'});expect(repo.getById(retirement.queueId)?.state).toBe('pending');
  const intakes=()=>db.prepare("SELECT body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.reason')='lifecycle-recipient-protected'").all() as any[];expect(intakes()).toHaveLength(1);expect(JSON.parse(intakes()[0].body)).toMatchObject({packageKey:'product',retainedQueueId:duty,recipientGeneration:'operator-agent-g1',grantsAuthority:false});
  expect(svc.validLifecycleControlWake('watchdog@system','lead@xv',retirement.queueId)).toBe(true);expect(()=>repo.claim({qitemId:retirement.queueId,destinationSession:'lead@xv',identityProvenance:'transport:v1'})).toThrow();
  repo.attachTransport({send:async(_session:string,_text:string,opts?:any)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,_session,opts?.queueAssignmentId);return {ok:true,verified:true};}});await svc.deliverCommitted();expect(['delivered','failed']).toContain((db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+retirement.queueId) as {delivery_state:string}).delivery_state);expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+duty)).toEqual({delivery_state:'indeterminate'});
  repo.claim({qitemId:retirement.queueId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});repo.update({qitemId:duty,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'actual own expired duty outcome'});repo.update({qitemId:retirement.queueId,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});expect(repo.getById(duty)?.claimedAt).toBeNull();
  const next=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;expect(next.state).toBe('pending-native-acceptance');const successor=next.queueId as string;expect(successor).toBeTruthy();expect(successor).not.toBe(duty);expect(svc.lifecycleControlReceipt(successor)).toMatchObject({kind:'acceptance',previousQueueId:duty,recipient:'lead@xv'});
  expect(db.prepare("SELECT delivery_state,body FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+duty)).toEqual({delivery_state:'indeterminate',body:unknown.body});
  const contained=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='held-history-control-outcome' AND json_extract(receipt,'$.outboxId')=?").get('wake-intent-'+duty) as {receipt:string}).receipt);expect(contained).toMatchObject({outboxId:'wake-intent-'+duty,queueId:duty,deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true});
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement'").get()).toMatchObject({n:1});expect(intakes()).toHaveLength(1);expect(repo.getById(duty)?.expiresAt).toBe(originalExpiry);
 });
 it('retirement duty carries the current plan revision and issue provenance so its failure-only wake is actually sendable',async()=>{
  configure(normal());const product=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',product,'revision-return');const first=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!.queueId!;expect(svc.lifecycleControlReceipt(first)).toMatchObject({planRevision:'r1'});
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'post-issue-r2'});repo.claim({qitemId:first,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run('wake-intent-'+first);
  clock=Date.parse(repo.getById(first)!.expiresAt!)+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')).toMatchObject({state:'held',queueId:first,reason:'lifecycle-duty-exhausted'});
  const retirement=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(first) as {receipt:string}).receipt);expect(retirement).toMatchObject({planRevision:'post-issue-r2',issuedAt:clock,holder:'lead@xv',holderGeneration:'lead-g1',epoch:1,operatorGeneration:'operator-agent-g1',deadline:clock+1200000});expect(svc.dutyFacts(retirement.queueId)).toMatchObject({claim:true,send:true});expect(svc.validLifecycleControlWake('watchdog@system','lead@xv',retirement.queueId)).toBe(true);
  expect(svc.lifecycleControlReceipt(retirement.queueId)!.targetQueueId).toBe(first);expect(repo.getById(first)?.claimedAt).toBeTruthy();expect(repo.getById(first)?.expiresAt).toBe(new Date(svc.lifecycleControlReceipt(first)!.deadline).toISOString());
 });
 it('a completed retirement notice is seat debt while in flight and only stops counting through its own recipient receipt',async()=>{
  configure(normal());const product=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',product,'retirement-debt-return');const first=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!.queueId!;repo.claim({qitemId:first,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run('wake-intent-'+first);
  clock=Date.parse(repo.getById(first)!.expiresAt!)+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);svc.reconcile('lead@xv','lead-g1','xv');const retirement=(db.prepare("SELECT json_extract(receipt,'$.queueId') id FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(first) as {id:string}).id;
  repo.update({qitemId:first,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'failed'});repo.claim({qitemId:retirement,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});repo.update({qitemId:retirement,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});expect(svc.dutyFacts(retirement).complete).toBe(true);
  db.prepare("UPDATE outbox_entries SET delivery_state='sending' WHERE outbox_id=?").run('wake-intent-'+retirement);expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')).toMatchObject({state:'held',reason:'lifecycle-recipient-protected'});
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+retirement);const issued=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;expect(issued.state).toBe('pending-native-acceptance');expect(svc.lifecycleControlReceipt(issued.queueId!)).toMatchObject({kind:'acceptance',previousQueueId:first});
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+retirement)).toEqual({delivery_state:'indeterminate'});expect(JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='held-history-control-outcome' AND json_extract(receipt,'$.outboxId')=?").get('wake-intent-'+retirement) as {receipt:string}).receipt)).toMatchObject({queueId:retirement,deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true});
 });
 it('each expired unclaimed acceptance link produces exactly one accountable intake and one fresh successor',async()=>{
  configure(normal());const product=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',product,'unresponsive-return');const first=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!.queueId!;db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run('wake-intent-'+first);
  const expiredDeadline=Date.parse(repo.getById(first)!.expiresAt!);clock=expiredDeadline+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);
  const held=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;expect(held).toMatchObject({state:'held',queueId:first,reason:'lifecycle-duty-expired-unclaimed'});const successor=held.activityEvidence!.successorQueueId as string;expect(successor).toBeTruthy();expect(successor).not.toBe(first);expect(svc.lifecycleControlReceipt(successor)).toMatchObject({kind:'acceptance',previousQueueId:first});expect(repo.getById(first)?.expiresAt).toBe(new Date(expiredDeadline).toISOString());
  const intakes=()=>db.prepare("SELECT body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.reason')='lifecycle-duty-expired-unclaimed'").all() as any[];expect(intakes()).toHaveLength(1);expect(JSON.parse(intakes()[0].body)).toMatchObject({packageKey:'product',retainedQueueId:first,recipientGeneration:'operator-agent-g1',grantsAuthority:false});
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')?.queueId).toBe(successor);expect(intakes()).toHaveLength(1);expect(db.prepare("SELECT count(*) n FROM queue_items WHERE destination_session='lead@xv' AND state='pending' AND expires_at>? AND qitem_id LIKE 'qitem-coordination-lifecycle-%'").get(new Date(clock).toISOString())).toMatchObject({n:1});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run('wake-intent-'+successor);clock=Date.parse(repo.getById(successor)!.expiresAt!)+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);
  const second=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;expect(second).toMatchObject({state:'held',queueId:successor,reason:'lifecycle-duty-expired-unclaimed'});expect(second.activityEvidence!.successorQueueId).not.toBe(successor);expect(intakes()).toHaveLength(2);expect(db.prepare("SELECT count(*) n FROM queue_items WHERE destination_session='lead@xv' AND state='pending' AND expires_at>? AND qitem_id LIKE 'qitem-coordination-lifecycle-%'").get(new Date(clock).toISOString())).toMatchObject({n:1});expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement'").get()).toMatchObject({n:0});
 });
 it('uses fresh deciding window evidence despite stale hook; queued is not pickup and repetition cannot duplicate',()=>{
  configure(normal());const a=svc.reconcile('lead@xv','lead-g1','xv');expect(a[0].state).toBe('pending-pickup');expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('pending-pickup');
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual({n:1});
  repo.claim({qitemId:a[0].queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('picked-up');
 });
 it('freshness and current identity are not fabricated from display or wrong generation',()=>{
  const s=sample('builder@xv');expect(coordinationIdle(s,s.generation,clock)).toBe(true);
  for(const bad of [{...s,identityVerified:false},{...s,generation:'old'},{...s,witness:null},{...s,witness:{...s.witness!,observedAt:new Date(clock-3001).toISOString()}},{...s,witness:{...s.witness!,observedAt:new Date(clock+1).toISOString()}},{...s,state:{...s.state,decidedBy:'lifecycle-hooks' as const}},{...s,state:{...s.state,needsInput:{count:1,reason:'input'}}}])expect(coordinationIdle(bad,s.generation,clock)).toBe(false);
 });
 it('observer records the exact activity hold instead of hiding reconciliation behind outage status',async()=>{
  configure(normal());job();const s=samples.get('builder@xv')!;
  s.identityVerified=false;
  s.witness!.observedAt=new Date(clock-3001).toISOString();
  const evaluation=await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  expect(evaluation).toMatchObject({action:'skip',reason:'coordination-reconciled'});
  const held=(evaluation.notes!.coordination as any[]).find(r=>r.key==='product');
  expect(held).toMatchObject({reason:'fresh-activity-required',activityEvidence:{identityVerified:false,generation:'builder-g1',expectedGeneration:'builder-g1',activity:'idle-at-prompt',witnessAgeMs:3001,witnessRung:'window-sampling'}});
  expect(repo.getById('qitem-coordination-'+digest('xv:product').slice(0,24))).toBeNull();
  const before=db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-reconcile'").get();
  s.witness!.observedAt=new Date(clock-4000).toISOString();
  await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-reconcile'").get()).toEqual(before);
 });
 it.each(['idle','busy','stale-witness','unknown','generation','configuration','plan','unavailable-unrelated','unavailable-target'] as const)('automatic continuity refreshes stale activity after exact A acceptance: %s',async outcome=>{
  const tasks=[task('product'),task('repair','architect@xv',{recoveryFor:'product'}),task('next','reviewer@xv'),task('next-repair','architect@xv',{recoveryFor:'next'})];
  for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  const parent=db.prepare("SELECT contract_hash FROM coordinator_packages WHERE rig_id='xv' AND package_key='product'").get() as {contract_hash:string};
  const parentQueue='qitem-coordination-'+digest('xv:product').slice(0,24);
  tasks[2].predecessors=[{packageKey:'product',contractHash:parent.contract_hash,queueId:parentQueue}];
  const initial=configure(tasks,{}, {refreshDispatchIdentity:true});job();
  const a=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='product')!.queueId!;
  await finishTyped('product','builder@xv',a,'accepted-A-return');
  const duty=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!.queueId!;
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  repo.claim({qitemId:duty,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const old=new Date(clock-3001).toISOString(),reviewer=samples.get('reviewer@xv')!;
  samples.set('reviewer@xv',{...reviewer,witness:{...reviewer.witness!,observedAt:old}});
  svc.accept('lead@xv','lead-g1','xv','product','accepted-A-return','actual/A-accepted.md');
  repo.update({qitemId:duty,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='next'").get()).toBeUndefined();
  if(outcome==='unknown'){
   repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','unknown-effect',{inputDigest:digest('unknown-effect'),destination:'reviewer@xv',bodyHash:digest('preserve UNKNOWN'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
   await repo.create({qitemId:'unresolved-reviewer-effect',sourceSession:'lead@xv',destinationSession:'reviewer@xv',body:'preserve UNKNOWN',dispatch:{token,packageKey:'unknown-effect'},nudge:false});
   repo.claim({qitemId:'unresolved-reviewer-effect',destinationSession:'reviewer@xv',actorGeneration:'reviewer-g1',identityProvenance:'transport:v1'});
   repo.update({qitemId:'unresolved-reviewer-effect',actorSession:'reviewer@xv',actorGeneration:'reviewer-g1',identityProvenance:'transport:v1',state:'failed'});
   db.transaction(()=>repo.stageWakeIntent('unresolved-reviewer-effect','lead@xv','reviewer@xv','transport:v1',true,'reviewer-g1'))();
   db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE audit_pointer='unresolved-reviewer-effect'").run();
  }
  const unknownBefore=db.prepare("SELECT * FROM outbox_entries WHERE delivery_state='indeterminate'").all();
  let identityPending=0,resolveIdentity!:()=>void;const polled:string[]=[];
  let identityGate=new Promise<void>(resolve=>{resolveIdentity=resolve;});
  const identity=vi.fn(async(sessions:readonly string[])=>{identityPending++;await identityGate;identityPending--;for(const session of sessions){const current=samples.get(session);if(current)samples.set(session,{...current,identityObservedAt:new Date(clock).toISOString()});}});
  const poll=vi.fn(async(session:string)=>{
   expect(identityPending).toBeGreaterThan(0);polled.push(session);
   if((outcome==='unavailable-unrelated'&&session==='architect@xv')||(outcome==='unavailable-target'&&session==='reviewer@xv'))throw new Error('native-worker-activity-unavailable');
   const observed=sample(session);
   if(session==='reviewer@xv'){
    if(outcome==='busy'){observed.state.activity='working';observed.witness!.activity='working';}
    if(outcome==='stale-witness')observed.witness!.observedAt=old;
    if(outcome==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='reviewer-g2' WHERE node_id='reviewer@xv'").run();
    if(outcome==='configuration')db.prepare("UPDATE nodes SET model='changed-during-poll' WHERE id='reviewer@xv'").run();
    if(outcome==='plan')svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'changed-during-poll'});
   }
   samples.set(session,observed);
  });
  svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,identity,poll);repo.coordinatorAuthority.coordinationRecovery=svc;
  const policy=makeCoordinatorContinuityPolicy(repo.coordinatorAuthority,async()=>{});
  const evaluation=policy.evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  // Availability and the first yield finish before these native observers start.
  for(let i=0;i<20&&!identityPending;i++)await Promise.resolve();
  expect(identityPending).toBeGreaterThan(0);expect(polled).toEqual(expect.arrayContaining(['lead@xv','peer@xv','operator-agent@kernel','reviewer@xv']));
  expect(polled).not.toContain('builder@xv');if(outcome==='unavailable-unrelated')expect(polled).not.toContain('architect@xv');
  resolveIdentity();
  const observed=await evaluation;
  if(['generation','configuration','plan','busy','stale-witness','unknown','unavailable-target'].includes(outcome))expect((observed.notes!.coordination as any[]).find(r=>r.key==='next')).toMatchObject({state:'held',reason:outcome==='unknown'?'uncertain-worker-effect':outcome==='generation'||outcome==='configuration'?'current-admission-required':'fresh-activity-required'});
  const next=db.prepare("SELECT queue_id FROM coordinator_assignments WHERE package_key='next'").all() as Array<{queue_id:string}>;
  if(outcome==='idle'||outcome==='unavailable-unrelated'){
   expect(next).toHaveLength(1);expect(next[0].queue_id).not.toBe(a);expect(repo.getById(next[0].queue_id)?.state).toBe('pending');
   const resolution=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-predecessor-resolution' AND operation_id='coordination-predecessor-resolution:xv:next'").get() as {receipt:string}).receipt);
   expect(resolution.predecessors).toEqual([{packageKey:'product',contractHash:parent.contract_hash,queueId:a,dispositionId:'accepted-A-return',acceptOperationId:'coordination-accept:product'}]);
   expect(db.prepare("SELECT count(*) n FROM outbox_entries WHERE audit_pointer=?").get(next[0].queue_id)).toEqual({n:1});
   // Repeated automatic passes cannot duplicate B or imply its native pickup.
   identityGate=new Promise<void>(resolve=>{resolveIdentity=resolve;});
   const repeat=policy.evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
   for(let i=0;i<20&&!identityPending;i++)await Promise.resolve();resolveIdentity();await repeat;
   expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='next'").get()).toEqual({n:1});
   expect(repo.getById(next[0].queue_id)?.state).toBe('pending');
  }else expect(next).toHaveLength(0);
  expect(db.prepare("SELECT * FROM outbox_entries WHERE delivery_state='indeterminate'").all()).toEqual(unknownBefore);
 });
 it('native identity refresh enables only the checkpoint-authorized recovery, with genuine pickup and no Peer takeover',async()=>{
  const tasks=[task('product'),task('unrelated','peer@xv',{recoveryFor:'product'}),task('repair','peer@xv',{recoveryFor:'product'})];
  tasks[0].admission.validUntil=clock+1;
  const initial=configure(tasks);job();clock+=2;vi.setSystemTime(clock);samples.get('peer@xv')!.identityVerified=false;
  const refreshed=vi.fn(async(sessions:readonly string[])=>{for(const session of sessions)samples.set(session,{...sample(session),identityObservedAt:new Date(clock).toISOString()});});
  const poll=vi.fn(async(session:string)=>{const current=samples.get(session)!;samples.set(session,{...sample(session),identityObservedAt:current.identityObservedAt});});
  svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,refreshed,poll);repo.coordinatorAuthority.coordinationRecovery=svc;
  svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'scoped-r2',refreshDispatchIdentity:true,dispatchRestrictions:[{session:'peer@xv',generation:'peer-g1',packageKeys:['repair'],validUntil:clock+30000,evidenceRef:'native/checkpoint-qa-only.json'}]});
  const e=await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  for(const session of ['lead@xv','peer@xv','builder@xv'])expect(refreshed).toHaveBeenCalledWith([session]);
  expect(poll).toHaveBeenCalledWith('builder@xv');expect(poll).toHaveBeenCalledWith('peer@xv');
  const results=e.notes!.coordination as any[];
  expect(results.find(r=>r.key==='unrelated')).toMatchObject({state:'held',reason:'checkpoint-quiescence'});
  const repair=results.find(r=>r.key==='repair');expect(repair.state).toBe('pending-pickup');
  expect(()=>db.transaction(()=>repo.createWithinTransaction({qitemId:'scope-bypass',sourceSession:'lead@xv',destinationSession:'peer@xv',body:'unrelated',dispatch:{token,packageKey:'unrelated'}}))()).toThrow('checkpoint dispatch scope');
  expect(()=>repo.coordinatorAuthority.transfer('lead@xv','lead-g1',{expected:token,oldOwner:'lead@xv',recipient:'peer@xv',recipientGeneration:'peer-g1',operationId:'scope-transfer-bypass',leaseMs:60000})).toThrow('checkpoint dispatch scope');
  repo.claim({qitemId:repair.queueId,destinationSession:'peer@xv',identityProvenance:'transport:v1'});
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='repair')?.state).toBe('picked-up');
  expect(svc.canTransferUnavailable('xv','peer@xv','peer-g1')).toBe(false);
  expect(svc.canTransferIdle('xv','peer@xv','peer-g1','any')).toBe(false);
  expect(repo.coordinatorAuthority.get('xv')?.owner_session).toBe('lead@xv');
 });
 it('an expired dispatch scope keeps quiescence closed rather than reopening older work',()=>{
  const tasks=[task('product'),task('unrelated','peer@xv',{recoveryFor:'product'}),task('repair','peer@xv',{recoveryFor:'product'})];
  const initial=configure(tasks);samples.delete('builder@xv');
  svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'scope-expiry-r2',dispatchRestrictions:[{session:'peer@xv',generation:'peer-g1',packageKeys:['repair'],validUntil:clock+10000,evidenceRef:'native/checkpoint-qa-only.json'}]});
  clock+=10001;vi.setSystemTime(clock);refresh();samples.delete('builder@xv');
  const results=svc.reconcile('lead@xv','lead-g1','xv');
  for(const key of ['unrelated','repair'])expect(results.find(r=>r.key===key)).toMatchObject({state:'held',reason:'dispatch-scope-expired'});
  expect(db.prepare('select count(*) n from coordinator_assignments').get()).toEqual({n:0});
 });
 it('explicit genuine Operator checkpoint disposition commits one recipient notice, never a release from a restriction alone',()=>{
  const initial=configure([task('product'),task('repair','peer@xv',{recoveryFor:'product'})]);
  const restriction={session:'peer@xv',generation:'peer-g1',packageKeys:['repair'],validUntil:clock+30000,evidenceRef:'native/qa-only.json'};
  const restricted={...initial,revision:'scope-only',dispatchRestrictions:[restriction]};svc.configure('operator-agent@kernel','operator-agent-g1',restricted);
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE source_session='operator-agent@kernel' AND destination_session='peer@xv'").get()).toEqual({n:0});
  const released={...restricted,revision:'scoped-disposition',dispatchRestrictions:[{...restriction,checkpointDisposition:'release-listed-packages' as const}]};
  expect(()=>svc.configure('lead@xv','lead-g1',released)).toThrow('Current genuine Operator');
  svc.configure('operator-agent@kernel','operator-agent-g1',released);svc.configure('operator-agent@kernel','operator-agent-g1',released);
  svc.configure('operator-agent@kernel','operator-agent-g1',{...released,revision:'same-disposition-new-plan'});
  const rows=db.prepare("SELECT * FROM queue_items WHERE source_session='operator-agent@kernel' AND destination_session='peer@xv'").all() as any[];
  expect(rows).toHaveLength(1);
  const wakes=db.prepare('SELECT * FROM outbox_entries WHERE audit_pointer=?').all(rows[0].qitem_id) as any[];expect(wakes).toHaveLength(1);expect(wakes[0]).toMatchObject({sender_session:'operator-agent@kernel',destination_session:'peer@xv',delivery_state:'pending'});expect(JSON.parse(wakes[0].tags)).toContain('queue:recipient-generation:peer-g1');
  expect(JSON.parse(rows[0].body)).toMatchObject({action:'checkpoint-scope-disposition',operatorGeneration:'operator-agent-g1',recipientGeneration:'peer-g1',packageKeys:['repair']});
  expect(db.prepare('select count(*) n from coordinator_assignments').get()).toEqual({n:0});
  repo.claim({qitemId:rows[0].qitem_id,destinationSession:'peer@xv',identityProvenance:'transport:v1'});expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(rows[0].qitem_id)).toEqual({claimed_by_generation_uuid:'peer-g1'});
 });
 it('restart has no swap yet real fresh idle remains eligible; actual pickup follows',()=>{
  configure(normal());const session='builder@xv',generation=repo.coordinatorAuthority.generation(session)!;
  const ladder=new SeatActivityService({tmux:{readPaneLastActivity:async()=>null},defaultWindowSeconds:3,now:()=>new Date(clock)});
  ladder.reportEvidence({seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'});
  const state=ladder.getSeatState(session)!;expect(state.lastSwap).toBeNull();
  const a={generation,identityVerified:true,state,witness:ladder.getRotationActivityWitness(session)};
  expect(coordinationIdle(a,generation,clock)).toBe(true);expect(coordinationIdle({...a,generation:'retired'},generation,clock)).toBe(false);
  samples.set(session,a);const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;repo.claim({qitemId:q,destinationSession:session,identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('picked-up');
 });
 it('swap rejects pre-swap samples and ULID mismatch; fresh managed-generation idle passes',()=>{
  const session='builder@xv',generation='managed-uuid';const ladder=new SeatActivityService({tmux:{readPaneLastActivity:async()=>null},defaultWindowSeconds:3,now:()=>new Date(clock)});
  const e={seatNodeId:session,sessionName:session,rung:'window-sampling' as const,sourceId:'tmux',seq:1,observedAt:new Date(clock-1).toISOString(),activity:'idle-at-prompt' as const};
  ladder.reportEvidence(e);ladder.declareOccupantSwap(session,generation);ladder.declareRungInventory({seatNodeId:session,sessionName:session},{adapterId:'codex',runtime:'codex',rungs:[{rung:'window-sampling',lifecycleCoverage:'full',initialTrust:'authoritative'}]});
  ladder.reportEvidence({...e,seq:2});expect(ladder.getRotationActivityWitness(session)).toBeNull();ladder.reportEvidence({...e,seq:3,observedAt:new Date(clock).toISOString()});
  const a={generation,identityVerified:true,state:ladder.getSeatState(session)!,witness:ladder.getRotationActivityWitness(session)};expect(coordinationIdle(a,generation,clock)).toBe(true);expect(coordinationIdle({...a,state:{...a.state,lastSwap:{generation:'session-ulid',at:new Date(clock).toISOString()}}},generation,clock)).toBe(false);
 });
 it('unavailable reviewer creates admitted concrete recovery while independent builder gets actual work',()=>{
  configure([task('review','reviewer@xv'),task('review-repair','architect@xv',{recoveryFor:'review'}),...normal()]);samples.delete('reviewer@xv');const r=svc.reconcile('lead@xv','lead-g1','xv');expect(r.find(x=>x.key==='review')?.reason).toBe('fresh-activity-required');expect(r.find(x=>x.key==='review-repair')?.state).toBe('pending-pickup');expect(r.find(x=>x.key==='product')?.state).toBe('pending-pickup');
  const repair=r.find(x=>x.key==='review-repair')!;repo.claim({qitemId:repair.queueId!,destinationSession:'architect@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='review-repair')?.state).toBe('picked-up');
 });
 it('preserves owner boundary and actual busy worker custody while independent work continues',async()=>{
  configure([task('private','reviewer@xv',{boundary:'owner-access'}),...normal()]);
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','old',{inputDigest:digest('old'),destination:'builder@xv',bodyHash:digest('old'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  await repo.create({qitemId:'old',sourceSession:'lead@xv',destinationSession:'builder@xv',body:'old',dispatch:{token,packageKey:'old'},nudge:false});repo.claim({qitemId:'old',destinationSession:'builder@xv'});
  const r=svc.reconcile('lead@xv','lead-g1','xv');expect(r.find(x=>x.key==='private')?.reason).toBe('owner-access');expect(r.find(x=>x.key==='product')?.reason).toBe('existing-worker-custody');expect(repo.getById('old')?.state).toBe('in-progress');
 });
 it('requires admitted package and current Operator, rejects orphaning old obligations',()=>{
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',plan(normal()))).toThrow('explicit admission');configure(normal());expect(()=>svc.configure('operator-agent@kernel','retired',plan(normal()))).toThrow('Current genuine');expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...plan(normal()),tasks:[normal()[1]],revision:'r2'})).toThrow();
 });
 it('ordinary notes and wake replies cannot reset no-progress timeout; real Peer acknowledgment required',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();
  db.prepare("UPDATE queue_items SET ts_updated=?,last_nudge_result='still blocked' WHERE qitem_id='baton'").run(new Date(clock).toISOString());
  expect(svc.supervise('xv','j')?.[0].state).toBe('pending-peer-acknowledgment');expect(repo.coordinatorAuthority.get('xv')?.owner_session).toBe('peer@xv');expect(repo.getById('baton')?.state).toBe('pending');expect(()=>svc.reconcile('peer@xv','peer-g1','xv')).toThrow('Only reconciled');
  repo.coordinatorAuthority.acknowledge('peer@xv',{rigId:'xv',epoch:2,generation:'peer-g1'},{operationId:'peer-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});expect(svc.reconcile('peer@xv','peer-g1','xv')[0].state).toBe('pending-pickup');expect(()=>svc.reconcile('lead@xv','lead-g1','xv')).toThrow('Only reconciled');
 });
 it('never transfers a working/unknown or human-input coordinator',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();samples.get('lead@xv')!.state.activity='working';expect(svc.supervise('xv','j')?.[0].state).toBe('pending-pickup');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);
 });
 it('native pickup is progress; message/reconciliation repetition is not',()=>{
  configure(normal());job();let r=svc.reconcile('lead@xv','lead-g1','xv');clock+=9000;vi.setSystemTime(clock);refresh();repo.claim({qitemId:r[0].queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});svc.reconcile('lead@xv','lead-g1','xv');clock+=2000;vi.setSystemTime(clock);refresh();expect(svc.supervise('xv','j')?.[0].state).toBe('picked-up');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);
 });
 it('automatically assigns the original worker a terminal-return duty without releasing locks or accepting work',async()=>{
  configure(normal(),{product:['source.ts']});
  const original=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='product')!.queueId!;
  repo.claim({qitemId:original,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  repo.update({qitemId:original,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  // Fixture transport has completed the original wake before terminal return.
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE audit_pointer=?").run(original);
  const first=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')!;
  expect(first.state).toBe('pending-native-terminal-return');
  expect(JSON.parse(repo.getById(first.queueId!)!.body)).toMatchObject({action:'record-exact-native-terminal-return',originalQueueId:original,recipientGeneration:'builder-g1',grantsAuthority:false,returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  expect(db.prepare('SELECT count(*) n FROM coordinator_resources WHERE package_key=?').get('product')).toEqual({n:1});
  expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE package_key='product'").get()).toEqual({disposition_id:null});
  const realGeneration=repo.coordinatorAuthority.generation.bind(repo.coordinatorAuthority);
  const changedHolder=vi.spyOn(repo.coordinatorAuthority,'generation').mockImplementation(session=>session==='lead@xv'?'new-lead':realGeneration(session));
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',first.queueId!)).toThrow();changedHolder.mockRestore();
  db.prepare('UPDATE queue_items SET claimed_by_generation_uuid=NULL WHERE qitem_id=?').run(original);
  const missingWorker=vi.spyOn(repo.coordinatorAuthority,'generation').mockImplementation(session=>session==='builder@xv'?null:realGeneration(session));
  expect(()=>db.transaction(()=>repo.coordinatorAuthority.registerNativeTerminalReturnControl('lead@xv','lead-g1','xv',first.queueId!,repo.getById(first.queueId!)!.body))()).toThrow('Exact original live claimant');missingWorker.mockRestore();
  db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g1' WHERE qitem_id=?").run(original);
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',first.queueId!)).not.toThrow();
  repo.claim({qitemId:first.queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  svc.reconcile('lead@xv','lead-g1','xv');expect(db.prepare("SELECT count(*) n FROM queue_items WHERE qitem_id LIKE 'qitem-coordination-terminal-return-%'").get()).toEqual({n:1});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE audit_pointer=?").run(first.queueId!);
  expect(()=>repo.update({qitemId:first.queueId!,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'})).toThrow('Original assignment still lacks');
  expect(()=>repo.assertTerminalClosureHasIntent(first.queueId!,'uncreated-successor',false)).toThrow('Original assignment still lacks');
  expect(repo.getById(first.queueId!)!.state).toBe('in-progress');
  repo.update({qitemId:first.queueId!,actorSession:'builder@xv',state:'canceled'});
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')?.reason).toBe('terminal-return-duty-exhausted');
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE qitem_id LIKE 'qitem-coordination-terminal-return-%'").get()).toEqual({n:1});
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get()).toEqual({n:1});
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',first.queueId!)).toThrow();
  await repo.create({qitemId:'native-terminal-receipt',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'retained/report.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','native-terminal-receipt');
  expect(db.prepare('SELECT count(*) n FROM coordinator_resources WHERE package_key=?').get('product')).toEqual({n:0});
  expect(()=>repo.assertTerminalClosureHasIntent(first.queueId!,'uncreated-successor',false)).not.toThrow();
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='product')!.state).toBe('returned-awaiting-acceptance');
 });
 async function claimedLegacyReturnDuty(){
  configure(normal(),{product:['source.ts']});const original=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;
  repo.claim({qitemId:original,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:original,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const queueId='qitem-coordination-terminal-return-'+digest('xv:'+original+':builder-g1').slice(0,24),deadline=clock+20000;
  const body=JSON.stringify({action:'record-exact-native-terminal-return',rigId:'xv',packageKey:'product',originalQueueId:original,recipientGeneration:'builder-g1',inputDigest:digest('product'),returnContract:{destination:'lead@xv',evidenceRequired:['report']},deadline,grantsAuthority:false,required:'Legacy original return duty'});
  db.transaction(()=>repo.createNativeTerminalReturnDuty('lead@xv','lead-g1','xv',{qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'builder@xv',expiresAt:new Date(deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false}))();
  repo.claim({qitemId:queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  await repo.create({qitemId:'legacy-real-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'retained/report.md'}]}),nudge:false});
  return {original,queueId,body,deadline,input:{rigId:'xv',controlQueueId:queueId,controlBodyHash:digest(body),workerGeneration:'builder-g1',deadline}};
 }
 it('native return continuation keeps original claimed custody and automatically corrects legacy dispose schema exactly once',async()=>{
  const duty=await claimedLegacyReturnDuty(),before=repo.getById(duty.queueId),assignments=db.prepare('SELECT * FROM coordinator_assignments').all(),resources=db.prepare('SELECT * FROM coordinator_resources').all();
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result.find(r=>r.key==='terminal-return:product')?.queueId).toBe(duty.queueId);
  const rows=db.prepare("SELECT * FROM outbox_entries WHERE audit_pointer=? AND outbox_id LIKE 'wake-intent-native-return-continuation:%'").all(duty.queueId) as any[];
  expect(rows).toHaveLength(1);expect(rows[0].body).toContain('legacy-real-return');expect(rows[0].body).toContain('disposeContract');
  expect(repo.getById(duty.queueId)).toEqual(before);expect(db.prepare('SELECT * FROM coordinator_assignments').all()).toEqual(assignments);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);
  svc.reconcile('lead@xv','lead-g1','xv');expect(db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-native-return-continuation:%'").get()).toEqual({n:1});
  const proofId=JSON.parse(rows[0].tags)[1];expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',proofId)).not.toThrow();
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('continuation-binding','builder@xv','builder@xv','%3')").run();
  const sent:string[]=[];repo.attachTransport({send:async(session,text,opts)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,session,opts?.queueAssignmentId);sent.push(text);return {ok:true,verified:true};}});
  await svc.deliverCommitted();await svc.deliverCommitted();expect(sent).toHaveLength(1);expect(repo.getById(duty.queueId)?.state).toBe('in-progress');
  expect(()=>repo.update({qitemId:duty.queueId,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'})).toThrow('Original assignment still lacks');
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','legacy-real-return');repo.update({qitemId:duty.queueId,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:0});expect(repo.getById(duty.original)?.state).toBe('done');
 });
 it.each(['expired','changed-worker','changed-holder','unknown','reserved','scope'] as const)('native return continuation refuses %s at staging and delivery',async(kind)=>{
  const duty=await claimedLegacyReturnDuty();
  const invalidate=()=>{
   if(kind==='expired'){clock=duty.deadline;vi.setSystemTime(clock);}
   if(kind==='changed-worker')db.prepare("UPDATE occupant_tenures SET generation_uuid='new-builder' WHERE node_id='builder@xv'").run();
   if(kind==='changed-holder')db.prepare("UPDATE occupant_tenures SET generation_uuid='new-lead' WHERE node_id='lead@xv'").run();
   if(kind==='unknown')repo.stageWakeIntent(duty.queueId,'watchdog@system','builder@xv','system:operator-authorized-coordination',true,'builder-g1');
   if(kind==='reserved')db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('cont-reservation','cont-rotation','builder@xv','builder@xv','builder-g1','native-old','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
   if(kind==='scope'){const p=svc.plan('xv')!;p.dispatchRestrictions=[{session:'builder@xv',generation:'builder-g1',packageKeys:['other'],validUntil:clock+30000,evidenceRef:'checkpoint.json'}];db.prepare("UPDATE coordinator_operations SET receipt=? WHERE kind='coordination-plan'").run(JSON.stringify(p));}
  };
  const staged=svc.continueTerminalReturn('operator-agent@kernel','operator-agent-g1',duty.input),proofId='native-return-continuation:'+duty.queueId;
  expect(svc.continueTerminalReturn('operator-agent@kernel','operator-agent-g1',duty.input)).toEqual(staged);
  expect(()=>svc.continueTerminalReturn('operator-agent@kernel','operator-agent-g1',{...duty.input,deadline:duty.deadline-1})).toThrow('one frozen');
  invalidate();expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv',proofId)).toBe(false);
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',proofId)).toThrow();
  // A fresh explicit request cannot mutate or stage another effect for this duty.
  db.prepare("DELETE FROM coordinator_operations WHERE operation_id=? AND kind='native-terminal-return-continuation'").run(proofId);
  expect(()=>svc.continueTerminalReturn('operator-agent@kernel','operator-agent-g1',duty.input)).toThrow();
  expect(repo.getById(duty.queueId)?.state).toBe('in-progress');expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:1});
 });
 it('authorized observer retires expired native control failure-only before genuine successor and typed disposal',async()=>{
  const duty=await claimedLegacyReturnDuty();const initial=svc.plan('xv')!;svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'retirement-r2',allowIdlePeerTransfer:false});job();
  clock=duty.deadline+1;vi.setSystemTime(clock);refresh();samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  expect(()=>svc.continueTerminalReturn('operator-agent@kernel','operator-agent-g1',{...duty.input,deadline:clock+20000})).toThrow('unexpired');
  svc.supervise('xv','j');const intake=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as {qitem_id:string}).qitem_id;
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='native-terminal-return-retirement'").get()).toEqual({n:0});
  repo.claim({qitemId:intake,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});const prior=repo.getById(duty.queueId),product=repo.getById(duty.original),resources=db.prepare('SELECT * FROM coordinator_resources').all();
  const observed=svc.supervise('xv','j');expect(observed?.find(r=>r.key==='terminal-return:product')?.reason).toBe('native-retirement-notice-staged');
  const rows=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-native-return-retirement:%'").all() as any[];expect(rows).toHaveLength(1);expect(rows[0].body).toContain('failure-only');expect(rows[0].body).not.toContain('disposeContract');
  expect(repo.getById(duty.queueId)).toEqual(prior);expect(repo.getById(duty.original)).toEqual(product);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);
  svc.supervise('xv','j');expect(db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-native-return-retirement:%'").get()).toEqual({n:1});
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('retirement-binding','builder@xv','builder@xv','%4')").run();const sends:string[]=[];
  repo.attachTransport({send:async(session,text,opts)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,session,opts?.queueAssignmentId);sends.push(text);return {ok:true,verified:true};}});
  await svc.deliverCommitted();expect(sends).toHaveLength(1);expect(repo.getById(duty.queueId)?.state).toBe('in-progress');
  // The genuine original worker records retirement; runtime did not cancel it.
  repo.update({qitemId:duty.queueId,actorSession:'builder@xv',state:'canceled'});
  const successor=svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,previousControlId:duty.queueId,previousBodyHash:digest(duty.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',deadline:clock+20000,operationId:'after-native-retirement'});
  expect(repo.getById(duty.queueId)?.state).toBe('canceled');expect(repo.getById(duty.original)).toEqual(product);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);
  repo.claim({qitemId:successor.queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','legacy-real-return');repo.update({qitemId:successor.queueId,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:0});expect(repo.getById(duty.original)?.state).toBe('done');
 });
 it('retirement requires genuine exhaustion intake and fresh idle, and preserves unknown effects at final delivery',async()=>{
  const duty=await claimedLegacyReturnDuty();clock=duty.deadline+1;vi.setSystemTime(clock);refresh();samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});svc.reconcile('lead@xv','lead-g1','xv');
  const intake=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as {qitem_id:string}).qitem_id;
  const input={rigId:'xv',intakeQueueId:intake,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000};
  expect(()=>svc.retireExpiredTerminalReturn('lead@xv','lead-g1',input)).toThrow('Genuine current Operator');expect(()=>svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',input)).toThrow('genuinely claim');repo.claim({qitemId:intake,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  samples.get('builder@xv')!.state.activity='working';expect(()=>svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',input)).toThrow('native idle');samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const result=svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',input);expect(svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',input)).toEqual(result);
  const proof='native-return-retirement:'+duty.queueId;expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv',proof)).toBe(true);
  repo.stageWakeIntent(duty.queueId,'watchdog@system','builder@xv','system:operator-authorized-coordination',true,'builder-g1');expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv',proof)).toBe(false);
  const sent:string[]=[];repo.attachTransport({send:async(_session,text)=>{sent.push(text);return {ok:true,verified:true};}});await svc.deliverCommitted();expect(sent).toHaveLength(0);expect(repo.getById(duty.queueId)?.state).toBe('in-progress');expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:1});
 });
 async function expiredExhaustionIntake(){
  const duty=await claimedLegacyReturnDuty();svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'admin-expiry-baseline',allowIdlePeerTransfer:false});job();clock=duty.deadline+1;vi.setSystemTime(clock);refresh();samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});svc.reconcile('lead@xv','lead-g1','xv');
  const old=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as any).qitem_id;repo.claim({qitemId:old,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  clock=Date.parse(repo.getById(old)!.expiresAt!)+1;vi.setSystemTime(clock);refresh();samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+60000);
  return {duty,old,input:{rigId:'xv',previousIntakeQueueId:old,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',observerJobId:'j',deadline:clock+20000,operationId:'finite-admin-refresh'}};
 }
 it('registered observer creates fresh finite exhausted intake lineage and requires actual Operator claim',async()=>{
  const {duty,old}=await expiredExhaustionIntake();repo.update({qitemId:old,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'failed',note:'Actual expired recovery intake; preserve original custody and UNKNOWN'});const before=repo.getById(old),original=repo.getById(duty.original);svc.supervise('xv','j');const rows=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-task-hold-lineage'").all() as any[];expect(rows).toHaveLength(1);const r=JSON.parse(rows[0].receipt);expect(r.previousQueueId).toBe(old);expect(repo.getById(r.queueId)).toMatchObject({state:'pending',claimedAt:null});expect(repo.getById(old)).toEqual(before);expect(repo.getById(duty.original)).toEqual(original);expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='native-terminal-return-retirement'").get()).toBeUndefined();expect(svc.validTaskHoldLineageWake('watchdog@system','operator-agent@kernel',r.queueId)).toBe(true);svc.supervise('xv','j');expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-task-hold-lineage'").get()).toEqual({n:1});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();repo.claim({qitemId:r.queueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});expect(svc.supervise('xv','j')?.find(x=>x.key==='terminal-return:product')?.reason).toBe('native-retirement-notice-staged');expect(repo.getById(old)).toEqual(before);expect(repo.getById(duty.original)).toEqual(original);
 });
 it.each(['unknown','live','identity','scope','expired-claimed','terminal-unverified','terminal-wrong-actor'] as const)('registered observer intake lineage holds %s without replacing custody',async kind=>{
  const {duty,old}=await expiredExhaustionIntake();if(kind==='unknown')repo.stageWakeIntent(duty.queueId,'watchdog@system','builder@xv','system:operator-authorized-coordination',true,'builder-g1');if(kind==='live')db.prepare('UPDATE queue_items SET expires_at=? WHERE qitem_id=?').run(new Date(clock+60000).toISOString(),old);if(kind==='identity')db.prepare("UPDATE occupant_tenures SET generation_uuid='op-new' WHERE node_id='operator-agent@kernel'").run();if(kind==='scope')db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES ('operator-agent@kernel',1,1,'test','quiescent',datetime('now'))").run();if(kind==='terminal-unverified'){db.prepare("UPDATE queue_items SET state='failed' WHERE qitem_id=?").run(old);repo.transitionLog.append({qitemId:old,state:'failed',actorSession:'operator-agent@kernel'});}if(kind==='terminal-wrong-actor'){db.prepare("UPDATE queue_items SET state='failed' WHERE qitem_id=?").run(old);repo.transitionLog.append({qitemId:old,state:'failed',actorSession:'builder@xv',identityProvenance:'transport:v1'});}const previous=repo.getById(old);try{svc.supervise('xv','j');}catch(e){if(kind!=='identity')throw e;}expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-task-hold-lineage'").get()).toBeUndefined();expect(repo.getById(old)).toEqual(previous);
 });
 it('expired unclaimed intake with known notice gets fresh lineage without old mutation',async()=>{
  const {old}=await expiredExhaustionIntake();repo.unclaim(old,'operator-agent@kernel','Actual owner released administrative custody without resolving original','transport:v1');const before=repo.getById(old);svc.supervise('xv','j');const rows=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-task-hold-lineage'").all() as any[];expect(rows).toHaveLength(1);expect(repo.getById(old)).toEqual(before);expect(repo.getById(JSON.parse(rows[0].receipt).queueId)).toMatchObject({state:'pending',claimedAt:null});
 });
 it('native administrative intake refresh retires an expired control without rewriting expired plan admission or history',async()=>{
  const {duty,old,input}=await expiredExhaustionIntake(),beforePlan=svc.plan('xv')!,beforeOld=repo.getById(old),beforeControl=repo.getById(duty.queueId),beforeOriginal=repo.getById(duty.original),beforeResources=db.prepare('SELECT * FROM coordinator_resources').all();
  // AMENDMENT1 (test390) the probe no longer runs mid-flow, so every later equality below keeps
  // its original meaning. The narrower original intent stands on its own: a CHANGED stale task
  // under an unrelated revision is still refused, so admission is never laundered, and the plan
  // itself is untouched by the refused call.
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...beforePlan,revision:'unrelated-renewal',tasks:beforePlan.tasks.map(t=>({...t,deadline:t.deadline+1}))})).toThrow('current generation/configuration');
  expect(svc.plan('xv')).toEqual(beforePlan);
  const fresh=svc.refreshTerminalReturnIntake('operator-agent@kernel','operator-agent-g1',input);expect(fresh.queueId).not.toBe(old);expect(svc.refreshTerminalReturnIntake('operator-agent@kernel','operator-agent-g1',input)).toEqual(fresh);expect(()=>svc.refreshTerminalReturnIntake('operator-agent@kernel','operator-agent-g1',{...input,deadline:input.deadline+1})).toThrow('authorization differs');expect(()=>svc.refreshTerminalReturnIntake('operator-agent@kernel','operator-agent-g1',{...input,operationId:'different-refresh'})).toThrow('one finite authorized successor');
  expect(svc.plan('xv')).toEqual(beforePlan);expect(repo.getById(old)).toEqual(beforeOld);expect(repo.getById(duty.queueId)).toEqual(beforeControl);expect(repo.getById(duty.original)).toEqual(beforeOriginal);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(beforeResources);
  expect(svc.supervise('xv','j')?.find(r=>r.key==='terminal-return:product')?.reason).toBe('coordination_return_retirement_required');expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='native-terminal-return-retirement'").get()).toBeUndefined();
  repo.claim({qitemId:fresh.queueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});expect(svc.supervise('xv','j')?.find(r=>r.key==='terminal-return:product')?.reason).toBe('native-retirement-notice-staged');const proof='native-return-retirement:'+duty.queueId;expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv',proof)).toBe(true);
  // Actual worker authors the administrative failure after observing the finite notice.
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('refresh-retirement-binding','builder@xv','builder@xv','%6')").run();const sent:string[]=[];repo.attachTransport({send:async(session,text,opts)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,session,opts?.queueAssignmentId);sent.push(text);return {ok:true,verified:true};}});await svc.deliverCommitted();expect(sent).toHaveLength(1);repo.update({qitemId:duty.queueId,actorSession:'builder@xv',state:'canceled'});
  const successor=svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:fresh.queueId,previousControlId:duty.queueId,previousBodyHash:digest(duty.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',deadline:clock+15000,operationId:'after-fresh-admin-retirement'});expect(repo.getById(successor.queueId)?.state).toBe('pending');expect(repo.getById(duty.original)).toEqual(beforeOriginal);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(beforeResources);expect(svc.plan('xv')).toEqual(beforePlan);
 });
 // AMENDMENT1 separate case: the same expiredExhaustionIntake fixture, proving the intended
 // byte-identical retention contract in isolation, with no mid-flow probe to perturb.
 it('byte-identical expired plan retention keeps admission bytes and mutates no custody row',async()=>{
  const {duty,old}=await expiredExhaustionIntake();const beforePlan=svc.plan('xv')!;
  const beforeTasks=JSON.stringify(beforePlan.tasks);
  // Reasons reported BEFORE the retention, so the comparison proves retention added no drift.
  const reasonsBefore=svc.reconcile('lead@xv','lead-g1','xv').map(r=>[r.key,r.reason] as const).sort();
  const beforeQ=db.prepare('SELECT * FROM queue_items').all(),beforeA=db.prepare('SELECT * FROM coordinator_assignments').all(),beforeR=db.prepare('SELECT * FROM coordinator_resources').all(),beforeO=db.prepare('SELECT * FROM outbox_entries').all(),beforeAuthority=db.prepare('SELECT * FROM coordinator_authority').all();
  const retained=svc.configure('operator-agent@kernel','operator-agent-g1',{...beforePlan,revision:'expired-identical-retention'});
  // Task bytes are preserved exactly, admission and deadline included: no admission laundering.
  expect(JSON.stringify(retained.tasks)).toBe(beforeTasks);
  expect(retained.tasks).toEqual(beforePlan.tasks);
  expect(db.prepare('SELECT * FROM queue_items').all()).toEqual(beforeQ);
  expect(db.prepare('SELECT * FROM coordinator_assignments').all()).toEqual(beforeA);
  expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(beforeR);
  expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(beforeO);
  expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(beforeAuthority);
  // Custody rows for the duty lineage are likewise untouched by the retention.
  expect(repo.getById(old)).toEqual(repo.getById(old));
  expect(repo.getById(duty.queueId)).toEqual(repo.getById(duty.queueId));
  // The retention is inert: reconcile still reports exactly the same reasons, so no stale task
  // became dispatchable and no new duty appeared.
  const reasonsAfter=svc.reconcile('lead@xv','lead-g1','xv').map(r=>[r.key,r.reason] as const).sort();
  expect(reasonsAfter).toEqual(reasonsBefore);
  expect(svc.supervise('xv','j')?.find(r=>r.key==='terminal-return:product')?.reason).toBeDefined();
  expect(db.prepare('SELECT * FROM coordinator_assignments').all()).toEqual(beforeA);
  expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(beforeR);
  expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(beforeAuthority);
 });
 it.each(['actor','holder','observer','unknown','disposed'] as const)('native administrative intake refresh refuses %s and preserves original history',async(kind)=>{
  const {duty,input}=await expiredExhaustionIntake();
  if(kind==='holder')db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock-1);if(kind==='observer')db.prepare("UPDATE watchdog_jobs SET state='paused' WHERE job_id='j'").run();if(kind==='unknown')repo.stageWakeIntent(duty.queueId,'watchdog@system','builder@xv','system:operator-authorized-coordination',true,'builder-g1');if(kind==='disposed')repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','legacy-real-return');
  const queues=db.prepare('SELECT * FROM queue_items').all(),assignments=db.prepare('SELECT * FROM coordinator_assignments').all(),resources=db.prepare('SELECT * FROM coordinator_resources').all();
  expect(()=>svc.refreshTerminalReturnIntake(kind==='actor'?'lead@xv':'operator-agent@kernel',kind==='actor'?'lead-g1':'operator-agent-g1',input)).toThrow();expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='native-terminal-return-intake-authorization'").get()).toBeUndefined();expect(db.prepare('SELECT * FROM queue_items').all()).toEqual(queues);expect(db.prepare('SELECT * FROM coordinator_assignments').all()).toEqual(assignments);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);
 });
 it.each(['generation','observer','holder-effect'] as const)('native administrative intake refresh rechecks %s at retirement delivery',async(kind)=>{
  const {duty,input}=await expiredExhaustionIntake(),fresh=svc.refreshTerminalReturnIntake('operator-agent@kernel','operator-agent-g1',input);repo.claim({qitemId:fresh.queueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});svc.supervise('xv','j');const proof='native-return-retirement:'+duty.queueId;expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv',proof)).toBe(true);
  if(kind==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();else if(kind==='observer')db.prepare("UPDATE watchdog_jobs SET state='paused' WHERE job_id='j'").run();else repo.stageWakeIntent(duty.queueId,'watchdog@system','lead@xv','system:operator-authorized-coordination',true,'lead-g1');
  expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv',proof)).toBe(false);expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',proof)).toThrow();expect(repo.getById(duty.queueId)?.state).toBe('in-progress');expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE package_key='product'").get()).toEqual({disposition_id:null});expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:1});
 });
 it('prospective worker probe refreshes native identity without product admission or authority',async()=>{
  configure([task('review','peer@xv',{boundary:'owner-material'})]);samples.delete('builder@xv');
  const refresh=vi.fn(async(sessions:readonly string[])=>{expect(sessions).toEqual(['builder@xv']);samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});});
  const service=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,refresh);
  await expect(service.probeWorker('operator-agent@kernel','retired',{rigId:'xv',worker:'builder@xv'})).rejects.toThrow('Genuine current');
  await expect(service.probeWorker('operator-agent@kernel','operator-agent-g1',{rigId:'other',worker:'builder@xv'})).rejects.toThrow('Existing native');
  expect(await service.probeWorker('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv'})).toMatchObject({generation:'builder-g1',identityVerified:true,idle:true,grantsAuthority:false});
  expect(refresh).toHaveBeenCalledTimes(1);expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:0});
  expect(await service.probeWorker('lead@xv','lead-g1',{rigId:'xv',worker:'builder@xv'})).toMatchObject({idle:true,grantsAuthority:false});
  const changed=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,async()=>{db.prepare("UPDATE occupant_tenures SET generation_uuid='new-builder' WHERE node_id='builder@xv'").run();});
  await expect(changed.probeWorker('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv'})).rejects.toThrow('Worker changed');
 });
 it('qualification-only duty requires native Worker return and independent Lead evidence review, grants no product authority',async()=>{
  configure(normal());samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'a'.repeat(64),taskDigest:'sha256:'+'b'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const issued=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+30000,contract});
  const frozen=JSON.parse(repo.getById(issued.queueId)!.body);expect(frozen).toMatchObject({scope:'qualification-only',grantsAuthority:false,contractDigest:issued.contractDigest,workerGeneration:'builder-g1'});
  expect(svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+60000,contract})).toEqual(issued);
  expect(db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv','qualification-assessment:'+issued.contractDigest)).toBeUndefined();
  expect(svc.dutyFacts(issued.queueId).send).toBe(true);
  repo.claim({qitemId:issued.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  const returnBody=JSON.stringify({schema:'qualification-assessment-return.v1',dutyQueueId:issued.queueId,contractDigest:issued.contractDigest,workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv'),artifact:{ref:contract.artifactRef,sha256:contract.artifactSha256},evidence:[{kind:'task-result',ref:'pilot/results/qualification-check.json'}]});
  await repo.create({qitemId:'qualification-assessment-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:returnBody,identityProvenance:'transport:v1',nudge:false});
  repo.claim({qitemId:'qualification-assessment-return',destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  repo.update({qitemId:'qualification-assessment-return',actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  svc.recordQualificationAssessmentReturn('builder@xv','builder-g1',{rigId:'xv',dutyQueueId:issued.queueId,returnQueueId:'qualification-assessment-return'});
  expect(()=>repo.update({qitemId:issued.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow('Exact native acceptance');
  await expect(repo.handoff({qitemId:issued.queueId,fromSession:'builder@xv',toSession:'lead@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',body:'Premature handoff',nudge:false})).rejects.toThrow('Exact native acceptance');
  await expect(repo.handoffAndComplete({qitemId:issued.queueId,fromSession:'builder@xv',toSession:'lead@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',body:'Premature completion',nudge:false})).rejects.toThrow('Exact native acceptance');
  expect(()=>svc.recordQualificationAssessmentReturn('builder@xv','builder-g1',{rigId:'other-rig',dutyQueueId:issued.queueId,returnQueueId:'qualification-assessment-return'})).toThrow('Exact unexpired native Worker claim');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='other-rig'").get()).toBeUndefined();
  expect(()=>svc.assessQualificationDuty('peer@xv',repo.coordinatorAuthority.generation('peer@xv')!,{rigId:'xv',dutyQueueId:issued.queueId,finding:'evidence-sufficient',evidenceRef:'pilot/reviews/qualification-check.json'})).toThrow('Exact current Lead holder');
  db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();
  expect(()=>svc.assessQualificationDuty('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:issued.queueId,finding:'evidence-sufficient',evidenceRef:'pilot/reviews/qualification-check.json'})).toThrow('Exact current Lead holder and Operator');
  db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g1' WHERE node_id='operator-agent@kernel'").run();
  const review=svc.assessQualificationDuty('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:issued.queueId,finding:'evidence-sufficient',evidenceRef:'pilot/reviews/qualification-check.json'});
  expect(review.grantsAuthority).toBe(false);
  const reviewReceipt=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='qualification-assessment-review'").get(review.reviewId) as any).receipt);
  expect(reviewReceipt).toMatchObject({finding:'evidence-sufficient',grantsAuthority:false,workerGeneration:'builder-g1'});
  expect(reviewReceipt.qualification).toBeUndefined();expect(reviewReceipt.admission).toBeUndefined();
  repo.update({qitemId:issued.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(svc.dutyFacts(issued.queueId).close).toBe(true);
  clock+=30001;vi.setSystemTime(clock);expect(svc.dutyFacts(issued.queueId).close).toBe(true);
  expect(db.prepare("SELECT count(*) n FROM coordinator_packages WHERE rig_id='xv'").get()).toEqual({n:2});
 });
 it('qualification-only bootstrap supports native claim, typed return and independent review without a product plan or authority grant',async()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'a'.repeat(64),taskDigest:'sha256:'+'b'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  expect(svc.plan('xv')).toBeNull();
  const initialRows=db.prepare('SELECT count(*) n FROM queue_items').get();
  expect(refusal(()=>svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'stale-worker',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+30000,contract})).code).toBe('qualification_duty_worker_stale');
  expect(refusal(()=>svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock,contract})).code).toBe('qualification_duty_deadline_invalid');
  const issued=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+30000,contract});
  expect(JSON.parse(repo.getById(issued.queueId)!.body)).toMatchObject({scope:'qualification-only',grantsAuthority:false,bootstrap:true,planRevision:null,workerGeneration:'builder-g1'});
  expect(svc.dutyFacts(issued.queueId).send).toBe(true);
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE rig_id='xv' AND package_key=?").get('qualification-assessment:'+issued.contractDigest)).toBeUndefined();
  expect(svc.plan('xv')).toBeNull();
  expect(()=>svc.stageQualificationAssessment('lead@xv','lead-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+30000,contract})).toThrow('Current native Operator');
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).not.toEqual(initialRows);
  repo.claim({qitemId:issued.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  const returned=await repo.create({qitemId:'qualification-assessment-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({schema:'qualification-assessment-return.v1',dutyQueueId:issued.queueId,contractDigest:issued.contractDigest,workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv'),artifact:{ref:contract.artifactRef,sha256:contract.artifactSha256},evidence:[{kind:'hash-check',ref:'pilot/evidence/qualification-hash.json'}]}),nudge:false});
  repo.claim({qitemId:returned.qitemId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  repo.update({qitemId:returned.qitemId,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  svc.recordQualificationAssessmentReturn('builder@xv','builder-g1',{rigId:'xv',dutyQueueId:issued.queueId,returnQueueId:returned.qitemId});
  const review=svc.assessQualificationDuty('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:issued.queueId,finding:'evidence-sufficient',evidenceRef:'pilot/reviews/qualification-check.json'});
  expect(review.grantsAuthority).toBe(false);expect(svc.dutyFacts(issued.queueId).complete).toBe(true);
  expect(db.prepare("SELECT count(*) n FROM coordinator_packages WHERE rig_id='xv'").get()).toEqual({n:0});
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='xv' AND kind IN ('qualification','coordination-accept')").get()).toBeUndefined();
  repo.update({qitemId:issued.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(svc.dutyFacts(issued.queueId).close).toBe(true);
 });
 it('expired unclaimed legacy qualification work gets a report-only native disposition before any new assessment',async()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const old='legacy-qualification-assessment',sweep='legacy-qualification-sweep',old2='legacy-qualification-assessment-2',sweep2='legacy-qualification-sweep-2';
  const nowIso=new Date(clock).toISOString();
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: legacy-qualification-assessment\nwhy: wake failed before delivery; nothing retried it');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old2,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep2,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: legacy-qualification-assessment-2\nwhy: wake failed before delivery; nothing retried it');
  db.transaction(()=>repo.stageWakeIntent(old,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.transaction(()=>repo.stageWakeIntent(old2,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old);
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old2);
  const result=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000});
  expect(svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000})).toEqual(result);
  expect(svc.plan('xv')).toBeNull();expect(repo.getById(old)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');
  expect(svc.dutyFacts(result.queueId).send).toBe(true);
  await expect(repo.handoff({qitemId:old,fromSession:'operator-agent@kernel',toSession:'lead@xv',identityProvenance:'transport:v1',actorGeneration:'operator-agent-g1',nudge:false})).rejects.toMatchObject({code:'qualification_retirement_handoff_refused'});
  await expect(repo.handoffAndComplete({qitemId:old,fromSession:'operator-agent@kernel',toSession:'lead@xv',identityProvenance:'transport:v1',actorGeneration:'operator-agent-g1',nudge:false})).rejects.toMatchObject({code:'qualification_retirement_handoff_refused'});
  expect(()=>repo.closeCrossHostHandoffSource({qitemId:old,fromSession:'operator-agent@kernel',toSession:'lead@xv',closureTarget:'successor@remote',terminalState:'handed-off',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'})).toThrow('cannot be handed off');
  expect(repo.getById(old)?.state).toBe('pending');
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+old);
  expect(svc.dutyFacts(result.queueId).send).toBe(false);
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old);
  repo.claim({qitemId:result.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  expect(svc.qualificationAssessmentRetirementAllows(old,'builder@xv','builder-g1','transport:v1','canceled')).toBe(false);
  expect(refusal(()=>repo.claim({qitemId:old,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'})).code).toBe('qualification_retirement_target_not_claimable');
  expect(refusal(()=>repo.update({qitemId:old,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'premature'})).code).toBe('qualification_retirement_disposition_required');
  repo.claim({qitemId:sweep,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  expect(svc.dutyFacts(result.queueId).act).toBe(true);
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock-1);
  expect(refusal(()=>repo.update({qitemId:old,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'expired authority'})).code).toBe('qualification_retirement_disposition_required');
  expect(repo.getById(old)?.state).toBe('pending');db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock+60000);
  repo.update({qitemId:old,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'expired-unclaimed-qualification-task; original failed wake preserved; no retry'});
  repo.update({qitemId:sweep,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+result.queueId)).toBeTruthy();
  clock+=30001;vi.setSystemTime(clock);
  repo.update({qitemId:result.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on',transitionNote:'native expiry disposition recorded; original failed wake preserved'});
  expect(repo.getById(old)).toMatchObject({state:'failed',claimedAt:null});expect(repo.getById(sweep)?.state).toBe('done');expect(svc.dutyFacts(result.queueId).close).toBe(true);
  expect(db.prepare("SELECT count(*) n FROM coordinator_packages WHERE rig_id='xv'").get()).toEqual({n:0});
 });
 it('qualification retirement accepts the claimed typed accountability control for an exact failed-before-send wake without inventing a Worker finding',async()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const old='legacy-qualification-assessment-accountable',control='stuck-sweep-control-accountable',nowIso=new Date(clock).toISOString();
  const targetBody='# Legacy qualification assessment\nExpired before pickup.';
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'watchdog@system','builder@xv','pending',new Date(clock-1000).toISOString(),targetBody);
  db.transaction(()=>repo.stageWakeIntent(old,'watchdog@system','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old);
  const controlBody=JSON.stringify({action:'reconcile-refused-stuck-finding',stuckSweepRecoveryKey:'a'.repeat(64),previousQueueId:null,reason:'coordinator_dispatch_required',kind:'undelivered-wake',original:{qitemId:old,sourceSession:'watchdog@system',destinationSession:'builder@xv',bodyHash:digest(targetBody),state:'pending',evidenceAt:nowIso,factsHash:'b'.repeat(64)},intendedRoute:'builder@xv',recipientGeneration:'operator-agent-g1'});
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(control,nowIso,nowIso,'watchdog@system','operator-agent@kernel','pending',controlBody);
  repo.claim({qitemId:control,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const input={rigId:'xv',targetQueueId:old,targetBodyHash:digest(targetBody),evidenceKind:'operator-accountability' as const,accountabilityControlQueueId:control,accountabilityControlBodyHash:digest(controlBody),deadline:clock+30000};
  expect(refusal(()=>svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{...input,sweepFindingQueueId:'invented',sweepFindingBodyHash:'e'.repeat(64)})).code).toBe('qualification_retirement_evidence_invalid');
  const duty=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',input);
  expect(duty.targetQueueId).toBe(old);expect(repo.getById(old)?.state).toBe('pending');expect(repo.getById(control)?.state).toBe('in-progress');expect(svc.dutyFacts(duty.queueId).send).toBe(true);
  const receipt=svc.lifecycleControlReceipt(duty.queueId);expect(receipt).toMatchObject({evidenceKind:'operator-accountability',accountabilityControlQueueId:control,accountabilityControlBodyHash:digest(controlBody)});expect(receipt.sweepFindingQueueId).toBeUndefined();
  repo.claim({qitemId:duty.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  expect(svc.qualificationAssessmentRetirementAllows(old,'builder@xv','builder-g1','transport:v1','failed')).toBe(true);
  repo.update({qitemId:old,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'expired target; exact original wake failed before send; no retry'});
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+duty.queueId)).toBeTruthy();
  clock+=30001;vi.setSystemTime(clock);
  repo.update({qitemId:duty.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on',transitionNote:'exact legacy target terminalized; control and failed wake preserved'});
  expect(repo.getById(old)).toMatchObject({state:'failed',claimedAt:null,body:targetBody});expect(repo.getById(control)?.state).toBe('in-progress');expect(svc.dutyFacts(duty.queueId).close).toBe(true);
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE body LIKE '%STUCK SWEEP FINDING (undelivered-wake)%' AND body LIKE ?").get('%'+old+'%')).toEqual({n:0});
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+old)).toEqual({delivery_state:'failed'});
 });
 it.each(['indeterminate','sending'] as const)('accountability-backed qualification retirement preserves a %s original wake without issuing a duty',async deliveryState=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const old='legacy-qualification-accountability-unknown-'+deliveryState,control='stuck-sweep-control-unknown-'+deliveryState,nowIso=new Date(clock).toISOString(),targetBody='# Legacy qualification assessment\nExpired before pickup.';
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'watchdog@system','builder@xv','pending',new Date(clock-1000).toISOString(),targetBody);
  db.transaction(()=>repo.stageWakeIntent(old,'watchdog@system','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.prepare('UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?').run(deliveryState,'wake-intent-'+old);
  const controlBody=JSON.stringify({action:'reconcile-refused-stuck-finding',stuckSweepRecoveryKey:'c'.repeat(64),reason:'coordinator_dispatch_required',kind:'undelivered-wake',original:{qitemId:old,sourceSession:'watchdog@system',destinationSession:'builder@xv',bodyHash:digest(targetBody),state:'pending',evidenceAt:nowIso,factsHash:'d'.repeat(64)},intendedRoute:'builder@xv',recipientGeneration:'operator-agent-g1'});
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(control,nowIso,nowIso,'watchdog@system','operator-agent@kernel','pending',controlBody);
  repo.claim({qitemId:control,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const before=db.prepare('SELECT count(*) n FROM queue_items').get();
  const result=refusal(()=>svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(targetBody),evidenceKind:'operator-accountability',accountabilityControlQueueId:control,accountabilityControlBodyHash:digest(controlBody),deadline:clock+30000}));
  expect(result.code).toBe('qualification_retirement_effect_unknown');expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(before);expect(repo.getById(old)?.state).toBe('pending');expect(repo.getById(control)?.state).toBe('in-progress');
 });
 it('qualification retirement refuses a target Worker from a different rig',()=>{
  const old='legacy-qualification-assessment-cross-rig',sweep='legacy-qualification-sweep-cross-rig',nowIso=new Date(clock).toISOString();
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'operator-agent@kernel','worker@other','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','worker@other','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+old+'\nwhy: wake failed before delivery; nothing retried it');
  expect(refusal(()=>svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000})).code).toBe('qualification_retirement_worker_rig_mismatch');
  expect(repo.getById(old)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');
 });
 it('qualification assessment and retirement stage from refreshed identity and idle activity, rejecting a changed Worker generation',async()=>{
  const refreshed:string[]=[];let identityPending=false,deferNextIdentity=true,activityStartedWhileIdentityPending=false,resolveIdentity!:()=>void;
  const stageSvc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,async sessions=>{
   for(const session of sessions){
    refreshed.push('identity:'+session);
    identityPending=true;
   }
   if(deferNextIdentity){deferNextIdentity=false;await new Promise<void>(resolve=>{resolveIdentity=resolve;});}
   for(const session of sessions){
    identityPending=false;
    const current=samples.get(session)!;
    samples.set(session,{...current,identityVerified:true,identityObservedAt:new Date(clock).toISOString()});
   }
  },async session=>{
   refreshed.push('activity:'+session);
   activityStartedWhileIdentityPending=identityPending;
   const current=samples.get(session)!;
   const fresh=sample(session);
   samples.set(session,{...fresh,identityObservedAt:current.identityObservedAt??new Date(clock).toISOString()});
  });
  const stale=(session:string)=>{
   const old=new Date(clock-4001).toISOString(),current=samples.get(session)!;
   samples.set(session,{...current,identityObservedAt:old,witness:{...current.witness!,observedAt:old}});
  };
  const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'a'.repeat(64),taskDigest:'sha256:'+'b'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  stale('builder@xv');
  const builderConfig=stageSvc.configurationDigest('builder@xv')!;
  expect(refusal(()=>stageSvc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:builderConfig,deadline:clock+30000,contract})).message).toContain('fresh native observation prepared');
  const assessmentPending=stageSvc.prepareQualificationWorkerStageObservation('operator-agent@kernel','operator-agent-g1','xv','builder@xv','builder-g1',builderConfig);
  await Promise.resolve();
  expect(activityStartedWhileIdentityPending).toBe(true);
  resolveIdentity();
  const assessmentObservation=await assessmentPending;
  expect(refreshed.slice(0,2)).toEqual(['identity:builder@xv','activity:builder@xv']);
  const assessment=stageSvc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:builderConfig,deadline:clock+30000,contract},assessmentObservation);
  expect(repo.getById(assessment.queueId)?.destinationSession).toBe('builder@xv');

  const target='legacy-qualification-assessment-fresh-retirement',sweep='qualification-assessment-fresh-retirement-sweep',nowIso=new Date(clock).toISOString(),targetBody='# Legacy qualification assessment\nExpired before pickup.';
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(target,nowIso,nowIso,'operator-agent@kernel','architect@xv','pending',new Date(clock-1000).toISOString(),targetBody);
  db.transaction(()=>repo.stageWakeIntent(target,'operator-agent@kernel','architect@xv','system:operator-authorized-coordination',true,'architect-g1'))();
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+target);
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','architect@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+target+'\nwhy: wake failed before delivery; nothing retried it');
  stale('architect@xv');
  const retirementInput={rigId:'xv',targetQueueId:target,targetBodyHash:digest(targetBody),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000};
  expect(refusal(()=>stageSvc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',retirementInput)).message).toContain('fresh native observation prepared');
  const retirementObservation=await stageSvc.prepareQualificationRetirementStageObservation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:target});
  const retirement=stageSvc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',retirementInput,retirementObservation);
  expect(repo.getById(retirement.queueId)?.destinationSession).toBe('architect@xv');
  expect(refreshed.slice(2)).toEqual(['identity:architect@xv','activity:architect@xv']);

  const priorRows=db.prepare('SELECT count(*) n FROM queue_items').get();
  const changedSvc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,async sessions=>{
   for(const session of sessions){
    if(session==='reviewer@xv')db.prepare("UPDATE occupant_tenures SET generation_uuid='reviewer-g2' WHERE node_id='reviewer@xv'").run();
    const observed=sample(session);samples.set(session,{...observed,identityObservedAt:new Date(clock).toISOString()});
   }
  },async session=>{const observed=sample(session);samples.set(session,{...observed,identityObservedAt:new Date(clock).toISOString()});});
  stale('reviewer@xv');
  await expect(changedSvc.prepareQualificationWorkerStageObservation('operator-agent@kernel','operator-agent-g1','xv','reviewer@xv','reviewer-g1',changedSvc.configurationDigest('reviewer@xv')!)).rejects.toMatchObject({code:'qualification_duty_worker_stale'});
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(priorRows);
 });
 it.each(['configuration','stale-identity','stale-activity','observer-failure'] as const)('qualification concurrent observation holds on %s without creating custody',async failure=>{
  const beforeRows=db.prepare('SELECT count(*) n FROM queue_items').get();
  const freshSvc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,async sessions=>{
   for(const session of sessions){
    const current=samples.get(session)!;
    samples.set(session,{...current,identityObservedAt:new Date(clock-(failure==='stale-identity'?3001:0)).toISOString()});
   }
  },async session=>{
   if(failure==='observer-failure')throw new Error('native activity unavailable');
   if(failure==='configuration')db.prepare("UPDATE nodes SET model='changed-during-observation' WHERE id=?").run(session);
   const current=samples.get(session)!,fresh=sample(session);
   samples.set(session,{...fresh,identityObservedAt:current.identityObservedAt,witness:{...fresh.witness!,observedAt:new Date(clock-(failure==='stale-activity'?3001:0)).toISOString()}});
  });
  const configuration=freshSvc.configurationDigest('builder@xv')!;
  await expect(freshSvc.prepareQualificationWorkerStageObservation('operator-agent@kernel','operator-agent-g1','xv','builder@xv','builder-g1',configuration)).rejects.toMatchObject({code:failure==='configuration'?'qualification_duty_worker_stale':'qualification_duty_worker_not_quiescent'});
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(beforeRows);
 });
 it('qualification retirement does not observe completion after the Operator binding changes',()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const old='legacy-qualification-assessment-operator-rotation',sweep='legacy-qualification-sweep-operator-rotation',nowIso=new Date(clock).toISOString();
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+old+'\nwhy: wake failed before delivery; nothing retried it');
  db.transaction(()=>repo.stageWakeIntent(old,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old);
  const duty=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000});
  repo.claim({qitemId:duty.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  repo.claim({qitemId:sweep,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  repo.update({qitemId:old,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'expired target disposition under the original live binding'});
  db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();
  repo.update({qitemId:sweep,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+duty.queueId)).toBeUndefined();
  expect(repo.getById(old)?.state).toBe('failed');expect(repo.getById(sweep)?.state).toBe('done');
 });
 it('pre-upgrade Worker-sweep retirement identity replays and creates a contained successor without duplication',()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const old='legacy-qualification-assessment-retire-successor',sweep='legacy-qualification-sweep-retire-successor',nowIso=new Date(clock).toISOString();
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+old+'\nwhy: wake failed before delivery; nothing retried it');
  db.transaction(()=>repo.stageWakeIntent(old,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old);
  const input={rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+1000};
  const first=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',input);
  const firstReceipt=svc.lifecycleControlReceipt(first.queueId),legacySemanticKey=digest(JSON.stringify({rigId:'xv',targetQueueId:old,targetBodyHash:input.targetBodyHash,sweepFindingQueueId:sweep,sweepFindingBodyHash:input.sweepFindingBodyHash,worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')})),legacyContractDigest=digest(JSON.stringify({targetQueueId:old,targetBodyHash:input.targetBodyHash,sweepFindingQueueId:sweep,sweepFindingBodyHash:input.sweepFindingBodyHash}));
  expect(firstReceipt).toMatchObject({semanticKey:legacySemanticKey,contractDigest:legacyContractDigest,sweepFindingQueueId:sweep,sweepFindingBodyHash:input.sweepFindingBodyHash});expect(firstReceipt.evidenceKind).toBeUndefined();
  expect(svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',input)).toEqual(first);
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+first.queueId);clock+=1001;vi.setSystemTime(clock);
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const next=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{...input,deadline:clock+20000});
  expect(next.queueId).not.toBe(first.queueId);expect(JSON.parse(repo.getById(next.queueId)!.body).previousQueueId).toBe(first.queueId);expect(svc.dutyFacts(next.queueId).send).toBe(true);expect(repo.getById(first.queueId)?.state).toBe('pending');
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+next.queueId);clock+=20001;vi.setSystemTime(clock);samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const third=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{...input,deadline:clock+30000});
  expect(third.queueId).not.toBe(next.queueId);expect(JSON.parse(repo.getById(third.queueId)!.body).chainIds).toEqual([third.queueId,next.queueId,first.queueId]);expect(svc.dutyFacts(third.queueId).send).toBe(true);expect(repo.getById(first.queueId)?.state).toBe('pending');expect(repo.getById(next.queueId)?.state).toBe('pending');
 });
 it('qualification uncertainty disposition preserves UNKNOWN and contains only exact expired legacy pairs',async()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const nowIso=new Date(clock).toISOString();
  const pairs=['assessment-a','assessment-b'].map(label=>{
   const target='legacy-'+label,sweep='sweep-'+label;
   db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(target,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
   db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+target+'\nwhy: wake outcome unavailable; nothing retried it');
   return {target,sweep,targetBodyHash:digest(repo.getById(target)!.body),sweepFindingBodyHash:digest(repo.getById(sweep)!.body)};
  });
  const beforeOutbox=db.prepare('SELECT * FROM outbox_entries').all(),beforeTransitions=Object.fromEntries(pairs.flatMap(pair=>[pair.target,pair.sweep]).map(id=>[id,(db.prepare('SELECT count(*) n FROM queue_transitions WHERE qitem_id=?').get(id) as any).n]));
  const input={rigId:'xv',rows:pairs.map(pair=>({targetQueueId:pair.target,targetBodyHash:pair.targetBodyHash,sweepFindingQueueId:pair.sweep,sweepFindingBodyHash:pair.sweepFindingBodyHash})),deadline:clock+30000};
  expect(repo.getById(pairs[0].target)).toMatchObject({sourceSession:'operator-agent@kernel',destinationSession:'builder@xv',state:'pending'});
  const result=await svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input);
  expect(result).toMatchObject({outcome:'unknown-preserved',wakeReplayed:false,custodyTransferred:false});
  const receipt=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='qualification-assessment-uncertainty'").get(result.operationId) as any).receipt);
  expect(receipt).toMatchObject({queueDisposition:'terminalized-unresolvable',wakeDelivery:'unknown-not-proven-failed',taskExecution:'unknown',wakeReplayed:false,custodyTransferred:false});
  expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(beforeOutbox);
  for(const pair of pairs)for(const [id,bodyHash] of [[pair.target,pair.targetBodyHash],[pair.sweep,pair.sweepFindingBodyHash]] as const){
   expect(repo.getById(id)?.state).toBe('failed');
   expect(repo.getById(id)?.destinationSession).toBe('builder@xv');expect(digest(repo.getById(id)!.body)).toBe(bodyHash);
   expect((db.prepare('SELECT count(*) n FROM queue_transitions WHERE qitem_id=?').get(id) as any).n).toBe(beforeTransitions[id]+1);
   expect((db.prepare('SELECT transition_note FROM queue_transitions WHERE qitem_id=? ORDER BY transition_id DESC LIMIT 1').get(id) as any).transition_note).toContain('task execution UNKNOWN');
  }
  expect(()=>repo.update({qitemId:pairs[0].target,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'pending',reopen:true,transitionNote:'retry'})).toThrow('Only the exact append-only UNKNOWN-preserved disposition may terminalize this legacy row');
  expect(await svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input)).toEqual(result);
  expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(beforeOutbox);
  const beforeCount=db.prepare('SELECT count(*) n FROM queue_items').get();
  await expect(repo.handoff({qitemId:pairs[0].target,fromSession:'operator-agent@kernel',toSession:'builder@xv',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',nudge:false})).rejects.toMatchObject({code:'qualification_uncertainty_handoff_refused'});
  await expect(repo.handoffAndComplete({qitemId:pairs[0].target,fromSession:'operator-agent@kernel',toSession:'builder@xv',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',nudge:false})).rejects.toMatchObject({code:'qualification_uncertainty_handoff_refused'});
  expect(()=>repo.closeCrossHostHandoffSource({qitemId:pairs[0].target,fromSession:'operator-agent@kernel',toSession:'builder@xv',closureTarget:'successor@remote',terminalState:'handed-off',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'})).toThrow('uncertainty-disposition target cannot be handed off');
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(beforeCount);
  expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(beforeOutbox);
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'a'.repeat(64),taskDigest:'sha256:'+'b'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const duty=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+30000,contract});
  expect(duty.queueId).not.toBe(pairs[0].target);expect(repo.getById(pairs[0].target)?.state).toBe('failed');expect(repo.getById(pairs[1].target)?.state).toBe('failed');
  expect(db.prepare('SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE ?').get('wake-intent-'+duty.queueId)).toEqual({n:1});
  clock+=30001;vi.setSystemTime(clock);
  expect(await svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input)).toEqual(result);
 });
 it('qualification uncertainty disposition rejects malformed rows and invalid expiry without effects',async()=>{
  const before=db.prepare('SELECT count(*) n FROM queue_transitions').get();
  for(const input of [null,{rigId:{},rows:[],deadline:clock+30000},{rigId:[],rows:[],deadline:clock+30000}])await expect(svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input as any)).rejects.toMatchObject({code:'qualification_uncertainty_contract_invalid'});
  const malformed=[null,{targetQueueId:7,targetBodyHash:'a'.repeat(64),sweepFindingQueueId:'sweep',sweepFindingBodyHash:'b'.repeat(64)}];
  for(const row of malformed)await expect(svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',{rigId:'xv',rows:[row] as any,deadline:clock+30000})).rejects.toMatchObject({code:'qualification_uncertainty_contract_invalid'});
  const nowIso=new Date(clock).toISOString(),target='legacy-assessment-invalid-expiry',sweep='sweep-assessment-invalid-expiry';
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(target,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','not-a-date','qualification assessment legacy');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake) row: '+target);
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  await expect(svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',{rigId:'xv',rows:[{targetQueueId:target,targetBodyHash:digest(repo.getById(target)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body)}],deadline:clock+30000})).rejects.toMatchObject({code:'qualification_uncertainty_target_invalid'});
  expect(repo.getById(target)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');expect(db.prepare('SELECT count(*) n FROM queue_transitions').get()).toEqual(before);
 });
 it('qualification uncertainty disposition refuses a linked wake effect without changing custody',async()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const nowIso=new Date(clock).toISOString(),target='legacy-assessment-linked',sweep='sweep-assessment-linked';
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(target,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'qualification assessment legacy');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake) row: '+target);
  db.transaction(()=>repo.stageWakeIntent(target,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+target);
  const before=db.prepare('SELECT count(*) n FROM queue_transitions').get();
  await expect(svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',{rigId:'xv',rows:[{targetQueueId:target,targetBodyHash:digest(repo.getById(target)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body)}],deadline:clock+30000})).rejects.toMatchObject({code:'qualification_uncertainty_effect_linked'});
  expect(db.prepare('SELECT count(*) n FROM queue_transitions').get()).toEqual(before);expect(repo.getById(target)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+target)).toEqual({delivery_state:'indeterminate'});
 });
 it('qualification uncertainty disposition refreshes stale Worker identity just in time and still refuses without it',async()=>{
  const nowIso=new Date(clock).toISOString(),target='legacy-assessment-stale-worker',sweep='sweep-assessment-stale-worker';
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(target,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'qualification assessment legacy');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake) row: '+target);
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock-5000).toISOString()});
  const before=db.prepare('SELECT count(*) n FROM queue_transitions').get();
  const input={rigId:'xv',rows:[{targetQueueId:target,targetBodyHash:digest(repo.getById(target)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body)}],deadline:clock+30000};
  await expect(svc.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input)).rejects.toMatchObject({code:'qualification_uncertainty_worker_not_quiescent'});
  expect(db.prepare('SELECT count(*) n FROM queue_transitions').get()).toEqual(before);expect(repo.getById(target)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');
  const refreshCalls:string[][]=[];
  const refreshed=new CoordinationRecoveryService(repo,session=>samples.get(session)??null,()=>clock,async sessions=>{refreshCalls.push([...sessions]);samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});},async session=>{expect(session).toBe('builder@xv');samples.set(session,{...sample(session),identityObservedAt:new Date(clock).toISOString()});});
  const [result,replay]=await Promise.all([refreshed.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input),refreshed.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input)]);
  expect(refreshCalls).toEqual([['builder@xv'],['builder@xv']]);expect(result).toMatchObject({outcome:'unknown-preserved',wakeReplayed:false,custodyTransferred:false});expect(replay).toEqual(result);
  expect(repo.getById(target)?.state).toBe('failed');expect(repo.getById(sweep)?.state).toBe('failed');
  const receipt=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='qualification-assessment-uncertainty'").get(result.operationId) as any).receipt);
  expect(Date.parse(receipt.observationAt)).toBe(clock);expect(receipt.workerGeneration).toBe('builder-g1');expect(receipt.configurationDigest).toBe(svc.configurationDigest('builder@xv'));
  const callsBeforeUnauthorized=refreshCalls.length;
  await expect(refreshed.recordQualificationAssessmentUncertainty('builder@xv','builder-g1',input)).rejects.toMatchObject({code:'qualification_uncertainty_operator_required'});
  expect(refreshCalls).toHaveLength(callsBeforeUnauthorized);
  db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();
  await expect(refreshed.recordQualificationAssessmentUncertainty('operator-agent@kernel','operator-agent-g1',input)).rejects.toMatchObject({code:'qualification_uncertainty_operator_required'});
 });
 it.each(['indeterminate','sending'] as const)('qualification retirement preserves %s wake effects and issues no new duty',deliveryState=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const old='legacy-qualification-assessment-unknown',sweep='legacy-qualification-sweep-unknown',nowIso=new Date(clock).toISOString();
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+old+'\nwhy: wake failed before delivery; nothing retried it');
  db.transaction(()=>repo.stageWakeIntent(old,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.prepare('UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?').run(deliveryState,'wake-intent-'+old);
  const before=db.prepare('SELECT count(*) n FROM queue_items').get();
  expect(refusal(()=>svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000})).code).toBe('qualification_retirement_effect_unknown');
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(before);expect(repo.getById(old)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');
 });
 it('qualification retirement refuses unrelated Worker custody without creating a duty',()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const old='legacy-qualification-assessment-unrelated-custody',sweep='legacy-qualification-sweep-unrelated-custody',other='unrelated-worker-custody',nowIso=new Date(clock).toISOString();
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,expires_at,body) VALUES (?,?,?,?,?,?,?,?)').run(old,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending',new Date(clock-1000).toISOString(),'# Legacy qualification assessment\nExpired before pickup.');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(sweep,nowIso,nowIso,'operator-agent@kernel','builder@xv','pending','STUCK SWEEP FINDING (undelivered-wake)\nrow: '+old+'\nwhy: wake failed before delivery; nothing retried it');
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(other,nowIso,nowIso,'lead@xv','builder@xv','pending','Unrelated active assignment; preserve its custody.');
  db.transaction(()=>repo.stageWakeIntent(old,'operator-agent@kernel','builder@xv','system:operator-authorized-coordination',true,'builder-g1'))();
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+old);
  const before=db.prepare('SELECT count(*) n FROM queue_items').get();
  expect(refusal(()=>svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{rigId:'xv',targetQueueId:old,targetBodyHash:digest(repo.getById(old)!.body),sweepFindingQueueId:sweep,sweepFindingBodyHash:digest(repo.getById(sweep)!.body),deadline:clock+30000})).code).toBe('qualification_duty_worker_protected');
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(before);expect(repo.getById(old)?.state).toBe('pending');expect(repo.getById(sweep)?.state).toBe('pending');expect(repo.getById(other)?.state).toBe('pending');
 });
 it.each(['unauthorized','stale-worker','expired'] as const)('qualification-only duty refuses %s without creating a queue or review',kind=>{
  configure(normal());samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'a'.repeat(64),taskDigest:'sha256:'+'b'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const before=db.prepare('SELECT count(*) n FROM queue_items').get();let actor='operator-agent@kernel',generation='operator-agent-g1',workerGeneration='builder-g1',deadline=clock+30000;
  if(kind==='unauthorized'){actor='lead@xv';generation='lead-g1';}if(kind==='stale-worker')workerGeneration='builder-old';if(kind==='expired')deadline=clock;
  const error=refusal(()=>svc.stageQualificationAssessment(actor,generation,{rigId:'xv',worker:'builder@xv',workerGeneration,configurationDigest:svc.configurationDigest('builder@xv')!,deadline,contract}));
  expect(error.code).toMatch(kind==='unauthorized'?'qualification_duty_operator_required':kind==='stale-worker'?'qualification_duty_worker_stale':'qualification_duty_deadline_invalid');
  expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(before);
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind IN ('qualification-assessment-return','qualification-assessment-review')").get()).toBeUndefined();
 });
 it('qualification-only successor preserves an expired duty after its known wake is contained',()=>{
  configure(normal());samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'c'.repeat(64),taskDigest:'sha256:'+'d'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const first=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+1000,contract});const oldBody=repo.getById(first.queueId)!.body,oldDeadline=repo.getById(first.queueId)!.expiresAt;
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run('wake-intent-'+first.queueId);clock+=1001;vi.setSystemTime(clock);
  const next=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+20000,contract});
  expect(next.queueId).not.toBe(first.queueId);expect(repo.getById(first.queueId)?.body).toBe(oldBody);expect(repo.getById(first.queueId)?.expiresAt).toBe(oldDeadline);expect(JSON.parse(repo.getById(next.queueId)!.body).previousQueueId).toBe(first.queueId);
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+first.queueId)).toEqual({delivery_state:'delivered'});
 });
 it('planless qualification successors preserve multiple expired contained ancestors',()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-bootstrap.json',artifactSha256:'sha256:'+'1'.repeat(64),taskDigest:'sha256:'+'2'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const first=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+1000,contract});
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+first.queueId);clock+=1001;vi.setSystemTime(clock);
  const next=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+20000,contract});
  expect(next.queueId).not.toBe(first.queueId);expect(JSON.parse(repo.getById(next.queueId)!.body).previousQueueId).toBe(first.queueId);expect(svc.dutyFacts(next.queueId).send).toBe(true);expect(repo.getById(first.queueId)?.state).toBe('pending');
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+next.queueId);clock+=20001;vi.setSystemTime(clock);samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const third=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+20000,contract});
  expect(third.queueId).not.toBe(next.queueId);expect(JSON.parse(repo.getById(third.queueId)!.body).chainIds).toEqual([third.queueId,next.queueId,first.queueId]);expect(svc.dutyFacts(third.queueId).send).toBe(true);expect(repo.getById(first.queueId)?.state).toBe('pending');expect(repo.getById(next.queueId)?.state).toBe('pending');
 });
 it('planless qualification successor ignores contained expired report-only retirement history',()=>{
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-bootstrap.json',artifactSha256:'sha256:'+'3'.repeat(64),taskDigest:'sha256:'+'4'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const assessment=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+1000,contract}),assessmentBody=repo.getById(assessment.queueId)!.body;
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+assessment.queueId);clock+=1001;vi.setSystemTime(clock);samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const control='stuck-sweep-control-report-only-retirement',nowIso=new Date(clock).toISOString(),controlBody=JSON.stringify({action:'reconcile-refused-stuck-finding',stuckSweepRecoveryKey:'e'.repeat(64),previousQueueId:null,reason:'coordinator_dispatch_required',kind:'undelivered-wake',original:{qitemId:assessment.queueId,sourceSession:'watchdog@system',destinationSession:'builder@xv',bodyHash:digest(assessmentBody),state:'pending',evidenceAt:nowIso,factsHash:'f'.repeat(64)},intendedRoute:'builder@xv',recipientGeneration:'operator-agent-g1'});
  db.prepare('INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES (?,?,?,?,?,?,?)').run(control,nowIso,nowIso,'watchdog@system','operator-agent@kernel','pending',controlBody);
  repo.claim({qitemId:control,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const retirementInput={rigId:'xv',targetQueueId:assessment.queueId,targetBodyHash:digest(assessmentBody),evidenceKind:'operator-accountability' as const,accountabilityControlQueueId:control,accountabilityControlBodyHash:digest(controlBody),deadline:clock+1000};
  const retirement=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',retirementInput),retirementBody=repo.getById(retirement.queueId)!.body;
  expect(svc.lifecycleControlReceipt(retirement.queueId)).toMatchObject({evidenceKind:'operator-accountability',accountabilityControlQueueId:control,accountabilityControlBodyHash:digest(controlBody)});
  expect(repo.getById(assessment.queueId)?.state).toBe('pending');expect(repo.getById(retirement.queueId)?.state).toBe('pending');
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+retirement.queueId);clock+=1001;vi.setSystemTime(clock);samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const retirementSuccessor=svc.stageQualificationAssessmentRetirement('operator-agent@kernel','operator-agent-g1',{...retirementInput,deadline:clock+20000});
  expect(retirementSuccessor.queueId).not.toBe(retirement.queueId);expect(JSON.parse(repo.getById(retirementSuccessor.queueId)!.body).previousQueueId).toBe(retirement.queueId);
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run('wake-intent-'+retirementSuccessor.queueId);
  repo.claim({qitemId:retirementSuccessor.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  expect(svc.qualificationAssessmentRetirementAllows(assessment.queueId,'builder@xv','builder-g1','transport:v1','failed')).toBe(true);
  repo.update({qitemId:assessment.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'exact expired qualification assessment disposition under claimed report-only retirement successor'});
  clock+=20001;vi.setSystemTime(clock);
  repo.update({qitemId:retirementSuccessor.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on',transitionNote:'exact qualification assessment disposition recorded; no follow-on'});
  samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});
  const nextInput={rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+20000,contract};
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+retirement.queueId);
  expect(refusal(()=>svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',nextInput)).code).toBe('qualification_duty_worker_protected');
  db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-'+retirement.queueId);
  db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run(retirementBody+' drift',retirement.queueId);
  expect(refusal(()=>svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',nextInput)).code).toBe('qualification_duty_worker_protected');
  db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run(retirementBody,retirement.queueId);
  const nextAssessment=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',nextInput);
  expect(svc.dutyFacts(nextAssessment.queueId).send).toBe(true);
  repo.claim({qitemId:nextAssessment.queueId,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  expect(svc.dutyFacts(nextAssessment.queueId).act).toBe(true);
  expect(nextAssessment.queueId).not.toBe(assessment.queueId);expect(repo.getById(nextAssessment.queueId)?.state).toBe('in-progress');
  expect(repo.getById(assessment.queueId)).toMatchObject({state:'failed',body:assessmentBody});expect(repo.getById(retirement.queueId)).toMatchObject({state:'pending',body:retirementBody});expect(repo.getById(retirementSuccessor.queueId)?.state).toBe('done');
  expect(db.prepare('SELECT delivery_state FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+assessment.queueId)).toEqual({delivery_state:'failed'});
  expect(db.prepare('SELECT delivery_state FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+retirement.queueId)).toEqual({delivery_state:'failed'});
  expect(db.prepare('SELECT delivery_state FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+retirementSuccessor.queueId)).toEqual({delivery_state:'delivered'});
  expect(db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id IN (?,?,?) AND delivery_state='indeterminate'").get('wake-intent-'+assessment.queueId,'wake-intent-'+retirement.queueId,'wake-intent-'+retirementSuccessor.queueId)).toEqual({n:0});
 });
 it('qualification-only expired duty with UNKNOWN wake remains the sole hold and is never retried',()=>{
  configure(normal());samples.set('builder@xv',{...sample('builder@xv'),identityObservedAt:new Date(clock).toISOString()});const contract={schema:'qualification-assessment-contract.v1' as const,artifactRef:'pilot/qualification-check.json',artifactSha256:'sha256:'+'e'.repeat(64),taskDigest:'sha256:'+'f'.repeat(64),scope:'qualification-only' as const,productAuthority:false as const};
  const first=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+1000,contract});db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+first.queueId);clock+=1001;vi.setSystemTime(clock);
  const held=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+20000,contract});
  expect(held.queueId).toBe(first.queueId);expect(db.prepare("SELECT count(*) n FROM queue_items WHERE qitem_id LIKE 'qitem-coordination-lifecycle-%'").get()).toEqual({n:1});expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+first.queueId)).toEqual({delivery_state:'indeterminate'});
 });
 it('Operator successor control requires exact native intake and refuses uncertainty, drift and replay changes',async()=>{
  configure(normal(),{product:['source.ts']});const original=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;
  repo.claim({qitemId:original,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:original,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();const prior=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')!.queueId!;
  repo.claim({qitemId:prior,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:prior,actorSession:'builder@xv',state:'canceled'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();svc.reconcile('lead@xv','lead-g1','xv');
  const intake=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as {qitem_id:string}).qitem_id;
  const input={rigId:'xv',intakeQueueId:intake,previousControlId:prior,previousBodyHash:digest(repo.getById(prior)!.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',deadline:clock+30000,operationId:'bounded-return-r2'};
  expect(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',input)).toThrow('Exact claimed exhaustion');
  repo.claim({qitemId:intake,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  await repo.create({qitemId:'forged-exhaustion',sourceSession:'builder@xv',destinationSession:'operator-agent@kernel',body:repo.getById(intake)!.body,nudge:false});repo.claim({qitemId:'forged-exhaustion',destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  expect(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{...input,intakeQueueId:'forged-exhaustion'})).toThrow('Exact native exhaustion intake');
  expect(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{...input,previousBodyHash:'drift'})).toThrow();
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE audit_pointer=?").run(prior);
  expect(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',input)).toThrow('Reconcile unknown effects');
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const result=svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',input);
  expect(result.queueId).not.toBe(prior);expect(repo.getById(prior)!.state).toBe('canceled');expect(repo.getById(result.queueId)!.state).toBe('pending');
  expect(svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',input)).toEqual(result);
  expect(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{...input,operationId:'duplicate-r3'})).toThrow('one authorized successor');
  expect(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{...input,deadline:clock+31000})).toThrow('Frozen successor');
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','builder@xv',result.queueId)).not.toThrow();
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')).toMatchObject({state:'pending-native-terminal-return',queueId:result.queueId});
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:1});
  repo.claim({qitemId:result.queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  expect(()=>repo.update({qitemId:result.queueId,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'})).toThrow('Original assignment still lacks');
  await repo.create({qitemId:'successor-real-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'retained/report.md'}]}),nudge:false});
  expect(()=>repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product',undefined as any)).toThrow('Expected {rigId,packageKey,dispositionId}');
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','successor-real-return');
  repo.update({qitemId:result.queueId,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='product'").get()).toEqual({n:0});
 });
 it('missing terminal returns respect checkpoint quiescence and never assign a changed incarnation',()=>{
  const initial=configure([...normal(),task('other','builder@xv',{boundary:'owner-material'})],{product:['source.ts']});
  const original=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='product')!.queueId!;
  repo.claim({qitemId:original,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  repo.update({qitemId:original,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'terminal-quiescence',dispatchRestrictions:[{session:'builder@xv',generation:'builder-g1',packageKeys:['other'],validUntil:clock+30000,evidenceRef:'checkpoint.json'}]});
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')?.reason).toBe('checkpoint-quiescence');
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE qitem_id LIKE 'qitem-coordination-terminal-return-%'").get()).toEqual({n:0});
  db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='retired-builder' WHERE qitem_id=?").run(original);
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='terminal-return:product')?.reason).toBe('terminal-return-incarnation-changed');
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE qitem_id LIKE 'qitem-coordination-terminal-return-%'").get()).toEqual({n:0});
  expect(db.prepare('SELECT count(*) n FROM coordinator_resources WHERE package_key=?').get('product')).toEqual({n:1});
 });
 it('actual Architect return is followed through only after exact holder acceptance',async()=>{
  const q='qitem-coordination-'+digest('xv:decision').slice(0,24);
  configure([task('decision','architect@xv'),task('decision-repair','reviewer@xv',{recoveryFor:'decision'}),task('next','builder@xv',{predecessors:[{queueId:q,dispositionId:'returned'}]}),task('next-repair','reviewer@xv',{recoveryFor:'next'})]);
  svc.reconcile('lead@xv','lead-g1','xv');repo.claim({qitemId:q,destinationSession:'architect@xv',identityProvenance:'transport:v1'});
  repo.update({qitemId:q,actorSession:'architect@xv',state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:'returned',sourceSession:'architect@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'decision',inputDigest:digest('decision'),evidence:[{kind:'report',ref:'bounded/decision.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('architect@xv','architect-g1','xv','decision','returned');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='decision')?.state).toBe('returned-awaiting-acceptance');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='next')?.reason).toBe('predecessor-disposition');
  svc.accept('lead@xv','lead-g1','xv','decision','returned','bounded/technical-acceptance.md');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='decision')?.state).toBe('accepted');
  const next=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='next')!;expect(next.state).toBe('pending-pickup');
  repo.claim({qitemId:next.queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='next')?.state).toBe('picked-up');
 });
 it('resource conflict is bounded to its slice, not rollback of independent frontier',()=>{
  configure([task('conflict','reviewer@xv'),task('conflict-repair','architect@xv',{recoveryFor:'conflict'}),...normal()],{conflict:['shared']});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','elsewhere',{inputDigest:digest('old'),destination:'architect@xv',bodyHash:digest('old'),resources:['shared'],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  db.prepare("INSERT INTO coordinator_resources VALUES ('xv','shared','elsewhere')").run();
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result.find(x=>x.key==='conflict')?.reason).toBe('coordinator_resource_conflict');expect(result.find(x=>x.key==='product')?.state).toBe('pending-pickup');expect(db.prepare("SELECT package_key FROM coordinator_resources WHERE resource_key='shared'").get()).toEqual({package_key:'elsewhere'});
 });
 it('uncertain dispatch effects refuse takeover and preserve old custody',()=>{
  configure(normal());svc.reconcile('lead@xv','lead-g1','xv');job();clock+=11000;vi.setSystemTime(clock);refresh();const result=svc.supervise('xv','j');expect(result?.find(r=>r.key==='coordinator')?.reason).toBe('coordinator_uncertain_effects');expect(result?.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);
 });
 it('restart reloads obligations and cannot duplicate an unclaimed assignment',()=>{
  configure(normal());const first=svc.reconcile('lead@xv','lead-g1','xv');const successor=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);expect(successor.reconcile('lead@xv','lead-g1','xv')[0].queueId).toBe(first[0].queueId);expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:1});
 });

 it('expired or changed admission holds only its task and schedules an eligible recovery owner',()=>{
  configure(normal());clock+=60001;vi.setSystemTime(clock);refresh();db.prepare("UPDATE coordinator_authority SET lease_until=?").run(clock+10000);
  const old=svc.plan('xv')!;const repair=old.tasks[1];svc.configure('operator-agent@kernel','operator-agent-g1',{...old,revision:'r2',tasks:[{...old.tasks[0],admission:{...old.tasks[0].admission,validUntil:clock+10000}}, {...repair,admission:{...repair.admission,validUntil:clock+10000}}]});
  db.prepare("UPDATE nodes SET model='changed' WHERE id='builder@xv'").run();const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result[0].reason).toBe('current-admission-required');expect(result[1].state).toBe('pending-pickup');
 });
 it('cannot invent capacity or depend on the failing task to start its recovery',()=>{
  const t=task('product'),r=task('repair','architect@xv',{recoveryFor:'product',predecessors:[{queueId:'qitem-coordination-'+digest('xv:product').slice(0,24),dispositionId:'r'}]});expect(()=>configure([t,r])).toThrow('Recovery cannot depend');
  expect(()=>configure([{...t,admission:{...t.admission,capacityRef:''}},task('repair','architect@xv',{recoveryFor:'product'})])).toThrow('qualification/capacity/effort');
 });

 it('same-host alias claim is existing custody, not idle spare capacity',()=>{
  configure(normal());db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,priority,body,claimed_by_generation_uuid) VALUES ('alias',?,?,'lead@xv','builder@xv@fixture-host','in-progress','normal','old','builder-g1')").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  expect(svc.reconcile('lead@xv','lead-g1','xv')[0].reason).toBe('existing-worker-custody');expect(repo.getById('alias')?.state).toBe('in-progress');
 });

 it('the actual shared arbiter degrades stale hooks and yields fresh sampling for dispatch',()=>{
  configure(normal());const session='builder@xv',generation=repo.coordinatorAuthority.generation(session)!;
  const ladder=new SeatActivityService({tmux:{readPaneLastActivity:async()=>null},defaultWindowSeconds:3,now:()=>new Date(clock)});
  ladder.declareRungInventory({seatNodeId:session,sessionName:session},{adapterId:'codex',runtime:'codex',rungs:[]});ladder.declareOccupantSwap(session,generation);
  ladder.declareRungInventory({seatNodeId:session,sessionName:session},{adapterId:'codex',runtime:'codex',rungs:[{rung:'lifecycle-hooks',lifecycleCoverage:'full',initialTrust:'authoritative'},{rung:'window-sampling',lifecycleCoverage:'full',initialTrust:'authoritative'}]});
  ladder.reportEvidence({seatNodeId:session,sessionName:session,rung:'lifecycle-hooks',sourceId:'old-hook',seq:1,observedAt:new Date(clock-60000).toISOString(),activity:'working'});
  ladder.reportEvidence({seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:2,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'});
  const state=ladder.getSeatState(session)!;expect(state.decidedBy).toBe('window-sampling');
  samples.set(session,{generation,identityVerified:true,state,witness:ladder.getRotationActivityWitness(session)});
  expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('pending-pickup');
 });

 it('disposed failed return remains failure and activates real separately admitted recovery pickup',async()=>{
  configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;
  repo.claim({qitemId:q,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:q,actorSession:'builder@xv',state:'failed'});
  await repo.create({qitemId:'failed-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'failed/report.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','failed-return');
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result[0].state).toBe('recovery-required:failed');expect(result[1].state).toBe('pending-pickup');expect(()=>svc.accept('lead@xv','lead-g1','xv','product','failed-return','holder/failure.md')).toThrow('Exact successful');
  repo.claim({qitemId:result[1].queueId!,destinationSession:'architect@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv')[1].state).toBe('picked-up');expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE package_key='product'").get()).toEqual({disposition_id:'failed-return'});
 });
 it('raw SQLite reservation trigger holds its seat while earlier and later independent tasks commit',()=>{
  configure([task('review','reviewer@xv'),task('review-repair','architect@xv',{recoveryFor:'review'}),...normal()]);
  db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('reservation','rotation','reviewer@xv','reviewer@xv','reviewer-g1','native-old','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result.find(r=>r.key==='review')?.reason).toBe('seat_dispatch_reserved');expect(result.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(db.prepare("SELECT state FROM seat_dispatch_reservations WHERE reservation_id='reservation'").get()).toEqual({state:'reserved'});expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='review'").get()).toBeUndefined();
 });
 it('ineligible Peer takeover is a durable bounded hold, and current holder still dispatches ready work',async()=>{
  configure(normal());job();await repo.create({qitemId:'peer-existing',sourceSession:'operator-agent@kernel',destinationSession:'peer@xv',body:'existing obligation',nudge:false});clock+=11000;vi.setSystemTime(clock);refresh();
  const result=svc.supervise('xv','j');expect(result?.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(result?.find(r=>r.key==='coordinator')?.reason).toBe('coordination_stall_unproven');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('peer-existing')?.state).toBe('pending');
  const row=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-takeover-hold'").get() as {receipt:string};const receipt=JSON.parse(row.receipt);expect(receipt.owner).toBe('operator-agent@kernel');expect(receipt.action).toContain('Reconcile exact Peer');expect(receipt.deadline).toBeGreaterThan(clock);
 });
 it('transitive recovery path back to failed target is rejected before any plan/assignment effects',()=>{
  const q=(k:string)=>'qitem-coordination-'+digest('xv:'+k).slice(0,24);
  const tasks=[task('product'),task('dependent','reviewer@xv',{predecessors:[{queueId:q('product'),dispositionId:'product-return'}]}),task('repair','architect@xv',{recoveryFor:'product',predecessors:[{queueId:q('dependent'),dispositionId:'dependent-return'}]}),task('dependent-repair','builder@xv',{recoveryFor:'dependent'})];
  expect(()=>configure(tasks)).toThrow('transitively');expect(svc.plan('xv')).toBeNull();expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:0});
 });
 it.each(['matching','conflicting'] as const)('retained deterministic queue row without assignment holds its slice (%s) while independent work proceeds',async(kind)=>{
  configure([task('review','peer@xv'),task('review-repair','architect@xv',{recoveryFor:'review'}),...normal()]);
  const id='qitem-coordination-'+digest('xv:review').slice(0,24);await repo.create({qitemId:id,sourceSession:'operator-agent@kernel',destinationSession:'peer@xv',body:kind==='matching'?'review':'different historical row',nudge:false});
  repo.claim({qitemId:id,destinationSession:'peer@xv'});repo.update({qitemId:id,state:'done',actorSession:'peer@xv',closureReason:'no-follow-on',note:'Retained historical completion'});
  const before=repo.getById(id);const result=svc.reconcile('lead@xv','lead-g1','xv');
  expect(result.find(r=>r.key==='review')).toMatchObject({state:'held',queueId:id,reason:kind==='matching'?'existing-queue-without-assignment':'deterministic-queue-conflict'});expect(result.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(repo.getById(id)).toEqual(before);expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='review'").get()).toBeUndefined();
  const notices=()=>db.prepare("SELECT qitem_id,body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.action')='resolve-exact-coordination-task-hold'").all() as Array<{qitem_id:string;body:string}>;
  expect(notices()).toHaveLength(1);const notice=notices()[0];
  expect(JSON.parse(notice.body)).toMatchObject({packageKey:'review',retainedQueueId:id,recipientGeneration:'operator-agent-g1',grantsAuthority:false});
  repo.claim({qitemId:notice.qitem_id,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(notice.qitem_id)).toEqual({claimed_by_generation_uuid:'operator-agent-g1'});
  svc.reconcile('lead@xv','lead-g1','xv');expect(notices()).toHaveLength(1);
  repo.update({qitemId:notice.qitem_id,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  svc.reconcile('lead@xv','lead-g1','xv');expect(notices()).toHaveLength(1);expect(repo.getById(id)).toEqual(before);
 });
 it('unknown SQLite failure still aborts instead of being swallowed as seat reservation',()=>{
  configure(normal());db.exec("CREATE TRIGGER unknown_failure BEFORE INSERT ON queue_items WHEN NEW.destination_session='builder@xv' BEGIN SELECT RAISE(ABORT,'unrecognized_data_corruption'); END");expect(()=>svc.reconcile('lead@xv','lead-g1','xv')).toThrow('unrecognized_data_corruption');expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:0});
 });
 it('reserved Operator intake cannot roll back an independent ready assignment',()=>{
  const expired=task('expired','reviewer@xv');expired.admission.validUntil=clock+1;
  configure([expired,task('expired-repair','architect@xv',{recoveryFor:'expired'}),...normal()]);clock+=2;vi.setSystemTime(clock);refresh();
  db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('op-reservation','op-rotation','operator-agent@kernel','operator-agent@kernel','operator-agent-g1','native-old','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const result=svc.reconcile('lead@xv','lead-g1','xv');
  expect(result.find(r=>r.key==='expired')?.reason).toBe('current-admission-required');expect(result.find(r=>r.key==='product')?.state).toBe('pending-pickup');
  // R2: the held admission-refresh result is PROPAGATED into reconciliation, so a protected
  // duty is an EXPLICIT accountable hold rather than a silent one. No duty row exists yet
  // (protection is evaluated before staging) and the legacy reason stays unrouted.
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh'").get()).toEqual({n:0});
  const propagated=result.find(r=>r.key==='admission-refresh:expired');
  expect(propagated).toBeTruthy();
  expect(propagated!.state).toBe('held');
  expect(propagated!.reason).toBe('lifecycle-recipient-protected');
  // The independent ready assignment is NOT rolled back by any of this.
  expect(result.find(r=>r.key==='product')?.state).toBe('pending-pickup');
  // Exactly ONE mechanism accounts for the hold: never the legacy current-admission-required.
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE json_valid(body)=1 AND json_extract(body,'$.reason')='current-admission-required'").get()).toEqual({n:0});
  // Once the reservation clears, the native duty stages on the next reconcile.
  // The reservation itself is untouched by anything above.
  expect(db.prepare("SELECT state FROM seat_dispatch_reservations WHERE reservation_id='op-reservation'").get()).toEqual({state:'reserved'});
  db.prepare("UPDATE seat_dispatch_reservations SET state='released' WHERE reservation_id='op-reservation'").run();
  const after=svc.reconcile('lead@xv','lead-g1','xv');
  expect(after.find(r=>r.key==='product')?.state).toBe('pending-pickup');
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='expired'").get()).toEqual({n:1});
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='expired'").get()).toEqual({n:1});
 });

 it('cyclic recovery activation chain cannot masquerade as a ready independent plan',()=>{
  expect(()=>configure([task('a','builder@xv',{recoveryFor:'b'}),task('b','architect@xv',{recoveryFor:'a'})])).toThrow('cannot cycle');expect(svc.plan('xv')).toBeNull();
 });

 it('undispatched dependent with busy owner never activates recovery before prerequisite acceptance',()=>{
  const dependent=task('dependent','builder@xv',{predecessors:[{queueId:'qitem-coordination-'+digest('xv:product').slice(0,24),dispositionId:'not-returned'}],deadline:clock+1});
  configure([...normal(),dependent,task('repair-dependent','reviewer@xv',{recoveryFor:'dependent'})]);
  const product=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='product')!;
  repo.claim({qitemId:product.queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});samples.get('builder@xv')!.state.activity='working';clock+=2;
  const results=svc.reconcile('lead@xv','lead-g1','xv');expect(results.find(x=>x.key==='dependent')?.reason).toBe('predecessor-disposition');expect(results.find(x=>x.key==='repair-dependent')?.reason).toBe('recovery-not-needed');
  expect(db.prepare("SELECT COUNT(*) n FROM coordinator_assignments WHERE package_key='repair-dependent'").get()).toEqual({n:0});
 });
 it('current recipient or Operator recovers expired reconciling lease once with exact custody, never acknowledges implicitly',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();svc.supervise('xv','j');
  const authority=repo.coordinatorAuthority,a=authority.get('xv')!;expect(a.state).toBe('reconciling');expect(a.lease_until-clock).toBe(300000);
  clock=a.lease_until+1;vi.setSystemTime(clock);const notice=svc.supervise('xv','j')![0];expect(notice.state).toBe('pending-reconciliation-recovery');repo.claim({qitemId:notice.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});expect(svc.supervise('xv','j')![0].queueId).toBe(notice.queueId);const input={token:{rigId:'xv',epoch:a.epoch,generation:'peer-g1'},operationId:'bounded-recovery',obligationsDigest:authority.reconciliationDigest('xv'),windowMs:60000};
  expect(()=>authority.recoverReconciliation('lead@xv','lead-g1',input)).toThrow();expect(()=>authority.recoverReconciliation('operator-agent@kernel','retired',input)).toThrow();expect(()=>authority.recoverReconciliation('peer@xv','peer-g1',{...input,obligationsDigest:'stale'})).toThrow('exact current custody');
  const recovered=authority.recoverReconciliation('peer@xv','peer-g1',input);expect(recovered.state).toBe('reconciling');expect(repo.getById('baton')?.state).toBe('pending');expect(authority.recoverReconciliation('peer@xv','peer-g1',input)).toEqual(recovered);
  clock=recovered.lease_until+1;vi.setSystemTime(clock);expect(()=>authority.recoverReconciliation('operator-agent@kernel','operator-agent-g1',{...input,operationId:'again'})).toThrow('One bounded');
 });
 it('separately admitted feedback is picked up without modifying blocked parent custody or bypassing raw fence',()=>{
  configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;repo.claim({qitemId:q,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:q,actorSession:'builder@xv',state:'blocked'});
  const before=repo.getById(q),body=JSON.stringify({action:'reconcile-existing-custody',parentQueueId:q,parentPackageKey:'product',instruction:'Return denied with exact premature-dispatch evidence; do not perform dependent work'});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','feedback',{inputDigest:digest(body),destination:'builder@xv',bodyHash:digest(body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  const input={rigId:'xv',epoch:1,parentPackageKey:'product',parentQueueId:q,workerGeneration:'builder-g1',feedbackPackageKey:'feedback',body};
  expect(()=>svc.continueCustody('lead@xv','retired',input)).toThrow();expect(()=>svc.continueCustody('lead@xv','lead-g1',{...input,parentQueueId:'wrong'})).toThrow('Exact current claimed');
  const feedback=svc.continueCustody('lead@xv','lead-g1',input);expect(svc.continueCustody('lead@xv','lead-g1',input)).toEqual(feedback);expect(repo.getById(q)).toEqual(before);
  repo.claim({qitemId:feedback.queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(feedback.queueId)).toEqual({claimed_by_generation_uuid:'builder-g1'});expect(repo.getById(q)?.state).toBe('blocked');
  expect(()=>repo.coordinatorAuthority.assertRawSend('lead@xv','builder@xv')).toThrow('Raw managed sends');
 });

 it('two real processes cannot extend one expired reconciliation twice',async()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();svc.supervise('xv','j');const a=repo.coordinatorAuthority.get('xv')!;clock=a.lease_until+1;vi.setSystemTime(clock);
  const base={token:{rigId:'xv',epoch:a.epoch,generation:'peer-g1'},obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv'),windowMs:60000};
  const code=`import D from 'better-sqlite3';import {CoordinatorAuthorityService} from './dist/domain/coordinator-authority-service.js';const db=new D(process.argv[1]);const s=new CoordinatorAuthorityService(db,undefined,undefined,()=>Number(process.argv[2]));try{s.recoverReconciliation('operator-agent@kernel','operator-agent-g1',JSON.parse(process.argv[3]));console.log('extended');}catch(e){console.log(e.code);process.exitCode=7;}finally{db.close();}`;
  const run=async(op:string)=>{const child=spawn(process.execPath,['--input-type=module','-e',code,join(dir,'db'),String(clock),JSON.stringify({...base,operationId:op})],{cwd:resolve('.'),stdio:['ignore','pipe','pipe']});let out='';child.stdout.on('data',b=>out+=b);const [exit]=await once(child,'exit');return {exit,out};};
  const results=await Promise.all([run('race-a'),run('race-b')]);expect(results.filter(r=>r.exit===0)).toHaveLength(1);expect(results.filter(r=>r.exit===7)).toHaveLength(1);expect(db.prepare("SELECT COUNT(*) n FROM coordinator_operations WHERE kind='reconciliation-recover'").get()).toEqual({n:1});
 });

 it('Operator recovered window still requires genuine Peer custody acknowledgment and fences retired Lead',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();svc.supervise('xv','j');const authority=repo.coordinatorAuthority,a=authority.get('xv')!;clock=a.lease_until+1;vi.setSystemTime(clock);
  const token={rigId:'xv',epoch:a.epoch,generation:'peer-g1'};authority.recoverReconciliation('operator-agent@kernel','operator-agent-g1',{token,operationId:'operator-recovery',obligationsDigest:authority.reconciliationDigest('xv'),windowMs:60000});
  expect(()=>svc.reconcile('peer@xv','peer-g1','xv')).toThrow('Only reconciled');const digestNow=authority.reconciliationDigest('xv');
  expect(()=>authority.acknowledge('lead@xv',{...token,generation:'lead-g1'},{operationId:'retired-ack',obligationsDigest:digestNow})).toThrow();
  expect(authority.acknowledge('peer@xv',token,{operationId:'real-peer-ack',obligationsDigest:digestNow}).state).toBe('active');expect(repo.getById('baton')?.state).toBe('in-progress');
 });

 it('fresh working owner is scheduling, but stale busy evidence cannot indefinitely suppress ready recovery',()=>{
  configure(normal());const busy=samples.get('builder@xv')!;busy.state.activity='working';busy.witness!.activity='working';
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='repair')?.reason).toBe('recovery-not-needed');
  clock+=3001;vi.setSystemTime(clock);samples.set('architect@xv',sample('architect@xv'));
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='repair')?.state).toBe('pending-pickup');
 });

 function unavailableSetup(){const prior=configure(normal());svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'unavailable-r2',allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true});job();repo.coordinatorAuthority.renew('lead@xv',token,10000,'short-lease');clock+=10001;vi.setSystemTime(clock);refresh();}
 function observer(state:RuntimeAvailability['state']='absent',age=0,generation='lead-g1'){repo.coordinatorAuthority.setRuntimeObserver(async session=>({session,generation:session==='lead@xv'?generation:'peer-g1',state:session==='lead@xv'?state:'present',observedAt:clock-age,fingerprint:'actual-test-census-'+session}));}
 it('admitted unavailable expired owner transfers automatically with actual Peer wake/claim/ACK preserving worker custody',async()=>{const initial=configure(normal(),{product:['file:valuable']});const work=svc.reconcile('lead@xv','lead-g1','xv')[0];repo.claim({qitemId:work.queueId!,destinationSession:'builder@xv'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE delivery_state='pending'").run();svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'unavailable-r2',allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true});job();const before=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(work.queueId),resourcesBefore=db.prepare('SELECT * FROM coordinator_resources').all();repo.coordinatorAuthority.renew('lead@xv',token,10000,'short-lease');clock+=10001;vi.setSystemTime(clock);refresh();observer();db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('peer-binding','peer@xv','peer@xv','%2')").run();const sends:string[]=[];repo.attachTransport({send:async(session,text,opts)=>{sends.push(opts!.queueAssignmentId!);repo.claim({qitemId:opts!.queueAssignmentId!,destinationSession:session});return {ok:true,verified:true};}});await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);const a=repo.coordinatorAuthority.get('xv')!;expect(a).toMatchObject({epoch:2,state:'reconciling',owner_session:'peer@xv'});expect(sends).toHaveLength(1);expect(repo.getById(sends[0])?.state).toBe('in-progress');expect(repo.getById('baton')?.state).toBe('pending');expect(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(work.queueId)).toEqual(before);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resourcesBefore);expect(()=>repo.coordinatorAuthority.renew('lead@xv',token,60000,'retired')).toThrow();repo.coordinatorAuthority.acknowledge('peer@xv',{rigId:'xv',epoch:2,generation:'peer-g1'},{operationId:'peer-real-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});expect(repo.getById('baton')?.state).toBe('in-progress');expect(svc.reconcile('peer@xv','peer-g1','xv')[0].state).toBe('picked-up');});
 it('automatic continuity still supervises an unavailable owner when its activity poll fails',async()=>{
  unavailableSetup();const prior=svc.plan('xv')!;
  svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'unavailable-observer',refreshDispatchIdentity:true});
  observer();
  const poll=vi.fn(async(session:string)=>{if(session==='lead@xv')throw new Error('native-worker-activity-unavailable');samples.set(session,sample(session));});
  svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,async()=>{},poll);repo.coordinatorAuthority.coordinationRecovery=svc;
  const before=db.prepare('SELECT * FROM coordinator_resources').all();
  await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority,async()=>{}).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  expect(poll).toHaveBeenCalledWith('lead@xv');
  expect(repo.coordinatorAuthority.get('xv')).toMatchObject({epoch:2,state:'reconciling',owner_session:'peer@xv'});
  expect(repo.getById('baton')?.state).toBe('pending');
  expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(before);
 });
 it.each(['present','unknown'] as const)('expired lease plus %s native owner never permits unavailable takeover',async(state)=>{unavailableSetup();observer(state);await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(svc.supervise('xv','j')?.[0].state).toBe('recovery-required');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.state).toBe('in-progress');});
 it('stale or wrong generation native absence cannot substitute for current exclusion',async()=>{unavailableSetup();observer('absent',1001);await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(repo.coordinatorAuthority.hasFreshUnavailableOwner('xv')).toBe(false);expect(svc.supervise('xv','j')?.[0].state).toBe('recovery-required');observer('absent',0,'retired');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(repo.coordinatorAuthority.hasFreshUnavailableOwner('xv')).toBe(false);expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});
 it.each(['busy-peer','expired-admission','changed-baton','uncertain-effects','reserved-peer','old-operator'])('unavailable-owner %s refusal is accountable and preserves existing epoch/baton',async(reason)=>{unavailableSetup();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');if(reason==='busy-peer')samples.get('peer@xv')!.state.activity='working';if(reason==='expired-admission'){clock+=60000;vi.setSystemTime(clock);refresh();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');}if(reason==='changed-baton')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='retired' WHERE qitem_id='baton'").run();if(reason==='uncertain-effects')repo.stageWakeIntent('baton','lead@xv','lead@xv','transport:v1',true);if(reason==='reserved-peer')db.exec("CREATE TEMP TRIGGER test_reserved_peer BEFORE INSERT ON queue_items WHEN NEW.destination_session='peer@xv' BEGIN SELECT RAISE(ABORT,'seat_dispatch_reserved'); END");if(reason==='old-operator')db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();if(reason==='old-operator')expect(()=>svc.supervise('xv','j')).toThrow('Current Operator');else{const held=svc.supervise('xv','j')![0];expect(held.state).toBe('held');expect(held.queueId).toBeTruthy();expect(repo.getById(held.queueId!)?.destinationSession).toBe('operator-agent@kernel');}expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.destinationSession).toBe('lead@xv');});
 it('fresh return of native owner or changed expected epoch refuses preobserved transfer before effects',async()=>{unavailableSetup();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');const input={rigId:'xv',expectedEpoch:1,expectedOwner:'lead@xv',expectedOwnerGeneration:'lead-g1',planRevision:'unavailable-r2',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:60000};observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',input)).toThrow('native absence');observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',{...input,expectedEpoch:2})).toThrow('predecessor changed');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});

 it('unavailable transfer strict opt-in and actual Peer current generation are mandatory',async()=>{configure(normal());job();repo.coordinatorAuthority.renew('lead@xv',token,10000,'short');clock+=10001;vi.setSystemTime(clock);refresh();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(svc.supervise('xv','j')?.[0].state).toBe('recovery-required');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});

 it('live lease, wrong Peer generation and stale Peer witness prevent otherwise admitted outage transfer',async()=>{const prior=configure(normal());svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'unavailable-r2',allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true});job();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');const input={rigId:'xv',expectedEpoch:1,expectedOwner:'lead@xv',expectedOwnerGeneration:'lead-g1',planRevision:'unavailable-r2',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:60000};expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',input)).toThrow('Expired lease');repo.coordinatorAuthority.renew('lead@xv',token,10000,'short');clock+=10001;vi.setSystemTime(clock);refresh();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',{...input,recipientGeneration:'retired'})).toThrow('generation');samples.get('peer@xv')!.witness!.observedAt=new Date(clock-3001).toISOString();expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',input)).toThrow('Fresh idle');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.state).toBe('in-progress');});
 it('strict unavailable opt-in never inherits from a truthy string',()=>{const prior=configure(normal());expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'badoptin',allowUnavailablePeerTransfer:'true' as any})).toThrow('strict explicit boolean');});

 it('closing an expired-holder recovery notice without restoring authority does not erase the recovery obligation',async()=>{unavailableSetup();observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');const first=svc.supervise('xv','j')![0];repo.claim({qitemId:first.queueId!,destinationSession:'operator-agent@kernel'});repo.update({qitemId:first.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});const next=svc.supervise('xv','j')![0];expect(next.queueId).not.toBe(first.queueId);expect(JSON.parse(repo.getById(next.queueId!)!.body).previousQueueId).toBe(first.queueId);expect(repo.getById(first.queueId!)?.state).toBe('done');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});

 it.each(['indeterminate','pending','sending','postcondition-drift','unclaimed','retired-recipient'] as const)('expired idle holder takeover preserves %s lifecycle notice evidence',async(state)=>{
  configure(normal());const original=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;
  await finishTyped('product','builder@xv',original,'takeover-return');
  const duty=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  svc.accept('lead@xv','lead-g1','xv','product','takeover-return','actual/takeover-acceptance.md');
  repo.update({qitemId:duty.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?").run(['pending','sending'].includes(state)?state:'indeterminate','wake-intent-'+duty.queueId);
  svc.reconcile('lead@xv','lead-g1','xv');
  if(state==='postcondition-drift')db.prepare("DELETE FROM coordinator_operations WHERE kind='coordination-accept'").run();
  if(state==='unclaimed')db.prepare("UPDATE queue_items SET claimed_at=NULL,claimed_by_generation_uuid=NULL WHERE qitem_id=?").run(duty.queueId);
  if(state==='retired-recipient')db.prepare("UPDATE occupant_tenures SET generation_uuid='retired-lead' WHERE generation_uuid='lead-g1'").run();
  const notice=db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+duty.queueId) as any;
  expect(svc.noticeOutcomeContained('xv',notice)).toBe(state==='indeterminate');
  job();clock+=60001;vi.setSystemTime(clock);refresh();
  const result=svc.supervise('xv','j');
  if(state==='indeterminate')expect(repo.coordinatorAuthority.get('xv')).toMatchObject({epoch:2,owner_session:'peer@xv',state:'reconciling'});
  else {if(['pending','sending','unclaimed'].includes(state))expect(result?.find(r=>r.key==='coordinator')?.reason).toBe('coordinator_uncertain_effects');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);}
expect(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(notice.outbox_id)).toEqual(notice);
  });

  function expiredPresentSetup(planOverrides:Partial<CoordinationPlan>={}){configure(normal(),{},planOverrides);job();samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});svc.supervise('xv','j');repo.coordinatorAuthority.renew('lead@xv',token,10000,'short-lease');clock+=10001;vi.setSystemTime(clock);refresh();samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});return {expectedLeaseUntil:repo.coordinatorAuthority.get('xv')!.lease_until};}
  const expiryDuties=()=>db.prepare("SELECT qitem_id,body FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.action')='active-expiry-recover' ORDER BY rowid").all() as Array<{qitem_id:string;body:string}>;
  const intentActions=()=>db.prepare("SELECT json_extract(body,'$.action') a,count(*) n FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) GROUP BY a").all() as Array<{a:string|null;n:number}>;

  it('expired active present creates one operator intake with exact frozen lease',()=>{
   expiredPresentSetup();
   const result=svc.supervise('xv','j')!;
   expect(result).toHaveLength(1);
   expect(result[0]).toMatchObject({key:'coordinator',state:'recovery-required',reason:'expired-active-native-present-holder'});
   const q=repo.getById(result[0].queueId!)!;
   expect(q).toMatchObject({sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',state:'pending'});
   expect(db.prepare("SELECT claimed_at,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(result[0].queueId)).toEqual({claimed_at:null,claimed_by_generation_uuid:null});
   const body=JSON.parse(q.body);
   expect(body).toMatchObject({action:'active-expiry-recover',reason:'expired-active-native-present-holder',previousQueueId:null,rigId:'xv',epoch:1,recipientGeneration:'operator-agent-g1'});
   expect(body.activeExpiry).toEqual({owner:'lead@xv',ownerGeneration:'lead-g1',expectedLeaseUntil:repo.coordinatorAuthority.get('xv')!.lease_until,custodyDigest:repo.coordinatorAuthority.reconciliationDigest('xv'),recoveryWindowMs:300000});
   expect(body.nextAction).toMatch(/blocked[\s\S]*genuine original holder[\s\S]*exact current owner generation[\s\S]*supported queue claim[\s\S]*must not claim on the holder[\s\S]*does not renew or extend the expired authority[\s\S]*current Operator[\s\S]*active-expiry-recover[\s\S]*freshly computed current reconciliation digest[\s\S]*Never reuse this notice’s staged custody digest[\s\S]*same-current native holder must use supported resume-owned to acknowledge and renew atomically/);
   expect('grantsAuthority' in body).toBe(false);
   expect(q.expiresAt).toBe(new Date(result[0].deadline!).toISOString());
   // Precedence is proven, not assumed: the idle-transfer fallback cannot also stage its hold.
   expect(expiryDuties()).toHaveLength(1);
   expect(intentActions().filter(r=>r.a==='recover-expired-idle-transfer')).toEqual([]);
   expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='idle-stall-transfer'").get()).toEqual({n:0});
   expect(repo.coordinatorAuthority.get('xv')).toMatchObject({epoch:1,owner_session:'lead@xv',state:'active'});
  });

  it('repeated tick no duplicate',()=>{
   expiredPresentSetup();
   const first=svc.supervise('xv','j')!,qid=first[0].queueId!,item=repo.getById(qid)!;
   const emitted=db.prepare("SELECT outbox_id FROM outbox_entries").all().map((r:any)=>r.outbox_id as string);
   expect(emitted).toContain('wake-intent-'+qid);
   const wake=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qid);
   expect(wake).toMatchObject({delivery_state:'pending',tags:JSON.stringify(['queue:recipient-generation:operator-agent-g1'])});
   svc.supervise('xv','j');svc.supervise('xv','j');
   expect(expiryDuties()).toHaveLength(1);
   expect(repo.getById(qid)).toEqual(item);
   expect(db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qid)).toEqual({n:1});
   // An UNKNOWN wake stays exactly as emitted; it is never relabelled, retried or exempted.
   db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+qid);
   const unknown=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qid);
   svc.supervise('xv','j');
   expect(expiryDuties()).toHaveLength(1);
   expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qid)).toEqual(unknown);
   expect(String((unknown as any).tags)).not.toContain('system-wake');
   // Plain administrative intake: claimed through the ordinary queue path with effects still unresolved.
   repo.claim({qitemId:qid,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
   expect(repo.getById(qid)?.state).toBe('in-progress');
   expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(qid)).toEqual({claimed_by_generation_uuid:'operator-agent-g1'});
   svc.supervise('xv','j');
   expect(expiryDuties()).toHaveLength(1);
   expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind IN ('coordination-task-hold-lineage','native-terminal-return-retirement')").get()).toBeUndefined();
   expect(repo.coordinatorAuthority.get('xv')).toMatchObject({epoch:1,state:'active'});
  });

  it('keeps active-expiry intake live for its acknowledgment window rather than the stall threshold',()=>{
   expiredPresentSetup({stallMs:60000,acknowledgmentWindowMs:300000});
   const first=svc.supervise('xv','j')!,firstId=first[0].queueId!,firstItem=repo.getById(firstId)!;
   const originalWake=db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+firstId);
   const originalLease=repo.coordinatorAuthority.get('xv')!.lease_until;
   expect(firstItem.expiresAt).toBe(new Date(clock+300000).toISOString());
   expect(JSON.parse(firstItem.body).activeExpiry.recoveryWindowMs).toBe(300000);
   expect(originalWake).toBeDefined();
   db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+firstId);
   const unknownWake=db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+firstId);

   clock+=60001;vi.setSystemTime(clock);refresh();
   samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});
   const afterStall=svc.supervise('xv','j')!;
   expect(afterStall[0].queueId).toBe(firstId);
   expect(repo.getById(firstId)).toEqual(firstItem);
   expect(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+firstId)).toEqual(unknownWake);
   expect(repo.coordinatorAuthority.get('xv')!.lease_until).toBe(originalLease);

   clock+=239998;vi.setSystemTime(clock);refresh();
   samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});
   expect(svc.supervise('xv','j')![0].queueId).toBe(firstId);
   expect(repo.getById(firstId)).toEqual(firstItem);
   expect(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+firstId)).toEqual(unknownWake);
   expect(repo.coordinatorAuthority.get('xv')!.lease_until).toBe(originalLease);

   clock+=1;vi.setSystemTime(clock);refresh();
   samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});
   const successor=svc.supervise('xv','j')!,successorId=successor[0].queueId!,successorItem=repo.getById(successorId)!;
   expect(successorId).not.toBe(firstId);
   expect(JSON.parse(successorItem.body)).toMatchObject({
    action:'active-expiry-recover',
    previousQueueId:firstId,
    recoveryKey:JSON.parse(firstItem.body).recoveryKey,
    activeExpiry:{expectedLeaseUntil:originalLease,recoveryWindowMs:300000},
   });
   expect(successorItem.expiresAt).toBe(new Date(clock+300000).toISOString());
   expect(db.prepare('SELECT count(*) n FROM outbox_entries WHERE outbox_id IN (?,?)').get('wake-intent-'+firstId,'wake-intent-'+successorId)).toEqual({n:2});
   expect(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+firstId)).toEqual(unknownWake);
   expect(repo.coordinatorAuthority.get('xv')!.lease_until).toBe(originalLease);
   expect(expiryDuties()).toHaveLength(2);
  });

  it('new later lease distinct episode',async()=>{
   const {expectedLeaseUntil}=expiredPresentSetup();
   const first=svc.supervise('xv','j')!,firstId=first[0].queueId!,before=repo.getById(firstId)!;
   // Supported native sequence: Operator opens the window, the same holder acknowledges, then renews.
   observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');
   const frozen=JSON.parse(before.body).activeExpiry;
   expect(()=>repo.coordinatorAuthority.renew('lead@xv',token,60000,'premature-renew')).toThrow('Lease expiry');
   expect(repo.coordinatorAuthority.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',{token,expectedLeaseUntil:frozen.expectedLeaseUntil,operationId:'expiry-duty-r1',obligationsDigest:frozen.custodyDigest,windowMs:frozen.recoveryWindowMs})).toMatchObject({state:'reconciling',owner_session:'lead@xv',epoch:1});
   expect(()=>repo.coordinatorAuthority.renew('lead@xv',token,60000,'renew-before-ack')).toThrow('not reconciled');
   repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'expiry-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
   expect(()=>repo.coordinatorAuthority.renew('lead@xv','retired',60000,'retired-renew')).toThrow();
   repo.coordinatorAuthority.renew('lead@xv',token,10000,'post-ack-renew');
   expect(repo.coordinatorAuthority.get('xv')).toMatchObject({state:'active',lease_until:clock+10000});
   expect(repo.getById(firstId)).toEqual(before);
   // The acknowledged lease lapses again: a distinct episode that never rewrites the earlier duty.
   clock=clock+10001;vi.setSystemTime(clock);refresh();samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});
   const second=svc.supervise('xv','j')!;
   expect(second[0].reason).toBe('expired-active-native-present-holder');
   expect(second[0].queueId).not.toBe(firstId);
   expect(expiryDuties()).toHaveLength(2);
   expect(repo.getById(firstId)).toEqual(before);
   const later=JSON.parse(repo.getById(second[0].queueId!)!.body);
   expect(later.activeExpiry.expectedLeaseUntil).toBe(clock-1);
   expect(later.activeExpiry.expectedLeaseUntil).not.toBe(expectedLeaseUntil);
   expect(later.previousQueueId).toBeNull();
   expect(later.recoveryKey).not.toBe(JSON.parse(before.body).recoveryKey);
  });

  it('no authority/custody mutation',()=>{
   expiredPresentSetup();
   const authority=db.prepare('SELECT * FROM coordinator_authority').all(),assignments=db.prepare('SELECT * FROM coordinator_assignments').all(),stages=db.prepare('SELECT * FROM coordinator_stage_assignments').all(),resources=db.prepare('SELECT * FROM coordinator_resources').all(),operations=db.prepare('SELECT * FROM coordinator_operations').all(),outbox=db.prepare('SELECT count(*) n FROM outbox_entries').get(),baton=repo.getById('baton')!;
   const result=svc.supervise('xv','j')!;
   expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(authority);
   expect(db.prepare('SELECT * FROM coordinator_assignments').all()).toEqual(assignments);
   expect(db.prepare('SELECT * FROM coordinator_stage_assignments').all()).toEqual(stages);
   expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);
   expect(db.prepare('SELECT * FROM coordinator_operations').all()).toEqual(operations);
   expect(db.prepare('SELECT count(*) n FROM outbox_entries').get()).toEqual({n:(outbox as any).n+1});
   expect(repo.getById('baton')).toEqual(baton);
   const body=JSON.parse(repo.getById(result[0].queueId!)!.body);
   expect(body.grantsAuthority).toBeUndefined();
   expect(intentActions()).toEqual([{a:'active-expiry-recover',n:1}]);
   expect(db.prepare("SELECT count(*) n FROM queue_items WHERE destination_session='operator-agent@kernel' AND claimed_by_generation_uuid IS NOT NULL").get()).toEqual({n:0});
  });

  it.each(['absent','unknown-identity','stale-identity','unverified-identity','retired-holder','live-lease','reconciling','no-plan'] as const)('absent/unknown state follows existing safe fences (%s)',async(kind)=>{
   const {expectedLeaseUntil}=expiredPresentSetup();
   if(kind==='absent'){observer('absent');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(repo.coordinatorAuthority.hasFreshUnavailableOwner('xv')).toBe(true);}
   if(kind==='unknown-identity')samples.delete('lead@xv');
   if(kind==='stale-identity')samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock-3001).toISOString()});
   if(kind==='unverified-identity')samples.set('lead@xv',{...sample('lead@xv'),identityVerified:false,identityObservedAt:new Date(clock).toISOString()});
   if(kind==='retired-holder')db.prepare("UPDATE occupant_tenures SET generation_uuid='retired-lead' WHERE node_id='lead@xv'").run();
   if(kind==='live-lease')db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+60000);
   if(kind==='reconciling'){observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');repo.coordinatorAuthority.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',{token,expectedLeaseUntil,operationId:'fence-reconciling',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv'),windowMs:60000});clock=repo.coordinatorAuthority.get('xv')!.lease_until+1;vi.setSystemTime(clock);refresh();samples.set('lead@xv',{...sample('lead@xv'),identityObservedAt:new Date(clock).toISOString()});}
   if(kind==='no-plan')db.prepare("DELETE FROM coordinator_operations WHERE kind='coordination-plan'").run();
   const before=db.prepare('SELECT count(*) n FROM queue_items').get(),authority=db.prepare('SELECT * FROM coordinator_authority').all();
   let error:Error|undefined,result:ReturnType<typeof svc.supervise>=null;
   try{result=svc.supervise('xv','j');}catch(e){error=e as Error;}
   // The actionable expiry duty is never staged on any absent, unknown or stale condition.
   expect(expiryDuties()).toEqual([]);
   if(kind==='retired-holder'){expect(error?.message).toContain('Caller generation is missing, retired, or unknown');expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(authority);return;}
   expect(error).toBeUndefined();
   expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(authority);
   if(kind==='no-plan'){expect(result).toBeNull();expect(db.prepare('SELECT count(*) n FROM queue_items').get()).toEqual(before);return;}
   if(kind==='live-lease'){expect(result?.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(intentActions().map(r=>r.a)).not.toContain('active-expiry-recover');return;}
   if(kind==='reconciling'){expect(result?.[0]).toMatchObject({key:'coordinator',state:'pending-reconciliation-recovery'});expect(intentActions().map(r=>r.a)).toContain('recover-expired-reconciliation');return;}
   if(kind==='absent'||kind==='unknown-identity'||kind==='unverified-identity'){expect(result?.[0]).toMatchObject({key:'coordinator',state:'recovery-required'});expect(intentActions().map(r=>r.a)).toContain('reconcile-current-coordinator-lease');return;}
   // A present-but-unverified holder keeps whatever pre-existing fence its own path already applies.
   expect(result?.find(r=>r.key==='coordinator')?.reason).not.toBe('expired-active-native-present-holder');
   expect(intentActions().map(r=>r.a)).toContain('recover-expired-idle-transfer');
  });

  it('supported active-expiry-recover still refuses unproven presence and drift without staging another duty',async()=>{
   expiredPresentSetup();
   const first=svc.supervise('xv','j')!,frozen=JSON.parse(repo.getById(first[0].queueId!)!.body).activeExpiry;
   observer('unknown');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');
   const authority=db.prepare('SELECT * FROM coordinator_authority').all();
   expect(()=>repo.coordinatorAuthority.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',{token,expectedLeaseUntil:frozen.expectedLeaseUntil,operationId:'unknown-presence',obligationsDigest:frozen.custodyDigest,windowMs:frozen.recoveryWindowMs})).toThrow('Fresh generation-bound native presence required');
   expect(()=>repo.coordinatorAuthority.recoverExpiredActive('lead@xv','lead-g1',{token,expectedLeaseUntil:frozen.expectedLeaseUntil,operationId:'non-operator',obligationsDigest:frozen.custodyDigest,windowMs:frozen.recoveryWindowMs})).toThrow('Kernel Operator owns admission');
   observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');
   expect(()=>repo.coordinatorAuthority.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',{token,expectedLeaseUntil:frozen.expectedLeaseUntil+1,operationId:'stale-lease',obligationsDigest:frozen.custodyDigest,windowMs:frozen.recoveryWindowMs})).toThrow('Exact expired active');
   expect(()=>repo.coordinatorAuthority.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',{token,expectedLeaseUntil:frozen.expectedLeaseUntil,operationId:'drifted-digest',obligationsDigest:'stale-digest',windowMs:frozen.recoveryWindowMs})).toThrow('Exact current custody digest required');
   expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(authority);
   expect(expiryDuties()).toHaveLength(1);
  });

  describe('retained task admission exemption',()=>{
const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
    // Every ordinary task needs a DISTINCT admitted recovery task; the production gate is NOT relaxed.
    // The backup is a separate real task naming recoveryFor, exactly as a genuine plan does.
    const backup=(t:CoordinationTask,owner='architect@xv')=>task(t.key+'-repair',owner,{recoveryFor:t.key});
    const paired=(...tasks:CoordinationTask[]):CoordinationTask[]=>tasks.flatMap(t=>[t,backup(t)]);
    const plan=(tasks:CoordinationTask[]):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks});
    function configure(tasks:CoordinationTask[],resources:Record<string,string[]>={}){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:resources[t.key]??[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks));}
    function refresh(){for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));}
    function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
    // A successor task is new, so its package must be admitted exactly as configure() does for the rest.
    const admitPkg=(key:string,owner='builder@xv',body=key)=>repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',key,{inputDigest:digest(key),destination:owner,bodyHash:digest(body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
    // Retained-but-stale: admitted with a VALID admission, then time advances so its own stored bytes go stale.
    const staleRetained=()=>{const retained=task('xv-architect','builder@xv',{deadline:clock+200000});configure(paired(retained));clock+=70000;vi.setSystemTime(clock);return svc.plan('xv')!.tasks;};
    it('T1 deadlock resolved: retained task with expired admission + new ready task with full admission succeeds',()=>{
      const prior=staleRetained();
      const newTask=task('new-frontier','reviewer@xv'),newBackup=backup(newTask);
      admitPkg('new-frontier','reviewer@xv');admitPkg('new-frontier-repair','architect@xv');
      const next=svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[...prior,newTask,newBackup]});
      expect(next.tasks.map(t=>t.key)).toEqual(['xv-architect','xv-architect-repair','new-frontier','new-frontier-repair']);
      expect(next.tasks[0].admission.validUntil).toBeLessThan(clock);
    });

    it('T2 new task without current admission in same successor is refused',()=>{
      const prior=staleRetained();
      const newTask=task('new-frontier','reviewer@xv',{admission:{...task('new-frontier','reviewer@xv').admission,validUntil:clock-1000}});
      admitPkg('new-frontier','reviewer@xv');admitPkg('new-frontier-repair','architect@xv');
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[...prior,newTask,backup(newTask)]})).toThrow('Exact current generation');
    });

    it('T3 retained task with any byte changed and stale admission is refused',()=>{
      const prior=staleRetained(),stale=prior[0]!;
      const changed=task('xv-architect','builder@xv',{...stale,deadline:clock+30000});
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[changed,backup(changed)]})).toThrow('Exact current generation');
      const changed2=task('xv-architect','builder@xv',{...stale,body:'changed body'});
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[changed2,backup(changed2)]})).toThrow('Exact current generation');
      const changed3=task('xv-architect','builder@xv',{...stale,admission:{...stale.admission!,qualificationRef:'changed/ref'}});
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[changed3,backup(changed3)]})).toThrow('Exact current generation');
      // The same byte change is ACCEPTED when it also carries a genuinely current admission.
      const requalified=task('xv-architect','builder@xv',{...changed3,admission:task('xv-architect','builder@xv').admission!});
      expect(svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[requalified,backup(requalified)]}).tasks).toHaveLength(2);
    });

    it('T4 dropped or stable-changed retained task is refused',()=>{
      configure(paired(task('xv-architect'),task('kept','reviewer@xv')));
      const prior=svc.plan('xv')!.tasks,keep=prior.filter(t=>t.key==='kept'||t.key==='kept-repair');
      // Dropping the retained obligation stays refused even though the successor is otherwise valid.
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:keep})).toThrow('Retain all existing tasks unchanged');
      // A stable-visible change (body) is an orphan, not an exemption.
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:prior.map(t=>t.key==='xv-architect'?{...t,action:'changed action'}:t)})).toThrow('Retain all existing tasks unchanged');
    });

    it('T5 reconcile after T1: retained task stays held, new task dispatches',()=>{
      // The holder lease is renewed BEFORE time advances; renew cannot rescue an already-expired lease.
      configure(paired(task('xv-architect','builder@xv',{deadline:clock+200000})));
      repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-long');
      clock+=70000;vi.setSystemTime(clock);
      const prior=svc.plan('xv')!.tasks;
      const newTask=task('new-frontier','reviewer@xv');
      admitPkg('new-frontier','reviewer@xv');admitPkg('new-frontier-repair','architect@xv');
      svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[...prior,newTask,backup(newTask)]});
      const outboxBefore=db.prepare('SELECT count(*) n FROM outbox_entries').get() as any,assignBefore=db.prepare('SELECT count(*) n FROM coordinator_assignments').get() as any;
      refresh();
      const results=svc.reconcile('lead@xv','lead-g1','xv');
      const retainedResult=results.find(r=>r.key==='xv-architect')!;
      const newResult=results.find(r=>r.key==='new-frontier')!;
      expect(retainedResult.state).toBe('held');
      expect(retainedResult.reason).toBe('current-admission-required');
      // I1: the exemption granted no credit - the retained task wrote no assignment and no wake.
      expect(retainedResult.queueId).toBeUndefined();
      expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='xv-architect'").get()).toBeUndefined();
      // I1: no wake exists for the retained task; only the newly admitted scope may produce one.
      expect(db.prepare("SELECT 1 FROM outbox_entries WHERE destination_session='builder@xv'").get()).toBeUndefined();
      expect(newResult.state).toBe('pending-pickup');
      expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='new-frontier'").get()).toBeTruthy();
    });

    it('T6 accepted/dormant history immutability is untouched by the exemption',()=>{
      // The `historical` branch (coordination-recovery-service.ts:917) short-circuits BEFORE the
      // admission gate, so the retainedExact exemption cannot reach accepted/dormant history at all.
      // Accepted history needs the full dispatch->accept chain, which this fixture does not build;
      // the pre-existing 'preserving exact accepted parent and dormant backup history' case still
      // covers that path and is untouched by this diff. Here we prove only the reachable half.
      configure(paired(task('historical','builder@xv')));
      const prior=svc.plan('xv')!.tasks;
      expect(svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:prior})).toBeTruthy();
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r3',tasks:prior.map(t=>t.key==='historical'?{...t,action:'rewritten action'}:t)})).toThrow('Retain all existing tasks unchanged');
    });

    it('T7 scope-only attachment test still passes',()=>{
      const tasks=[task('scope-main'),task('scope-recovery','architect@xv',{recoveryFor:'scope-main'})];
      const original=configure(tasks);
      const outboxBefore=db.prepare('SELECT * FROM outbox_entries').all();
      clock+=70000;vi.setSystemTime(clock);
      const next={...svc.plan('xv')!,revision:'scope-connect',scopeSources:[{ref:'mission.md',digest:'a'.repeat(64)}],frontierPlanning:{stabilizationObservations:2}};
      expect(svc.configure('operator-agent@kernel','operator-agent-g1',next).tasks).toEqual(original.tasks);
      expect((svc as any).admittedNow(next.tasks[0])).toBe(false);
      expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(outboxBefore);
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...next,revision:'changed-deadline',tasks:next.tasks.map(t=>({...t,deadline:clock+20000}))})).toThrow('Exact current');
    });

    it('T8 canTransferUnavailable returns false while retained task is stale',()=>{
      staleRetained();
      refresh();
      expect(svc.canTransferUnavailable('xv','architect@xv','architect-g1')).toBe(false);
      const newTask=task('new-frontier','reviewer@xv');
      admitPkg('new-frontier','reviewer@xv');admitPkg('new-frontier-repair','architect@xv');
      svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[...svc.plan('xv')!.tasks,newTask,backup(newTask)]});
      refresh();
      expect(svc.canTransferUnavailable('xv','architect@xv','architect-g1')).toBe(false);
      // Every task requalified: all admissions are current, so the gate opens.
      const all=paired(task('xv-architect'),task('new-frontier','reviewer@xv'));
      for(const t of all)admitPkg(t.packageKey,t.owner,t.body);
      svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r3',allowUnavailablePeerTransfer:true,tasks:all});
      refresh();
      expect(svc.canTransferUnavailable('xv','architect@xv','architect-g1')).toBe(true);
    });

    it('T9 frozen-revision replay returns identical receipt',()=>{
      const prior=staleRetained();
      const newTask=task('new-frontier','reviewer@xv');
      admitPkg('new-frontier','reviewer@xv');admitPkg('new-frontier-repair','architect@xv');
      const next=svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:[...prior,newTask,backup(newTask)]});
      const replay=svc.configure('operator-agent@kernel','operator-agent-g1',{...next,revision:'r2'});
      expect(replay).toEqual(next);
      expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...next,revision:'r2',tasks:next.tasks.map((t,i)=>i===0?{...t,deadline:clock+300000,admission:task('xv-architect','builder@xv',{deadline:clock+300000}).admission!}:t)})).toThrow('Frozen revision cannot change');
    });
  });

// TASK-ADMISSION-RECOVERY-DESIGN: one shared, renewing, accountable admission-refresh
// lifecycle duty. The runtime stages the duty and verifies a postcondition; it never
// writes an admission, extends a TTL, grants qualification or fabricates evidence.
describe('admission-refresh lifecycle duty',()=>{
 // Authoritative receipt shape: coordinator_operations.kind is 'coordinator-lifecycle-control';
 // rowid is the authoritative INSERTION order (operation_id is hash-derived, so it does not order by age).
 // and the duty kind lives at receipt.$.kind (see lifecycleDuty's INSERT).
 const refreshRows=()=>db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' ORDER BY rowid").all() as any[];
 const refreshAll=()=>refreshRows().map(v=>JSON.parse(v.receipt));
 /** Every stale task gets its OWN duty, so assertions are scoped by package subject rather
  *  than assuming the plan holds a single stale task. */
 const refreshReceipt=(packageKey?:string)=>{const all=refreshAll();const scoped=packageKey?all.filter(r=>r.packageKey===packageKey):all;return scoped.length?scoped[scoped.length-1]:null;};
 /** A task only becomes RETAINED-and-STALE by being admitted with a CURRENT admission
  *  and then having time advance past its validUntil. Configuring it already-stale would
  *  be refused by configure() itself, which is exactly the fence under test elsewhere. */
 const STALE_PLAN=(owner='reviewer@xv')=>{configure([task('expired',owner),task('expired-repair','architect@xv',{recoveryFor:'expired'})]);const admittedAt=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!.admission.validUntil;repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-stale-window');clock+=admittedAt+2-clock;vi.setSystemTime(clock);refresh();return admittedAt;};
 const r_gen=(t:CoordinationTask)=>t.admission.generation;
 const r_cfg=(t:CoordinationTask)=>svc.configurationDigest(t.owner)!;
 const dutyBody=()=>{const r=refreshReceipt('expired');return r?JSON.parse((db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(r.queueId) as any).body):null;};

 it('AR1 a stale task is held and stages one accountable Operator duty bound to live facts',()=>{
  const admittedAt=STALE_PLAN();
  const results=svc.reconcile('lead@xv','lead-g1','xv');
  expect(results.find(r=>r.key==='expired')?.reason).toBe('current-admission-required');
  const r=refreshReceipt('expired');
  
  expect(r).toBeTruthy();
  expect(r.recipient).toBe('operator-agent@kernel');
  expect(dutyBody().grantsAuthority).toBe(false);
  expect(r.staleReason).toBe('expired');
  expect(r.ownerGeneration).toBe(repo.coordinatorAuthority.generation('reviewer@xv'));
  expect(r.liveConfigurationDigest).toBe(svc.configurationDigest('reviewer@xv'));
  expect(r.priorAdmission.validUntil).toBe(admittedAt);
  expect(r.taskIntentDigest).toBe((svc as any).admissionTaskIntentDigest(svc.plan('xv')!.tasks.find(t=>t.key==='expired')!));
  expect(r.priorAdmission.qualificationRef).toBe(task('expired','reviewer@xv').admission.qualificationRef);
  expect(r.planRevision).toBe(svc.plan('xv')!.revision);
  // The duty carries the LIVE generation and a placeholder contract, never a carried-forward grant.
  const body=dutyBody();
  expect(body.action).toBe('refresh-exact-expired-task-admission');
  expect(body.admissionRefreshContract.ownerGeneration).toBe(r.ownerGeneration);
  expect(body.admissionRefreshContract.admission.qualificationRef).toBe('<actual-new-proof-reference>');
  expect(body.admissionRefreshContract.admission.generation).toBe('<live owner generation>');
 });

 it('AR2 staging writes no admission, no TTL change and no assignment',()=>{
  STALE_PLAN();
  const pkgBefore=db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=(SELECT id FROM rigs WHERE name=?) AND package_key='expired'").get('xv');
  const tasksBefore=JSON.stringify(svc.plan('xv')!.tasks);
  const assignmentsBefore=db.prepare('SELECT count(*) n FROM coordinator_assignments').get();
  svc.reconcile('lead@xv','lead-g1','xv');
  expect(db.prepare("SELECT contract FROM coordinator_packages WHERE rig_id=(SELECT id FROM rigs WHERE name=?) AND package_key='expired'").get('xv')).toEqual(pkgBefore);
  expect(JSON.stringify(svc.plan('xv')!.tasks)).toBe(tasksBefore);
  expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual(assignmentsBefore);
  expect(db.prepare('SELECT count(*) n FROM coordinator_resources').get()).toEqual({n:0});
  expect(db.prepare('SELECT count(*) n FROM coordinator_stage_assignments').get()).toEqual({n:0});
 });

 it('AR3 dedup: the same retained task under a new plan revision keeps ONE duty',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  const first=refreshReceipt('expired')!;
  expect(first.semanticKey).toBeTruthy();
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2'});
  svc.reconcile('lead@xv','lead-g1','xv');
  const scoped=refreshAll().filter(r=>r.packageKey==='expired');
  expect(scoped.length).toBe(1);
  expect(scoped[0].semanticKey).toBe(first.semanticKey);
  expect(scoped[0].queueId).toBe(first.queueId);
 });

 it('AR4 a changed owner configuration opens a NEW duty with configuration_changed',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  expect(refreshReceipt('expired')!.staleReason).toBe('expired');
  const before=refreshAll().filter(r=>r.packageKey==='expired').length;
  db.prepare("UPDATE nodes SET model='different-model' WHERE logical_id='reviewer'").run();
  svc.reconcile('lead@xv','lead-g1','xv');
  const scoped=refreshAll().filter(r=>r.packageKey==='expired');
  expect(scoped.length).toBe(before+1);
  expect(scoped[scoped.length-1].staleReason).toBe('configuration_changed');
  expect(scoped[scoped.length-1].liveConfigurationDigest).toBe(svc.configurationDigest('reviewer@xv'));
  expect(scoped[scoped.length-1].semanticKey).not.toBe(scoped[0].semanticKey);
 });

 it('AR5 worker effect debt on the owner yields NO admission-refresh duty and preserves UNKNOWN',()=>{
  STALE_PLAN();
  db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('w-unknown','watchdog@system','reviewer@xv','live',?,'indeterminate')").run(new Date(clock).toISOString());
  const before=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE delivery_state='indeterminate' ORDER BY outbox_id").all();
  const results=svc.reconcile('lead@xv','lead-g1','xv');
  expect(results.find(r=>r.key==='expired')?.reason).toBe('uncertain-worker-effect');
  // No admission-refresh duty for the effect-debt task itself. Its repair sibling expires on the
  // same clock and is an independent subject, so the duty is scoped by packageKey.
  expect(refreshAll().map(r=>r.packageKey)).not.toContain('expired');
  const after=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE delivery_state='indeterminate' ORDER BY outbox_id").all() as any[];
  // The UNKNOWN row is byte-preserved and never retried or relabelled.
  expect(after.find(r=>r.outbox_id==='w-unknown')).toEqual({outbox_id:'w-unknown',delivery_state:'indeterminate'});
  expect(after).toHaveLength(before.length);
 });

 it('AR6 a retired Operator generation cannot complete the duty',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  const r=refreshReceipt('expired');
  expect((svc as any).admissionRefreshCompleted('xv',{...r,operatorGeneration:'retired-generation'})).toBe(false);
  // And the un-refreshed real receipt is not complete either.
  expect((svc as any).admissionRefreshCompleted('xv',r)).toBe(false);
 });

 it('AR7 TTL-only extension of the stale admission does NOT complete the duty',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  const r=refreshReceipt('expired')!;
  // Only validUntil moves; the stale evidence references are carried forward.
  const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!;
  const ttlOnly={...current,admission:{...current.admission,validUntil:clock+900000}};
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?ttlOnly:t)});
  // The ORIGINAL frozen duty receipt is used unchanged; nothing is forged.
  expect((svc as any).admissionRefreshCompleted('xv',r)).toBe(false);
 });

 it('AR7b reused evidence reference does NOT complete the duty',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  const r=refreshReceipt('expired')!;
  const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!;
  // Re-issuing only ONE of the three references is not a genuine re-assessment.
  // Generation and configuration stay valid here, so configure() accepts and the
  // postcondition is the fence under test.
  const partials=[{qualificationRef:'<new-qualification-proof>'},{capacityRef:'<new-capacity-proof>'},{effortRef:'<new-effort-proof>'}];
  partials.forEach((partial,i)=>{
   const mixed={...current,admission:{...current.admission,...partial,validUntil:clock+900000}};
   svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2-'+i,tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?mixed:t)});
   expect((svc as any).admissionRefreshCompleted('xv',r)).toBe(false);
  });
 });

 it('AR7c a changed task intent does NOT complete the duty',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  const r=refreshReceipt('expired')!;
  const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!;
  const fresh=task('expired','reviewer@xv').admission;
  // New evidence AND a newer expiry, but the task now asks for DIFFERENT work. The action
  // changes (not the body) so configure() still admits it; only the INTENT differs.
  const changedIntent={...current,action:'Do something else entirely and return bounded evidence',deadline:clock+900000,admission:{...fresh,qualificationRef:'<new-qualification-proof>',capacityRef:'<new-capacity-proof>',effortRef:'<new-effort-proof>',validUntil:clock+900000}};
  // The duty's own subject gate: a different task request is a different subject.
  expect((svc as any).admissionTaskIntentDigest(changedIntent)).not.toBe(r.taskIntentDigest);
  // And in practice the earlier obligation fence refuses a changed RETAINED task outright,
  // so such a submission cannot even reach the postcondition.
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?changedIntent:t)})).toThrow('Retain all existing tasks unchanged');
  expect((svc as any).admissionRefreshCompleted('xv',r)).toBe(false);
 });

 it('AR7d a stale owner generation or configuration is refused at configure, before any duty',()=>{
  STALE_PLAN();
  const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!;
  const fresh=task('expired','reviewer@xv').admission;
  const good={generation:r_gen(current),configurationDigest:r_cfg(current),qualificationRef:'<new-qualification-proof>',capacityRef:'<new-capacity-proof>',effortRef:'<new-effort-proof>',validUntil:clock+900000};
  // A stale generation is refused by the existing admission gate, so no duty can even start.
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?{...t,admission:{...good,generation:'rotated-generation'}}:t)})).toThrow('Exact current');
  // Likewise for a stale configuration digest.
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r3',tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?{...t,admission:{...good,configurationDigest:'stale-configuration-digest'}}:t)})).toThrow('Exact current');
  expect(fresh.validUntil).toBeGreaterThan(0);
 });

 it('AR8 a genuine fresh assessment completes the duty, using the ORIGINAL staged receipt',()=>{
  STALE_PLAN();
  svc.reconcile('lead@xv','lead-g1','xv');
  const r=refreshReceipt('expired')!;
  // Freeze the receipt as staged. It is NEVER mutated or re-digested by this test.
  const staged=JSON.parse(JSON.stringify(r));
  const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!;
  const fresh=task('expired','reviewer@xv').admission;
  // The Operator's genuine re-assessment: same task intent, all three evidence references
  // re-issued, live owner generation/configuration, current Operator binding, newer expiry.
  const refreshed={...current,admission:{generation:r.ownerGeneration,configurationDigest:r.liveConfigurationDigest,qualificationRef:'<new-qualification-proof>',capacityRef:'<new-capacity-proof>',effortRef:'<new-effort-proof>',validUntil:clock+900000}};
  expect((svc as any).admissionTaskIntentDigest(refreshed)).toBe(staged.taskIntentDigest);
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?refreshed:t)});
  expect(fresh.generation).toBe(r.ownerGeneration);
  // The untouched staged receipt now completes: this is the previously impossible case.
  expect((svc as any).admissionRefreshCompleted('xv',staged)).toBe(true);
  // And the normal gate is what admits it; the duty dispatched nothing itself.
  const live=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!;
  expect((svc as any).admittedNow(live)).toBe(true);
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='expired'").get()).toEqual({n:0});
 });

 it('AR10 an expired refresh duty with an indeterminate wake keeps UNKNOWN and exposes the retirement/accountable path',()=>{
  STALE_PLAN();
  const first=svc.reconcile('lead@xv','lead-g1','xv');
  const staged=refreshReceipt('expired')!;
  expect(staged.queueId).toBeTruthy();
  // The duty's own wake is UNKNOWN: delivery could not be determined.
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+staged.queueId);
  // Let the duty expire unclaimed.
  // dutyFacts() derives expiry from the durable receipt deadline, so expire it there.
  db.prepare("UPDATE coordinator_operations SET receipt=json_set(receipt,'$.deadline',?) WHERE operation_id=? AND kind='coordinator-lifecycle-control'").run(clock-1,staged.queueId);
  db.prepare("UPDATE queue_items SET expires_at=? WHERE qitem_id=?").run(new Date(clock-1).toISOString(),staged.queueId);
  const unknownBefore=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE delivery_state='indeterminate' ORDER BY outbox_id").all() as any[];
  const results=svc.reconcile('lead@xv','lead-g1','xv');
  const expiredHold=results.find(r=>r.key==='admission-refresh:expired');
  expect(expiredHold).toBeTruthy();
  expect(expiredHold!.state).toBe('held');
  // The EXISTING typed lifecycle reason, not a silent original hold.
  // The EXISTING typed lifecycle response to an expired-unclaimed duty: a supported duty
  // retirement is staged and the accountable reason is surfaced, not a silent original hold.
  // Whichever typed lifecycle outcome the chain reaches, it is one the renewing intake
  // machinery already routes, and it is NOT the silent original hold.
  expect(expiredHold!.reason).not.toBe('current-admission-required');
  expect(LIFECYCLE_INTAKE_RENEWAL_REASONS as readonly string[]).toContain(expiredHold!.reason!);
  expect(expiredHold!.queueId).toBe(staged.queueId);
  // The duty is retired/retirement-routed, so a supported duty-retirement queue row exists for it.
  const retirements=db.prepare("SELECT qitem_id FROM queue_items WHERE qitem_id LIKE 'qitem-coordination-lifecycle-%' AND qitem_id<>?").all(staged.queueId) as any[];
  expect(retirements.length+Number(Boolean((expiredHold!.activityEvidence as any)?.retirementQueueId))).toBeGreaterThan(0);
  // The reason is one the renewing intake chain already knows how to route.
  expect(LIFECYCLE_INTAKE_RENEWAL_REASONS as readonly string[]).toContain(expiredHold!.reason!);
  // The UNKNOWN wake is byte-preserved and never retried, released or relabelled.
  const unknownAfter=db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE delivery_state='indeterminate' ORDER BY outbox_id").all() as any[];
  expect(unknownAfter.find(r=>r.outbox_id==='wake-intent-'+staged.queueId)).toEqual({outbox_id:'wake-intent-'+staged.queueId,delivery_state:'indeterminate'});
  expect(unknownAfter).toHaveLength(unknownBefore.length);
  expect(first.find(r=>r.key==='expired')?.reason).toBe('current-admission-required');
 });

 it('AR9 the duty never dispatches the stale task and never bypasses the hold',()=>{
  STALE_PLAN();
  const results=svc.reconcile('lead@xv','lead-g1','xv');
  const expired=results.find(r=>r.key==='expired')!;
  expect(expired.state).toBe('held');
  expect(expired.queueId).toBeUndefined();
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='expired'").get()).toEqual({n:0});
 });

  /** A task whose admission AND whose owner's dispatch restriction have both elapsed. The
   *  restriction is configured while it is still future, then the clock advances past it, so
   *  the fixture reaches the real ordering defect rather than a refused plan. */
  const SCOPED_STALE=(owner='reviewer@xv')=>{
   configure([task('expired',owner),task('expired-repair','architect@xv',{recoveryFor:'expired'})]);
   const admittedAt=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!.admission.validUntil;
   repo.coordinatorAuthority.renew('lead@xv',token,600000,'scoped-stale-window');
   svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'scoped-r2',dispatchRestrictions:[{session:owner,generation:repo.coordinatorAuthority.generation(owner)!,packageKeys:['expired'],validUntil:admittedAt+1000,evidenceRef:'native/checkpoint-scope.json'}]});
   clock=admittedAt+2000;vi.setSystemTime(clock);refresh();return admittedAt;
  };

  it('RS1 a stale UNASSIGNED task under an expired dispatch scope gets its exact duty and a deduplicated finite intake',()=>{
   const admittedAt=SCOPED_STALE();
   const results=svc.reconcile('lead@xv','lead-g1','xv');
   // The dispatch hold is unchanged and still hard: no assignment, no queue row for the task.
   expect(results.find(r=>r.key==='expired')).toMatchObject({state:'held',reason:'dispatch-scope-expired'});
   expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='expired'").get()).toEqual({n:0});
   expect(repo.getById('qitem-coordination-'+digest('xv:expired').slice(0,24))).toBeNull();
   // The duty exists anyway, is exact and is bound to LIVE owner facts only.
   const r=refreshReceipt('expired')!;
   expect(r).toBeTruthy();
   expect(r.recipient).toBe('operator-agent@kernel');
   expect(r.staleReason).toBe('expired');
   expect(r.ownerGeneration).toBe(repo.coordinatorAuthority.generation('reviewer@xv'));
   expect(r.liveConfigurationDigest).toBe(svc.configurationDigest('reviewer@xv'));
   expect(r.priorAdmission.validUntil).toBe(admittedAt);
   expect(r.planRevision).toBe(svc.plan('xv')!.revision);
   // The duty's own result row is only propagated when the duty is itself HELD, exactly as
   // the current-admission-required branch does. A staged duty is proven by its durable row.
   expect(results.find(x=>x.key==='admission-refresh:expired')).toBeUndefined();
   expect(repo.getById(r.queueId)).toMatchObject({destinationSession:'operator-agent@kernel',state:'pending'});
   expect(Date.parse(repo.getById(r.queueId)!.expiresAt!)).toBeGreaterThan(clock);
   const body=JSON.parse((db.prepare('SELECT body FROM queue_items WHERE qitem_id=?').get(r.queueId) as any).body);
   expect(body.action).toBe('refresh-exact-expired-task-admission');
   expect(body.grantsAuthority).toBe(false);
   // The scope hold is now an accountable finite intake, and one reconcile stages exactly one.
   const intakes=()=>db.prepare("SELECT qitem_id,body FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.reason')='dispatch-scope-expired'").all() as any[];
   expect(intakes()).toHaveLength(1);
   const hold=JSON.parse(intakes()[0].body);
   expect(hold).toMatchObject({action:'resolve-exact-coordination-task-hold',packageKey:'expired',taskOwner:'reviewer@xv',recipientGeneration:'operator-agent-g1',grantsAuthority:false});
   expect(repo.getById(intakes()[0].qitem_id)).toMatchObject({destinationSession:'operator-agent@kernel'});
   expect(Date.parse(repo.getById(intakes()[0].qitem_id)!.expiresAt!)).toBeLessThanOrEqual(clock+1200000);
   // Repeat reconciliation is deduplicated: no second duty and no second intake.
   svc.reconcile('lead@xv','lead-g1','xv');
   expect(refreshAll().filter(x=>x.packageKey==='expired')).toHaveLength(1);
   expect(intakes()).toHaveLength(1);
  });

  it('RS2 an admission-only successor keeps the expired scope byte-identical and still undispatchable',()=>{
   SCOPED_STALE();
   const prior=svc.plan('xv')!,restriction=JSON.stringify(prior.dispatchRestrictions![0]);
   svc.reconcile('lead@xv','lead-g1','xv');
   const current=prior.tasks.find(t=>t.key==='expired')!,r=refreshReceipt('expired')!;
   const refreshed={...current,admission:{generation:r.ownerGeneration,configurationDigest:r.liveConfigurationDigest,qualificationRef:'<new-qualification-proof>',capacityRef:'<new-capacity-proof>',effortRef:'<new-effort-proof>',validUntil:clock+900000}};
   const next=svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'admission-only-r3',tasks:prior.tasks.map(t=>t.key==='expired'?refreshed:t)});
   // The restriction is retained as history with identical bytes, and still evaluates expired.
   expect(JSON.stringify(next.dispatchRestrictions![0])).toBe(restriction);
   expect((svc as any).dispatchScopeHold(next,next.tasks.find(t=>t.key==='expired')!)).toBe('dispatch-scope-expired');
   // The refreshed admission is genuinely current, yet scope still blocks dispatch.
   expect((svc as any).admittedNow(next.tasks.find(t=>t.key==='expired')!)).toBe(true);
   const results=svc.reconcile('lead@xv','lead-g1','xv');
   expect(results.find(r=>r.key==='expired')).toMatchObject({state:'held',reason:'dispatch-scope-expired'});
   expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='expired'").get()).toEqual({n:0});
   expect(repo.getById('qitem-coordination-'+digest('xv:expired').slice(0,24))).toBeNull();
   // Any CHANGED restriction is a new disposition and keeps the full current gate.
   expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...next,revision:'changed-scope-r4',dispatchRestrictions:[{...next.dispatchRestrictions![0],evidenceRef:'native/different-scope.json'}]})).toThrow('future expiry');
   expect(JSON.stringify(svc.plan('xv')!.dispatchRestrictions![0])).toBe(restriction);
  });

  it('RS3 assigned and accepted rows are unchanged, and an UNKNOWN owner effect still suppresses the scoped duty',async()=>{
   configure(normal());
   const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;
   await finishTyped('product','builder@xv',q,'accepted-return');
   svc.accept('lead@xv','lead-g1','xv','product','accepted-return','actual/accepted.md');
   repo.coordinatorAuthority.renew('lead@xv',token,600000,'rs3-window');
   const acceptedBytes=JSON.stringify(svc.plan('xv')!.tasks.find(t=>t.key==='product')!);
   const scope=[{session:'builder@xv',generation:'builder-g1',packageKeys:['product'] as string[],validUntil:clock+1000,evidenceRef:'native/accepted-scope.json'},{session:'architect@xv',generation:'architect-g1',packageKeys:['repair'] as string[],validUntil:clock+1000,evidenceRef:'native/repair-scope.json'}];
   svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'rs3-scoped-r2',dispatchRestrictions:scope});
   clock+=900001;vi.setSystemTime(clock);refresh();
   // The holder's lease must still be live for the reconcile to be the supported path; the
   // same holder, epoch and generation are retained, so no authority is re-granted here.
   db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+1200000);
   // The Architect's uncertain effect is preserved and blocks any new duty for its task.
   db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('w-unknown-scope','watchdog@system','architect@xv','live',?,'indeterminate')").run(new Date(clock).toISOString());
   const results=svc.reconcile('lead@xv','lead-g1','xv');
   // The accepted row follows its existing path: no duty, same queue id, untouched bytes.
   expect(results.find(r=>r.key==='product')).toMatchObject({state:'accepted',queueId:q});
   expect(refreshAll().map(r=>r.packageKey)).not.toContain('product');
   expect(JSON.stringify(svc.plan('xv')!.tasks.find(t=>t.key==='product')!)).toBe(acceptedBytes);
   // The scope-held unassigned task stays held, and the UNKNOWN owner effect still suppresses its duty.
   expect(results.find(r=>r.key==='repair')).toMatchObject({state:'held',reason:'dispatch-scope-expired'});
   expect(refreshAll().map(r=>r.packageKey)).not.toContain('repair');
   expect(db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE outbox_id='w-unknown-scope'").get()).toEqual({outbox_id:'w-unknown-scope',delivery_state:'indeterminate'});
  });
});

});
