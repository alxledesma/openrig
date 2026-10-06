import {RuntimeOutcomeAssessment,sanitizedAssessmentEvidence,type OutcomePolicy} from '../src/domain/runtime-outcome-assessment.js';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {resolve} from 'node:path';
import { describe,it,expect,beforeEach,afterEach,vi } from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,coordinationIdle,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {SeatActivityService} from '../src/domain/seat-activity-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';import {seed,token} from './helpers/coordinator-fixture.js';
describe('qualification historical complete-but-unclosable duty',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 const plan=(tasks:CoordinationTask[]):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks});
 function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
 function configure(tasks:CoordinationTask[],resources:Record<string,string[]>={},revision='r1'){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:resources[t.key]??[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});return svc.configure('operator-agent@kernel','operator-agent-g1',{...plan(tasks),revision});}
 function refresh(){for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));}
 function job(){db.prepare(`INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')`).run(new Date(clock).toISOString());}
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'coordination-'));db=createDb(join(dir,'db'));seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;});
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});

 const cfg=()=>({schemaVersion:1,mode:'observe',enabled:true,primary:'local',providers:{local:{adapter:'laya',endpoint:'http://127.0.0.1:18091/v1/systemone',model:'typed-decisions',acceptedModels:['qualified-fixture'],timeoutMs:100,allowedData:['private'],maxResponseBytes:16384,capabilities:{schema:'systemone.v1',primitives:['choice'],maxStateChars:10000,maxQuestions:10,maxOptions:10,inputCoverage:'reported'},threshold:{minTopProbability:0.8,calibrationId:'synthetic-test'}}}});
 function outcome(fetch:typeof globalThis.fetch,mode:'observe'|'enforce'='enforce',negative=false){const c=cfg();const r=new RuntimeOutcomeAssessment(repo,{fetchImpl:fetch},()=>clock);repo.coordinatorAuthority.runtimeOutcomeAssessment=r;const p:OutcomePolicy={rigId:'xv',revision:'p1',mode,operatorGeneration:'operator-agent-g1',dataClass:'private',allowPaid:false,allowUnqualifiedNegativeAdvice:negative,adapterConfig:c,qualification:{ref:'synthetic-test-only',providerConfigDigest:digest(JSON.stringify(c)),validUntil:clock+60000}};r.configure('operator-agent@kernel','operator-agent-g1',p);return r;}
 const reply=(choice='yes',coverage=true)=>vi.fn(async()=>new Response(JSON.stringify({model:'qualified-fixture',answers:{outcome:{type:'choice',choice,probabilities:{yes:choice==='yes'?.9:.05,no:choice==='no'?.9:.05,unknown:choice==='unknown'?.9:.05}}},usage:coverage?{input_tokens:150,output_tokens:0,truncated:false}:{input_tokens:150,output_tokens:0}}),{status:200})) as unknown as typeof fetch;
 async function returned(state:'done'|'failed'='done'){const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;repo.claim({qitemId:q,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:q,actorSession:'builder@xv',state,closureReason:state==='done'?'no-follow-on':undefined});await repo.create({qitemId:'result',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'report.md'}],outcomeSummary:'Required implementation will be finished later'}),nudge:false});repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','result');return q;}
 function storedPolicy(){return JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='runtime-outcome-policy' ORDER BY rowid DESC LIMIT 1").get() as {receipt:string}).receipt) as OutcomePolicy;}
 function qualificationRefresh(p:OutcomePolicy,dutyQueueId:string){return {rigId:'xv',dutyQueueId,operationId:'fresh-proof',policyRevision:p.revision,policyDigest:digest(JSON.stringify(p)),qualifiedAt:clock,qualification:{...p.qualification,ref:'actual-new-dated-proof',validUntil:clock+60000}};}
 const normal=()=>[task('product'),task('repair','architect@xv',{recoveryFor:'product'})];
 const completion=(id:string)=>{const r=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+id) as {receipt:string}|undefined;return r?JSON.parse(r.receipt):null;};
 const retirementFor=(id:string)=>db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(id) as {receipt:string}|undefined;
 /** The observed production failure: the qualification refresh committed in time, the observer tick missed the deadline. */
 function missedQualification(){
  configure(normal());const r=outcome(reply()),p=r.policy('xv')!;
  clock=p.qualification.validUntil+1;vi.setSystemTime(clock);
  const id=r.stagePolicyBoundary('xv')!,duty=svc.lifecycleControlReceipt(id)!;
  repo.claim({qitemId:id,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  vi.spyOn(svc,'observeLifecycleCompletion').mockImplementation(()=>{});
  r.refreshQualification('operator-agent@kernel','operator-agent-g1',qualificationRefresh(p,id));vi.restoreAllMocks();
  clock=duty.deadline+1;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+3600000);
  return {r,id,duty};
 }

 it('a complete but unclosable qualification duty gets the accountable failure-only retirement; the success receipt, timestamps and effect are untouched',()=>{
  const {r,id}=missedQualification();
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,expired:true,close:false,retired:false,failedByRecipient:false});
  expect(completion(id)).toBeNull();expect(()=>repo.update({qitemId:id,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow();
  const qualification=db.prepare("SELECT * FROM coordinator_operations WHERE kind='runtime-outcome-qualification'").all(),outboxBefore=db.prepare('SELECT * FROM outbox_entries ORDER BY outbox_id').all() as Array<{outbox_id:string}>;
  expect(retirementFor(id)).toBeUndefined();
  svc.reconcile('lead@xv','lead-g1','xv');
  const ret=retirementFor(id);expect(ret).toBeTruthy();const rr=JSON.parse(ret!.receipt);
  expect(rr).toMatchObject({kind:'lifecycle-retirement',targetQueueId:id,recipient:'operator-agent@kernel'});
  expect(JSON.parse(repo.getById(rr.queueId)!.body)).toMatchObject({action:'report-own-expired-administrative-duty-outcome',targetQueueId:id,grantsAuthority:false});
  expect(repo.getById(id)?.state).toBe('in-progress');expect(completion(id)).toBeNull();                       // no back-dated observation, nothing closed
  expect(db.prepare("SELECT * FROM coordinator_operations WHERE kind='runtime-outcome-qualification'").all()).toEqual(qualification);   // the success receipt is preserved
  for(const old of outboxBefore)expect(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(old.outbox_id)).toEqual(old);   // every existing effect row is untouched (UNKNOWN preserved)
  expect(db.prepare('SELECT 1 FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+rr.queueId)).toBeTruthy();                       // the retirement duty has its own accountable notice
  svc.reconcile('lead@xv','lead-g1','xv');expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(id)).toEqual({n:1});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  repo.claim({qitemId:rr.queueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  repo.update({qitemId:id,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'own expiry disposition'});
  expect(repo.getById(id)?.state).toBe('failed');expect(svc.dutyFacts(id)).toMatchObject({failedByRecipient:true,retired:true});expect(svc.dutyFacts(rr.queueId)).toMatchObject({complete:true});
  expect(db.prepare("SELECT * FROM coordinator_operations WHERE kind='runtime-outcome-qualification'").all()).toEqual(qualification);
  expect(r.policy('xv')!.qualification.ref).not.toBe(storedPolicy().qualification.ref);                          // the refreshed qualification stands
 });

 it('no retirement for a duty that closed in time, is incomplete, unexpired or unclaimed',()=>{
  configure(normal());const r=outcome(reply()),p=r.policy('xv')!;
  clock=p.qualification.validUntil+1;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+3600000);
  const id=r.stagePolicyBoundary('xv')!,duty=svc.lifecycleControlReceipt(id)!;
  svc.reconcile('lead@xv','lead-g1','xv');expect(retirementFor(id)).toBeUndefined();                               // live, unclaimed, incomplete
  repo.claim({qitemId:id,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  svc.reconcile('lead@xv','lead-g1','xv');expect(retirementFor(id)).toBeUndefined();                               // claimed, incomplete, unexpired
  r.refreshQualification('operator-agent@kernel','operator-agent-g1',qualificationRefresh(p,id));                   // captured now
  clock=duty.deadline+1;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+3600000);
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,close:true});svc.reconcile('lead@xv','lead-g1','xv');expect(retirementFor(id)).toBeUndefined();    // closable: nothing to retire
 });
});
