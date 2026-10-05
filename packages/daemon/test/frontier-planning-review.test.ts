import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationResult,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import type {ScopeSource,WorkClass} from '../src/domain/frontier-planning.js';
import {seed,token} from './helpers/coordinator-fixture.js';

/** Targeted regressions for the independent REQUEST_CHANGES review.
 *  B1 malformed/uncited citation bypass, B2 missing shared Act facet,
 *  B3 product+scope-bound completion, independent confirmation and reopen,
 *  N1 bounded observations, N3 admission provenance and product class,
 *  N4 frozen monotone receipt and idempotent replay. */
const BRIEF:ScopeSource={ref:'/app-handy/PRODUCT_BRIEF.md',digest:'b7b12dd8e79333976c0cc94578b18eed7fca40035f08a9e11be2b3cf1931f9b5'};
const ROADMAP:ScopeSource={ref:'/app-handy/ROADMAP.md',digest:'ba4006b7e320091a18d13af564a4c1e853ca0eddff9c461b17fa23035d126441'};

describe('frontier planning review regressions',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>,workClasses:Map<string,WorkClass>;
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();workClasses=new Map();dir=mkdtempSync(join(tmpdir(),'frontier-review-'));db=createDb(join(dir,'db'));seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;});
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});

 function sample(session:string):CoordinationActivity{const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
 function refresh(){for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));}
 function advance(ms:number){clock+=ms;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+600000);}
 function task(key:string,owner='builder@xv',more:Partial<CoordinationTask>&{workClass?:WorkClass}={}):CoordinationTask{
  const {workClass,...rest}=more;if(workClass)workClasses.set(key,workClass);
  return {key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...rest} as CoordinationTask;}
 const plan=(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks,scopeSources:[BRIEF,ROADMAP],...more});
 function admitContract(key:string,owner:string,workClass:WorkClass='product'){
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',key,{inputDigest:digest(key),destination:owner,bodyHash:digest(key),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass});}
 function configure(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}){
  for(const t of tasks)admitContract(t.packageKey,t.owner,workClasses.get(t.packageKey)??'product');
  return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,more));}
 const adminOnly=()=>[task('capacity-inquiry','reviewer@xv',{boundary:'owner-material',workClass:'administrative'})];
 const leadOwned=()=>[...adminOnly(),task('coordination-note','lead@xv',{boundary:'owner-material',workClass:'administrative'})];
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const frontierResult=()=>reconcile().find(r=>r.key==='frontier') as CoordinationResult|undefined;
 const planningControls=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning'").get() as {n:number};
 const observations=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='frontier-observation'").get() as {n:number};
 const dutyPacket=(queueId:string)=>JSON.parse(repo.getById(queueId)!.body);
 const stabilize=(states:readonly string[])=>{let r=frontierResult()!;for(let i=0;i<6&&states.includes(r.state);i++){advance(300001);r=frontierResult()!;}return r;};
 const proposal=(citations:unknown[]=[ROADMAP],over:Record<string,unknown>={})=>[{packageKey:'next-frontier',citations,resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},...over}];

 /** The real chain: planning duty -> scope-cited proposal -> Operator admission ->
  *  existing materialization -> genuine pickup -> genuine acceptance. */
 async function scopeBoundAcceptedPackage(citations:unknown[]=[ROADMAP]){
  const first=configure(adminOnly()),planning=stabilize(['stabilizing']),body=dutyPacket(planning.queueId!);
  repo.claim({qitemId:planning.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:planning.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal(citations)});
  const admission=frontierResult()!;
  repo.claim({qitemId:admission.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{inputDigest:digest('next-frontier'),destination:'builder@xv',bodyHash:digest('next-frontier'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass:'product'}}]});
  repo.update({qitemId:admission.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const materialize=reconcile().find(r=>r.key==='materialization:next-frontier')!;
  repo.claim({qitemId:materialize.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const next=task('next-frontier','builder@xv'),backup=task('next-frontier-repair','architect@xv',{recoveryFor:'next-frontier'});
  // next-frontier was already admitted through the frontier flow with its bound
  // contract; a package revision may not change its admitted contract bytes.
  admitContract(backup.packageKey,backup.owner);
  const retained=svc.plan('xv')!.tasks.map(t=>({...t,admission:{...t.admission,validUntil:clock+600000}}));
  svc.configure('operator-agent@kernel','operator-agent-g1',{...first,revision:'materialized-r2',tasks:[...retained,next,backup]});
  repo.update({qitemId:materialize.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  const assignment=reconcile().find(r=>r.key==='next-frontier')!;
  repo.claim({qitemId:assignment.queueId!,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  repo.update({qitemId:assignment.queueId!,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  await repo.create({qitemId:'next-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'next-frontier',inputDigest:digest('next-frontier'),evidence:[{kind:'report',ref:'actual/next.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','next-frontier','next-return');
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  svc.accept('lead@xv','lead-g1','xv','next-frontier','next-return','actual/accepted.md');
  return {planning,admission,packageKey:'next-frontier' as const};}

 // ------------------------------------------------------------------- B1
 it.each([
  ['empty citation object', {}],
  ['unknown ref without digest', {ref:'/elsewhere/GOALS.md'}],
  ['null citation', null],
  ['frozen ref without digest', {ref:ROADMAP.ref}],
  ['frozen ref with empty digest', {ref:ROADMAP.ref,digest:''}],
  ['frozen ref with wrong digest', {ref:ROADMAP.ref,digest:'0'.repeat(64)}]
 ])('refuses a plan-proposal candidate with a %s',(_label,citation)=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal([citation])})).toThrow('cite a frozen scope ref');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-admission'").get()).toBeUndefined();
 });

 it('refuses any plan-proposal while the genuine Operator has configured no scope source',()=>{
  configure(adminOnly(),{scopeSources:[]});
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal([{}])})).toThrow('cite a frozen scope ref');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
 });

 // ------------------------------------------------------------------- B2
 it.each(['epoch','lease','plan','reservation','restriction'])('refuses a Lead disposition once the shared act facet turns false on %s',(kind)=>{
  configure(kind==='restriction'?leadOwned():adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string,disposition:'plan-proposal' as const,proposal:proposal()};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  if(kind==='epoch')db.prepare('UPDATE coordinator_authority SET epoch=epoch+1').run();
  if(kind==='lease')db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock-1);
  // D10 r3 treats a plan revision as provenance, so this negative drives a revision that also
  // changes the frozen scope sources, which is a genuine Act-facet break the engine still enforces.
  if(kind==='plan'){const p=svc.plan('xv')!;svc.configure('operator-agent@kernel','operator-agent-g1',{...p,revision:'act-facet-r2',scopeSources:[{ref:BRIEF.ref,digest:'ac946e05f79b53b884bc2805048f49d12f0a84120f2e4cd4ee0bc3d6465c1f8a'}],tasks:p.tasks.map(t=>({...t,admission:{...t.admission,validUntil:clock+600000}}))});}
  if(kind==='reservation')db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('frontier-reservation','frontier-rotation','lead@xv','lead@xv','lead-g1','native-old','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  if(kind==='restriction'){const p=svc.plan('xv')!;svc.configure('operator-agent@kernel','operator-agent-g1',{...p,revision:'act-restriction-r2',tasks:p.tasks.map(t=>({...t,admission:{...t.admission,validUntil:clock+600000}})),dispatchRestrictions:[{session:'lead@xv',generation:'lead-g1',packageKeys:['coordination-note'],validUntil:clock+30000,evidenceRef:'native/checkpoint-evidence.json'}]});}
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',input)).toThrow('act facet');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key='next-frontier'").get()).toBeUndefined();
 });

 it('refuses the Operator admission once the shared act facet turns false',()=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  const admission=frontierResult()!;
  repo.claim({qitemId:admission.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  // Epoch is Operator-independent, so this negative uses the reservation gate the Act facet still enforces.
  db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('admission-reservation','admission-rotation','operator-agent@kernel','operator-agent@kernel','operator-agent-g1','native-old','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{inputDigest:digest('next-frontier'),destination:'builder@xv',bodyHash:digest('next-frontier'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass:'product'}}]})).toThrow('act facet');
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key='next-frontier'").get()).toBeUndefined();
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-admission-disposition'").get()).toBeUndefined();
 });

 // ------------------------------------------------------------------- B3
 it('refuses a frontier-complete that maps a scope item to an accepted but unbound package',async()=>{
  configure([task('legacy-product'),task('legacy-repair','architect@xv',{recoveryFor:'legacy-product'})]);
  const q=reconcile()[0]!.queueId!;
  repo.claim({qitemId:q,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  repo.update({qitemId:q,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  await repo.create({qitemId:'legacy-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'legacy-product',inputDigest:digest('legacy-product'),evidence:[{kind:'report',ref:'actual/legacy.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','legacy-product','legacy-return');
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  svc.accept('lead@xv','lead-g1','xv','legacy-product','legacy-return','actual/accepted.md');
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const deferred={ref:BRIEF.ref,deferral:{reason:'Not covered by any admitted package',authorizationRef:'owner/roadmap-r2'}};
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:BRIEF.ref,acceptedPackageKey:'legacy-product'},deferred]})).toThrow('scope-bound');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[deferred,{ref:ROADMAP.ref,deferral:{reason:'Deferred',authorizationRef:'owner/roadmap-r2'}}]})).not.toThrow();
 });

 it('holds frontier-complete until the genuine current Operator independently confirms it',async()=>{
  await scopeBoundAcceptedPackage();
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,acceptedPackageKey:'next-frontier'},{ref:BRIEF.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-r2'}}]});
  expect(frontierResult()?.state).not.toBe('frontier-complete');
  const confirmation=frontierResult()!;
  expect(confirmation).toMatchObject({state:'pending-native-frontier-confirmation'});
  expect(repo.getById(confirmation.queueId!)!.destinationSession).toBe('operator-agent@kernel');
  expect(()=>svc.recordFrontierConfirmation('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:recorded.completionDigest!})).toThrow('Current genuine Operator required');
  repo.claim({qitemId:confirmation.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const ack=svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:recorded.completionDigest!,evidenceRef:'operator/frontier-review-1'});
  expect(ack).toMatchObject({actor:'operator-agent@kernel',generation:'operator-agent-g1'});
  expect(frontierResult()).toMatchObject({state:'frontier-complete'});
  expect(()=>repo.update({qitemId:confirmation.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).not.toThrow();
 });

 it('keeps a blocked frontier accountable and reopens it only through the recorded unblock disposition',()=>{
  configure(adminOnly(),{scopeSources:[]});
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const blocked=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-blocked',boundary:'scope-source-missing',unblockCondition:'genuine Operator binds the goal documents'});
  const held=frontierResult()!;
  expect(held).toMatchObject({state:'held',reason:'frontier-boundary-blocked',activityEvidence:{boundary:'scope-source-missing'}});
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-boundary-intake'").get()).toBeTruthy();
  expect(db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='frontier-boundary-intake'").get()!.receipt).toContain('genuine Operator binds the goal documents');
  expect(planningControls()).toEqual({n:1});
  expect(()=>svc.recordFrontierReopen('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,dispositionDigest:blocked.dispositionDigest!,evidenceRef:'owner/scope-bound.json'})).toThrow('Current genuine Operator required');
  const reopened=svc.recordFrontierReopen('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:duty.queueId!,dispositionDigest:blocked.dispositionDigest!,evidenceRef:'operator/scope-bound.json'});
  expect(reopened).toMatchObject({actor:'operator-agent@kernel',boundary:'scope-source-missing'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const successor=stabilize(['held','stabilizing']);
  expect(successor.state).toBe('pending-native-frontier-planning');
  expect(successor.queueId).not.toBe(duty.queueId);
  expect(planningControls()).toEqual({n:2});
 });

 it('routes an Operator proposal refusal to an accountable reopen instead of a silent terminal',()=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  const admission=frontierResult()!;
  repo.claim({qitemId:admission.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const declined=svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,declined:{reason:'Candidate duplicates an existing roadmap item'}});
  expect(declined.declined).toMatchObject({reason:'Candidate duplicates an existing roadmap item'});
  const held=frontierResult()!;
  expect(held).toMatchObject({state:'held',reason:'frontier-boundary-declined'});
  expect(planningControls()).toEqual({n:1});
  const reopened=svc.recordFrontierReopen('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:duty.queueId!,dispositionDigest:recorded.proposal!.proposalDigest,evidenceRef:'operator/decline-revisit.json'});
  expect(reopened.evidenceRef).toBe('operator/decline-revisit.json');
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const successor=stabilize(['frontier-admission-declined','stabilizing']);
  expect(successor.state).toBe('pending-native-frontier-planning');
  expect(planningControls()).toEqual({n:2});
 });

 // ------------------------------------------------------------------- N1
 it('keeps frontier observations bounded once the stabilization decision is durable',()=>{
  configure(adminOnly());
  stabilize(['stabilizing']);
  const decided=observations();
  expect(decided.n).toBeLessThanOrEqual(3);
  for(let i=0;i<12;i++){advance(60001);reconcile();}
  expect(observations()).toEqual(decided);
  for(let i=0;i<5;i++)reconcile();
  expect(observations()).toEqual(decided);
  expect(svc.frontierProjection('xv')!.stabilization).toMatchObject({ready:true});
  expect(planningControls()).toEqual({n:1});
 });

 // ------------------------------------------------------------------- N3
 it('binds the proposal scope citations into the admitted contract and refuses a non-product work class',async()=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  const admission=frontierResult()!;
  repo.claim({qitemId:admission.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  const contract={inputDigest:digest('next-frontier'),destination:'builder@xv',bodyHash:digest('next-frontier'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}};
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{...contract,workClass:'administrative' as WorkClass}}]})).toThrow('Work class must be product');
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{...contract,resources:['invented.ts']}}]})).toThrow('match the frozen proposal');
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{...contract,returnContract:{destination:'peer@xv',evidenceRequired:['report']}}}]})).toThrow('match the frozen proposal');
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key='next-frontier'").get()).toBeUndefined();
  svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{...contract,workClass:'product' as WorkClass}}]});
  const row=db.prepare("SELECT contract FROM coordinator_packages WHERE package_key='next-frontier'").get() as {contract:string};
  expect(JSON.parse(row.contract)).toMatchObject({workClass:'product',scopeCitations:[ROADMAP]});
 });

 it('only accepts a scope mapping to an admitted product package that cites that exact scope ref',async()=>{
  await scopeBoundAcceptedPackage();
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const deferred={ref:BRIEF.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-r2'}};
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,acceptedPackageKey:'next-frontier-repair'},deferred]})).toThrow('scope-bound');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:BRIEF.ref,acceptedPackageKey:'next-frontier'},deferred]})).toThrow('scope-bound');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,acceptedPackageKey:'next-frontier'},deferred]})).not.toThrow();
 });

 // ------------------------------------------------------------------- N4
 it('keeps a recorded completion monotone against the duty frozen scope and replays idempotently after expiry',async()=>{
  const {planning}=await scopeBoundAcceptedPackage();
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const mapping=[{ref:ROADMAP.ref,acceptedPackageKey:'next-frontier'},{ref:BRIEF.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-r2'}}];
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping});
  const packet=dutyPacket(duty.queueId!),scopeSources=packet.scopeSources as ScopeSource[];
  // The Operator re-measures a source after the record: the duty stays complete.
  db.prepare("UPDATE coordinator_operations SET receipt=json_set(receipt,'$.scopeSources',json(?)) WHERE kind='coordination-plan'").run(JSON.stringify([{ref:BRIEF.ref,digest:'ac946e05f79b53b884bc2805048f49d12f0a84120f2e4cd4ee0bc3d6465c1f8a'},ROADMAP]));
  expect(svc.lifecycleControlCompleted(duty.queueId!)).toBe(true);
  // An exact replay after the finite duty expires returns the same receipt.
  clock=Date.parse(repo.getById(duty.queueId!)!.expiresAt!)+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+600000);refresh();
  expect(svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping})).toEqual(recorded);
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,deferral:{reason:'changed',authorizationRef:'owner/other'}},{ref:BRIEF.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-r2'}}]})).toThrow('cannot change');
  expect(scopeSources).toEqual([BRIEF,ROADMAP]);
  expect(planning.queueId).not.toBe(duty.queueId);
 });

 it('requires a fresh genuine Operator confirmation after a reopened completion is resubmitted identically',async()=>{
  await scopeBoundAcceptedPackage();
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const mapping=[{ref:ROADMAP.ref,acceptedPackageKey:'next-frontier'},{ref:BRIEF.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-r2'}}];
  const first=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping});
  const firstConfirmation=frontierResult()!;
  expect(firstConfirmation).toMatchObject({state:'pending-native-frontier-confirmation'});
  repo.claim({qitemId:firstConfirmation.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:firstConfirmation.queueId!,completionDigest:first.completionDigest!,evidenceRef:'operator/first-review.json'});
  expect(frontierResult()).toMatchObject({state:'frontier-complete'});
  // The Operator reviews again, finds the deferral invalid, and reopens with evidence.
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const reopened=svc.recordFrontierReopen('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:duty.queueId!,dispositionDigest:first.dispositionDigest!,evidenceRef:'operator/deferral-invalid.json'});
  expect(reopened.reopenDigest).toBeTruthy();
  expect(frontierResult()?.state).not.toBe('frontier-complete');
  // The Lead re-records the byte-identical mapping on the reopen successor duty.
  const successor=stabilize(['stabilizing']);
  expect(successor.state).toBe('pending-native-frontier-planning');
  expect(successor.queueId).not.toBe(duty.queueId);
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  repo.claim({qitemId:successor.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const second=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:successor.queueId!,frontierDigest:dutyPacket(successor.queueId!).frontierDigest,disposition:'frontier-complete',mapping});
  expect(second.completionDigest).toBe(first.completionDigest);
  // The earlier confirmation must not carry over: a fresh Operator confirmation is required.
  const secondConfirmation=frontierResult()!;
  expect(secondConfirmation).toMatchObject({state:'pending-native-frontier-confirmation'});
  expect(secondConfirmation.queueId).not.toBe(firstConfirmation.queueId);
  repo.claim({qitemId:secondConfirmation.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:secondConfirmation.queueId!,completionDigest:second.completionDigest!,evidenceRef:'operator/second-review.json'});
  expect(frontierResult()).toMatchObject({state:'frontier-complete'});
 });
});