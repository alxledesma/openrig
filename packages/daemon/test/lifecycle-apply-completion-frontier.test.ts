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

describe('frontier apply-time completion capture',()=>{
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

 // Each frontier operation freezes its duty's completion inside its own transaction, before the deadline.
 const completion=(id:string)=>{const r=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+id) as {receipt:string}|undefined;return r?JSON.parse(r.receipt):null;};
 const closeDuty=(id:string,session:string,gen:string)=>repo.update({qitemId:id,actorSession:session,actorGeneration:gen,identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
 const claim=(id:string,session:string,gen:string)=>repo.claim({qitemId:id,destinationSession:session,actorGeneration:gen,identityProvenance:'transport:v1'});
 const dead=(id:string)=>svc.lifecycleControlReceipt(id)!.deadline as number;
 const past=(id:string)=>{advance(dead(id)-clock+1);};

 it('frontier plan: the recorded disposition freezes completion, the duty closes after a missed tick, and a refused record writes nothing',()=>{
  configure(adminOnly());const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  claim(duty.queueId!,'lead@xv','lead-g1');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal([{ref:'/elsewhere/x.md',digest:'0'.repeat(64)}])})).toThrow();
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,frontierDigest:'f'.repeat(64),disposition:'plan-proposal',proposal:proposal()})).toThrow();
  expect(completion(duty.queueId!)).toBeNull();
  svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal()});
  expect(completion(duty.queueId!)).toMatchObject({queueId:duty.queueId,at:clock,outcomeOnly:true});expect(completion(duty.queueId!).at).toBeLessThan(dead(duty.queueId!));
  past(duty.queueId!);expect(svc.dutyFacts(duty.queueId!)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(duty.queueId!,'lead@xv','lead-g1');expect(repo.getById(duty.queueId!)?.state).toBe('done');
 });

 it('frontier plan counterfactual: with the capture disabled the same record cannot close after the deadline',()=>{
  configure(adminOnly());const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);claim(duty.queueId!,'lead@xv','lead-g1');
  vi.spyOn(svc,'observeLifecycleCompletion').mockImplementation(()=>{});
  svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  expect(completion(duty.queueId!)).toBeNull();past(duty.queueId!);
  expect(svc.dutyFacts(duty.queueId!)).toMatchObject({complete:true,close:false,expired:true});
  expect(()=>closeDuty(duty.queueId!,'lead@xv','lead-g1')).toThrow();
 });

 it('frontier admission: admitting the proposal freezes completion before the deadline; a wrong actor or a partial admission writes nothing; replay writes nothing',()=>{
  configure(adminOnly());const planning=stabilize(['stabilizing']),body=dutyPacket(planning.queueId!);claim(planning.queueId!,'lead@xv','lead-g1');
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:planning.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  const admission=frontierResult()!;claim(admission.queueId!,'operator-agent@kernel','operator-agent-g1');
  const admitted=[{packageKey:'next-frontier',contract:{inputDigest:digest('next-frontier'),destination:'builder@xv',bodyHash:digest('next-frontier'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}}];
  expect(()=>svc.admitFrontierProposal('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted})).toThrow();
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[]})).toThrow('Partial admission');
  expect(completion(admission.queueId!)).toBeNull();
  const receipt=svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted});
  expect(completion(admission.queueId!)).toMatchObject({queueId:admission.queueId,at:clock});expect(completion(admission.queueId!).at).toBeLessThan(dead(admission.queueId!));
  past(admission.queueId!);const before=(db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number}).n;
  expect(svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted})).toEqual(receipt);
  expect((db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number}).n).toBe(before);   // replay after the deadline writes nothing
  expect(svc.dutyFacts(admission.queueId!)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(admission.queueId!,'operator-agent@kernel','operator-agent-g1');expect(repo.getById(admission.queueId!)?.state).toBe('done');
 });

 it('frontier confirmation: the Operator confirmation freezes completion before the deadline and closes after a missed tick; a Lead cannot confirm',async()=>{
  await scopeBoundAcceptedPackage();
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);claim(duty.queueId!,'lead@xv','lead-g1');
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,acceptedPackageKey:'next-frontier'},{ref:BRIEF.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-r2'}}]});
  const confirmation=frontierResult()!;claim(confirmation.queueId!,'operator-agent@kernel','operator-agent-g1');
  expect(()=>svc.recordFrontierConfirmation('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:recorded.completionDigest!,evidenceRef:'x'})).toThrow('Current genuine Operator required');
  expect(()=>svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:'0'.repeat(64),evidenceRef:'x'})).toThrow();
  expect(completion(confirmation.queueId!)).toBeNull();
  svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:recorded.completionDigest!,evidenceRef:'operator/frontier-review-1'});
  expect(completion(confirmation.queueId!)).toMatchObject({queueId:confirmation.queueId,at:clock});expect(completion(confirmation.queueId!).at).toBeLessThan(dead(confirmation.queueId!));
  past(confirmation.queueId!);expect(svc.dutyFacts(confirmation.queueId!)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(confirmation.queueId!,'operator-agent@kernel','operator-agent-g1');expect(repo.getById(confirmation.queueId!)?.state).toBe('done');
 });

 it('a failing capture rolls the frontier record back and a retry succeeds',()=>{
  configure(adminOnly());const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);claim(duty.queueId!,'lead@xv','lead-g1');
  const input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()};
  const spy=vi.spyOn(svc,'observeLifecycleCompletion').mockImplementationOnce(()=>{throw new Error('observer failed');});
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',input)).toThrow('observer failed');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();expect(completion(duty.queueId!)).toBeNull();
  spy.mockRestore();svc.recordFrontierPlan('lead@xv','lead-g1',input);expect(completion(duty.queueId!)).not.toBeNull();
 });
});
