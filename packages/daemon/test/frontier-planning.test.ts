import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationResult,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import type {ScopeSource,WorkClass} from '../src/domain/frontier-planning.js';
import {seed,token} from './helpers/coordinator-fixture.js';

/** Real refs and digests from the prepared snapshot. The runtime never reads or
 *  interprets their contents: only the genuine Operator binds them. */
const BRIEF:ScopeSource={ref:'/app-handy/PRODUCT_BRIEF.md',digest:'b7b12dd8e79333976c0cc94578b18eed7fca40035f08a9e11be2b3cf1931f9b5'};
const ROADMAP:ScopeSource={ref:'/app-handy/ROADMAP.md',digest:'ba4006b7e320091a18d13af564a4c1e853ca0eddff9c461b17fa23035d126441'};
const DRIFTED_ROADMAP:ScopeSource={ref:'/app-handy/ROADMAP.md',digest:'ac946e05f79b53b884bc2805048f49d12f0a84120f2e4cd4ee0bc3d6465c1f8a'};
const contractFor=(key:string,workClass:WorkClass='product')=>({inputDigest:digest(key),destination:'builder@xv',bodyHash:digest(key),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass});

describe('exhausted product frontier planning lifecycle',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>,workClasses:Map<string,WorkClass>;
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();workClasses=new Map();dir=mkdtempSync(join(tmpdir(),'frontier-'));db=createDb(join(dir,'db'));seed(db);
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
 function configure(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}){
  for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{...contractFor(t.key,workClasses.get(t.packageKey)??'product'),destination:t.owner,bodyHash:digest(t.body),inputDigest:digest(t.key)});
  return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,more));}
 const adminOnly=()=>[task('capacity-inquiry','reviewer@xv',{boundary:'owner-material',workClass:'administrative'})];
 const productWork=()=>[task('product'),task('repair','architect@xv',{recoveryFor:'product'})];
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const frontierResult=()=>reconcile().find(r=>r.key==='frontier') as CoordinationResult|undefined;
 const planningControls=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning'").get() as {n:number};
 const dutyPacket=(queueId:string)=>JSON.parse(repo.getById(queueId)!.body);
 const proposal=(over:Record<string,unknown>={})=>[{packageKey:'next-frontier',citations:[ROADMAP],resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},...over}];
 async function finishTyped(packageKey:string,owner:string,queueId:string,returnId:string){
  repo.claim({qitemId:queueId,destinationSession:owner,identityProvenance:'transport:v1'});repo.update({qitemId:queueId,actorSession:owner,state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:returnId,sourceSession:owner,destinationSession:'lead@xv',body:JSON.stringify({packageKey,inputDigest:digest(packageKey),evidence:[{kind:'report',ref:'actual/'+packageKey+'.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose(owner,repo.coordinatorAuthority.generation(owner)!,'xv',packageKey,returnId);db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();}
 /** Drives the durable observation runs until the stabilization floor is met.
  *  Bounded so a regression fails loudly instead of hanging. */
 function stabilize(states:readonly string[]):CoordinationResult{
  let result=frontierResult()!;
  for(let i=0;i<6&&states.includes(result.state);i++){advance(300001);result=frontierResult()!;}
  return result;}

 it('issues exactly one accountable Lead planning duty once an administrative-only frontier durably stabilizes',()=>{
  configure(adminOnly());
  expect(svc.frontierProjection('xv')).toMatchObject({state:'EXHAUSTED',reason:'no-authorized-product-frontier-remains'});
  expect(frontierResult()).toMatchObject({state:'stabilizing',reason:'frontier-stabilization-pending'});
  expect(planningControls()).toEqual({n:0});
  const duty=stabilize(['stabilizing']);
  expect(duty.state).toBe('pending-native-frontier-planning');
  expect(planningControls()).toEqual({n:1});
  expect(JSON.parse(db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='coordinator-lifecycle-control'").get(duty.queueId)!.receipt as string)).toMatchObject({kind:'frontier-planning',recipient:'lead@xv',recipientGeneration:'lead-g1',holder:'lead@xv',epoch:1,grantsAuthority:false,packageKey:'frontier-planning'});
  const packet=dutyPacket(duty.queueId!);
  expect(packet).toMatchObject({action:'plan-exact-next-product-frontier',grantsAuthority:false,scopeSources:[BRIEF,ROADMAP]});
  expect(packet.required).toContain('coordinator coordination-frontier-plan');
  expect(packet.required).toContain('scope-source-missing');
  expect(svc.frontierProjection('xv')!.stabilization).toMatchObject({requiredObservations:2,requiredMs:300000,ready:true});
  expect(svc.frontierProjection('xv')!.stabilization.observations).toBeGreaterThanOrEqual(2);
  // A repeated observation yields the same duty and never a completeness claim.
  expect(frontierResult()).toMatchObject({state:'pending-native-frontier-planning',queueId:duty.queueId});
  expect(planningControls()).toEqual({n:1});
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','lead@xv',duty.queueId!)).not.toThrow();
 });

 it('does not duplicate planning while admitted product scope is in flight or awaiting acceptance',async()=>{
  configure(productWork());
  expect(svc.frontierProjection('xv')).toMatchObject({state:'ACTIVE'});
  expect(frontierResult()).toBeUndefined();
  const product=reconcile()[0]!.queueId!;
  repo.claim({qitemId:product,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  expect(svc.frontierProjection('xv')).toMatchObject({state:'ACTIVE'});
  repo.update({qitemId:product,actorSession:'builder@xv',state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:'returned',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'actual/product.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','returned');
  expect(svc.frontierProjection('xv')!.packages).toContainEqual({packageKey:'product',workClass:'product',status:'awaiting-acceptance',legacyClass:false});
  expect(frontierResult()).toBeUndefined();
  advance(300001);advance(300001);
  expect(frontierResult()).toBeUndefined();
  expect(planningControls()).toEqual({n:0});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  svc.accept('lead@xv','lead-g1','xv','product','returned','actual/accepted.md');
  // The dormant recovery backup is not work, so the frontier is now genuinely exhausted.
  expect(svc.frontierProjection('xv')).toMatchObject({state:'EXHAUSTED'});
  expect(frontierResult()).toMatchObject({state:'stabilizing',reason:'frontier-stabilization-pending'});
  expect(planningControls()).toEqual({n:0});
 });

 it('keeps a registered unplanned product package on its existing materialization duty instead of planning',()=>{
  configure(adminOnly());
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','unplanned-product',{inputDigest:digest('unplanned'),destination:'builder@xv',bodyHash:digest('unplanned-product'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass:'product'});
  expect(svc.frontierProjection('xv')).toMatchObject({state:'MATERIALIZABLE'});
  const r=reconcile();
  expect(r.find(x=>x.key==='materialization:unplanned-product')).toMatchObject({state:'pending-native-materialization'});
  expect(r.find(x=>x.key==='frontier')).toBeUndefined();
  advance(300001);advance(300001);advance(300001);
  expect(reconcile().find(x=>x.key==='frontier')).toBeUndefined();
  expect(planningControls()).toEqual({n:0});
 });

 it('refuses an uncited or mis-digested proposal and admits only the genuine claimed Lead disposition',()=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  expect(duty.state).toBe('pending-native-frontier-planning');
  expect(()=>svc.recordFrontierPlan('operator-agent@kernel','operator-agent-g1',{...input,disposition:'plan-proposal',proposal:proposal()})).toThrow('genuine current recipient');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal()})).toThrow('Exact genuine native claim');
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'});
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,frontierDigest:'0'.repeat(64),disposition:'plan-proposal',proposal:proposal()})).toThrow('frozen frontier digest');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal({citations:[{ref:'/elsewhere/GOALS.md',digest:ROADMAP.digest}]})})).toThrow('cite a frozen scope ref');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal({citations:[{ref:ROADMAP.ref,digest:DRIFTED_ROADMAP.digest}]})})).toThrow('cite a frozen scope ref');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal({returnContract:{destination:'lead@xv',evidenceRequired:[]}})})).toThrow('non-empty required evidence');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
  const receipt=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal()});
  expect(receipt).toMatchObject({disposition:'plan-proposal',actor:'lead@xv',generation:'lead-g1',proposal:{packages:[{packageKey:'next-frontier',citations:[ROADMAP]}]}});
  expect(svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal()})).toEqual(receipt);
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal({packageKey:'other'})})).toThrow('cannot change');
  // The shared close gate accepts exactly this attributed disposition.
  expect(()=>repo.update({qitemId:duty.queueId!,actorSession:'lead@xv',state:'done',closureReason:'no-follow-on'})).not.toThrow();
 });

 it('hands a scope-cited proposal to the current Operator, then real admission reaches the existing qualified dispatch',()=>{
  const first=configure(adminOnly()),duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  const admission=frontierResult()!;
  expect(admission).toMatchObject({state:'pending-native-frontier-admission'});
  const admissionBody=dutyPacket(admission.queueId!);
  expect(repo.getById(admission.queueId!)!.destinationSession).toBe('operator-agent@kernel');
  expect(admissionBody).toMatchObject({action:'admit-or-refuse-exact-scope-cited-frontier-proposal',proposalDigest:recorded.proposal!.proposalDigest});
  expect(admissionBody.proposal).toEqual([{packageKey:'next-frontier',citations:[ROADMAP],resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}]);
  // The Lead's proposal creates nothing: no package, no plan task, no dispatch.
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key='next-frontier'").get()).toBeUndefined();
  expect(svc.plan('xv')!.tasks.map(t=>t.packageKey)).toEqual(['capacity-inquiry']);
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual({n:0});
  expect(()=>svc.admitFrontierProposal('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:admissionBody.proposalDigest,admitted:[{packageKey:'next-frontier',contract:contractFor('next-frontier')}]})).toThrow('Current genuine Operator required');
  repo.claim({qitemId:admission.queueId!,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:admissionBody.proposalDigest,admitted:[{packageKey:'next-frontier'}]})).toThrow('Exact supported package contract');
  expect(()=>svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:admissionBody.proposalDigest,admitted:[{packageKey:'next-frontier',contract:contractFor('next-frontier')},{packageKey:'smuggled',contract:contractFor('smuggled')}]})).toThrow('Partial admission');
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key IN ('next-frontier','smuggled')").get()).toBeUndefined();
  const admitted=svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:admissionBody.proposalDigest,admitted:[{packageKey:'next-frontier',contract:contractFor('next-frontier')}]});
  const registered=db.prepare("SELECT contract_hash FROM coordinator_packages WHERE package_key='next-frontier'").get() as {contract_hash:string};
  expect(admitted).toMatchObject({actor:'operator-agent@kernel',generation:'operator-agent-g1',admitted:[{packageKey:'next-frontier',contractHash:registered.contract_hash}]});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  expect(()=>repo.update({qitemId:admission.queueId!,actorSession:'operator-agent@kernel',state:'done',closureReason:'no-follow-on'})).not.toThrow();
  // The existing materialization duty, unchanged, now carries the work forward.
  const materialize=reconcile().find(r=>r.key==='materialization:next-frontier')!;
  expect(materialize).toMatchObject({state:'pending-native-materialization'});
  expect(reconcile().find(r=>r.key==='frontier')).toBeUndefined();
  repo.claim({qitemId:materialize.queueId!,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});
  const next=task('next-frontier','builder@xv'),backup=task('next-frontier-repair','architect@xv',{recoveryFor:'next-frontier'});
  for(const t of [next,backup])repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{...contractFor(t.key),destination:t.owner,bodyHash:digest(t.body),inputDigest:digest(t.key)});
  // The genuine Operator refreshes the retained task's current admission for the successor revision.
  const retained=svc.plan('xv')!.tasks.map(t=>({...t,admission:{...t.admission,validUntil:clock+600000}}));
  svc.configure('operator-agent@kernel','operator-agent-g1',{...first,revision:'materialized-r2',tasks:[...retained,next,backup]});
  expect(()=>repo.update({qitemId:materialize.queueId!,actorSession:'operator-agent@kernel',state:'done',closureReason:'no-follow-on'})).not.toThrow();
  const assignment=reconcile().find(r=>r.key==='next-frontier')!;
  expect(assignment).toMatchObject({state:'pending-pickup'});
  repo.claim({qitemId:assignment.queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  expect(reconcile().find(r=>r.key==='next-frontier')).toMatchObject({state:'picked-up'});
  expect(svc.frontierProjection('xv')).toMatchObject({state:'ACTIVE'});
 });

 it('records a legitimate completion only against accepted work, and scope digest drift fences the stale record',async()=>{
  const first=configure(productWork());
  const q=reconcile()[0]!.queueId!;await finishTyped('product','builder@xv',q,'product-return');
  svc.accept('lead@xv','lead-g1','xv','product','product-return','actual/accepted.md');
  expect(svc.frontierProjection('xv')).toMatchObject({state:'EXHAUSTED'});
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'});
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:BRIEF.ref},{ref:ROADMAP.ref}]})).toThrow('Map to exactly one');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:BRIEF.ref,acceptedPackageKey:'never-admitted'},{ref:ROADMAP.ref,deferral:{reason:'later',authorizationRef:'owner/x'}}]})).toThrow('accepted package or an authorized deferral');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,deferral:{reason:'later',authorizationRef:'owner/x'}}]})).toThrow('exactly one mapping');
  const receipt=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[{ref:BRIEF.ref,acceptedPackageKey:'product'},{ref:ROADMAP.ref,deferral:{reason:'Deferred by the owner roadmap decision',authorizationRef:'owner/roadmap-decision-7'}}]});
  expect(receipt.disposition).toBe('frontier-complete');
  expect(frontierResult()).toMatchObject({state:'frontier-complete',reason:'complete-as-of:'+receipt.scopeSourcesDigest});
  expect(planningControls()).toEqual({n:1});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  // The Operator re-measures a source: the digest moves and the completion is stale.
  svc.configure('operator-agent@kernel','operator-agent-g1',{...first,revision:'scope-drift-r2',scopeSources:[BRIEF,DRIFTED_ROADMAP]});
  const after=frontierResult()!;
  expect(after.state).not.toBe('frontier-complete');
  expect(planningControls()).toEqual({n:1});
  advance(300001);
  const fresh=stabilize(['stabilizing']);
  expect(fresh.state).toBe('pending-native-frontier-planning');
  expect(fresh.queueId).not.toBe(duty.queueId);
  expect(planningControls()).toEqual({n:2});
 });

 it('gives missing scope an accountable boundary and refuses completeness against an empty scope',()=>{
  const initial=configure(adminOnly(),{scopeSources:[]});
  expect(initial.scopeSources).toEqual([]);
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  expect(duty.state).toBe('pending-native-frontier-planning');
  expect(body.scopeSources).toEqual([]);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'});
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:[]})).toThrow('Scope sources must be configured');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-blocked',boundary:'owner-material',unblockCondition:'owner provides the brief'})).toThrow('the only accountable boundary is scope-source-missing');
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-blocked',boundary:'scope-source-missing'})).toThrow('Exactly the disposition payload');
  const blocked=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-blocked',boundary:'scope-source-missing',unblockCondition:'genuine Operator binds the goal documents with digests'});
  expect(blocked).toMatchObject({disposition:'frontier-blocked',boundary:'scope-source-missing'});
  // A protected boundary is a hold, never completeness.
  expect(frontierResult()).toMatchObject({state:'held',reason:'scope-source-missing'});
  // Binding scope sources reopens planning under a new digest.
  const retained=svc.plan('xv')!.tasks.map(t=>({...t,admission:{...t.admission,validUntil:clock+600000}}));
  svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'scope-bound-r2',tasks:retained,scopeSources:[BRIEF,ROADMAP]});
  expect(frontierResult()?.state).toBe('stabilizing');
  expect(planningControls()).toEqual({n:1});
 });

 it('reuses the shared duty fences: an uncertain notice and epoch drift never bypass a control',()=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  expect(duty.state).toBe('pending-native-frontier-planning');
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE audit_pointer=?").run(duty.queueId);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'});
  const notice=db.prepare("SELECT outbox_id FROM outbox_entries WHERE audit_pointer=?").get(duty.queueId)! as {outbox_id:string};
  // A planning duty is not a held-history control and consumes no foreign notice.
  expect(svc.heldHistoryAuthoringClaimAllowed(duty.queueId!,'lead@xv','lead-g1','transport:v1')).toBe(false);
  expect(svc.heldHistoryNoticeOutcomeContained('xv',db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(notice.outbox_id))).toBe(false);
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal()})).not.toThrow();
  // UNKNOWN stays UNKNOWN: no retry, no restatement, no synthetic delivery.
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get(notice.outbox_id)).toMatchObject({delivery_state:'indeterminate'});
  // Epoch drift supersedes the binding: no fresh grant, no acceptance, no invented history.
  db.prepare('UPDATE coordinator_authority SET epoch=epoch+1').run();
  expect(svc.validLifecycleControlWake('watchdog@system','lead@xv',duty.queueId!)).toBe(false);
  expect(()=>repo.coordinatorAuthority.assertManagedSend('watchdog@system','lead@xv',duty.queueId!)).toThrow();
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key='next-frontier'").get()).toBeUndefined();
 });

 it('refuses a recorded disposition after the finite duty expires and never extends its authority',()=>{
  configure(adminOnly());
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!),input={rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest as string};
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const expiry=repo.getById(duty.queueId!)!.expiresAt!;
  clock=Date.parse(expiry)+1;vi.setSystemTime(clock);db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+600000);refresh();
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposal()})).toThrow('Expired duty authority is never extended');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
  expect(repo.getById(duty.queueId!)!.expiresAt).toBe(expiry);
 });

 it('holds the planning duty behind checkpoint quiescence instead of reporting completeness',()=>{
  configure([...adminOnly(),task('coordination-note','lead@xv',{boundary:'owner-material',workClass:'administrative'})],{dispatchRestrictions:[{session:'lead@xv',generation:'lead-g1',packageKeys:['coordination-note'],validUntil:clock+30000,evidenceRef:'native/checkpoint-evidence.json'}]});
  expect(svc.frontierProjection('xv')).toMatchObject({state:'EXHAUSTED'});
  const held=stabilize(['stabilizing']);
  expect(held).toMatchObject({state:'held',reason:'lifecycle-recipient-protected'});
  expect(held.state).not.toBe('frontier-complete');
  expect(planningControls()).toEqual({n:0});
 });
});