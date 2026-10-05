import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationResult,type CoordinationTask} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

/** Affected D11 integration cases against the actual D10 shared duty engine.
 *  Every frontier duty is claimed through the real queue-repository claim fence and
 *  decided by the real dutyFacts facets; nothing here mocks the shared engine. */
const BRIEF={ref:'/app-handy/PRODUCT_BRIEF.md',digest:'b7b12dd8e79333976c0cc94578b18eed7fca40035f08a9e11be2b3cf1931f9b5'};
const ROADMAP={ref:'/app-handy/ROADMAP.md',digest:'ba4006b7e320091a18d13af564a4c1e853ca0eddff9c461b17fa23035d126441'};

describe('frontier lifecycle on the shared D10 duty engine',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'frontier-int-'));db=createDb(join(dir,'db'));seed(db);
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
 function task(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask{return {key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more};}
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const frontierResult=()=>reconcile().find(r=>r.key==='frontier') as CoordinationResult|undefined;
 const delivered=()=>db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
 const claimLead=(queueId:string)=>repo.claim({qitemId:queueId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
 const claimOperator=(queueId:string)=>repo.claim({qitemId:queueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
 const proposalInput=(packageKey='next-frontier')=>[{packageKey,citations:[ROADMAP],resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}];
 const admittedContract=(key:string)=>({inputDigest:digest(key),destination:'builder@xv',bodyHash:digest(key),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass:'product' as const});
 const deferrals=[{ref:ROADMAP.ref,deferral:{reason:'No admitted package carries this scope item',authorizationRef:'owner/roadmap-2'}},{ref:BRIEF.ref,deferral:{reason:'Deferred by owner decision',authorizationRef:'owner/roadmap-3'}}];

 function admitPackage(key:string,owner:string,workClass:'product'|'administrative'='product'){repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',key,{inputDigest:digest(key),destination:owner,bodyHash:digest(key),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass});}
 function configureAdminOnly(){const t=task('capacity-inquiry','reviewer@xv',{boundary:'owner-material'});admitPackage('capacity-inquiry','reviewer@xv','administrative');return svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,scopeSources:[BRIEF,ROADMAP],tasks:[t]});}
 function stabilize(){let r=frontierResult()!;for(let i=0;i<6&&r.state==='stabilizing';i++){advance(300001);r=frontierResult()!;}return r;}
 const dutyPacket=(queueId:string)=>JSON.parse(repo.getById(queueId)!.body);
 const planningInput=(duty:CoordinationResult)=>({rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:dutyPacket(duty.queueId!).frontierDigest as string});

 it('registers frontier kinds in the shared kind table and lets the shared engine decide claim and act',()=>{
  configureAdminOnly();
  const duty=stabilize();
  expect(duty.state).toBe('pending-native-frontier-planning');
  expect(svc.lifecycleControlReceipt(duty.queueId!)).toMatchObject({kind:'frontier-planning',recipient:'lead@xv',recipientGeneration:'lead-g1',holder:'lead@xv',holderGeneration:'lead-g1'});
  expect(svc.dutyFacts(duty.queueId!)).toMatchObject({claim:true,complete:false,retired:false,expired:false});
  claimLead(duty.queueId!);
  expect(svc.dutyFacts(duty.queueId!)).toMatchObject({claim:true,act:true,complete:false});
  // The D10 claim fence requires the explicit claimant generation for any lifecycle control.
  expect(()=>repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',identityProvenance:'transport:v1'})).toThrow('Exact current native Lead and live authoring duty proof required');
 });

 it.each(['epoch','lease','holder'])('refuses the Lead disposition through the shared engine on stale %s',(kind)=>{
  configureAdminOnly();
  const duty=stabilize(),input=planningInput(duty);
  claimLead(duty.queueId!);
  if(kind==='epoch')db.prepare('UPDATE coordinator_authority SET epoch=epoch+1').run();
  if(kind==='holder')db.prepare("UPDATE occupant_tenures SET generation_uuid='lead-g2' WHERE node_id='lead@xv'").run();
  if(kind==='lease')db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock-1);
  if(kind!=='lease')expect(svc.dutyFacts(duty.queueId!).act).toBe(false);
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-blocked',boundary:'owner-material',unblockCondition:'owner supplies material'})).toThrow(/act facet|genuine current recipient/);
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
 });

 it.each(['reservation','restriction','unknown'])('preserves shared %s protection and never issues a claimable frontier duty',(kind)=>{
  configureAdminOnly();
  if(kind==='reservation')db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('frontier-res','frontier-rotation','lead@xv','lead@xv','lead-g1','native-old','operator-agent@kernel','operator-agent-g1','h','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  if(kind==='unknown')repo.stageWakeIntent('frontier-unknown','watchdog@system','lead@xv','system:operator-authorized-coordination',true,'lead-g1');
  if(kind==='restriction'){const p=svc.plan('xv')!,t=task('coordination-note','lead@xv',{boundary:'owner-material'});admitPackage('coordination-note','lead@xv','administrative');svc.configure('operator-agent@kernel','operator-agent-g1',{...p,revision:'r2-restriction',tasks:[...p.tasks,t],dispatchRestrictions:[{session:'lead@xv',generation:'lead-g1',packageKeys:['coordination-note'],validUntil:clock+30000,evidenceRef:'native/checkpoint.json'}]});}
  let r=frontierResult()!;
  for(let i=0;i<6&&r.state==='stabilizing';i++){advance(300001);r=frontierResult()!;}
  expect(['held','stabilizing']).toContain(r.state);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning'").get()).toEqual({n:0});
  if(kind==='unknown'){
   const notice=db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id='wake-intent-frontier-unknown'").get() as {delivery_state:string};
   expect(['pending','indeterminate']).toContain(notice.delivery_state);
   expect(notice.delivery_state).not.toBe('delivered');
  }
 });

 it('routes a recipient-protected frontier hold into at most one accountable shared intake',()=>{
  configureAdminOnly();
  repo.stageWakeIntent('frontier-unknown','watchdog@system','lead@xv','system:operator-authorized-coordination',true,'lead-g1');
  let r=frontierResult()!;
  for(let i=0;i<6&&r.state==='stabilizing';i++){advance(300001);r=frontierResult()!;}
  expect(r.state).toBe('held');
  const intakes=db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.reason') IN ('lifecycle-recipient-protected','frontier-planning-duty-exhausted')").all() as Array<{qitem_id:string}>;
  expect(intakes.length).toBeLessThanOrEqual(1);
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning'").get()).toEqual({n:0});
 });

 it('carries a product proposal through genuine Operator admission, materialization and real worker pickup on the shared engine',()=>{
  const first=configureAdminOnly();
  const duty=stabilize(),input=planningInput(duty);
  claimLead(duty.queueId!);
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'plan-proposal',proposal:proposalInput()});
  const admission=frontierResult()!;
  expect(admission).toMatchObject({state:'pending-native-frontier-admission'});
  expect(repo.getById(admission.queueId!)!.destinationSession).toBe('operator-agent@kernel');
  expect(svc.dutyFacts(admission.queueId!)).toMatchObject({claim:true,complete:false});
  claimOperator(admission.queueId!);
  expect(()=>svc.admitFrontierProposal('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:admittedContract('next-frontier')}]})).toThrow('Current genuine Operator required');
  const admitted=svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:admittedContract('next-frontier')}]});
  expect(admitted.admitted[0]).toMatchObject({packageKey:'next-frontier',scopeCitations:[ROADMAP]});
  delivered();
  // The Operator closes its own duty with exact native custody fields, never a Lead claim.
  expect(()=>repo.update({qitemId:admission.queueId!,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow();
  expect(()=>repo.update({qitemId:admission.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).not.toThrow();
  // The shared materialization duty, unchanged, carries the registered product.
  const materialize=reconcile().find(r=>r.key==='materialization:next-frontier')!;
  expect(materialize).toMatchObject({state:'pending-native-materialization'});
  expect(svc.dutyFacts(materialize.queueId!)).toMatchObject({claim:true,complete:false});
  claimOperator(materialize.queueId!);
  expect(()=>repo.update({qitemId:materialize.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow();
  const next=task('next-frontier','builder@xv'),backup=task('next-frontier-repair','architect@xv',{recoveryFor:'next-frontier'});
  admitPackage(backup.packageKey,backup.owner);
  const retained=svc.plan('xv')!.tasks.map(t=>({...t,admission:{...t.admission,validUntil:clock+600000}}));
  svc.configure('operator-agent@kernel','operator-agent-g1',{...first,revision:'materialized-r2',tasks:[...retained,next,backup]});
  expect(()=>repo.update({qitemId:materialize.queueId!,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).not.toThrow();
  // Real worker assignment, genuine pickup, real typed return and acceptance.
  const assignment=reconcile().find(r=>r.key==='next-frontier')!;
  expect(assignment).toMatchObject({state:'pending-pickup'});
  repo.claim({qitemId:assignment.queueId!,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1'});
  expect(reconcile().find(r=>r.key==='next-frontier')).toMatchObject({state:'picked-up'});
  repo.update({qitemId:assignment.queueId!,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  delivered();
  // Real pickup and a real typed return were reached; the frontier is not exhausted while the
  // configured recovery task is still planned, and the returned product awaits genuine acceptance.
  expect(repo.getById(assignment.queueId!)!.state).toBe('done');
  expect(svc.frontierProjection('xv')).toMatchObject({state:'ACTIVE'});
  expect(svc.frontierProjection('xv')!.packages).toContainEqual({packageKey:'next-frontier',workClass:'product',status:'awaiting-acceptance',legacyClass:false});
 });

 it('requires a fresh Operator confirmation after a reopened completion is resubmitted identically',()=>{
  configureAdminOnly();
  const duty=stabilize(),input=planningInput(duty);
  claimLead(duty.queueId!);
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{...input,disposition:'frontier-complete',mapping:deferrals});
  const confirmation=frontierResult()!;
  expect(confirmation).toMatchObject({state:'pending-native-frontier-confirmation'});
  expect(repo.getById(confirmation.queueId!)!.destinationSession).toBe('operator-agent@kernel');
  claimOperator(confirmation.queueId!);
  expect(()=>svc.recordFrontierConfirmation('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:recorded.completionDigest!,evidenceRef:'x'})).toThrow('Current genuine Operator required');
  svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:confirmation.queueId!,completionDigest:recorded.completionDigest!,evidenceRef:'operator/first.json'});
  expect(frontierResult()).toMatchObject({state:'frontier-complete'});
  delivered();
  const reopened=svc.recordFrontierReopen('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:duty.queueId!,dispositionDigest:recorded.dispositionDigest!,evidenceRef:'operator/deferral-invalid.json'});
  expect(reopened.reopenDigest).toBeTruthy();
  expect(frontierResult()?.state).not.toBe('frontier-complete');
  const successor=stabilize();
  expect(successor.state).toBe('pending-native-frontier-planning');
  expect(successor.queueId).not.toBe(duty.queueId);
  delivered();
  claimLead(successor.queueId!);
  const second=svc.recordFrontierPlan('lead@xv','lead-g1',{...planningInput(successor),disposition:'frontier-complete',mapping:deferrals});
  expect(second.completionDigest).toBe(recorded.completionDigest);
  const secondConfirmation=frontierResult()!;
  expect(secondConfirmation).toMatchObject({state:'pending-native-frontier-confirmation'});
  expect(secondConfirmation.queueId).not.toBe(confirmation.queueId);
  claimOperator(secondConfirmation.queueId!);
  svc.recordFrontierConfirmation('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:secondConfirmation.queueId!,completionDigest:second.completionDigest!,evidenceRef:'operator/second.json'});
  expect(frontierResult()).toMatchObject({state:'frontier-complete'});
 });
});