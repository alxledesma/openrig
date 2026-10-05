import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationResult,type CoordinationTask} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

/** B-INT-R1: every frontier hold keeps an accountable owner through the shared
 *  task-hold intake. A first-issue hold has no duty row, so the hold itself must
 *  carry the reserved package key, its exact native owner and the frozen
 *  frontier/proposal/completion identity. */
const BRIEF={ref:'/app-handy/PRODUCT_BRIEF.md',digest:'b7b12dd8e79333976c0cc94578b18eed7fca40035f08a9e11be2b3cf1931f9b5'};
const ROADMAP={ref:'/app-handy/ROADMAP.md',digest:'ba4006b7e320091a18d13af564a4c1e853ca0eddff9c461b17fa23035d126441'};

describe('frontier holds route to exactly one accountable shared intake',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'frontier-intake-'));db=createDb(join(dir,'db'));seed(db);
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
 const delivered=()=>db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
 const claimLead=(q:string)=>repo.claim({qitemId:q,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
 const claimOperator=(q:string)=>repo.claim({qitemId:q,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
 const block=(session:string,id:string)=>repo.stageWakeIntent(id,'watchdog@system',session,'system:operator-authorized-coordination',true,repo.coordinatorAuthority.generation(session)!);
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const frontierResult=()=>reconcile().find(r=>r.key==='frontier') as CoordinationResult;
 const intakes=()=>db.prepare("SELECT qitem_id,body FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_valid(body) AND json_extract(body,'$.action')='resolve-exact-coordination-task-hold'").all() as Array<{qitem_id:string;body:string}>;
 const probe=(key:string)=>JSON.parse(intakes()[0]!.body)[key] as string;

 function configureAdminOnly(){const t:CoordinationTask={key:'capacity-inquiry',packageKey:'capacity-inquiry',owner:'reviewer@xv',action:'Answer',deadline:clock+20000,body:'capacity-inquiry',predecessors:[],admission:{generation:'reviewer-g1',configurationDigest:svc.configurationDigest('reviewer@xv')!,qualificationRef:'q',capacityRef:'c',effortRef:'e',validUntil:clock+60000},boundary:'owner-material'};repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','capacity-inquiry',{inputDigest:digest('capacity-inquiry'),destination:'reviewer@xv',bodyHash:digest('capacity-inquiry'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass:'administrative'});return svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,scopeSources:[BRIEF,ROADMAP],tasks:[t]});}
 function stabilize(){let r=frontierResult();for(let i=0;i<6&&r.state==='stabilizing';i++){advance(300001);r=frontierResult();}return r;}
 const dutyPacket=(q:string)=>JSON.parse(repo.getById(q)!.body);

 it('first-issue planning hold yields exactly one intake for the Lead planning duty',()=>{
  configureAdminOnly();
  block('lead@xv','lead-unknown');
  let r=stabilize();
  expect(r.state).toBe('held');
  expect(r.reason).toBe('lifecycle-recipient-protected');
  expect(r.queueId).toBeUndefined();
  expect(intakes()).toHaveLength(1);
  expect(probe('packageKey')).toBe('frontier-planning');
  expect(probe('taskOwner')).toBe('lead@xv');
  expect(r.subject!.identity).toBeTruthy();
  // Repeated observation never stages a second accountable item.
  for(let i=0;i<4;i++){advance(60001);reconcile();}
  expect(intakes()).toHaveLength(1);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning'").get()).toEqual({n:0});
 });

 it('first-issue admission hold reports the real held state and yields exactly one intake',()=>{
  configureAdminOnly();
  const duty=stabilize();
  claimLead(duty.queueId!);
  svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:dutyPacket(duty.queueId!).frontierDigest,disposition:'plan-proposal',proposal:[{packageKey:'next-frontier',citations:[ROADMAP],resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}]});
  block('operator-agent@kernel','operator-unknown');
  const r=frontierResult();
  expect(r.state).toBe('held');
  expect(r.state).not.toBe('pending-native-frontier-admission');
  expect(r.reason).toBe('lifecycle-recipient-protected');
  expect(intakes()).toHaveLength(1);
  expect(probe('packageKey')).toBe('frontier-admission');
  expect(probe('taskOwner')).toBe('operator-agent@kernel');
  for(let i=0;i<3;i++){advance(60001);reconcile();}
  expect(intakes()).toHaveLength(1);
 });

 it('first-issue confirmation hold yields exactly one intake bound to that completion',()=>{
  configureAdminOnly();
  const duty=stabilize();
  claimLead(duty.queueId!);
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:dutyPacket(duty.queueId!).frontierDigest,disposition:'frontier-complete',mapping:[{ref:ROADMAP.ref,deferral:{reason:'Not yet addressed',authorizationRef:'owner/roadmap-2'}},{ref:BRIEF.ref,deferral:{reason:'Deferred by owner decision',authorizationRef:'owner/roadmap-3'}}]});
  block('operator-agent@kernel','operator-unknown');
  const r=frontierResult();
  expect(r.state).toBe('held');
  expect(r.state).not.toBe('pending-native-frontier-confirmation');
  expect(intakes()).toHaveLength(1);
  expect(probe('packageKey')).toBe('frontier-confirmation');
  expect(r.subject!.identity).toContain(recorded.completionDigest!);
  for(let i=0;i<3;i++){advance(60001);reconcile();}
  expect(intakes()).toHaveLength(1);
 });

 it('a blocked frontier disposition routes exactly one intake carrying its boundary provenance',()=>{
  configureAdminOnly();
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2-no-scope',scopeSources:[]});
  const duty=stabilize();
  claimLead(duty.queueId!);
  svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:dutyPacket(duty.queueId!).frontierDigest,disposition:'frontier-blocked',boundary:'scope-source-missing',unblockCondition:'genuine Operator binds the goal documents'});
  const r=frontierResult();
  expect(r).toMatchObject({state:'held',reason:'frontier-boundary-blocked'});
  expect(r.activityEvidence).toMatchObject({boundary:'scope-source-missing',unblockCondition:'genuine Operator binds the goal documents'});
  expect(intakes()).toHaveLength(1);
  expect(probe('packageKey')).toBe('frontier-planning');
  expect(db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='frontier-boundary-intake'").get()!.receipt).toContain('scope-source-missing');
  for(let i=0;i<3;i++){advance(60001);reconcile();}
  expect(intakes()).toHaveLength(1);
 });

 it('an Operator proposal refusal routes exactly one intake carrying the decline reason',()=>{
  configureAdminOnly();
  const duty=stabilize();
  claimLead(duty.queueId!);
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:dutyPacket(duty.queueId!).frontierDigest,disposition:'plan-proposal',proposal:[{packageKey:'next-frontier',citations:[ROADMAP],resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}]});
  const admission=frontierResult();
  claimOperator(admission.queueId!);
  svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,declined:{reason:'Candidate duplicates an existing roadmap item'}});
  delivered();
  const r=frontierResult();
  expect(r).toMatchObject({state:'held',reason:'frontier-boundary-declined'});
  expect(r.activityEvidence).toMatchObject({disposition:'frontier-admission-declined',declineReason:'Candidate duplicates an existing roadmap item'});
  expect(r.subject!.identity).toBe(recorded.proposal!.proposalDigest);
  expect(intakes()).toHaveLength(1);
  expect(probe('packageKey')).toBe('frontier-admission');
  expect(db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='frontier-boundary-intake'").get()!.receipt).toContain('Candidate duplicates an existing roadmap item');
  for(let i=0;i<3;i++){advance(60001);reconcile();}
  expect(intakes()).toHaveLength(1);
 });
});