import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';
import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {diagnoseSeatParked} from '../src/domain/parked-query.js';
import {makeParkedOwnerConsumerPolicy, RESERVE_PREFIX, CLOSE_PREFIX} from '../src/domain/policies/parked-owner-consumer.js';
import type {PolicyJob} from '../src/domain/policies/types.js';
import {runStuckSweep} from '../src/domain/queue-stuck-sweep.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

describe('actionable obligation wake revisions',()=>{

 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,outbox:OutboxHandler,clock:number,samples:Map<string,CoordinationActivity>;
 const sessions=['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv','operator-agent@kernel'];
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 const plan=(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,...more,tasks});
 function sample(session:string):CoordinationActivity{const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
 function refresh(){for(const s of sessions)samples.set(s,sample(s));}
 function admit(tasks:CoordinationTask[]){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});}
 function configure(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}){admit(tasks);return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,more));}
 function nativeClaim(qitemId:string,owner:string){return repo.claim({qitemId,destinationSession:owner,actorGeneration:repo.coordinatorAuthority.generation(owner)!,identityProvenance:'transport:v1'});}
 function normal(){return [task('product'),task('repair','architect@xv',{recoveryFor:'product'})];}
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const notice=(id:string)=>db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+id) as any;
 beforeEach(async()=>{
  vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'shared-repair-'));db=createDb(join(dir,'db'));seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});outbox=new OutboxHandler(db);repo.attachOutbox(outbox);
  for(const s of sessions)db.prepare('INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES (?,?,?,?)').run('binding-'+s,s,s,'%1');
  repo.attachTransport({send:async()=>({ok:true,verified:true})});
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});
 async function finishTyped(packageKey:string,owner:string,queueId:string,returnId:string,state:'done'|'failed'='done'){
  if(repo.getById(queueId)!.state==='pending')nativeClaim(queueId,owner);
  repo.update({qitemId:queueId,actorSession:owner,actorGeneration:repo.coordinatorAuthority.generation(owner)!,identityProvenance:'transport:v1',state,...(state==='done'?{closureReason:'no-follow-on' as const}:{})});
  await repo.create({qitemId:returnId,sourceSession:owner,destinationSession:'lead@xv',body:JSON.stringify({packageKey,inputDigest:digest(packageKey),evidence:[{kind:'report',ref:'actual/'+packageKey+'.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose(owner,repo.coordinatorAuthority.generation(owner)!,'xv',packageKey,returnId);
  await svc.deliverCommitted();
 }

 async function assignment(verified=false){configure(normal());const first=reconcile().find(r=>r.key==='product')!;expect(first.state).toBe('pending-pickup');repo.attachTransport({send:async()=>({ok:true,verified})});await svc.deliverCommitted();return first.queueId!;}
 function outcome(id:string){return db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='assignment-wake-outcome'").get('assignment-wake-outcome:'+id) as {receipt:string}|undefined;}
 let activity='idle-at-prompt';
 function diagnosis(){return diagnoseSeatParked({getSeatState:()=>({...sample('lead@xv').state,activity} as any),listOpenObligations:(destinationSession,limit)=>({rows:repo.list({destinationSession,state:['pending','in-progress','blocked'],limit}).map(r=>({qitemId:r.qitemId,state:r.state as 'pending'|'in-progress'|'blocked',summary:r.summary})),limit}),isStandingAuthorityMarker:id=>repo.isStandingAuthorityMarker(id),getParkWake:id=>repo.getParkWakeStatus(id)}, {seatNodeId:'lead@xv',sessionName:'lead@xv'});}
 function policy(recoveryOwnsWake:()=>boolean=()=>false){return makeParkedOwnerConsumerPolicy({diagnoseRig:()=>({seats:[diagnosis() as any]}),history:{listForJob:()=>[],countForJob:()=>0},rows:{listTransitions:id=>repo.listTransitions(id),appendNote:(id,note)=>{repo.update({qitemId:id,actorSession:'watchdog@system',transitionNote:note});return {ok:true};},recordNudgeResult:(id,result)=>repo.recordNudgeAttempt(id,result),listOpenIds:dest=>repo.list({destinationSession:dest,state:['pending','in-progress','blocked'],limit:500}).map(r=>r.qitemId),semanticRevision:id=>repo.parkedObligationRevision(id),ordinaryWorkActionable:id=>repo.ordinaryWorkActionable(id),recoveryOwnsWake}});}
 const job={jobId:'parked-test',target:{session:'parked-owner-consumer@xv'}} as PolicyJob;
 async function work(id='real-work'){await repo.create({qitemId:id,sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'bounded actual work',nudge:false});return id;}
 beforeEach(()=>{activity='idle-at-prompt';});
 it('exact canonical baton stays visible but cannot park/wake or generate ordinary sweep findings',async()=>{
  db.prepare('UPDATE queue_items SET closure_required_at=? WHERE qitem_id=?').run(new Date(clock-600000).toISOString(),'baton');
  const before=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get('baton');
  expect(diagnosis()).toMatchObject({parked:false,obligations:{openCount:0,authority:[{qitemId:'baton'}]}});
  expect((await policy().evaluate(job)).action).toBe('skip');
  const sweep=await runStuckSweep({db,queueRepo:repo,now:new Date(clock),log:()=>{},resolveOrchestrator:()=>null});expect(sweep.findings.some(r=>r.qitemId==='baton')).toBe(false);expect(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get('baton')).toEqual(before);
 });
 it.each(['lookalike','expired','wrong-generation','wrong-owner','reconciling'] as const)('keeps %s ordinary/authority recovery actionable',async kind=>{
  const id=kind==='lookalike'?await work('baton-lookalike'):'baton';
  if(kind==='expired')db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock-1);
  if(kind==='wrong-generation')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='old' WHERE qitem_id='baton'").run();
  if(kind==='wrong-owner')db.prepare("UPDATE queue_items SET destination_session='peer@xv' WHERE qitem_id='baton'").run();
  if(kind==='reconciling')db.prepare("UPDATE coordinator_authority SET state='reconciling'").run();
  expect(repo.ordinaryWorkActionable(id)).toBe(true);if(kind!=='wrong-owner')expect(diagnosis().parked).toBe(true);
 });
 it('real ready work wakes once across activity/prompt churn, state-preserving notes, nudges and restart',async()=>{
  const id=await work();const p=policy();expect((await p.evaluate(job)).action).toBe('send');
  const revision=repo.parkedObligationRevision(id);activity='active';expect((await p.evaluate(job)).action).toBe('skip');activity='idle-at-prompt';
  repo.recordNudgeAttempt(id,'failed: diagnostic');repo.update({qitemId:id,actorSession:'watchdog@system',transitionNote:'diagnostic activity observed'});
  expect(repo.parkedObligationRevision(id)).toBe(revision);expect((await policy().evaluate(job)).action).toBe('skip');
  expect(repo.listTransitions(id).filter(t=>t.transitionNote?.startsWith(RESERVE_PREFIX))).toHaveLength(1);expect(repo.listTransitions(id).some(t=>t.transitionNote?.startsWith(CLOSE_PREFIX))).toBe(false);
 });
 it.each(['obligation','custody','blocker'] as const)('genuine %s change rearms exactly once',async kind=>{
  const id=await work();if(kind==='blocker'){await work('blocker');db.prepare("UPDATE queue_items SET blocked_on='blocker',state='blocked' WHERE qitem_id=?").run(id);}
  expect((await policy().evaluate(job)).action).toBe('send');
  if(kind==='obligation')await work('new-obligation');
  if(kind==='custody')nativeClaim(id,'lead@xv');
  if(kind==='blocker')db.prepare("UPDATE queue_items SET destination_session='builder@xv',state='done' WHERE qitem_id='blocker'").run();
  expect((await policy().evaluate(job)).action).toBe('send');expect((await policy().evaluate(job)).action).toBe('skip');
 });
 it('legacy activity-closed receipt is baselined without resending; a later real custody change still wakes',async()=>{
  const id=await work();const {createHash}=await import('node:crypto');const hash=createHash('sha256').update(id).digest('hex').slice(0,16);const key='lead@xv|'+hash+'#4';repo.update({qitemId:id,actorSession:'watchdog@system',transitionNote:RESERVE_PREFIX+' '+key+'; obligations '+id});repo.update({qitemId:id,actorSession:'watchdog@system',transitionNote:CLOSE_PREFIX+' '+key+' (seat resumed)'});
  expect((await policy().evaluate(job)).action).toBe('skip');expect((await policy().evaluate(job)).action).toBe('skip');nativeClaim(id,'lead@xv');expect((await policy().evaluate(job)).action).toBe('send');
 });
 it('current recovery owner and UNKNOWN notice retain ownership and bytes',async()=>{
  const id=await work();db.prepare("INSERT INTO outbox_entries(outbox_id,ts_dispatched,sender_session,destination_session,body,delivery_state) VALUES ('unknown',?,'sender','lead@xv','preserve','indeterminate')").run(new Date(clock).toISOString());const before=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='unknown'").get();
  expect((await policy(()=>true).evaluate(job)).action).toBe('skip');expect(repo.listTransitions(id).some(t=>t.transitionNote?.startsWith(RESERVE_PREFIX))).toBe(false);expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='unknown'").get()).toEqual(before);
 });
});
