import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationResult,type CoordinationTask} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

/**
 * SYSTEM-WAKE-RECONCILIATION-GAP.json, observed class:
 * cross-rig system-origin administrative notices whose guarded wake sits
 * `indeterminate` forever. The underlying queue item is genuine administrative
 * work with a typed rig origin, but no supported outcome-only receipt class
 * exists for it, so the Operator seat keeps uncontained effect debt and the
 * frontier admission duty can never be issued.
 *
 * The first case states the end state and therefore fails on bde39c02. Every
 * other case states protections that must survive whatever closes the gap.
 */
const ROADMAP={ref:'/app-handy/ROADMAP.md',digest:'ba4006b7e320091a18d13af564a4c1e853ca0eddff9c461b17fa23035d126441'};
const BRIEF={ref:'/app-handy/PRODUCT_BRIEF.md',digest:'b7b12dd8e79333976c0cc94578b18eed7fca40035f08a9e11be2b3cf1931f9b5'};

describe('system-origin administrative wake outcome convergence',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'system-wake-'));db=createDb(join(dir,'db'));seed(db);
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
 /** Genuine transport delivery of everything actually deliverable. The one
   *  notice this class is about is made indeterminate on purpose and never appears here. */
 function deliverPendingTransport(){repo.attachTransport({send:async(_s:string,_b:string,opts?:any)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,_s,opts?.queueAssignmentId);return {ok:true,verified:true};}});return repo.drainPendingWakeIntents();}
 const claimLead=(q:string)=>repo.claim({qitemId:q,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
 const claimOperator=(q:string)=>repo.claim({qitemId:q,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const frontierResult=()=>reconcile().find(r=>r.key==='frontier') as CoordinationResult;
 const dutyPacket=(q:string)=>JSON.parse(repo.getById(q)!.body);
 const wakeState=(qitem:string)=>(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitem) as {delivery_state:string}|undefined)?.delivery_state;
 const outboxRows=()=>db.prepare('SELECT outbox_id,delivery_state,sender_session,destination_session,audit_pointer FROM outbox_entries').all() as Array<{outbox_id:string;delivery_state:string;sender_session:string;destination_session:string;audit_pointer:string}>;

 function configureAdminOnly(){const t:CoordinationTask={key:'capacity-inquiry',packageKey:'capacity-inquiry',owner:'reviewer@xv',action:'Answer',deadline:clock+20000,body:'capacity-inquiry',predecessors:[],admission:{generation:'reviewer-g1',configurationDigest:svc.configurationDigest('reviewer@xv')!,qualificationRef:'q',capacityRef:'c',effortRef:'e',validUntil:clock+60000},boundary:'owner-material'};repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','capacity-inquiry',{inputDigest:digest('capacity-inquiry'),destination:'reviewer@xv',bodyHash:digest('capacity-inquiry'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass:'administrative'});return svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,scopeSources:[BRIEF,ROADMAP],tasks:[t]});}
 function stabilize(){let r=frontierResult();for(let i=0;i<6&&r.state==='stabilizing';i++){advance(300001);r=frontierResult();}return r;}

 /** A genuine system-origin administrative item with a typed rig origin, and the
  *  guarded wake the runtime staged for it that then went indeterminate. */
 function stageSystemAdministrativeEffect(reasons=['rig-plan-absent'],rigId='xv'){
  // A genuine runtime rollout item: its id and rolloutKey are recomputable, so the
  // provenance is verified rather than trusted from the body action alone.
  const recipientGeneration='operator-agent-g1';
  const rolloutKey=digest(rigId+':'+recipientGeneration+':'+reasons.join('|'));
  const previousQueueId=null;
  const qitemId='qitem-resilience-rollout-'+digest(rolloutKey+':'+(previousQueueId??'initial')).slice(0,24);
  db.transaction(()=>repo.createWithinTransaction({qitemId,sourceSession:'watchdog@system',destinationSession:'operator-agent@kernel',expiresAt:new Date(clock+1200000).toISOString(),body:JSON.stringify({action:'materialize-standard-resilience',rolloutKey,previousQueueId,rigId,rigName:rigId,policyRef:'resilience-rollout',reasons,recipientGeneration,deadline:clock+1200000,grantsAuthority:false,required:'Genuine native custody only.'}),identityProvenance:'system:operator-authorized-coordination',nudge:false}))();
  repo.stageWakeIntent(qitemId,'watchdog@system','operator-agent@kernel','system:operator-authorized-coordination',true,recipientGeneration);
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+qitemId);
  return qitemId;
 }


 async function stageDiagnostic(legacy=false){
  configureAdminOnly();const body='original diagnostic target';
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','diagnostic-target',{inputDigest:digest('original'),destination:'builder@xv',bodyHash:digest(body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  await repo.create({qitemId:'diagnostic-target',sourceSession:'lead@xv',destinationSession:'builder@xv',body,dispatch:{token,packageKey:'diagnostic-target'},nudge:false});
  repo.recordNudgeAttempt('diagnostic-target','failed:transport unavailable');
  const {runStuckSweep}=await import('../src/domain/queue-stuck-sweep.js');
  await runStuckSweep({db,queueRepo:repo,now:new Date(clock),log:()=>{},resolveOrchestrator:()=>null});
  const row=db.prepare("SELECT qitem_id,body FROM queue_items WHERE qitem_id LIKE 'qitem-stuck-sweep-control-%' LIMIT 1").get() as any;
  expect(row).toBeTruthy();const qid=row.qitem_id;
  if(legacy){db.prepare("DELETE FROM coordinator_operations WHERE operation_id=?").run('diagnostic-wake-producer:'+qid);const b=JSON.parse(row.body);delete b.rigId;db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run(JSON.stringify(b),qid);}
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+qid);
  return qid;
 }
 it('contains real producer-bound completed diagnostic wakes without changing UNKNOWN delivery',async()=>{
  const qid=await stageDiagnostic();claimOperator(qid);
  const notice=()=>db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid);
  const before=notice();reconcile();expect(svc.noticeOutcomeContained('xv',notice())).toBe(false);
  repo.update({qitemId:qid,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  reconcile();expect(svc.noticeOutcomeContained('xv',notice())).toBe(true);expect(notice()).toEqual(before);
  db.prepare("DELETE FROM coordinator_operations WHERE operation_id=?").run('diagnostic-wake-producer:'+qid);expect(svc.noticeOutcomeContained('xv',notice())).toBe(false);
 });
 it('requires explicit native exact-snapshot disposition for older diagnostic wakes and rejects drift',async()=>{
  const qid=await stageDiagnostic(true),notice=()=>db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid) as any;
  claimOperator(qid);repo.update({qitemId:qid,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  const before=notice();reconcile();expect(svc.noticeOutcomeContained('xv',notice())).toBe(false);
  const input={rigId:'xv',outboxId:'wake-intent-'+qid,noticeSnapshotHash:digest(JSON.stringify(before)),taskBodyHash:digest(repo.getById(qid)!.body),evidenceRef:'native-diagnostic-terminal.json'};
  expect(()=>svc.disposeDiagnosticWake('lead@xv','lead-g1',input)).toThrow();
  expect(()=>svc.disposeDiagnosticWake('operator-agent@kernel','operator-agent-g1',{...input,noticeSnapshotHash:'drift'})).toThrow();
  svc.disposeDiagnosticWake('operator-agent@kernel','operator-agent-g1',input);expect(svc.noticeOutcomeContained('xv',notice())).toBe(true);expect(notice()).toEqual(before);
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='diagnostic-wake-producer'").get()).toBeUndefined();
  db.prepare("UPDATE queue_items SET body=body||' ' WHERE qitem_id=?").run(qid);expect(svc.noticeOutcomeContained('xv',notice())).toBe(false);
 });
 it('reproduces the deadlock: a genuinely finished system-origin item still blocks the Operator frontier duty forever',async()=>{
  configureAdminOnly();
  const qitemId=stageSystemAdministrativeEffect();
  expect(wakeState(qitemId)).toBe('indeterminate');
  const duty=stabilize();
  claimLead(duty.queueId!);
  svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:dutyPacket(duty.queueId!).frontierDigest,disposition:'plan-proposal',proposal:[{packageKey:'next-frontier',citations:[ROADMAP],resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}]});

  // The Operator seat is now blocked by the unresolved wake.
  const blocked=frontierResult();
  expect(blocked).toMatchObject({state:'held',reason:'lifecycle-recipient-protected'});

  // The administrative item itself finishes, with exact native claim and provenance.
  claimOperator(qitemId);
  repo.update({qitemId,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(repo.getById(qitemId)!.state).toBe('done');
  expect(wakeState(qitemId)).toBe('indeterminate');

  // OBSERVED GAP, closed: no acknowledgment, lifecycle-control or direct-effect receipt
  // class existed for this notice, and this path creates no product authority.
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='outbox-recipient-duty'").get()).toEqual({n:0});
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='held-history-control-outcome'").get()).toEqual({n:0});
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-accept'").get()).toEqual({n:0});

  // An ordinary reconcile pass is how the registered observer reaches this recording,
  // both before and after a plan exists.
  advance(60001);reconcile();

  // Ordinary transport resolves every deliverable notice. The rollout notice is not one
  // of them: it stays UNKNOWN and byte-identical through the whole case.
  await deliverPendingTransport();
  expect(wakeState(qitemId)).toBe('indeterminate');
  const rowBefore=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitemId);

  // Exactly one outcome-only receipt, written by the ordinary observer pass, bound to the
  // completed native work. It records an outcome; it never acknowledges delivery.
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='system-wake-outcome'").get()).toEqual({n:1});
  const receipt=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='system-wake-outcome'").get() as {receipt:string}).receipt);
  expect(receipt).toMatchObject({outboxId:'wake-intent-'+qitemId,taskQueueId:qitemId,action:'materialize-standard-resilience',rigId:'xv',recipient:'operator-agent@kernel',recipientGeneration:'operator-agent-g1',taskStateAtRecord:'done',deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,nonExecutable:true,grantsAuthority:false});
  expect(receipt.noticeSnapshotHash).toBe(digest(JSON.stringify(rowBefore)));
  expect(receipt.taskBodyHash).toBe(digest(repo.getById(qitemId)!.body));
  expect(receipt.claimTransitionId).toBeGreaterThan(0);
  expect(svc.noticeOutcomeContained('xv',rowBefore)).toBe(true);
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitemId)).toEqual(rowBefore);
  expect(wakeState(qitemId)).toBe('indeterminate');

  // End state the gap blocked: the genuine Operator is admitted to the frontier duty.
  const issued=frontierResult();
  expect(issued).toMatchObject({state:'pending-native-frontier-admission'});
  const admissionId=issued.queueId!;
  expect(svc.lifecycleControlReceipt(admissionId)).toMatchObject({kind:'frontier-admission',recipient:'operator-agent@kernel',recipientGeneration:'operator-agent-g1'});

  // Continued admission through the supported public path: the genuine Operator claims
  // the exact issued duty under its own current native identity and admits the proposal.
  const before2=db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='system-wake-outcome'").get();
  expect(()=>claimOperator(duty.queueId!)).toThrow();
  claimOperator(admissionId);
  expect(repo.getById(admissionId)).toMatchObject({state:'in-progress'});
  expect((db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(admissionId) as {claimed_by_generation_uuid:string}).claimed_by_generation_uuid).toBe('operator-agent-g1');
  const planReceipt=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get() as {receipt:string}).receipt);
  svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admissionId,proposalDigest:digest(JSON.stringify(planReceipt.proposal)),admitted:[{packageKey:'next-frontier',contract:{inputDigest:digest('next-frontier'),destination:'builder@xv',bodyHash:digest('next-frontier'),resources:[],workClass:'product',scopeCitations:[ROADMAP],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}}]});
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE rig_id='xv' AND package_key='next-frontier'").get()).toBeTruthy();
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='system-wake-outcome'").get()).toEqual(before2);
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitemId)).toEqual(rowBefore);
  expect(wakeState(qitemId)).toBe('indeterminate');
 });

 it('a claimed but unresolved task-hold intake contains its wake, which unblocks the admission it announces',()=>{
  configureAdminOnly();
  const qitemId=stageSystemAdministrativeEffect();
  claimOperator(qitemId);
  repo.update({qitemId,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  advance(60001);reconcile();
  (svc as any).stageTaskHoldIntake('xv',repo.coordinatorAuthority.get('xv'),svc.plan('xv'),{key:'capacity-inquiry',state:'held',reason:'lifecycle-recipient-protected',queueId:null},[]);
  // The hold this very boundary produces is addressed to the same Operator seat.
  const hold=db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.action')='resolve-exact-coordination-task-hold' ORDER BY rowid DESC LIMIT 1").get() as {qitem_id:string}|undefined;
  expect(hold).toBeTruthy();
  const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+hold!.qitem_id) as any;
  // Claim only: the intake is owned but unresolved. Claim is the supported way to own it,
  // so this must already be contained; a terminal-only rule would deadlock here.
  expect(row.delivery_state).toBe('pending');
  expect(svc.noticeOutcomeContained('xv',row)).toBe(false);
  claimOperator(hold!.qitem_id);
  expect(repo.getById(hold!.qitem_id)!.state).toBe('in-progress');
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+hold!.qitem_id);
  reconcile();
  expect(svc.noticeOutcomeContained('xv',db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get(row.outbox_id))).toBe(true);
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+hold!.qitem_id)).toEqual({delivery_state:'indeterminate'});
  const receipt=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='system-wake-outcome' AND operation_id=?").get('system-wake-outcome:wake-intent-'+hold!.qitem_id) as {receipt:string}).receipt);
  expect(receipt).toMatchObject({action:'resolve-exact-coordination-task-hold',taskQueueId:hold!.qitem_id,taskStateAtRecord:'in-progress',deliveryConclusion:'unknown',outcomeOnly:true,grantsAuthority:false});
  // Admission is no longer held by this class of notice.
  expect(frontierResult().state).not.toBe('held');
  // The rollout notice is still contained from the earlier pass, and unchanged.
  expect(svc.noticeOutcomeContained('xv',db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitemId))).toBe(true);
 });

 it('a pending or sending notice of this class is never contained, even with genuine custody',()=>{
  configureAdminOnly();
  const qitemId=stageSystemAdministrativeEffect();
  claimOperator(qitemId);
  repo.update({qitemId,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  for(const state of ['pending','sending'] as const){
   db.prepare("UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?").run(state,'wake-intent-'+qitemId);
   const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitemId) as any;
   expect(svc.noticeOutcomeContained('xv',row)).toBe(false);
   expect((svc as any).workerEffectDebt('operator-agent@kernel')).toBe(true);
  }
 });

 it.each(['recover-unavailable-coordinator','recover-expired-idle-transfer','restore-current-held-history-binding','reconcile-current-coordinator-lease'])('records native custody of genuine %s recovery notices without granting authority',action=>{
  configureAdminOnly();
  const qid=(svc as any).stageCoordinatorRecovery('xv',1,'operator-agent-g1',action,'current-holder-acknowledgment-or-lease',clock+60000);
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+qid);
  const before=db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid);
  const authorityBefore=db.prepare('SELECT * FROM coordinator_authority').all();
  reconcile();
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id=?").get('system-wake-outcome:wake-intent-'+qid)).toBeUndefined();
  const originalBody=repo.getById(qid)!.body;
  db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run(JSON.stringify({...JSON.parse(originalBody),recoveryKey:'forged'}),qid);
  expect((svc as any).systemTaskProof(qid,'operator-agent@kernel')).toBeNull();
  db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run(originalBody,qid);
  claimOperator(qid);reconcile();
  const wake=db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid) as any;
  expect(svc.noticeOutcomeContained('xv',wake)).toBe(true);
  expect(wake).toEqual(before);
  expect(db.prepare('SELECT * FROM coordinator_authority').all()).toEqual(authorityBefore);
  const receipt=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='system-wake-outcome' AND operation_id=?").get('system-wake-outcome:wake-intent-'+qid) as any;
  expect(JSON.parse(receipt.receipt)).toMatchObject({action,outcomeOnly:true,grantsAuthority:false,taskStateAtRecord:'in-progress'});
  const body=JSON.parse(repo.getById(qid)!.body);
  db.prepare('UPDATE queue_items SET body=? WHERE qitem_id=?').run(JSON.stringify({...body,reason:'forged-reason'}),qid);
  expect(svc.noticeOutcomeContained('xv',wake)).toBe(false);
 });

 it('contains completed transfer pointer custody only with exact native epoch acknowledgment, even after lease expiry',()=>{
  configureAdminOnly();
  repo.coordinatorAuthority.transfer('lead@xv','lead-g1',{expected:token,oldOwner:'lead@xv',recipient:'peer@xv',recipientGeneration:'peer-g1',operationId:'sw-transfer',leaseMs:60000});
  const qid='qitem-coordination-peer-'+digest('xv:2').slice(0,24);
  db.transaction(()=>repo.createWithinTransaction({qitemId:qid,sourceSession:'watchdog@system',destinationSession:'peer@xv',body:JSON.stringify({action:'reconcile-transferred-baton',rigId:'xv',epoch:2,batonId:'baton',deadline:clock+60000,recipientGeneration:'peer-g1'}),identityProvenance:'system:operator-authorized-coordination',nudge:false}))();
  repo.stageWakeIntent(qid,'watchdog@system','peer@xv','system:operator-authorized-coordination',true,'peer-g1');
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+qid);
  const before=db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid) as any;
  repo.claim({qitemId:qid,destinationSession:'peer@xv',actorGeneration:'peer-g1',identityProvenance:'transport:v1'});
  const record=()=>{(svc as any).recordSystemWakeOutcomes('xv');return svc.noticeOutcomeContained('xv',db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid));};
  expect(record()).toBe(false);
  repo.coordinatorAuthority.acknowledge('peer@xv',{rigId:'xv',epoch:2,generation:'peer-g1'},{operationId:'sw-peer-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  expect(record()).toBe(false); // ACK alone does not close the intake.
  repo.update({qitemId:qid,actorSession:'peer@xv',actorGeneration:'peer-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  expect(record()).toBe(true);
  expect(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+qid)).toEqual(before);
  db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock-1);
  expect(record()).toBe(true); // Historical consumption, not lease renewal.
  expect(repo.coordinatorAuthority.get('xv')!.lease_until).toBe(clock-1);
  db.prepare("DELETE FROM coordinator_operations WHERE operation_id='sw-peer-ack'").run();
  expect(record()).toBe(false);
 });

 it('a pre-plan rollout item is recorded through the registered Operator observer path',async()=>{
  // No plan and no holder: rollout items exist precisely when a rig has neither.
  expect(svc.plan('xv')).toBeNull();
  const qitemId=stageSystemAdministrativeEffect();
  claimOperator(qitemId);
  repo.update({qitemId,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'blocked',note:'waiting on an admitted dependency'});
  db.prepare("INSERT OR REPLACE INTO watchdog_jobs(job_id,policy,state,target_session,registered_by_session,registered_by_generation_uuid,interval_seconds,spec_yaml,registered_at) VALUES ('sw-job','coordinator-continuity','active','operator-agent@kernel','operator-agent@kernel','operator-agent-g1',60,'context: {}','2026-10-05T12:00:00Z')").run();
  // The genuine watchdog policy entry, unmodified: evaluate() calls supervise() before
  // resumeAdministrativeDuties, and nothing in it requires a plan or a holder first.
  const {makeCoordinatorContinuityPolicy}=await import('../src/domain/policies/coordinator-continuity.js');
  const outcome=await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'sw-job',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  expect(outcome).toBeDefined();
  const receipt=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='system-wake-outcome' AND operation_id=?").get('system-wake-outcome:wake-intent-'+qitemId) as {receipt:string}|undefined;
  expect(receipt).toBeTruthy();
  expect(JSON.parse(receipt!.receipt)).toMatchObject({rigId:'xv',action:'materialize-standard-resilience',taskStateAtRecord:'blocked',recipientGeneration:'operator-agent-g1',outcomeOnly:true,grantsAuthority:false});
  expect(svc.plan('xv')).toBeNull();
  expect((db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qitemId) as {delivery_state:string}).delivery_state).toBe('indeterminate');
 });

 it('refuses arbitrary and spoofed wakes: only the exact staged system wake is a real effect',()=>{
  configureAdminOnly();
  const qitemId=stageSystemAdministrativeEffect();
  claimOperator(qitemId);reconcile();
  const wake=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get("wake-intent-"+qitemId) as any;
  expect(svc.noticeOutcomeContained('xv',wake)).toBe(true);
  expect(wake).toMatchObject({outbox_id:'wake-intent-'+qitemId,sender_session:'watchdog@system',destination_session:'operator-agent@kernel',delivery_state:'indeterminate'});
  expect(svc.noticeOutcomeContained('xv',{...wake,sender_session:'lead@xv'})).toBe(false);
  expect(svc.noticeOutcomeContained('xv',{...wake,audit_pointer:'qitem-other'})).toBe(false);
  expect(svc.noticeOutcomeContained('xv',{...wake,delivery_state:'pending'})).toBe(false);
  expect(svc.noticeOutcomeContained('xv',{...wake,delivery_state:'sending'})).toBe(false);
  expect(svc.noticeOutcomeContained('xv',{...wake,delivery_state:'delivered'})).toBe(false);
  expect(svc.noticeOutcomeContained('xv',{...wake,body:wake.body+' injected action'})).toBe(false);
  expect(svc.noticeOutcomeContained('other-rig',wake)).toBe(true); // Uses the pointed-to task rig and its own receipt.
  // Quarantined look-alikes are not contained either.
  db.prepare("INSERT INTO outbox_historical_quarantines VALUES (?,?,?,?,?,?,?,?)").run(wake.outbox_id,"xv","hash","op","auth",clock+60000,"held",new Date(clock).toISOString());
  expect(svc.noticeOutcomeContained('xv',wake)).toBe(false);
 });

 it('repeated reconciliation is idempotent and never manufactures a receipt',()=>{
  configureAdminOnly();
  const qitemId=stageSystemAdministrativeEffect();
  claimOperator(qitemId);
  repo.update({qitemId,actorSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  const receipts=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind IN ('held-history-control-outcome','outbox-recipient-duty','coordination-accept')").get();
  const before=receipts();
  for(let i=0;i<5;i++){advance(60001);reconcile();}
  expect(receipts()).toEqual(before);
  expect(wakeState(qitemId)).toBe('indeterminate');
 });
 it.each(['generation','released-claim','task-body','notice-bytes','creation-provenance','claim-provenance','plan-generation','unclaimed'])('receipt guard remains debt after %s drift',kind=>{
  configureAdminOnly();const qid=stageSystemAdministrativeEffect();claimOperator(qid);reconcile();
  const row=()=>db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qid) as any;
  expect(svc.noticeOutcomeContained('xv',row())).toBe(true);
  if(kind==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='rotated' WHERE generation_uuid='operator-agent-g1'").run();
  if(kind==='released-claim')db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL WHERE qitem_id=?").run(qid);
  if(kind==='task-body')db.prepare("UPDATE queue_items SET body=json_set(body,'$.required','changed') WHERE qitem_id=?").run(qid);
  if(kind==='notice-bytes')db.prepare("UPDATE outbox_entries SET body=body||'changed' WHERE outbox_id=?").run('wake-intent-'+qid);
  if(kind==='creation-provenance')db.prepare("UPDATE queue_transitions SET identity_provenance='transport:v1' WHERE qitem_id=? AND transition_note='created'").run(qid);
  if(kind==='claim-provenance')db.prepare("UPDATE queue_transitions SET identity_provenance='system:other' WHERE qitem_id=? AND transition_note='claimed'").run(qid);
  if(kind==='plan-generation')db.prepare("UPDATE coordinator_operations SET receipt=json_set(receipt,'$.operatorGeneration','stale') WHERE kind='coordination-plan'").run();
  if(kind==='unclaimed')db.prepare("DELETE FROM queue_transitions WHERE qitem_id=? AND transition_note='claimed'").run(qid);
  expect(svc.noticeOutcomeContained('xv',row())).toBe(false);
  expect((svc as any).workerEffectDebt('operator-agent@kernel')).toBe(true);
  expect(row().delivery_state).toBe('indeterminate');
 });

 it('records an older rig notice despite over 2000 newer unrelated rows',()=>{
  configureAdminOnly();const qid=stageSystemAdministrativeEffect();claimOperator(qid);
  const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+qid) as any;
  const cols=Object.keys(row);const insert=db.prepare('INSERT INTO outbox_entries ('+cols.join(',')+') VALUES ('+cols.map(()=>'?').join(',')+')');
  db.transaction(()=>{for(let i=0;i<2001;i++){const copy={...row,outbox_id:'unrelated-'+i,audit_pointer:null};insert.run(...cols.map(c=>copy[c]));}})();
  reconcile();expect(svc.noticeOutcomeContained('xv',row)).toBe(true);
  const before=db.prepare("SELECT * FROM coordinator_operations WHERE kind='system-wake-outcome'").all();reconcile();
  expect(db.prepare("SELECT * FROM coordinator_operations WHERE kind='system-wake-outcome'").all()).toEqual(before);
 });
 it('contains a genuine claimed renewal intake but refuses a spoofed chain',()=>{
  configureAdminOnly();const held={key:'capacity-inquiry',state:'held',reason:'lifecycle-recipient-protected',queueId:null};
  const stage=()=> (svc as any).stageTaskHoldIntake('xv',repo.coordinatorAuthority.get('xv'),svc.plan('xv'),held,[]);
  stage();const root=(db.prepare("SELECT qitem_id FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.action')='resolve-exact-coordination-task-hold'").get() as any).qitem_id;
  advance(1200001);stage();
  const next=(db.prepare("SELECT qitem_id FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.rootQueueId')=?").get(root) as any).qitem_id;
  claimOperator(next);db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+next);reconcile();
  const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get('wake-intent-'+next) as any;
  expect(svc.noticeOutcomeContained('xv',row)).toBe(true);
  db.prepare("UPDATE queue_items SET body=json_set(body,'$.rootQueueId','spoof') WHERE qitem_id=?").run(next);
  expect(svc.noticeOutcomeContained('xv',row)).toBe(false);
 });

 it('both shared Operator debt gates consume a foreign rig receipt and preserve unclaimed debt',()=>{
  configureAdminOnly();const qid=stageSystemAdministrativeEffect();claimOperator(qid);reconcile();
  const exclude=db.prepare("SELECT outbox_id FROM outbox_entries WHERE outbox_id!=?").all('wake-intent-'+qid).map((r:any)=>r.outbox_id);
  expect((svc as any).workerEffectDebt('operator-agent@kernel',exclude)).toBe(false);
  expect(repo.heldHistoryAuthoringDebtReady('kernel','operator-agent@kernel',exclude)).toBe(true);
  db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL WHERE qitem_id=?").run(qid);
  expect((svc as any).workerEffectDebt('operator-agent@kernel',exclude)).toBe(true);
  expect(repo.heldHistoryAuthoringDebtReady('kernel','operator-agent@kernel',exclude)).toBe(false);
 });

});
