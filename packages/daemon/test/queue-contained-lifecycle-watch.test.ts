import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import Database from 'better-sqlite3';
import {seed,token} from './helpers/coordinator-fixture.js';
import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {runStuckSweep} from '../src/domain/queue-stuck-sweep.js';
import {runWakeLadderTick} from '../src/domain/queue-wake-ladder.js';

describe('contained lifecycle control shared consumers',()=>{
 let db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,identityObservedAt:new Date(clock).toISOString(),state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();db=new Database(':memory:');seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});samples=new Map();for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;});
 afterEach(()=>{db.close();vi.useRealTimers();});
 function contained(){
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
  return {assessment,retirement,retirementSuccessor,contract};
 }
 function protectedBytes(ids:string[]){return {queues:ids.map(id=>db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(id)),outbox:ids.map(id=>db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+id)),authority:db.prepare('SELECT * FROM coordinator_authority').all(),evidence:db.prepare('SELECT * FROM queue_native_custody_evidence').all()};}
 it('shares exact positive proof at stage, sweep, ladder and bound wait without changing history',async()=>{
  const {assessment,retirement,retirementSuccessor,contract}=contained(),id=retirement.queueId;
  const ids=[assessment.queueId,id,retirementSuccessor.queueId];
  // Handoff eligibility is unrelated metadata; it makes the executing ladder a real consumer.
  db.prepare("UPDATE queue_items SET handed_off_from='fixture-predecessor',last_nudge_result='failed:fixture',last_nudge_attempt=? WHERE qitem_id=?").run(new Date(clock-600000).toISOString(),id);
  const before=protectedBytes(ids);expect(svc.dutyFacts(id)).toMatchObject({complete:true,expired:true,retired:true});expect(repo.genericWatchActionable(id)).toBe(false);
  const sweep=await runStuckSweep({db,queueRepo:repo,now:new Date(clock),unclaimedAgeMinutes:0,resolveOrchestrator:()=>null,log:()=>{}});expect(sweep.findings.some(f=>f.qitemId===id)).toBe(false);expect(sweep.refusals?.some(f=>f.qitemId===id)??false).toBe(false);
  const attempted:string[]=[];await runWakeLadderTick({db,queueRepo:repo,now:new Date(clock),attemptWake:async(q)=>{attempted.push(q);return 'failed:fixture';},log:()=>{}});expect(attempted).not.toContain(id);
  // The actual bound evaluator reads its generated-timer binding, never message prose.
  const wake=(repo as any).wakeRepo;const binding=vi.spyOn(wake,'findCurrentQitemsByGeneratedTimer').mockReturnValue([{qitemId:id,state:'pending'}]);
  expect(repo.evaluateWaitReminder({jobId:'exact-bound-timer'})).toMatchObject({action:'terminal',reason:'queue_control_contained'});binding.mockRestore();
  expect(protectedBytes(ids)).toEqual(before);
  const next=svc.stageQualificationAssessment('operator-agent@kernel','operator-agent-g1',{rigId:'xv',worker:'builder@xv',workerGeneration:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,deadline:clock+20000,contract});expect(next.queueId).not.toBe(assessment.queueId);
 });
 it.each(['unknown','claimed','active','body','target','generation','configuration','rig','incomplete'] as const)('keeps %s proof actionable and immutable',async kind=>{
  const {assessment,retirement}=contained(),id=retirement.queueId;
  if(kind==='unknown')db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+id);
  if(kind==='claimed')db.prepare("UPDATE queue_items SET claimed_at=? WHERE qitem_id=?").run(new Date(clock).toISOString(),id);
  if(kind==='active'){clock-=21002;vi.setSystemTime(clock);}
  if(kind==='body')db.prepare("UPDATE queue_items SET body=body||' drift' WHERE qitem_id=?").run(id);
  if(kind==='target')db.prepare("UPDATE queue_items SET body=body||' drift' WHERE qitem_id=?").run(assessment.queueId);
  if(kind==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='changed' WHERE node_id='builder@xv'").run();
  if(kind==='configuration')db.prepare("UPDATE nodes SET model='changed' WHERE id='builder@xv'").run();
  if(kind==='rig')db.prepare("UPDATE nodes SET rig_id='other' WHERE id='builder@xv'").run();
  if(kind==='incomplete')db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id=?").run(assessment.queueId);
  const before=protectedBytes([assessment.queueId,id]);expect(repo.genericWatchActionable(id)).toBe(true);expect(protectedBytes([assessment.queueId,id])).toEqual(before);expect(repo.genericWatchActionable('baton')).toBe(true);
 });
 it.each(['contained','unrelated','claimed','unknown'] as const)('ordinary admitted dispatch retains %s custody semantics',kind=>{
  const {retirement}=contained();
  if(kind==='claimed')db.prepare("UPDATE queue_items SET claimed_at=? WHERE qitem_id=?").run(new Date(clock).toISOString(),retirement.queueId);
  if(kind==='unknown')db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+retirement.queueId);
  if(kind==='unrelated')db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES ('unrelated',?,?,'sender','builder@xv','pending','preserve')").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const task={key:'product',packageKey:'product',owner:'builder@xv',action:'bounded product',deadline:clock+20000,body:'product',predecessors:[],admission:{generation:'builder-g1',configurationDigest:svc.configurationDigest('builder@xv')!,qualificationRef:'actual/qualified',capacityRef:'actual/capacity',effortRef:'actual/effort',validUntil:clock+60000}};
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','product',{inputDigest:digest('product'),destination:'builder@xv',bodyHash:digest('product'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});const repair={...task,key:'repair',packageKey:'repair',owner:'architect@xv',body:'repair',recoveryFor:'product',admission:{...task.admission,generation:'architect-g1',configurationDigest:svc.configurationDigest('architect@xv')!}};repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','repair',{inputDigest:digest('repair'),destination:'architect@xv',bodyHash:digest('repair'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});svc.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'product',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:false,tasks:[task,repair]});
  const before=protectedBytes([retirement.queueId]);const result=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='product')!;
  expect(result.state).toBe(kind==='contained'?'pending-pickup':'held');if(kind!=='contained')expect(result.reason).toBe(kind==='unknown'?'uncertain-worker-effect':'existing-worker-custody');expect(protectedBytes([retirement.queueId]).queues).toEqual(before.queues);expect(protectedBytes([retirement.queueId]).outbox).toEqual(before.outbox);
 });
});
