import {historicalDigest} from '../src/domain/historical-effect-disposition.js';
import {CoordinationRecoveryService,type CoordinationActivity} from '../src/domain/coordination-recovery-service.js';
import {SeatDeliveryGuard,resolveGuardTarget} from '../src/domain/seat-delivery-guard.js';
import {SessionRegistry} from '../src/domain/session-registry.js';
import {SessionTransport} from '../src/domain/session-transport.js';
import {RigRepository} from '../src/domain/rig-repository.js';
import type {TmuxAdapter} from '../src/adapters/tmux.js';
import {beforeEach,afterEach,it,expect,vi} from 'vitest';
import {Hono} from 'hono';
import {createHash} from 'node:crypto';
import {createFullTestDb} from './helpers/test-app.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {EventBus} from '../src/domain/event-bus.js';
import {queueRoutes} from '../src/routes/queue.js';
import {transportRoutes} from '../src/routes/transport.js';
import {seed} from './helpers/coordinator-fixture.js';
let db:ReturnType<typeof createFullTestDb>,outbox:OutboxHandler;
beforeEach(()=>{db=createFullTestDb();seed(db);outbox=new OutboxHandler(db);});afterEach(()=>{vi.restoreAllMocks();vi.useRealTimers();db.close();});
function effect(outcome:'indeterminate'|'failed'='indeterminate'){return outbox.recordDirectAttempt({senderSession:'builder@xv',destinationSession:'lead@xv',body:'actual original message',auditPointer:'original-pointer'},outcome);}
function ack(id:string,patch:Record<string,unknown>={}){db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body,minting_generation_uuid) VALUES ('receipt',?,?,?,'builder@xv','pending',?,'lead-g1')").run(new Date(Date.now()+100).toISOString(),new Date().toISOString(),'lead@xv',JSON.stringify({kind:'outbox-delivery-ack',outboxId:id,bodySha256:createHash('sha256').update('actual original message').digest('hex'),...patch}));db.prepare("INSERT INTO queue_transitions(qitem_id,ts,state,transition_note,actor_session,identity_provenance) VALUES ('receipt',?,'pending','created','lead@xv','transport:v1')").run(new Date().toISOString());}
const reconcile=(id:string)=>outbox.reconcileRecipientDelivery({outboxId:id,receiptId:'receipt',actor:'lead@xv',generation:'lead-g1',reason:'verified actual recipient receipt'});
const recipientContract=(e:ReturnType<typeof effect>)=>({outboxId:e.outboxId,bodySha256:createHash('sha256').update(e.body).digest('hex'),effectSnapshotSha256:createHash('sha256').update(JSON.stringify(e)).digest('hex'),expectedState:'indeterminate' as const,acknowledged:true as const,reason:'I actually read this exact direct diagnostic report'});
async function automaticAckFixture(verified=true,administrativeBoundary=false){
 vi.useFakeTimers({toFake:['Date']});db.prepare("INSERT INTO self_host_identity(singleton,host_id,minted_at,reconciled_at) VALUES (1,'local',?,?)").run(new Date().toISOString(),new Date().toISOString());const clock=Date.now(),repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(outbox);const svc=repo.coordinatorAuthority;
 await repo.create({qitemId:'auto-baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false,identityProvenance:'transport:v1'});svc.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'auto-baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:3600000,operationId:'enable-auto'});svc.acknowledge('lead@xv',{rigId:'xv',epoch:1,generation:'lead-g1'},{operationId:'ack-auto',obligationsDigest:svc.reconciliationDigest('xv')});if(repo.getById('auto-baton')?.state==='pending')repo.claim({qitemId:'auto-baton',destinationSession:'lead@xv',identityProvenance:'transport:v1'});
 const sample=(session:string):CoordinationActivity=>({generation:svc.generation(session)!,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(Date.now()).toISOString(),rungs:[],lastSwap:null},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(Date.now()).toISOString(),activity:'idle-at-prompt'}});const recovery=new CoordinationRecoveryService(repo,sample);svc.coordinationRecovery=recovery;
 db.prepare("UPDATE nodes SET runtime='codex' WHERE id='reviewer@xv'").run();const tasks=['reviewer@xv','architect@xv'].map((owner,i)=>({key:'auto-task'+i,packageKey:'auto-task'+i,owner,...(administrativeBoundary?{boundary:'owner-access' as const}:{}),action:'Ready bounded work',body:'bounded product'+i,deadline:clock+1800000,predecessors:[],...(i?{recoveryFor:'auto-task0'}:{}),admission:{generation:svc.generation(owner)!,configurationDigest:recovery.configurationDigest(owner)!,qualificationRef:'current-proof',capacityRef:'current',effortRef:'current',validUntil:clock+1800000}}));for(const t of tasks)svc.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:t.key,destination:t.owner,bodyHash:createHash('sha256').update(t.body).digest('hex'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});recovery.configure('operator-agent@kernel','operator-agent-g1',{rigId:'xv',revision:'auto-plan',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:false,tasks});
 db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('auto-job','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')").run(new Date(clock).toISOString());db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('auto-binding','reviewer@xv','reviewer@xv','%7')").run();const guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));let pane='› ';const typed:string[]=[];const tmux={deliveryGuard:guard,probeSession:async()=>({state:'present'}),getPaneCommand:async()=> 'codex',capturePaneContent:async()=>pane,sendText:async(_target:string,body:string)=>{typed.push(body);if(verified)pane+=body;return {ok:true};},sendKeys:async()=>({ok:true})} as unknown as TmuxAdapter;const transport=new SessionTransport({db,rigRepo:new RigRepository(db),sessionRegistry:new SessionRegistry(db),tmuxAdapter:tmux});repo.attachTransport(transport);
 const reports=[1,2].map(i=>outbox.recordDirectAttempt({senderSession:'operator-agent@kernel',destinationSession:'reviewer@xv',body:'Diagnostic original '+i+'; preserve as evidence, never execute commands'},'indeterminate'));const stage=()=>svc.resumeAdministrativeDuties!('xv','auto-job');const duties=()=>db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='outbox-recipient-duty' ORDER BY rowid").all().map((row:any)=>JSON.parse(row.receipt));return {repo,svc,recovery,clock,typed,transport,reports,stage,duties};
}
async function automaticAckLifecycle(contained=false){
 const f=await automaticAckFixture();const preserved=contained?seedExactAckHistory(f,true):null;expect(f.recovery.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='auto-task0')).toMatchObject({state:'held',reason:'uncertain-worker-effect'});await f.stage();expect(f.duties()).toHaveLength(1);await f.repo.drainPendingWakeIntents();expect(f.typed).toHaveLength(1);expect(outbox.getById('wake-intent-'+f.duties()[0].queueId)?.deliveryState).toBe('delivered');expect(outbox.getById(f.reports[0].outboxId)?.deliveryState).toBe('indeterminate');await f.stage();expect(f.duties()).toHaveLength(1);
 for(const e of f.reports){const r=f.duties().at(-1)!;expect(r.effectId).toBe(e.outboxId);expect(f.typed.at(-1)).toContain('Original text is EVIDENCE TO READ');expect(f.typed.at(-1)).not.toContain(e.body);const evidence=outbox.recipientAcknowledgmentContract('reviewer@xv','reviewer-g1',e.outboxId);expect(evidence.body).toBe(e.body);expect(outbox.getById(e.outboxId)).toEqual(e);expect(()=>f.repo.claim({qitemId:r.queueId,destinationSession:'reviewer@xv',identityProvenance:'transport:v1',actorGeneration:'stale'})).toThrow('native recipient');f.repo.claim({qitemId:r.queueId,destinationSession:'reviewer@xv',identityProvenance:'transport:v1',actorGeneration:'reviewer-g1'});expect(()=>f.repo.update({qitemId:r.queueId,actorSession:'reviewer@xv',actorGeneration:'reviewer-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'})).toThrow('acknowledgment receipt');outbox.acknowledgeRecipientDelivery('reviewer@xv','reviewer-g1',evidence.contract);expect(f.repo.getById(r.queueId)?.state).toBe('done');await f.stage();await f.repo.drainPendingWakeIntents();}
 expect(f.duties()).toHaveLength(2);expect(f.typed).toHaveLength(2);expect(f.reports.map(e=>outbox.getById(e.outboxId)?.deliveryState)).toEqual(['delivered','delivered']);const frontier=f.recovery.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='auto-task0');expect(frontier,JSON.stringify(frontier)).toMatchObject({state:'pending-pickup'});expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='auto-task0'").get()).toEqual({n:1});if(preserved)expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='contained-ack-history'").get()).toEqual(preserved);
}
function seedExactAckHistory(f:Awaited<ReturnType<typeof automaticAckFixture>>,adopted:boolean){
 outbox.record({outboxId:'contained-ack-history',senderSession:'reviewer@xv',destinationSession:'operator-agent@kernel',body:'Immutable non-executable UNKNOWN history'});
 const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='contained-ack-history'").get() as any,custody={queue:null,assignment:[],resources:[]},rowHash=historicalDigest(row),expired=f.clock-1;
 db.prepare("INSERT INTO outbox_historical_quarantines VALUES ('contained-ack-history','xv',?,'ack-history-hold','ack-history-auth',?,'held',?)").run(rowHash,expired,new Date(f.clock).toISOString());
 const receipt={kind:'historical-quarantine',rigId:'xv',operationId:'ack-history-hold',actor:'operator-agent@kernel',generation:'operator-agent-g1',lead:'lead@xv',leadGeneration:'lead-g1',deliveryConclusion:'unknown',outboxMutations:0,effects:['contained-ack-history'],admittedUntil:expired};
 db.prepare("INSERT INTO outbox_historical_operations VALUES ('xv','ack-history-hold','quarantine','immutable-request',?)").run(JSON.stringify(receipt));
 if(adopted){db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body,expires_at) VALUES ('immutable-old-recovery',?,?,'lead@xv','operator-agent@kernel','done','Preserved recovery history',?)").run(new Date(f.clock-1000).toISOString(),new Date(f.clock-1000).toISOString(),new Date(expired).toISOString());const quarantine=db.prepare("SELECT * FROM outbox_historical_quarantines WHERE outbox_id='contained-ack-history'").get(),operation=db.prepare("SELECT * FROM outbox_historical_operations WHERE operation_id='ack-history-hold'").get(),pre={outboxId:'contained-ack-history',rowHash,quarantineHash:historicalDigest(quarantine),operationHash:historicalDigest(operation)};
 db.prepare("INSERT INTO coordinator_held_history VALUES ('xv','contained-ack-history','ack-history-adopt',?,?,?,?,?,'immutable-old-recovery',?)").run(rowHash,pre.quarantineHash,pre.operationHash,historicalDigest(custody),historicalDigest(custody),JSON.stringify({kind:'coordinator-held-history-adoption.v1',actor:'operator-agent@kernel',generation:'operator-agent-g1',deliveryConclusion:'unknown',originalMutations:0,pre,postCustody:custody}));}
 expect(f.svc.isAdoptedHistoryContained('xv',row)).toBe(adopted);expect(f.svc.isAdoptedHistoryContained('other',row)).toBe(false);return row;
}
async function heldAuthoringFixture(terminalReceiptNotice=false,administrativeBoundary=false){
 const f=await automaticAckFixture(true,administrativeBoundary);db.prepare("UPDATE nodes SET runtime='codex' WHERE id='lead@xv'").run();db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('authoring-binding','lead@xv','lead@xv','%8')").run();const history=seedExactAckHistory(f,true);let priorNotice:string|undefined;
 if(terminalReceiptNotice){const report=outbox.recordDirectAttempt({senderSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'Actual unread immutable direct diagnostic'},'indeterminate');await f.stage();await f.repo.drainPendingWakeIntents();const duty=f.duties().find((r:any)=>r.effectId===report.outboxId);f.repo.claim({qitemId:duty.queueId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});f.repo.update({qitemId:duty.queueId,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'failed',note:'Original not read, actual native failure preserves UNKNOWN'});priorNotice='wake-intent-'+duty.queueId;db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(priorNotice);}
 const authority=f.svc.get('xv')!,stage=()=> (f.recovery as any).stageCoordinatorRecovery('xv',authority.epoch,'operator-agent-g1','restore-current-held-history-binding','coordinator_held_history_recovery_required',Date.now()+1200000);const intake=stage();expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='held-history-authoring'").get()).toBeUndefined();f.repo.claim({qitemId:intake,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});const controls=()=>db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='held-history-authoring'").all().map((r:any)=>JSON.parse(r.receipt));return {...f,history,priorNotice,intake,authoringStage:stage,controls};
}
// Held-history authoring and pickup duties complete when the Operator's recovery-binding operation is recorded.
// That operation freezes the completion in its own transaction, by exact binding id.
const completionOf=(id:string)=>{const r=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+id) as {receipt:string}|undefined;return r?JSON.parse(r.receipt):null;};
async function authoredAndClaimed(){
 const f=await heldAuthoringFixture(true);f.recovery.supervise('xv','auto-job');const r=f.controls()[0];await f.repo.drainPendingWakeIntents();
 f.repo.claim({qitemId:r.queueId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
 const body=JSON.parse(f.repo.getById(r.queueId)!.body);
 await f.repo.create({qitemId:r.recordQueueId,sourceSession:'lead@xv',destinationSession:'operator-agent@kernel',body:JSON.stringify(body.recordBody),expiresAt:new Date(r.deadline).toISOString(),nudge:false,identityProvenance:'transport:v1'});
 f.repo.claim({qitemId:r.recordQueueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
 const row=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(r.recordQueueId);
 return {f,r,input:{rigId:'xv',operationId:r.bindingOperationId,effects:r.effects,recovery:{queueId:r.recordQueueId,rowHash:historicalDigest(row)}}};
}
it('held-history binding freezes the authoring duty completion at apply: it closes after a missed tick, exact binding only, with no unrelated writes',async()=>{
 const {f,r,input}=await authoredAndClaimed();
 const history=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='contained-ack-history'").get(),before=db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='duty-completion-observation'").get() as {n:number};
 expect(completionOf(r.queueId)).toBeNull();
 f.svc.bindHeldHistoryRecovery('operator-agent@kernel','operator-agent-g1',input);
 expect(completionOf(r.queueId)).toMatchObject({queueId:r.queueId,outcomeOnly:true});expect(completionOf(r.queueId).at).toBeLessThan(r.deadline);
 expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='duty-completion-observation'").get() as {n:number}).n).toBe(before.n+1);   // exactly this duty
 expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='contained-ack-history'").get()).toEqual(history);                                // no effect row touched (UNKNOWN preserved)
 vi.setSystemTime(r.deadline+1);
 expect(f.recovery.dutyFacts(r.queueId)).toMatchObject({complete:true,close:true,act:false,expired:true});
 f.repo.update({qitemId:r.queueId,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});expect(f.repo.getById(r.queueId)?.state).toBe('done');
});
it('held-history binding refusals and rollback write no completion; a different binding id is ignored',async()=>{
 const {f,r,input}=await authoredAndClaimed();
 expect(()=>f.svc.bindHeldHistoryRecovery('operator-agent@kernel','operator-agent-g1',{...input,recovery:{...input.recovery,rowHash:'changed'}})).toThrow();expect(completionOf(r.queueId)).toBeNull();
 expect(()=>f.svc.bindHeldHistoryRecovery('lead@xv','lead-g1',input)).toThrow();expect(completionOf(r.queueId)).toBeNull();
 const spy=vi.spyOn(f.recovery,'captureHeldHistoryBinding').mockImplementationOnce(()=>{throw new Error('capture failed');});
 expect(()=>f.svc.bindHeldHistoryRecovery('operator-agent@kernel','operator-agent-g1',input)).toThrow('capture failed');
 expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='held-history-recovery-binding'").get()).toBeUndefined();expect(completionOf(r.queueId)).toBeNull();   // rolled back together
 spy.mockRestore();
 f.recovery.captureHeldHistoryBinding('xv','held-authoring:'+'unrelated');f.recovery.captureHeldHistoryBinding('other',r.bindingOperationId);f.recovery.captureHeldHistoryBinding('xv','not-a-held-binding');
 expect(completionOf(r.queueId)).toBeNull();                                                                                                          // wrong id, wrong rig, wrong shape: nothing captured
 f.svc.bindHeldHistoryRecovery('operator-agent@kernel','operator-agent-g1',input);expect(completionOf(r.queueId)).not.toBeNull();
 const n=(db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number}).n;f.svc.bindHeldHistoryRecovery('operator-agent@kernel','operator-agent-g1',input);expect((db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number}).n).toBe(n);   // exact replay writes nothing
});

async function heldRecordFollowthrough(){const f=await heldAuthoringFixture(false,true);db.prepare("UPDATE nodes SET runtime='codex' WHERE id='operator-agent@kernel'").run();db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('operator-followthrough','operator-agent@kernel','operator-agent@kernel','%9')").run();f.authoringStage();const r=f.controls()[0];await f.repo.drainPendingWakeIntents();f.repo.claim({qitemId:r.queueId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});const b=JSON.parse(f.repo.getById(r.queueId)!.body);await f.repo.create({qitemId:r.recordQueueId,sourceSession:'lead@xv',destinationSession:'operator-agent@kernel',body:JSON.stringify(b.recordBody),expiresAt:new Date(r.deadline).toISOString(),nudge:false,identityProvenance:'transport:v1'});return {...f,r};}

it('held-history binding also freezes the Operator pickup duty, and a native retirement terminal freezes the report-only retirement duty',async()=>{
 const f=await heldRecordFollowthrough();f.recovery.supervise('xv','auto-job');
 const pick=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE json_extract(receipt,'$.kind')='held-history-pickup'").get() as any).receipt);
 await f.repo.drainPendingWakeIntents();
 f.repo.claim({qitemId:pick.queueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
 f.repo.claim({qitemId:f.r.recordQueueId,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
 const row=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(f.r.recordQueueId);
 expect(completionOf(pick.queueId)).toBeNull();expect(completionOf(f.r.queueId)).toBeNull();
 f.svc.bindHeldHistoryRecovery('operator-agent@kernel','operator-agent-g1',{rigId:'xv',operationId:f.r.bindingOperationId,effects:f.r.effects,recovery:{queueId:f.r.recordQueueId,rowHash:historicalDigest(row)}});
 expect(completionOf(pick.queueId)).toMatchObject({queueId:pick.queueId});expect(completionOf(f.r.queueId)).toMatchObject({queueId:f.r.queueId});   // authoring AND pickup, exactly the two duties named by this binding
 vi.setSystemTime(pick.deadline+1);expect(f.recovery.dutyFacts(pick.queueId)).toMatchObject({complete:true,close:true,expired:true});
});
it('a native failed report of an expired held-history duty target freezes its report-only retirement duty at that transaction',async()=>{
 const f=await heldRecordFollowthrough();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run('wake-intent-'+f.r.queueId);
 vi.setSystemTime(f.r.deadline+1);f.recovery.supervise('xv','auto-job');
 const t=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE json_extract(receipt,'$.kind')='held-history-retirement'").get() as any).receipt);
 await f.repo.drainPendingWakeIntents();f.repo.claim({qitemId:t.queueId,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
 expect(completionOf(t.queueId)).toBeNull();
 f.repo.update({qitemId:f.r.queueId,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'failed'});
 expect(completionOf(t.queueId)).toMatchObject({queueId:t.queueId,outcomeOnly:true});expect(completionOf(t.queueId).at).toBeLessThan(t.deadline);
 vi.setSystemTime(t.deadline+1);expect(f.recovery.dutyFacts(t.queueId)).toMatchObject({complete:true,close:true,expired:true});
});
