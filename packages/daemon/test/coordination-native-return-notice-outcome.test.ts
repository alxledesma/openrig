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
import {digest} from '../src/domain/coordinator-authority-service.js';
import {archiveAgedTerminalTransitions} from '../src/domain/queue-retention.js';
import {seed,token} from './helpers/coordinator-fixture.js';

// G15 (delta to 349b4d70 incl. R7 custody evidence): native-return instruction wakes are
// outcome-contained only through producer-bound exact native outcomes (C1); the failure-only
// retirement path may proceed past THIS lineage's own uncontained indeterminate notices
// (Root-approved C2). Pending/sending and unrelated effects never qualify; nothing is
// rewritten or re-sent; missing schema or legacy-absent immutable evidence stays held.
describe('G15 native-return instruction-wake outcome containment',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 const sessions=['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv','operator-agent@kernel'];
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 /** Captures a typed refusal. CoordinatorFenceError exposes `code` as a property, so a code
 * assertion cannot be expressed with toThrow(string), which matches only the message. */
 function refusal(fn:()=>unknown):{code?:string;message:string}{try{fn();}catch(e){return {code:(e as {code?:string}).code,message:(e as Error).message};}throw new Error('expected a typed refusal, but the call succeeded');}
 const plan=(tasks:CoordinationTask[],overrides:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,...overrides,tasks});
function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
  function configure(tasks:CoordinationTask[],resources:Record<string,string[]>={},planOverrides:Partial<CoordinationPlan>={}){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:resources[t.key]??[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,planOverrides));}
 function refresh(){for(const s of sessions)samples.set(s,{...sample(s),identityObservedAt:new Date(clock).toISOString()});}
 function job(){db.prepare(`INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')`).run(new Date(clock).toISOString());}
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const noticeRow=(outboxId:string)=>db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(outboxId) as any;
 const normal=()=>[task('product'),task('repair','architect@xv',{recoveryFor:'product'})];
 beforeEach(async()=>{
  vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'g15-'));db=createDb(join(dir,'db'));seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  for(const s of sessions)db.prepare('INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES (?,?,?,?)').run('binding-'+s,s,s,'%1');
  repo.attachTransport({send:async()=>({ok:true,verified:true})});
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,(s)=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});
 async function claimedLegacyReturnDuty(){
  configure(normal(),{product:['source.ts']},{allowIdlePeerTransfer:false});const original=reconcile()[0].queueId!;
  repo.claim({qitemId:original,destinationSession:'builder@xv',identityProvenance:'transport:v1',actorGeneration:'builder-g1'});repo.update({qitemId:original,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  const queueId='qitem-coordination-terminal-return-'+digest('xv:'+original+':builder-g1').slice(0,24),deadline=clock+20000;
  const body=JSON.stringify({action:'record-exact-native-terminal-return',rigId:'xv',packageKey:'product',originalQueueId:original,recipientGeneration:'builder-g1',inputDigest:digest('product'),returnContract:{destination:'lead@xv',evidenceRequired:['report']},deadline,grantsAuthority:false,required:'claim exactly'});
  db.transaction(()=>repo.createNativeTerminalReturnDuty('lead@xv','lead-g1','xv',{qitemId:queueId,sourceSession:'watchdog@system',destinationSession:'builder@xv',expiresAt:new Date(deadline).toISOString(),body,identityProvenance:'system:operator-authorized-coordination',nudge:false}))();
  repo.claim({qitemId:queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1',actorGeneration:'builder-g1'});
  await repo.create({qitemId:'legacy-real-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'retained/report.md'}]}),nudge:false});
  return {original,queueId,body,deadline,input:{rigId:'xv',controlQueueId:queueId,controlBodyHash:digest(body),workerGeneration:'builder-g1',deadline}};
 }
 async function stalledContinuation(){
  const duty=await claimedLegacyReturnDuty();
  const staged=svc.continueTerminalReturn('operator-agent@kernel','operator-agent-g1',duty.input);
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(staged.outboxId);
  return {duty,outboxId:staged.outboxId};
 }
 // Exhaustion intake, genuinely claimed by current Operator, control expired.
 function exhaustedIntake(duty:any){job();clock=duty.deadline+1;vi.setSystemTime(clock);refresh();reconcile();
  const intake=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as {qitem_id:string}).qitem_id;
  repo.claim({qitemId:intake,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});return intake;}
 const outcomeRow=(outboxId:string)=>db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='native-return-notice-outcome'").get('native-return-notice-outcome:'+outboxId) as {receipt:string}|undefined;
 function cancelControl(qitemId:string){repo.update({qitemId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'canceled'});}
 function dropCustodyEvidence(){db.prepare('DROP TABLE queue_native_custody_evidence').run();}// legacy/pre-R7 database shape
 function tamperCustodyReceipt(qitemId:string,mutate:(receipt:any)=>void){
  const transition=repo.transitionLog.latestForQitem(qitemId)!;
  const recorded=db.prepare('SELECT receipt FROM queue_native_custody_evidence WHERE transition_id=?').get(transition.transitionId) as {receipt:string};
  const receipt=JSON.parse(recorded.receipt);mutate(receipt);
  // Fault injection starts from a real producer receipt; production UPDATE/DELETE is prohibited.
  dropCustodyEvidence();
  db.exec("CREATE TABLE queue_native_custody_evidence(transition_id INTEGER PRIMARY KEY,qitem_id TEXT NOT NULL,receipt TEXT NOT NULL)");
  db.prepare('INSERT INTO queue_native_custody_evidence VALUES(?,?,?)').run(transition.transitionId,qitemId,JSON.stringify(receipt));
 }
 function forgeCustodyReceiptWithWrongGeneration(qitemId:string){tamperCustodyReceipt(qitemId,r=>r.actorGeneration='some-other-generation');}
 async function retirement(){
  const continued=await stalledContinuation(),intake=exhaustedIntake(continued.duty);
  const ret=svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,controlQueueId:continued.duty.queueId,controlBodyHash:digest(continued.duty.body),workerGeneration:'builder-g1',deadline:clock+20000});
  return {...continued,intake,ret};
 }
 function successorInput(duty:any,intake:string){return {rigId:'xv',intakeQueueId:intake,previousControlId:duty.queueId,previousBodyHash:digest(duty.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',deadline:clock+20000,operationId:'g15-new-successor'};}

 it('C1 continuation: genuine typed disposal contains the UNKNOWN notice without touching notice bytes',async()=>{
  const {duty,outboxId}=await stalledContinuation();
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','legacy-real-return');
  reconcile();
  expect(JSON.parse(outcomeRow(outboxId)!.receipt)).toMatchObject({proofId:'native-return-continuation:'+duty.queueId,worker:'builder@xv',workerGeneration:'builder-g1',dispositionOperationId:'legacy-real-return',deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false});
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(true);
  cancelControl(duty.queueId);// duty completes truthfully after disposal
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(true);// recorded disposition remains a valid witness after the new terminal outcome
  expect(reconcile().find(r=>r.key==='acceptance:product')).toBeDefined();
 });
 it('C1 primary: real R7 custody evidence on the native terminal transition contains the retirement notice; successor authorizable',async()=>{
  const {duty,outboxId}=await stalledContinuation();
  const intake=exhaustedIntake(duty);
  const ret=svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000} as any);
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  cancelControl(duty.queueId);reconcile();
  expect((db.prepare('SELECT count(*) n FROM queue_native_custody_evidence').get() as {n:number}).n).toBeGreaterThan(0);
  expect(JSON.parse(outcomeRow(ret.outboxId)!.receipt)).toMatchObject({proofId:'native-return-retirement:'+duty.queueId,deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false});
  expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(true);
  const successor=svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,previousControlId:duty.queueId,previousBodyHash:digest(duty.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',deadline:clock+20000,operationId:'g15-successor'});
  expect(successor.queueId).toBeTruthy();
  expect(db.prepare('SELECT count(*) n FROM outbox_entries WHERE outbox_id IN (?,?)').get(outboxId,ret.outboxId)).toEqual({n:2});// no resend
 });
 it('missing-schema fail-closed: legacy database without custody evidence keeps retirement notice held and :80 refusing',async()=>{
  const {duty}=await stalledContinuation();
  const intake=exhaustedIntake(duty);
  const ret=svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000} as any);
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  cancelControl(duty.queueId);dropCustodyEvidence();reconcile();
  expect(outcomeRow(ret.outboxId)).toBeUndefined();
  expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(false);
  expect(refusal(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,previousControlId:duty.queueId,previousBodyHash:digest(duty.body),workerGeneration:'builder-g1',holderGeneration:'lead-g1',deadline:clock+20000,operationId:'g15-refuse'}))).toMatchObject({code:'coordination_return_successor_unknown_effect'});
 });
 it('wrong immutable generation in otherwise valid evidence is rejected; mutable claim fields alone never prove anything',async()=>{
  const {duty}=await stalledContinuation();
  const intake=exhaustedIntake(duty);
  const ret=svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000} as any);
  db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  cancelControl(duty.queueId);
  forgeCustodyReceiptWithWrongGeneration(duty.queueId);
  db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g1' WHERE qitem_id=?").run(duty.queueId);// mutable field points "right"
  reconcile();
  expect(outcomeRow(ret.outboxId)).toBeUndefined();
 });
 it('custody evidence rows are immutable: tampering attempts abort',async()=>{
  const {duty}=await stalledContinuation();
  const intake=exhaustedIntake(duty);
  svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000} as any);
  cancelControl(duty.queueId);
  const row=db.prepare('SELECT transition_id FROM queue_native_custody_evidence LIMIT 1').get() as {transition_id:number};
  expect(()=>db.prepare("UPDATE queue_native_custody_evidence SET receipt='{}' WHERE transition_id=?").run(row.transition_id)).toThrow();
  expect(()=>db.prepare('DELETE FROM queue_native_custody_evidence WHERE transition_id=?').run(row.transition_id)).toThrow();
 });
 it('C2 lineage exclusion: retirement stages despite this lineage uncontained continuation notice; nothing resent',async()=>{
  const {duty,outboxId}=await stalledContinuation();
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);
  job();clock=duty.deadline+1;vi.setSystemTime(clock);refresh();
  expect(reconcile().find(r=>r.key==='terminal-return:product')?.reason).not.toBe('uncertain-worker-effect');// former permanent dead end
  const intake=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as {qitem_id:string}).qitem_id;
  repo.claim({qitemId:intake,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  expect(svc.supervise('xv','j')?.find(r=>r.key==='terminal-return:product')?.reason).toBe('native-retirement-notice-staged');
  expect(db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-native-return-continuation:%'").get()).toEqual({n:1});
  expect(noticeRow(outboxId).delivery_state).toBe('indeterminate');
 });
 it.each(['pending','sending'] as const)('C2/C1 never exclude or contain %s transport state',async(state)=>{
  const {duty,outboxId}=await stalledContinuation();
  db.prepare('UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?').run(state,outboxId);
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);
  job();clock=duty.deadline+1;vi.setSystemTime(clock);refresh();
  const intake=(db.prepare("SELECT qitem_id FROM queue_items WHERE destination_session='operator-agent@kernel' AND json_extract(body,'$.reason')='terminal-return-duty-exhausted'").get() as {qitem_id:string}|undefined);
  if(intake)repo.claim({qitemId:intake.qitem_id,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  expect(refusal(()=>svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake?.qitem_id??'',controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000} as any))).toMatchObject({code:'coordination_return_continuation_unknown_effect'});
 });
 it('C2 stays lineage-exact: an unrelated worker UNKNOWN still blocks retirement',async()=>{
  const {duty}=await stalledContinuation();
  const intake=exhaustedIntake(duty);
  db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,tags,urgency,ts_dispatched,delivery_state,audit_pointer,identity_provenance) VALUES('wake-intent-unrelated-x','builder@xv','lead@xv','unrelated','[]','routine',?,'indeterminate',NULL,NULL)").run(new Date(clock).toISOString());
  expect(refusal(()=>svc.retireExpiredTerminalReturn('operator-agent@kernel','operator-agent-g1',{rigId:'xv',intakeQueueId:intake,controlQueueId:duty.queueId,controlBodyHash:digest(duty.body),workerGeneration:'builder-g1',deadline:clock+20000} as any))).toMatchObject({code:'coordination_return_continuation_unknown_effect'});
 });
 it('binding drift defeats containment: tag, audit_pointer, sender and cross-rig mismatches stay blocking',async()=>{
  const {duty,outboxId}=await stalledContinuation();
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','legacy-real-return');reconcile();
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(true);
  const pristine=noticeRow(outboxId);
  const restore=()=>{db.prepare('UPDATE outbox_entries SET tags=?,audit_pointer=?,sender_session=? WHERE outbox_id=?').run(pristine.tags,pristine.audit_pointer,pristine.sender_session,outboxId);};
  db.prepare("UPDATE outbox_entries SET tags='[\"queue:native-return-continuation\",\"other\",\"\"]' WHERE outbox_id=?").run(outboxId);
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);restore();
  db.prepare('UPDATE outbox_entries SET audit_pointer=? WHERE outbox_id=?').run('qitem-other',outboxId);
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);restore();
  db.prepare("UPDATE outbox_entries SET sender_session='lead@xv' WHERE outbox_id=?").run(outboxId);
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);restore();
  expect(svc.noticeOutcomeContained('other',noticeRow(outboxId))).toBe(false);
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(true);
 });
 it('C2 authorizes and delivers only its exact failure-only retirement despite continuation UNKNOWN',async()=>{
  const {duty,outboxId,ret}=await retirement(),before=noticeRow(outboxId),control=repo.getById(duty.queueId),product=repo.getById(duty.original),resources=db.prepare('SELECT * FROM coordinator_resources').all();
  const sends:string[]=[];
  repo.attachTransport({send:async(session,body,opts)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,session,opts?.queueAssignmentId);if(opts?.committedOutboxIds?.includes(ret.outboxId)){sends.push(body);return {ok:true,verified:false};}return {ok:true,verified:true};}});
  expect(svc.validTerminalReturnContinuationWake('watchdog@system','builder@xv','native-return-retirement:'+duty.queueId)).toBe(true);
  await svc.deliverCommitted();
  expect(sends).toHaveLength(1);expect(sends[0]).toContain('failure-only');
  expect(noticeRow(ret.outboxId).delivery_state).toBe('indeterminate');expect(noticeRow(outboxId)).toEqual(before);
  await svc.deliverCommitted();expect(sends).toHaveLength(1);// UNKNOWN is never retried
  expect(repo.getById(duty.queueId)).toEqual({...control,lastNudgeAttempt:new Date(clock).toISOString(),lastNudgeResult:'delivered-ack-pending'});expect(repo.getById(duty.original)).toEqual(product);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);
  expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(false);
 });
 it('no native retirement leaves notices held and successor authorization refused; ordinary work remains debt-blocked',async()=>{
  const {duty,intake,ret}=await retirement();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  reconcile();expect(outcomeRow(ret.outboxId)).toBeUndefined();expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(false);
  expect(refusal(()=>svc.authorizeTerminalReturnSuccessor('operator-agent@kernel','operator-agent-g1',successorInput(duty,intake)))).toMatchObject({code:'coordination_return_successor_required'});
  const prior=svc.plan('xv')!;configure([...prior.tasks,task('independent'),task('independent-repair','architect@xv',{recoveryFor:'independent'})],{product:['source.ts']},{...prior,revision:'r2'});
  expect(reconcile().find(r=>r.key==='independent')).toMatchObject({state:'held',reason:'uncertain-worker-effect'});
 });
 it('retirement notice exhaustion remains a finite owned boundary with no further notice or resend',async()=>{
  const {ret}=await retirement();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  const notices=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-native-return-%'").all();
  clock=ret.deadline+1;vi.setSystemTime(clock);refresh();
  expect(svc.supervise('xv','j')?.find(r=>r.key==='terminal-return:product')).toMatchObject({state:'held',reason:'native-retirement-notice-exhausted'});
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-native-return-%'").all()).toEqual(notices);
 });
 it('primary proof reads genuine archived terminal evidence and survives exact native same-state notes',async()=>{
  const {duty,ret}=await retirement();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  cancelControl(duty.queueId);
  repo.update({qitemId:duty.queueId,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',transitionNote:'Retained exact terminal control; no follow-on authority'});
  archiveAgedTerminalTransitions(db,{nowIso:new Date(clock+40*86400000).toISOString(),transitionsRetentionDays:30,terminalStates:['canceled']});
  expect(db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=?').all(duty.queueId)).toEqual([]);
  reconcile();expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(true);
 });
 it('terminal transition before notice dispatch cannot prove that notice outcome',async()=>{
  const {duty,ret}=await retirement();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  vi.setSystemTime(clock-2);cancelControl(duty.queueId);vi.setSystemTime(clock);
  reconcile();expect(outcomeRow(ret.outboxId)).toBeUndefined();expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(false);
 });
 it.each(['non-native','other-actor','replacement'] as const)('primary %s terminal actor never proves original-generation retirement',async(mode)=>{
  const {duty,ret}=await retirement();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);
  let actorSession='builder@xv',actorGeneration='builder-g1',identityProvenance='transport:v1';
  if(mode==='non-native')identityProvenance='claimed:v1';
  if(mode==='other-actor'){actorSession='reviewer@xv';actorGeneration='reviewer-g1';}
  if(mode==='replacement'){db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();actorGeneration='builder-g2';}
  repo.update({qitemId:duty.queueId,actorSession,actorGeneration,identityProvenance,state:'canceled'});
  reconcile();expect(outcomeRow(ret.outboxId)).toBeUndefined();expect(svc.noticeOutcomeContained('xv',noticeRow(ret.outboxId))).toBe(false);
 });
 it.each(['transition-note','before-generation','after-body'] as const)('immutable proof rejects %s receipt drift against the real transition and snapshots',async(mode)=>{
  const {duty,ret}=await retirement();db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(ret.outboxId);cancelControl(duty.queueId);
  tamperCustodyReceipt(duty.queueId,r=>{if(mode==='transition-note')r.transition.transitionNote='changed';if(mode==='before-generation')r.beforeQueue.claimed_by_generation_uuid='wrong';if(mode==='after-body')r.afterQueue.body='changed';});
  reconcile();expect(outcomeRow(ret.outboxId)).toBeUndefined();
 });
 it.each(['body','control-receipt','notice-receipt','outbox-id'] as const)('producer-bound %s drift is neither contained nor granted C2 retirement exclusion',async(mode)=>{
  const {duty,outboxId}=await stalledContinuation();
  if(mode==='body')db.prepare("UPDATE outbox_entries SET body='changed' WHERE outbox_id=?").run(outboxId);
  if(mode==='control-receipt')db.prepare("UPDATE coordinator_operations SET receipt='null' WHERE rig_id='xv' AND operation_id=? AND kind='native-terminal-return-control'").run(duty.queueId);
  if(mode==='notice-receipt'){const op=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=?").get('native-return-continuation:'+duty.queueId) as {receipt:string};const r=JSON.parse(op.receipt);r.controlReceiptHash='changed';db.prepare('UPDATE coordinator_operations SET receipt=? WHERE operation_id=?').run(JSON.stringify(r),'native-return-continuation:'+duty.queueId);}
  if(mode==='outbox-id')db.prepare('UPDATE outbox_entries SET outbox_id=? WHERE outbox_id=?').run(outboxId+'-changed',outboxId);
  const effect=noticeRow(mode==='outbox-id'?outboxId+'-changed':outboxId);
  expect(svc.noticeOutcomeContained('xv',effect)).toBe(false);
  // Test the binding refusal directly; corrupting a control receipt also invalidates its unrelated duty APIs.
  expect((svc as any).nativeReturnRetirementChainNotices('xv',duty.original,'builder@xv','builder-g1')).toEqual([]);
 });
 it('current generation drift invalidates a recorded witness instead of attributing old native evidence to a replacement',async()=>{
  const {duty,outboxId}=await stalledContinuation();repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','legacy-real-return');reconcile();
  const witness=outcomeRow(outboxId);expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(true);
  db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
  expect(svc.noticeOutcomeContained('xv',noticeRow(outboxId))).toBe(false);expect(outcomeRow(outboxId)).toEqual(witness);
 });

 });
