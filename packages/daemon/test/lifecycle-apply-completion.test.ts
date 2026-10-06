import { LIFECYCLE_INTAKE_RENEWAL_REASONS,makeCoordinatorContinuityPolicy} from '../src/domain/policies/coordinator-continuity.js';
import type {RuntimeAvailability} from '../src/domain/coordinator-runtime-availability.js';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {resolve} from 'node:path';
import { describe,it,expect,beforeEach,afterEach,vi } from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,LIFECYCLE_INTAKE_RENEWAL_REASONS,coordinationIdle,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {SeatActivityService} from '../src/domain/seat-activity-service.js';
import {RuntimeOutcomeAssessment} from '../src/domain/runtime-outcome-assessment.js';
import {digest} from '../src/domain/coordinator-authority-service.js';import {seed,token} from './helpers/coordinator-fixture.js';
describe('lifecycle apply-time completion capture',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 /** Captures a typed refusal. CoordinatorFenceError exposes `code` as a property, so a code
 * assertion cannot be expressed with toThrow(string), which matches only the message. */
 function refusal(fn:()=>unknown):{code?:string;message:string}{try{fn();}catch(e){return {code:(e as {code?:string}).code,message:(e as Error).message};}throw new Error('expected a typed refusal, but the call succeeded');}
 const plan=(tasks:CoordinationTask[],overrides:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,...overrides,tasks});
function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
  function configure(tasks:CoordinationTask[],resources:Record<string,string[]>={},planOverrides:Partial<CoordinationPlan>={}){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:resources[t.key]??[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,planOverrides));}
 function refresh(){for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));}
 function job(){db.prepare(`INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')`).run(new Date(clock).toISOString());}
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'coordination-'));db=createDb(join(dir,'db'));seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;});
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});
 const normal=()=>[task('product'),task('repair','architect@xv',{recoveryFor:'product'})];
 async function finishTyped(packageKey:string,owner:string,queueId:string,returnId:string){
  repo.claim({qitemId:queueId,destinationSession:owner,identityProvenance:'transport:v1'});repo.update({qitemId:queueId,actorSession:owner,state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:returnId,sourceSession:owner,destinationSession:'lead@xv',body:JSON.stringify({packageKey,inputDigest:digest(packageKey),evidence:[{kind:'report',ref:'actual/'+packageKey+'.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose(owner,repo.coordinatorAuthority.generation(owner)!,'xv',packageKey,returnId);db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
 }

 // ---------------------------------------------------------------------------------------------
 // Class contract: the authorized operation that makes a duty's postcondition true freezes the
 // completion (now, before the deadline) inside its own transaction. No tick, no scan, no late rescue.
 // ---------------------------------------------------------------------------------------------
 const completion=(id:string)=>{const r=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='duty-completion-observation'").get('duty-completion:'+id) as {receipt:string}|undefined;return r?JSON.parse(r.receipt):null;};
 const deliver=()=>db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
 const claimDuty=(id:string,session:string,gen:string)=>repo.claim({qitemId:id,destinationSession:session,actorGeneration:gen,identityProvenance:'transport:v1'});
 const closeDuty=(id:string,session:string,gen:string)=>repo.update({qitemId:id,actorSession:session,actorGeneration:gen,identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
 const pass=(ms:number)=>{clock+=ms;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+3600000);};
 const opsCount=()=>(db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number}).n;
 async function acceptanceDuty(){
  configure(normal());const original=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',original,'ret');
  const duty=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='acceptance:product')!;deliver();claimDuty(duty.queueId!,'lead@xv','lead-g1');
  return {id:duty.queueId!,deadline:svc.lifecycleControlReceipt(duty.queueId!)!.deadline};
 }
 const accept=()=>svc.accept('lead@xv','lead-g1','xv','product','ret','actual/accepted.md');

 it('acceptance: completion is frozen when accept commits, so the duty closes after a missed observer tick',async()=>{
  const {id,deadline}=await acceptanceDuty();expect(svc.dutyFacts(id)).toMatchObject({claim:true,complete:false});
  accept();
  expect(completion(id)).toMatchObject({queueId:id,outcomeOnly:true,at:clock});expect(completion(id).at).toBeLessThan(deadline);
  pass(deadline-clock+1);                                                  // no reconcile, no observer tick
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(id,'lead@xv','lead-g1');expect(repo.getById(id)?.state).toBe('done');
 });

 it('counterfactual: with the capture disabled the same acceptance leaves no observation and the duty cannot close',async()=>{
  const {id,deadline}=await acceptanceDuty();
  vi.spyOn(svc as any,'recordCompletionObservation').mockImplementation(()=>{});accept();
  expect(completion(id)).toBeNull();pass(deadline-clock+1);
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,close:false,expired:true});
  expect(()=>closeDuty(id,'lead@xv','lead-g1')).toThrow('Exact native acceptance');
 });

 it('acceptance refusals and late acceptance never create an observation or rescue the duty',async()=>{
  const {id,deadline}=await acceptanceDuty();
  expect(()=>svc.accept('lead@xv','lead-g1','xv','product','not-the-return','x.md')).toThrow();
  expect(()=>svc.accept('peer@xv','peer-g1','xv','product','ret','x.md')).toThrow();
  expect(()=>svc.accept('lead@xv','lead-g1','xv','product','ret','')).toThrow();
  expect(completion(id)).toBeNull();expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();
  pass(deadline-clock+1);accept();                                          // the fact commits, but after the deadline
  expect(completion(id)).toBeNull();
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,close:false,act:false,expired:true});
  expect(()=>closeDuty(id,'lead@xv','lead-g1')).toThrow('Exact native acceptance');
 });

 it('replay after the deadline writes nothing; a failing capture rolls the acceptance back and a retry succeeds',async()=>{
  const {id,deadline}=await acceptanceDuty();
  const spy=vi.spyOn(svc as any,'recordCompletionObservation').mockImplementationOnce(()=>{throw new Error('observer failed');});
  expect(()=>accept()).toThrow('observer failed');
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeUndefined();expect(completion(id)).toBeNull();
  spy.mockRestore();accept();const first=completion(id);expect(first).not.toBeNull();
  pass(deadline-clock+1);const before=opsCount();accept();expect(opsCount()).toBe(before);expect(completion(id)).toEqual(first);
 });


 it('capture and configure resolve duties with indexed exact lookups only, never the rig operation history',async()=>{
  const {id}=await acceptanceDuty();accept();
  const key=(db.prepare('SELECT queue_id FROM coordinator_assignments WHERE package_key=?').get('product') as any).queue_id+':ret';
  const spy=vi.spyOn(db,'prepare');(svc as any).captureDutyCompletion('xv','acceptance',key);
  const sqls=spy.mock.calls.map(c=>String(c[0]));spy.mockRestore();
  expect(sqls.some(q=>/coordinator-lifecycle-control/.test(q)&&/FROM coordinator_operations/.test(q)&&!/rootId|operation_id|targetQueueId/.test(q))).toBe(false);
  const plan=(sql:string,...args:unknown[])=>(db.prepare('EXPLAIN QUERY PLAN '+sql).all(...args) as Array<{detail:string}>).map(r=>r.detail).join(' ');
  expect(plan("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.rootId')=? ORDER BY rowid DESC LIMIT 1",'xv','x')).toContain('idx_coordinator_lifecycle_root');
  expect(plan("SELECT operation_id,receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')=? ORDER BY rowid DESC LIMIT 16",'xv','x')).toContain('idx_coordinator_lifecycle_kind_package');
  expect(id).toBeTruthy();
  // configure no longer issues the rig-wide lifecycle-control scan at all.
  const spy2=vi.spyOn(db,'prepare');svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'no-scan-r2'});
  const configureSql=spy2.mock.calls.map(c=>String(c[0]));spy2.mockRestore();
  expect(configureSql.some(q=>q.includes("SELECT operation_id FROM coordinator_operations WHERE rig_id=? AND kind='coordinator-lifecycle-control'"))).toBe(false);
 });

 const chainRow=(id:string,root:string,kind:string,semanticKey:string,rig='xv')=>db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run(rig,id,'coordinator-lifecycle-control',JSON.stringify({kind,queueId:id,rootId:root,semanticKey}),'x');
 it('exact-subject lookup returns the live link of a mixed-binding chain, validates the subject and scopes the rig',()=>{
  const kind='acceptance',key='q:d',root=(svc as any).dutyRootId('xv',kind,key);
  const l1=root,l2=(svc as any).dutySuccessorId(root,l1,'lead-g1:1'),l3=(svc as any).dutySuccessorId(root,l2,'lead-g2:3'),l4=(svc as any).dutySuccessorId(root,l3,'lead-g3:9');
  chainRow(l1,root,kind,key);chainRow(l2,root,kind,key);chainRow(l3,root,kind,key);   // links bound to three different generations and epochs
  expect((svc as any).liveDutyLink('xv',kind,key)).toBe(l3);
  chainRow(l4,root,kind,key);expect((svc as any).liveDutyLink('xv',kind,key)).toBe(l4);
  chainRow('qitem-other-subject','qitem-other-root','acceptance','other:d');expect((svc as any).liveDutyLink('xv',kind,key)).toBe(l4);
  expect((svc as any).liveDutyLink('xv',kind,'missing')).toBeNull();expect((svc as any).liveDutyLink('other',kind,key)).toBeNull();
  const forged=(svc as any).dutyRootId('xv','acceptance','forged:d');chainRow('qitem-forged',forged,'recovery','forged:d');
  expect((svc as any).liveDutyLink('xv','acceptance','forged:d')).toBeNull();           // right root, different immutable subject: refused
 });

 it('a link bound to a superseded holder epoch is not the live duty: nothing is captured and nothing breaks',async()=>{
  const {id}=await acceptanceDuty();db.prepare('UPDATE coordinator_authority SET epoch=epoch+1').run();
  expect(svc.dutyFacts(id).superseded).toBe(true);accept();
  expect(completion(id)).toBeNull();expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeTruthy();
 });

 it('the retirement sweep is rig-scoped before its limit, so 150 older items of another rig never starve a real duty',async()=>{
  const {id,deadline}=await acceptanceDuty();
  vi.spyOn(svc as any,'recordCompletionObservation').mockImplementation(()=>{});accept();vi.restoreAllMocks();
  pass(deadline-clock+1);
  db.prepare("INSERT INTO queue_items(qitem_id,source_session,destination_session,state,body,ts_created,ts_updated) VALUES ('other-baton','worker@other','worker@other','in-progress','b',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  db.prepare("INSERT INTO coordinator_authority VALUES ('other','other-baton','worker@other','worker-g1',1,?,'active','op-other','[\"worker@other\",\"worker@other\"]',NULL)").run(clock+3600000);
  const iso=new Date(clock-1000).toISOString(),past=new Date(clock-5000).toISOString();
  for(let n=0;n<150;n++){const fid='qitem-coordination-lifecycle-'+'0'.repeat(14)+'ot'+String(n).padStart(4,'0');
   db.prepare("INSERT INTO queue_items(qitem_id,source_session,destination_session,state,body,ts_created,ts_updated,claimed_at,claimed_by_generation_uuid,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?)").run(fid,'watchdog@system','lead@xv','in-progress','fake',past,past,past,'lead-g1',iso);
   db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('other',fid,'coordinator-lifecycle-control',JSON.stringify({kind:'acceptance',queueId:fid,rootId:fid,semanticKey:'fake:'+n,packageKey:'fake',recipient:'lead@xv'}),'x');}
  const retirement=()=>db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(id);
  svc.reconcile('lead@xv','lead-g1','xv');expect(retirement()).toBeTruthy();   // a LIMIT 100 applied before the rig filter would have starved it
  expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE rig_id='other' AND kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement'").get() as {n:number}).n).toBe(0);   // the other rig was not touched
 });

 it('inside one rig, 150 older ineligible claimed-expired duties are walked fairly across passes until the real duty is reached',async()=>{
  const {id,deadline}=await acceptanceDuty();
  vi.spyOn(svc as any,'recordCompletionObservation').mockImplementation(()=>{});accept();vi.restoreAllMocks();pass(deadline-clock+1);
  const iso=new Date(clock-1000).toISOString(),past=new Date(clock-5000).toISOString();
  for(let n=0;n<150;n++){const fid='qitem-coordination-lifecycle-'+'0'.repeat(14)+'xv'+String(n).padStart(4,'0');
   db.prepare("INSERT INTO queue_items(qitem_id,source_session,destination_session,state,body,ts_created,ts_updated,claimed_at,claimed_by_generation_uuid,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?)").run(fid,'watchdog@system','lead@xv','in-progress','fake',past,past,past,'lead-g1',iso);
   db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv',fid,'coordinator-lifecycle-control',JSON.stringify({kind:'acceptance',queueId:fid,rootId:fid,semanticKey:'fake:'+n,packageKey:'fake',recipient:'lead@xv'}),'x');}
  const retirement=()=>db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(id);
  for(let n=0;n<3&&!retirement();n++)svc.reconcile('lead@xv','lead-g1','xv');
  expect(retirement()).toBeTruthy();
  const plan=(db.prepare("EXPLAIN QUERY PLAN SELECT q.qitem_id FROM queue_items q INDEXED BY idx_queue_items_state JOIN coordinator_operations o ON o.rig_id=? AND o.operation_id=q.qitem_id AND o.kind='coordinator-lifecycle-control' WHERE q.state IN ('in-progress','blocked') AND q.qitem_id>? LIMIT 100").all('xv','') as Array<{detail:string}>).map(r=>r.detail).join(' ');
  expect(plan).toContain('idx_queue_items_state');
 });

 // Kinds whose postcondition embeds admission validity (recovery, materialization, admission-refresh) need an admission that outlives the duty deadline.
 const long=(t:CoordinationTask):CoordinationTask=>({...t,admission:{...t.admission,validUntil:clock+7200000}});
 it('recovery: the owned-recovery fact freezes completion before the deadline and closes after a missed tick; a wrong duty id refuses',async()=>{
  configure(normal().map(long));const original=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',original,'incomplete-return');
  const assessment=new RuntimeOutcomeAssessment(repo,{},()=>clock);repo.coordinatorAuthority.runtimeOutcomeAssessment=assessment;
  const policy={rigId:'xv',revision:'fixture-enforce',mode:'enforce'};db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv','fixture-policy','runtime-outcome-policy',JSON.stringify(policy),'fixture');
  db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv','fixture-incomplete','runtime-outcome-recovery',JSON.stringify({packageKey:'product',dispositionId:'incomplete-return',policyRevision:policy.revision}),'fixture');
  const results=svc.reconcile('lead@xv','lead-g1','xv'),duty=results.find(r=>r.key==='acceptance:product')!,repair=results.find(r=>r.key==='repair')!;deliver();claimDuty(duty.queueId!,'lead@xv','lead-g1');
  const deadline=svc.lifecycleControlReceipt(duty.queueId!)!.deadline,input={rigId:'xv',dutyQueueId:duty.queueId!,recoveryPackageKey:'repair',recoveryQueueId:repair.queueId!,evidenceRef:'actual/recovery-pickup.json'};
  repo.claim({qitemId:repair.queueId!,destinationSession:'architect@xv',identityProvenance:'transport:v1'});deliver();
  expect(()=>svc.recordLifecycleRecovery('lead@xv','lead-g1',{...input,dutyQueueId:'qitem-not-a-duty'})).toThrow();expect(completion(duty.queueId!)).toBeNull();
  svc.recordLifecycleRecovery('lead@xv','lead-g1',input);
  expect(completion(duty.queueId!)).toMatchObject({queueId:duty.queueId,at:clock});expect(completion(duty.queueId!).at).toBeLessThan(deadline);
  pass(deadline-clock+1);expect(svc.dutyFacts(duty.queueId!)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(duty.queueId!,'lead@xv','lead-g1');expect(repo.getById(duty.queueId!)?.state).toBe('done');
 });

 it('materialization: the plan revision that plans the package freezes its duty (keyed capture) and the duty closes after a missed tick',async()=>{
  const historical=configure(normal()),q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;await finishTyped('product','builder@xv',q,'historical-return');svc.accept('lead@xv','lead-g1','xv','product','historical-return','actual/accepted-parent.md');
  clock+=60001;vi.setSystemTime(clock);refresh();db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+3600000);
  const next=long(task('new-frontier','reviewer@xv',{predecessors:[{queueId:q,dispositionId:'historical-return'}]})),backup=long(task('z-new-backup','peer@xv',{recoveryFor:'new-frontier'}));
  for(const t of [next,backup])repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  const intake=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='materialization:new-frontier')!;
  repo.attachTransport({send:async(session,_text,opts)=>{repo.coordinatorAuthority.assertManagedSend(opts?.actorSession,session,opts?.queueAssignmentId);return {ok:true,verified:true};}});await svc.deliverCommitted();
  claimDuty(intake.queueId!,'operator-agent@kernel','operator-agent-g1');const deadline=svc.lifecycleControlReceipt(intake.queueId!)!.deadline;
  expect(()=>closeDuty(intake.queueId!,'operator-agent@kernel','operator-agent-g1')).toThrow('Exact native acceptance');
  vi.spyOn(svc as any,'observeDuty').mockImplementation(()=>{});            // disable the legacy full scan: only the keyed capture can freeze it
  expect(completion(intake.queueId!)).toBeNull();
  svc.configure('operator-agent@kernel','operator-agent-g1',{...historical,revision:'materialized-r2',tasks:[...historical.tasks,next,backup]});
  expect(completion(intake.queueId!)).toMatchObject({queueId:intake.queueId,at:clock});expect(completion(intake.queueId!).at).toBeLessThan(deadline);
  pass(deadline-clock+1);expect(svc.dutyFacts(intake.queueId!)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(intake.queueId!,'operator-agent@kernel','operator-agent-g1');expect(repo.getById(intake.queueId!)?.state).toBe('done');
 });

 it('a plan revision that completes no duty, or touches a different package, writes no observation', async()=>{
  const historical=configure(normal());
  const before=db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='duty-completion-observation'").get() as {n:number};
  svc.configure('operator-agent@kernel','operator-agent-g1',{...historical,revision:'same-tasks-r2'});
  expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='duty-completion-observation'").get() as {n:number}).n).toBe(before.n);
 });

 it('admission-refresh: the plan revision that refreshes a task freezes its duty (keyed capture), only that task, and closes after a missed tick',()=>{
  configure([task('expired','reviewer@xv'),task('expired-repair','architect@xv',{recoveryFor:'expired'})]);
  const admittedAt=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!.admission.validUntil;repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-stale-window');clock+=admittedAt+2-clock;vi.setSystemTime(clock);refresh();
  svc.reconcile('lead@xv','lead-g1','xv');
  const row=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='expired'").get() as {receipt:string},r=JSON.parse(row.receipt);
  deliver();claimDuty(r.queueId,'operator-agent@kernel','operator-agent-g1');
  const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!,refreshed={...current,admission:{generation:r.ownerGeneration,configurationDigest:r.liveConfigurationDigest,qualificationRef:'<new-qualification-proof>',capacityRef:'<new-capacity-proof>',effortRef:'<new-effort-proof>',validUntil:clock+7200000}};
  vi.spyOn(svc as any,'observeDuty').mockImplementation(()=>{});            // disable the legacy full scan: only the keyed capture can freeze it
  expect(completion(r.queueId)).toBeNull();
  svc.configure('operator-agent@kernel','operator-agent-g1',{...svc.plan('xv')!,revision:'r2',tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?refreshed:t)});
  expect(completion(r.queueId)).toMatchObject({queueId:r.queueId,at:clock});expect(completion(r.queueId).at).toBeLessThan(r.deadline);
  pass(r.deadline-clock+1);expect(svc.dutyFacts(r.queueId)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(r.queueId,'operator-agent@kernel','operator-agent-g1');expect(repo.getById(r.queueId)?.state).toBe('done');
 });


 const staleRefresh=()=>{
  configure([task('expired','reviewer@xv'),task('expired-repair','architect@xv',{recoveryFor:'expired'})]);
  const admittedAt=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!.admission.validUntil;repo.coordinatorAuthority.renew('lead@xv',token,600000,'renew-stale-window');clock+=admittedAt+2-clock;vi.setSystemTime(clock);refresh();
  svc.reconcile('lead@xv','lead-g1','xv');
 };
 const refreshDuties=()=>(db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='admission-refresh' AND json_extract(receipt,'$.packageKey')='expired' ORDER BY rowid").all() as Array<{receipt:string}>).map(r=>JSON.parse(r.receipt));
 const refreshPlan=(r:any,revision:string)=>{const current=svc.plan('xv')!.tasks.find(t=>t.key==='expired')!,refreshed={...current,admission:{generation:repo.coordinatorAuthority.generation('reviewer@xv')!,configurationDigest:svc.configurationDigest('reviewer@xv')!,qualificationRef:'<new-q>'+revision,capacityRef:'<new-c>'+revision,effortRef:'<new-e>'+revision,validUntil:clock+7200000}};return {...svc.plan('xv')!,revision,tasks:svc.plan('xv')!.tasks.map(t=>t.key==='expired'?refreshed:t)};};
 it('admission-refresh after the owner configuration moved: the recorded duty that matches the live facts is captured, the older one is not, and no authority is relaxed',()=>{
  staleRefresh();const first=refreshDuties()[0];
  db.prepare("UPDATE nodes SET model='different-model' WHERE logical_id='reviewer'").run();svc.reconcile('lead@xv','lead-g1','xv');
  const duties=refreshDuties();expect(duties.length).toBe(2);const second=duties[1];expect(second.liveConfigurationDigest).toBe(svc.configurationDigest('reviewer@xv'));expect(second.liveConfigurationDigest).not.toBe(first.liveConfigurationDigest);
  deliver();claimDuty(second.queueId,'operator-agent@kernel','operator-agent-g1');
  svc.configure('operator-agent@kernel','operator-agent-g1',refreshPlan(second,'r-moved'));
  expect(completion(second.queueId)).toMatchObject({queueId:second.queueId,at:clock});expect(completion(second.queueId).at).toBeLessThan(second.deadline);
  expect(completion(first.queueId)).toBeNull();expect(svc.dutyFacts(first.queueId)).toMatchObject({complete:false});   // issued under the old configuration: still incomplete
  pass(second.deadline-clock+1);expect(svc.dutyFacts(second.queueId)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(second.queueId,'operator-agent@kernel','operator-agent-g1');expect(repo.getById(second.queueId)?.state).toBe('done');
 });


 it('admission-refresh: more than 16 newer duties of other subjects for the same package cannot hide the completing duty (the exact subject is in the query, before the limit)',()=>{
  staleRefresh();const real=refreshDuties()[0];
  // 20 NEWER admission-refresh controls for the same package but other task intents / stale expiries (distractor subjects).
  for(let n=0;n<20;n++){const id='qitem-coordination-lifecycle-distractor-'+String(n).padStart(3,'0');
   db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv',id,'coordinator-lifecycle-control',JSON.stringify({kind:'admission-refresh',queueId:id,rootId:id,semanticKey:'d'+n,packageKey:'expired',taskKey:'expired',owner:'reviewer@xv',taskIntentDigest:'other-intent-'+n,priorAdmission:{validUntil:1000+n}}),'x');}
  expect(refreshDuties().length).toBe(21);                                     // the real duty is the OLDEST of 21 for this package
  deliver();claimDuty(real.queueId,'operator-agent@kernel','operator-agent-g1');
  svc.configure('operator-agent@kernel','operator-agent-g1',refreshPlan(real,'r-distractors'));
  expect(completion(real.queueId)).toMatchObject({queueId:real.queueId,at:clock});expect(completion(real.queueId).at).toBeLessThan(real.deadline);
  expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='duty-completion-observation' AND operation_id LIKE 'duty-completion:qitem-coordination-lifecycle-distractor-%'").get() as {n:number}).n).toBe(0);   // no distractor was touched
  pass(real.deadline-clock+1);expect(svc.dutyFacts(real.queueId)).toMatchObject({complete:true,close:true,act:false,expired:true});
  closeDuty(real.queueId,'operator-agent@kernel','operator-agent-g1');expect(repo.getById(real.queueId)?.state).toBe('done');
 });

 it('admission-refresh when the configuration moved before any new duty was issued: a refresh against the live facts does not complete or capture the older recorded duty',()=>{
  staleRefresh();const first=refreshDuties()[0];
  db.prepare("UPDATE nodes SET model='different-model' WHERE logical_id='reviewer'").run();     // moved, no reconcile yet, no second duty
  svc.configure('operator-agent@kernel','operator-agent-g1',refreshPlan(first,'r-live'));
  expect(refreshDuties().length).toBe(1);expect(completion(first.queueId)).toBeNull();expect(svc.dutyFacts(first.queueId)).toMatchObject({complete:false});
 });

 it('historical edge: a complete, expired, claimed duty with no timely observation gets the accountable failure-only retirement, without back-dating or reapplying the effect',async()=>{
  const {id,deadline}=await acceptanceDuty();
  vi.spyOn(svc as any,'recordCompletionObservation').mockImplementation(()=>{});accept();vi.restoreAllMocks();   // the pre-fix production state
  const acceptance=db.prepare("SELECT * FROM coordinator_operations WHERE kind='coordination-accept'").all();
  pass(deadline-clock+1);
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,expired:true,close:false,retired:false,failedByRecipient:false});
  const retirementsBefore=db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement'").get() as {n:number};
  svc.reconcile('lead@xv','lead-g1','xv');
  const ret=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(id) as {receipt:string}|undefined;
  expect(retirementsBefore.n).toBe(0);expect(ret).toBeTruthy();const r=JSON.parse(ret!.receipt);
  expect(r).toMatchObject({kind:'lifecycle-retirement',targetQueueId:id,recipient:'lead@xv'});
  expect(JSON.parse(repo.getById(r.queueId)!.body)).toMatchObject({action:'report-own-expired-administrative-duty-outcome',targetQueueId:id,grantsAuthority:false});
  expect(completion(id)).toBeNull();expect(repo.getById(id)?.state).toBe('in-progress');                        // nothing back-dated, nothing closed
  expect(db.prepare("SELECT * FROM coordinator_operations WHERE kind='coordination-accept'").all()).toEqual(acceptance); // the success receipt is untouched
  svc.reconcile('lead@xv','lead-g1','xv');
  expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement' AND json_extract(receipt,'$.targetQueueId')=?").get(id) as {n:number}).n).toBe(1); // one, not repeated
  expect(()=>closeDuty(id,'lead@xv','lead-g1')).toThrow('Exact native acceptance');                          // still no late success rescue
  deliver();claimDuty(r.queueId,'lead@xv','lead-g1');
  repo.update({qitemId:id,actorSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1',state:'failed',transitionNote:'own expiry disposition'});
  expect(repo.getById(id)?.state).toBe('failed');expect(svc.dutyFacts(r.queueId)).toMatchObject({complete:true});expect(svc.dutyFacts(id)).toMatchObject({failedByRecipient:true,retired:true});
  // The native terminal report is itself a completing fact: its retirement duty is frozen at that transaction, before its own deadline.
  expect(completion(r.queueId)).toMatchObject({queueId:r.queueId,at:clock});expect(completion(r.queueId).at).toBeLessThan(r.deadline);
  pass(r.deadline-clock+1);expect(svc.dutyFacts(r.queueId)).toMatchObject({complete:true,close:true,expired:true});closeDuty(r.queueId,'lead@xv','lead-g1');expect(repo.getById(r.queueId)?.state).toBe('done');
  expect(db.prepare("SELECT * FROM coordinator_operations WHERE kind='coordination-accept'").all()).toEqual(acceptance);
 });

 it('the historical retirement stages nothing for an incomplete, unexpired, unclaimed or already closable duty',async()=>{
  const {id,deadline}=await acceptanceDuty();
  svc.reconcile('lead@xv','lead-g1','xv');                                                    // live, incomplete
  const retired=()=>(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='lifecycle-retirement'").get() as {n:number}).n;
  expect(retired()).toBe(0);accept();pass(deadline-clock+1);svc.reconcile('lead@xv','lead-g1','xv');
  expect(svc.dutyFacts(id)).toMatchObject({complete:true,close:true});expect(retired()).toBe(0);  // closable duty: nothing to retire
 });
});
