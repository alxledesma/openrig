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
import {RuntimeOutcomeAssessment} from '../src/domain/runtime-outcome-assessment.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

describe('coordinator assignment wake outcome regression',()=>{

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
 it('exact initial producer wake is contained only after native claim; UNKNOWN bytes and custody remain intact',async()=>{
  const id=await assignment(),before=notice(id);expect(before).toMatchObject({delivery_state:'indeterminate',sender_session:'lead@xv',destination_session:'builder@xv',audit_pointer:id});expect(svc.noticeOutcomeContained('xv',before)).toBe(false);nativeClaim(id,'builder@xv');reconcile();
  const saved=outcome(before.outbox_id)!;expect(JSON.parse(saved.receipt)).toMatchObject({taskQueueId:id,recipient:'builder@xv',recipientGeneration:'builder-g1',deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false});expect(svc.noticeOutcomeContained('xv',notice(id))).toBe(true);expect(notice(id)).toEqual(before);expect(repo.getById(id)).toMatchObject({state:'in-progress',destinationSession:'builder@xv'});
  // Actual terminal return can stage and claim the Lead acceptance duty despite
  // its own sender debt, without manufacturing a transport delivery receipt.
  repo.attachTransport({send:async()=>({ok:true,verified:true})});await finishTyped('product','builder@xv',id,'result');const duty=reconcile().find(r=>r.key==='acceptance:product')!;await svc.deliverCommitted();expect(svc.dutyFacts(duty.queueId!)).toMatchObject({claim:true});nativeClaim(duty.queueId!,'lead@xv');expect(notice(id)).toEqual(before);expect(svc.noticeOutcomeContained('xv',before)).toBe(true);
 });
 it('unclaimed UNKNOWN wake stays debt with no mutation or receipt',async()=>{const id=await assignment(),before=notice(id);reconcile();expect(outcome(before.outbox_id)).toBeUndefined();expect(svc.noticeOutcomeContained('xv',before)).toBe(false);expect(notice(id)).toEqual(before);expect(repo.getById(id)?.state).toBe('pending');});
 it.each(['pending','sending'] as const)('native pickup never contains %s transport state',async(state)=>{const id=await assignment(true);db.prepare('UPDATE outbox_entries SET delivery_state=? WHERE outbox_id=?').run(state,'wake-intent-'+id);nativeClaim(id,'builder@xv');const before=notice(id);reconcile();expect(outcome(before.outbox_id)).toBeUndefined();expect(svc.noticeOutcomeContained('xv',before)).toBe(false);expect(notice(id)).toEqual(before);});
 it('abandoned sending becomes UNKNOWN through recovery, never inferred delivered',async()=>{const id=await assignment(true);db.prepare("UPDATE outbox_entries SET delivery_state='sending' WHERE outbox_id=?").run('wake-intent-'+id);nativeClaim(id,'builder@xv');reconcile();expect(outcome('wake-intent-'+id)).toBeUndefined();expect(outbox.reconcileAbandonedSending('wake-intent-')).toBe(1);const before=notice(id);reconcile();expect(svc.noticeOutcomeContained('xv',before)).toBe(true);expect(notice(id)).toEqual(before);});
 it.each(['non-native','retired-claim','sender','generation','body','assignment-body','contract','pointer','foreign-rig'] as const)('refuses %s proof drift',async(kind)=>{
  const id=await assignment();if(kind==='non-native')repo.claim({qitemId:id,destinationSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'cli:user'});else nativeClaim(id,'builder@xv');
  if(kind==='retired-claim')db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
  if(kind==='sender')db.prepare("UPDATE outbox_entries SET sender_session='peer@xv' WHERE outbox_id=?").run('wake-intent-'+id);
  if(kind==='generation')db.prepare("UPDATE outbox_entries SET body=replace(body,'gen lead-g1','gen retired') WHERE outbox_id=?").run('wake-intent-'+id);
  if(kind==='body')db.prepare("UPDATE outbox_entries SET body=body||'drift' WHERE outbox_id=?").run('wake-intent-'+id);
  if(kind==='assignment-body')db.prepare("UPDATE queue_items SET body='drift' WHERE qitem_id=?").run(id);
  if(kind==='contract')db.prepare("UPDATE coordinator_packages SET contract=replace(contract,'builder@xv','reviewer@xv') WHERE rig_id='xv' AND package_key='product'").run();
  if(kind==='pointer')db.prepare("UPDATE outbox_entries SET audit_pointer='baton' WHERE outbox_id=?").run('wake-intent-'+id);
  const before=notice(id);reconcile();expect(svc.noticeOutcomeContained(kind==='foreign-rig'?'other':'xv',before)).toBe(false);if(kind!=='foreign-rig')expect(outcome(before.outbox_id)).toBeUndefined();expect(notice(id)).toEqual(before);
 });
 it('a recorded receipt cannot survive later immutable notice drift',async()=>{const id=await assignment();nativeClaim(id,'builder@xv');reconcile();expect(outcome('wake-intent-'+id)).toBeTruthy();db.prepare("UPDATE outbox_entries SET body=body||'drift' WHERE outbox_id=?").run('wake-intent-'+id);expect(svc.noticeOutcomeContained('xv',notice(id))).toBe(false);});
 it('epoch-2 holder re-wake binds actual authority receipt rather than epoch-1 assignment owner',async()=>{
  const id=await assignment(true);repo.coordinatorAuthority.transfer('lead@xv','lead-g1',{expected:token,oldOwner:'lead@xv',recipient:'peer@xv',recipientGeneration:'peer-g1',operationId:'transfer',leaseMs:60000});repo.coordinatorAuthority.acknowledge('peer@xv',{rigId:'xv',epoch:2,generation:'peer-g1'},{operationId:'peer-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  expect(repo.stageCoordinatorAssignmentWake({rigId:'xv',epoch:2,generation:'peer-g1',actor:'peer@xv',queueId:id,recipient:'builder@xv',recipientGeneration:'builder-g1',now:clock})).toBe(true);repo.attachTransport({send:async()=>({ok:true,verified:false})});await svc.deliverCommitted();nativeClaim(id,'builder@xv');svc.reconcile('peer@xv','peer-g1','xv');
  const row=db.prepare("SELECT * FROM outbox_entries WHERE audit_pointer=? AND outbox_id LIKE 'wake-intent-coordinator-%'").get(id) as any;expect(row.sender_session).toBe('peer@xv');expect(svc.noticeOutcomeContained('xv',row)).toBe(true);expect(JSON.parse(outcome(row.outbox_id)!.receipt)).toMatchObject({deliveryConclusion:'unknown'});
  const tags=JSON.parse(row.tags);const forged=JSON.parse(tags[1]);forged.epoch=3;tags[1]=JSON.stringify(forged);db.prepare('UPDATE outbox_entries SET tags=? WHERE outbox_id=?').run(JSON.stringify(tags),row.outbox_id);expect(svc.noticeOutcomeContained('xv',db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(row.outbox_id))).toBe(false);
 });

 it('accountable intake UNKNOWN wake is contained only by its actual native Operator pickup',async()=>{
  configure(normal(),{dispatchRestrictions:[{session:'builder@xv',generation:'builder-g1',packageKeys:['product'],validUntil:clock+1,evidenceRef:'actual/scope'}]});clock+=2;vi.setSystemTime(clock);refresh();repo.attachTransport({send:async()=>({ok:true,verified:false})});reconcile();await svc.deliverCommitted();
  const q=db.prepare("SELECT qitem_id FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.action')='resolve-exact-coordination-task-hold' AND json_extract(body,'$.reason')='dispatch-scope-expired' ORDER BY rowid LIMIT 1").get() as {qitem_id:string};expect(q).toBeTruthy();const before=notice(q.qitem_id);expect(svc.noticeOutcomeContained('xv',before)).toBe(false);nativeClaim(q.qitem_id,'operator-agent@kernel');reconcile();expect(svc.noticeOutcomeContained('xv',before)).toBe(true);expect(notice(q.qitem_id)).toEqual(before);
 });
 it('registered terminal-return control UNKNOWN pointer is contained by exact native worker pickup',async()=>{
  const original=await assignment();nativeClaim(original,'builder@xv');repo.update({qitemId:original,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  const duty=reconcile().find(r=>r.key==='terminal-return:product')!;expect(duty.state).toBe('pending-native-terminal-return');await svc.deliverCommitted();const before=notice(duty.queueId!);expect(svc.noticeOutcomeContained('xv',before)).toBe(false);nativeClaim(duty.queueId!,'builder@xv');reconcile();expect(svc.noticeOutcomeContained('xv',before)).toBe(true);expect(notice(duty.queueId!)).toEqual(before);expect(repo.getById(original)?.state).toBe('done');expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE queue_id=?").get(original)).toEqual({disposition_id:null});
 });

 it('rig-keyed recorder pages beyond 200 unclaimed producer wakes without retrying any UNKNOWN row',async()=>{
  configure(normal());repo.attachTransport({send:async()=>({ok:true,verified:false})});let last='';
  for(let i=0;i<201;i++){const key='page-'+i,t=task(key);admit([t]);last='qitem-coordination-'+digest('xv:'+key).slice(0,24);await repo.create({qitemId:last,sourceSession:'lead@xv',destinationSession:'builder@xv',body:t.body,dispatch:{token,packageKey:key},identityProvenance:'system:operator-authorized-coordination',nudge:true});}
  const before=db.prepare("SELECT * FROM outbox_entries ORDER BY rowid").all();nativeClaim(last,'builder@xv');reconcile();expect(outcome('wake-intent-'+last)).toBeTruthy();expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='assignment-wake-outcome'").get()).toEqual({n:1});expect(db.prepare("SELECT * FROM outbox_entries ORDER BY rowid").all().slice(0,before.length)).toEqual(before);
 });
});
