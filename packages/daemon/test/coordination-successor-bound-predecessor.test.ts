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

describe('successor bound predecessor — genuine acceptance releases dependent',()=>{

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

 function bound(key:string){const pkg=db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv',key) as {contract_hash:string};return {packageKey:key,contractHash:pkg.contract_hash,queueId:'qitem-coordination-'+digest('xv:'+key).slice(0,24)};}
 function prebound(){const tasks=[...normal(),task('next','reviewer@xv'),task('next-repair','architect@xv',{recoveryFor:'next'})];admit(tasks);tasks[2]!.predecessors=[bound('product')];configure(tasks);return tasks;}
 function semanticIncomplete(){
  // Same qualified mock provider contract as runtime-outcome-assessment.test.ts.
  const c={schemaVersion:1,mode:'observe',enabled:true,primary:'local',providers:{local:{adapter:'laya',endpoint:'http://127.0.0.1:18091/v1/systemone',model:'typed-decisions',acceptedModels:['qualified-fixture'],timeoutMs:100,allowedData:['private'],maxResponseBytes:16384,capabilities:{schema:'systemone.v1',primitives:['choice'],maxStateChars:10000,maxQuestions:10,maxOptions:10,inputCoverage:'reported'},threshold:{minTopProbability:0.8,calibrationId:'synthetic-test'}}}};
  const fetchImpl=vi.fn(async()=>new Response(JSON.stringify({model:'qualified-fixture',answers:{outcome:{type:'choice',choice:'no',probabilities:{yes:.05,no:.9,unknown:.05}}},usage:{input_tokens:150,output_tokens:0,truncated:false}}),{status:200}));
  const assessment=new RuntimeOutcomeAssessment(repo,{fetchImpl},()=>clock);repo.coordinatorAuthority.runtimeOutcomeAssessment=assessment;
  const policy={rigId:'xv',revision:'fixture-enforce',mode:'enforce' as const,operatorGeneration:'operator-agent-g1',dataClass:'private' as const,allowPaid:false,allowUnqualifiedNegativeAdvice:false,adapterConfig:c,qualification:{ref:'synthetic-test-only',providerConfigDigest:digest(JSON.stringify(c)),validUntil:clock+60000}};
  assessment.configure('operator-agent@kernel','operator-agent-g1',policy);
  db.prepare('INSERT INTO coordinator_operations VALUES (?,?,?,?,?)').run('xv','fixture-incomplete','runtime-outcome-recovery',JSON.stringify({packageKey:'product',dispositionId:'incomplete-return',policyRevision:policy.revision}),'fixture');return assessment;
 }
 async function originalReturned(state:'done'|'failed'='done'){const first=reconcile().find(r=>r.key==='product')!;expect(first.state).toBe('pending-pickup');await svc.deliverCommitted();await finishTyped('product','builder@xv',first.queueId!,'incomplete-return',state);return first.queueId!;}
 async function claimAcceptance(packageKey:string){const duty=reconcile().find(r=>r.key==='acceptance:'+packageKey)!;expect(duty.state).toBe('pending-native-acceptance');await svc.deliverCommitted();nativeClaim(duty.queueId!,'lead@xv');return duty.queueId!;}
 async function acceptOriginal(){const duty=await claimAcceptance('product');svc.accept('lead@xv','lead-g1','xv','product','incomplete-return','actual/product-accepted.md');expect(repo.getById(duty)?.state).toBe('done');await svc.deliverCommitted();}
 function nextAssignment(){return db.prepare("SELECT * FROM coordinator_assignments WHERE rig_id='xv' AND package_key='next'").get() as any;}
 it('genuine acceptance creates real prebound B and freezes exact resolved receipt without reconfigure',async()=>{
  const tasks=prebound(),original=await originalReturned();expect(reconcile().find(r=>r.key==='next')).toMatchObject({state:'held',reason:'predecessor-disposition'});await acceptOriginal();
  const next=nextAssignment();expect(next.queue_id).toBe('qitem-coordination-'+digest('xv:next').slice(0,24));expect(repo.getById(next.queue_id)).toMatchObject({destinationSession:'reviewer@xv',state:'pending',body:'next'});expect(notice(next.queue_id)).toMatchObject({sender_session:'lead@xv',destination_session:'reviewer@xv',audit_pointer:next.queue_id});
  const r=JSON.parse((db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id='coordination-predecessor-resolution:xv:next'").get() as {receipt:string}).receipt);
  expect(r).toEqual({predecessors:[{...tasks[2]!.predecessors[0],dispositionId:'incomplete-return',acceptOperationId:'coordination-accept:product'}],planRevision:'r1'});
  expect(r.predecessors[0].queueId).toBe(original);nativeClaim(next.queue_id,'reviewer@xv');expect(reconcile().find(r=>r.key==='next')?.state).toBe('picked-up');
  const counts=db.prepare("SELECT (SELECT count(*) FROM coordinator_assignments WHERE package_key='next') assignments,(SELECT count(*) FROM outbox_entries WHERE audit_pointer=?) wakes").get(next.queue_id);
  svc.accept('lead@xv','lead-g1','xv','product','incomplete-return','actual/product-accepted.md');reconcile();expect(db.prepare("SELECT (SELECT count(*) FROM coordinator_assignments WHERE package_key='next') assignments,(SELECT count(*) FROM outbox_entries WHERE audit_pointer=?) wakes").get(next.queue_id)).toEqual(counts);
 });
 it('accepted repair permits genuine A acceptance to release ordinary B while semantic marker remains',async()=>{
  prebound();await originalReturned();const assessment=semanticIncomplete();const results=reconcile();expect(results.find(r=>r.key==='product')?.state).toBe('recovery-required:semantic-incomplete');expect(results.find(r=>r.key==='acceptance:product')?.state).toBe('pending-native-acceptance');expect(results.find(r=>r.key==='next')).toMatchObject({state:'held',reason:'predecessor-disposition'});
  const repair=results.find(r=>r.key==='repair')!;expect(repair.state).toBe('pending-pickup');const duty=await claimAcceptance('product');nativeClaim(repair.queueId!,'architect@xv');
  svc.recordLifecycleRecovery('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty,recoveryPackageKey:'repair',recoveryQueueId:repair.queueId!,evidenceRef:'actual/recovery-pickup.json'});
  await finishTyped('repair','architect@xv',repair.queueId!,'repair-return');await assessment.drain('xv');await claimAcceptance('repair');svc.accept('lead@xv','lead-g1','xv','repair','repair-return','actual/repair-accepted.md');
  expect(nextAssignment()).toBeUndefined();expect(assessment.requiresRecovery('xv','product')).toBe(true);svc.accept('lead@xv','lead-g1','xv','product','incomplete-return','actual/product-accepted.md');expect(nextAssignment()).toBeTruthy();
 });
 it('classifier-required unresolved A still holds real B and refuses acceptance',async()=>{
  prebound();await originalReturned();semanticIncomplete();const results=reconcile();expect(results.find(r=>r.key==='acceptance:product')?.state).toBe('pending-native-acceptance');expect(results.find(r=>r.key==='product')?.state).toBe('recovery-required:semantic-incomplete');expect(results.find(r=>r.key==='repair')?.state).toBe('pending-pickup');expect(results.find(r=>r.key==='next')).toMatchObject({state:'held',reason:'predecessor-disposition'});
  expect(()=>svc.accept('lead@xv','lead-g1','xv','product','incomplete-return','actual/no-repair.md')).toThrow('Incomplete/unverified');expect(nextAssignment()).toBeUndefined();
 });
 it('failed A cannot be satisfied by accepted recovery',async()=>{
  prebound();await originalReturned('failed');const repair=reconcile().find(r=>r.key==='repair')!;await svc.deliverCommitted();await finishTyped('repair','architect@xv',repair.queueId!,'repair-return');await claimAcceptance('repair');svc.accept('lead@xv','lead-g1','xv','repair','repair-return','actual/repair-accepted.md');expect(reconcile().find(r=>r.key==='next')).toMatchObject({state:'held',reason:'predecessor-disposition'});expect(nextAssignment()).toBeUndefined();expect(()=>svc.accept('lead@xv','lead-g1','xv','product','incomplete-return','actual/failed.md')).toThrow('successful attributed');
 });
 it('busy B is dispatched on next idle observer tick without replan',async()=>{
  prebound();await originalReturned();samples.get('reviewer@xv')!.state.activity='working';samples.get('reviewer@xv')!.witness!.activity='working';await acceptOriginal();expect(nextAssignment()).toBeUndefined();refresh();
  db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')").run(new Date(clock).toISOString());svc.supervise('xv','j');expect(nextAssignment()).toBeTruthy();
 });
 it('expired B admission still holds after A acceptance',async()=>{
  const tasks=prebound();await originalReturned();tasks[2]!.admission.validUntil=clock+1;svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,{revision:'short-b'}));clock+=2;vi.setSystemTime(clock);refresh();await acceptOriginal();expect(nextAssignment()).toBeUndefined();expect(reconcile().find(r=>r.key==='next')).toMatchObject({state:'held',reason:'current-admission-required'});
 });
 it('resolution insert failure rolls back B queue, assignment and wake without losing accepted A',async()=>{
  prebound();await originalReturned();samples.get('reviewer@xv')!.state.activity='working';samples.get('reviewer@xv')!.witness!.activity='working';await acceptOriginal();refresh();
  db.exec("CREATE TEMP TRIGGER refuse_resolution BEFORE INSERT ON coordinator_operations WHEN NEW.kind='coordination-predecessor-resolution' BEGIN SELECT RAISE(ABORT,'resolution-write-failed'); END");expect(()=>reconcile()).toThrow('resolution-write-failed');expect(nextAssignment()).toBeUndefined();expect(repo.getById('qitem-coordination-'+digest('xv:next').slice(0,24))).toBeNull();expect(db.prepare("SELECT 1 FROM outbox_entries WHERE audit_pointer=?").get('qitem-coordination-'+digest('xv:next').slice(0,24))).toBeUndefined();expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id='coordination-accept:product'").get()).toBeTruthy();
 });
 it('legacy exact queue/disposition predecessor keeps its original shape',async()=>{
  configure(normal());const original=await originalReturned();await acceptOriginal();const next=task('next','reviewer@xv',{predecessors:[{queueId:original,dispositionId:'incomplete-return'}]});const tasks=[...svc.plan('xv')!.tasks,next,task('next-repair','architect@xv',{recoveryFor:'next'})];configure(tasks,{revision:'exact-pair'});expect(reconcile().find(r=>r.key==='next')?.state).toBe('pending-pickup');expect(svc.plan('xv')!.tasks.find(t=>t.key==='next')!.predecessors).toEqual([{queueId:original,dispositionId:'incomplete-return'}]);
 });
 it.each(['hash','queue','self','recovery-target','recovery-dependent','cycle','recovery-cycle','retained','instance','extra'] as const)('rejects invalid prebound %s reference before effects',async(kind)=>{
  const tasks=[...normal(),task('next','reviewer@xv'),task('next-repair','architect@xv',{recoveryFor:'next'})];admit(tasks);tasks[2]!.predecessors=[bound('product')];
  if(kind==='hash')(tasks[2]!.predecessors[0] as any).contractHash=digest('wrong');
  if(kind==='queue')(tasks[2]!.predecessors[0] as any).queueId='wrong-instance';
  if(kind==='self')tasks[2]!.predecessors=[bound('next')];
  if(kind==='recovery-target')tasks[2]!.predecessors=[bound('repair')];
  if(kind==='recovery-dependent')tasks[3]!.predecessors=[bound('product')];
  if(kind==='cycle')tasks[0]!.predecessors=[bound('next')];
  if(kind==='recovery-cycle')tasks[0]!.predecessors=[{queueId:bound('repair').queueId,dispositionId:'not-yet-returned'}];
  if(kind==='extra')(tasks[2]!.predecessors[0] as any).dispositionId='forged';
  if(kind==='retained')await repo.create({qitemId:bound('product').queueId,sourceSession:'builder@xv',destinationSession:'lead@xv',body:'retained',nudge:false});
  if(kind==='instance'){await repo.create({qitemId:'different-instance',sourceSession:'lead@xv',destinationSession:'builder@xv',body:'product',dispatch:{token,packageKey:'product'},identityProvenance:'system:operator-authorized-coordination',nudge:false});}
  expect(()=>configure(tasks)).toThrow(/Pre-bound successor reference refused|cycle/);expect(nextAssignment()).toBeUndefined();
 });
 it('assigned B predecessor cannot be rebound',async()=>{prebound();await originalReturned();await acceptOriginal();const tasks=svc.plan('xv')!.tasks.map(t=>t.key==='next'?{...t,predecessors:[]}:t);expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,{revision:'rebound'}))).toThrow();});
 it('same package key accepted in another rig cannot resolve B',async()=>{
  prebound();reconcile();await svc.deliverCommitted();
  for(const name of ['foreign-lead','foreign-peer']){const id=name+'@other';db.prepare("INSERT INTO nodes(id,rig_id,logical_id) VALUES (?,'other',?)").run(id,name);db.prepare('INSERT INTO sessions(id,node_id,session_name) VALUES (?,?,?)').run(id,id,id);db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES (?,?,1,?,'fresh')").run(id,id,name+'-g1');samples.set(id,sample(id));}
  await repo.create({qitemId:'other-baton',sourceSession:'operator-agent@kernel',destinationSession:'foreign-lead@other',body:'coordinate other',nudge:false});repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'other',batonId:'other-baton',owner:'foreign-lead@other',ownerGeneration:'foreign-lead-g1',coordinators:['foreign-lead@other','foreign-peer@other'],leaseMs:60000,operationId:'enable'});repo.coordinatorAuthority.acknowledge('foreign-lead@other',{rigId:'other',epoch:1,generation:'foreign-lead-g1'},{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('other')});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','other','product',{inputDigest:digest('product'),destination:'worker@other',bodyHash:digest('product'),resources:[],returnContract:{destination:'foreign-lead@other',evidenceRequired:['report']}});
  await repo.create({qitemId:'foreign-product',sourceSession:'foreign-lead@other',destinationSession:'worker@other',body:'product',dispatch:{token:{rigId:'other',epoch:1,generation:'foreign-lead-g1'},packageKey:'product'},identityProvenance:'system:operator-authorized-coordination',nudge:false});nativeClaim('foreign-product','worker@other');repo.update({qitemId:'foreign-product',actorSession:'worker@other',actorGeneration:'worker-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:'foreign-result',sourceSession:'worker@other',destinationSession:'foreign-lead@other',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'actual/foreign.md'}]}),nudge:false});repo.coordinatorAuthority.dispose('worker@other','worker-g1','other','product','foreign-result');svc.accept('foreign-lead@other','foreign-lead-g1','other','product','foreign-result','actual/foreign-accept.md');
  expect(reconcile().find(r=>r.key==='next')).toMatchObject({state:'held',reason:'predecessor-disposition'});expect(nextAssignment()).toBeUndefined();
 });
});
