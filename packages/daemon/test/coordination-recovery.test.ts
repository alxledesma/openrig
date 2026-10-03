import { describe,it,expect,beforeEach,afterEach,vi } from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,coordinationIdle,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {SeatActivityService} from '../src/domain/seat-activity-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';import {seed,token} from './helpers/coordinator-fixture.js';
describe('durable coordination recovery',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 const plan=(tasks:CoordinationTask[]):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks});
 function sample(session:string):CoordinationActivity {const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
 function configure(tasks:CoordinationTask[],resources:Record<string,string[]>={}){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:resources[t.key]??[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks));}
 function refresh(){for(const s of ['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv'])samples.set(s,sample(s));}
 function job(){db.prepare(`INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')`).run(new Date(clock).toISOString());}
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'coordination-'));db=createDb(join(dir,'db'));seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;});
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});
 const normal=()=>[task('product'),task('repair','architect@xv',{recoveryFor:'product'})];
 it('uses fresh deciding window evidence despite stale hook; queued is not pickup and repetition cannot duplicate',()=>{
  configure(normal());const a=svc.reconcile('lead@xv','lead-g1','xv');expect(a[0].state).toBe('pending-pickup');expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('pending-pickup');
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual({n:1});
  repo.claim({qitemId:a[0].queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('picked-up');
 });
 it('freshness and current identity are not fabricated from display or wrong generation',()=>{
  const s=sample('builder@xv');expect(coordinationIdle(s,s.generation,clock)).toBe(true);
  for(const bad of [{...s,identityVerified:false},{...s,generation:'old'},{...s,witness:null},{...s,witness:{...s.witness!,observedAt:new Date(clock-3001).toISOString()}},{...s,witness:{...s.witness!,observedAt:new Date(clock+1).toISOString()}},{...s,state:{...s.state,decidedBy:'lifecycle-hooks' as const}},{...s,state:{...s.state,needsInput:{count:1,reason:'input'}}}])expect(coordinationIdle(bad,s.generation,clock)).toBe(false);
 });
 it('unavailable reviewer creates admitted concrete recovery while independent builder gets actual work',()=>{
  configure([task('review','reviewer@xv'),task('review-repair','architect@xv',{recoveryFor:'review'}),...normal()]);samples.delete('reviewer@xv');const r=svc.reconcile('lead@xv','lead-g1','xv');expect(r.find(x=>x.key==='review')?.reason).toBe('fresh-activity-required');expect(r.find(x=>x.key==='review-repair')?.state).toBe('pending-pickup');expect(r.find(x=>x.key==='product')?.state).toBe('pending-pickup');
  const repair=r.find(x=>x.key==='review-repair')!;repo.claim({qitemId:repair.queueId!,destinationSession:'architect@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='review-repair')?.state).toBe('picked-up');
 });
 it('preserves owner boundary and actual busy worker custody while independent work continues',async()=>{
  configure([task('private','reviewer@xv',{boundary:'owner-access'}),...normal()]);
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','old',{inputDigest:digest('old'),destination:'builder@xv',bodyHash:digest('old'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  await repo.create({qitemId:'old',sourceSession:'lead@xv',destinationSession:'builder@xv',body:'old',dispatch:{token,packageKey:'old'},nudge:false});repo.claim({qitemId:'old',destinationSession:'builder@xv'});
  const r=svc.reconcile('lead@xv','lead-g1','xv');expect(r.find(x=>x.key==='private')?.reason).toBe('owner-access');expect(r.find(x=>x.key==='product')?.reason).toBe('existing-worker-custody');expect(repo.getById('old')?.state).toBe('in-progress');
 });
 it('requires admitted package and current Operator, rejects orphaning old obligations',()=>{
  expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',plan(normal()))).toThrow('explicit admission');configure(normal());expect(()=>svc.configure('operator-agent@kernel','retired',plan(normal()))).toThrow('Current genuine');expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...plan(normal()),tasks:[normal()[1]],revision:'r2'})).toThrow();
 });
 it('ordinary notes and wake replies cannot reset no-progress timeout; real Peer acknowledgment required',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();
  db.prepare("UPDATE queue_items SET ts_updated=?,last_nudge_result='still blocked' WHERE qitem_id='baton'").run(new Date(clock).toISOString());
  expect(svc.supervise('xv','j')?.[0].state).toBe('pending-peer-acknowledgment');expect(repo.coordinatorAuthority.get('xv')?.owner_session).toBe('peer@xv');expect(repo.getById('baton')?.state).toBe('pending');expect(()=>svc.reconcile('peer@xv','peer-g1','xv')).toThrow('Only reconciled');
  repo.coordinatorAuthority.acknowledge('peer@xv',{rigId:'xv',epoch:2,generation:'peer-g1'},{operationId:'peer-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});expect(svc.reconcile('peer@xv','peer-g1','xv')[0].state).toBe('pending-pickup');expect(()=>svc.reconcile('lead@xv','lead-g1','xv')).toThrow('Only reconciled');
 });
 it('never transfers a working/unknown or human-input coordinator',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();samples.get('lead@xv')!.state.activity='working';expect(svc.supervise('xv','j')?.[0].state).toBe('pending-pickup');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);
 });
 it('native pickup is progress; message/reconciliation repetition is not',()=>{
  configure(normal());job();let r=svc.reconcile('lead@xv','lead-g1','xv');clock+=9000;vi.setSystemTime(clock);refresh();repo.claim({qitemId:r[0].queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});svc.reconcile('lead@xv','lead-g1','xv');clock+=2000;vi.setSystemTime(clock);refresh();expect(svc.supervise('xv','j')?.[0].state).toBe('picked-up');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);
 });
 it('actual Architect return is followed through only after exact holder acceptance',async()=>{
  const q='qitem-coordination-'+digest('xv:decision').slice(0,24);
  configure([task('decision','architect@xv'),task('decision-repair','reviewer@xv',{recoveryFor:'decision'}),task('next','builder@xv',{predecessors:[{queueId:q,dispositionId:'returned'}]}),task('next-repair','reviewer@xv',{recoveryFor:'next'})]);
  svc.reconcile('lead@xv','lead-g1','xv');repo.claim({qitemId:q,destinationSession:'architect@xv',identityProvenance:'transport:v1'});
  repo.update({qitemId:q,actorSession:'architect@xv',state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:'returned',sourceSession:'architect@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'decision',inputDigest:digest('decision'),evidence:[{kind:'report',ref:'bounded/decision.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('architect@xv','architect-g1','xv','decision','returned');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='next')?.reason).toBe('predecessor-disposition');
  svc.accept('lead@xv','lead-g1','xv','decision','returned','bounded/technical-acceptance.md');
  const next=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='next')!;expect(next.state).toBe('pending-pickup');
  repo.claim({qitemId:next.queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='next')?.state).toBe('picked-up');
 });
 it('resource conflict is bounded to its slice, not rollback of independent frontier',()=>{
  configure([task('conflict','reviewer@xv'),task('conflict-repair','architect@xv',{recoveryFor:'conflict'}),...normal()],{conflict:['shared']});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','elsewhere',{inputDigest:digest('old'),destination:'architect@xv',bodyHash:digest('old'),resources:['shared'],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  db.prepare("INSERT INTO coordinator_resources VALUES ('xv','shared','elsewhere')").run();
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result.find(x=>x.key==='conflict')?.reason).toBe('coordinator_resource_conflict');expect(result.find(x=>x.key==='product')?.state).toBe('pending-pickup');expect(db.prepare("SELECT package_key FROM coordinator_resources WHERE resource_key='shared'").get()).toEqual({package_key:'elsewhere'});
 });
 it('uncertain dispatch effects refuse takeover and preserve old custody',()=>{
  configure(normal());svc.reconcile('lead@xv','lead-g1','xv');job();clock+=11000;vi.setSystemTime(clock);refresh();const result=svc.supervise('xv','j');expect(result?.find(r=>r.key==='coordinator')?.reason).toBe('coordinator_uncertain_effects');expect(result?.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);
 });
 it('restart reloads obligations and cannot duplicate an unclaimed assignment',()=>{
  configure(normal());const first=svc.reconcile('lead@xv','lead-g1','xv');const successor=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);expect(successor.reconcile('lead@xv','lead-g1','xv')[0].queueId).toBe(first[0].queueId);expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:1});
 });

 it('expired or changed admission holds only its task and schedules an eligible recovery owner',()=>{
  configure(normal());clock+=60001;vi.setSystemTime(clock);refresh();db.prepare("UPDATE coordinator_authority SET lease_until=?").run(clock+10000);
  const old=svc.plan('xv')!;const repair=old.tasks[1];svc.configure('operator-agent@kernel','operator-agent-g1',{...old,revision:'r2',tasks:[{...old.tasks[0],admission:{...old.tasks[0].admission,validUntil:clock+10000}}, {...repair,admission:{...repair.admission,validUntil:clock+10000}}]});
  db.prepare("UPDATE nodes SET model='changed' WHERE id='builder@xv'").run();const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result[0].reason).toBe('current-admission-required');expect(result[1].state).toBe('pending-pickup');
 });
 it('cannot invent capacity or depend on the failing task to start its recovery',()=>{
  const t=task('product'),r=task('repair','architect@xv',{recoveryFor:'product',predecessors:[{queueId:'qitem-coordination-'+digest('xv:product').slice(0,24),dispositionId:'r'}]});expect(()=>configure([t,r])).toThrow('Recovery cannot depend');
  expect(()=>configure([{...t,admission:{...t.admission,capacityRef:''}},task('repair','architect@xv',{recoveryFor:'product'})])).toThrow('qualification/capacity/effort');
 });

 it('same-host alias claim is existing custody, not idle spare capacity',()=>{
  configure(normal());db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,priority,body,claimed_by_generation_uuid) VALUES ('alias',?,?,'lead@xv','builder@xv@fixture-host','in-progress','normal','old','builder-g1')").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  expect(svc.reconcile('lead@xv','lead-g1','xv')[0].reason).toBe('existing-worker-custody');expect(repo.getById('alias')?.state).toBe('in-progress');
 });

 it('the actual shared arbiter degrades stale hooks and yields fresh sampling for dispatch',()=>{
  configure(normal());const session='builder@xv',generation=repo.coordinatorAuthority.generation(session)!;
  const ladder=new SeatActivityService({tmux:{readPaneLastActivity:async()=>null},defaultWindowSeconds:3,now:()=>new Date(clock)});
  ladder.declareRungInventory({seatNodeId:session,sessionName:session},{adapterId:'codex',runtime:'codex',rungs:[]});ladder.declareOccupantSwap(session,generation);
  ladder.declareRungInventory({seatNodeId:session,sessionName:session},{adapterId:'codex',runtime:'codex',rungs:[{rung:'lifecycle-hooks',lifecycleCoverage:'full',initialTrust:'authoritative'},{rung:'window-sampling',lifecycleCoverage:'full',initialTrust:'authoritative'}]});
  ladder.reportEvidence({seatNodeId:session,sessionName:session,rung:'lifecycle-hooks',sourceId:'old-hook',seq:1,observedAt:new Date(clock-60000).toISOString(),activity:'working'});
  ladder.reportEvidence({seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:2,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'});
  const state=ladder.getSeatState(session)!;expect(state.decidedBy).toBe('window-sampling');
  samples.set(session,{generation,identityVerified:true,state,witness:ladder.getRotationActivityWitness(session)});
  expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('pending-pickup');
 });

 it('disposed failed return remains failure and activates real separately admitted recovery pickup',async()=>{
  configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;
  repo.claim({qitemId:q,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:q,actorSession:'builder@xv',state:'failed'});
  await repo.create({qitemId:'failed-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'product',inputDigest:digest('product'),evidence:[{kind:'report',ref:'failed/report.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','product','failed-return');
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result[0].state).toBe('recovery-required:failed');expect(result[1].state).toBe('pending-pickup');expect(()=>svc.accept('lead@xv','lead-g1','xv','product','failed-return','holder/failure.md')).toThrow('Exact successful');
  repo.claim({qitemId:result[1].queueId!,destinationSession:'architect@xv',identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv')[1].state).toBe('picked-up');expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE package_key='product'").get()).toEqual({disposition_id:'failed-return'});
 });
 it('raw SQLite reservation trigger holds its seat while earlier and later independent tasks commit',()=>{
  configure([task('review','reviewer@xv'),task('review-repair','architect@xv',{recoveryFor:'review'}),...normal()]);
  db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('reservation','rotation','reviewer@xv','reviewer@xv','reviewer-g1','native-old','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const result=svc.reconcile('lead@xv','lead-g1','xv');expect(result.find(r=>r.key==='review')?.reason).toBe('seat_dispatch_reserved');expect(result.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(db.prepare("SELECT state FROM seat_dispatch_reservations WHERE reservation_id='reservation'").get()).toEqual({state:'reserved'});expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='review'").get()).toBeUndefined();
 });
 it('ineligible Peer takeover is a durable bounded hold, and current holder still dispatches ready work',async()=>{
  configure(normal());job();await repo.create({qitemId:'peer-existing',sourceSession:'operator-agent@kernel',destinationSession:'peer@xv',body:'existing obligation',nudge:false});clock+=11000;vi.setSystemTime(clock);refresh();
  const result=svc.supervise('xv','j');expect(result?.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(result?.find(r=>r.key==='coordinator')?.reason).toBe('coordination_stall_unproven');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('peer-existing')?.state).toBe('pending');
  const row=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-takeover-hold'").get() as {receipt:string};const receipt=JSON.parse(row.receipt);expect(receipt.owner).toBe('operator-agent@kernel');expect(receipt.action).toContain('Reconcile exact Peer');expect(receipt.deadline).toBeGreaterThan(clock);
 });
 it('transitive recovery path back to failed target is rejected before any plan/assignment effects',()=>{
  const q=(k:string)=>'qitem-coordination-'+digest('xv:'+k).slice(0,24);
  const tasks=[task('product'),task('dependent','reviewer@xv',{predecessors:[{queueId:q('product'),dispositionId:'product-return'}]}),task('repair','architect@xv',{recoveryFor:'product',predecessors:[{queueId:q('dependent'),dispositionId:'dependent-return'}]}),task('dependent-repair','builder@xv',{recoveryFor:'dependent'})];
  expect(()=>configure(tasks)).toThrow('transitively');expect(svc.plan('xv')).toBeNull();expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:0});
 });
 it('unknown SQLite failure still aborts instead of being swallowed as seat reservation',()=>{
  configure(normal());db.exec("CREATE TRIGGER unknown_failure BEFORE INSERT ON queue_items WHEN NEW.destination_session='builder@xv' BEGIN SELECT RAISE(ABORT,'unrecognized_data_corruption'); END");expect(()=>svc.reconcile('lead@xv','lead-g1','xv')).toThrow('unrecognized_data_corruption');expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:0});
 });

 it('cyclic recovery activation chain cannot masquerade as a ready independent plan',()=>{
  expect(()=>configure([task('a','builder@xv',{recoveryFor:'b'}),task('b','architect@xv',{recoveryFor:'a'})])).toThrow('cannot cycle');expect(svc.plan('xv')).toBeNull();
 });

});
