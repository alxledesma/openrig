import {makeCoordinatorContinuityPolicy} from '../src/domain/policies/coordinator-continuity.js';
import type {RuntimeAvailability} from '../src/domain/coordinator-runtime-availability.js';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {resolve} from 'node:path';
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
 it('observer records the exact activity hold instead of hiding reconciliation behind outage status',async()=>{
  configure(normal());job();const s=samples.get('builder@xv')!;
  s.identityVerified=false;
  s.witness!.observedAt=new Date(clock-3001).toISOString();
  const evaluation=await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  expect(evaluation).toMatchObject({action:'skip',reason:'coordination-reconciled'});
  const held=(evaluation.notes!.coordination as any[]).find(r=>r.key==='product');
  expect(held).toMatchObject({reason:'fresh-activity-required',activityEvidence:{identityVerified:false,generation:'builder-g1',expectedGeneration:'builder-g1',activity:'idle-at-prompt',witnessAgeMs:3001,witnessRung:'window-sampling'}});
  expect(repo.getById('qitem-coordination-'+digest('xv:product').slice(0,24))).toBeNull();
  const before=db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-reconcile'").get();
  s.witness!.observedAt=new Date(clock-4000).toISOString();
  svc.reconcile('lead@xv','lead-g1','xv');
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-reconcile'").get()).toEqual(before);
 });
 it('native identity refresh enables only the checkpoint-authorized recovery, with genuine pickup and no Peer takeover',async()=>{
  const tasks=[task('product'),task('unrelated','peer@xv',{recoveryFor:'product'}),task('repair','peer@xv',{recoveryFor:'product'})];
  const initial=configure(tasks);job();samples.delete('builder@xv');samples.get('peer@xv')!.identityVerified=false;
  svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock,async()=>{samples.set('peer@xv',sample('peer@xv'));});repo.coordinatorAuthority.coordinationRecovery=svc;
  svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'scoped-r2',refreshDispatchIdentity:true,dispatchRestrictions:[{session:'peer@xv',generation:'peer-g1',packageKeys:['repair'],validUntil:clock+30000,evidenceRef:'native/checkpoint-qa-only.json'}]});
  const e=await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);
  const results=e.notes!.coordination as any[];
  expect(results.find(r=>r.key==='unrelated')).toMatchObject({state:'held',reason:'checkpoint-quiescence'});
  const repair=results.find(r=>r.key==='repair');expect(repair.state).toBe('pending-pickup');
  expect(()=>db.transaction(()=>repo.createWithinTransaction({qitemId:'scope-bypass',sourceSession:'lead@xv',destinationSession:'peer@xv',body:'unrelated',dispatch:{token,packageKey:'unrelated'}}))()).toThrow('checkpoint dispatch scope');
  expect(()=>repo.coordinatorAuthority.transfer('lead@xv','lead-g1',{expected:token,oldOwner:'lead@xv',recipient:'peer@xv',recipientGeneration:'peer-g1',operationId:'scope-transfer-bypass',leaseMs:60000})).toThrow('checkpoint dispatch scope');
  repo.claim({qitemId:repair.queueId,destinationSession:'peer@xv',identityProvenance:'transport:v1'});
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='repair')?.state).toBe('picked-up');
  expect(svc.canTransferUnavailable('xv','peer@xv','peer-g1')).toBe(false);
  expect(svc.canTransferIdle('xv','peer@xv','peer-g1','any')).toBe(false);
  expect(repo.coordinatorAuthority.get('xv')?.owner_session).toBe('lead@xv');
 });
 it('an expired dispatch scope keeps quiescence closed rather than reopening older work',()=>{
  const tasks=[task('product'),task('unrelated','peer@xv',{recoveryFor:'product'}),task('repair','peer@xv',{recoveryFor:'product'})];
  const initial=configure(tasks);samples.delete('builder@xv');
  svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'scope-expiry-r2',dispatchRestrictions:[{session:'peer@xv',generation:'peer-g1',packageKeys:['repair'],validUntil:clock+10000,evidenceRef:'native/checkpoint-qa-only.json'}]});
  clock+=10001;vi.setSystemTime(clock);refresh();samples.delete('builder@xv');
  const results=svc.reconcile('lead@xv','lead-g1','xv');
  for(const key of ['unrelated','repair'])expect(results.find(r=>r.key===key)).toMatchObject({state:'held',reason:'dispatch-scope-expired'});
  expect(db.prepare('select count(*) n from coordinator_assignments').get()).toEqual({n:0});
 });
 it('explicit genuine Operator checkpoint disposition commits one recipient notice, never a release from a restriction alone',()=>{
  const initial=configure([task('product'),task('repair','peer@xv',{recoveryFor:'product'})]);
  const restriction={session:'peer@xv',generation:'peer-g1',packageKeys:['repair'],validUntil:clock+30000,evidenceRef:'native/qa-only.json'};
  const restricted={...initial,revision:'scope-only',dispatchRestrictions:[restriction]};svc.configure('operator-agent@kernel','operator-agent-g1',restricted);
  expect(db.prepare("SELECT count(*) n FROM queue_items WHERE source_session='operator-agent@kernel' AND destination_session='peer@xv'").get()).toEqual({n:0});
  const released={...restricted,revision:'scoped-disposition',dispatchRestrictions:[{...restriction,checkpointDisposition:'release-listed-packages' as const}]};
  expect(()=>svc.configure('lead@xv','lead-g1',released)).toThrow('Current genuine Operator');
  svc.configure('operator-agent@kernel','operator-agent-g1',released);svc.configure('operator-agent@kernel','operator-agent-g1',released);
  svc.configure('operator-agent@kernel','operator-agent-g1',{...released,revision:'same-disposition-new-plan'});
  const rows=db.prepare("SELECT * FROM queue_items WHERE source_session='operator-agent@kernel' AND destination_session='peer@xv'").all() as any[];
  expect(rows).toHaveLength(1);
  const wakes=db.prepare('SELECT * FROM outbox_entries WHERE audit_pointer=?').all(rows[0].qitem_id) as any[];expect(wakes).toHaveLength(1);expect(wakes[0]).toMatchObject({sender_session:'operator-agent@kernel',destination_session:'peer@xv',delivery_state:'pending'});expect(JSON.parse(wakes[0].tags)).toContain('queue:recipient-generation:peer-g1');
  expect(JSON.parse(rows[0].body)).toMatchObject({action:'checkpoint-scope-disposition',operatorGeneration:'operator-agent-g1',recipientGeneration:'peer-g1',packageKeys:['repair']});
  expect(db.prepare('select count(*) n from coordinator_assignments').get()).toEqual({n:0});
  repo.claim({qitemId:rows[0].qitem_id,destinationSession:'peer@xv',identityProvenance:'transport:v1'});expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(rows[0].qitem_id)).toEqual({claimed_by_generation_uuid:'peer-g1'});
 });
 it('restart has no swap yet real fresh idle remains eligible; actual pickup follows',()=>{
  configure(normal());const session='builder@xv',generation=repo.coordinatorAuthority.generation(session)!;
  const ladder=new SeatActivityService({tmux:{readPaneLastActivity:async()=>null},defaultWindowSeconds:3,now:()=>new Date(clock)});
  ladder.reportEvidence({seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'});
  const state=ladder.getSeatState(session)!;expect(state.lastSwap).toBeNull();
  const a={generation,identityVerified:true,state,witness:ladder.getRotationActivityWitness(session)};
  expect(coordinationIdle(a,generation,clock)).toBe(true);expect(coordinationIdle({...a,generation:'retired'},generation,clock)).toBe(false);
  samples.set(session,a);const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;repo.claim({qitemId:q,destinationSession:session,identityProvenance:'transport:v1'});expect(svc.reconcile('lead@xv','lead-g1','xv')[0].state).toBe('picked-up');
 });
 it('swap rejects pre-swap samples and ULID mismatch; fresh managed-generation idle passes',()=>{
  const session='builder@xv',generation='managed-uuid';const ladder=new SeatActivityService({tmux:{readPaneLastActivity:async()=>null},defaultWindowSeconds:3,now:()=>new Date(clock)});
  const e={seatNodeId:session,sessionName:session,rung:'window-sampling' as const,sourceId:'tmux',seq:1,observedAt:new Date(clock-1).toISOString(),activity:'idle-at-prompt' as const};
  ladder.reportEvidence(e);ladder.declareOccupantSwap(session,generation);ladder.declareRungInventory({seatNodeId:session,sessionName:session},{adapterId:'codex',runtime:'codex',rungs:[{rung:'window-sampling',lifecycleCoverage:'full',initialTrust:'authoritative'}]});
  ladder.reportEvidence({...e,seq:2});expect(ladder.getRotationActivityWitness(session)).toBeNull();ladder.reportEvidence({...e,seq:3,observedAt:new Date(clock).toISOString()});
  const a={generation,identityVerified:true,state:ladder.getSeatState(session)!,witness:ladder.getRotationActivityWitness(session)};expect(coordinationIdle(a,generation,clock)).toBe(true);expect(coordinationIdle({...a,state:{...a.state,lastSwap:{generation:'session-ulid',at:new Date(clock).toISOString()}}},generation,clock)).toBe(false);
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
 it.each(['matching','conflicting'] as const)('retained deterministic queue row without assignment holds its slice (%s) while independent work proceeds',async(kind)=>{
  configure([task('review','peer@xv'),task('review-repair','architect@xv',{recoveryFor:'review'}),...normal()]);
  const id='qitem-coordination-'+digest('xv:review').slice(0,24);await repo.create({qitemId:id,sourceSession:'operator-agent@kernel',destinationSession:'peer@xv',body:kind==='matching'?'review':'different historical row',nudge:false});
  repo.claim({qitemId:id,destinationSession:'peer@xv'});repo.update({qitemId:id,state:'done',actorSession:'peer@xv',closureReason:'no-follow-on',note:'Retained historical completion'});
  const before=repo.getById(id);const result=svc.reconcile('lead@xv','lead-g1','xv');
  expect(result.find(r=>r.key==='review')).toMatchObject({state:'held',queueId:id,reason:kind==='matching'?'existing-queue-without-assignment':'deterministic-queue-conflict'});expect(result.find(r=>r.key==='product')?.state).toBe('pending-pickup');expect(repo.getById(id)).toEqual(before);expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='review'").get()).toBeUndefined();
 });
 it('unknown SQLite failure still aborts instead of being swallowed as seat reservation',()=>{
  configure(normal());db.exec("CREATE TRIGGER unknown_failure BEFORE INSERT ON queue_items WHEN NEW.destination_session='builder@xv' BEGIN SELECT RAISE(ABORT,'unrecognized_data_corruption'); END");expect(()=>svc.reconcile('lead@xv','lead-g1','xv')).toThrow('unrecognized_data_corruption');expect(db.prepare('SELECT count(*) n FROM coordinator_assignments').get()).toEqual({n:0});
 });

 it('cyclic recovery activation chain cannot masquerade as a ready independent plan',()=>{
  expect(()=>configure([task('a','builder@xv',{recoveryFor:'b'}),task('b','architect@xv',{recoveryFor:'a'})])).toThrow('cannot cycle');expect(svc.plan('xv')).toBeNull();
 });

 it('undispatched dependent with busy owner never activates recovery before prerequisite acceptance',()=>{
  const dependent=task('dependent','builder@xv',{predecessors:[{queueId:'qitem-coordination-'+digest('xv:product').slice(0,24),dispositionId:'not-returned'}],deadline:clock+1});
  configure([...normal(),dependent,task('repair-dependent','reviewer@xv',{recoveryFor:'dependent'})]);
  const product=svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='product')!;
  repo.claim({qitemId:product.queueId!,destinationSession:'builder@xv',identityProvenance:'transport:v1'});samples.get('builder@xv')!.state.activity='working';clock+=2;
  const results=svc.reconcile('lead@xv','lead-g1','xv');expect(results.find(x=>x.key==='dependent')?.reason).toBe('predecessor-disposition');expect(results.find(x=>x.key==='repair-dependent')?.reason).toBe('recovery-not-needed');
  expect(db.prepare("SELECT COUNT(*) n FROM coordinator_assignments WHERE package_key='repair-dependent'").get()).toEqual({n:0});
 });
 it('current recipient or Operator recovers expired reconciling lease once with exact custody, never acknowledges implicitly',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();svc.supervise('xv','j');
  const authority=repo.coordinatorAuthority,a=authority.get('xv')!;expect(a.state).toBe('reconciling');expect(a.lease_until-clock).toBe(300000);
  clock=a.lease_until+1;vi.setSystemTime(clock);const notice=svc.supervise('xv','j')![0];expect(notice.state).toBe('pending-reconciliation-recovery');repo.claim({qitemId:notice.queueId!,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});expect(svc.supervise('xv','j')![0].queueId).toBe(notice.queueId);const input={token:{rigId:'xv',epoch:a.epoch,generation:'peer-g1'},operationId:'bounded-recovery',obligationsDigest:authority.reconciliationDigest('xv'),windowMs:60000};
  expect(()=>authority.recoverReconciliation('lead@xv','lead-g1',input)).toThrow();expect(()=>authority.recoverReconciliation('operator-agent@kernel','retired',input)).toThrow();expect(()=>authority.recoverReconciliation('peer@xv','peer-g1',{...input,obligationsDigest:'stale'})).toThrow('exact current custody');
  const recovered=authority.recoverReconciliation('peer@xv','peer-g1',input);expect(recovered.state).toBe('reconciling');expect(repo.getById('baton')?.state).toBe('pending');expect(authority.recoverReconciliation('peer@xv','peer-g1',input)).toEqual(recovered);
  clock=recovered.lease_until+1;vi.setSystemTime(clock);expect(()=>authority.recoverReconciliation('operator-agent@kernel','operator-agent-g1',{...input,operationId:'again'})).toThrow('One bounded');
 });
 it('separately admitted feedback is picked up without modifying blocked parent custody or bypassing raw fence',()=>{
  configure(normal());const q=svc.reconcile('lead@xv','lead-g1','xv')[0].queueId!;repo.claim({qitemId:q,destinationSession:'builder@xv',identityProvenance:'transport:v1'});repo.update({qitemId:q,actorSession:'builder@xv',state:'blocked'});
  const before=repo.getById(q),body=JSON.stringify({action:'reconcile-existing-custody',parentQueueId:q,parentPackageKey:'product',instruction:'Return denied with exact premature-dispatch evidence; do not perform dependent work'});
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','feedback',{inputDigest:digest(body),destination:'builder@xv',bodyHash:digest(body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
  const input={rigId:'xv',epoch:1,parentPackageKey:'product',parentQueueId:q,workerGeneration:'builder-g1',feedbackPackageKey:'feedback',body};
  expect(()=>svc.continueCustody('lead@xv','retired',input)).toThrow();expect(()=>svc.continueCustody('lead@xv','lead-g1',{...input,parentQueueId:'wrong'})).toThrow('Exact current claimed');
  const feedback=svc.continueCustody('lead@xv','lead-g1',input);expect(svc.continueCustody('lead@xv','lead-g1',input)).toEqual(feedback);expect(repo.getById(q)).toEqual(before);
  repo.claim({qitemId:feedback.queueId,destinationSession:'builder@xv',identityProvenance:'transport:v1'});expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(feedback.queueId)).toEqual({claimed_by_generation_uuid:'builder-g1'});expect(repo.getById(q)?.state).toBe('blocked');
  expect(()=>repo.coordinatorAuthority.assertRawSend('lead@xv','builder@xv')).toThrow('Raw managed sends');
 });

 it('two real processes cannot extend one expired reconciliation twice',async()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();svc.supervise('xv','j');const a=repo.coordinatorAuthority.get('xv')!;clock=a.lease_until+1;vi.setSystemTime(clock);
  const base={token:{rigId:'xv',epoch:a.epoch,generation:'peer-g1'},obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv'),windowMs:60000};
  const code=`import D from 'better-sqlite3';import {CoordinatorAuthorityService} from './dist/domain/coordinator-authority-service.js';const db=new D(process.argv[1]);const s=new CoordinatorAuthorityService(db,undefined,undefined,()=>Number(process.argv[2]));try{s.recoverReconciliation('operator-agent@kernel','operator-agent-g1',JSON.parse(process.argv[3]));console.log('extended');}catch(e){console.log(e.code);process.exitCode=7;}finally{db.close();}`;
  const run=async(op:string)=>{const child=spawn(process.execPath,['--input-type=module','-e',code,join(dir,'db'),String(clock),JSON.stringify({...base,operationId:op})],{cwd:resolve('.'),stdio:['ignore','pipe','pipe']});let out='';child.stdout.on('data',b=>out+=b);const [exit]=await once(child,'exit');return {exit,out};};
  const results=await Promise.all([run('race-a'),run('race-b')]);expect(results.filter(r=>r.exit===0)).toHaveLength(1);expect(results.filter(r=>r.exit===7)).toHaveLength(1);expect(db.prepare("SELECT COUNT(*) n FROM coordinator_operations WHERE kind='reconciliation-recover'").get()).toEqual({n:1});
 });

 it('Operator recovered window still requires genuine Peer custody acknowledgment and fences retired Lead',()=>{
  configure(normal());job();clock+=11000;vi.setSystemTime(clock);refresh();svc.supervise('xv','j');const authority=repo.coordinatorAuthority,a=authority.get('xv')!;clock=a.lease_until+1;vi.setSystemTime(clock);
  const token={rigId:'xv',epoch:a.epoch,generation:'peer-g1'};authority.recoverReconciliation('operator-agent@kernel','operator-agent-g1',{token,operationId:'operator-recovery',obligationsDigest:authority.reconciliationDigest('xv'),windowMs:60000});
  expect(()=>svc.reconcile('peer@xv','peer-g1','xv')).toThrow('Only reconciled');const digestNow=authority.reconciliationDigest('xv');
  expect(()=>authority.acknowledge('lead@xv',{...token,generation:'lead-g1'},{operationId:'retired-ack',obligationsDigest:digestNow})).toThrow();
  expect(authority.acknowledge('peer@xv',token,{operationId:'real-peer-ack',obligationsDigest:digestNow}).state).toBe('active');expect(repo.getById('baton')?.state).toBe('in-progress');
 });

 it('fresh working owner is scheduling, but stale busy evidence cannot indefinitely suppress ready recovery',()=>{
  configure(normal());const busy=samples.get('builder@xv')!;busy.state.activity='working';busy.witness!.activity='working';
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='repair')?.reason).toBe('recovery-not-needed');
  clock+=3001;vi.setSystemTime(clock);samples.set('architect@xv',sample('architect@xv'));
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(x=>x.key==='repair')?.state).toBe('pending-pickup');
 });

 function unavailableSetup(){const prior=configure(normal());svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'unavailable-r2',allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true});job();repo.coordinatorAuthority.renew('lead@xv',token,10000,'short-lease');clock+=10001;vi.setSystemTime(clock);refresh();}
 function observer(state:RuntimeAvailability['state']='absent',age=0,generation='lead-g1'){repo.coordinatorAuthority.setRuntimeObserver(async session=>({session,generation:session==='lead@xv'?generation:'peer-g1',state:session==='lead@xv'?state:'present',observedAt:clock-age,fingerprint:'actual-test-census-'+session}));}
 it('admitted unavailable expired owner transfers automatically with actual Peer wake/claim/ACK preserving worker custody',async()=>{const initial=configure(normal(),{product:['file:valuable']});const work=svc.reconcile('lead@xv','lead-g1','xv')[0];repo.claim({qitemId:work.queueId!,destinationSession:'builder@xv'});db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE delivery_state='pending'").run();svc.configure('operator-agent@kernel','operator-agent-g1',{...initial,revision:'unavailable-r2',allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true});job();const before=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(work.queueId),resourcesBefore=db.prepare('SELECT * FROM coordinator_resources').all();repo.coordinatorAuthority.renew('lead@xv',token,10000,'short-lease');clock+=10001;vi.setSystemTime(clock);refresh();observer();db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('peer-binding','peer@xv','peer@xv','%2')").run();const sends:string[]=[];repo.attachTransport({send:async(session,text,opts)=>{sends.push(opts!.queueAssignmentId!);repo.claim({qitemId:opts!.queueAssignmentId!,destinationSession:session});return {ok:true,verified:true};}});await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority).evaluate({jobId:'j',registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'xv'}} as any);const a=repo.coordinatorAuthority.get('xv')!;expect(a).toMatchObject({epoch:2,state:'reconciling',owner_session:'peer@xv'});expect(sends).toHaveLength(1);expect(repo.getById(sends[0])?.state).toBe('in-progress');expect(repo.getById('baton')?.state).toBe('pending');expect(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(work.queueId)).toEqual(before);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resourcesBefore);expect(()=>repo.coordinatorAuthority.renew('lead@xv',token,60000,'retired')).toThrow();repo.coordinatorAuthority.acknowledge('peer@xv',{rigId:'xv',epoch:2,generation:'peer-g1'},{operationId:'peer-real-ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});expect(repo.getById('baton')?.state).toBe('in-progress');expect(svc.reconcile('peer@xv','peer-g1','xv')[0].state).toBe('picked-up');});
 it.each(['present','unknown'] as const)('expired lease plus %s native owner never permits unavailable takeover',async(state)=>{unavailableSetup();observer(state);await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(svc.supervise('xv','j')?.[0].state).toBe('recovery-required');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.state).toBe('in-progress');});
 it('stale or wrong generation native absence cannot substitute for current exclusion',async()=>{unavailableSetup();observer('absent',1001);await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(repo.coordinatorAuthority.hasFreshUnavailableOwner('xv')).toBe(false);expect(svc.supervise('xv','j')?.[0].state).toBe('recovery-required');observer('absent',0,'retired');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(repo.coordinatorAuthority.hasFreshUnavailableOwner('xv')).toBe(false);expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});
 it.each(['busy-peer','expired-admission','changed-baton','uncertain-effects','reserved-peer','old-operator'])('unavailable-owner %s refusal is accountable and preserves existing epoch/baton',async(reason)=>{unavailableSetup();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');if(reason==='busy-peer')samples.get('peer@xv')!.state.activity='working';if(reason==='expired-admission'){clock+=60000;vi.setSystemTime(clock);refresh();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');}if(reason==='changed-baton')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='retired' WHERE qitem_id='baton'").run();if(reason==='uncertain-effects')repo.stageWakeIntent('baton','lead@xv','lead@xv','transport:v1',true);if(reason==='reserved-peer')db.exec("CREATE TEMP TRIGGER test_reserved_peer BEFORE INSERT ON queue_items WHEN NEW.destination_session='peer@xv' BEGIN SELECT RAISE(ABORT,'seat_dispatch_reserved'); END");if(reason==='old-operator')db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();if(reason==='old-operator')expect(()=>svc.supervise('xv','j')).toThrow('Current Operator');else{const held=svc.supervise('xv','j')![0];expect(held.state).toBe('held');expect(held.queueId).toBeTruthy();expect(repo.getById(held.queueId!)?.destinationSession).toBe('operator-agent@kernel');}expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.destinationSession).toBe('lead@xv');});
 it('fresh return of native owner or changed expected epoch refuses preobserved transfer before effects',async()=>{unavailableSetup();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');const input={rigId:'xv',expectedEpoch:1,expectedOwner:'lead@xv',expectedOwnerGeneration:'lead-g1',planRevision:'unavailable-r2',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:60000};observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',input)).toThrow('native absence');observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',{...input,expectedEpoch:2})).toThrow('predecessor changed');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});

 it('unavailable transfer strict opt-in and actual Peer current generation are mandatory',async()=>{configure(normal());job();repo.coordinatorAuthority.renew('lead@xv',token,10000,'short');clock+=10001;vi.setSystemTime(clock);refresh();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(svc.supervise('xv','j')?.[0].state).toBe('recovery-required');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});

 it('live lease, wrong Peer generation and stale Peer witness prevent otherwise admitted outage transfer',async()=>{const prior=configure(normal());svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'unavailable-r2',allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true});job();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');const input={rigId:'xv',expectedEpoch:1,expectedOwner:'lead@xv',expectedOwnerGeneration:'lead-g1',planRevision:'unavailable-r2',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:60000};expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',input)).toThrow('Expired lease');repo.coordinatorAuthority.renew('lead@xv',token,10000,'short');clock+=10001;vi.setSystemTime(clock);refresh();observer();await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',{...input,recipientGeneration:'retired'})).toThrow('generation');samples.get('peer@xv')!.witness!.observedAt=new Date(clock-3001).toISOString();expect(()=>repo.coordinatorAuthority.transferObservedUnavailable('j',input)).toThrow('Fresh idle');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.state).toBe('in-progress');});
 it('strict unavailable opt-in never inherits from a truthy string',()=>{const prior=configure(normal());expect(()=>svc.configure('operator-agent@kernel','operator-agent-g1',{...prior,revision:'badoptin',allowUnavailablePeerTransfer:'true' as any})).toThrow('strict explicit boolean');});

 it('closing an expired-holder recovery notice without restoring authority does not erase the recovery obligation',async()=>{unavailableSetup();observer('present');await repo.coordinatorAuthority.refreshRuntimeAvailability('xv');const first=svc.supervise('xv','j')![0];repo.claim({qitemId:first.queueId!,destinationSession:'operator-agent@kernel'});repo.update({qitemId:first.queueId!,actorSession:'operator-agent@kernel',state:'done',closureReason:'no-follow-on'});const next=svc.supervise('xv','j')![0];expect(next.queueId).not.toBe(first.queueId);expect(JSON.parse(repo.getById(next.queueId!)!.body).previousQueueId).toBe(first.queueId);expect(repo.getById(first.queueId!)?.state).toBe('done');expect(repo.coordinatorAuthority.get('xv')?.epoch).toBe(1);});

});
