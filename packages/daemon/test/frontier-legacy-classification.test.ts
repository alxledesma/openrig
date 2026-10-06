import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationResult,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import type {ScopeSource,WorkClass} from '../src/domain/frontier-planning.js';
import {seed,token} from './helpers/coordinator-fixture.js';

/** Targeted regressions for the independent REQUEST_CHANGES review.
 *  B1 malformed/uncited citation bypass, B2 missing shared Act facet,
 *  B3 product+scope-bound completion, independent confirmation and reopen,
 *  N1 bounded observations, N3 admission provenance and product class,
 *  N4 frozen monotone receipt and idempotent replay. */
const BRIEF:ScopeSource={ref:'/app-handy/PRODUCT_BRIEF.md',digest:'b7b12dd8e79333976c0cc94578b18eed7fca40035f08a9e11be2b3cf1931f9b5'};
const ROADMAP:ScopeSource={ref:'/app-handy/ROADMAP.md',digest:'ba4006b7e320091a18d13af564a4c1e853ca0eddff9c461b17fa23035d126441'};

describe('frontier legacy classification (S1) and S2/S4 interactions',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>,workClasses:Map<string,WorkClass>;
 beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});clock=Date.now();workClasses=new Map();dir=mkdtempSync(join(tmpdir(),'frontier-review-'));db=createDb(join(dir,'db'));seed(db);
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
 function task(key:string,owner='builder@xv',more:Partial<CoordinationTask>&{workClass?:WorkClass}={}):CoordinationTask{
  const {workClass,...rest}=more;if(workClass)workClasses.set(key,workClass);
  return {key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...rest} as CoordinationTask;}
 const plan=(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks,scopeSources:[BRIEF,ROADMAP],...more});
 function admitContract(key:string,owner:string,workClass:WorkClass='product'){
  repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',key,{inputDigest:digest(key),destination:owner,bodyHash:digest(key),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},workClass});}
 function configure(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}){
  for(const t of tasks)admitContract(t.packageKey,t.owner,workClasses.get(t.packageKey)??'product');
  return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,more));}
 const adminOnly=()=>[task('capacity-inquiry','reviewer@xv',{boundary:'owner-material',workClass:'administrative'})];
 const leadOwned=()=>[...adminOnly(),task('coordination-note','lead@xv',{boundary:'owner-material',workClass:'administrative'})];
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const frontierResult=()=>reconcile().find(r=>r.key==='frontier') as CoordinationResult|undefined;
 const planningControls=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-planning'").get() as {n:number};
 const observations=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='frontier-observation'").get() as {n:number};
 const dutyPacket=(queueId:string)=>JSON.parse(repo.getById(queueId)!.body);
 const stabilize=(states:readonly string[])=>{let r=frontierResult()!;for(let i=0;i<6&&states.includes(r.state);i++){advance(300001);r=frontierResult()!;}return r;};
 const proposal=(citations:unknown[]=[ROADMAP],over:Record<string,unknown>={})=>[{packageKey:'next-frontier',citations,resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']},...over}];

 const LEGACY='legacy-admin-readiness';
 /** A genuinely old package: admitted WITHOUT a work class, exactly as production history holds them. */
 function admitLegacy(key:string,owner:string){repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',key,{inputDigest:digest(key),destination:owner,bodyHash:digest(key),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});}
 const hashOf=(key:string)=>(db.prepare("SELECT contract_hash h FROM coordinator_packages WHERE rig_id='xv' AND package_key=?").get(key) as {h:string}).h;
 const NO_ASSIGNMENT={queueId:null,claimedByGeneration:null,claimedAt:null};
 const classify=(over:Record<string,unknown>={},actor='operator-agent@kernel',gen='operator-agent-g1')=>svc.recordFrontierLegacyClassification(actor,gen,{rigId:'xv',packageKey:LEGACY,contractHash:over.packageKey===undefined?hashOf(LEGACY):'0'.repeat(64),workClass:'administrative',evidenceRef:'operator/evidence/legacy-admin-1.md',observed:NO_ASSIGNMENT,...over} as never);
 const latestId=(key:string,hash:string)=>{const rows=db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='frontier-legacy-classification' ORDER BY rowid").all() as Array<{receipt:string}>;const m=rows.map(r=>JSON.parse(r.receipt)).filter(c=>c.packageKey===key&&c.contractHash===hash);return m.length?m[m.length-1].operationId as string:'frontier-legacy-class:none';};
 const revoke=(over:Record<string,unknown>={})=>{const packageKey=(over.packageKey as string|undefined)??LEGACY,contractHash=(over.contractHash as string|undefined)??(over.packageKey===undefined?hashOf(LEGACY):'0'.repeat(64));return svc.revokeFrontierLegacyClassification('operator-agent@kernel','operator-agent-g1',{rigId:'xv',packageKey,contractHash,classificationId:latestId(packageKey,contractHash),evidenceRef:'operator/evidence/revoke-1.md',...over} as never);};
 /** Old work held by an owner boundary: a protected hold the frontier must keep showing. */
 function heldLegacy(more:Partial<CoordinationTask>={}){admitLegacy(LEGACY,'reviewer@xv');return svc.configure('operator-agent@kernel','operator-agent-g1',plan([task(LEGACY,'reviewer@xv',{boundary:'owner-material',...more})]));}
 const snapshot=()=>svc.frontierProjection('xv')!;
 const classificationRows=()=>(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind IN ('frontier-legacy-classification','frontier-legacy-classification-revocation')").get() as {n:number}).n;

 it('a legacy package alone holds the frontier and mints no planning duty (current behavior, unattested)',()=>{
  heldLegacy();
  expect(snapshot()).toMatchObject({state:'PROTECTED-HOLD',excluded:[]});
  expect(snapshot().packages).toContainEqual({packageKey:LEGACY,workClass:'product',status:'held',legacyClass:true});
  for(let i=0;i<4;i++){advance(300001);reconcile();}
  expect(planningControls().n).toBe(0);
 });

 it('a valid attestation permits planning while the old held work stays visible with its real status',()=>{
  heldLegacy();
  const before=snapshot().frontierDigest,receipt=classify();
  expect(receipt).toMatchObject({workClass:'administrative',grantsAuthority:false,observed:NO_ASSIGNMENT,actor:'operator-agent@kernel'});
  const after=snapshot();
  expect(after.state).toBe('EXHAUSTED');
  expect(after.frontierDigest).not.toBe(before);
  expect(after.packages).toContainEqual({packageKey:LEGACY,workClass:'administrative',status:'held',legacyClass:true});
  expect(after.excluded).toEqual([{packageKey:LEGACY,attestedClass:'administrative',status:'held',queueId:null,classificationId:receipt.operationId,protectedReason:'owner-boundary'}]);
  const duty=stabilize(['stabilizing']);
  expect(duty.state).toBe('pending-native-frontier-planning');
  expect(dutyPacket(duty.queueId!).excludedProtected).toEqual(after.excluded);
 });

 it('attestation lifts only planning flags: the stored contract, the task hold and resources stay exactly as before',()=>{
  heldLegacy();
  const contractBefore=db.prepare("SELECT contract,contract_hash FROM coordinator_packages WHERE package_key=?").get(LEGACY);
  classify();
  expect(db.prepare("SELECT contract,contract_hash FROM coordinator_packages WHERE package_key=?").get(LEGACY)).toEqual(contractBefore);
  expect(reconcile().find(r=>r.key===LEGACY)!.state).toBe('held');
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key=?").get(LEGACY)).toEqual({n:0});
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources").get()).toEqual({n:0});
 });

 it.each([
  ['a non-Operator actor',{actor:'lead@xv',gen:'lead-g1'},'genuine Operator'],
  ['a stale Operator generation',{gen:'operator-agent-g0'},'genuine Operator'],
  ['an unknown package',{over:{packageKey:'no-such-package'}},'No such registered package'],
  ['a hash that is not the stored contract hash',{over:{contractHash:'0'.repeat(64)}},'differs from the stored'],
  ['a malformed hash',{over:{contractHash:'abc'}},'stored contract hash required'],
  ['the product class',{over:{workClass:'product'}},'administrative or inquiry'],
  ['the recovery class',{over:{workClass:'recovery'}},'administrative or inquiry'],
  ['empty evidence',{over:{evidenceRef:'  '}},'evidence reference'],
  ['an absent observation',{over:{observed:undefined}},'observed assignment'],
  ['an incomplete observation',{over:{observed:{queueId:null}}},'observed assignment'],
  ['an observation of an assignment that does not exist',{over:{observed:{queueId:'qitem-fictional',claimedByGeneration:null,claimedAt:null}}},'no longer matches']
 ] as Array<[string,{actor?:string;gen?:string;over?:Record<string,unknown>},string]>)('refuses %s',(_l,args,message)=>{
  heldLegacy();
  expect(()=>classify(args.over??{},args.actor,args.gen)).toThrow(message);
  expect(classificationRows()).toBe(0);
  expect(snapshot().state).toBe('PROTECTED-HOLD');
 });

 it('refuses a package whose frozen contract already carries a work class, and never classifies by name',()=>{
  configure([task('has-class','reviewer@xv',{boundary:'owner-material',workClass:'administrative'})]);
  expect(()=>classify({packageKey:'has-class',contractHash:hashOf('has-class')})).toThrow('no work class');
 });

 it('never classifies by name: an administrative-looking legacy package stays product work until attested',()=>{
  admitLegacy('capacity-administrative-inquiry','reviewer@xv');
  svc.configure('operator-agent@kernel','operator-agent-g1',plan([task('capacity-administrative-inquiry','reviewer@xv',{boundary:'owner-material'})]));
  expect(snapshot().state).toBe('PROTECTED-HOLD');
  expect(snapshot().excluded).toEqual([]);
 });

 it('replays an exact attestation, refuses a silent change, and supersedes only through explicit revocation',()=>{
  heldLegacy();
  const first=classify();
  expect(classify()).toEqual(first);
  expect(classificationRows()).toBe(1);
  expect(()=>classify({evidenceRef:'operator/evidence/other.md'})).toThrow('revoke it explicitly');
  expect(()=>classify({workClass:'inquiry'})).toThrow('revoke it explicitly');
  const lifted=snapshot().frontierDigest;
  const revoked=revoke();
  expect(revoked).toMatchObject({classificationId:first.operationId,grantsAuthority:false});
  expect(revoke()).toEqual(revoked);
  expect(()=>revoke({evidenceRef:'operator/evidence/different.md'})).toThrow('already revoked');
  expect(snapshot()).toMatchObject({state:'PROTECTED-HOLD',excluded:[]});
  expect(snapshot().frontierDigest).not.toBe(lifted);
  const second=classify({workClass:'inquiry',evidenceRef:'operator/evidence/legacy-admin-2.md'});
  expect(second.operationId).not.toBe(first.operationId);
  expect(snapshot().excluded[0]).toMatchObject({attestedClass:'inquiry',classificationId:second.operationId});
  // History is append-only: the first receipt and its revocation are still there, unmodified.
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='frontier-legacy-classification'").get()).toEqual({n:2});
  expect(()=>revoke({packageKey:'no-such'})).toThrow('No such attestation');
  expect(()=>revoke({classificationId:''})).toThrow('exact attestation id');
 });

 it('a delayed revoke replay after revoke and re-attest returns only its own receipt and never revokes the successor',()=>{
  heldLegacy();
  const a=classify(),revokedA=revoke({classificationId:a.operationId});
  const b=classify({evidenceRef:'operator/evidence/legacy-admin-B.md'});
  expect(b.operationId).not.toBe(a.operationId);
  expect(snapshot().excluded[0]).toMatchObject({classificationId:b.operationId});
  const rowsBefore=classificationRows();
  // Exact replay of the stale revoke: the original A receipt comes back, nothing is written.
  expect(revoke({classificationId:a.operationId})).toEqual(revokedA);
  expect(classificationRows()).toBe(rowsBefore);
  expect(snapshot().excluded[0]).toMatchObject({classificationId:b.operationId,attestedClass:'administrative'});
  expect(snapshot().state).toBe('EXHAUSTED');
  // The same stale target with different evidence is a conflict, never a revoke of B.
  expect(()=>revoke({classificationId:a.operationId,evidenceRef:'operator/evidence/other.md'})).toThrow('already revoked');
  expect(snapshot().excluded[0]).toMatchObject({classificationId:b.operationId});
  // An unknown id is refused.
  expect(()=>revoke({classificationId:'frontier-legacy-class:fictional'})).toThrow('No such attestation');
  // Only the exact current id revokes B.
  const revokedB=revoke({classificationId:b.operationId});
  expect(revokedB.classificationId).toBe(b.operationId);
  expect(snapshot().excluded).toEqual([]);
 });

 it('refuses to revoke an attestation that a newer one superseded without revocation',()=>{
  orphanFixture('blocked');
  const a=attestOrphan();
  db.prepare("UPDATE queue_items SET state='in-progress',claimed_at=?,claimed_by_generation_uuid='builder-g1' WHERE qitem_id='qitem-old-orphan'").run(iso());
  const b=attestOrphan({evidenceRef:'operator/evidence/orphan-new-claim.md'});
  expect(b.operationId).not.toBe(a.operationId);
  expect(()=>revoke({packageKey:ORPHAN,contractHash:hashOf(ORPHAN),classificationId:a.operationId})).toThrow('newer attestation supersedes');
  expect(snapshot().excluded[0]).toMatchObject({classificationId:b.operationId});
 });

 // ---------------------------------------------------- assignment-bound cases
 const ORPHAN='old-orphan-assignment';
 const iso=()=>new Date(clock).toISOString();
 /** An old assignment whose package no current plan task owns, planted directly in the temporary database. */
 function plantOrphan(state:'blocked'|'pending'|'in-progress'='blocked',claim:{at:string;generation:string}|null=null,queueId='qitem-old-orphan'){
  db.prepare("DELETE FROM coordinator_assignments WHERE package_key=?").run(ORPHAN);
  db.prepare("DELETE FROM queue_items WHERE qitem_id=?").run(queueId);
  db.prepare("INSERT INTO queue_items(qitem_id,source_session,destination_session,state,body,ts_created,ts_updated,claimed_at,claimed_by_generation_uuid) VALUES (?,?,?,?,?,?,?,?,?)").run(queueId,'lead@xv','builder@xv',state,'legacy-orphan',iso(),iso(),claim?.at??null,claim?.generation??null);
  db.prepare("INSERT INTO coordinator_assignments(rig_id,package_key,queue_id,destination,body_hash,owner_session,owner_generation,epoch,disposition_id,source_queue_id) VALUES ('xv',?,?,?,?,?,?,1,NULL,NULL)").run(ORPHAN,queueId,'builder@xv',digest(ORPHAN),'lead@xv','lead-g1');
 }
 function orphanFixture(state:'blocked'|'pending'|'in-progress'='blocked',claim:{at:string;generation:string}|null=null){
  configure(adminOnly());admitLegacy(ORPHAN,'builder@xv');plantOrphan(state,claim);}
 const observeOrphan=()=>{const r=db.prepare("SELECT a.queue_id q,x.claimed_by_generation_uuid g,x.claimed_at c FROM coordinator_assignments a JOIN queue_items x ON x.qitem_id=a.queue_id WHERE a.package_key=?").get(ORPHAN) as {q:string;g:string|null;c:string|null};return {queueId:r.q,claimedByGeneration:r.g,claimedAt:r.c};};
 const attestOrphan=(over:Record<string,unknown>={})=>classify({packageKey:ORPHAN,contractHash:hashOf(ORPHAN),observed:observeOrphan(),evidenceRef:'operator/evidence/orphan-1.md',...over});

 it('reports a blocked assignment with no current plan task as held, never as a fictitious pending pickup',()=>{
  orphanFixture('blocked');
  const s=snapshot();
  expect(s.packages).toContainEqual({packageKey:ORPHAN,workClass:'product',status:'held',legacyClass:true});
  expect(s.state).toBe('PROTECTED-HOLD');
  expect(s.reason).toBe('product-work-held-by-protection');
 });

 it('attestation of a blocked no-plan-task assignment frees planning but reports the local protected reason',()=>{
  orphanFixture('blocked');
  const receipt=attestOrphan();
  expect(receipt.observed).toEqual({queueId:'qitem-old-orphan',claimedByGeneration:null,claimedAt:null});
  const s=snapshot();
  expect(s.state).toBe('EXHAUSTED');
  expect(s.excluded).toEqual([{packageKey:ORPHAN,attestedClass:'administrative',status:'held',queueId:'qitem-old-orphan',classificationId:receipt.operationId,protectedReason:'protected-no-plan-task'}]);
 });

 it('a changed assignment, claim generation or claim incarnation voids the attestation and is never carried over',()=>{
  orphanFixture('blocked');
  attestOrphan();
  expect(snapshot().state).toBe('EXHAUSTED');
  // The same queue item claimed afterwards is a different claim incarnation.
  db.prepare("UPDATE queue_items SET state='in-progress',claimed_at=?,claimed_by_generation_uuid='builder-g1' WHERE qitem_id='qitem-old-orphan'").run(iso());
  expect(snapshot().state).not.toBe('EXHAUSTED');
  expect(snapshot().excluded).toEqual([]);
  // The old observation cannot be replayed onto the changed assignment.
  expect(()=>attestOrphan({observed:{queueId:'qitem-old-orphan',claimedByGeneration:null,claimedAt:null}})).toThrow('no longer matches');
  const second=attestOrphan({evidenceRef:'operator/evidence/orphan-2.md'});
  expect(() => revoke({packageKey:ORPHAN,contractHash:hashOf(ORPHAN)})).not.toThrow();
  expect(second.observed.claimedByGeneration).toBe('builder-g1');
  // Only the claim incarnation changes (same queue item, same generation): void again.
  attestOrphan({evidenceRef:'operator/evidence/orphan-3.md'});
  expect(snapshot().state).toBe('EXHAUSTED');
  db.prepare("UPDATE queue_items SET claimed_at=? WHERE qitem_id='qitem-old-orphan'").run(new Date(clock+5000).toISOString());
  expect(snapshot().state).not.toBe('EXHAUSTED');
  // A brand new assignment queue item for the same package is a new assignment as well.
  attestOrphan({evidenceRef:'operator/evidence/orphan-4.md'});
  expect(snapshot().state).toBe('EXHAUSTED');
  plantOrphan('blocked',null,'qitem-old-orphan-2');
  expect(snapshot().state).not.toBe('EXHAUSTED');
  expect(snapshot().excluded).toEqual([]);
 });

 const tableDigest=()=>Object.fromEntries(['coordinator_packages','coordinator_assignments','coordinator_resources','queue_items','outbox_entries','coordinator_held_history','coordinator_authority'].map(t=>[t,digest(JSON.stringify(db.prepare(`SELECT * FROM ${t} ORDER BY rowid`).all()))]));
 it('attestation leaves custody, resource locks, outbox/UNKNOWN effects, held history, claims and authority byte-identical',()=>{
  orphanFixture('blocked');
  db.prepare("INSERT INTO coordinator_resources(rig_id,resource_key,package_key) VALUES ('xv','locked-resource',?)").run(ORPHAN);
  const before=tableDigest();
  attestOrphan();
  expect(tableDigest()).toEqual(before);
  revoke({packageKey:ORPHAN,contractHash:hashOf(ORPHAN)});
  expect(tableDigest()).toEqual(before);
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key=?").get(ORPHAN)).toEqual({n:1});
 });

 it('status churn of attested or dormant work neither changes the planning identity nor resets stabilization',()=>{
  orphanFixture('blocked');
  attestOrphan();
  const digestBefore=snapshot().frontierDigest;
  expect(frontierResult()!.state).toBe('stabilizing');
  const run=()=>(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='frontier-observation'").get() as {n:number}).n;
  const first=run();
  // Same assignment and claim, different local status: attested work is shown, not counted.
  db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id='qitem-old-orphan'").run();
  const churned=snapshot();
  expect(churned.excluded[0]!.status).toBe('pending-pickup');
  expect(churned.frontierDigest).toBe(digestBefore);
  const planning=stabilize(['stabilizing']);
  expect(run()).toBeGreaterThan(first);
  expect(snapshot().frontierDigest).toBe(digestBefore);
  expect(planning.state).toBe('pending-native-frontier-planning');
  expect(planningControls().n).toBe(1);
 });

 // --------------------------------------------------------- S2 proposals
 async function recordProposal(packages:unknown[]){
  const duty=stabilize(['stabilizing']),body=dutyPacket(duty.queueId!);
  repo.claim({qitemId:duty.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  return {duty,body,record:()=>svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:duty.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:packages})};
 }
 it.each([
  ['a repeated package key in one proposal',()=>[...proposal(),...proposal()],'once'],
  ['an already registered package key',()=>proposal([ROADMAP],{packageKey:'capacity-inquiry'}),'already registered or planned']
 ] as Array<[string,()=>unknown[],string]>)('refuses a proposal with %s and records nothing',async(_l,make,message)=>{
  configure(adminOnly());
  const p=await recordProposal(make());
  expect(()=>p.record()).toThrow(message);
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toBeUndefined();
 });

 it('refuses a key already named by another recorded proposal that still awaits admission',async()=>{
  configure(adminOnly());
  const first=await recordProposal(proposal());
  first.record();
  // A second planning duty is a separate obligation only after a reopen; here a second duty row is forged
  // for the same frozen digest to prove the guard itself, not the single-obligation gate.
  const forgedId='qitem-forged-second-planning-duty';
  const control=db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=?").get(first.duty.queueId!) as {receipt:string};
  const second={...JSON.parse(control.receipt),queueId:forgedId};
  db.prepare("INSERT INTO coordinator_operations VALUES ('xv',?,?,?,?)").run(forgedId,'coordinator-lifecycle-control',JSON.stringify(second),digest(JSON.stringify(second)));
  const open=(svc as unknown as {frontierPlanning():{openProposals(r:string):unknown[]}}).frontierPlanning().openProposals('xv');
  expect(open).toHaveLength(1);
  const guard=(svc as unknown as {frontierPlanning():{proposalKeysAvailable(r:string,d:string,p:unknown[]):void}}).frontierPlanning();
  expect(()=>guard.proposalKeysAvailable('xv',forgedId,proposal() as never)).toThrow('still awaiting admission');
 });

 it('an exact replay of an admitted proposal still returns its frozen receipt, not a new-key refusal',async()=>{
  configure(adminOnly());
  const p=await recordProposal(proposal()),recorded=p.record(),admission=frontierResult()!;
  repo.claim({qitemId:admission.queueId!,destinationSession:'operator-agent@kernel',actorGeneration:'operator-agent-g1',identityProvenance:'transport:v1'});
  svc.admitFrontierProposal('operator-agent@kernel','operator-agent-g1',{rigId:'xv',dutyQueueId:admission.queueId!,proposalDigest:recorded.proposal!.proposalDigest,admitted:[{packageKey:'next-frontier',contract:{inputDigest:digest('next-frontier'),destination:'builder@xv',bodyHash:digest('next-frontier'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}}]});
  expect(db.prepare("SELECT 1 FROM coordinator_packages WHERE package_key='next-frontier'").get()).toBeTruthy();
  expect(()=>svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:p.duty.queueId!,frontierDigest:p.body.frontierDigest,disposition:'plan-proposal',proposal:proposal()})).not.toThrow();
 });

 // ---------------------------------------- S4 one obligation across digest change
 it('classification-driven digest change cannot mint a second live proposal while one awaits admission',async()=>{
  heldLegacy();classify();
  const planning=stabilize(['stabilizing']),body=dutyPacket(planning.queueId!);
  repo.claim({qitemId:planning.queueId!,destinationSession:'lead@xv',actorGeneration:'lead-g1',identityProvenance:'transport:v1'});
  const recorded=svc.recordFrontierPlan('lead@xv','lead-g1',{rigId:'xv',dutyQueueId:planning.queueId!,frontierDigest:body.frontierDigest,disposition:'plan-proposal',proposal:proposal()});
  const admission=frontierResult()!;
  expect(admission.reason).toBeUndefined();
  const admissionControls=()=>(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordinator-lifecycle-control' AND json_extract(receipt,'$.kind')='frontier-admission'").get() as {n:number}).n;
  expect(admissionControls()).toBe(1);
  // Genuine digest change: the attestation is superseded (revoke then attest again with new evidence).
  const digestAtProposal=snapshot().frontierDigest;
  revoke();
  classify({evidenceRef:'operator/evidence/legacy-admin-reissued.md'});
  expect(snapshot().frontierDigest).not.toBe(digestAtProposal);
  expect(snapshot().state).toBe('EXHAUSTED');
  for(let i=0;i<3;i++){advance(300001);const r=frontierResult();expect(r!.queueId).toBe(admission.queueId);}
  expect(planningControls().n).toBe(1);
  expect(admissionControls()).toBe(1);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='frontier-plan-disposition'").get()).toEqual({n:1});
  expect(recorded.proposal!.proposalDigest).toBeTruthy();
 });
});
