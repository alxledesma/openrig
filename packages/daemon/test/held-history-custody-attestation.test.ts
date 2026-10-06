import {beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {createDb} from '../src/db/connection.js';import {EventBus} from '../src/domain/event-bus.js';import {QueueRepository} from '../src/domain/queue-repository.js';import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity} from '../src/domain/coordination-recovery-service.js';
import {CoordinatorAuthorityService,digest,legacyProposalDigest,type LegacyEnrollment} from '../src/domain/coordinator-authority-service.js';
import {HistoricalEffectDispositionService,historicalDigest,type HistoricalPlan,type HistoricalDisposition} from '../src/domain/historical-effect-disposition.js';
import {SeatDeliveryGuard,resolveGuardTarget} from '../src/domain/seat-delivery-guard.js';import {seed,token} from './helpers/coordinator-fixture.js';
let dir:string,db:ReturnType<typeof createDb>,repo:QueueRepository,outbox:OutboxHandler,history:HistoricalEffectDispositionService;
const actor='operator-agent@kernel',gen='operator-agent-g1';const svc=()=>repo.coordinatorAuthority;
beforeEach(async()=>{vi.useFakeTimers({toFake:['Date']});dir=mkdtempSync(join(tmpdir(),'held-adopt-'));db=createDb(join(dir,'db'));seed(db);db.prepare("INSERT INTO self_host_identity(singleton,host_id,minted_at,reconciled_at) VALUES (1,?,?,?)").run("local",new Date().toISOString(),new Date().toISOString());for(const n of ['lead@xv','peer@xv','builder@xv','reviewer@xv',actor])db.prepare('INSERT INTO bindings(id,node_id,tmux_session) VALUES (?,?,?)').run(n,n,n);repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});outbox=new OutboxHandler(db);repo.attachOutbox(outbox);history=new HistoricalEffectDispositionService(db);
 await repo.create({qitemId:'baton',sourceSession:actor,destinationSession:'lead@xv',body:'coordinate',nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'baton',destinationSession:'lead@xv'});
 await repo.create({qitemId:'work',sourceSession:'lead@xv',destinationSession:'builder@xv',body:'valuable ongoing work',nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'work',destinationSession:'builder@xv'});
 outbox.record({outboxId:'wake-intent-old',senderSession:'lead@xv',destinationSession:'builder@xv',body:'UNKNOWN prior input',auditPointer:'work'});
 const p:HistoricalPlan={rigId:'xv',leadBatonId:'baton',leadGeneration:'lead-g1',operatorGeneration:gen,operationId:'hold',authorizationId:'hold-auth',expiresAt:Date.now()+600000,effects:history.inspect('xv',['wake-intent-old'])};
 await repo.create({qitemId:'hold-auth',sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'outbox-historical-quarantine-authorization',requestDigest:historicalDigest({actor,generation:gen,input:p})}),nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'hold-auth',destinationSession:actor});history.quarantine(actor,gen,p);
});afterEach(()=>{vi.useRealTimers();db.close();rmSync(dir,{recursive:true,force:true});});
async function packet():Promise<LegacyEnrollment>{
 const inv=svc().legacyInventory('xv','authorization',true);const deadline=Date.now()+600000;
 await repo.create({qitemId:'recovery',sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'coordinator-held-history-recovery.v1',rigId:'xv',operationId:'migrate',owner:actor,generation:gen,lead:'lead@xv',leadGeneration:'lead-g1',effects:inv.heldHistory?.map(h=>h.outboxId)??['wake-intent-old'],action:'reconcile-preserved-unknown-history',deadline,returnPath:{session:'lead@xv',queueId:'recovery'}}),expiresAt:new Date(deadline).toISOString(),nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'recovery',destinationSession:actor});
 const inventory=svc().legacyInventory('xv','authorization',true);
 return {rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'migrate',authorizationId:'authorization',inventory,heldHistoryRecovery:{queueId:'recovery',rowHash:historicalDigest(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get('recovery'))},obligations:inventory.rows.map(q=>q.queueId==='work'?{queueId:'work',kind:'work' as const,evidenceRef:'exact-current-work-contract',packageKey:'p1',resourceScope:'exclusive' as const,contract:{inputDigest:digest('inputs'),bodyHash:digest('valuable ongoing work'),destination:'builder@xv',resources:['source/a'],returnContract:{destination:'lead@xv',evidenceRequired:['tests']}}}:{queueId:q.queueId,kind:'coordination' as const,evidenceRef:'actual-control-return'})};
}
async function authorize(p:LegacyEnrollment){await repo.create({qitemId:'authorization',sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'coordinator-legacy-enrollment',proposalDigest:legacyProposalDigest(p)}),identityProvenance:'transport:v1',nudge:false});repo.claim({qitemId:'authorization',destinationSession:actor});}
const migrate=(p:LegacyEnrollment)=>svc().migrateLegacy(actor,gen,p);
const original=()=>({effects:db.prepare('SELECT * FROM outbox_entries ORDER BY outbox_id').all(),work:repo.getById('work'),holds:db.prepare('SELECT * FROM outbox_historical_quarantines').all()});

// ---------------------------------------------------------------------------------------------
// Custody attestation: an adopted row whose queue item was touched (ts_updated moved) after adoption.
// ---------------------------------------------------------------------------------------------
const ROW=()=>db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-old'").get() as Record<string,unknown>;
const contained=()=>svc().isAdoptedHistoryContained('xv',ROW());
const tsOf=()=>(db.prepare("SELECT ts_updated t FROM queue_items WHERE qitem_id='work'").get() as {t:string}).t;
/** Adopt through the real migration, acknowledge, and give the item's pre-adoption history an older timestamp, as a real clock would. */
async function adopted(){
 const p=await packet();await authorize(p);migrate(p);
 db.prepare("UPDATE queue_transitions SET ts=? WHERE qitem_id='work' AND state='pending'").run(new Date(Date.parse(tsOf())-5000).toISOString());
 svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 // The holder's baton claim must be native, as the existing adoption path also requires.
 db.prepare("UPDATE queue_transitions SET identity_provenance='transport:v1' WHERE qitem_id='baton' AND transition_note='claimed'").run();
}
/** A recorded same-state touch: ts_updated moves and ONE native transition carries exactly that timestamp. */
function touch(offsetMs=60000,actorSession='builder@xv',provenance:string|null='transport:v1'){
 const later=new Date(Date.parse(tsOf())+offsetMs).toISOString();
 db.prepare("UPDATE queue_items SET ts_updated=? WHERE qitem_id='work'").run(later);
 db.prepare("INSERT INTO queue_transitions(qitem_id,ts,state,transition_note,actor_session,identity_provenance) VALUES ('work',?,'in-progress','re-asserted',?,?)").run(later,actorSession,provenance);
 return later;
}
const noteOnly=(afterMs:number)=>db.prepare("INSERT INTO queue_transitions(qitem_id,ts,state,transition_note,actor_session,identity_provenance) VALUES ('work',?,'in-progress','note',?,?)").run(new Date(Date.parse(tsOf())+afterMs).toISOString(),'builder@xv','transport:v1');
async function recoveryItem(id:string,operationId:string,effects:string[]){
 const deadline=Date.now()+600000;
 await repo.create({qitemId:id,sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'coordinator-held-history-recovery.v1',rigId:'xv',operationId,owner:actor,generation:gen,lead:'lead@xv',leadGeneration:'lead-g1',effects,action:'reconcile-preserved-unknown-history',deadline,returnPath:{session:'lead@xv',queueId:id}}),expiresAt:new Date(deadline).toISOString(),nudge:false,identityProvenance:'transport:v1'});
 repo.claim({qitemId:id,destinationSession:actor,identityProvenance:'transport:v1'});
 return {queueId:id,rowHash:historicalDigest(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(id))};
}
const describe1=()=>(svc().describeHeldHistoryCustody(actor,gen,{rigId:'xv',outboxIds:['wake-intent-old']}) as {effects:Array<any>}).effects[0];
async function request(operationId:string){
 const e=describe1();expect(e.ok).toBe(true);
 const a=svc().get('xv')!;
 return {rigId:'xv',operationId,expected:{rigId:'xv',epoch:a.epoch,generation:a.owner_generation},effects:[e.evidence],recovery:await recoveryItem('rec-'+operationId,operationId,['wake-intent-old'])};
}
const attest=(r:any)=>svc().attestHeldHistoryCustody(actor,gen,r);
const attestationRows=()=>(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind IN ('held-history-custody-attestation','held-history-custody-attestation-effect')").get() as {n:number}).n;
const immutable=()=>({held:db.prepare('SELECT * FROM coordinator_held_history ORDER BY outbox_id').all(),outbox:db.prepare('SELECT * FROM outbox_entries ORDER BY outbox_id').all(),holds:db.prepare('SELECT * FROM outbox_historical_quarantines').all(),ops:db.prepare('SELECT * FROM outbox_historical_operations').all(),work:db.prepare("SELECT * FROM queue_items WHERE qitem_id='work'").get(),assignments:db.prepare('SELECT * FROM coordinator_assignments').all(),resources:db.prepare('SELECT * FROM coordinator_resources').all()});

it('timestamp-only drift is attested from a recorded native cause: containment returns, UNKNOWN and every original row are untouched, replay is exact',async()=>{
 await adopted();expect(contained()).toBe(true);
 const frozenTs=tsOf(),later=touch();
 expect(contained()).toBe(false);expect(()=>svc().heldHistoryAuthoringSnapshot('xv')).toThrow('Immutable adopted containment changed');
 const e=describe1();expect(e).toMatchObject({ok:true,evidence:{outboxId:'wake-intent-old',drift:[{path:'queue.ts_updated',frozen:frozenTs,current:later}],cause:{queueId:'work',ts:later,state:'in-progress',actorSession:'builder@xv',identityProvenance:'transport:v1'}}});
 const r=await request('attest1');const stateBefore=ROW().delivery_state;expect(['pending','indeterminate']).toContain(stateBefore);const before=immutable(),opsBefore=db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number};
 const receipt=attest(r) as any;
 expect(receipt).toMatchObject({kind:'coordinator-held-history-custody-attestation.v1',actor,deliveryConclusion:'unknown',originalMutations:0});
 expect(contained()).toBe(true);expect(svc().heldHistoryAuthoringSnapshot('xv')).toHaveLength(1);
 expect(immutable()).toEqual(before);                                   // nothing original was written
 expect((db.prepare('SELECT count(*) n FROM coordinator_operations').get() as {n:number}).n).toBe(opsBefore.n+2); // attestation + one lookup marker
 expect(ROW().delivery_state).toBe(stateBefore);expect(ROW().delivery_state).not.toBe('delivered'); // UNKNOWN preserved, never delivered
 expect(attest(r)).toEqual(receipt);expect(attestationRows()).toBe(2);   // exact replay writes nothing
 noteOnly(90000);expect(contained()).toBe(true);                         // later note-only transitions leave the bytes unchanged
 expect(()=>attest({...r,effects:[{...r.effects[0],history:{...r.effects[0].history,count:1}}]})).toThrow('Operation ID reused');
});

it('a later touch voids the attestation until the new exact bytes are attested; an old attestation never matches new bytes',async()=>{
 await adopted();touch();attest(await request('first'));expect(contained()).toBe(true);
 touch(120000);expect(contained()).toBe(false);
 const r2=await request('second');attest(r2);expect(contained()).toBe(true);
 expect(()=>attest(r2)).not.toThrow();expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='held-history-custody-attestation'").get()).toEqual({n:2});
});

it('undrifted, unadopted and unknown rows are refused with typed reasons',async()=>{
 await adopted();
 expect(describe1()).toMatchObject({ok:false,code:'custody_attestation_not_needed'});
 outbox.record({outboxId:'plain-uncontained',senderSession:'lead@xv',destinationSession:'builder@xv',body:'x'});
 const d=(svc().describeHeldHistoryCustody(actor,gen,{rigId:'xv',outboxIds:['plain-uncontained','no-such']}) as any).effects;
 expect(d.map((x:any)=>x.code)).toEqual(['custody_attestation_not_adopted','custody_attestation_unknown_effect']);
});

it.each([
 ['no recorded transition at the timestamp',()=>{db.prepare("UPDATE queue_items SET ts_updated=? WHERE qitem_id='work'").run(new Date(Date.parse(tsOf())+60000).toISOString());},'custody_attestation_cause_missing'],
 ['a transition one millisecond away',()=>{const t=touch();db.prepare("UPDATE queue_transitions SET ts=? WHERE qitem_id='work' AND ts=?").run(new Date(Date.parse(t)+1).toISOString(),t);},'custody_attestation_cause_missing'],
 ['a cause without native provenance',()=>{touch(60000,'builder@xv',null);},'custody_attestation_cause_missing'],
 ['a state cycle after the frozen timestamp',()=>{touch();db.prepare("INSERT INTO queue_transitions(qitem_id,ts,state,transition_note,actor_session,identity_provenance) VALUES ('work',?,'pending','released','builder@xv','transport:v1')").run(new Date(Date.parse(tsOf())-1000).toISOString());},'custody_attestation_state_cycle'],
 ['any non-timestamp custody change',()=>{touch();db.prepare("UPDATE queue_items SET body='changed' WHERE qitem_id='work'").run();},'custody_attestation_not_timestamp_only'],
 ['a timestamp not later than the frozen one',()=>{db.prepare("UPDATE queue_items SET ts_updated=? WHERE qitem_id='work'").run(new Date(Date.parse(tsOf())-1000).toISOString());},'custody_attestation_timestamp_invalid'],
] as Array<[string,()=>void,string]>)('refuses %s and writes nothing',async(_l,mutate,code)=>{
 await adopted();mutate();expect(describe1()).toMatchObject({ok:false,code});
 const a=svc().get('xv')!,r={rigId:'xv',operationId:'bad',expected:{rigId:'xv',epoch:a.epoch,generation:a.owner_generation},effects:[{outboxId:'wake-intent-old'} as any],recovery:await recoveryItem('rec-bad','bad',['wake-intent-old'])};
 const before=immutable();let thrown:any;try{attest(r);}catch(e){thrown=e;}expect(thrown?.code).toBe(code);expect(immutable()).toEqual(before);expect(attestationRows()).toBe(0);expect(contained()).toBe(false);
});

it('tampered or stale evidence, missing authority and bad recovery claims refuse atomically',async()=>{
 await adopted();touch();const r=await request('auth');const before=immutable();
 const deny=(req:any,actorName=actor,g=gen,msg?:string)=>{expect(()=>svc().attestHeldHistoryCustody(actorName,g,req)).toThrow(msg);expect(attestationRows()).toBe(0);expect(contained()).toBe(false);};
 deny({...r,effects:[{...r.effects[0],history:{...r.effects[0].history,digest:'0'.repeat(64)}}]},actor,gen,'differs from the live');
 deny({...r,effects:[{...r.effects[0],attestedCustodyHash:'1'.repeat(64)}]},actor,gen,'differs from the live');
 deny({...r,effects:[{...r.effects[0],cause:{...r.effects[0].cause,transitionId:r.effects[0].cause.transitionId+1}}]},actor,gen,'differs from the live');
 deny(r,'lead@xv','lead-g1','Kernel Operator');
 deny(r,actor,'operator-agent-g0','generation');
 deny({...r,expected:{...r.expected,epoch:r.expected.epoch+1}},actor,gen,'no longer holds');
 deny({...r,effects:[r.effects[0],r.effects[0]]},actor,gen,'Exact bounded');
 deny({...r,recovery:{...r.recovery,rowHash:'changed'}},actor,gen,'Finite current Lead-authored');
 const other=await recoveryItem('rec-other','a-different-operation',['wake-intent-old']);deny({...r,recovery:other},actor,gen,'Finite current Lead-authored');  // recovery bound to another operation
 db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL WHERE qitem_id='rec-auth'").run();deny(r,actor,gen,'Finite current Lead-authored');  // recovery no longer claimed
 expect(immutable().held).toEqual(before.held);
 // Touch after describing: the live bytes moved, so the old evidence is refused.
 db.prepare("UPDATE queue_items SET state='in-progress',claimed_by_generation_uuid=? WHERE qitem_id='rec-auth'").run(gen);touch(180000);deny(r,actor,gen,'differs from the live');
});

it('adoption stays single-shot and immutable: re-adopting an attested row is refused and the frozen custody hash cannot be edited',async()=>{
 await adopted();touch();attest(await request('keep'));
 const frozen=db.prepare("SELECT post_custody_hash FROM coordinator_held_history").get() as {post_custody_hash:string};
 const a=svc().get('xv')!,ref=svc().heldHistoryAuthoringSnapshot('xv');
 expect(()=>svc().adoptHeldHistory(actor,gen,{rigId:'xv',operationId:'again',expected:{rigId:'xv',epoch:a.epoch,generation:a.owner_generation},effects:ref,recovery:{queueId:'rec-keep',rowHash:'x'}})).toThrow();
 expect(()=>db.prepare("UPDATE coordinator_held_history SET post_custody_hash='forged'").run()).toThrow('immutable');
 expect(()=>db.prepare("DELETE FROM coordinator_held_history").run()).toThrow('immutable');
 expect(db.prepare("SELECT post_custody_hash FROM coordinator_held_history").get()).toEqual(frozen);expect(contained()).toBe(true);
});
