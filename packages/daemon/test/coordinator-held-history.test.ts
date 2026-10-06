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
it('explicit contained history imports actual claimed work and recovery, ACK and independent transport pickup without history replay',async()=>{
 const p=await packet();await authorize(p);const before=original();expect(migrate(p).state).toBe('reconciling');expect(original()).toEqual(before);expect(migrate(p).epoch).toBe(1);
 const debt=db.prepare('SELECT * FROM coordinator_held_history').get() as any;expect(debt.pre_custody_hash).not.toBe(debt.post_custody_hash);expect(JSON.parse(debt.receipt)).toMatchObject({deliveryConclusion:'unknown',originalMutations:0,recovery:{queueId:'recovery'}});
 svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 const contract={inputDigest:digest('independent'),destination:'reviewer@xv',bodyHash:digest('ready independent work'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['review']}};svc().admit(actor,gen,'xv','ready',contract);
 const pickups:string[]=[];repo.attachTransport({send:async(_s,_b,opts)=>{for(const id of opts!.committedOutboxIds!){const e=outbox.getById(id)!;if(e.auditPointer){repo.claim({qitemId:e.auditPointer,destinationSession:e.destinationSession});pickups.push(e.auditPointer);}}return {ok:true,verified:true};}});
 const q=await repo.create({qitemId:'ready-work',sourceSession:'lead@xv',destinationSession:'reviewer@xv',body:'ready independent work',dispatch:{token,packageKey:'ready'},nudge:true});await repo.drainPendingWakeIntents();expect(pickups).toContain(q.qitemId);expect(repo.getById(q.qitemId)?.state).toBe('in-progress');expect(outbox.getById('wake-intent-old')?.deliveryState).toBe('pending');expect(outbox.isHistoricalQuarantined('wake-intent-old')).toBe(true);
});
it('default inventory and enable remain refusing; bare or changed quarantine provenance never qualifies containment',async()=>{
 expect(svc().legacyInventory('xv','authorization').uncertainEffects).toHaveLength(1);expect(()=>svc().enable(actor,gen,{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'})).toThrow();
 for(const sql of ["DELETE FROM outbox_historical_operations","UPDATE outbox_historical_quarantines SET original_hash='forged'","UPDATE outbox_historical_operations SET receipt='null'"]){db.exec('SAVEPOINT trial');db.exec(sql);expect(svc().legacyInventory('xv','authorization',true).uncertainEffects).toHaveLength(1);db.exec('ROLLBACK TO trial');db.exec('RELEASE trial');}
});
it('mixed held plus uncontained effect refuses atomically',async()=>{outbox.record({outboxId:'uncontained',senderSession:'lead@xv',destinationSession:'builder@xv',body:'unknown uncontained'});const p=await packet();await authorize(p);const before=original();expect(()=>migrate(p)).toThrow('Uncertain delivery');expect(original()).toEqual(before);expect(svc().get('xv')).toBeUndefined();expect(db.prepare('SELECT count(*) n FROM coordinator_held_history').get()).toEqual({n:0});});
it('current source/hold/custody/generation/expiry/recovery claim drift refuses without partial adoption',async()=>{
 for(const kind of ['body','hold','claim','operator','expiry','recovery-claim','recovery-purpose']){
  const p=await packet();await authorize(p);db.exec('SAVEPOINT trial');
  if(kind==='body')db.prepare("UPDATE outbox_entries SET body='changed'").run();if(kind==='hold')db.prepare("UPDATE outbox_historical_quarantines SET state='disposed'").run();if(kind==='claim')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='old' WHERE qitem_id='work'").run();if(kind==='operator')db.prepare("UPDATE occupant_tenures SET generation_uuid='new' WHERE node_id=?").run(actor);if(kind==='expiry')db.prepare("UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id='recovery'").run();if(kind==='recovery-claim')db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id='recovery'").run();if(kind==='recovery-purpose')db.prepare("UPDATE queue_items SET body='{}' WHERE qitem_id='recovery'").run();
  const before=original();expect(()=>migrate(p)).toThrow();expect(original()).toEqual(before);expect(svc().get('xv')).toBeUndefined();expect(db.prepare('SELECT count(*) n FROM coordinator_held_history').get()).toEqual({n:0});db.exec('ROLLBACK TO trial');db.exec('RELEASE trial');db.prepare("DELETE FROM queue_transitions WHERE qitem_id IN ('authorization','recovery')").run();db.prepare("DELETE FROM queue_items WHERE qitem_id IN ('authorization','recovery')").run();
 }
});
it('custody mapping is exact and resource overlap rolls back adoption and preserves original claims',async()=>{
 await repo.create({qitemId:'other-work',sourceSession:'lead@xv',destinationSession:'reviewer@xv',body:'review',nudge:false});const p=await packet();p.obligations=p.obligations.map(o=>o.queueId==='other-work'?{queueId:o.queueId,kind:'work',evidenceRef:'review',packageKey:'p2',resourceScope:'exclusive',contract:{inputDigest:digest('i2'),bodyHash:digest('review'),destination:'reviewer@xv',resources:['source/a'],returnContract:{destination:'lead@xv',evidenceRequired:['review']}}}:o);await authorize(p);const before=original();expect(()=>migrate(p)).toThrow('overlaps');expect(original()).toEqual(before);for(const t of ['coordinator_held_history','coordinator_authority','coordinator_resources','coordinator_assignments'])expect(db.prepare(`SELECT count(*) n FROM ${t}`).get()).toEqual({n:0});
});
it('restart retains non-executable immutable held debt and supports existing guarded retirement order',async()=>{
 const p=await packet();await authorize(p);migrate(p);const guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));const freshSvc=new CoordinatorAuthorityService(db),freshOutbox=new OutboxHandler(db);expect(freshSvc.get('xv')?.state).toBe('reconciling');expect(freshOutbox.listPending('wake-intent-')).toEqual([]);expect(freshOutbox.claimForDelivery('wake-intent-old')).toBe(false);
 for(const sql of ["DELETE FROM outbox_historical_quarantines","UPDATE outbox_historical_quarantines SET state='disposed'","UPDATE outbox_entries SET delivery_state='sending'","UPDATE outbox_entries SET body='new'","DELETE FROM outbox_entries","DELETE FROM coordinator_held_history","UPDATE outbox_historical_operations SET receipt='{}'","DELETE FROM outbox_historical_operations"]){expect(()=>db.exec(sql)).toThrow(/protected|immutable/);}
 // Existing supported service retirement order must remain possible, not a trigger-only raw state rewrite.
 const ref=history.inspect('xv',['wake-intent-old'])[0]!;const d:HistoricalDisposition={rigId:'xv',leadBatonId:'baton',leadGeneration:'lead-g1',operatorGeneration:gen,operationId:'withdraw',authorizationId:'dispose-auth',expiresAt:Date.now()+600000,quarantineOperationId:'hold',effect:ref,action:'withdraw-obsolete-wake',reason:'Current work retained; withdraw only exact old intention',evidenceRef:'current-custodian-decision'};
 const body=JSON.stringify({kind:'outbox-historical-disposition-authorization',requestDigest:historicalDigest({actor,generation:gen,input:d})});
 svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 await repo.create({qitemId:'dispose-auth',sourceSession:'lead@xv',destinationSession:actor,body,nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'dispose-auth',destinationSession:actor});
 expect(d.effect.custodyHash).not.toBe(p.inventory.heldHistory![0]!.custodyHash);expect(await history.dispose(actor,gen,d,guard)).toMatchObject({deliveryConclusion:'unknown',queueMutations:0});expect(outbox.getById('wake-intent-old')?.deliveryState).toBe('retired');expect(db.prepare('SELECT count(*) n FROM coordinator_held_history').get()).toEqual({n:1});
});
it('other rig continues while only exactly adopted held debt permits genuine unavailable-owner transfer',async()=>{
 const p=await packet();await authorize(p);migrate(p);const q=await repo.create({qitemId:'other-ready',sourceSession:'worker@other',destinationSession:actor,body:'independent result',nudge:false});expect(q.qitemId).toBe('other-ready');expect(svc().legacyInventory('other','none',true).heldHistory).toEqual([]);
 svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 let clock=Date.now();
 const sample=(session:string):CoordinationActivity=>({generation:svc().generation(session)!,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:null},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}});
 const recovery=new CoordinationRecoveryService(repo,s=>sample(s),()=>clock);svc().coordinationRecovery=recovery;
 const tasks=['reviewer@xv','architect@xv'].map((owner,i)=>({key:'task'+i,packageKey:'task'+i,owner,action:'Bounded review/recovery',body:'task'+i,deadline:clock+60000,predecessors:[],...(i?{recoveryFor:'task0'}:{}),admission:{generation:svc().generation(owner)!,configurationDigest:recovery.configurationDigest(owner)!,qualificationRef:'independent-current',capacityRef:'current',effortRef:'current',validUntil:clock+60000}}));
 for(const t of tasks)svc().admit(actor,gen,'xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 recovery.configure(actor,gen,{rigId:'xv',revision:'current',operatorGeneration:gen,stallMs:10000,allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true,tasks});
 db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j',?,'coordinator-continuity',1,'context: {}','active',?,?,?)").run(actor,actor,new Date(clock).toISOString(),gen);
 svc().renew('lead@xv',token,1000,'short');clock+=1001;vi.setSystemTime(clock);svc().setRuntimeObserver(async session=>({session,generation:svc().generation(session)!,state:session==='lead@xv'?'absent':'present',observedAt:clock,fingerprint:'actual-fixture-native-census'}));await svc().refreshRuntimeAvailability('xv');
 const input={rigId:'xv',expectedEpoch:1,expectedOwner:'lead@xv',expectedOwnerGeneration:'lead-g1',planRevision:'current',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:60000};
 outbox.record({outboxId:'fresh-uncertain',senderSession:'lead@xv',destinationSession:'builder@xv',body:'not adopted'});expect(()=>svc().transferObservedUnavailable('j',input)).toThrow('Uncertain effects');expect(svc().get('xv')?.epoch).toBe(1);
 // Supported exact unrelated effect state resolution is fixture setup only; adopted held original stays unchanged.
 outbox.markFailed('fresh-uncertain');
 // Terminal or expired accountability is a takeover hold, not global dispatch suspension.
 for(const sql of ["UPDATE queue_items SET state='done' WHERE qitem_id='recovery'","UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id='recovery'"]){db.exec('SAVEPOINT accountability');db.exec(sql);expect(()=>svc().transferObservedUnavailable('j',input)).toThrow('held-history-recovery-bind');expect(svc().get('xv')?.epoch).toBe(1);db.exec('ROLLBACK TO accountability');db.exec('RELEASE accountability');}
 db.prepare("UPDATE queue_items SET state='done' WHERE qitem_id='recovery'").run();
 const refs=svc().legacyInventory('xv','new-auth',true).heldHistory!;const deadline=clock+600000;
 const body=JSON.stringify({kind:'coordinator-held-history-recovery.v1',rigId:'xv',operationId:'rebind',owner:actor,generation:gen,lead:'lead@xv',leadGeneration:'lead-g1',effects:refs.map(h=>h.outboxId),action:'reconcile-preserved-unknown-history',deadline,returnPath:{session:'lead@xv',queueId:'recovery-new'}});
 await repo.create({qitemId:'recovery-new',sourceSession:'lead@xv',destinationSession:actor,body,expiresAt:new Date(deadline).toISOString(),nudge:false,identityProvenance:'transport:v1'});
 const bind=()=>({rigId:'xv',operationId:'rebind',effects:refs,recovery:{queueId:'recovery-new',rowHash:historicalDigest(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get('recovery-new'))}});
 expect(()=>svc().bindHeldHistoryRecovery(actor,gen,bind())).toThrow('actual Operator claimed');repo.claim({qitemId:'recovery-new',destinationSession:actor});const bound=bind();expect(svc().bindHeldHistoryRecovery(actor,gen,bound)).toMatchObject({originalMutations:0});expect(svc().bindHeldHistoryRecovery(actor,gen,bound)).toMatchObject({originalMutations:0});
 db.exec('SAVEPOINT expiredReplay');db.prepare("UPDATE queue_items SET state='done' WHERE qitem_id='recovery-new'").run();expect(()=>svc().bindHeldHistoryRecovery(actor,gen,bound)).toThrow();db.exec('ROLLBACK TO expiredReplay');db.exec('RELEASE expiredReplay');
 expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='held-history-recovery-binding'").get()).toEqual({n:1});
 const before=original();expect(svc().transferObservedUnavailable('j',input)).toMatchObject({epoch:2,state:'reconciling',owner_session:'peer@xv'});expect(repo.getById('work')).toEqual(before.work);expect(outbox.getById('wake-intent-old')?.deliveryState).toBe('pending');expect(repo.getById('baton')?.state).toBe('pending');repo.claim({qitemId:'baton',destinationSession:'peer@xv',identityProvenance:'transport:v1'});svc().acknowledge('peer@xv',{...token,epoch:2,generation:'peer-g1'},{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'peer-ack'});expect(svc().get('xv')?.state).toBe('active');
});

it('expired history recovery is renewed only from genuine unavailable-owner supervise provenance and actual Operator pickup',async()=>{
 const p=await packet();await authorize(p);migrate(p);const q=await repo.create({qitemId:'other-ready',sourceSession:'worker@other',destinationSession:actor,body:'independent result',nudge:false});expect(q.qitemId).toBe('other-ready');expect(svc().legacyInventory('other','none',true).heldHistory).toEqual([]);
 svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 let clock=Date.now();
 const sample=(session:string):CoordinationActivity=>({generation:svc().generation(session)!,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:null},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}});
 const recovery=new CoordinationRecoveryService(repo,s=>sample(s),()=>clock);svc().coordinationRecovery=recovery;
 const tasks=['reviewer@xv','architect@xv'].map((owner,i)=>({key:'task'+i,packageKey:'task'+i,owner,action:'Bounded review/recovery',body:'task'+i,deadline:clock+60000,predecessors:[],...(i?{recoveryFor:'task0'}:{}),admission:{generation:svc().generation(owner)!,configurationDigest:recovery.configurationDigest(owner)!,qualificationRef:'independent-current',capacityRef:'current',effortRef:'current',validUntil:clock+60000}}));
 for(const t of tasks)svc().admit(actor,gen,'xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 recovery.configure(actor,gen,{rigId:'xv',revision:'current',operatorGeneration:gen,stallMs:10000,allowIdlePeerTransfer:false,allowUnavailablePeerTransfer:true,tasks});
 db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j',?,'coordinator-continuity',1,'context: {}','active',?,?,?)").run(actor,actor,new Date(clock).toISOString(),gen);
 svc().renew('lead@xv',token,1000,'short');clock+=1001;vi.setSystemTime(clock);svc().setRuntimeObserver(async session=>({session,generation:svc().generation(session)!,state:session==='lead@xv'?'absent':'present',observedAt:clock,fingerprint:'actual-fixture-native-census'}));await svc().refreshRuntimeAvailability('xv');
 const input={rigId:'xv',expectedEpoch:1,expectedOwner:'lead@xv',expectedOwnerGeneration:'lead-g1',planRevision:'current',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:60000};
 db.prepare("UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id='recovery'").run();
 const originalBefore=original();const held=recovery.supervise('xv','j')!;expect(held[0]).toMatchObject({state:'held',reason:'coordinator_held_history_recovery_required'});
 const id=held[0]!.queueId!;const task=repo.getById(id)!;expect(task.state).toBe('pending');expect(JSON.parse(task.body).heldHistoryAdmission).toMatchObject({jobId:'j',epoch:1,owner:'lead@xv',ownerGeneration:'lead-g1',planRevision:'current'});
 const bind=()=>({rigId:'xv',operationId:'observer-renewal',effects:svc().legacyInventory('xv','none',true).heldHistory!,recovery:{queueId:id,rowHash:historicalDigest(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(id))}});
 expect(()=>svc().bindHeldHistoryRecovery(actor,gen,bind())).toThrow('Actual current Operator claim');
 // Supported committed transport delivery precedes the actual recipient claim.
 repo.attachTransport({send:async()=>({ok:true,verified:true})});await recovery.deliverCommitted();repo.claim({qitemId:id,destinationSession:actor,identityProvenance:'transport:v1'});
 const bound=bind();
 for(const kind of ['owner-present','plan','job','operator','expiry','claimed','receipt','held-ref']){db.exec('SAVEPOINT refused');
   if(kind==='owner-present'){svc().setRuntimeObserver(async session=>({session,generation:svc().generation(session)!,state:'present',observedAt:clock,fingerprint:'live-returning-owner'}));await svc().refreshRuntimeAvailability('xv');}
   if(kind==='plan')db.prepare("UPDATE coordinator_operations SET receipt=json_set(receipt,'$.revision','changed') WHERE rig_id='xv' AND kind='coordination-plan'").run();
   if(kind==='job')db.prepare("UPDATE watchdog_jobs SET state='stopped' WHERE job_id='j'").run();
   if(kind==='operator')db.prepare("UPDATE occupant_tenures SET generation_uuid='retired' WHERE node_id=?").run(actor);
   if(kind==='expiry')db.prepare("UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id=?").run(id);
   if(kind==='claimed')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='retired' WHERE qitem_id=?").run(id);
   if(kind==='receipt')db.prepare("UPDATE coordinator_operations SET receipt='{}' WHERE kind='held-history-recovery-notice'").run();
   const request=kind==='held-ref'?{...bound,effects:[]}:['expiry','claimed'].includes(kind)?{...bound,recovery:bind().recovery}:bound;expect(()=>svc().bindHeldHistoryRecovery(actor,gen,request)).toThrow();expect(svc().get('xv')?.epoch).toBe(1);db.exec('ROLLBACK TO refused');db.exec('RELEASE refused');
   svc().setRuntimeObserver(async session=>({session,generation:svc().generation(session)!,state:session==='lead@xv'?'absent':'present',observedAt:clock,fingerprint:'actual-fixture-native-census'}));await svc().refreshRuntimeAvailability('xv');
 }
 expect(svc().bindHeldHistoryRecovery(actor,gen,bound)).toMatchObject({originalMutations:0});expect(svc().bindHeldHistoryRecovery(actor,gen,bound)).toMatchObject({originalMutations:0});
 db.exec('SAVEPOINT staleReplay');db.prepare("UPDATE queue_items SET state='done' WHERE qitem_id=?").run(id);expect(()=>svc().bindHeldHistoryRecovery(actor,gen,bound)).toThrow();db.exec('ROLLBACK TO staleReplay');db.exec('RELEASE staleReplay');
 const after=recovery.supervise('xv','j')!;expect(after[0]).toMatchObject({state:'pending-peer-acknowledgment',reason:'fresh-native-unavailable-owner'});expect(svc().get('xv')?.epoch).toBe(2);expect(repo.getById('work')).toEqual(originalBefore.work);expect(outbox.getById('wake-intent-old')).toMatchObject({deliveryState:'pending'});
 expect(svc().bindHeldHistoryRecovery.bind(svc(),actor,gen,bound)).toThrow();
 expect(repo.getById('baton')?.state).toBe('pending');repo.claim({qitemId:'baton',destinationSession:'peer@xv',identityProvenance:'transport:v1'});svc().acknowledge('peer@xv',{...token,epoch:2,generation:'peer-g1'},{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'peer-ack'});expect(svc().get('xv')?.state).toBe('active');
});

it('automatic frontier ignores only exact adopted contained Peer return while preserving UNKNOWN and ordinary dispatch across expired recovery',async()=>{
 const oldReturn=await repo.create({qitemId:'old-peer-return',sourceSession:'peer@xv',destinationSession:'reviewer@xv',body:'Prior bounded peer result retained',nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:oldReturn.qitemId,destinationSession:'reviewer@xv',identityProvenance:'transport:v1'});repo.update({qitemId:oldReturn.qitemId,actorSession:'reviewer@xv',state:'done',closureReason:'no-follow-on'});
 outbox.record({outboxId:'peer-held-return',senderSession:'peer@xv',destinationSession:'reviewer@xv',body:'Prior bounded peer result retained',auditPointer:oldReturn.qitemId});
 const hp:HistoricalPlan={rigId:'xv',leadBatonId:'baton',leadGeneration:'lead-g1',operatorGeneration:gen,operationId:'peer-hold',authorizationId:'peer-hold-auth',expiresAt:Date.now()+600000,effects:history.inspect('xv',['peer-held-return'])};
 await repo.create({qitemId:'peer-hold-auth',sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'outbox-historical-quarantine-authorization',requestDigest:historicalDigest({actor,generation:gen,input:hp})}),nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'peer-hold-auth',destinationSession:actor});history.quarantine(actor,gen,hp);
 const p=await packet();await authorize(p);migrate(p);svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 const clock=Date.now();const sample=(session:string):CoordinationActivity=>({generation:svc().generation(session)!,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:null},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}});
 const recovery=new CoordinationRecoveryService(repo,s=>sample(s),()=>clock);svc().coordinationRecovery=recovery;
 const tasks:any[]=[{key:'peer-ready',packageKey:'peer-ready',owner:'reviewer@xv',action:'Independent bounded result',body:'Compute a bounded result',deadline:clock+60000,predecessors:[],admission:{generation:'reviewer-g1',configurationDigest:recovery.configurationDigest('reviewer@xv')!,qualificationRef:'current-bounded',capacityRef:'current',effortRef:'current',validUntil:clock+60000}}];
 svc().admit(actor,gen,'xv','peer-ready',{inputDigest:digest('peer-ready'),destination:'reviewer@xv',bodyHash:digest(tasks[0]!.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 tasks.push({...tasks[0],key:'peer-ready-repair',packageKey:'peer-ready-repair',owner:'architect@xv',recoveryFor:'peer-ready',admission:{...tasks[0].admission,generation:'architect-g1',configurationDigest:recovery.configurationDigest('architect@xv')!}});svc().admit(actor,gen,'xv','peer-ready-repair',{inputDigest:digest('peer-ready-repair'),destination:'architect@xv',bodyHash:digest(tasks[1].body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 recovery.configure(actor,gen,{rigId:'xv',revision:'peer-ready-plan',operatorGeneration:gen,stallMs:10000,allowIdlePeerTransfer:false,tasks});
 db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('worker-job',?,'coordinator-continuity',1,'context: {}','active',?,?,?)").run(actor,actor,new Date(clock).toISOString(),gen);
 // Deliberate corrupt fixtures use reversible savepoints; production trigger guards stay enabled.
 for(const kind of ['missing-adoption','foreign-adoption','missing-hold','changed-ledger','changed-receipt','changed-custody','changed-operation','changed-hold','current-uncertain']){db.exec('SAVEPOINT rejection');
  if(kind==='missing-adoption'||kind==='foreign-adoption'){db.exec('DROP TRIGGER coordinator_held_no_delete');if(kind==='foreign-adoption')db.exec("INSERT INTO coordinator_held_history SELECT 'other',outbox_id,migration_operation_id,original_row_hash,quarantine_hash,quarantine_operation_hash,pre_custody_hash,post_custody_hash,recovery_queue_id,receipt FROM coordinator_held_history WHERE outbox_id='peer-held-return'");db.prepare("DELETE FROM coordinator_held_history WHERE rig_id='xv' AND outbox_id='peer-held-return'").run();}
  if(kind==='missing-hold'){db.exec('DROP TRIGGER coordinator_held_quarantine_no_delete');db.prepare("DELETE FROM outbox_historical_quarantines WHERE outbox_id='peer-held-return'").run();}
  if(kind==='changed-ledger'||kind==='changed-receipt'){db.exec('DROP TRIGGER coordinator_held_no_update');db.prepare("UPDATE coordinator_held_history SET "+(kind==='changed-ledger'?"original_row_hash='changed'":"receipt='{}'")+" WHERE outbox_id='peer-held-return'").run();}
  if(kind==='changed-operation'){db.exec('DROP TRIGGER coordinator_held_operation_no_update');db.prepare("UPDATE outbox_historical_operations SET receipt='null' WHERE operation_id='peer-hold'").run();}
  if(kind==='changed-hold'){db.exec('DROP TRIGGER coordinator_held_quarantine_no_drift');db.prepare("UPDATE outbox_historical_quarantines SET original_hash='changed' WHERE outbox_id='peer-held-return'").run();}
  if(kind==='changed-custody')db.prepare("UPDATE queue_items SET summary='changed prior custody' WHERE qitem_id='old-peer-return'").run();
  if(kind==='current-uncertain')outbox.record({outboxId:'peer-current-uncertain',senderSession:'reviewer@xv',destinationSession:'lead@xv',body:'Fresh uncertain input'});
  expect(recovery.supervise('xv','worker-job')?.find(r=>r.key==='peer-ready')).toMatchObject({state:'held',reason:'uncertain-worker-effect'});expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='peer-ready'").get()).toEqual({n:0});db.exec('ROLLBACK TO rejection');db.exec('RELEASE rejection');
 }
 db.prepare("UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id='recovery'").run();const preserved=original();const priorReturn=repo.getById('old-peer-return');
 const pickups:string[]=[];repo.attachTransport({send:async(_s,_body,opts)=>{for(const id of opts!.committedOutboxIds!){const e=outbox.getById(id)!;if(e.auditPointer){repo.claim({qitemId:e.auditPointer,destinationSession:e.destinationSession,identityProvenance:'transport:v1'});pickups.push(e.auditPointer);}}return {ok:true,verified:true};}});
 const r=recovery.supervise('xv','worker-job')!.find(r=>r.key==='peer-ready')!;expect(r.state).toBe('pending-pickup');await recovery.deliverCommitted();expect(pickups).toContain(r.queueId);expect(repo.getById(r.queueId!)?.state).toBe('in-progress');expect(recovery.supervise('xv','worker-job')?.find(x=>x.key==='peer-ready')?.state).toBe('picked-up');
 repo.update({qitemId:r.queueId!,actorSession:'reviewer@xv',state:'done',closureReason:'no-follow-on'});await repo.create({qitemId:'peer-ready-return',sourceSession:'reviewer@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'peer-ready',inputDigest:digest('peer-ready'),evidence:[{kind:'report',ref:'bounded/peer-ready.md'}]}),nudge:false,identityProvenance:'transport:v1'});svc().dispose('reviewer@xv','reviewer-g1','xv','peer-ready','peer-ready-return');recovery.accept('lead@xv','lead-g1','xv','peer-ready','peer-ready-return','bounded/accepted-peer-ready.md');
 expect(repo.getById('old-peer-return')).toEqual(priorReturn);expect(outbox.getById('peer-held-return')?.deliveryState).toBe('pending');expect(original().work).toEqual(preserved.work);expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id IN ('wake-intent-old','peer-held-return') ORDER BY outbox_id").all()).toEqual((preserved.effects as any[]).filter(e=>['wake-intent-old','peer-held-return'].includes(e.outbox_id)));expect(outbox.isHistoricalQuarantined('peer-held-return')).toBe(true);
});

it('expired held-history binding holds idle takeover without aborting independent ready work',async()=>{
 const p=await packet();await authorize(p);migrate(p);svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack'});
 let clock=Date.now();
 const sample=(session:string):CoordinationActivity=>({generation:svc().generation(session)!,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:null},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}});
 const recovery=new CoordinationRecoveryService(repo,s=>sample(s),()=>clock);svc().coordinationRecovery=recovery;
 const tasks=['reviewer@xv','architect@xv'].map((owner,i)=>({key:'task'+i,packageKey:'task'+i,owner,action:'Bounded review/recovery',body:'task'+i,deadline:clock+60000,predecessors:[],...(i?{recoveryFor:'task0'}:{}),admission:{generation:svc().generation(owner)!,configurationDigest:recovery.configurationDigest(owner)!,qualificationRef:'independent-current',capacityRef:'current',effortRef:'current',validUntil:clock+60000}}));
 for(const t of tasks)svc().admit(actor,gen,'xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 recovery.configure(actor,gen,{rigId:'xv',revision:'current',operatorGeneration:gen,stallMs:10000,allowIdlePeerTransfer:true,allowUnavailablePeerTransfer:false,tasks});
 db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('j',?,'coordinator-continuity',1,'context: {}','active',?,?,?)").run(actor,actor,new Date(clock).toISOString(),gen);

 clock+=11000;vi.setSystemTime(clock);db.prepare("UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id='recovery'").run();
 const before=original();const results=recovery.supervise('xv','j')!;
 expect(results.find(r=>r.key==='coordinator')).toMatchObject({state:'held',reason:'coordinator_held_history_recovery_required'});
 const notice=repo.getById(results.find(r=>r.key==='coordinator')!.queueId!)!;expect(notice.destinationSession).toBe(actor);expect(JSON.parse(notice.body)).toMatchObject({action:'restore-current-held-history-binding',recipientGeneration:gen});expect(JSON.parse(notice.body).heldHistoryAdmission).toBeUndefined();expect(JSON.parse(notice.body).nextAction).toContain('actual current Lead');
 expect(results.find(r=>r.key==='task0')?.state).toBe('pending-pickup');expect(svc().get('xv')?.epoch).toBe(1);expect(repo.getById('baton')?.destinationSession).toBe('lead@xv');
 const after=original();expect(after.work).toEqual(before.work);expect(after.holds).toEqual(before.holds);expect(after.effects.filter((e:any)=>e.outbox_id==='wake-intent-old')).toEqual(before.effects);clock+=11000;vi.setSystemTime(clock);const second=recovery.supervise('xv','j')!;expect(second.find(r=>r.key==='task0')?.state).toBe('pending-pickup');expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='task0'").get()).toEqual({n:1});expect(second.find(r=>r.key==='coordinator')?.queueId).toBe(notice.qitemId);repo.attachTransport({send:async()=>({ok:true,verified:true})});await recovery.deliverCommitted();repo.claim({qitemId:notice.qitemId,destinationSession:actor});expect(db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get(notice.qitemId)).toEqual({claimed_by_generation_uuid:gen});
});

async function currentAdoptionPacket(){
 const p=await packet();await authorize(p);migrate(p);svc().acknowledge('lead@xv',token,{obligationsDigest:svc().reconciliationDigest('xv'),operationId:'ack-current'});
 db.prepare("UPDATE coordinator_authority SET epoch=3,lease_until=? WHERE rig_id='xv'").run(Date.now()+600000);
 // Native current holder claim, independently enforced by the new operation.
 db.prepare("UPDATE queue_transitions SET identity_provenance='transport:v1' WHERE qitem_id='baton' AND transition_note='claimed'").run();
 outbox.record({outboxId:'current-diagnostic',senderSession:'reviewer@xv',destinationSession:actor,body:'Diagnostic evidence only; UNKNOWN retained',auditPointer:'work',identityProvenance:'transport:v1'});
 const hp:HistoricalPlan={rigId:'xv',leadBatonId:'baton',leadGeneration:'lead-g1',operatorGeneration:gen,operationId:'current-hold',authorizationId:'current-hold-auth',expiresAt:Date.now()+600000,effects:history.inspect('xv',['current-diagnostic'])};
 await repo.create({qitemId:hp.authorizationId,sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'outbox-historical-quarantine-authorization',requestDigest:historicalDigest({actor,generation:gen,input:hp})}),nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:hp.authorizationId,destinationSession:actor,identityProvenance:'transport:v1'});history.quarantine(actor,gen,hp);
 const effects=svc().legacyInventory('xv','none',true).heldHistory!.filter(h=>h.outboxId==='current-diagnostic'),deadline=Date.now()+600000;
 await repo.create({qitemId:'current-recovery',sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'coordinator-held-history-recovery.v1',rigId:'xv',operationId:'adopt-current',owner:actor,generation:gen,lead:'lead@xv',leadGeneration:'lead-g1',effects:effects.map(h=>h.outboxId),action:'reconcile-preserved-unknown-history',deadline,returnPath:{session:'lead@xv',queueId:'current-recovery'}}),expiresAt:new Date(deadline).toISOString(),nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:'current-recovery',destinationSession:actor,identityProvenance:'transport:v1'});
 return {rigId:'xv',operationId:'adopt-current',expected:{...token,epoch:3},effects,recovery:{queueId:'current-recovery',rowHash:historicalDigest(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get('current-recovery'))}};
}
it('current enabled epoch adoption unblocks only exact contained worker debt without original mutations',async()=>{
 const input=await currentAdoptionPacket(),a=svc().get('xv'),resources=db.prepare('SELECT * FROM coordinator_resources').all(),row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='current-diagnostic'").get() as any;
 expect(svc().isAdoptedHistoryContained('xv',row)).toBe(false);expect(()=>svc().migrateLegacy(actor,gen,{rigId:'xv',owner:'lead@xv',ownerGeneration:'lead-g1',operationId:'not-migration'} as LegacyEnrollment)).toThrow('enabled epoch');
 const clock=Date.now(),sample=(session:string):CoordinationActivity=>({generation:svc().generation(session)!,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:null},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}});
 const recovery=new CoordinationRecoveryService(repo,s=>sample(s),()=>clock);svc().coordinationRecovery=recovery;
 const tasks=['reviewer@xv','architect@xv'].map((owner,i)=>({key:'current-task'+i,packageKey:'current-task'+i,owner,action:'Independent ready work',body:'new current work'+i,deadline:clock+60000,predecessors:[],...(i?{recoveryFor:'current-task0'}:{}),admission:{generation:svc().generation(owner)!,configurationDigest:recovery.configurationDigest(owner)!,qualificationRef:'actual-current',capacityRef:'current',effortRef:'current',validUntil:clock+60000}}));
 for(const t of tasks)svc().admit(actor,gen,'xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});
 recovery.configure(actor,gen,{rigId:'xv',revision:'current-epoch',operatorGeneration:gen,stallMs:10000,allowIdlePeerTransfer:false,tasks});
 expect(recovery.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='current-task0')).toMatchObject({state:'held',reason:'uncertain-worker-effect'});
 const before=original();const receipt=svc().adoptHeldHistory(actor,gen,input);expect(receipt).toMatchObject({deliveryConclusion:'unknown',originalMutations:0,expected:{epoch:3}});expect(svc().adoptHeldHistory(actor,gen,input)).toEqual(receipt);
 expect(original()).toEqual(before);expect(svc().get('xv')).toEqual(a);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual(resources);expect(svc().isAdoptedHistoryContained('xv',row)).toBe(true);expect(svc().isAdoptedHistoryContained('other',row)).toBe(false);expect(outbox.claimForDelivery('current-diagnostic')).toBe(false);expect(outbox.listPending('').some(r=>r.outboxId==='current-diagnostic')).toBe(false);
 db.exec('SAVEPOINT unrelated');outbox.record({outboxId:'unrelated-new-debt',senderSession:'reviewer@xv',destinationSession:actor,body:'Unknown independent operation'});expect(recovery.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='current-task0')).toMatchObject({state:'held',reason:'uncertain-worker-effect'});db.exec('ROLLBACK TO unrelated');db.exec('RELEASE unrelated');
 expect(recovery.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='current-task0')).toMatchObject({state:'pending-pickup'});expect(db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='current-task0'").get()).toEqual({n:1});
 expect(()=>svc().adoptHeldHistory(actor,gen,{...input,recovery:{...input.recovery,rowHash:'changed'}})).toThrow('Operation ID reused');expect(()=>db.prepare("UPDATE outbox_entries SET body='replay' WHERE outbox_id='current-diagnostic'").run()).toThrow('protected');expect(outbox.getById('current-diagnostic')?.deliveryState).toBe('pending');
});
it('current enabled epoch adoption refuses custody identity authority and unknown operation drift atomically',async()=>{
 const input=await currentAdoptionPacket();
 for(const kind of ['actor','generation','epoch','holder-generation','lease','baton','baton-native','recovery-native','recovery-claim','recovery-expiry','row','quarantine','operation','custody','foreign','new-operation']){db.exec('SAVEPOINT refusal');let request=input,caller=actor,callerGeneration=gen;
  if(kind==='actor')caller='lead@xv';if(kind==='generation')callerGeneration='retired';if(kind==='epoch')request={...input,expected:{...input.expected,epoch:2}};
  if(kind==='holder-generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='replacement' WHERE node_id='lead@xv'").run();if(kind==='lease')db.prepare("UPDATE coordinator_authority SET lease_until=1").run();if(kind==='baton')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='retired' WHERE qitem_id='baton'").run();if(kind==='baton-native'||kind==='recovery-native')db.prepare("UPDATE queue_transitions SET identity_provenance=NULL WHERE qitem_id=? AND transition_note='claimed'").run(kind==='baton-native'?'baton':'current-recovery');
  if(kind==='recovery-claim')db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id='current-recovery'").run();if(kind==='recovery-expiry')db.prepare("UPDATE queue_items SET expires_at='2000-01-01T00:00:00Z' WHERE qitem_id='current-recovery'").run();if(kind==='row')db.prepare("UPDATE outbox_entries SET body='changed' WHERE outbox_id='current-diagnostic'").run();if(kind==='quarantine')db.prepare("UPDATE outbox_historical_quarantines SET state='disposed' WHERE outbox_id='current-diagnostic'").run();if(kind==='operation')db.prepare("UPDATE outbox_historical_operations SET receipt='{}' WHERE operation_id='current-hold'").run();if(kind==='custody')db.prepare("UPDATE queue_items SET summary='changed custody' WHERE qitem_id='work'").run();if(kind==='foreign')db.prepare("UPDATE outbox_historical_quarantines SET rig_id='other' WHERE outbox_id='current-diagnostic'").run();if(kind==='new-operation')request={...input,operationId:'different-purpose'};
const preserved=original(),authority=svc().get('xv');expect(()=>svc().adoptHeldHistory(caller,callerGeneration,request)).toThrow();expect(original()).toEqual(preserved);expect(svc().get('xv')).toEqual(authority);expect(db.prepare("SELECT count(*) n FROM coordinator_held_history WHERE outbox_id='current-diagnostic'").get()).toEqual({n:0});expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='held-history-adoption'").get()).toEqual({n:0});db.exec('ROLLBACK TO refusal');db.exec('RELEASE refusal');
  }
});

// Queue columns migration 090 and 091 appended after an adoption receipt was already frozen.
const lateNullableColumns=['reply_to','human_questions','human_answers'];
/** Test-side canonical form. The first case proves it is the service's own, so a digest built with
 *  it refutes exactly what the service would compute. */
const custodyDigest=(value:unknown):string=>{const c=(v:unknown):string=>Array.isArray(v)?`[${v.map(c).join(',')}]`:v!==null&&typeof v==='object'?`{${Object.keys(v as Record<string,unknown>).sort().map(k=>`${JSON.stringify(k)}:${c((v as Record<string,unknown>)[k])}`).join(',')}}`:JSON.stringify(v);return digest(c(value));};
/** Refreezes only the adopted custody reference. Every immutable outbox, quarantine and operation
 *  hash and every other ledger column is left exactly as adoption wrote it. */
function refreezeAdoptionCustody(rigId:string,outboxId:string,transform:(custody:Record<string,any>)=>Record<string,any>):Record<string,any>{
  const live=(svc() as any).historyCustody(db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get(outboxId)) as Record<string,any>;
  const frozen=transform(JSON.parse(JSON.stringify(live)));
  const held=db.prepare('SELECT receipt FROM coordinator_held_history WHERE rig_id=? AND outbox_id=?').get(rigId,outboxId) as {receipt:string};
  const receipt=JSON.parse(held.receipt);receipt.postCustody=frozen;
  db.exec('DROP TRIGGER IF EXISTS coordinator_held_no_update');
  db.prepare('UPDATE coordinator_held_history SET post_custody_hash=?,receipt=? WHERE rig_id=? AND outbox_id=?').run(custodyDigest(frozen),JSON.stringify(receipt),rigId,outboxId);
  return frozen;
}
/** The custody as it stood before 090/091: those columns were absent, not null. */
const freezeBeforeNullableColumns=(rigId:string,outboxId:string)=>refreezeAdoptionCustody(rigId,outboxId,c=>{for(const column of lateNullableColumns){expect(Object.prototype.hasOwnProperty.call(c.queue,column)).toBe(true);expect(c.queue[column]).toBeNull();delete c.queue[column];}return c;});

it('custody adopted before migrations 090/091 stays contained for exactly null additive columns and for nothing else',async()=>{
  expect(custodyDigest({b:[1,null],a:'x'})).toBe(legacyProposalDigest({b:[1,null],a:'x'} as any));
  const p=await packet();await authorize(p);migrate(p);
  const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-old'").get() as any;
  const ledger=()=>db.prepare("SELECT * FROM coordinator_held_history WHERE rig_id='xv' AND outbox_id='wake-intent-old'").get() as any;
  const effect=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-old'").get(),quarantines=db.prepare("SELECT * FROM outbox_historical_quarantines ORDER BY outbox_id").all(),operations=db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all(),adopted=ledger();
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(true);

  db.exec('SAVEPOINT before090');
  const frozen=freezeBeforeNullableColumns('xv','wake-intent-old'),old=ledger();
  // Schema evolution only. The effect, its hold and every operation receipt are byte identical,
  // and the frozen reference is intact; only the custody hash moved with the schema.
  expect(frozen.queue).not.toHaveProperty('reply_to');expect(frozen.queue).not.toHaveProperty('human_questions');expect(frozen.queue).not.toHaveProperty('human_answers');
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-old'").get()).toEqual(effect);
  expect(db.prepare("SELECT * FROM outbox_historical_quarantines ORDER BY outbox_id").all()).toEqual(quarantines);
  expect(db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all()).toEqual(operations);
  expect([old.original_row_hash,old.quarantine_hash,old.quarantine_operation_hash,old.pre_custody_hash]).toEqual([adopted.original_row_hash,adopted.quarantine_hash,adopted.quarantine_operation_hash,adopted.pre_custody_hash]);
  expect(old.post_custody_hash).not.toBe(adopted.post_custody_hash);
  expect(custodyDigest(JSON.parse(old.receipt).postCustody)).toBe(old.post_custody_hash);
  // Current custody genuinely fails exact byte equality against that reference; only the three
  // additive null columns stand between them.
  expect(custodyDigest((svc() as any).historyCustody(row))).not.toBe(old.post_custody_hash);
  // Contained, so ordinary dispatch is permitted; delivery stays UNKNOWN and unexecutable.
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(true);
  expect(svc().heldHistoryAuthoringSnapshot('xv').map(h=>h.outboxId)).toEqual(['wake-intent-old']);
  expect(outbox.getById('wake-intent-old')?.deliveryState).toBe('pending');
  expect(outbox.isHistoricalQuarantined('wake-intent-old')).toBe(true);
  expect(outbox.claimForDelivery('wake-intent-old')).toBe(false);

  const refusals:Array<[string,()=>void]>=[
   // The same three columns carrying content are custody, not schema.
   ['nonnull-reply-to',()=>db.prepare("UPDATE queue_items SET reply_to='q-7' WHERE qitem_id='work'").run()],
   ['nonnull-human-questions',()=>db.prepare("UPDATE queue_items SET human_questions='[1]' WHERE qitem_id='work'").run()],
   ['nonnull-human-answers',()=>db.prepare("UPDATE queue_items SET human_answers='answered' WHERE qitem_id='work'").run()],
   // Body, state and claim are compared exactly.
   ['body',()=>db.prepare("UPDATE queue_items SET body='changed' WHERE qitem_id='work'").run()],
   ['state',()=>db.prepare("UPDATE queue_items SET state='done' WHERE qitem_id='work'").run()],
   ['claim',()=>db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='retired' WHERE qitem_id='work'").run()],
   ['summary',()=>db.prepare("UPDATE queue_items SET summary='changed' WHERE qitem_id='work'").run()],
   // An added column outside those three is not invisible.
   ['unknown-added-column',()=>db.exec('ALTER TABLE queue_items ADD COLUMN ledger_probe TEXT')],
   // Assignment and resource custody are compared exactly.
   ['assignment',()=>db.prepare("DELETE FROM coordinator_assignments WHERE queue_id='work'").run()],
   ['resource',()=>db.prepare("DELETE FROM coordinator_resources WHERE rig_id='xv' AND resource_key='source/a'").run()],
  ];
  for(const [kind,mutate] of refusals){db.exec('SAVEPOINT refusal');const preserved=ledger();mutate();
   expect(svc().isAdoptedHistoryContained('xv',row),kind).toBe(false);
   expect([preserved.post_custody_hash,preserved.receipt],kind).toEqual([ledger().post_custody_hash,ledger().receipt]);
   db.exec('ROLLBACK TO refusal');db.exec('RELEASE refusal');}
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(true);

  // A receipt that already carried the columns is unaffected while they agree, and refused once they drift.
  db.exec('SAVEPOINT alreadyPresent');refreezeAdoptionCustody('xv','wake-intent-old',c=>(c.queue.human_answers=null,c));
  expect(Object.prototype.hasOwnProperty.call(JSON.parse(ledger().receipt).postCustody.queue,'human_answers')).toBe(true);
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(true);
  db.prepare("UPDATE queue_items SET human_answers='drifted' WHERE qitem_id='work'").run();
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(false);
  db.exec('ROLLBACK TO alreadyPresent');db.exec('RELEASE alreadyPresent');

  // A frozen value that is currently null is drift, not schema evolution.
  db.exec('SAVEPOINT frozenValue');refreezeAdoptionCustody('xv','wake-intent-old',c=>(c.queue.reply_to='q-frozen',c));
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(false);
  db.exec('ROLLBACK TO frozenValue');db.exec('RELEASE frozenValue');

  // Tampering with the frozen reference itself is still refused.
  db.exec('SAVEPOINT tamperedReference');db.exec('DROP TRIGGER IF EXISTS coordinator_held_no_update');
  db.prepare("UPDATE coordinator_held_history SET post_custody_hash=? WHERE rig_id='xv' AND outbox_id='wake-intent-old'").run(custodyDigest({queue:null,assignment:[],resources:[]}));
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(false);
  db.exec('ROLLBACK TO tamperedReference');db.exec('RELEASE tamperedReference');

  // Nothing above was persisted: the immutable effect, hold and operation receipts are untouched.
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-old'").get()).toEqual(effect);
  expect(db.prepare("SELECT * FROM outbox_historical_quarantines ORDER BY outbox_id").all()).toEqual(quarantines);
  expect(db.prepare("SELECT * FROM coordinator_operations ORDER BY rig_id,operation_id").all()).toEqual(operations);
  db.exec('ROLLBACK TO before090');db.exec('RELEASE before090');
  expect(svc().isAdoptedHistoryContained('xv',row)).toBe(true);
});
