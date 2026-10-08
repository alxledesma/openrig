import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { contextRefreshSchema } from "../src/db/migrations/108_context_refresh.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { ContextRefreshService, contextRefreshDigest, type ContextRefreshOptions, type ContextRefreshCheckpointReceipt } from "../src/domain/context-refresh-service.js";
import { DEFAULT_CONTEXT_REFRESH_POLICY, type ContextRefreshActor, type ContextRefreshGrant, type ContextRefreshTarget,
 type ContextRefreshObservation, type ContextRefreshCheckpoint, type ContextRefreshReservationEvidence } from "../src/domain/context-refresh-contract.js";

const operator={session:'operator-agent@kernel',generation:'operator-agent-g1'},nodeId='peer@xv';
const hash=(x:string)=>contextRefreshDigest(x);
describe('context refresh finite grant and durable state machine',()=>{
 let db:Database.Database,now:number,service:ContextRefreshService,options:ContextRefreshOptions,grant:ContextRefreshGrant;
 let target:ContextRefreshTarget,observation:ContextRefreshObservation,checkpoint:ContextRefreshCheckpoint|null;
 let receipt:ContextRefreshReservationEvidence|null,queueReceipt:ContextRefreshCheckpointReceipt|null,native:boolean,cutoverReady:boolean;
 const ids=(suffix='one')=>({attemptId:'attempt-'+suffix,operationId:'rotate-'+suffix,reservationId:'reservation-'+suffix});
 function current(id:string):ContextRefreshTarget|null {
  const row=db.prepare("SELECT n.id,n.runtime,n.model,s.session_name,s.resume_token,t.generation_uuid FROM nodes n JOIN sessions s ON s.node_id=n.id JOIN occupant_tenures t ON t.node_id=n.id WHERE n.id=? ORDER BY t.generation_ordinal DESC,s.id DESC LIMIT 1").get(id) as any;
  return row?{nodeId:row.id,sessionName:row.session_name,generation:row.generation_uuid,runtime:row.runtime,nativeId:row.resume_token,configurationDigest:hash([row.runtime,row.model].join('/'))}:null;
 }
 function assertActor(actor:ContextRefreshActor){const t=current(actor.session);if(!t||t.generation!==actor.generation)throw Error('actual actor generation changed');}
 const evaluate=()=>service.evaluate(operator,grant.grantId,nodeId);
 function ready(){observation.usage!.usedPercent=85;checkpoint={target,checkpointId:'checkpoint',checkpointHash:hash('checkpoint'),queueDigest:hash('queue'),authoredBy:{session:nodeId,generation:target.generation},outstandingEffects:0};}
 function evidence(state:ContextRefreshReservationEvidence['state']='reserved'):ContextRefreshReservationEvidence {
  return {reservationId:'reservation-one',operationId:'rotate-one',targetNodeId:nodeId,predecessorGeneration:target.generation,predecessorNativeId:target.nativeId,checkpointHash:checkpoint!.checkpointHash,state,successor:null,successorVerified:false,custodyVerified:true,successorAck:null,independentAcceptance:null,releasedBy:null,releaseMode:null,receiptDigest:hash('actual-reservation-'+state)};
 }
 async function prepared(){ready();return service.prepareAttempt(operator,grant.grantId,nodeId,ids());}
 async function reserved(){await prepared();expect(service.beginEffect(operator,'attempt-one','reserve').maySendEffect).toBe(true);receipt=evidence();expect(service.reconcile(operator,'attempt-one').phase).toBe('reserved');}
 async function committed(){
  await reserved();expect(service.beginEffect(operator,'attempt-one','handover').maySendEffect).toBe(true);
  db.prepare("UPDATE occupant_tenures SET generation_uuid='peer-g2' WHERE node_id=?").run(nodeId);
  db.prepare("UPDATE sessions SET resume_token='native-successor' WHERE node_id=?").run(nodeId);
  receipt={...evidence('committed'),successor:{generation:'peer-g2',nativeId:'native-successor',configurationDigest:target.configurationDigest},successorVerified:true};
  expect(service.reconcile(operator,'attempt-one').phase).toBe('committed-awaiting-acceptance');
 }
 beforeEach(()=>{
  db=createDb();seed(db);migrate(db,[contextRefreshSchema]);now=Date.UTC(2026,9,7,23);
  db.prepare("UPDATE nodes SET runtime='codex',model='gpt-6-luna'").run();
  db.prepare("UPDATE sessions SET resume_token='native-'||session_name,status='running',startup_status='ready'").run();
  target=current(nodeId)!;native=true;cutoverReady=true;checkpoint=null;receipt=null;queueReceipt=null;
  observation={identity:target,observedAt:now,capability:'codex-reserved-fresh',native:{verified:true,observedAt:now,launchId:'target-launch',fingerprint:'actual-kernel-lineage'},activity:{value:'idle',observedAt:now},usage:{usedPercent:74,observedAt:now,source:'codex_token_count_jsonl',cursor:'token-1'},compactions:{count:7,observedAt:now,source:'native_compacted_records',cursor:'compact-7'},holds:[]};
  grant={grantId:'grant-one',kind:'context-refresh',executor:{...operator,nodeId:operator.session,launchId:'actual-operator-launch',configurationDigest:current(operator.session)!.configurationDigest},targets:[target],policy:{...DEFAULT_CONTEXT_REFRESH_POLICY},policyRevision:'root-approved-v1',validUntil:now+3600000,validator:{session:'reviewer@xv',generation:'reviewer-g1'},recoveryOwner:operator};
  options={db,now:()=>now,assertCurrentOperator:assertActor,assertCurrentActor:assertActor,
   assertExecutor:g=>{assertActor(g.executor);if(!native||g.executor.launchId!=='actual-operator-launch'||g.executor.configurationDigest!==current(operator.session)!.configurationDigest)throw Error('actual native executor proof missing');},
   currentTarget:current,observe:async()=>structuredClone(observation),checkpoint:()=>checkpoint,checkpointReceipt:()=>queueReceipt,reservationEvidence:()=>receipt,
   assertCutoverReady:()=>{if(!cutoverReady)throw Error('exact lifecycle precondition failed');}};
  service=new ContextRefreshService(options);service.grant(operator,grant);
 });
 afterEach(()=>db.close());

 it.each([[74,'watching','observe'],[75,'preparation-needed','request-checkpoint'],[84,'preparation-needed','request-checkpoint'],[85,'preparation-needed','request-checkpoint']])('observes %s percent without a checkpoint as %s',async(percent,phase,action)=>{
  observation.usage!.usedPercent=Number(percent);expect(await evaluate()).toMatchObject({phase,action,rotateThreshold:Number(percent)>=85});
 });
 it('keeps the 75..84 prepare band from rotating a verified checkpoint',async()=>{
  ready();observation.usage!.usedPercent=84;expect(await evaluate()).toMatchObject({phase:'checkpoint-ready',action:'observe',rotateThreshold:false});
  observation.usage!.usedPercent=85;expect(await evaluate()).toMatchObject({action:'reserve',rotateThreshold:true});
 });
 it('retains native-thread baseline across service/grant restart and fires at exactly two compactions',async()=>{
  expect(await evaluate()).toMatchObject({baselineCompactions:7,compactionsSinceBaseline:0});
  observation.compactions!.count=8;expect(await evaluate()).toMatchObject({prepareThreshold:false,compactionsSinceBaseline:1});
  service=new ContextRefreshService(options);service.grant(operator,{...grant,grantId:'grant-two'});
  observation.compactions!.count=9;observation.usage=null;
  expect(await service.evaluate(operator,'grant-two',nodeId)).toMatchObject({action:'request-checkpoint',rotateThreshold:true,baselineCompactions:7,compactionsSinceBaseline:2});
  observation.compactions!.count=8;expect((await evaluate()).holds).toContain('compaction-evidence-invalid');
  expect(()=>db.prepare('DELETE FROM context_refresh_baselines').run()).toThrow('retained');
 });
 it.each(['stale-usage','future-usage','missing-usage','old-generation','model-drift','native-stale','busy','activity-stale','runtime-failure','authority','unknown-effects'])( 'holds %s without creating effect debt',async(kind)=>{
  observation.usage!.usedPercent=95;
  if(kind==='stale-usage')observation.usage!.observedAt=now-120001;
  if(kind==='future-usage')observation.usage!.observedAt=now+1;
  if(kind==='missing-usage')observation.usage=null;
  if(kind==='old-generation')observation.identity={...target,generation:'retired'};
  if(kind==='model-drift')db.prepare("UPDATE nodes SET model='new-model' WHERE id=?").run(nodeId);
  if(kind==='native-stale')observation.native.observedAt=now-5001;
  if(kind==='busy')observation.activity.value='busy';
  if(kind==='activity-stale')observation.activity.observedAt=now-5001;
  if(kind==='runtime-failure')observation.holds=['runtime-not-ready'];
  if(kind==='authority')observation.holds=['authority-transfer-required'];
  if(kind==='unknown-effects')observation.holds=['effects-unresolved'];
  expect(await evaluate()).toMatchObject({phase:'prerequisite-held',action:'none'});
  expect(db.prepare('SELECT count(*) n FROM context_refresh_attempts').get()).toEqual({n:0});
 });
 it('holds Pi capability honestly despite fresh idle usage and an explicit grant',async()=>{
  db.prepare("UPDATE nodes SET runtime='pi' WHERE id=?").run(nodeId);target=current(nodeId)!;
  const piGrant={...grant,grantId:'pi-grant',targets:[target]};service.grant(operator,piGrant);
  observation.identity=target;observation.usage!.usedPercent=99;observation.capability='unsupported';
  expect(await service.evaluate(operator,piGrant.grantId,nodeId)).toMatchObject({phase:'prerequisite-held',action:'none',holds:['runtime-unsupported']});
 });
 it('Pi baseline binds exact native FILE header and source across restart',async()=>{
  db.prepare("UPDATE nodes SET runtime='pi' WHERE id=?").run(nodeId);
  db.prepare("UPDATE sessions SET resume_token='/private/pi/session.jsonl' WHERE node_id=?").run(nodeId);
  target=current(nodeId)!;const g={...grant,grantId:'pi-native',targets:[target]};service.grant(operator,g);
  observation.identity=target;observation.capability='pi-reserved-fresh';observation.usage=null;observation.holds=['usage-unavailable'];
  const cursor=(header='actual-header',file=target.nativeId)=>JSON.stringify({version:1,sessionFile:file,sessionHeaderId:header});
  observation.compactions={count:7,observedAt:now,source:'pi_compaction_jsonl',cursor:cursor()};
  expect(await service.evaluate(operator,g.grantId,nodeId)).toMatchObject({baselineCompactions:7});
  service=new ContextRefreshService(options);observation.compactions.count=9;
  for(const [source,raw] of [['codex_compacted_jsonl',cursor()],['pi_compaction_jsonl',cursor('replaced')],['pi_compaction_jsonl',cursor('actual-header','/other')],['pi_compaction_jsonl','malformed']]) {
   observation.compactions.source=source;observation.compactions.cursor=raw;
   expect((await service.evaluate(operator,g.grantId,nodeId)).holds).toContain('compaction-evidence-invalid');
   expect(db.prepare('SELECT highest_count FROM context_refresh_baselines WHERE node_id=?').get(nodeId)).toEqual({highest_count:7});
  }
  observation.compactions.source='pi_compaction_jsonl';observation.compactions.cursor=cursor();
  expect(await service.evaluate(operator,g.grantId,nodeId)).toMatchObject({action:'request-checkpoint',compactionsSinceBaseline:2});
 });
 it('requires actual separate Operator, finite scope and immutable exact grant',async()=>{
  expect(()=>service.grant({session:'peer@xv',generation:'peer-g1'},grant)).toThrow('Kernel Operator');
  expect(()=>service.grant(operator,{...grant,grantId:'expired',validUntil:now})).toThrow('finite');
  expect(()=>service.grant(operator,{...grant,policyRevision:'changed'})).toThrow('Immutable');
  expect(()=>service.grant(operator,{...grant,grantId:'self',targets:[current(operator.session)!]})).toThrow('self-refresh');
  expect(()=>db.prepare("UPDATE context_refresh_grants SET grant_json='{}'").run()).toThrow('immutable');
  now=grant.validUntil;expect(await evaluate()).toMatchObject({phase:'scope-ended',action:'none'});
 });
 it('holds new effects when genuine executor proof or current generation is lost',async()=>{
  native=false;await expect(evaluate()).rejects.toThrow('native executor');native=true;
  db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-new' WHERE node_id=?").run(operator.session);
  await expect(evaluate()).rejects.toThrow('actor generation');
 });
 it('queues at most once and reconciles a lost checkpoint response through its exact real receipt',async()=>{
  observation.usage!.usedPercent=75;
  const request=await service.prepareCheckpointRequest(operator,grant.grantId,nodeId,'checkpoint-op');
  expect((await service.beginCheckpointRequest(operator,'checkpoint-op')).maySendEffect).toBe(true);
  expect((await service.beginCheckpointRequest(operator,'checkpoint-op')).maySendEffect).toBe(false);
  expect(service.reconcileCheckpoint(operator,'checkpoint-op')).toBe('uncertainty-held');
  expect(()=>service.grant(operator,{...grant,grantId:'bypass'})).toThrow('Node-wide');
  queueReceipt={operationId:request.operationId,qitemId:request.qitemId,requestDigest:hash('wrong'),receiptDigest:hash('real-queue-receipt')};
  expect(service.reconcileCheckpoint(operator,'checkpoint-op')).toBe('uncertainty-held');
  queueReceipt.requestDigest=contextRefreshDigest(request);
  expect(service.reconcileCheckpoint(operator,'checkpoint-op')).toBe('receipt-confirmed');
  expect(await evaluate()).toMatchObject({phase:'checkpoint-requested',action:'observe'});
 });
 it('safely cancels unsent checkpoint preparation while retaining it and allows a new exact request',async()=>{
  observation.usage!.usedPercent=75;await service.prepareCheckpointRequest(operator,grant.grantId,nodeId,'first-request');
  observation.activity.value='busy';await expect(service.beginCheckpointRequest(operator,'first-request')).rejects.toThrow('eligibility changed');
  service.cancelCheckpointPreparation(operator,'first-request');observation.activity.value='idle';
  await service.prepareCheckpointRequest(operator,grant.grantId,nodeId,'second-request');
  expect((await service.beginCheckpointRequest(operator,'second-request')).maySendEffect).toBe(true);
  expect(()=>service.cancelCheckpointPreparation(operator,'second-request')).toThrow('never-sent');
  expect(db.prepare('SELECT count(*) n FROM context_refresh_checkpoint_requests').get()).toEqual({n:2});
 });
 it('safe final pre-effect refusal leaves cancellable preparation; future attempt preserves earlier evidence',async()=>{
  await prepared();expect(await evaluate()).toMatchObject({phase:'checkpoint-ready',action:'none'});cutoverReady=false;expect(()=>service.beginEffect(operator,'attempt-one','reserve')).toThrow('precondition');
  expect(service.cancelPreparation(operator,'attempt-one').phase).toBe('cancelled-before-effect');cutoverReady=true;
  await service.prepareAttempt(operator,grant.grantId,nodeId,ids('two'));
  expect(service.beginEffect(operator,'attempt-two','reserve').maySendEffect).toBe(true);
  expect(()=>service.cancelPreparation(operator,'attempt-two')).toThrow('may have begun');
  expect(db.prepare('SELECT count(*) n FROM context_refresh_attempts').get()).toEqual({n:2});
 });
 it('retains UNKNOWN across new grant and new generation while unrelated targets remain independent',async()=>{
  await prepared();expect(service.beginEffect(operator,'attempt-one','reserve').maySendEffect).toBe(true);
  expect(service.reconcile(operator,'attempt-one').phase).toBe('uncertainty-held');
  expect(service.beginEffect(operator,'attempt-one','reserve').maySendEffect).toBe(false);
  db.prepare("UPDATE occupant_tenures SET generation_uuid='peer-g2' WHERE node_id=?").run(nodeId);
  expect(()=>service.grant(operator,{...grant,grantId:'new-gen',targets:[current(nodeId)!]})).toThrow('Node-wide');
  expect(()=>service.grant(operator,{...grant,grantId:'unrelated',targets:[current('builder@xv')!]})).not.toThrow();
  expect(()=>db.prepare("UPDATE context_refresh_attempts SET phase='prepared' WHERE attempt_id='attempt-one'").run()).toThrow('cannot replay');
 });
 it('reconciles exact reserve receipt without duplicate send and never replays an uncertain handover',async()=>{
  await reserved();expect(service.reconcile(operator,'attempt-one').phase).toBe('reserved');
  expect(service.beginEffect(operator,'attempt-one','reserve').maySendEffect).toBe(false);
  expect(service.beginEffect(operator,'attempt-one','handover').maySendEffect).toBe(true);
  expect(service.reconcile(operator,'attempt-one').phase).toBe('uncertainty-held'); // receipt still only reserved
  expect(service.beginEffect(operator,'attempt-one','handover').maySendEffect).toBe(false);
 });
 it('requires genuine current successor and independent acceptance, then exact release receipt before refreshed',async()=>{
  await committed();expect(()=>service.beginEffect(operator,'attempt-one','release')).toThrow('acceptance');
  receipt!.successorAck={session:nodeId,generation:'peer-g2'};receipt!.independentAcceptance=operator;
  expect(()=>service.beginEffect(operator,'attempt-one','release')).toThrow('acceptance');
  receipt!.independentAcceptance=grant.validator;
  expect(service.beginEffect(operator,'attempt-one','release').maySendEffect).toBe(true);
  expect(service.reconcile(operator,'attempt-one').phase).toBe('uncertainty-held');
  expect(service.beginEffect(operator,'attempt-one','release').maySendEffect).toBe(false);
  receipt!.state='released';receipt!.releaseMode='accepted_successor';receipt!.releasedBy=operator;
  service.revoke(operator,grant.grantId);now=grant.validUntil+1;native=false;
  expect(service.reconcile(operator,'attempt-one').phase).toBe('refreshed');
  expect(service.reconcile(operator,'attempt-one').phase).toBe('refreshed');
  expect(()=>db.prepare("DELETE FROM context_refresh_events").run()).toThrow('retained');
 });
 it.each(['wrong-checkpoint','wrong-successor','custody-unverified','retired-validator'])( 'cannot count %s receipt as completed refresh',async(kind)=>{
  await committed();receipt!.successorAck={session:nodeId,generation:'peer-g2'};receipt!.independentAcceptance=grant.validator;
  receipt!.state='released';receipt!.releaseMode='accepted_successor';receipt!.releasedBy=operator;
  if(kind==='wrong-checkpoint')receipt!.checkpointHash=hash('different');
  if(kind==='wrong-successor')receipt!.successor!.nativeId=target.nativeId;
  if(kind==='custody-unverified')receipt!.custodyVerified=false;
  if(kind==='retired-validator')db.prepare("UPDATE occupant_tenures SET generation_uuid='reviewer-new' WHERE node_id='reviewer@xv'").run();
  expect(service.reconcile(operator,'attempt-one').phase).toBe('uncertainty-held');
 });
 it('settles a lost handover request only through the actual supported pre-replacement cancellation receipt',async()=>{
  await reserved();expect(service.beginEffect(operator,'attempt-one','handover').maySendEffect).toBe(true);
  expect(service.reconcile(operator,'attempt-one').phase).toBe('uncertainty-held');
  expect(()=>service.cancelPreparation(operator,'attempt-one')).toThrow('may have begun');
  receipt!.state='released';receipt!.releaseMode='cancel_before_replacement';receipt!.releasedBy=operator;
  expect(service.reconcile(operator,'attempt-one').phase).toBe('cancelled-before-effect');
  await service.prepareAttempt(operator,grant.grantId,nodeId,ids('after-disposition'));
  expect(service.beginEffect(operator,'attempt-after-disposition','reserve').maySendEffect).toBe(true);
  expect(service.beginEffect(operator,'attempt-one','handover').maySendEffect).toBe(false);
 });
 it('rejects unawaited async proof callbacks before any effect permit',async()=>{
  await prepared();options.assertCutoverReady=async()=>{};
  expect(()=>service.beginEffect(operator,'attempt-one','reserve')).toThrow('synchronously');
  expect(service.attempt('attempt-one').phase).toBe('prepared');
  options.assertExecutor=async()=>{};
  expect(()=>service.grant(operator,{...grant,grantId:'async-proof'})).toThrow('synchronously');
 });
 it('retains pre-existing native-duty node debt across context refresh grants',()=>{
  db.prepare("INSERT INTO native_duty_grants VALUES('old-scope','digest','{}','old','old-gen',1,NULL)").run();
  db.prepare("INSERT INTO native_duty_registrations VALUES('old-reg','old-scope','old-launch',77,?,'old-session','retired','codex','config','fingerprint','held',1,2,'unknown',1)").run(nodeId);
  db.prepare("INSERT INTO native_duty_intents VALUES('old-reg','old-op','{}','hash','hash',1,'uncertainty-held')").run();
  expect(()=>service.grant(operator,{...grant,grantId:'must-hold'})).toThrow('Node-wide');
 });
});
