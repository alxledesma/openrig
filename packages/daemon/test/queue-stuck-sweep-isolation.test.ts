import { CoordinationRecoveryService } from '../src/domain/coordination-recovery-service.js';
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import Database from 'better-sqlite3';
import {seed,token} from './helpers/coordinator-fixture.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {EventBus} from '../src/domain/event-bus.js';
import {digest,legacyProposalDigest,type LegacyEnrollment} from '../src/domain/coordinator-authority-service.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {runStuckSweep,createStuckSweepStatus,STUCK_SWEEP_FINDING_TAG,findingDedupTag} from '../src/domain/queue-stuck-sweep.js';

describe('instance sweep isolates refused enabled scope without borrowing dispatch authority',()=>{
 let db:Database.Database,repo:QueueRepository;
 beforeEach(()=>{db=new Database(':memory:');seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date().toISOString(),new Date().toISOString());repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));});
 afterEach(()=>db.close());
 async function setup(healthyFirst=false){
  async function old(id:string,source:string,dest:string){await repo.create({qitemId:id,sourceSession:source,destinationSession:dest,body:id,nudge:false});const past=new Date(Date.now()-120*60000).toISOString();db.prepare('UPDATE queue_items SET ts_created=? WHERE qitem_id=?').run(past,id);db.prepare('UPDATE queue_transitions SET ts=? WHERE qitem_id=?').run(past,id);}
  if(healthyFirst)await old('healthy','worker@other','worker@other');
  await old('legacy-unadmitted','lead@xv','builder@xv');
  if(!healthyFirst)await old('healthy','worker@other','worker@other');
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  const packet:LegacyEnrollment={rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'migrate',authorizationId:'authorization',inventory:repo.coordinatorAuthority.legacyInventory('xv','authorization'),obligations:[{queueId:'baton',kind:'coordination',evidenceRef:'baton-evidence'},{queueId:'legacy-unadmitted',kind:'work',evidenceRef:'actual-prior-work',resourceScope:'read-only',packageKey:'original',contract:{inputDigest:digest('actual-prior-inputs'),destination:'builder@xv',bodyHash:digest('legacy-unadmitted'),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}}}]};
  await repo.create({qitemId:'authorization',sourceSession:'lead@xv',destinationSession:'operator-agent@kernel',body:JSON.stringify({kind:'coordinator-legacy-enrollment',proposalDigest:legacyProposalDigest(packet)}),nudge:false});repo.claim({qitemId:'authorization',destinationSession:'operator-agent@kernel'});
  repo.coordinatorAuthority.migrateLegacy('operator-agent@kernel','operator-agent-g1',packet);
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
 }
 const raw=()=>({old:db.prepare("SELECT * FROM queue_items WHERE qitem_id='legacy-unadmitted'").get(),authority:db.prepare('SELECT * FROM coordinator_authority').all(),resources:db.prepare('SELECT * FROM coordinator_resources').all(),assignments:db.prepare('SELECT * FROM coordinator_assignments').all()});
 const sweep=()=>{const status=createStuckSweepStatus();return runStuckSweep({db,queueRepo:repo,status,resolveOrchestrator:()=>null,log:()=>{}}).then(result=>({result:result as any,status:status.snapshot()}));};
 it('first enabled unadmitted candidate refuses, yet healthy other rig and later resolution run; raw custody unchanged',async()=>{
  await setup();const before=raw();
  await repo.create({qitemId:'resolved-finding',sourceSession:'worker@other',destinationSession:'worker@other',body:'resolved diagnostic',tags:[STUCK_SWEEP_FINDING_TAG,findingDedupTag('unclaimed-obligation','already-resolved')],nudge:false});
  const {result,status}=await sweep();expect(result.outcome).toBe('failed');expect(result.refusals).toEqual(expect.arrayContaining([expect.objectContaining({qitemId:'legacy-unadmitted',code:'coordinator_envelope_required'})]));
  expect(result.findings).toEqual(expect.arrayContaining([expect.objectContaining({qitemId:'healthy',action:'created'}),expect.objectContaining({findingQitemId:'resolved-finding',action:'closed'})]));
  expect(repo.getById('resolved-finding')?.state).toBe('done');expect(raw()).toEqual(before);expect(status.findingsRouted).toBe(2);
 });
 it('already committed earlier healthy finding is reported accurately after later refusal',async()=>{
  await setup(true);const {result,status}=await sweep();expect(result.outcome).toBe('failed');expect(result.findings.filter((x:any)=>x.action==='created')).toHaveLength(2);expect(status.findingsRouted).toBe(2);expect(result.findings[0].qitemId).toBe('healthy');
 });
 it('recovery is a distinct system notice with actual current Operator task/return, not a fabricated assignment',async()=>{
  await setup();const {result}=await sweep();const refusal=result.refusals.find((x:any)=>x.qitemId==='legacy-unadmitted');expect(refusal.recoveryQueueId).toBeTruthy();const q=repo.getById(refusal.recoveryQueueId)!;const b=JSON.parse(q.body);
  expect(q.sourceSession).toBe('watchdog@system');expect(q.destinationSession).toBe('operator-agent@kernel');expect(b.original.sourceSession).toBe('lead@xv');expect(b.original.qitemId).toBe('legacy-unadmitted');expect(b.recipientGeneration).toBe('operator-agent-g1');expect(b.reason).toBe('coordinator_envelope_required');expect(b.deadline).toBeGreaterThan(Date.now());expect(b.returnPath.queueId).toBe(q.qitemId);expect(b.returnPath.actor).toBe('operator-agent@kernel');expect(b.required).toContain('Preserve');
  expect(db.prepare('SELECT * FROM coordinator_assignments').all()).toHaveLength(1);expect(db.prepare('SELECT * FROM coordinator_resources').all()).toEqual([]);
  repo.claim({qitemId:q.qitemId,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});expect(repo.getById(q.qitemId)?.state).toBe('in-progress');
 });
 it('same open facts/gen dedup; actual closure and generation change create successor without rewriting originals',async()=>{
  await setup();const first=(await sweep()).result.refusals[0].recoveryQueueId;expect((await sweep()).result.refusals[0].recoveryQueueId).toBe(first);
  repo.claim({qitemId:first,destinationSession:'operator-agent@kernel'});await repo.update({qitemId:first,actorSession:'operator-agent@kernel',state:'done',closureReason:'no-follow-on'});
  const second=(await sweep()).result.refusals[0].recoveryQueueId;expect(second).not.toBe(first);expect(JSON.parse(repo.getById(second)!.body).previousQueueId).toBe(first);
  db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();const third=(await sweep()).result.refusals[0].recoveryQueueId;expect(third).not.toBe(second);expect(JSON.parse(repo.getById(third)!.body).recipientGeneration).toBe('operator-agent-g2');expect(repo.getById(second)?.state).toBe('pending');expect(repo.getById(first)?.state).toBe('done');
 });
 it('source fact change gets new bounded notice; terminal underlying does not auto-complete claimed Operator work',async()=>{
  await setup();const first=(await sweep()).result.refusals[0].recoveryQueueId;repo.claim({qitemId:first,destinationSession:'operator-agent@kernel'});
  db.prepare("UPDATE queue_items SET body='changed historical evidence' WHERE qitem_id='legacy-unadmitted'").run();const second=(await sweep()).result.refusals[0].recoveryQueueId;expect(second).not.toBe(first);expect(JSON.parse(repo.getById(first)!.body).original.bodyHash).not.toBe(JSON.parse(repo.getById(second)!.body).original.bodyHash);
  await repo.update({qitemId:'legacy-unadmitted',actorSession:'lead@xv',state:'done',closureReason:'no-follow-on'});await sweep();expect(repo.getById(first)?.state).toBe('in-progress');expect(repo.getById(second)?.state).toBe('pending');
 });
 it('absent actual Operator retains explicit failure and healthy progress without fabricating queue custody',async()=>{
  await setup();db.prepare("DELETE FROM occupant_tenures WHERE node_id='operator-agent@kernel'").run();const {result,status}=await sweep();expect(result.refusals[0].recoveryError).toBe('stuck_sweep_operator_unavailable');expect(result.refusals[0].recoveryQueueId).toBeUndefined();expect(result.findings).toHaveLength(1);expect(status.findingsRouted).toBe(1);
 });
 it('late successor drift prevents old-generation notice transport, while new current recovery reaches real claim',async()=>{
  await setup();const first=(await sweep()).result.refusals[0].recoveryQueueId;db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id='operator-agent@kernel'").run();db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('op-binding','operator-agent@kernel','operator-agent@kernel','%1')").run();const sent:string[]=[];repo.attachTransport({send:async(dest,_text,opts)=>{if(dest==='operator-agent@kernel'){sent.push(opts!.queueAssignmentId!);repo.claim({qitemId:opts!.queueAssignmentId!,destinationSession:dest,identityProvenance:'transport:v1'});}return {ok:true,verified:true};}});
  await repo.drainPendingWakeIntents();expect(sent).toEqual([]);expect(repo.getById(first)?.state).toBe('pending');const current=(await sweep()).result.refusals[0].recoveryQueueId;expect(sent).toEqual([current]);expect(repo.getById(current)?.state).toBe('in-progress');expect(repo.getById(first)?.state).toBe('pending');
 });
 it('guarded resolution refusal is local, keeps its queue open, and later resolution still commits',async()=>{
  await setup();
  for(const id of ['guarded-finding','later-finding'])await repo.create({qitemId:id,sourceSession:'worker@other',destinationSession:'worker@other',body:'existing diagnostic',tags:[STUCK_SWEEP_FINDING_TAG,findingDedupTag('unclaimed-obligation',id+'-resolved-source')],nudge:false});
  db.exec("CREATE TRIGGER guarded_finding_update BEFORE UPDATE OF state ON queue_items WHEN OLD.qitem_id='guarded-finding' BEGIN SELECT RAISE(ABORT,'fixture_guarded_resolution'); END");
  const {result,status}=await sweep();expect(result.refusals).toEqual(expect.arrayContaining([expect.objectContaining({qitemId:'guarded-finding',recoveryQueueId:expect.any(String)})]));expect(repo.getById('guarded-finding')?.state).toBe('pending');expect(repo.getById('later-finding')?.state).toBe('done');expect(status.findingsRouted).toBe(3);
 });

 it('source changes after refusal but before recovery commit are explicitly rechecked, without stale notice creation',async()=>{
  await setup();const create=repo.create.bind(repo);let changed=false;
  vi.spyOn(repo,'create').mockImplementation(async input=>{try{return await create(input);}catch(error){if(!changed&&input.sourceSession==='lead@xv'){changed=true;db.prepare("UPDATE queue_items SET body='actual late source evidence' WHERE qitem_id='legacy-unadmitted'").run();}throw error;}});
  const {result,status}=await sweep();expect(result.refusals[0].recoveryError).toBe('stuck_sweep_source_changed');expect(result.refusals[0].recoveryQueueId).toBeUndefined();expect(result.findings).toHaveLength(1);expect(result.findings[0].qitemId).toBe('healthy');expect(status.findingsRouted).toBe(1);expect(repo.getById('legacy-unadmitted')?.body).toBe('actual late source evidence');
 });

 it('expired unclaimed finite control creates exactly one linked successor and preserves original bytes',async()=>{
  await setup();const first=(await sweep()).result.refusals[0].recoveryQueueId;const original=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(first);const deadline=JSON.parse(repo.getById(first)!.body).deadline;
  const run=()=>runStuckSweep({db,queueRepo:repo,now:new Date(deadline),resolveOrchestrator:()=>null,log:()=>{}});
  const next=(await run()).refusals![0].recoveryQueueId!;expect(next).not.toBe(first);expect(JSON.parse(repo.getById(next)!.body).previousQueueId).toBe(first);expect(JSON.parse(repo.getById(next)!.body).deadline).toBe(deadline+60000);expect((await run()).refusals![0].recoveryQueueId).toBe(next);expect(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(first)).toEqual(original);
 });
 it.each(['claimed','unknown'])('expired %s control remains held without replacement or clearing debt',async mode=>{
  await setup();const first=(await sweep()).result.refusals[0].recoveryQueueId;const deadline=JSON.parse(repo.getById(first)!.body).deadline;
  if(mode==='claimed')repo.claim({qitemId:first,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});
  else db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE audit_pointer=?").run(first);
  const before=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(first);const effects=db.prepare('SELECT * FROM outbox_entries WHERE audit_pointer=?').all(first);
  const result=await runStuckSweep({db,queueRepo:repo,now:new Date(deadline),resolveOrchestrator:()=>null,log:()=>{}});
  expect(result.refusals![0].recoveryError).toBe('stuck_sweep_expired_control_held');expect(result.refusals![0].recoveryQueueId).toBeUndefined();expect(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(first)).toEqual(before);expect(db.prepare('SELECT * FROM outbox_entries WHERE audit_pointer=?').all(first)).toEqual(effects);
 });
 it('only exact durable accepted disposition satisfies terminal seat custody; blocked unaccepted return stays held',async()=>{
  await setup();repo.claim({qitemId:'legacy-unadmitted',destinationSession:'builder@xv',identityProvenance:'transport:v1'});
  await repo.create({qitemId:'typed-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'original',inputDigest:digest('actual-prior-inputs'),evidence:[{kind:'report',ref:'actual-report'}]}),identityProvenance:'transport:v1',nudge:false});
  await repo.update({qitemId:'legacy-unadmitted',actorSession:'builder@xv',state:'done',closureReason:'handed_off_to',closureTarget:'lead@xv',identityProvenance:'transport:v1'});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','original','typed-return');
  repo.claim({qitemId:'typed-return',destinationSession:'lead@xv',identityProvenance:'transport:v1'});await repo.update({qitemId:'typed-return',actorSession:'lead@xv',state:'blocked',identityProvenance:'transport:v1'});
  expect(((await sweep()).result.refusals ?? []).some((r:any)=>r.qitemId==='legacy-unadmitted')).toBe(true);
  const service=new CoordinationRecoveryService(repo,()=>null);service.accept('lead@xv','lead-g1','xv','original','typed-return','actual-technical-acceptance');
  expect(((await sweep()).result.refusals ?? []).some((r:any)=>r.qitemId==='legacy-unadmitted')).toBe(false);
  db.prepare("UPDATE queue_items SET body='tampered' WHERE qitem_id='typed-return'").run();expect(((await sweep()).result.refusals ?? []).some((r:any)=>r.qitemId==='legacy-unadmitted')).toBe(true);
 });

});
