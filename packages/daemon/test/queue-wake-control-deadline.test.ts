import {createHash} from "node:crypto";
import {describe,it,expect} from "vitest";
import Database from "better-sqlite3";
import {seed} from "./helpers/coordinator-fixture.js";
import {QueueRepository} from "../src/domain/queue-repository.js";
import {EventBus} from "../src/domain/event-bus.js";
import {OutboxHandler} from "../src/domain/outbox-handler.js";
import {runWakeLadderTick,WakeLadderScheduler} from "../src/domain/queue-wake-ladder.js";

async function fixture(){
 const db=new Database(":memory:");seed(db);
 db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host','now','now')").run();
 const repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});
 const outbox=new OutboxHandler(db);repo.attachOutbox(outbox);
 db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES('op','operator-agent@kernel','operator-agent@kernel','%1')").run();
 const sends:string[]=[];repo.attachTransport({send:async(_dest:string,_body:string,opts:any)=>{sends.push(opts.queueAssignmentId);return {ok:true,verified:false};}} as any);
 await repo.create({qitemId:'protected',sourceSession:'sender@external',destinationSession:'builder@xv',body:'valuable original',nudge:false});
 db.prepare("UPDATE queue_items SET handed_off_from='origin',last_nudge_result='failed:offline' WHERE qitem_id='protected'").run();
 db.prepare(`INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('r','op','builder@xv','builder@xv','builder-g1','native','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved','now','now')`).run();
 const firstAt=new Date();const tick=(at:Date)=>runWakeLadderTick({db,queueRepo:repo,now:at,attemptWake:async(id)=>{repo.recordNudgeAttempt(id,'verified');return 'verified';}});
 const first=await tick(firstAt),qid=first.refusals![0]!.recoveryQueueId!;
 return {db,repo,outbox,sends,firstAt,tick,qid};
}
function deadlineRows(db:Database.Database){return db.prepare("SELECT qitem_id FROM queue_items WHERE json_valid(body) AND json_extract(body,'$.action')='reconcile-overdue-wake-control'").all() as {qitem_id:string}[];}
describe('finite wake-control deadline backstop',()=>{
 it('persists deadline and exposes real scanner instead of generic60minute net',async()=>{const f=await fixture();try{
  const row=f.repo.getById(f.qid)!;const deadline=new Date(f.firstAt.getTime()+60000).toISOString();
  expect(row.expiresAt).toBe(deadline);expect(row.waiting?.deadlineAt).toBe(deadline);
  expect(row.waiting?.nextBackstop.mechanism).toBe('wake-ladder:control-deadline');expect(row.waiting?.nextBackstop.intervalSeconds).toBe(60);
 }finally{f.db.close();}});
 it('overdue escalates once to current Operator and never re-drives indeterminate parent',async()=>{const f=await fixture();try{
  const original=f.db.prepare("SELECT * FROM queue_items WHERE qitem_id='protected'").get();const intent=f.outbox.getById('wake-intent-'+f.qid);expect(intent?.deliveryState).toBe('indeterminate');
  await f.tick(new Date(f.firstAt.getTime()+59999));expect(deadlineRows(f.db)).toHaveLength(0);
  const result:any=await f.tick(new Date(f.firstAt.getTime()+60001));expect(deadlineRows(f.db)).toHaveLength(1);
  const child=f.repo.getById(deadlineRows(f.db)[0]!.qitem_id)!;expect(child.destinationSession).toBe('operator-agent@kernel');expect(JSON.parse(child.body).recipientGeneration).toBe('operator-agent-g1');
  expect(result.refusals.some((r:any)=>r.code==='wake_control_deadline_missed'&&r.recoveryQueueId===child.qitemId)).toBe(true);
  await f.tick(new Date(f.firstAt.getTime()+180000));expect(deadlineRows(f.db)).toHaveLength(1);expect(f.sends.filter(id=>id===f.qid)).toHaveLength(1);expect(f.sends.filter(id=>id===child.qitemId)).toHaveLength(1);
  expect(f.outbox.getById('wake-intent-'+f.qid)).toEqual(intent);expect(f.db.prepare("SELECT * FROM queue_items WHERE qitem_id='protected'").get()).toEqual(original);
 }finally{f.db.close();}});
 it('genuine Operator claims correlated escalation; blocked owned parent is never rewritten',async()=>{const f=await fixture();try{
  f.repo.attachTransport({send:async(_d:string,_b:string,o:any)=>{if(o.queueAssignmentId!==f.qid)f.repo.claim({qitemId:o.queueAssignmentId,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});return {ok:true,verified:true};}} as any);
  await f.tick(new Date(f.firstAt.getTime()+60001));expect(f.repo.getById(deadlineRows(f.db)[0]!.qitem_id)?.state).toBe('in-progress');
  f.repo.claim({qitemId:f.qid,destinationSession:'operator-agent@kernel',identityProvenance:'transport:v1'});
  await f.repo.update({qitemId:f.qid,actorSession:'operator-agent@kernel',state:'blocked',blockedOn:'auth:real-boundary',wakeAfterSeconds:1800});
  const before=f.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(f.qid);await f.tick(new Date(f.firstAt.getTime()+240000));expect(f.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(f.qid)).toEqual(before);expect(deadlineRows(f.db)).toHaveLength(1);
 }finally{f.db.close();}});
 it('missing actual Operator remains visible failure without creating children',async()=>{const f=await fixture();try{
  f.db.prepare("DELETE FROM occupant_tenures WHERE node_id='operator-agent@kernel'").run();
  const result:any=await f.tick(new Date(f.firstAt.getTime()+60001));expect(result.refusals.some((r:any)=>r.code==='wake_control_operator_unavailable')).toBe(true);expect(deadlineRows(f.db)).toHaveLength(0);
 }finally{f.db.close();}});
 it('recipient lifecycle rotation gates stale committed intent before physical send',async()=>{const f=await fixture();try{
  const deliver=f.repo.deliverWakeForSuccessor.bind(f.repo);f.repo.deliverWakeForSuccessor=async(id,d,n,s)=>{if(id!==f.qid)f.db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-g2' WHERE node_id='operator-agent@kernel'").run();await deliver(id,d,n,s);};
  await f.tick(new Date(f.firstAt.getTime()+60001));expect(deadlineRows(f.db)).toHaveLength(1);const id=deadlineRows(f.db)[0]!.qitem_id;
  expect(f.sends).not.toContain(id);expect(f.outbox.getById('wake-intent-'+id)?.deliveryState).toBe('failed');expect(f.repo.getById(f.qid)?.claimedAt).toBeNull();
 }finally{f.db.close();}});
 it('late changed/terminal source facts do not fabricate original completion or escalate obsolete state',async()=>{const f=await fixture();try{
  f.db.prepare("UPDATE seat_dispatch_reservations SET state='released' WHERE reservation_id='r'").run();
  await f.repo.update({qitemId:'protected',actorSession:'sender@external',state:'done',closureReason:'no-follow-on'});const before=f.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(f.qid);
  const result:any=await f.tick(new Date(f.firstAt.getTime()+60001));expect(result.controls.some((r:any)=>r.qitemId===f.qid&&r.disposition==='source-terminal')).toBe(true);
  expect(deadlineRows(f.db)).toHaveLength(0);expect(f.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(f.qid)).toEqual(before);
 }finally{f.db.close();}});
 it('foreign correlated ID refuses rather than crediting false custody or minting a replacement',async()=>{const f=await fixture();try{
  const id='qitem-wake-control-overdue-'+createHash('sha256').update(JSON.stringify([f.qid,'operator-agent-g1'])).digest('hex').slice(0,24);
  await f.repo.create({qitemId:id,sourceSession:'sender@external',destinationSession:'worker@other',body:'unrelated valuable work',nudge:false});
  const before=f.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(id);const result=await f.tick(new Date(f.firstAt.getTime()+60001));
  expect(result.refusals?.some(r=>r.code==='wake_control_recovery_conflict')).toBe(true);expect(deadlineRows(f.db)).toHaveLength(0);expect(f.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(id)).toEqual(before);expect(f.sends).not.toContain(id);
 }finally{f.db.close();}});
 it('changed source facts refuse old deadline while independent current work still progresses',async()=>{const f=await fixture();try{
  f.db.prepare("UPDATE seat_dispatch_reservations SET state='released' WHERE reservation_id='r'").run();
  await f.repo.update({qitemId:'protected',actorSession:'sender@external',state:'blocked',blockedOn:'external:actual-boundary',wakeAfterSeconds:1800});
  await f.repo.create({qitemId:'healthy',sourceSession:'sender@external',destinationSession:'worker@other',body:'independent ready work',nudge:false});
  f.db.prepare("UPDATE queue_items SET handed_off_from='origin',last_nudge_result='failed:offline' WHERE qitem_id='healthy'").run();
  const result=await f.tick(new Date(f.firstAt.getTime()+60001));expect(result.refusals?.some(r=>r.code==='wake_control_source_changed')).toBe(true);expect(deadlineRows(f.db)).toHaveLength(0);expect(result.actions.some(r=>r.qitemId==='healthy'&&r.action==='retry')).toBe(true);expect(f.repo.getById(f.qid)?.state).toBe('pending');
 }finally{f.db.close();}});
 it('existing scheduler bounds scan cadence to60s without another timer engine',()=>{let ms=0;const scheduler=new WakeLadderScheduler({runTick:async()=>({outcome:'clean',actions:[]}),setTimer:(_cb,n)=>{ms=n;return {} as any;},clearTimer:()=>{}});scheduler.start();expect(ms).toBeLessThanOrEqual(60000);void scheduler.stop();});
});
