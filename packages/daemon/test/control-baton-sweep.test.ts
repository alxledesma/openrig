import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type Database from 'better-sqlite3';
import { createDb } from '../src/db/connection.js';
import { seed } from './helpers/coordinator-fixture.js';
import { EventBus } from '../src/domain/event-bus.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { createStuckSweepStatus, runStuckSweep } from '../src/domain/queue-stuck-sweep.js';
import { makeParkedOwnerConsumerPolicy, makeRigAnchor, PARKED_OWNER_POLICY_NAME } from '../src/domain/policies/parked-owner-consumer.js';
import type { PolicyJob } from '../src/domain/policies/types.js';
let db:Database.Database,repo:QueueRepository,batonId:string,workId:string;
beforeEach(async()=>{
 db=createDb();seed(db);repo=new QueueRepository(db,new EventBus(db),{validateRig:()=>true});
 // Ordinary product work in another rig remains ordinary; no native effects.
 const baton=await repo.create({sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'canonical control',nudge:false});
 const work=await repo.create({sourceSession:'operator-agent@kernel',destinationSession:'worker@other',body:'ordinary product',nudge:false});
 batonId=baton.qitemId;workId=work.qitemId;
 repo.claim({qitemId:batonId,destinationSession:'lead@xv'});repo.claim({qitemId:workId,destinationSession:'worker@other'});
 const past=new Date(Date.now()-3600000).toISOString();
 db.prepare('UPDATE queue_items SET claimed_at=?,closure_required_at=?,ts_updated=? WHERE qitem_id IN (?,?)').run(past,past,past,batonId,workId);
 db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='lead-g1' WHERE qitem_id=?").run(batonId);
 // Authoritative enrollment fixture; expired lease deliberately does NOT grant authority.
 db.prepare('INSERT INTO coordinator_authority (rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,state,operation_id,coordinators,recovery_queue_id) VALUES (?,?,?,?,?,?,?,?,?,NULL)').run('xv',batonId,'lead@xv','lead-g1',1,Date.now()-1000,'active','fixture-enrollment',JSON.stringify(['lead@xv','peer@xv']));
});
afterEach(()=>db.close());
it.each(['active','reconciling','recovery'])('generic sweep excludes enrolled %s baton despite expired lease and still routes ordinary overdue work',async state=>{
 db.prepare('UPDATE coordinator_authority SET state=? WHERE rig_id=?').run(state,'xv');
 expect(repo.isStandingAuthorityMarker(batonId)).toBe(false);expect(repo.ordinaryWorkActionable(batonId)).toBe(false);
 const before=db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(batonId),transitions=repo.listTransitions(batonId);
 const result=await runStuckSweep({db,queueRepo:repo,status:createStuckSweepStatus(),resolveOrchestrator:()=>null,isRegisteredHost:()=>false,log:()=>{}});
 expect(result.findings.some(f=>f.qitemId===workId)).toBe(true);expect(result.findings.some(f=>f.qitemId===batonId)).toBe(false);
 expect(db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(batonId)).toEqual(before);expect(repo.listTransitions(batonId)).toEqual(transitions);
});
it('parked-owner delivery boundary excludes enrolled baton from its named mutation guidance but retains ordinary work',async()=>{
 const notes:Array<{id:string;note:string}>=[];
 const policy=makeParkedOwnerConsumerPolicy({diagnoseRig:()=>({seats:[{sessionName:'lead@xv',parked:true,activity:{value:'idle-at-prompt',needsInput:{count:0,reason:null}},obligations:{items:[{qitemId:batonId,state:'in-progress',summary:null}],held:[]}}]}),history:{listForJob:()=>[],countForJob:()=>0},rows:{
  listTransitions:id=>repo.listTransitions(id),appendNote:(id,note)=>{notes.push({id,note});return {ok:true};},recordNudgeResult:vi.fn(),listOpenIds:()=>[batonId,workId],ordinaryWorkActionable:id=>repo.ordinaryWorkActionable(id),
 }});
 const job={jobId:'fixture-job',policy:PARKED_OWNER_POLICY_NAME,target:{session:makeRigAnchor('xv')},context:{},intervalSeconds:120,lastFireAt:null,lastEvaluationAt:null} as PolicyJob;
 expect((await policy.evaluate(job)).action).not.toBe('send');expect(notes).toHaveLength(0);
 // Same existing consumer boundary with an ordinary obligation must still send.
 const ordinary=makeParkedOwnerConsumerPolicy({diagnoseRig:()=>({seats:[{sessionName:'worker@other',parked:true,activity:{value:'idle-at-prompt',needsInput:{count:0,reason:null}},obligations:{items:[{qitemId:workId,state:'in-progress',summary:null}],held:[]}}]}),history:{listForJob:()=>[],countForJob:()=>0},rows:{listTransitions:id=>repo.listTransitions(id),appendNote:(id,note)=>{notes.push({id,note});return {ok:true};},recordNudgeResult:vi.fn(),listOpenIds:()=>[workId],ordinaryWorkActionable:id=>repo.ordinaryWorkActionable(id)}});
 const result=await ordinary.evaluate({...job,target:{session:makeRigAnchor('other')}});
 expect(result.action).toBe('send');expect(JSON.stringify(result)).toContain(workId);expect(JSON.stringify(result)).not.toContain(batonId);expect(notes.every(n=>n.id===workId)).toBe(true);
});
it.each(['removed-enrollment','stale-generation','wrong-destination','wrong-claim','terminal','unregistered-rig'])('%s is not silently excluded as canonical control',async kind=>{
 if(kind==='removed-enrollment')db.prepare('DELETE FROM coordinator_authority WHERE rig_id=?').run('xv');
 if(kind==='stale-generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='new-generation' WHERE node_id='lead@xv'").run();
 if(kind==='wrong-destination')db.prepare("UPDATE queue_items SET destination_session='peer@xv' WHERE qitem_id=?").run(batonId);
 if(kind==='wrong-claim')db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='other-generation' WHERE qitem_id=?").run(batonId);
 if(kind==='terminal')db.prepare("UPDATE queue_items SET state='done' WHERE qitem_id=?").run(batonId);
 if(kind==='unregistered-rig')db.prepare("UPDATE coordinator_authority SET owner_session='worker@other',owner_generation='worker-g1' WHERE rig_id='xv'").run();
 expect(repo.coordinatorAuthority.isEnrolledControlBaton(batonId)).toBe(false);expect(repo.ordinaryWorkActionable(batonId)).toBe(true);
});
