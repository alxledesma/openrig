import { beforeEach, afterEach, it, expect, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import Database from "better-sqlite3";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { SeatDispatchReservationService, type ReservationRequest } from "../src/domain/seat-dispatch-reservation.js";
import { rotationActiveQueueRows, rotationLocalAddresses } from "../src/domain/rotation-local-custody.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { Hono } from "hono";
import { queueRoutes } from "../src/routes/queue.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { DiscoveryRepository } from "../src/domain/discovery-repository.js";
import { SeatHandoverService } from "../src/domain/seat-handover-service.js";
import { DefaultOccupantInvalidator } from "../src/domain/occupant-invalidator.js";
import { observeCodexSandbox } from "../src/domain/permission-drift.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { readHistoricalFailure } from "../src/domain/failed-precommit-proof.js";
import { canonical } from "../src/domain/seat-dispatch-reservation.js";

let db: Database.Database, guard: SeatDeliveryGuard, service: SeatDispatchReservationService, repo: QueueRepository;
let dir: string, file: string, nativeId: string;
let verify: ReturnType<typeof vi.fn>;
let census: ReturnType<typeof vi.fn>;
const target="builder@xv", operator="operator-agent@kernel", profile="fixture-profile";
const runtimeContract={runtime:"codex",model:"gpt-6.1-sol",provider:"openai",profile,effort:"low",permissions:{sandbox:{type:"workspace-write"},approval:"never"}};
const hash=()=>createHash("sha256").update(fs.readFileSync(path.join(dir,`${profile}.config.toml`))).digest("hex");
function request(): ReservationRequest {return {reservationId:"rotation-attempt-1",operationId:"operation-1",nodeId:target,generation:"builder-g1",reason:"bounded fixture cutover",profileSha256:hash(),expected:{protocol:"generation-queue-runtime-idle-v1",generation:"native-old",queue:[],runtimeContract,checkpointHash:"exact-checkpoint",reservationId:"rotation-attempt-1",operationId:"operation-1"}};}
async function reserve(){return service.reserve(operator,"operator-agent-g1",request());}
function makeService(){guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));service=new SeatDispatchReservationService({db,guard,verifyPredecessor:async(s,e)=>verify(s,e),observeSuccessor:async()=>({nativeId,runtimeContract}),censusFailedAttempt:async(r,e)=>census(r,e)});}
beforeEach(()=>{
 dir=fs.mkdtempSync(path.join(os.tmpdir(),"dispatch-reservation-"));file=path.join(dir,"state.sqlite");db=new Database(file);seed(db);
 db.prepare("UPDATE sessions SET status='running',startup_status='ready'").run();
 db.prepare("UPDATE nodes SET runtime='codex',model='gpt-6.1-sol',cwd=?,codex_config_profile=? WHERE id=?").run(dir,profile,target);
 db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date().toISOString(),new Date().toISOString());
 fs.writeFileSync(path.join(dir,`${profile}.config.toml`),'model="gpt-6.1-sol"\nmodel_provider="openai"\n',{mode:0o600});vi.stubEnv("CODEX_HOME",dir);
 nativeId="native-new";census=vi.fn(async()=>({remainingPids:[],observedAt:new Date().toISOString()}));verify=vi.fn(async()=>{});makeService();repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>resolveGuardTarget(db,s)?.occupant??null});repo.attachOutbox(new OutboxHandler(db));
});
afterEach(()=>{db.close();fs.rmSync(dir,{recursive:true,force:true});vi.unstubAllEnvs();});
async function source(destination=target){return repo.create({sourceSession:"lead@xv",destinationSession:destination,body:"preserve exact producer obligation",nudge:false});}
function realHandover(eventBus = new EventBus(db), successorRuntimeContract = {...runtimeContract,permissions:{sandbox:{type:"workspace-write"},approval:"never"}}) {
 const sessionRegistry = new SessionRegistry(db);
 sessionRegistry.updateBinding(target,{tmuxSession:target,tmuxPane:"%0"});
 const fakeTmux = {
   deliveryGuard:guard,hasSession:async()=>true,createSession:async()=>({ok:true}),
   listPanes:async()=>[{id:"%0",index:0,cwd:dir,width:80,height:24,active:true}],
   killSession:async()=>({ok:true}),respawnPane:async()=>({ok:true}),setRemainOnExit:async()=>({ok:true}),
   signalPaneProcess:async()=>({ok:true}),isPaneDead:async()=>true,sendText:async()=>({ok:true}),
   sendKeys:async()=>({ok:true}),capturePaneScreen:async()=>"checkpoint present",
   getDefaultShell:async()=>"/bin/zsh",getPaneCommand:async()=>"zsh",
 } as unknown as TmuxAdapter;
 return new SeatHandoverService({
   db,rigRepo:new RigRepository(db),sessionRegistry,discoveryRepo:new DiscoveryRepository(db),eventBus,
   tmuxAdapter:fakeTmux,dispatchReservations:service,rotationPrecondition:async()=>{},
   runtimeAdapters:{codex:{runtime:"codex",launchHarness:async()=>({ok:true,resumeToken:"native-new",resumeType:"codex_id",appliedLaunch:observeCodexSandbox(" -s workspace-write")}),checkReady:async()=>({ready:true})} as never},
   occupantInvalidator:new DefaultOccupantInvalidator({enforcer:{invalidateOccupant:()=>{}},contextUsage:{invalidateOccupantSidecar:()=>{}},queue:repo}),
   now:()=>new Date("2026-10-03T13:00:00Z"),sleep:async()=>{},readinessTimeoutMs:50,
 });
}
async function realClaimedCutover(faultAfterReservationCommit=false){
 // The general coordinator fixture uses the seat name as its original session ID;
 // native session history uses ordered IDs. Give the old fixture row an earlier ID.
 db.prepare("UPDATE sessions SET id='0-builder-old' WHERE id=?").run(target);
 const claimed=await source();repo.claim({qitemId:claimed.qitemId,destinationSession:target});
 const req=request();req.expected.runtimeContract={...runtimeContract,permissions:{sandbox:{type:"workspace-write"},approval:"never"}};
 // The expected native queue is the real in-progress claim, not a fabricated empty list.
 req.expected.queue=rotationActiveQueueRows(db,target);
 const r=await service.reserve(operator,"operator-agent-g1",req);
 const bus=new EventBus(db);
 if(faultAfterReservationCommit) vi.spyOn(bus,"persistWithinTransaction").mockImplementation(()=>{throw new Error("fault after atomic successor reservation commit");});
 const handover=realHandover(bus);
 const result=await handover.handover({seatRef:target,source:"fresh",reason:"bounded claimed-item canary",operator,
   rotationExpected:req.expected,rotationActor:operator,rotationActorGeneration:"operator-agent-g1"});
 return {claimed,r,result};
}

it("persistent reservation binds current actor/node/generation, audited exact checkpoint/config and replay",async()=>{
 const r=await reserve();expect(r.state).toBe("reserved");expect(guard.ownsLifecycle(target)).toBe(false);expect(await reserve()).toEqual(r);
 expect(db.prepare("SELECT count(*) AS n FROM seat_dispatch_reservation_audit").get()).toEqual({n:1});
 await expect(service.reserve(operator,"operator-agent-g1",{...request(),reason:"changed"})).rejects.toThrow("replay differs");
});
it("actual create/transactional create/claim/unclaim/update/handoff/completion/readdress refuse atomically and preserve all original rows",async()=>{
 const original=await source(), claimedBefore=await source(), elsewhere=await source("peer@xv");
 repo.claim({qitemId:claimedBefore.qitemId,destinationSession:target});await reserve();
 const before=db.prepare("SELECT * FROM queue_items ORDER BY qitem_id").all();
 await expect(source()).rejects.toThrow("seat_dispatch_reserved");
 expect(()=>db.transaction(()=>repo.createWithinTransaction({sourceSession:"lead@xv",destinationSession:target,body:"transactional"})).immediate()).toThrow("seat_dispatch_reserved");
 expect(()=>repo.claim({qitemId:original.qitemId,destinationSession:target})).toThrow("seat_dispatch_reserved");
 expect(()=>repo.unclaim(claimedBefore.qitemId,target,"predecessor release attempt")).toThrow("seat_dispatch_reserved");
 expect(()=>repo.update({qitemId:original.qitemId,state:"blocked",blockedOn:"exact-blocker",actorSession:target})).toThrow("seat_dispatch_reserved");
 await expect(repo.handoff({qitemId:elsewhere.qitemId,toSession:target,fromSession:"peer@xv",body:"handoff"})).rejects.toThrow("seat_dispatch_reserved");
 await expect(repo.handoffAndComplete({qitemId:elsewhere.qitemId,toSession:target,fromSession:"peer@xv",body:"completion"})).rejects.toThrow("seat_dispatch_reserved");
 expect(()=>repo.routeToFallback(elsewhere.qitemId,target,"target fallback")).toThrow("seat_dispatch_reserved");
 expect(db.prepare("SELECT * FROM queue_items ORDER BY qitem_id").all()).toEqual(before);
 expect((await source("reviewer@xv")).state).toBe("pending");
});
it("same-host qualified destination fenced, genuine remote destination and unrelated work unchanged",async()=>{
 await reserve();await expect(source(`${target}@fixture-host`)).rejects.toThrow("seat_dispatch_reserved");
 expect((await source(`${target}@another-host`)).state).toBe("pending");
});
it("local qualified obligations and resource scopes enter both frozen snapshot and native expected queue; foreign host stays out",async()=>{
 const local=await source(`${target}@fixture-host`), remote=await source(`${target}@another-host`);
 const baton=await source("lead@xv");
 db.prepare("INSERT INTO coordinator_authority(rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,state,operation_id,coordinators) VALUES('xv',?,'lead@xv','lead-g1',1,9999999999999,'active','fixture-authority',?)").run(baton.qitemId,JSON.stringify(["lead@xv","peer@xv"]));
 db.prepare("INSERT INTO coordinator_packages(rig_id,package_key,contract,contract_hash,admitted_by) VALUES('xv','local-package','{}','hash','lead@xv')").run();
 db.prepare("INSERT INTO coordinator_packages(rig_id,package_key,contract,contract_hash,admitted_by) VALUES('xv','foreign-package','{}','hash','lead@xv')").run();
 db.prepare("INSERT INTO coordinator_packages(rig_id,package_key,contract,contract_hash,admitted_by) VALUES('xv','stage-local','{}','hash','lead@xv')").run();
 db.prepare("INSERT INTO coordinator_packages(rig_id,package_key,contract,contract_hash,admitted_by) VALUES('xv','stage-foreign','{}','hash','lead@xv')").run();
 db.prepare("INSERT INTO coordinator_assignments(rig_id,package_key,queue_id,destination,body_hash,owner_session,owner_generation,epoch) VALUES('xv','local-package','local-q',?, 'hash','lead@xv','lead-g1',1)").run(`${target}@fixture-host`);
 db.prepare("INSERT INTO coordinator_assignments(rig_id,package_key,queue_id,destination,body_hash,owner_session,owner_generation,epoch) VALUES('xv','foreign-package','foreign-q',?, 'hash','lead@xv','lead-g1',1)").run(`${target}@another-host`);
 db.prepare("INSERT INTO coordinator_stage_assignments(rig_id,package_key,source,destination,body_hash,queue_id,source_generation) VALUES('xv','stage-local',?,'reviewer@xv','hash','stage-local-q','builder-g1')").run(`${target}@fixture-host`);
 db.prepare("INSERT INTO coordinator_stage_assignments(rig_id,package_key,source,destination,body_hash,queue_id,source_generation) VALUES('xv','stage-foreign',?,'reviewer@xv','hash','stage-foreign-q','builder-g1')").run(`${target}@another-host`);
 db.prepare("INSERT INTO coordinator_resources(rig_id,resource_key,package_key) VALUES('xv','source/local','local-package')").run();
 db.prepare("INSERT INTO coordinator_resources(rig_id,resource_key,package_key) VALUES('xv','source/foreign','foreign-package')").run();
 db.prepare("INSERT INTO coordinator_resources(rig_id,resource_key,package_key) VALUES('xv','source/stage-local','stage-local')").run();
 db.prepare("INSERT INTO coordinator_resources(rig_id,resource_key,package_key) VALUES('xv','source/stage-foreign','stage-foreign')").run();
 expect(rotationLocalAddresses(db,target)).toEqual([target,`${target}@fixture-host`]);
 const active=rotationActiveQueueRows(db,target);
 expect(active.map(x=>x.id)).toEqual([local.qitemId]);
 expect(active[0]).toMatchObject({destinationSession:`${target}@fixture-host`,bodyHash:expect.stringMatching(/^[a-f0-9]{64}$/)});
 const req=request();req.expected.queue=active;
 const r=await service.reserve(operator,"operator-agent-g1",req);
 const frozen=JSON.parse(r.frozen_snapshot);
 expect(frozen.queue.map((x:{qitem_id:string})=>x.qitem_id)).toEqual([local.qitemId]);
 expect(frozen.queue[0].destination_session).toBe(`${target}@fixture-host`);
 expect(frozen.queue.map((x:{qitem_id:string})=>x.qitem_id)).not.toContain(remote.qitemId);
 expect(frozen.resources).toEqual([{resource_key:"source/local",package_key:"local-package"},{resource_key:"source/stage-local",package_key:"stage-local"}]);
 expect(JSON.parse(r.expected_json).queue).toEqual(active);
});
it("missing durable local host identity refuses rather than omitting qualified custody",async()=>{
 db.prepare("DELETE FROM self_host_identity WHERE singleton=1").run();
 expect(()=>rotationActiveQueueRows(db,target)).toThrow("host identity");
 await expect(reserve()).rejects.toThrow("host identity");
});
it("real second process SQL writer cannot admit work while fence persists across restart/crash",async()=>{
 await reserve();db.close();db=new Database(file);makeService();
 const result=spawnSync(process.execPath,['-e',`const D=require('better-sqlite3');const d=new D(process.argv[1]);try{d.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES('racing','now','now','lead@xv','builder@xv','pending','must retain producer request')").run();process.exitCode=9;}catch(e){console.log(e.message);process.exitCode=0;}finally{d.close();}`,file],{encoding:'utf8',cwd:path.resolve('.')});
 expect(result.status).toBe(0);expect(result.stdout.trim()).toBe("seat_dispatch_reserved");expect(db.prepare("SELECT 1 FROM queue_items WHERE qitem_id='racing'").get()).toBeUndefined();expect(service.get("rotation-attempt-1").state).toBe("reserved");
});
it("migration 092 upgrades an already-091 database and retains its ordinary dispatch fence",async()=>{
 const prior=new Database(":memory:");
 try {
   migrate(prior,ALL_MIGRATIONS.filter(m=>m.name!=="092_reserved_claim_release.sql"));
   expect(prior.prepare("SELECT name FROM schema_migrations WHERE name='091_seat_dispatch_reservations.sql'").get()).toBeDefined();
   migrate(prior,ALL_MIGRATIONS);
   expect(prior.prepare("SELECT name FROM schema_migrations WHERE name='092_reserved_claim_release.sql'").get()).toBeDefined();
   expect(prior.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name='seat_dispatch_queue_update'").get()).toBeDefined();
 } finally { prior.close(); }
});
it("claim-release permit is transaction-local to the swap; another DB connection cannot use it",async()=>{
 const claimed=await source();repo.claim({qitemId:claimed.qitemId,destinationSession:target});
 const r=await reserve();
 await guard.lifecycle([target],async()=>{
   service.start(r,operator,"operator-agent-g1");
   expect(()=>db.transaction(()=>{
     service.prepareClaimRelease(r.reservation_id,"builder-g1");
     const child=spawnSync(process.execPath,['-e',`const D=require('better-sqlite3');const d=new D(process.argv[1]);d.pragma('busy_timeout=50');try{d.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL,claimed_at=NULL,closure_required_at=NULL WHERE qitem_id=?").run(process.argv[2]);process.exitCode=9;}catch(e){console.log(e.message);process.exitCode=0;}finally{d.close();}`,file,claimed.qitemId],{encoding:'utf8',cwd:path.resolve('.')});
     expect(child.status).toBe(0);expect(child.stdout).toMatch(/database is locked|seat_dispatch_reserved/);
     repo.releaseClaimsByGeneration("builder-g1");
     throw new Error("injected crash before seat-swap commit");
   })()).toThrow("injected crash");
 },r.reservation_id);
 expect(service.get(r.reservation_id).state).toBe("started");
 expect(db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(claimed.qitemId))
   .toEqual({state:"in-progress",claimed_by_generation_uuid:"builder-g1"});
 expect(db.prepare("SELECT COUNT(*) AS n FROM seat_dispatch_claim_releases").get()).toEqual({n:0});
 await expect(source()).rejects.toThrow("seat_dispatch_reserved");
});
it("competing queue mutation during asynchronous locked recensus makes reservation refuse",async()=>{
 verify.mockImplementationOnce(async()=>{await source();});await expect(reserve()).rejects.toThrow("changed during preflight");
 expect(db.prepare("SELECT count(*) AS n FROM seat_dispatch_reservations").get()).toEqual({n:0});expect(repo.list({destinationSession:target})).toHaveLength(1);
});
it("held lifecycle prevents reserve until original operation completes; postwait generation drift refuses",async()=>{
 let resume!:()=>void;const held=guard.lifecycle([target],async()=>new Promise<void>(r=>resume=r));await new Promise(r=>setTimeout(r,0));const pending=reserve();
 db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id=?").run(target);resume();await held;await expect(pending).rejects.toThrow("changed");
});
it("stale actor or target generation and stale operation never become current authority",async()=>{
 await expect(service.reserve(operator,"old-operator",request())).rejects.toThrow("current local actor");
 await expect(service.reserve(operator,"operator-agent-g1",{...request(),generation:"old-builder"})).rejects.toThrow("Incumbent changed");
 await reserve();await expect(service.release(operator,"operator-agent-g1","rotation-attempt-1",{operationId:"other",reason:"bad",mode:"cancel_before_replacement"})).rejects.toThrow("Exact operation");
});
it("TUI appended profile bytes require current hash; old hash refuses without silently changing effective tuple",async()=>{
 const old=request();fs.appendFileSync(path.join(dir,`${profile}.config.toml`),'\n[tui]\nscreen_reader_detection_done=true\n');
 await expect(service.reserve(operator,"operator-agent-g1",old)).rejects.toThrow("Profile bytes changed");expect((await reserve()).state).toBe("reserved");
});
it("automatic input retained and unrelated lifecycle refuses while exact lifecycle lease joins",async()=>{
 await reserve();const outbox=new OutboxHandler(db);
 const held=await guard.operation(target,async()=>{throw new Error("must not paste");},async(binding)=>outbox.retain({outboxId:"retained-at-reservation",senderSession:"lead@xv",destinationSession:target,body:"accepted durable message"},binding));
 expect(held.deliveryState).toBe("retained");expect(outbox.getById(held.outboxId)?.body).toBe("accepted durable message");
 await expect(guard.lifecycle([target],async()=>{})).rejects.toThrow("durable cutover reservation");
 await guard.lifecycle([target],async()=>expect(guard.ownsLifecycle(target)).toBe(true),"rotation-attempt-1");
});
it("pre-cutover exact current incumbent or Kernel Operator can cancel; no time or process-exit auto-unlock",async()=>{
 await reserve();const cancelled=await service.release(operator,"operator-agent-g1","rotation-attempt-1",{operationId:"operation-1",reason:"proven no replacement",mode:"cancel_before_replacement"});expect(cancelled.state).toBe("released");expect((await source()).state).toBe("pending");
});
async function commitSuccessor(){const r=await reserve();await guard.lifecycle([target],async()=>{service.assertHandover(r.reservation_id,r.operation_id,target,operator,"operator-agent-g1",request().expected);service.start(r,operator,"operator-agent-g1");db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('successor',?,2,'builder-g2','fresh')").run(target);db.prepare("UPDATE nodes SET handover_result='complete' WHERE id=?").run(target);db.prepare("UPDATE sessions SET resume_token='native-new' WHERE node_id=?").run(target);guard.rebindLifecycle(target);service.committed(r.reservation_id,operator,"operator-agent-g1");},r.reservation_id);}
const receipt=(kind:"successor_ack"|"independent_acceptance")=>({operationId:"operation-1",checkpointHash:"exact-checkpoint",kind,evidenceRef:"fixture:actual-native-bounded-receipt"});
it("started/uncertain replacement cannot cancel or expire into an unlocked seat",async()=>{
 const r=await reserve();await guard.lifecycle([target],async()=>service.start(r,operator,"operator-agent-g1"),r.reservation_id);
 await expect(service.release(operator,"operator-agent-g1",r.reservation_id,{operationId:r.operation_id,reason:"uncertain",mode:"cancel_before_replacement"})).rejects.toThrow("Replacement may have occurred");expect(service.get(r.reservation_id).state).toBe("started");
});
it("handover start and commit audit the current performing actor, not a different reserver",async()=>{
 const r=await service.reserve(target,"builder-g1",request());
 await guard.lifecycle([target],async()=>{
   service.assertHandover(r.reservation_id,r.operation_id,target,operator,"operator-agent-g1",request().expected);
   service.start(r,operator,"operator-agent-g1");
   db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('operator-handover-successor',?,2,'builder-g2','fresh')").run(target);
   db.prepare("UPDATE nodes SET handover_result='complete' WHERE id=?").run(target);
   db.prepare("UPDATE sessions SET resume_token='native-new' WHERE node_id=?").run(target);
   guard.rebindLifecycle(target);
   service.committed(r.reservation_id,operator,"operator-agent-g1");
 },r.reservation_id);
 const audit=db.prepare("SELECT action,actor_session,actor_generation FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id").all(r.reservation_id);
 expect(audit).toEqual([
   {action:"reserved",actor_session:target,actor_generation:"builder-g1"},
   {action:"replacement_started",actor_session:operator,actor_generation:"operator-agent-g1"},
   {action:"successor_committed",actor_session:operator,actor_generation:"operator-agent-g1"},
 ]);
 expect(service.get(r.reservation_id)).toMatchObject({performer_session:operator,performer_generation:"operator-agent-g1"});
 expect(()=>db.prepare("UPDATE seat_dispatch_reservations SET performer_session='reviewer@xv' WHERE reservation_id=?").run(r.reservation_id)).toThrow("seat_dispatch_performer_immutable");
});
it("actual handover performer and release owner cannot self-accept an incumbent-created reservation",async()=>{
 const r=await service.reserve(target,"builder-g1",request());
 await guard.lifecycle([target],async()=>{
   service.assertHandover(r.reservation_id,r.operation_id,target,operator,"operator-agent-g1",request().expected);
   service.start(r,operator,"operator-agent-g1");
   db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('operator-performed-successor',?,2,'builder-g2','fresh')").run(target);
   db.prepare("UPDATE nodes SET handover_result='complete' WHERE id=?").run(target);
   db.prepare("UPDATE sessions SET resume_token='native-new' WHERE node_id=?").run(target);
   guard.rebindLifecycle(target);service.committed(r.reservation_id,operator,"operator-agent-g1");
 },r.reservation_id);
 await service.attest(target,"builder-g2",r.reservation_id,receipt("successor_ack"));
 await expect(service.attest(operator,"operator-agent-g1",r.reservation_id,receipt("independent_acceptance"))).rejects.toThrow("distinct independent");
 await expect(service.release(operator,"operator-agent-g1",r.reservation_id,{operationId:r.operation_id,reason:"cannot self approve",mode:"accepted_successor"})).rejects.toThrow("receipts required");
 await service.attest("reviewer@xv","reviewer-g1",r.reservation_id,receipt("independent_acceptance"));
 expect((await service.release(operator,"operator-agent-g1",r.reservation_id,{operationId:r.operation_id,reason:"separate validator",mode:"accepted_successor"})).state).toBe("released");
});
it("fresh committed successor requires current native proof, actual successor receipt and distinct validator before matched Operator release",async()=>{
 await commitSuccessor();const release={operationId:"operation-1",reason:"accepted actual successor",mode:"accepted_successor" as const};
 await expect(service.release(operator,"operator-agent-g1","rotation-attempt-1",release)).rejects.toThrow("receipts required");
 await expect(service.attest(target,"builder-g1","rotation-attempt-1",receipt("successor_ack"))).rejects.toThrow("current local actor");
 await service.attest(target,"builder-g2","rotation-attempt-1",receipt("successor_ack"));
 await expect(service.attest(target,"builder-g2","rotation-attempt-1",receipt("independent_acceptance"))).rejects.toThrow("distinct");
 await service.attest("reviewer@xv","reviewer-g1","rotation-attempt-1",receipt("independent_acceptance"));
 expect((await service.release(operator,"operator-agent-g1","rotation-attempt-1",release)).state).toBe("released");
 expect((await service.release(operator,"operator-agent-g1","rotation-attempt-1",release)).state).toBe("released");expect((await source()).state).toBe("pending");
});
it("same native UUID, changed config or retired validator invalidates acceptance instead of releasing",async()=>{
 await commitSuccessor();nativeId="native-old";await expect(service.attest(target,"builder-g2","rotation-attempt-1",receipt("successor_ack"))).rejects.toThrow("successor");nativeId="native-new";
 await service.attest(target,"builder-g2","rotation-attempt-1",receipt("successor_ack"));await service.attest("reviewer@xv","reviewer-g1","rotation-attempt-1",receipt("independent_acceptance"));
 db.prepare("UPDATE occupant_tenures SET generation_uuid='reviewer-g2' WHERE node_id='reviewer@xv'").run();
 await expect(service.release(operator,"operator-agent-g1","rotation-attempt-1",{operationId:"operation-1",reason:"must refuse",mode:"accepted_successor"})).rejects.toThrow("receipts required");
 fs.appendFileSync(path.join(dir,`${profile}.config.toml`),'# drift');await expect(service.attest(target,"builder-g2","rotation-attempt-1",receipt("successor_ack"))).rejects.toThrow("successor");expect(service.get("rotation-attempt-1").state).toBe("committed");
});
it("queue API returns structured retryable409 rather than false delivery/500",async()=>{
 await reserve();const app=new Hono();app.use('*',async(c,next)=>{c.set('queueRepo' as never,repo as never);await next();});app.route('/queue',queueRoutes());
 const response=await app.request('/queue/create',{method:'POST',headers:{'Content-Type':'application/json','X-OpenRig-Session':'lead@xv'},body:JSON.stringify({sourceSession:'lead@xv',destinationSession:target,body:'retain in producer obligation'})});expect(response.status).toBe(409);expect(await response.json()).toMatchObject({error:'seat_dispatch_reserved',retryable:true});expect(repo.list({destinationSession:target})).toHaveLength(0);
});
it("real DB handover releases only the retiring in-progress claim inside the fenced seat swap",async()=>{
 const unrelated=await source("reviewer@xv");
 const beforeUnrelated=db.prepare("SELECT * FROM queue_items WHERE qitem_id=?").get(unrelated.qitemId);
 const {claimed,r,result}=await realClaimedCutover();
 expect(result,JSON.stringify(result)).toMatchObject({ok:true});
 expect(service.get(r.reservation_id)).toMatchObject({state:"committed",successor_generation:expect.any(String)});
 expect(db.prepare("SELECT state,claimed_by_generation_uuid,claimed_at FROM queue_items WHERE qitem_id=?").get(claimed.qitemId))
   .toEqual({state:"pending",claimed_by_generation_uuid:null,claimed_at:null});
 expect(db.prepare("SELECT * FROM queue_items WHERE qitem_id=?").get(unrelated.qitemId)).toEqual(beforeUnrelated);
 const releases=db.prepare("SELECT qitem_id,post_ts_updated FROM seat_dispatch_claim_releases WHERE reservation_id=?").all(r.reservation_id);
 expect(releases).toEqual([{qitem_id:claimed.qitemId,post_ts_updated:expect.any(String)}]);
 await expect(source()).rejects.toThrow("seat_dispatch_reserved");
 expect(()=>repo.claim({qitemId:claimed.qitemId,destinationSession:target})).toThrow("seat_dispatch_reserved");
 db.prepare("UPDATE sessions SET status='running',startup_status='ready' WHERE node_id=? AND status!='superseded'").run(target);
 await service.attest(target,service.get(r.reservation_id).successor_generation!,r.reservation_id,receipt("successor_ack"));
 await service.attest("reviewer@xv","reviewer-g1",r.reservation_id,receipt("independent_acceptance"));
 expect((await service.release(operator,"operator-agent-g1",r.reservation_id,{operationId:r.operation_id,reason:"exact successor accepted",mode:"accepted_successor"})).state).toBe("released");
});
it("fault after reservation commit rolls back seat, claim release, and successor commit together",async()=>{
 const {claimed,r,result}=await realClaimedCutover(true);
 expect(result).toMatchObject({ok:false,code:"handover_commit_failed"});
 expect(service.get(r.reservation_id)).toMatchObject({state:"started",successor_generation:null});
 expect(guard.target(target).occupant).toBe("builder-g1");
 expect(db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?").get(claimed.qitemId))
   .toEqual({state:"in-progress",claimed_by_generation_uuid:"builder-g1"});
 expect(db.prepare("SELECT COUNT(*) AS n FROM seat_dispatch_claim_releases WHERE reservation_id=?").get(r.reservation_id)).toEqual({n:0});
 await expect(source()).rejects.toThrow("seat_dispatch_reserved");
});
it("accepted successor proof rejects a claim-release ledger that differs from its actual postimage",async()=>{
 const {claimed,r,result}=await realClaimedCutover();expect(result).toMatchObject({ok:true});
 db.prepare("UPDATE sessions SET status='running',startup_status='ready' WHERE node_id=? AND status!='superseded'").run(target);
 db.prepare("UPDATE seat_dispatch_claim_releases SET post_ts_updated='forged-postimage' WHERE reservation_id=? AND qitem_id=?")
   .run(r.reservation_id,claimed.qitemId);
 await expect(service.attest(target,service.get(r.reservation_id).successor_generation!,r.reservation_id,receipt("successor_ack")))
   .rejects.toThrow("Current fresh successor/config/queue continuity proof unavailable");
 expect(service.get(r.reservation_id).state).toBe("committed");
 await expect(source()).rejects.toThrow("seat_dispatch_reserved");
});

const abandonment={operationId:"operation-1",reason:"exact failed attempt with recovered native predecessor",mode:"abandon_failed_precommit" as const};
async function failedAttempt(){
 const r=await reserve();await guard.lifecycle([target],async()=>{
  service.start(r,operator,"operator-agent-g1");service.recordPrepared(r,operator,"operator-agent-g1","prepared-g2");
  service.recordFailedPrecommit(r.reservation_id,operator,"operator-agent-g1",{preparedGeneration:"prepared-g2",discoveredId:null,nativeId:"native-failed",replacementStarted:true},"handover_commit_failed","completed");
 },r.reservation_id);return r;
}
it("real atomic rollback records exact failure cleanup; abandonment preserves custody and grants no credit",async()=>{
 const {r,claimed,result}=await realClaimedCutover(true);expect(result).toMatchObject({ok:false,code:"handover_commit_failed"});
 const rows=()=>({queue:db.prepare("SELECT * FROM queue_items").all(),sessions:db.prepare("SELECT * FROM sessions").all(),tenures:db.prepare("SELECT * FROM occupant_tenures").all(),resources:db.prepare("SELECT * FROM coordinator_resources").all()});const before=rows();
 const failure=db.prepare("SELECT evidence_json FROM seat_dispatch_reservation_audit WHERE action='failed_precommit'").get() as {evidence_json:string};
 expect(JSON.parse(failure.evidence_json)).toMatchObject({operationId:r.operation_id,code:"handover_commit_failed",cleanup:"completed",effects:{preparedGeneration:expect.any(String),discoveredId:expect.any(String),nativeId:"native-new"}});
 expect(()=>repo.unclaim(claimed.qitemId,target,"unsafe")).toThrow("seat_dispatch_reserved");
 const released=await service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment);
 expect(released).toMatchObject({state:"released",successor_generation:null,successor_native_id:null});expect(verify).toHaveBeenCalledTimes(2);expect(census).toHaveBeenCalledOnce();expect(rows()).toEqual(before);
 expect(db.prepare("SELECT COUNT(*) AS n FROM seat_dispatch_claim_releases").get()).toEqual({n:0});
 const audit=db.prepare("SELECT evidence_json FROM seat_dispatch_reservation_audit WHERE action IN ('failed_precommit_recovered','released')").all() as Array<{evidence_json:string}>;expect(audit).toHaveLength(2);for(const x of audit)expect(JSON.parse(x.evidence_json)).toMatchObject({deliveryOrQualificationCredit:false,continuityCredit:false});
 expect(await service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).toEqual(released);
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,{...abandonment,reason:"changed"})).rejects.toThrow("replay differs");
 await expect(service.reserve(operator,"operator-agent-g1",request())).rejects.toThrow("replay differs");
 const next=request();next.reservationId="rotation-attempt-2";next.operationId="operation-2";next.expected={...next.expected,reservationId:next.reservationId,operationId:next.operationId,checkpointHash:"fresh-checkpoint"};expect((await service.reserve(operator,"operator-agent-g1",next)).state).toBe("reserved");
});
it("recovery requires current genuine Operator, exact operation/mode and original replay generation",async()=>{
 const r=await failedAttempt();await expect(service.abandonFailedPrecommit(target,"builder-g1",r.reservation_id,abandonment)).rejects.toThrow("genuine Kernel Operator");
 await expect(service.abandonFailedPrecommit(operator,"retired",r.reservation_id,abandonment)).rejects.toThrow("current local actor");
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,{...abandonment,operationId:"wrong"})).rejects.toThrow("exact operation");
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,{...abandonment,mode:"unknown" as never})).rejects.toThrow("Explicit failed");
 await service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment);db.prepare("UPDATE occupant_tenures SET generation_uuid='operator-agent-g2' WHERE node_id=?").run(operator);
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g2",r.reservation_id,abandonment)).rejects.toThrow("replay differs");
});
it("reserved, committed, historical missing proof and uncertain cleanup never unlock",async()=>{
 const r=await reserve();await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow("Exact started");
 await guard.lifecycle([target],async()=>service.start(r,operator,"operator-agent-g1"),r.reservation_id);
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow("historical reconciliation required");
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,{...abandonment,historicalProofRef:"invented"})).rejects.toThrow("historical reconciliation required");
 await guard.lifecycle([target],async()=>{service.recordPrepared(r,operator,"operator-agent-g1","prepared-g2");service.recordFailedPrecommit(r.reservation_id,operator,"operator-agent-g1",{preparedGeneration:"prepared-g2",discoveredId:null,nativeId:null,replacementStarted:true},"fault","uncertain");},r.reservation_id);
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow("uncertain");
 db.prepare("UPDATE seat_dispatch_reservations SET state='committed',successor_generation='g2' WHERE reservation_id=?").run(r.reservation_id);
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow("Exact started");await expect(source()).rejects.toThrow("seat_dispatch_reserved");
});
it.each(["generation","profile","config","successor","ledger","discovery","native","session"])("%s drift/remaining effect preserves reservation fence",async(kind)=>{
 const local=await source(`${target}@fixture-host`);repo.claim({qitemId:local.qitemId,destinationSession:`${target}@fixture-host`});const r=await failedAttempt();
 if(kind==="generation")db.prepare("UPDATE occupant_tenures SET generation_uuid='wrong' WHERE node_id=?").run(target);
 if(kind==="profile")fs.appendFileSync(path.join(dir,`${profile}.config.toml`),'# drift');
 if(kind==="config")db.prepare("UPDATE nodes SET model='wrong' WHERE id=?").run(target);
 if(kind==="successor")db.prepare("UPDATE seat_dispatch_reservations SET successor_native_id='possible-commit' WHERE reservation_id=?").run(r.reservation_id);
 if(kind==="ledger")db.prepare("INSERT INTO seat_dispatch_claim_releases(reservation_id,qitem_id,predecessor_generation,preimage_ts_updated) VALUES(?,?,?,?)").run(r.reservation_id,local.qitemId,"builder-g1","now");
 if(kind==="discovery")new DiscoveryRepository(db).upsertDiscoveredSession({tmuxSession:target,tmuxPane:"%0",runtimeHint:"codex",confidence:"high"});
 if(kind==="native")census.mockResolvedValue({remainingPids:[1234],observedAt:new Date().toISOString()});
 if(kind==="session")db.prepare("UPDATE sessions SET resume_token='changed' WHERE node_id=?").run(target);
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow();expect(service.get(r.reservation_id).state).toBe("started");
 await expect(source(`${target}@fixture-host`)).rejects.toThrow("seat_dispatch_reserved");expect(()=>repo.unclaim(local.qitemId,`${target}@fixture-host`,"unsafe")).toThrow("seat_dispatch_reserved");
});
it("async proof drift is detected again in final transaction",async()=>{
 const r=await failedAttempt();verify.mockImplementationOnce(async()=>{db.prepare("UPDATE nodes SET cwd='drifted' WHERE id=?").run(target);});await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow("Frozen canonical/alias");expect(service.get(r.reservation_id).state).toBe("started");
});
it("fresh compiled second process handover cannot acquire target/Operator during recovery",async()=>{
 const r=await failedAttempt();let resume!:()=>void,entered!:()=>void;const ready=new Promise<void>(resolve=>entered=resolve);verify.mockImplementationOnce(async()=>{entered();await new Promise<void>(resolve=>resume=resolve);});
 const recovering=service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment);await ready;
 const child=spawnSync(process.execPath,['--input-type=module','-e',`import D from 'better-sqlite3';import {SeatDispatchReservationService} from './dist/domain/seat-dispatch-reservation.js';import {SeatDeliveryGuard,resolveGuardTarget} from './dist/domain/seat-delivery-guard.js';const db=new D(process.argv[1]);const guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));const service=new SeatDispatchReservationService({db,guard,verifyPredecessor:async()=>{},observeSuccessor:async()=>({nativeId:'unused',runtimeContract:{}})});try{await service.withAttemptLock('rotation-attempt-1','operator-agent@kernel','operator-agent-g1',async()=>{throw new Error('must never run');});process.exitCode=9;}catch(e){console.log(e.code);process.exitCode=e.code==='reservation_attempt_busy'?0:8;}finally{db.close();}`,file],{encoding:'utf8',cwd:path.resolve('.')});
 expect(child.status,child.stderr).toBe(0);expect(child.stdout.trim()).toBe("reservation_attempt_busy");expect(service.get(r.reservation_id).state).toBe("started");resume();expect((await recovering).state).toBe("released");expect(db.prepare("SELECT COUNT(*) AS n FROM seat_dispatch_attempt_locks").get()).toEqual({n:0});
});
it("historical reconciliation performs fresh proof and records reconciliation now without fabricating old audit",async()=>{
 const r=await reserve();await guard.lifecycle([target],async()=>service.start(r,operator,"operator-agent-g1"),r.reservation_id);
 const current=service.get(r.reservation_id),effects={preparedGeneration:"historical-prepared",discoveredId:null,nativeId:"historical-failed-native",replacementStarted:true};
 const sha=(s:string)=>createHash("sha256").update(s).digest("hex");
 const tuple={reservationId:r.reservation_id,operationId:r.operation_id,nodeId:r.node_id,predecessorGeneration:r.predecessor_generation,predecessorNativeId:r.predecessor_native_id,performerSession:current.performer_session,performerGeneration:current.performer_generation,
  fullSnapshotSha256:sha(service.snapshot(r.node_id,r.session_name)),predecessorRowsSha256:sha(canonical({binding:db.prepare("SELECT * FROM bindings WHERE node_id=?").all(r.node_id),sessions:db.prepare("SELECT * FROM sessions WHERE node_id=? ORDER BY id").all(r.node_id),tenures:db.prepare("SELECT * FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal").all(r.node_id)}))};
 fs.mkdirSync(path.join(dir,"frozen"));const write=(name:string,x:unknown)=>{const relative=`frozen/${name}.json`,bytes=JSON.stringify(x);fs.writeFileSync(path.join(dir,relative),bytes);return {path:relative,sha256:sha(bytes)};};
 const failure=write("failure",{result:{ok:false,code:"handover_commit_failed"}}),attribution=write("attribution",{...tuple,effects});const proof=write("proof",{protocol:"failed-precommit-history-v1",...tuple,effects,failure,attribution});
 service=new SeatDispatchReservationService({db,guard,verifyPredecessor:async(s,e)=>verify(s,e),observeSuccessor:async()=>({nativeId,runtimeContract}),censusFailedAttempt:async(r,e)=>census(r,e),historicalFailure:(ref,hash,r)=>readHistoricalFailure(dir,ref,hash,r)});
 expect((await service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,{...abandonment,historicalProofRef:proof.path,historicalProofSha256:proof.sha256})).state).toBe("released");
 expect(db.prepare("SELECT action FROM seat_dispatch_reservation_audit ORDER BY id").all()).toEqual([{action:"reserved"},{action:"replacement_started"},{action:"historical_failed_precommit_reconciled"},{action:"released"}]);expect(census).toHaveBeenCalledOnce();
});
it.each(["body","claim","resource"])("actual frozen alias %s drift retains fence",async(kind)=>{
 const local=await source(`${target}@fixture-host`),baton=await source("lead@xv");
 db.prepare("INSERT INTO coordinator_authority VALUES('xv',?,'lead@xv','lead-g1',1,9999999999999,'active','fixture-op','[]',NULL)").run(baton.qitemId);
 db.prepare("INSERT INTO coordinator_packages VALUES('xv','p','{}','hash','lead@xv')").run();db.prepare("INSERT INTO coordinator_assignments(rig_id,package_key,queue_id,destination,body_hash,owner_session,owner_generation,epoch) VALUES('xv','p',?,?, 'hash','lead@xv','lead-g1',1)").run(local.qitemId,`${target}@fixture-host`);db.prepare("INSERT INTO coordinator_resources VALUES('xv','source/file','p')").run();const r=await failedAttempt();
 if(kind==="resource")db.prepare("UPDATE coordinator_resources SET resource_key='drifted' WHERE resource_key='source/file'").run();
 else{const sql=(db.prepare("SELECT sql FROM sqlite_master WHERE name='seat_dispatch_queue_update'").get() as {sql:string}).sql;db.exec("DROP TRIGGER seat_dispatch_queue_update");if(kind==="body")db.prepare("UPDATE queue_items SET body='drifted' WHERE qitem_id=?").run(local.qitemId);else db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='drifted' WHERE qitem_id=?").run(local.qitemId);db.exec(sql);}
 await expect(service.abandonFailedPrecommit(operator,"operator-agent-g1",r.reservation_id,abandonment)).rejects.toThrow("Frozen canonical/alias");expect(service.get(r.reservation_id).state).toBe("started");expect(()=>db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES('probe','now','now','lead@xv','builder@xv','pending','retain')").run()).toThrow("seat_dispatch_reserved");
});
