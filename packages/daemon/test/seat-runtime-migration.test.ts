import { beforeEach, afterEach, it, expect, vi } from "vitest";
import Database from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { SeatHandoverService } from "../src/domain/seat-handover-service.js";
import { SeatDispatchReservationService } from "../src/domain/seat-dispatch-reservation.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { DiscoveryRepository } from "../src/domain/discovery-repository.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SeatRuntimeMigration } from "../src/domain/seat-runtime-migration.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { DefaultOccupantInvalidator } from "../src/domain/occupant-invalidator.js";
import type { RuntimeMigrationRequest } from "../src/domain/seat-runtime-migration.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";

const target="peer@xv", operator="operator-agent@kernel", oldGeneration="peer-g1", nativeId="00000000-0000-4000-8000-000000000001";
const targetConfig={runtime:"codex" as const,provider:"openai" as const,model:"gpt-6-luna",effort:"high",codexConfigProfile:"migration-peer"};
let db:Database.Database,dir:string,guard:SeatDeliveryGuard,registry:SessionRegistry,bus:EventBus,service:SeatHandoverService;
let tmux:any,adapter:any,proof:any,oldSession:string,history:string;
let nativeProcesses:Set<number>;
const input=(runtimeMigration:RuntimeMigrationRequest,dryRun=false)=>({seatRef:target,source:"fresh",reason:"fresh zero-custody peer",operator:"untrusted-body@claim",rotationActor:operator,rotationActorGeneration:"operator-agent-g1",runtimeMigration,dryRun});
const node=()=>db.prepare("SELECT runtime,model,effort,codex_config_profile FROM nodes WHERE id=?").get(target);
const reservation=()=>db.prepare("SELECT * FROM seat_dispatch_reservations").get() as any;
async function prepare(){const r=await service.handover(input({operationId:"peer-mig-1",target:targetConfig},true));expect(r.ok,JSON.stringify(r)).toBe(true);return (r as any).plan.runtimeMigration as RuntimeMigrationRequest;}
function makeService(){
 const queue=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>resolveGuardTarget(db,s)?.occupant??null});
 return new SeatHandoverService({db,rigRepo:new RigRepository(db),sessionRegistry:registry,discoveryRepo:new DiscoveryRepository(db),eventBus:bus,tmuxAdapter:tmux as TmuxAdapter,
  runtimeAdapters:{codex:adapter},migrationPiSkillRoot:()=>path.join(dir,"pi-skills"),migrationPredecessorProcessExists:pid=>nativeProcesses.has(pid),migrationPiProve:async()=>({...proof,quiescence:{settled:true,observedAt:new Date().toISOString()}}),
  occupantInvalidator:new DefaultOccupantInvalidator({enforcer:{invalidateOccupant:()=>{}},contextUsage:{invalidateOccupantSidecar:()=>{}},queue}),sleep:async()=>{},readinessTimeoutMs:20});
}
beforeEach(()=>{
 nativeProcesses=new Set();
 dir=fs.mkdtempSync(path.join(os.tmpdir(),"migration-fixture-"));db=new Database(":memory:");seed(db);
 db.prepare("UPDATE sessions SET status='running',startup_status='ready'").run();
 db.prepare("UPDATE sessions SET id='0-peer-old' WHERE node_id=?").run(target);oldSession="0-peer-old";
 db.prepare("UPDATE nodes SET runtime='pi',model='old-pi',effort='low',cwd=? WHERE id=?").run(dir,target);
 db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date().toISOString(),new Date().toISOString());
 history=path.join(dir,"pi-session.jsonl");fs.writeFileSync(history,'{"id":"aabbccdd","history":"preserve"}\n');
 db.prepare("UPDATE sessions SET resume_type='pi_session_file',resume_token=? WHERE id=?").run(history,oldSession);
 fs.writeFileSync(path.join(dir,"guidance.md"),"Generic successor guidance");fs.writeFileSync(path.join(dir,"role.md"),"Generic role context");
 db.prepare("INSERT INTO node_startup_context(node_id,projection_entries_json,resolved_files_json,startup_actions_json,runtime) VALUES(?,'[]',?,?,'pi')").run(target,JSON.stringify([
  {path:"guidance.md",absolutePath:path.join(dir,"guidance.md"),ownerRoot:dir,deliveryHint:"guidance_merge",required:true,appliesOn:["fresh_start"]},
  {path:"role.md",absolutePath:path.join(dir,"role.md"),ownerRoot:dir,deliveryHint:"send_text",required:true,appliesOn:["fresh_start"]}
 ]),JSON.stringify([{type:"send_text",value:"OpenRig session identity: peer@xv",builtin:"session_identity",phase:"after_ready",appliesOn:["fresh_start"],idempotent:true},{type:"send_text",value:"Read the durable plan before work",phase:"after_ready",appliesOn:["fresh_start"],idempotent:true}]));
 registry=new SessionRegistry(db);registry.updateBinding(target,{tmuxSession:target,tmuxPane:"%0"});registry.updateBinding(operator,{tmuxSession:operator,tmuxPane:"%1"});
 guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));bus=new EventBus(db);
 tmux={deliveryGuard:guard,hasSession:vi.fn(async()=>true),listPanes:vi.fn(async()=>[{id:"%0",index:0,cwd:dir,width:80,height:24,active:true}]),
  killSession:vi.fn(async()=>({ok:true})),respawnPane:vi.fn(async()=>({ok:true})),setRemainOnExit:vi.fn(async()=>({ok:true})),signalPaneProcess:vi.fn(async()=>({ok:true})),isPaneDead:vi.fn(async()=>true),
  sendText:vi.fn(async()=>({ok:true})),sendKeys:vi.fn(async()=>({ok:true})),capturePaneScreen:vi.fn(async()=>"retained native history"),getDefaultShell:vi.fn(async()=>"/bin/zsh"),getPaneCommand:vi.fn(async()=>"zsh")};
 adapter={runtime:"codex",preflightRuntimeMigration:vi.fn(async()=>({profileSha256:"a".repeat(64),authenticated:true,effective:{model:targetConfig.model,provider:"openai",effort:"high",approval:"on-request",sandbox:"workspace-write"}})),
  project:vi.fn(async()=>({projected:[],skipped:[],failed:[]})),deliverStartup:vi.fn(async()=>({delivered:1,failed:[]})),
  launchHarness:vi.fn(async()=>({ok:true,resumeToken:nativeId,resumeType:"codex_id"})),checkReady:vi.fn(async()=>({ready:true}))};
 proof={state:"present",generation:oldGeneration,launchId:"pi-launch-old",lastEntryId:"aabbccdd",fingerprint:JSON.stringify({pane:"%0",runner:[10,9],pi:[11,10],launchId:"pi-launch-old",sidecarUpdatedAt:new Date().toISOString(),genSources:["env","env"]})};
 service=makeService();
});
afterEach(()=>{db.close();fs.rmSync(dir,{recursive:true,force:true});});

it("binds target launch, config and startup runtime atomically, preserves Pi history and exact new generation lineage",async()=>{
 const packet=await prepare();expect(adapter.launchHarness).not.toHaveBeenCalled();expect(reservation()).toBeUndefined();
 proof.fingerprint=JSON.stringify({...JSON.parse(proof.fingerprint),sidecarUpdatedAt:new Date(Date.now()+1).toISOString()});
 const priorHistory=fs.readFileSync(history,"utf8");const r=await service.handover(input(packet));expect(r.ok,JSON.stringify(r)).toBe(true);
 expect(node()).toEqual({runtime:"codex",model:"gpt-6-luna",effort:"high",codex_config_profile:"migration-peer"});
 expect(db.prepare("SELECT runtime FROM node_startup_context WHERE node_id=?").get(target)).toEqual({runtime:"codex"});
 expect((r as any).result.operator).toBe(operator);
 const generation=registry.currentOccupantTenure(target)!.generationUuid;expect(generation).not.toBe(oldGeneration);
 expect(adapter.launchHarness).toHaveBeenCalledWith(expect.objectContaining({model:targetConfig.model,effort:"high",codexConfigProfile:targetConfig.codexConfigProfile,launchGeneration:generation,launchPosture:"floor",tmuxPane:"%0"}),{name:target});
 expect(adapter.deliverStartup.mock.calls.map((c:any)=>c[0].map((f:any)=>f.deliveryHint))).toEqual([["guidance_merge"],["send_text"]]);
 expect(tmux.sendText.mock.calls.some((c:any)=>c[1]==="Read the durable plan before work")).toBe(true);
 expect(db.prepare("SELECT status,resume_type,resume_token FROM sessions WHERE id=?").get(oldSession)).toEqual({status:"superseded",resume_type:"pi_session_file",resume_token:history});
 expect(fs.readFileSync(history,"utf8")).toBe(priorHistory);expect(reservation()).toMatchObject({state:"released",predecessor_generation:oldGeneration,predecessor_native_id:history,successor_generation:generation,successor_native_id:nativeId});
 expect(db.prepare("SELECT count(*) AS n FROM seat_dispatch_claim_releases").get()).toEqual({n:0});expect(tmux.killSession).not.toHaveBeenCalled();
 const replay=await service.handover(input(packet));expect(replay).toMatchObject({ok:false,code:"runtime_migration_refused",refusalCode:"runtime_migration_attempt_exists"});expect(adapter.launchHarness).toHaveBeenCalledTimes(1);
});

it.each(["claim","authority","profile","auth","cursor","config","actor","typing-guard"])("refuses %s before any process effects",async(kind)=>{
 const packet=await prepare();
 if(kind==="claim")db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body,claimed_by_generation_uuid) VALUES('claimed','now','now','lead@xv',?,'in-progress','preserved',?)").run(target,oldGeneration);
 if(kind==="authority") {
 db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES('baton','now','now','lead@xv',?,'pending','preserved authority')").run(target);
 db.prepare("INSERT INTO coordinator_authority(rig_id,baton_id,owner_session,owner_generation,epoch,lease_until,state,operation_id,coordinators) VALUES('xv','baton',?,?,1,1,'reconciling','op',?)").run(target,oldGeneration,JSON.stringify([target,"lead@xv"]));
 }
 if(kind==="profile")adapter.preflightRuntimeMigration.mockResolvedValue({profileSha256:"b".repeat(64),authenticated:true,effective:{model:targetConfig.model,provider:"openai",effort:"high",approval:"on-request",sandbox:"danger-full-access"}});
 if(kind==="auth")adapter.preflightRuntimeMigration.mockRejectedValue(new Error("auth missing"));
 if(kind==="cursor")proof.lastEntryId="eeff0011";
 if(kind==="config")db.prepare("UPDATE nodes SET model='different' WHERE id=?").run(target);
 if(kind==="actor")db.prepare("UPDATE sessions SET status='exited' WHERE node_id=?").run(operator);
 if(kind==="typing-guard")await guard.set(target,true,"operator","explicit protection");
 const before=node(),r=await service.handover(input(packet));expect(r).toMatchObject({ok:false,code:"runtime_migration_refused"});expect(adapter.launchHarness).not.toHaveBeenCalled();expect(tmux.signalPaneProcess).not.toHaveBeenCalled();expect(reservation()).toBeUndefined();expect(node()).toEqual(before);
 if(kind==="typing-guard")expect(r).toMatchObject({refusalCode:"typing_guard_enabled"});
});

it.each(["launch","delivery","commit"])("retains unknown one-shot exclusion and predecessor configuration after %s failure",async(kind)=>{
 const packet=await prepare();
 if(kind==="launch")adapter.launchHarness.mockResolvedValue({ok:false,error:"native unavailable"});
 if(kind==="delivery")adapter.deliverStartup.mockImplementation(async(files:any[])=>({delivered:0,failed:files.some(f=>f.deliveryHint==="send_text")?[{path:"role.md",error:"failed"}]:[]}));
 if(kind==="commit")vi.spyOn(bus,"persistWithinTransaction").mockImplementation(()=>{throw new Error("commit fault");});
 const r=await service.handover(input(packet));expect(r).toMatchObject({ok:false,code:"runtime_migration_unknown",blindRetryAllowed:false});expect(reservation()).toMatchObject({state:"started",predecessor_generation:oldGeneration});
 expect(node()).toMatchObject({runtime:"pi",model:"old-pi"});expect(registry.currentOccupantTenure(target)!.generationUuid).toBe(oldGeneration);expect(fs.existsSync(history)).toBe(true);expect(tmux.killSession).not.toHaveBeenCalled();
 const rotations=new SeatDispatchReservationService({db,guard,verifyPredecessor:async()=>{},observeSuccessor:async()=>({nativeId:"unused",runtimeContract:{}})});
 expect(()=>rotations.get(reservation().reservation_id)).toThrow("rotation cannot");
 await expect(guard.operation(target,async()=>{})).rejects.toThrow("durable cutover reservation");
 expect(()=>db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES('racing','now','now','lead@xv',?,'pending','keep producer')").run(target)).toThrow("seat_dispatch_reserved");
 const count=adapter.launchHarness.mock.calls.length;await service.handover(input(packet));expect(adapter.launchHarness).toHaveBeenCalledTimes(count);
});

it("retains historical UNKNOWN and terminal claimant provenance without treating it as active custody",async()=>{
 db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body,claimed_by_generation_uuid) VALUES('historical','now','now','lead@xv',?,'handed-off','original obligation',?)").run(target,oldGeneration);
 const outbox=new OutboxHandler(db);const entry=outbox.record({senderSession:target,destinationSession:"lead@xv",body:"unresolved historical result"});
 db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id=?").run(entry.outboxId);
 const rows=db.prepare("SELECT * FROM outbox_entries").all(),queue=db.prepare("SELECT * FROM queue_items").all();const packet=await prepare();
 const r=await service.handover(input(packet));expect(r.ok,JSON.stringify(r)).toBe(true);expect(db.prepare("SELECT * FROM outbox_entries").all()).toEqual(rows);expect(db.prepare("SELECT * FROM queue_items").all()).toEqual(queue);
 const controller=new SeatRuntimeMigration({db,rigRepo:new RigRepository(db),sessionRegistry:registry,eventBus:bus,tmuxAdapter:tmux});
 expect(controller.inspect("peer-mig-1",operator,"operator-agent-g1")).toMatchObject({ok:true,state:"committed",nativeIdentityRecorded:true,blindRetryAllowed:false});
});
it("refuses a changed native cursor in the last preeffect check and a changed predecessor resume binding",async()=>{
 const packet=await prepare();const normal=adapter.preflightRuntimeMigration.getMockImplementation();let calls=0;
 adapter.preflightRuntimeMigration.mockImplementation(async(...args:any[])=>{if(++calls===2)proof.lastEntryId="12345678";return normal(...args);});
 expect(await service.handover(input(packet))).toMatchObject({ok:false,refusalCode:"runtime_migration_native_changed"});expect(tmux.signalPaneProcess).not.toHaveBeenCalled();expect(reservation()).toBeUndefined();
 proof.lastEntryId="aabbccdd";db.prepare("UPDATE sessions SET resume_token=? WHERE id=?").run(path.join(dir,"different.jsonl"),oldSession);
 expect(await service.handover(input(packet))).toMatchObject({ok:false,refusalCode:"runtime_migration_changed"});expect(tmux.signalPaneProcess).not.toHaveBeenCalled();
});

it("refuses silently dropping actual Pi skills and accepts only prepared byte-identical Codex counterparts",async()=>{
 const source=path.join(dir,"pi-skills","current","SKILL.md");fs.mkdirSync(path.dirname(source),{recursive:true});fs.writeFileSync(source,"actual current native skill");
 expect(await service.handover(input({operationId:"peer-mig-1",target:targetConfig},true))).toMatchObject({ok:false,refusalCode:"runtime_migration_skills_unprepared"});expect(tmux.signalPaneProcess).not.toHaveBeenCalled();
 const destination=path.join(dir,".agents","skills","current","SKILL.md");fs.mkdirSync(path.dirname(destination),{recursive:true});fs.writeFileSync(destination,fs.readFileSync(source));
 const packet=await prepare();fs.writeFileSync(destination,"changed skill");
 expect(await service.handover(input(packet))).toMatchObject({ok:false,refusalCode:"runtime_migration_skills_unprepared"});expect(tmux.signalPaneProcess).not.toHaveBeenCalled();
 fs.writeFileSync(destination,fs.readFileSync(source));expect((await service.handover(input(packet))).ok).toBe(true);expect(fs.readFileSync(source,"utf8")).toBe("actual current native skill");
});

it("does not launch Codex while an exact native Pi predecessor process survives pane replacement",async()=>{
 const packet=await prepare();nativeProcesses.add(11);
 const r=await service.handover(input(packet));expect(r).toMatchObject({ok:false,code:"runtime_migration_unknown",blindRetryAllowed:false});
 expect(adapter.launchHarness).not.toHaveBeenCalled();expect(adapter.project).not.toHaveBeenCalled();expect(reservation()).toMatchObject({state:"started"});expect(node()).toMatchObject({runtime:"pi"});expect(fs.existsSync(history)).toBe(true);
});
