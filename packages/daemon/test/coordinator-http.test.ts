import { describe,it,expect,beforeEach,afterEach } from "vitest";
import { Hono } from "hono";
import { createDb } from "../src/db/connection.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { SessionTransport } from "../src/domain/session-transport.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { queueRoutes } from "../src/routes/queue.js";
import { transportRoutes } from "../src/routes/transport.js";
import { coordinatorRoutes } from "../src/routes/coordinator.js";
import { digest } from "../src/domain/coordinator-authority-service.js";
import { seed,token } from "./helpers/coordinator-fixture.js";
import type Database from "better-sqlite3";
import type { TmuxAdapter } from "../src/adapters/tmux.js";

describe("real managed queue/transport boundary",()=>{
 let db:Database.Database,repo:QueueRepository,app:Hono,transport:SessionTransport,paneWrites:number;
 beforeEach(async()=>{
  db=createDb();seed(db);const bus=new EventBus(db);repo=new QueueRepository(db,bus);repo.attachOutbox(new OutboxHandler(db));paneWrites=0;
  const tmux={hasSession:async()=>true,probeSession:async()=>({state:"present"}),sendText:async()=>{paneWrites++;return {ok:true}},sendKeys:async()=>{paneWrites++;return {ok:true}},capturePaneContent:async()=>"idle",getPaneCommand:async()=>null,listSessions:async()=>[],listWindows:async()=>[],listPanes:async()=>[]} as unknown as TmuxAdapter;
  transport=new SessionTransport({db,rigRepo:new RigRepository(db),sessionRegistry:new SessionRegistry(db),tmuxAdapter:tmux,sleep:async()=>{}});
  await repo.create({qitemId:"baton",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"coordinate",nudge:false});
  repo.coordinatorAuthority.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:3600000,operationId:"enable"});
  repo.coordinatorAuthority.acknowledge("lead@xv",token,{operationId:"ack",obligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv")});
  repo.coordinatorAuthority.admit("operator-agent@kernel","operator-agent-g1","xv","p",{inputDigest:digest("i"),destination:"builder@xv",bodyHash:digest("build"),resources:["a"],returnContract:{destination:"lead@xv",evidenceRequired:["proof"]}});
  app=new Hono();app.use("*",async(c,next)=>{c.set("queueRepo" as never,repo);c.set("eventBus" as never,bus);c.set("sessionTransport" as never,transport);await next();});
  app.route("/api/queue",queueRoutes());app.route("/api/transport",transportRoutes());app.route("/api/coordinator",coordinatorRoutes({bearerToken:"test-token"}));
 });
 afterEach(()=>db.close());
 const call=(path:string,body:unknown,headers:Record<string,string>={})=>app.request(path,{method:"POST",headers:{"Content-Type":"application/json",...headers},body:JSON.stringify(body)});
 const caller={"X-OpenRig-Session":"lead@xv","X-OpenRig-Occupant-Generation":"lead-g1"};
 it("omitted envelope/tags cannot use legacy enabled-rig create",async()=>{
  const result=await call("/api/queue/create",{destinationSession:"builder@xv",body:"build"},caller);expect(result.status).toBe(409);expect((await result.json()).error).toBe("coordinator_envelope_required");expect(paneWrites).toBe(0);
 });
 it("body generation cannot supersede missing or mismatched immutable header",async()=>{
  for(const headers of [{"X-OpenRig-Session":"lead@xv"},{...caller,"X-OpenRig-Occupant-Generation":"lead-g2"}]){
   const result=await call("/api/queue/create",{destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p"}},headers);expect(result.status).toBe(409);expect((await result.json()).error).toBe("coordinator_generation_mismatch");
  }expect(repo.getById("work")).toBeNull();expect(paneWrites).toBe(0);
 });
 it("raw route cannot use caller body claims or a forged internal wake option",async()=>{
  const result=await call("/api/transport/send",{session:"builder@xv",text:"do this",actorSession:"worker@other",queueAssignmentId:"anything"},caller);expect(result.status).toBe(409);expect(paneWrites).toBe(0);
 });
 it("actual domain transport blocks bypass even with no route repository dependency",async()=>{
  expect((await transport.send("builder@xv","do this",{actorSession:"lead@xv"})).reason).toBe("coordinator_raw_dispatch_refused");
  expect((await transport.send("builder@xv","do this",{actorSession:"lead@xv",queueAssignmentId:"fake"})).reason).toBe("coordinator_wake_receipt_invalid");expect(paneWrites).toBe(0);
 });
 it("committed assignment wake stays allowed after epoch transfer without redispatch",async()=>{
  const row=await repo.create({qitemId:"work",sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p"},nudge:false});
  repo.coordinatorAuthority.transfer("lead@xv","lead-g1",{expected:token,oldOwner:"lead@xv",recipient:"peer@xv",recipientGeneration:"peer-g1",operationId:"transfer",leaseMs:3600000});
  const result=await transport.send("builder@xv","Queue handoff: work - check your queue.",{actorSession:"lead@xv",queueAssignmentId:row.qitemId});
  expect(result.ok).toBe(true);expect(paneWrites).toBe(2);expect(repo.coordinatorAuthority.obligations("xv")).toHaveLength(1);
 });
 it("cross-host forwarding is rejected before any local row or pane effect",async()=>{
  const result=await call("/api/queue/create",{destinationSession:"builder@xv",body:"build",hostId:"remote",dispatch:{token,packageKey:"p"}},caller);expect(result.status).toBe(409);expect((await result.json()).error).toBe("coordinator_cross_host_refused");expect(paneWrites).toBe(0);
 });
 it("mutation control requires terminal bearer and current Operator identity",async()=>{
  const res=await call("/api/coordinator/admit",{rigId:"xv",packageKey:"other",contract:{}},caller);expect(res.status).toBe(401);
  const spoof=await call("/api/coordinator/admit",{rigId:"xv",packageKey:"other",contract:{}},{...caller,Authorization:"Bearer test-token"});expect(spoof.status).toBe(409);expect((await spoof.json()).error).toBe("coordinator_operator_required");
 });
 it("registered native identity mapping does not assume logical-id equals session stem",async()=>{
  db.prepare("UPDATE nodes SET logical_id='orch1.lead' WHERE id='lead@xv'").run();
  const result=await call("/api/queue/create",{destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p"},nudge:false},caller);expect(result.status).toBe(201);
 });
});
