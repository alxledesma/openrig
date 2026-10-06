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
 it("resume-owned derives the caller token and digest from immutable auth headers, never the body",async()=>{
  // Put the rig in the reconciling-but-LIVE state this command exists to continue. This route
  // fixture uses the real production clock, so the lease is live against Date.now().
  const live=Date.now()+60000;
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=?,operation_id='custody' WHERE rig_id='xv'").run(live);
  // The real CLI sends the installed native bearer plus seat identity headers.
  const native={...caller,Authorization:"Bearer test-token"};
  // The supported read requires the same native bearer the CLI already sends.
  const shown=await app.request("/api/coordinator/xv",{headers:{Authorization:"Bearer test-token"}});
  expect(shown.status).toBe(200);
  const current=await shown.json() as any;
  // The command derives its expected contract from this supported read, never from a hand-typed field.
  expect(current.obligationsDigest).toMatch(/^[0-9a-f]{64}$/);
  expect(current.authority.epoch).toBeGreaterThan(0);
  // Extra body claims must not be able to influence the outcome.
  const forged={rigId:"xv",leaseMs:120000,operationId:"http-resume-1",expectedEpoch:current.authority.epoch,expectedObligationsDigest:current.obligationsDigest,token:{rigId:"other",epoch:9999,generation:"attacker-gen"},actorSession:"attacker@elsewhere",obligationsDigest:"attacker-digest"};
  const ok=await call("/api/coordinator/resume-owned",forged,native);
  expect(ok.status).toBe(200);
  const receipt=await ok.json() as any;
  // Acknowledged, renewed, and every body claim ignored.
  expect(receipt.state).toBe("active");
  expect(receipt.rig_id).toBe("xv");
  expect(receipt.owner_session).toBe("lead@xv");
  expect(receipt.owner_generation).toBe("lead-g1");
  expect(receipt.operation_id).toBe("http-resume-1");
  // A stale expected contract refuses instead of silently resuming fresh state: an obligation
  // that moves after the read invalidates the digest the caller presented.
  await repo.create({qitemId:"stale-obligation",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"{}",nudge:false});
  const stale=await call("/api/coordinator/resume-owned",{rigId:"xv",leaseMs:120000,operationId:"http-resume-4",expectedEpoch:current.authority.epoch,expectedObligationsDigest:current.obligationsDigest},native);
  expect(stale.status).toBe(409);
  expect((await stale.json()).error).toBe("coordinator_reconciliation_changed");
  // A stale epoch refuses the same way.
  const staleEpoch=await call("/api/coordinator/resume-owned",{rigId:"xv",leaseMs:120000,operationId:"http-resume-5",expectedEpoch:current.authority.epoch+5,expectedObligationsDigest:current.obligationsDigest},native);
  expect(staleEpoch.status).toBe(409);
  expect((await staleEpoch.json()).error).toBe("coordinator_cas_lost");
  expect(receipt.lease_until).toBeGreaterThan(Date.now());
  const logged=db.prepare("SELECT request_hash FROM coordinator_operations WHERE operation_id='http-resume-1'").get() as any;
  expect(logged.request_hash).toMatch(/^[0-9a-f]{64}$/);
  // A foreign generation header cannot resume the owner's authority.
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=? WHERE rig_id='xv'").run(live);
  const again=await (await app.request("/api/coordinator/xv",{headers:{Authorization:"Bearer test-token"}})).json() as any;
  const foreign=await call("/api/coordinator/resume-owned",{rigId:"xv",leaseMs:120000,operationId:"http-resume-2",expectedEpoch:again.authority.epoch,expectedObligationsDigest:again.obligationsDigest},{...native,"X-OpenRig-Occupant-Generation":"rotated-gen"});
  expect(foreign.status).toBe(409);
  expect((await foreign.json()).error).toBe("coordinator_generation_mismatch");
  // A missing identity header is refused at the boundary and never reaches authority.
  const anonymous=await call("/api/coordinator/resume-owned",{rigId:"xv",leaseMs:120000,operationId:"http-resume-3",expectedEpoch:again.authority.epoch,expectedObligationsDigest:again.obligationsDigest},{Authorization:"Bearer test-token"});
  expect(anonymous.status).toBe(403);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE operation_id LIKE 'http-resume-%'").get()).toEqual({n:1});
 });
 it("expired-window-recover requires Operator identity, real internal quiescence and explicit guard ON",async()=>{
  const op={"X-OpenRig-Session":"operator-agent@kernel","X-OpenRig-Occupant-Generation":"operator-agent-g1"};
  const auth={...op,Authorization:"Bearer test-token"};
  const shown=await (await app.request("/api/coordinator/xv",{headers:{Authorization:"Bearer test-token"}})).json() as any;
  // The body carries NO proof at all: the service reads its own refreshed native evidence.
  const body={rigId:"xv",operationId:"http-recover",windowMs:120000,expectedEpoch:shown.authority.epoch,expectedOwnerGeneration:"lead-g1",expectedCustodyDigest:shown.obligationsDigest,conflictOperationId:"never-recorded",conflictKind:"reconciliation-recover"};
  // A fabricated proof in the body is ignored as data and cannot substitute for native evidence.
  const forged={...body,quiescenceProof:{settled:true,observedAt:new Date().toISOString(),generation:"lead-g1",launch:"forged-launch",pid:999999}};
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=? WHERE rig_id='xv'").run(1);
  const unobserved=await call("/api/coordinator/expired-window-recover",forged,auth);
  expect(unobserved.status).toBe(409);
  expect((await unobserved.json()).error).toBe("coordinator_owner_unobserved");
  // An absent delivery guard refuses. This is intentional: the operator must enable it explicitly.
  repo.coordinatorAuthority.setRuntimeObserver(async session=>({session,generation:"lead-g1",state:"present",observedAt:Date.now(),fingerprint:"http-native",quiescence:{settled:true,observedAt:new Date().toISOString()}} as never));
  const noGuard=await call("/api/coordinator/expired-window-recover",body,auth);
  expect(noGuard.status).toBe(409);
  expect((await noGuard.json()).error).toBe("coordinator_guard_not_enabled");
  // Guard desired but not effective still refuses.
  db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES('lead@xv',1,0,'test','test','2026-01-01T00:00:00.000Z')").run();
  expect((await call("/api/coordinator/expired-window-recover",body,auth)).status).toBe(409);
  db.prepare("UPDATE seat_delivery_guards SET effective=1 WHERE node_id='lead@xv'").run();
  // Real internal evidence plus guard ON now reaches the incident fences: this fixture has no spent
  // recovery receipt, so it refuses on the ORIGINAL receipt requirement rather than on proof or guard.
  const reached=await call("/api/coordinator/expired-window-recover",body,auth);
  expect(reached.status).toBe(409);
  expect((await reached.json()).error).toBe("coordinator_reconciliation_unrecovered");
  // Quiescence must be genuinely positive: a null settled observation refuses.
  repo.coordinatorAuthority.setRuntimeObserver(async session=>({session,generation:"lead-g1",state:"present",observedAt:Date.now(),fingerprint:"http-native",quiescence:{settled:null,observedAt:null}} as never));
  expect((await call("/api/coordinator/expired-window-recover",{...body,operationId:"http-recover-2"},auth)).status).toBe(409);
  // The holder can never invoke this path itself, and no identity never reaches authority.
  expect((await call("/api/coordinator/expired-window-recover",body,{...caller,Authorization:"Bearer test-token"})).status).toBe(409);
  expect((await call("/api/coordinator/expired-window-recover",body,{Authorization:"Bearer test-token"})).status).toBe(403);
  expect(db.prepare("SELECT count(*) n FROM coordinator_operations WHERE operation_id LIKE 'http-recover%'").get()).toEqual({n:0});
 });
 it("registered native identity mapping does not assume logical-id equals session stem",async()=>{
  db.prepare("UPDATE nodes SET logical_id='orch1.lead' WHERE id='lead@xv'").run();
  const result=await call("/api/queue/create",{destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p"},nudge:false},caller);expect(result.status).toBe(201);
 });
});
