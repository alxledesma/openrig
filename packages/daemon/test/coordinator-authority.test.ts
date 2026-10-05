import { beforeEach,afterEach,describe,it,expect } from "vitest";
import { mkdtempSync,rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../src/db/all-migrations.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { CoordinatorAuthorityService, digest } from "../src/domain/coordinator-authority-service.js";
import type Database from "better-sqlite3";

import {assertCoordinatorRotationSafe} from "../src/domain/rotation-facts-resolver.js";
import {makeCoordinatorContinuityPolicy} from "../src/domain/policies/coordinator-continuity.js";
import { seed,token } from "./helpers/coordinator-fixture.js";
describe("coordinator exclusion and custody",()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinatorAuthorityService,clock:number;
 beforeEach(async()=>{
  dir=mkdtempSync(join(tmpdir(),"r07-"));db=createDb(join(dir,"db.sqlite"));seed(db);
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>svc.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  clock=10000;svc=new CoordinatorAuthorityService(db,bus,repo.transitionLog,()=>clock);
  await repo.create({qitemId:"baton",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"coordinate",nudge:false});
  svc.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:10000,operationId:"enable"});
  // Repository's production clock is real. Use a long real expiry for queue admission tests.
  db.prepare("UPDATE coordinator_authority SET lease_until=?").run(Date.now()+3600000);
  svc.acknowledge("lead@xv",token,{operationId:"ack",obligationsDigest:svc.reconciliationDigest("xv")});
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});});
 async function absent(state:"present"|"absent"|"unknown"="absent", age=0, generation?:string){
  svc.setRuntimeObserver(async session=>({session,generation:generation??svc.generation(session)!,state,observedAt:clock-age,fingerprint:"test-native-observation"}));
  await svc.refreshRuntimeAvailability("xv");
 }
 it("samples independent coordinators concurrently without weakening freshness",async()=>{
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();
  let release:()=>void=()=>{};const wait=new Promise<void>(resolve=>release=resolve);let calls=0;
  svc.setRuntimeObserver(async session=>{calls++;if(session==='lead@xv')await wait;return {session,generation:svc.generation(session)!,state:"absent",observedAt:clock,fingerprint:"probe"};});
  const refresh=svc.refreshRuntimeAvailability("xv");expect(calls).toBe(2);release();await refresh;
  expect(svc.observeContinuity("xv")).not.toBeNull();clock+=1001;expect(svc.observeContinuity("xv")).toBeNull();
 });
 it("delayed absence cannot overwrite a newer live observation",async()=>{
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();
  let release:()=>void=()=>{};let started:()=>void=()=>{};const begun=new Promise<void>(r=>started=r);const wait=new Promise<void>(r=>release=r);let calls=0;
  svc.setRuntimeObserver(async session=>{const first=++calls===1;if(first){started();await wait;}return {session,generation:svc.generation(session)!,state:first?"absent":"present",observedAt:clock,fingerprint:"probe"};});
  const old=svc.refreshRuntimeAvailability("xv");await begun;
  await svc.refreshRuntimeAvailability("xv");release();await old;
  expect(svc.observeContinuity("xv")).toBeNull();
 });
 it("runtime exclusion refuses active, unknown, stale and drifted-generation evidence",async()=>{
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();
  for(const state of ["present","unknown"] as const){await absent(state);expect(svc.observeContinuity("xv")).toBeNull();}
  await absent("absent",1001);expect(svc.observeContinuity("xv")).toBeNull();
  await absent("absent",0,"retired");expect(svc.observeContinuity("xv")).toBeNull();
  await absent();expect(svc.observeContinuity("xv")).not.toBeNull();
  expect(db.prepare("SELECT 1 FROM sessions WHERE session_name='lead@xv'").get()).toBeTruthy();
 });
 const contract=()=>({inputDigest:digest("inputs"),destination:"builder@xv",bodyHash:digest("build"),resources:["source/a"],returnContract:{destination:"lead@xv",evidenceRequired:["test-report"]}});
 const transfer=()=>svc.transfer("lead@xv","lead-g1",{expected:token,oldOwner:"lead@xv",recipient:"peer@xv",recipientGeneration:"peer-g1",leaseMs:10000,operationId:"transfer"});
 const create=()=>repo.create({qitemId:"work",sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p1"},nudge:false});
 it("discovers exact return evidence without writes and refuses body drift",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();
  const snapshot=()=>JSON.stringify(["coordinator_assignments","coordinator_resources","coordinator_authority","queue_items","coordinator_operations"].map(table=>db.prepare(`SELECT * FROM ${table}`).all()));
  const before=snapshot();const instructions=svc.returnInstructionsFor("work");
  expect(instructions?.returnDestination).toBe("lead@xv");
  expect(instructions?.payloadTemplate).toEqual({packageKey:"p1",inputDigest:digest("inputs"),evidence:[{kind:"test-report",ref:null}]});
  expect(instructions?.constitutesAcceptance).toBe(false);expect(snapshot()).toBe(before);
  expect(svc.returnInstructionsFor("missing")).toBeNull();
  db.prepare("UPDATE queue_items SET body='drift' WHERE qitem_id='work'").run();
  expect(svc.returnInstructionsFor("work")).toBeNull();
 });
 it("starts disabled for dispatch until exact acknowledgment",()=>{
  transfer();expect(()=>db.transaction(()=>svc.reserve("peer@xv","builder@xv","build","w",{token:{rigId:"xv",epoch:2,generation:"peer-g1"},packageKey:"p1"})).immediate()).toThrow("not reconciled");
 });
 it("atomically moves only canonical baton and records exclusion",()=>{
  transfer();expect(svc.get("xv")?.epoch).toBe(2);expect(repo.getById("baton")?.destinationSession).toBe("peer@xv");
  expect(repo.transitionLog.listForQitem("baton").at(-1)?.transitionNote).toContain("epoch 2");
 });
 it("retired create has zero row, event, transition and wake intent effects",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());transfer();
  const before=db.prepare("SELECT count(*) n FROM events").get();await expect(create()).rejects.toThrow("no longer holds");
  expect(repo.getById("work")).toBeNull();expect(db.prepare("SELECT count(*) n FROM events").get()).toEqual(before);
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual({n:0});
 });
 it("same package different IDs retries return sole row without another event",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());const first=await create();const before=db.prepare("SELECT count(*) n FROM events").get();
  const retry=await repo.create({qitemId:"different",sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p1"},nudge:false});
  expect(retry.qitemId).toBe(first.qitemId);expect(repo.getById("different")).toBeNull();expect(db.prepare("SELECT count(*) n FROM events").get()).toEqual(before);
 });
 it("new holder returns same committed assignment after reconcile; worker claim survives",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();repo.claim({qitemId:"work",destinationSession:"builder@xv"});repo.recordNudgeAttempt("work","failed:timeout outcome unknown");
  const original=repo.getById("work");transfer();
  const next={rigId:"xv",epoch:2,generation:"peer-g1"};svc.acknowledge("peer@xv",next,{operationId:"peer-ack",obligationsDigest:svc.reconciliationDigest("xv")});
  db.prepare("UPDATE coordinator_authority SET lease_until=?").run(Date.now()+3600000);
  const result=await repo.create({sourceSession:"peer@xv",destinationSession:"builder@xv",body:"build",dispatch:{token:next,packageKey:"p1"},nudge:false});
  expect(result).toEqual(original);expect(result.qitemId).toBe("work");expect(svc.obligations("xv")).toHaveLength(1);
 });
 it("same-address replacement cannot inherit previous generation capability",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());
  db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES ('new','lead@xv',2,'lead-g2','fresh')").run();
  await expect(create()).rejects.toThrow("retired, or unknown");expect(repo.getById("work")).toBeNull();
 });
 it("wrong epoch/generation and omitted envelope cannot bypass enabled worker boundaries",async()=>{
  for(const dispatch of [undefined,{token:{...token,epoch:0},packageKey:"p1"},{token:{...token,generation:"other"},packageKey:"p1"}])await expect(repo.create({sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch,nudge:false})).rejects.toThrow();
  await expect(repo.create({sourceSession:"unknown",destinationSession:"builder@xv",body:"build",nudge:false})).rejects.toThrow("admitted stage transition");
 });
 it("internal transactional scribe is fenced before its queue mutation",()=>{
  transfer();expect(()=>db.transaction(()=>repo.createWithinTransaction({sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p1"},nudge:false})).immediate()).toThrow("no longer holds");
 });
 it("both handoff forms are fenced with unchanged source history",async()=>{
  const src=await repo.create({sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"source",nudge:false});transfer();const before=repo.getById(src.qitemId);
  for(const verb of [repo.handoff.bind(repo),repo.handoffAndComplete.bind(repo)])await expect(verb({qitemId:src.qitemId,fromSession:"lead@xv",toSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p1"},nudge:false})).rejects.toThrow("no longer holds");
  expect(repo.getById(src.qitemId)).toEqual(before);
 });
 it("frozen resource stays reserved until exact terminal worker disposition",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p2",contract());await expect(repo.create({sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p2"},nudge:false})).rejects.toThrow("remains reserved");
  expect(()=>svc.dispose("builder@xv","builder-g1","xv","p1","return")).toThrow("terminal return");
  repo.claim({qitemId:"work",destinationSession:"builder@xv"});repo.update({qitemId:"work",actorSession:"builder@xv",state:"done",closureReason:"no-follow-on"});
  await expect(Promise.resolve().then(()=>svc.dispose("builder@xv","builder-g1","xv","p1","return"))).rejects.toThrow("required evidence");
  await repo.create({qitemId:"return",sourceSession:"builder@xv",destinationSession:"lead@xv",body:JSON.stringify({packageKey:"p1",inputDigest:digest("inputs"),evidence:[{kind:"test-report",ref:"durable/report"}]}),nudge:false});
  svc.dispose("builder@xv","builder-g1","xv","p1","return");svc.dispose("builder@xv","builder-g1","xv","p1","return");
  expect(()=>svc.dispose("builder@xv","builder-g1","xv","p1","different")).toThrow("already consumed");
  await expect(repo.create({sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p2"},nudge:false})).resolves.toBeTruthy();
 });
 it("admission refuses altered package and never trusts caller minted revisions",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());
  expect(()=>svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",{...contract(),inputDigest:"changed"})).toThrow("cannot change");
  await expect(create()).resolves.toBeTruthy();
  await expect(repo.create({sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"new-caller-revision"},nudge:false})).rejects.toThrow("cannot mint");
 });
 it("raw coordinator sends and missing source fail; informational communication and worker returns work",async()=>{
  expect(()=>svc.assertRawSend("lead@xv","builder@xv")).toThrow("Raw managed");expect(()=>svc.assertRawSend(undefined,"builder@xv")).toThrow();
  expect(()=>svc.assertRawSend("lead@xv","peer@xv")).not.toThrow();expect(()=>svc.assertRawSend("builder@xv","lead@xv")).not.toThrow();
  await expect(repo.create({sourceSession:"builder@xv",destinationSession:"lead@xv",body:"review result",nudge:false})).resolves.toBeTruthy();
 });
 it("other rigs keep legacy queue and raw send behavior",async()=>{
  await expect(repo.create({sourceSession:"someone@other",destinationSession:"worker@other",body:"legacy",nudge:false})).resolves.toBeTruthy();
  expect(()=>svc.assertRawSend(undefined,"worker@other")).not.toThrow();
 });
 it("scope cannot be bypassed with a host-qualified local target",()=>{
  expect(()=>svc.assertRawSend("lead@xv","builder@xv@remote")).toThrow();
 });
 it("expiry alone never elects or grants product dispatch",()=>{
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();expect(()=>db.transaction(()=>svc.reserve("lead@xv","builder@xv","build","q",{token,packageKey:"p1"})).immediate()).toThrow("expiry does not elect");
  expect(svc.get("xv")?.owner_session).toBe("lead@xv");expect(()=>svc.recordOutage("operator-agent@kernel","operator-agent-g1","xv","incident")).toThrow("fresh generation-bound native absence");
 });
 it("both-down recovery is deduplicated, leaves workers untouched and grants Operator no dispatch",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();const old=repo.getById("work");
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();await absent();
  const intake=svc.recordOutage("operator-agent@kernel","operator-agent-g1","xv","deregistered-both");expect(svc.recordOutage("operator-agent@kernel","operator-agent-g1","xv","same-incident")).toEqual(intake);
  expect(repo.getById(intake.queueId)?.destinationSession).toBe("operator-agent@kernel");expect(repo.getById("work")).toEqual(old);
  await expect(repo.create({sourceSession:"operator-agent@kernel",destinationSession:"builder@xv",body:"build",nudge:false})).rejects.toThrow();
 });
 it("isolated unavailable-owner recovery can transfer to the surviving peer only after exact Operator disposition",async()=>{
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();await absent();
  const recovery=svc.recordOutage("operator-agent@kernel","operator-agent-g1","xv","owner-deregistered");
  const change={expected:token,oldOwner:"lead@xv",recipient:"peer@xv",recipientGeneration:"peer-g1",leaseMs:10000,operationId:"operator-transfer",recoveryEvidenceId:recovery.queueId};
  expect(()=>svc.transfer("operator-agent@kernel","operator-agent-g1",change)).toThrow("completed Operator recovery evidence");
  repo.claim({qitemId:recovery.queueId,destinationSession:"operator-agent@kernel"});repo.update({qitemId:recovery.queueId,actorSession:"operator-agent@kernel",state:"done",closureReason:"no-follow-on"});
  const receipt=svc.transfer("operator-agent@kernel","operator-agent-g1",change);expect(receipt.epoch).toBe(2);expect(receipt.state).toBe("reconciling");expect(receipt.recovery_queue_id).toBeNull();
 });
 it("reconciliation cannot acknowledge changed claim or pending wake evidence",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();transfer();const observed=svc.reconciliationDigest("xv");repo.claim({qitemId:"work",destinationSession:"builder@xv"});
  expect(()=>svc.acknowledge("peer@xv",{rigId:"xv",epoch:2,generation:"peer-g1"},{operationId:"bad-ack",obligationsDigest:observed})).toThrow("Reconcile exact");expect(svc.get("xv")?.state).toBe("reconciling");
 });
 it("committed pending wake intent remains byte-identical across transfer and restart",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());
  await repo.create({qitemId:"pending-wake",sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p1"}});
  const intent=db.prepare("SELECT * FROM outbox_entries WHERE audit_pointer='pending-wake'").all();expect(intent).toHaveLength(1);
  transfer();const other=createDb(join(dir,"db.sqlite"));try{expect(other.prepare("SELECT * FROM outbox_entries WHERE audit_pointer='pending-wake'").all()).toEqual(intent);}finally{other.close();}
 });
 it("active authority refuses a missing or independently closed canonical baton",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());
  db.prepare("UPDATE queue_items SET state='done' WHERE qitem_id='baton'").run();
  await expect(create()).rejects.toThrow("exact canonical baton claim");expect(repo.getById("work")).toBeNull();
 });
 it("changed transfer contract cannot reuse an operation ID",()=>{
  transfer();expect(()=>svc.transfer("lead@xv","lead-g1",{expected:token,oldOwner:"lead@xv",recipient:"lead@xv",recipientGeneration:"lead-g1",leaseMs:10000,operationId:"transfer"})).toThrow("changed contract");
  expect(svc.get("xv")?.epoch).toBe(2);
 });
 it("holder renews its own lease and expired/retired tokens never revive",()=>{
  svc.renew("lead@xv",token,20000,"renew");expect(svc.get("xv")?.lease_until).toBe(clock+20000);
  transfer();expect(()=>svc.renew("lead@xv",token,20000,"another-renew")).toThrow("no longer holds");
 });
 it("automatic scope refuses an internal writer outside a transaction",()=>{
  expect(()=>svc.reserve("lead@xv","builder@xv","build","q",{token,packageKey:"p1"})).toThrow("requires its queue write transaction");
 });
 it("restart preserves active holder, epoch and sole assignment",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();transfer();
  const next=createDb(join(dir,"db.sqlite"));try{const reopened=new CoordinatorAuthorityService(next,undefined,undefined,()=>clock);expect(reopened.get("xv")?.epoch).toBe(2);expect(reopened.get("xv")?.state).toBe("reconciling");expect(reopened.obligations("xv")).toHaveLength(1);}finally{next.close();}
 });
 it("native observer requires expiry and authoritative exclusion, then dedups across restart",async()=>{
  expect(svc.observeContinuity("xv")).toBeNull();
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();
  expect(svc.observeContinuity("xv")).toBeNull(); // stale or unknown activity cannot exclude registered owner
  await absent();
  const observed=svc.observeContinuity("xv")!;
  expect(()=>svc.recordObservedOutage("xv",1,"observer",observed.evidenceId)).toThrow("explicitly register");
  db.prepare("INSERT INTO watchdog_jobs(job_id,policy,spec_yaml,target_session,interval_seconds,registered_by_session,registered_at,registered_by_generation_uuid) VALUES ('observer','coordinator-continuity','context: {}','operator-agent@kernel',60,'operator-agent@kernel','now','operator-agent-g1')").run();
  const first=svc.recordObservedOutage("xv",1,"observer",observed.evidenceId);
  const restarted=new CoordinatorAuthorityService(db,undefined,undefined,()=>clock);
  restarted.setRuntimeObserver(async session=>({session,generation:restarted.generation(session)!,state:"absent",observedAt:clock,fingerprint:"test-native-absence"}));await restarted.refreshRuntimeAvailability("xv");
  expect(restarted.recordObservedOutage("xv",1,"observer",observed.evidenceId)).toEqual(first);
  expect(repo.getById(first.queueId)?.sourceSession).toBe("watchdog@system");
  expect(repo.getById(first.queueId)?.destinationSession).toBe("operator-agent@kernel");
  expect(svc.get("xv")?.epoch).toBe(1); // observer never transfers authority
  expect(()=>svc.recordObservedOutage("xv",2,"observer",observed.evidenceId)).toThrow("changed since");
  db.prepare("UPDATE watchdog_jobs SET registered_by_generation_uuid='old'").run();
  expect(()=>svc.recordObservedOutage("xv",1,"observer",observed.evidenceId)).toThrow();
 });

 it("opt-in policy is quiet on unknown/live observations and emits typed recovery only",async()=>{
  const policy=makeCoordinatorContinuityPolicy(svc);
  const job={registeredBySession:"operator-agent@kernel",target:{session:"operator-agent@kernel"},context:{rigId:"xv"}} as any;
  expect((await policy.evaluate(job)).action).toBe("skip");
  db.prepare("UPDATE coordinator_authority SET lease_until=1").run();
  expect((await policy.evaluate(job)).action).toBe("skip");
  await absent();
  const outcome=await policy.evaluate(job);
  expect(outcome.action).toBe("send");
  if(outcome.action==="send")expect(outcome.coordinatorRecovery).toEqual(svc.observeContinuity("xv"));
  expect((await policy.evaluate({...job,registeredBySession:"lead@xv"})).action).toBe("skip");
  expect(svc.get("xv")?.state).toBe("active");
 });

 it("automatic rotation cannot orphan holder but former holder is eligible after transfer",()=>{
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date().toISOString(),new Date().toISOString());
  expect(()=>assertCoordinatorRotationSafe(db,"lead@xv")).toThrow("transfer and reconcile");
  expect(()=>assertCoordinatorRotationSafe(db,"builder@xv")).not.toThrow();
  transfer();
  expect(()=>assertCoordinatorRotationSafe(db,"peer@xv")).toThrow();
  expect(()=>assertCoordinatorRotationSafe(db,"lead@xv")).not.toThrow();
  db.prepare("UPDATE coordinator_authority SET owner_session='peer@xv@fixture-host' WHERE rig_id='xv'").run();
  expect(()=>assertCoordinatorRotationSafe(db,"peer@xv")).toThrow("transfer and reconcile");
 });


 it("actual builder review, reviewer fixback and Architect question flows remain authorized",async()=>{
  const c={...contract(),returnContract:{...contract().returnContract,transitions:[{source:"builder@xv",destination:"reviewer@xv",bodyHash:digest("review")},{source:"reviewer@xv",destination:"builder@xv",bodyHash:digest("fix")},{source:"builder@xv",destination:"architect@xv",bodyHash:digest("question")}]}};
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",c);await create();repo.claim({qitemId:"work",destinationSession:"builder@xv"});
  const env={token:{rigId:"xv",epoch:1,generation:"builder-g1"},packageKey:"p1"};
  expect(()=>svc.assertRawSend("builder@xv","reviewer@xv")).not.toThrow();
  expect(()=>svc.assertRawSend("reviewer@xv","builder@xv")).not.toThrow();
  expect(()=>svc.assertRawSend("builder@xv","architect@xv")).not.toThrow();
  const review=await repo.handoff({qitemId:"work",fromSession:"builder@xv",toSession:"reviewer@xv",body:"review",dispatch:env,nudge:false});
  repo.claim({qitemId:review.created.qitemId,destinationSession:"reviewer@xv"});
  const fix=await repo.handoff({qitemId:review.created.qitemId,fromSession:"reviewer@xv",toSession:"builder@xv",body:"fix",dispatch:{...env,token:{...env.token,generation:"reviewer-g1"}},nudge:false});
  repo.claim({qitemId:fix.created.qitemId,destinationSession:"builder@xv"});
  const question=await repo.create({qitemId:"question",sourceSession:"builder@xv",destinationSession:"architect@xv",body:"question",dispatch:env,nudge:false});
  expect(question.destinationSession).toBe("architect@xv");
  await expect(repo.create({sourceSession:"builder@xv",destinationSession:"architect@xv",body:"other",dispatch:env,nudge:false})).rejects.toThrow("frozen return");
  await expect(repo.create({sourceSession:"builder@xv",destinationSession:"architect@xv",body:"question",dispatch:{...env,token:{...env.token,generation:"retired"}},nudge:false})).rejects.toThrow("retired");
  expect(()=>svc.assertManagedSend("builder@xv","architect@xv","question")).not.toThrow();
  // Reusing the admitted stage from a different source queue must not fabricate closure.
  await expect(repo.handoff({qitemId:fix.created.qitemId,fromSession:"builder@xv",toSession:"reviewer@xv",body:"review",dispatch:env,nudge:false})).rejects.toThrow("Replay source differs");
  expect(repo.getById(fix.created.qitemId)?.state).toBe("in-progress");
 });
 it("initial enable refuses unreconciled legacy obligations",async()=>{
  await repo.create({qitemId:"other-baton",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"legacy",nudge:false});
  db.prepare("DELETE FROM coordinator_operations").run();db.prepare("DELETE FROM coordinator_authority").run();
  expect(()=>svc.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:10000,operationId:"again"})).toThrow("Nonempty rig");
 });

 it("enable refuses uncertain legacy effects even with no other queue obligation",()=>{
  db.prepare("DELETE FROM coordinator_operations").run();db.prepare("DELETE FROM coordinator_authority").run();
  db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES ('uncertain','lead@xv','builder@xv','legacy','now','pending')").run();
  expect(()=>svc.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:10000,operationId:"again"})).toThrow("Uncertain delivery");
  expect(svc.get("xv")).toBeUndefined();
 });
 it("reconciliation digest changes when reservations or stage custody changes",async()=>{
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",contract());await create();
  const before=svc.reconciliationDigest("xv");
  db.prepare("INSERT INTO coordinator_resources VALUES ('xv','new-resource','p1')").run();
  expect(svc.reconciliationDigest("xv")).not.toBe(before);
 });

 it("full builder review fixback lifecycle releases reservations only after attributed complete return",async()=>{
  const c={...contract(),returnContract:{...contract().returnContract,transitions:[{source:"builder@xv",destination:"reviewer@xv",bodyHash:digest("review")},{source:"reviewer@xv",destination:"builder@xv",bodyHash:digest("fix")} ]}};
  svc.admit("operator-agent@kernel","operator-agent-g1","xv","p1",c);await create();repo.claim({qitemId:"work",destinationSession:"builder@xv"});
  const env={token:{rigId:"xv",epoch:1,generation:"builder-g1"},packageKey:"p1"};
  const review=await repo.handoff({qitemId:"work",fromSession:"builder@xv",toSession:"reviewer@xv",body:"review",dispatch:env,nudge:false});repo.claim({qitemId:review.created.qitemId,destinationSession:"reviewer@xv"});
  const fix=await repo.handoff({qitemId:review.created.qitemId,fromSession:"reviewer@xv",toSession:"builder@xv",body:"fix",dispatch:{...env,token:{...env.token,generation:"reviewer-g1"}},nudge:false});repo.claim({qitemId:fix.created.qitemId,destinationSession:"builder@xv"});
  await repo.create({qitemId:"full-return",sourceSession:"builder@xv",destinationSession:"lead@xv",body:JSON.stringify({packageKey:"p1",inputDigest:digest("inputs"),evidence:[{kind:"test-report",ref:"report"}]}),nudge:false});
  expect(()=>svc.dispose("builder@xv","builder-g1","xv","p1","full-return")).toThrow("Open stage custody");
  repo.update({qitemId:fix.created.qitemId,actorSession:"builder@xv",state:"done",closureReason:"no-follow-on"});
  expect(repo.getById(review.created.qitemId)?.state).toBe("handed-off");
  svc.dispose("builder@xv","builder-g1","xv","p1","full-return");
  expect(db.prepare("SELECT count(*) n FROM coordinator_resources WHERE package_key='p1'").get()).toEqual({n:0});
  expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE package_key='p1'").get()).toEqual({disposition_id:"full-return"});
 });

});
