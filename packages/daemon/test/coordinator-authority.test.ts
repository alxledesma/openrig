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
 it("expired active recovery is bounded native Operator administration and preserves custody",async()=>{
  db.prepare('UPDATE coordinator_authority SET lease_until=1').run();await absent('present');
  const before=db.prepare('SELECT * FROM queue_items').all(),effects=db.prepare('SELECT * FROM outbox_entries').all();
  const input={token,expectedLeaseUntil:1,operationId:'expired-active-r1',obligationsDigest:svc.reconciliationDigest('xv'),windowMs:10000};
  expect(()=>svc.renew('lead@xv',token,10000,'invalid-renew')).toThrow('Lease expiry');
  const recovered=svc.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',input);
  expect(recovered).toMatchObject({state:'reconciling',epoch:1,owner_session:'lead@xv',owner_generation:'lead-g1'});
  expect(db.prepare('SELECT * FROM queue_items').all()).toEqual(before);expect(db.prepare('SELECT * FROM outbox_entries').all()).toEqual(effects);
  expect(svc.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',input)).toEqual(recovered);
  expect(()=>svc.recoverExpiredActive('operator-agent@kernel','operator-agent-g1',{...input,operationId:'chain'})).toThrow('Exact expired active');
  svc.acknowledge('lead@xv',token,{operationId:'recovery-ack',obligationsDigest:svc.reconciliationDigest('xv')});expect(svc.get('xv')?.state).toBe('active');
 });
 it.each(['absent','unknown','stale','retired-generation','changed-digest','changed-lease','wrong-baton','wrong-actor','live-lease'] as const)("active expiry recovery refuses %s without changing authority",async(reason)=>{
  db.prepare('UPDATE coordinator_authority SET lease_until=1').run();await absent('present');
  const input={token,expectedLeaseUntil:1,operationId:'refused-'+reason,obligationsDigest:svc.reconciliationDigest('xv'),windowMs:10000};
  if(reason==='absent'||reason==='unknown')await absent(reason);
  if(reason==='stale')clock+=1001;
  if(reason==='retired-generation')await absent('present',0,'retired');
  if(reason==='changed-digest')input.obligationsDigest='changed';
  if(reason==='changed-lease')input.expectedLeaseUntil=2;
  if(reason==='wrong-baton'){db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='wrong' WHERE qitem_id='baton'").run();input.obligationsDigest=svc.reconciliationDigest('xv');}
  if(reason==='live-lease'){db.prepare('UPDATE coordinator_authority SET lease_until=?').run(clock+10000);input.expectedLeaseUntil=clock+10000;}
  const before=svc.get('xv');
  expect(()=>svc.recoverExpiredActive(reason==='wrong-actor'?'lead@xv':'operator-agent@kernel',reason==='wrong-actor'?'lead-g1':'operator-agent-g1',input)).toThrow();
  expect(svc.get('xv')).toEqual(before);
 });
 it("expired owner cannot manufacture authority through self-transfer",()=>{
  db.prepare('UPDATE coordinator_authority SET lease_until=1').run();
  expect(()=>svc.transfer('lead@xv','lead-g1',{expected:token,oldOwner:'lead@xv',recipient:'lead@xv',recipientGeneration:'lead-g1',operationId:'loophole',leaseMs:10000})).toThrow('Lease expiry');
  expect(svc.get('xv')?.epoch).toBe(1);
 });
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

describe("coordinator resume-owned shared native owner continuation",()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinatorAuthorityService,clock:number;
 beforeEach(async()=>{
  dir=mkdtempSync(join(tmpdir(),"resume-owned-"));db=createDb(join(dir,"db.sqlite"));seed(db);
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>svc.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  clock=10000;svc=new CoordinatorAuthorityService(db,bus,repo.transitionLog,()=>clock);
  await repo.create({qitemId:"baton",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"coordinate",nudge:false});
  svc.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:10000,operationId:"enable"});
  db.prepare("UPDATE coordinator_authority SET lease_until=?").run(Date.now()+3600000);
  svc.acknowledge("lead@xv",token,{operationId:"ack",obligationsDigest:svc.reconciliationDigest("xv")});
  // Reconciling but LIVE: the owner took custody and has not yet acknowledged.
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=?,operation_id='custody' WHERE rig_id='xv'").run(clock+5000);
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});});
 const transitions=()=>db.prepare("SELECT count(*) n FROM queue_transitions WHERE qitem_id='baton'").get() as any;
 const ops=()=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='resume-owned'").get() as any;

 it("acknowledges a live reconciling owner and renews atomically",()=>{
  expect(svc.get("xv")!.state).toBe("reconciling");
  const baselineTransitions=transitions().n;
  const out=svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:120000,operationId:"resume-1",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  expect(out.state).toBe("active");
  expect(out.lease_until).toBe(clock+120000);
  expect(out.operation_id).toBe("resume-1");
  // The baton is claimed by this generation and exactly one transition was appended.
  const baton=db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id='baton'").get() as any;
  expect(baton).toEqual({state:"in-progress",claimed_by_generation_uuid:"lead-g1"});
  expect(transitions().n).toBe(baselineTransitions+1);
  expect(ops().n).toBe(1);
 });

 it("renews an already active owner with equivalent fences",()=>{
  // Re-acknowledge so this case starts from a genuinely ACTIVE owner.
  db.prepare("UPDATE coordinator_authority SET state='active' WHERE rig_id='xv'").run();
  expect(svc.get("xv")!.state).toBe("active");
  const before=transitions().n;
  const out=svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-active",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  expect(out.state).toBe("active");
  expect(out.lease_until).toBe(clock+60000);
  // No re-acknowledgment: no duplicate queue event.
  expect(transitions().n).toBe(before);
  expect(ops().n).toBe(1);
 });

 it("refuses a foreign caller and a foreign generation",()=>{
  expect(()=>svc.resumeOwned("peer@xv","peer-g1",{rigId:"xv",leaseMs:60000,operationId:"foreign-actor"})).toThrow(/Only the genuine recorded owner/);
  expect(()=>svc.resumeOwned("lead@xv","rotated-generation",{rigId:"xv",leaseMs:60000,operationId:"foreign-gen"})).toThrow(/caller generation differs/);
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(ops().n).toBe(0);
 });

 it("never recovers an expired lease",()=>{
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock-1);
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-expired",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/expired lease is recovered only by the explicit expiry-recovery path/);
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(ops().n).toBe(0);
 });
 it("refuses a stale expected obligations digest instead of accepting its own fresh read",async()=>{
  const expected=svc.reconciliationDigest("xv");
  // A stale read contract refuses; the server no longer blesses whatever it observes itself.
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"stale-digest",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:"0".repeat(64)})).toThrow(/Obligations changed since the expected read/);
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(ops().n).toBe(0);
  // An obligation moves after the caller read it: the same stale contract now refuses.
  await repo.create({qitemId:"stale-obligation",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"{}",nudge:false});
  expect(svc.reconciliationDigest("xv")).not.toBe(expected);
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"stale-digest-2",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:expected})).toThrow(/Obligations changed since the expected read/);
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(ops().n).toBe(0);
  // The freshly derived contract from the same moment succeeds.
  expect(svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"fresh-digest",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")}).state).toBe("active");
  expect(ops().n).toBe(1);
 });

 it("acknowledges a genuinely pending, unclaimed (null) baton but refuses a foreign claim",()=>{
  // A freshly transferred or enabled owner may hold a pending baton nobody has claimed yet.
  db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL WHERE qitem_id='baton'").run();
  const out=svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"null-baton-ok",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  expect(out.state).toBe("active");
  expect(db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id='baton'").get()).toEqual({state:"in-progress",claimed_by_generation_uuid:"lead-g1"});
  expect(ops().n).toBe(1);
  // A pending baton already claimed by ANOTHER generation still refuses.
  db.prepare("UPDATE coordinator_authority SET state='reconciling' WHERE rig_id='xv'").run();
  db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid='other-gen' WHERE qitem_id='baton'").run();
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"null-baton-foreign",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/exact canonical baton/);
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(ops().n).toBe(1);
  // The active branch stays strict: in-progress and claimed by the current generation only.
  db.prepare("UPDATE coordinator_authority SET state='active' WHERE rig_id='xv'").run();
  db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL WHERE qitem_id='baton'").run();
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"active-null-baton",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/exact canonical baton claim/);
  expect(ops().n).toBe(1);
 });

 it("refuses a changed expected epoch or digest under the same operation id",()=>{
  const before=ops().n;
  svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"payload-guard",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  expect(ops().n).toBe(before+1);
  // Both expected fields are immutable submitted input, so changing either under the same id is a
  // conflicting payload rather than a silent receipt.
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"payload-guard",expectedEpoch:svc.get("xv")!.epoch+3,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/Operation ID reused/);
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"payload-guard",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:"0".repeat(64)})).toThrow(/Operation ID reused/);
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:99000,operationId:"payload-guard",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/Operation ID reused/);
  expect(ops().n).toBe(before+1);
 });

 it("refuses a stale expected epoch",()=>{
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"stale-epoch",expectedEpoch:svc.get("xv")!.epoch+7,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/epoch advanced since the expected read/);
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(ops().n).toBe(0);
 });

 it("replays exactly after a real pending-baton acknowledgment and an unrelated obligation change",async()=>{
  // This is the reconciliation case the review named: the acknowledgment itself changes baton
  // fields inside obligations, so a replay hash that included live derived state would self-conflict.
  expect(db.prepare("SELECT state FROM coordinator_authority WHERE rig_id='xv'").get()).toEqual({state:"reconciling"});
  // A genuinely PENDING baton, so the acknowledgment really does claim it and moves the fields
  // that obligations hash over.
  db.prepare("UPDATE queue_items SET state='pending',claimed_by_generation_uuid=NULL WHERE qitem_id='baton'").run();
  const digestAtRead=svc.reconciliationDigest("xv");
  // The prepared contract: exactly what a timed-out call would resubmit via --replay-contract.
  const prepared={rigId:"xv",leaseMs:120000,operationId:"replay-after-ack",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:digestAtRead};
  const first=svc.resumeOwned("lead@xv","lead-g1",prepared);
  const afterFirst=transitions().n;
  // Obligations genuinely moved: the baton is now in-progress and claimed.
  expect(svc.reconciliationDigest("xv")).not.toBe(digestAtRead);
  expect(transitions().n).toBe(afterFirst);
  // An unrelated new obligation must not break replay either.
  await repo.create({qitemId:"replay-unrelated",sourceSession:"operator-agent@kernel",destinationSession:"peer@xv",body:"{}",nudge:false});
  // Replay resubmits the ORIGINAL prepared contract, never a fresh read.
  const again=svc.resumeOwned("lead@xv","lead-g1",prepared);
  expect(again).toEqual(first);
  expect(transitions().n).toBe(afterFirst);
  expect(ops().n).toBe(1);
  // And it stays replayable even after the renewed lease lapses.
  clock+=999999;
  expect(svc.resumeOwned("lead@xv","lead-g1",prepared)).toEqual(first);
  expect(transitions().n).toBe(afterFirst);
  expect(ops().n).toBe(1);
 });

 it("refuses a blank, oversized or non-string operation id before any mutation",()=>{
  const beforeTransitions=transitions().n,beforeBaton=db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id='baton'").get();
  for(const [why,operationId] of [["blank",""],["whitespace","   "],["oversized","x".repeat(161)],["non-string",123],["missing",undefined]] as const){
   expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId,expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")} as any)).toThrow(/Bounded attributed operation ID required/);
  }
  expect(svc.get("xv")!.state).toBe("reconciling");
  expect(transitions().n).toBe(beforeTransitions);
  expect(ops().n).toBe(0);
  expect(db.prepare("SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id='baton'").get()).toEqual(beforeBaton);
 });

 it("refuses an unknown or retired authority state instead of implicitly activating it",()=>{
  const baseline=transitions().n;
  for(const state of ["recovery"]){
   db.prepare("UPDATE coordinator_authority SET state=? WHERE rig_id='xv'").run(state);
   expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"state-"+state,expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/Authority state recovery cannot be resumed/);
   expect(svc.get("xv")!.state).toBe(state);
  }
  expect(ops().n).toBe(0);
  expect(transitions().n).toBe(baseline);
 });


 it("refuses a conflicting payload under the same operation id",()=>{
  svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:120000,operationId:"resume-conflict",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-conflict",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/Operation ID reused/);
  expect(ops().n).toBe(1);
 });

 it("refuses an active owner whose canonical baton is missing or wrongly claimed",async()=>{
  db.prepare("UPDATE coordinator_authority SET state='active' WHERE rig_id='xv'").run();
  for(const [why,patch] of [
   ["pending",()=>db.prepare("UPDATE queue_items SET state='pending' WHERE qitem_id='baton'").run()],
   ["unclaimed",()=>db.prepare("UPDATE queue_items SET claimed_by_generation_uuid=NULL WHERE qitem_id='baton'").run()],
   ["foreign generation",()=>db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='other-gen' WHERE qitem_id='baton'").run()],
   ["wrong destination",()=>db.prepare("UPDATE coordinator_authority SET baton_id='other-baton' WHERE rig_id='xv'").run()]
  ]){
   // Restore a valid baton before each case (the delete case is now a wrong-destination one).
   db.prepare("UPDATE coordinator_authority SET baton_id='baton' WHERE rig_id='xv'").run();
   db.prepare("UPDATE queue_items SET state='in-progress',claimed_by_generation_uuid='lead-g1',destination_session='lead@xv' WHERE qitem_id='baton'").run();
   if(why==="wrong destination")await repo.create({qitemId:"other-baton",sourceSession:"operator-agent@kernel",destinationSession:"peer@xv",body:"coordinate",nudge:false});
   patch();
   const before=db.prepare("SELECT lease_until,operation_id FROM coordinator_authority WHERE rig_id='xv'").get();
   expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"baton-"+why,expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")})).toThrow(/exact canonical baton claim/);
   // Refusal renews nothing and logs no receipt.
   expect(db.prepare("SELECT lease_until,operation_id FROM coordinator_authority WHERE rig_id='xv'").get()).toEqual(before);
   expect(ops().n).toBe(0);
  }
  // The valid baton still renews, proving the fence is the claim and not the state alone.
  db.prepare("UPDATE coordinator_authority SET baton_id='baton' WHERE rig_id='xv'").run();
  db.prepare("UPDATE queue_items SET state='in-progress',claimed_by_generation_uuid='lead-g1',destination_session='lead@xv' WHERE qitem_id='baton'").run();
  expect(svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"baton-valid",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")}).state).toBe("active");
  expect(ops().n).toBe(1);
 });

 it("binds the current epoch from the expected contract, not from the body",()=>{
  // A forged epoch alongside the contract is ignored: expectedEpoch is the only epoch input.
  const forged={rigId:"xv",leaseMs:60000,operationId:"epoch-1",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv"),epoch:9999};
  const out=svc.resumeOwned("lead@xv","lead-g1",forged as any);
  expect(out.epoch).toBe(svc.get("xv")!.epoch);
  expect(out.epoch).not.toBe(9999);
  // After a rotation the receipt carries the new epoch, read fresh.
  db.prepare("UPDATE coordinator_authority SET epoch=epoch+1 WHERE rig_id='xv'").run();
  const rotated=svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"epoch-2",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  expect(rotated.epoch).toBe(out.epoch+1);
  expect(rotated.lease_until).toBe(clock+60000);
 });

 it("leaves protected claims, resources and uncertain effects untouched",()=>{
  repo.create({qitemId:"kept-claim",sourceSession:"operator-agent@kernel",destinationSession:"peer@xv",body:"{}",nudge:false});
  db.prepare("DELETE FROM coordinator_resources WHERE rig_id='xv'").run();
  db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('w-unknown','watchdog@system','peer@xv','live',?,'indeterminate')").run(new Date().toISOString());
  const snapshot=()=>({
   assignments:db.prepare("SELECT * FROM coordinator_assignments WHERE rig_id='xv'").all(),
   claims:db.prepare("SELECT qitem_id,state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id='kept-claim'").all(),
   resources:db.prepare("SELECT * FROM coordinator_resources WHERE rig_id='xv'").all(),
   unknown:db.prepare("SELECT outbox_id,delivery_state FROM outbox_entries WHERE delivery_state='indeterminate'").all()
  });
  const before=snapshot();
  svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-protected",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")});
  const after=snapshot();
  expect(after.assignments).toEqual(before.assignments);
  expect(after.claims).toEqual(before.claims);
  expect(after.resources).toEqual(before.resources);
  // The UNKNOWN wake is byte-preserved: never retried, released or relabelled.
  expect(after.unknown).toEqual(before.unknown);
 });
});

describe("expired reconciling window bounded successor recovery",()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinatorAuthorityService,clock:number;
 const lease=10000;
 beforeEach(async()=>{
  dir=mkdtempSync(join(tmpdir(),"expired-window-"));db=createDb(join(dir,"db.sqlite"));seed(db);
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>svc.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  clock=10000;svc=new CoordinatorAuthorityService(db,bus,repo.transitionLog,()=>clock);
  await repo.create({qitemId:"baton",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"coordinate",nudge:false});
  svc.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:lease,operationId:"enable"});
  db.prepare("UPDATE coordinator_authority SET lease_until=?").run(Date.now()+3600000);
  svc.acknowledge("lead@xv",token,{operationId:"ack",obligationsDigest:svc.reconciliationDigest("xv")});
  // Spend the single reconciliation-recover, then let that window expire.
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=? WHERE rig_id='xv'").run(clock-1);
  svc.recoverReconciliation("operator-agent@kernel","operator-agent-g1",{token:{rigId:"xv",epoch:1,generation:"lead-g1"},operationId:"spent-recovery",obligationsDigest:svc.reconciliationDigest("xv"),windowMs:60000});
  // That one reconciliation window is now spent and has itself expired.
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock-1);
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});});
 const ops=(kind:string)=>db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind=?").get(kind) as any;
 const transitions=()=>db.prepare("SELECT count(*) n FROM queue_transitions WHERE qitem_id='baton'").get() as any;
 const custody=()=>svc.reconciliationDigest("xv");
 // Real internal native evidence: the service reads its own refreshed observation, never a body field.
 const observe=(state:"present"|"absent"|"unknown"="present",quiescence:{settled:boolean|null;observedAt:string|null}|null={settled:true,observedAt:new Date(clock-50).toISOString()},generation="lead-g1")=>{
  svc.setRuntimeObserver(async session=>({session,generation,state,observedAt:clock-1,fingerprint:"test-native-observation",quiescence} as never));
 };
 const guard=(desired:number,effective:number)=>db.prepare("INSERT INTO seat_delivery_guards(node_id,desired,effective,actor,reason,changed_at) VALUES('lead@xv',?,?,'test','test','2026-01-01T00:00:00.000Z') ON CONFLICT(node_id) DO UPDATE SET desired=excluded.desired,effective=excluded.effective").run(desired,effective);
 const guardOn=()=>guard(1,1);
 // A durable, different-kind reservation of the same operation ID: real backend no-effect evidence.
 // The incident's own spent recovery receipt IS the durable evidence: coordinator_operations is keyed
 // by (rig, operation id), so the holder's acknowledge under that same id is what the backend refused.
 const reserveConflict=async()=>{
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=? WHERE rig_id='xv'").run(clock-1);
 };
 const ready=()=>{observe();guardOn();};
 // The route refreshes availability before the mutation; the test does the same.
 const call=async(over:Record<string,unknown>={})=>{await svc.refreshRuntimeAvailability("xv");return svc.recoverExpiredReconciling("operator-agent@kernel","operator-agent-g1",{
  rigId:"xv",operationId:"successor-1",windowMs:120000,
  expectedEpoch:1,expectedOwnerGeneration:"lead-g1",expectedCustodyDigest:custody(),
  conflictOperationId:"spent-recovery",conflictKind:"reconciliation-recover",
  ...over});};

 it("grants exactly one bounded successor window at epoch plus one",async()=>{
  await reserveConflict();ready();
  const before=transitions().n;
  const out=await call();
  expect(out.incidentAnchor).toBe("xv#1#lead-g1");
  expect(out.epoch).toBe(2);
  expect(out.leaseUntil).toBe(clock+120000);
  // Reconciling only: never active, and the original operation and baton are preserved.
  const row=svc.get("xv")!;
  expect(row.state).toBe("reconciling");
  expect(row.baton_id).toBe("baton");
  expect(row.owner_session).toBe("lead@xv");
  expect(row.owner_generation).toBe("lead-g1");
  expect(row.lease_until).toBe(clock+120000);
  // No queue mutation, no dispatch, no admission, no qualification.
  expect(transitions().n).toBe(before);
  expect(db.prepare("SELECT count(*) n FROM coordinator_packages WHERE rig_id='xv'").get()).toEqual({n:0});
  expect(ops("expired-window-successor").n).toBe(1);
 });

 it("refuses a second successor for the same incident across epochs",async()=>{
  await reserveConflict();ready();
  await call();
  // The new window expires too, and a second attempt at the same incident must refuse.
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock-1);
  await expect(call({operationId:"successor-2",expectedEpoch:svc.get("xv")!.epoch})).rejects.toThrow(/already consumed its single bounded successor window/);
  expect(ops("expired-window-successor").n).toBe(1);
 });

 it("closes the incident once the holder genuinely acknowledges",async()=>{
  await reserveConflict();ready();
  await call();
  // The holder genuinely acknowledges and resumes the successor window, which ends the incident.
  expect(svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-ends-incident",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:svc.reconciliationDigest("xv")}).state).toBe("active");
  db.prepare("UPDATE coordinator_authority SET state='reconciling',lease_until=? WHERE rig_id='xv'").run(clock-1);
  await expect(call({operationId:"successor-after-ack",expectedEpoch:svc.get("xv")!.epoch})).rejects.toThrow(/already acknowledged this incident/);
  expect(ops("expired-window-successor").n).toBe(1);
 });

 it("refuses missing, absent, null, false and foreign-generation internal quiescence evidence",async()=>{
  await reserveConflict();ready();
  for(const [why,setter] of [
   ["no observation",()=>svc.setRuntimeObserver(async()=>null)],
   ["absent occupant",()=>observe("absent",{settled:true,observedAt:new Date(clock-50).toISOString()})],
   ["unknown occupant",()=>observe("unknown",{settled:true,observedAt:new Date(clock-50).toISOString()})],
   ["absent quiescence",()=>observe("present",null)],
   ["null settled",()=>observe("present",{settled:null,observedAt:new Date(clock-50).toISOString()})],
   ["false settled",()=>observe("present",{settled:false,observedAt:new Date(clock-50).toISOString()})],
   ["null observedAt",()=>observe("present",{settled:true,observedAt:null})],
   ["malformed observedAt",()=>observe("present",{settled:true,observedAt:"not-a-time"})],
   ["future observedAt",()=>observe("present",{settled:true,observedAt:new Date(clock+5000).toISOString()})],
   ["foreign generation",()=>observe("present",{settled:true,observedAt:new Date(clock-50).toISOString()},"other-gen")],
   ["stale observation",()=>{observe();svc.setRuntimeObserver(async session=>({session,generation:"lead-g1",state:"present",observedAt:clock-5000,fingerprint:"test-native-observation",quiescence:{settled:true,observedAt:new Date(clock-50).toISOString()}} as never));}]
  ] as const){
   setter();await svc.refreshRuntimeAvailability("xv");
   await expect(call({operationId:"proof-"+why.replace(/\s/g,"-")})).rejects.toThrow();
   expect(svc.get("xv")!.epoch).toBe(1);
   expect(ops("expired-window-successor").n).toBe(0);
  }
  // Real, current, positive internal evidence passes.
  ready();await svc.refreshRuntimeAvailability("xv");
  expect((await call()).epoch).toBe(2);
 });

 it("refuses a caller-authored proof, since the contract carries no proof field",async()=>{
  await reserveConflict();
  // With NO internal native evidence at all, a fabricated launch/pid in the body cannot rescue the call.
  await expect(call({operationId:"forged-proof",quiescenceProof:{settled:true,observedAt:new Date(clock-50).toISOString(),generation:"lead-g1",launch:"forged-launch",pid:999999}} as never)).rejects.toThrow(/fresh current native observation/);
  expect(ops("expired-window-successor").n).toBe(0);
  // A body proof field is ignored as data once real internal evidence exists.
  ready();expect((await call({quiescenceProof:{settled:false,observedAt:null,generation:"attacker",launch:"forged",pid:1}} as never)).epoch).toBe(2);
 });

 it("refuses unless the owner node's delivery guard is desired AND effective",async()=>{
  await reserveConflict();observe();await svc.refreshRuntimeAvailability("xv");
  await expect(call({operationId:"no-guard"})).rejects.toThrow(/guard must be desired AND effective/);
  guard(1,0);
  await expect(call({operationId:"guard-effective-off"})).rejects.toThrow(/guard must be desired AND effective/);
  guard(0,1);
  await expect(call({operationId:"guard-desired-off"})).rejects.toThrow(/guard must be desired AND effective/);
  expect(ops("expired-window-successor").n).toBe(0);
  guardOn();expect((await call({operationId:"guard-on"})).epoch).toBe(2);
 });

 it("refuses a live reservation bound to the owner node or session",async()=>{
  await reserveConflict();ready();
  db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES('r1','op1','lead@xv','lead@xv','lead-g1','n1','operator-agent@kernel','operator-agent-g1','h','{}','{}','reserved','2026-01-01T00:00:00.000Z','2026-01-01T00:00:00.000Z')").run();
  await expect(call({operationId:"node-reservation"})).rejects.toThrow(/outstanding dispatch reservation/);
  // The same reservation registered against the node while naming another session is still caught.
  db.prepare("UPDATE seat_dispatch_reservations SET session_name='someone-else'").run();
  await expect(call({operationId:"node-reservation-2"})).rejects.toThrow(/outstanding dispatch reservation/);
  expect(ops("expired-window-successor").n).toBe(0);
  db.prepare("DELETE FROM seat_dispatch_reservations").run();
  expect((await call({operationId:"reservation-clear"})).epoch).toBe(2);
 });

 it("refuses only an actually in-flight send, and preserves pending and UNKNOWN rows",async()=>{
  await reserveConflict();ready();
  const stamp=new Date().toISOString();
  const custodyBefore=svc.reconciliationDigest("xv");
  for(const [why,state] of [["sending","sending"],["retained","retained"],["indeterminate","indeterminate"]] as const){
   db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES(?,?,?,?,?,?)").run(`w-${state}`,"lead@xv","builder@xv","{}",stamp,state);
  }
  // An ACTUALLY in-flight send, exactly as OutboxHandler.beginSend writes it, forbids the recovery.
  await expect(call({operationId:"sending-refused"})).rejects.toThrow(/in-flight send for the owner/);
  expect(ops("expired-window-successor").n).toBe(0);
  expect(svc.get("xv")!.epoch).toBe(1);
  // Clearing ONLY the in-flight row unblocks it: pending and retained/UNKNOWN debt are not sends.
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id='w-sending'").run();
  expect((await call({operationId:"pending-preserved"})).epoch).toBe(2);
  // Custody, the pending row and both historical UNKNOWN rows are byte-preserved: nothing was
  // reclassified, retried, released or relabelled by the recovery.
  expect(svc.reconciliationDigest("xv")).toBe(custodyBefore);
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id='w-retained'").get()).toEqual({delivery_state:"retained"});
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id='w-indeterminate'").get()).toEqual({delivery_state:"indeterminate"});
 });

 it("a known unattempted pending send does not block the recovery",async()=>{
  await reserveConflict();ready();
  db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('w-pending','lead@xv','builder@xv','{}',?,'pending')").run(new Date().toISOString());
  // 'pending' was never dispatched, so it is not an in-flight send and must not block recovery.
  expect((await call({operationId:"pending-allowed"})).epoch).toBe(2);
  // The pending row is preserved exactly, still pending and unattempted.
  expect(db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id='w-pending'").get()).toEqual({delivery_state:"pending"});
  expect(ops("expired-window-successor").n).toBe(1);
 });

 it("refuses a non-Operator caller and a foreign owner generation",async()=>{
  await reserveConflict();ready();
  expect(()=>svc.recoverExpiredReconciling("lead@xv","lead-g1",{rigId:"xv",operationId:"holder-self",windowMs:120000,expectedEpoch:1,expectedOwnerGeneration:"lead-g1",expectedCustodyDigest:custody(),conflictOperationId:"spent-recovery",conflictKind:"reconciliation-recover"})).toThrow(/Kernel Operator owns/);
  await expect(call({operationId:"wrong-gen",expectedOwnerGeneration:"other-gen"})).rejects.toThrow(/exact current owner generation/);
  await expect(call({operationId:"wrong-epoch",expectedEpoch:9})).rejects.toThrow(/epoch differs/);
  expect(ops("expired-window-successor").n).toBe(0);
 });

 it("refuses without the original spent recovery receipt or with conflicting custody",async()=>{
  await reserveConflict();ready();
  db.prepare("DELETE FROM coordinator_operations WHERE kind='reconciliation-recover' AND rig_id='xv'").run();
  await expect(call({operationId:"no-spent"})).rejects.toThrow(/original spent recovery receipt/);
  await expect(call({operationId:"drift-custody",expectedCustodyDigest:"0".repeat(64)})).rejects.toThrow(/Custody changed/);
  expect(ops("expired-window-successor").n).toBe(0);
 });

 it("binds conflict evidence to the original incident and refuses unrelated ids or fabricated kinds",async()=>{
  await reserveConflict();ready();
  // An operation id that is not this incident's own spent recovery operation cannot stand in, even
  // when a durable row of some other kind exists.
  await expect(call({operationId:"unrelated-conflict",conflictOperationId:"ack"})).rejects.toThrow(/original incident's own spent recovery operation/);
  await expect(call({operationId:"never-recorded",conflictOperationId:"never-recorded"})).rejects.toThrow(/original incident's own spent recovery operation/);
  // A caller cannot name an acknowledge or a successor as the conflicting kind.
  await expect(call({operationId:"ack-kind",conflictKind:"acknowledge"})).rejects.toThrow(/incident's own durable recovery operation/);
  await expect(call({operationId:"successor-kind",conflictKind:"expired-window-successor"})).rejects.toThrow(/incident's own durable recovery operation/);
  // The receipt itself, not a fabricated failure receipt, is the only accepted evidence.
  await expect(call({operationId:"lying-kind",conflictKind:"transfer"})).rejects.toThrow(/incident's own durable recovery operation/);
  expect(ops("expired-window-successor").n).toBe(0);
  // Removing the durable receipt removes the evidence entirely.
  db.prepare("DELETE FROM coordinator_operations WHERE rig_id='xv' AND kind='reconciliation-recover'").run();
  await expect(call({operationId:"no-receipt"})).rejects.toThrow(/original spent recovery receipt/);
  expect(ops("expired-window-successor").n).toBe(0);
 });

 it("refuses a live window, an out-of-range window and a non-expired reconciling state",async()=>{
  await reserveConflict();ready();
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock+5000);
  await expect(call({operationId:"live-window"})).rejects.toThrow(/already expired reconciling window/);
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock-1);
  for(const windowMs of [9999,900001]){
   await expect(call({operationId:"window-"+windowMs,windowMs})).rejects.toThrow(/10 seconds to 15 minutes/);
  }
  db.prepare("UPDATE coordinator_authority SET state='active' WHERE rig_id='xv'").run();
  await expect(call({operationId:"active-state"})).rejects.toThrow(/Only an expired reconciling window/);
  expect(ops("expired-window-successor").n).toBe(0);
 });

 it("replays exactly under the same operation id with no second window",async()=>{
  await reserveConflict();ready();
  const first=await call();
  expect(first.epoch).toBe(2);
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(clock+999999);
  const again=await call();
  expect(again).toEqual(first);
  expect(ops("expired-window-successor").n).toBe(1);
  // A changed payload under the same id refuses.
  await expect(call({windowMs:60000})).rejects.toThrow(/Operation ID reused/);
 });

 it("the genuine holder must still resume-owned separately",async()=>{
  await reserveConflict();ready();
  await call();
  // Reconciling with an epoch the holder did not read yet: resume-owned still refuses a stale contract.
  expect(()=>svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-after-successor",expectedEpoch:1,expectedObligationsDigest:custody()})).toThrow(/epoch advanced/);
  expect(svc.resumeOwned("lead@xv","lead-g1",{rigId:"xv",leaseMs:60000,operationId:"resume-after-successor-2",expectedEpoch:svc.get("xv")!.epoch,expectedObligationsDigest:custody()}).state).toBe("active");
 });
});
