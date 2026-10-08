import { FileDutyJournal, HolderContinuationExecutor, inheritedNativeDutyTransport } from "../src/adapters/native-duty-supervisor.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type Database from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import ts from "typescript";
import { NativeDutyLaunchStore, observeNativeDutyLaunch, type PreparedNativeDutyLaunch } from "../src/domain/native-duty-launch.js";
import type { NativeProcessRow } from "../src/domain/native-process-lineage.js";
import { createDb } from "../src/db/connection.js";
import { seed, token } from "./helpers/coordinator-fixture.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { digest } from "../src/domain/coordinator-authority-service.js";
import { CoordinationRecoveryService, type CoordinationTask, type CoordinationActivity } from "../src/domain/coordination-recovery-service.js";
import { NativeDutyIntegration } from "../src/domain/native-duty-integration.js";
import { nativeDutySupervisionRoutes } from "../src/routes/native-duty-supervision.js";
import { parseRuntimeMigration } from "../src/domain/seat-runtime-migration.js";
import { canonical } from "../src/domain/seat-dispatch-reservation.js";
import type { NativeDutyActor, NativeDutyProof, NativeDutyScope } from "../src/domain/native-duty-contract.js";

describe("native duty actual authority integration",()=>{
 let db:Database.Database,repo:QueueRepository,recovery:CoordinationRecoveryService,integration:NativeDutyIntegration;
 let app:ReturnType<typeof nativeDutySupervisionRoutes>,now:number,scope:NativeDutyScope,lifecycle:boolean,proofPatch:Partial<NativeDutyProof>,proofMissing:boolean;
 const operator={session:"operator-agent@kernel",generation:"operator-agent-g1"};
 const holder={session:"lead@xv",generation:"lead-g1"};
 const enrollment={scopeId:"scope-worker-plan",launchId:"actual-launch",supervisorPid:710};
 const enrollmentUrl=()=>"/enrollment?"+new URLSearchParams({...enrollment,supervisorPid:String(enrollment.supervisorPid)});
 function activity(session:string):CoordinationActivity {
  const generation=repo.coordinatorAuthority.generation(session)!;
  return {generation,identityVerified:true,state:{seatNodeId:session,activity:"idle-at-prompt",needsInput:{count:0,reason:null},decidedBy:"window-sampling",seq:1,changedAt:new Date(now).toISOString(),rungs:[],lastSwap:{generation,at:new Date(now).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:"window-sampling",sourceId:"tmux",seq:1,observedAt:new Date(now).toISOString(),activity:"idle-at-prompt"}};
 }
 async function call(route:string,body?:unknown,as:NativeDutyActor=holder,auth=true) {
  const response=await app.request(route,{method:body===undefined?"GET":"POST",headers:{...(auth?{Authorization:"Bearer private-fixture"}:{}),"X-OpenRig-Session":as.session,"X-OpenRig-Occupant-Generation":as.generation,"Content-Type":"application/json"},...(body===undefined?{}:{body:JSON.stringify(body)})});
  return {status:response.status,body:await response.json() as any};
 }
 async function grantAndRegister() {
  expect((await call("/grant",scope,operator)).status).toBe(201);
  const response=await call("/register",enrollment);expect(response.status).toBe(201);return response.body.registrationId as string;
 }
 const request=(operationId="bounded-native-resume")=>({rigId:"xv",operationId,leaseMs:10000,expectedEpoch:repo.coordinatorAuthority.get("xv")!.epoch,expectedObligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv")});
 beforeEach(async()=>{
  vi.useFakeTimers({toFake:["Date"]});now=Date.UTC(2026,9,7,16);vi.setSystemTime(now);lifecycle=false;proofPatch={};proofMissing=false;
  db=createDb();seed(db); // seed applies the actual ALL_MIGRATIONS, including durable intent triggers.
  db.prepare("UPDATE nodes SET runtime='codex' WHERE id='lead@xv'").run();
  db.prepare("INSERT INTO self_host_identity VALUES(1,'integration-host',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  await repo.create({qitemId:"baton",sourceSession:operator.session,destinationSession:holder.session,body:"coordinate",nudge:false});
  repo.coordinatorAuthority.enable(operator.session,operator.generation,{rigId:"xv",batonId:"baton",owner:holder.session,ownerGeneration:holder.generation,coordinators:[holder.session,"peer@xv"],leaseMs:60000,operationId:"enable"});
  repo.coordinatorAuthority.acknowledge(holder.session,token,{operationId:"ack",obligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv")});
  recovery=new CoordinationRecoveryService(repo,activity,()=>now);repo.coordinatorAuthority.coordinationRecovery=recovery;
  const task=(key:string,owner:string,extra:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:"Return exact evidence for "+key,body:key,deadline:now+30000,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:recovery.configurationDigest(owner)!,qualificationRef:"independent/"+key,capacityRef:"capacity/"+key,effortRef:"effort/"+key,validUntil:now+60000},...extra});
  const tasks=[task("product","builder@xv"),task("repair","architect@xv",{recoveryFor:"product"})];
  for(const t of tasks)repo.coordinatorAuthority.admit(operator.session,operator.generation,"xv",t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:holder.session,evidenceRequired:["report"]}});
  recovery.configure(operator.session,operator.generation,{rigId:"xv",revision:"actual-worker-plan",operatorGeneration:operator.generation,stallMs:10000,allowIdlePeerTransfer:true,tasks});
  scope={scopeId:enrollment.scopeId,nodeId:holder.session,sessionName:holder.session,generation:holder.generation,runtime:"codex",rigId:"xv",configurationDigest:recovery.configurationDigest(holder.session)!,validUntil:now+20000,maxLeaseMs:15000,kind:"holder-continuation"};
  integration=new NativeDutyIntegration({db,authority:repo.coordinatorAuthority,now:()=>now,lifecycleActive:()=>lifecycle,
   binding:session=>{const row=db.prepare("SELECT n.id nodeId,n.rig_id rigId,n.runtime FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE s.session_name=?").get(session) as {nodeId:string;rigId:string;runtime:string}|undefined;const generation=repo.coordinatorAuthority.generation(session);return row&&generation?{...row,session,generation}:null;},
   observe:async(current,launchId,supervisorPid)=>proofMissing?null:{nodeId:current.nodeId,sessionName:current.sessionName,generation:current.generation,runtime:current.runtime,launchId,supervisorPid,configurationDigest:current.configurationDigest,fingerprint:"independent-native-sample",observedAt:now,nativePresent:true,supervisorIsNativeAncestor:true,lifecycleReserved:false,...proofPatch},
  });
  app=nativeDutySupervisionRoutes({bearerToken:"private-fixture",service:integration.service,refreshNative:(actor,input)=>integration.refreshNative(actor,input),enrollment:(actor,input)=>integration.enrollment(actor,input)});
 });
 afterEach(()=>{db?.close();vi.useRealTimers();});

 it("gives each same-generation managed launch an immutable fresh scope and enrolls only its exact intent",async()=>{
  const dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),"native-duty-launch-scope-")));
  try {
   const node=path.join(dir,"node"),entry=path.join(dir,"supervisor.js");
   fs.writeFileSync(node,"pinned fixture node",{mode:0o700});fs.writeFileSync(entry,"pinned fixture supervisor",{mode:0o600});
   let storedAt=now-10000;
   const store=new NativeDutyLaunchStore({root:path.join(dir,"intents"),nodeExecutable:node,supervisorEntry:entry,now:()=>++storedAt});
   const input={nodeId:holder.session,sessionName:holder.session,generation:holder.generation,runtime:"codex" as const,harness:{executable:node,args:["harness.js"],cwd:dir}};
   // Execute the actual startup composition without starting a daemon or native process.
   const startup=fs.readFileSync(new URL("../src/startup.ts",import.meta.url),"utf8");
   const start=startup.indexOf("  const nativeDutyLaunch = {"),end=startup.indexOf("  const seatLaunchEnvironment =",start);
   expect(start).toBeGreaterThan(0);expect(end).toBeGreaterThan(start);
   const code=ts.transpileModule(startup.slice(start,end),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.None}}).outputText;
   const uuid=vi.fn(randomUUID);
   const wrap=new Function("nativeDutyStore","nativeDutyNodes","queueRepoInstance","randomUUID",code+"; return nativeDutyLaunch;")(store,new Set([holder.session]),repo,uuid) as {wrap:(launchInput:typeof input)=>Promise<{args:string[]}>};
   const launch=async()=>{const descriptor=await wrap.wrap(input);const config=JSON.parse(fs.readFileSync(descriptor.args[2]!,"utf8"));const read=store.read(config.scopeId,config.launchId)!;expect(read).not.toBeNull();return {intent:read.intent,config:read.config,publicEnvironment:{OPENRIG_NODE_ID:holder.session,OPENRIG_SESSION_NAME:holder.session,OPENRIG_OCCUPANT_GENERATION:holder.generation,OPENRIG_RUNTIME:"codex"}};};
   const legacy=store.prepare({...input,scopeId:`native-duty:${holder.session}:${holder.generation}`,launchId:"legacy-launch",configurationDigest:scope.configurationDigest,pollMs:5000});
   let active:Pick<PreparedNativeDutyLaunch,"intent"|"config"|"publicEnvironment">=legacy,duplicate=false;
   integration=new NativeDutyIntegration({db,authority:repo.coordinatorAuthority,now:()=>now,
    binding:session=>session===holder.session?{nodeId:holder.session,session,generation:holder.generation,runtime:"codex",rigId:"xv"}:null,
    observe:(current,launchId,supervisorPid)=>observeNativeDutyLaunch(store,{scope:current,launchId,supervisorPid},{now:()=>now,
     currentBinding:async()=>({nodeId:holder.session,sessionName:holder.session,generation:holder.generation,runtime:"codex",configurationDigest:scope.configurationDigest,pane:"%fixture",lifecycleReserved:false}),
     tmux:{getPanePid:async()=>10},
     listProcesses:async()=>{const row=(pid:number,ppid:number,name:string,command:string):NativeProcessRow=>({pid,ppid,executableName:name,command,startedAt:"stable-"+pid,pgid:20,tpgid:20});return [row(10,1,"zsh","/bin/zsh"),row(20,10,"node",`${node} ${entry} --supervise ${active.intent.configPath}`),row(30,20,"node",`${node} harness.js`),row(40,30,"codex","/native/codex --no-daemon"),...(duplicate?[row(41,30,"codex","/native/codex --no-daemon")]:[])];},
     verifyProcessIdentity:async(pid,expected,argv)=>JSON.stringify(expected)===JSON.stringify(active.publicEnvironment)&&(!argv||JSON.stringify(argv)===JSON.stringify(pid===20?[node,entry,"--supervise",active.intent.configPath]:[node,...active.config.harness.args])),
    }),
   });
   app=nativeDutySupervisionRoutes({bearerToken:"private-fixture",service:integration.service,refreshNative:(actor,i)=>integration.refreshNative(actor,i),enrollment:(actor,i)=>integration.enrollment(actor,i)});
   const reference=(p:typeof active)=>({scopeId:p.intent.scopeId,launchId:p.intent.launchId,supervisorPid:20});
   const discover=(p:typeof active)=>call("/enrollment?"+new URLSearchParams({...reference(p),supervisorPid:"20"}));
   const grantScope=(p:typeof active,validUntil:number)=>({...scope,scopeId:p.intent.scopeId,validUntil});
   // A legacy intent and revoked registration remain readable, without revival.
   expect((await call("/grant",grantScope(legacy,now+10000),operator)).status).toBe(201);
   const legacyRegistered=await call("/register",reference(legacy));expect(legacyRegistered.status).toBe(201);
   expect((await call("/revoke",{scopeId:legacy.intent.scopeId},operator)).status).toBe(200);
   const legacyGrant=JSON.stringify(db.prepare("SELECT * FROM native_duty_grants WHERE scope_id=?").get(legacy.intent.scopeId));
   const legacyBytes=fs.readFileSync(legacy.intentPath);
   const first=await launch();active=first;
   expect((await discover(first)).body.state).toBe("waiting");
   expect((await call("/grant",grantScope(first,now+1000),operator)).status).toBe(201);
   const firstRegistered=await call("/register",reference(first));expect(firstRegistered.status).toBe(201);
   const firstGrant=JSON.stringify(db.prepare("SELECT * FROM native_duty_grants WHERE scope_id=?").get(first.intent.scopeId)),firstBytes=fs.readFileSync(path.join(path.dirname(first.intent.configPath),"launch-intent.json"));
   now+=1001;vi.setSystemTime(now);
   const second=await launch();active=second;
   expect(uuid).toHaveBeenCalledTimes(2);expect(second.intent.scopeId).not.toBe(first.intent.scopeId);expect(second.intent.launchId).not.toBe(first.intent.launchId);
   for(const p of [first,second]){expect(p.intent.scopeId).toBe(`native-duty:${p.intent.launchId}`);expect(p.intent.nodeId).toBe(holder.session);expect(p.intent.generation).toBe(holder.generation);}
   expect((await call("/grant",grantScope(first,now+10000),operator)).body.error).toBe("native_duty_grant_conflict");
   expect((await call("/grant",grantScope(second,now+10000),operator)).status).toBe(201);
   expect((await discover(second)).body.state).toBe("ready");
   expect((await call("/enrollment?"+new URLSearchParams({...reference(second),launchId:first.intent.launchId,supervisorPid:"20"}))).body.state).toBe("waiting");
   duplicate=true;expect((await discover(second)).body.state).toBe("waiting");duplicate=false;
   const secondRegistered=await call("/register",reference(second));expect(secondRegistered.status).toBe(201);
   expect((await discover(second)).body.registrationId).toBe(secondRegistered.body.registrationId);
   expect((await discover(first)).body.registrationId).toBe(firstRegistered.body.registrationId);expect((await discover(legacy)).body.registrationId).toBe(legacyRegistered.body.registrationId);
   expect(store.read(legacy.intent.scopeId,legacy.intent.launchId)?.intent).toEqual(legacy.intent);expect(fs.readFileSync(legacy.intentPath)).toEqual(legacyBytes);
   expect(fs.readFileSync(path.join(path.dirname(first.intent.configPath),"launch-intent.json"))).toEqual(firstBytes);
   expect(JSON.stringify(db.prepare("SELECT * FROM native_duty_grants WHERE scope_id=?").get(first.intent.scopeId))).toBe(firstGrant);
   expect(JSON.stringify(db.prepare("SELECT * FROM native_duty_grants WHERE scope_id=?").get(legacy.intent.scopeId))).toBe(legacyGrant);
   // A third fresh scope is not a bypass of unresolved node-wide effect debt.
   const operation=request("scope-lifetime-unknown");expect((await call("/prepare",{registrationId:secondRegistered.body.registrationId,request:operation})).status).toBe(201);
   expect((await call("/in-flight",{registrationId:secondRegistered.body.registrationId,operationId:operation.operationId})).body.maySendEffect).toBe(true);
   const debt=JSON.stringify(db.prepare("SELECT * FROM native_duty_intents").all()),third=await launch();
   expect((await call("/grant",grantScope(third,now+10000),operator)).body.error).toBe("native_duty_unresolved_intent");
   expect(JSON.stringify(db.prepare("SELECT * FROM native_duty_intents").all())).toBe(debt);
  }finally{fs.rmSync(dir,{recursive:true,force:true});}
 });

 it("authorizes a coordinating holder for genuine worker-owned bounded work and confirms the actual coordinator receipt",async()=>{
  expect(recovery.plan("xv")!.tasks.every(t=>t.owner!==holder.session)).toBe(true);
  const registrationId=await grantAndRegister(),resume=request();
  expect((await call("/prepare",{registrationId,request:resume})).body.phase).toBe("prepared");
  expect((await call("/in-flight",{registrationId,operationId:resume.operationId})).body.maySendEffect).toBe(true);
  repo.coordinatorAuthority.resumeOwned(holder.session,holder.generation,resume);
  expect((await call("/reconcile",{registrationId,operationId:resume.operationId})).body.phase).toBe("receipt-confirmed");
  expect(repo.coordinatorAuthority.operationReceipt("xv",resume.operationId)?.kind).toBe("resume-owned");
 });

 it("ends the work scope only after genuine exact acceptance, including a dormant mandatory recovery backup",async()=>{
  await repo.create({qitemId:"product-work",sourceSession:holder.session,destinationSession:"builder@xv",body:"product",dispatch:{token,packageKey:"product"},nudge:false});
  repo.claim({qitemId:"product-work",destinationSession:"builder@xv",identityProvenance:"transport:v1"});
  repo.update({qitemId:"product-work",actorSession:"builder@xv",state:"done",closureReason:"no-follow-on"});
  await repo.create({qitemId:"product-return",sourceSession:"builder@xv",destinationSession:holder.session,body:JSON.stringify({packageKey:"product",inputDigest:digest("product"),evidence:[{kind:"report",ref:"actual/product.md"}]}),nudge:false});
  repo.coordinatorAuthority.dispose("builder@xv","builder-g1","xv","product","product-return");
  db.prepare("UPDATE outbox_entries SET delivery_state='delivered'").run();
  expect((await call("/grant",scope,operator)).status).toBe(201); // done + disposition is not acceptance
  recovery.accept(holder.session,holder.generation,"xv","product","product-return","actual/independent-acceptance.md");
  expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE kind='coordination-accept'").get()).toBeTruthy();
  expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE package_key='repair'").get()).toBeUndefined();
  expect((await call("/grant",{...scope,scopeId:"after-exact-acceptance"},operator)).body.error).toBe("native_duty_scope_not_approved");
 });

 it.each(["memory","reservation"])("temporary %s exclusion holds SAME helper then clears without stopping registration or authorizing effects",async(kind)=>{
  scope={...scope,validUntil:now+30000}; // Inside existing task deadline; leave the unchanged transport budget.
  const registrationId=await grantAndRegister();
  const original=db.prepare("SELECT * FROM native_duty_registrations WHERE registration_id=?").get(registrationId);
  if(kind==="memory")lifecycle=true;
  else db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('busy-reservation','migration','lead@xv','lead@xv','lead-g1','prior-native','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  const journalRoot=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),"temporary-duty-")));fs.chmodSync(journalRoot,0o700);
  const values={OPENRIG_SESSION_NAME:holder.session,OPENRIG_OCCUPANT_GENERATION:holder.generation,OPENRIG_URL:"http://127.0.0.1:45678",OPENRIG_TERMINAL_BEARER_TOKEN:"private-fixture"};
  const previous=new Map(Object.keys(values).map(k=>[k,process.env[k]])),oldFetch=globalThis.fetch;const routes:string[]=[];
  try{
   Object.assign(process.env,values);
   globalThis.fetch=(async(input,init)=>{const url=new URL(String(input));const route=url.pathname.replace('/api/native-duty','');routes.push(route);return app.request(route,init);}) as typeof fetch;
   const transport=inheritedNativeDutyTransport().transport;
   transport.show=async()=>{const a=repo.coordinatorAuthority.get("xv")!,baton=repo.getById(a.baton_id)!;return {authority:a,obligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv"),obligations:[{openQueue:[{qitem_id:a.baton_id,destination_session:holder.session,state:baton.state,claimed_by_generation_uuid:holder.generation}]}]};};
   const journal=new FileDutyJournal(journalRoot),executor=new HolderContinuationExecutor(transport,journal,holder,{now:()=>now,sleep:async()=>{}},()=>true,()=>{throw Error("busy/ample lease cannot prepare");});
   for(let i=0;i<5;i++){expect(await executor.step(registrationId)).toBe("held");now+=1000;vi.setSystemTime(now);}
   expect(db.prepare("SELECT * FROM native_duty_registrations WHERE registration_id=?").get(registrationId)).toEqual(original);
   expect(journal.read()).toBeNull();expect(db.prepare("SELECT count(*) n FROM native_duty_intents").get()).toEqual({n:0});
   expect((await call("/register",{...enrollment,launchId:"busy-new"})).body.error).toBe("native_duty_temporary_exclusion");
   expect((await call("/prepare",{registrationId,request:request("busy-prepare")})).body.error).toBe("native_duty_temporary_exclusion");
   expect((await call("/in-flight",{registrationId,operationId:"busy-prepare"})).body.error).toBe("native_duty_temporary_exclusion");
   if(kind==="memory")lifecycle=false;else db.prepare("UPDATE seat_dispatch_reservations SET state='released' WHERE reservation_id='busy-reservation'").run();
   expect(await executor.step(registrationId)).toBe("watching");expect(integration.service.status(registrationId).phase).toBe("watching");
   expect(routes.filter(r=>r==='/heartbeat')).toHaveLength(6);
   expect(routes.every(r=>r.startsWith('/status/')||r==='/heartbeat')).toBe(true);
  }finally{globalThis.fetch=oldFetch;for(const[k,v]of previous){if(v===undefined)delete process.env[k];else process.env[k]=v;}fs.rmSync(journalRoot,{recursive:true,force:true});}
 });
 it("temporary native proof exclusion race stays observational and does not waive wrong identity",async()=>{
  const registrationId=await grantAndRegister();proofPatch={lifecycleReserved:true};
  expect((await call("/heartbeat",{registrationId})).body.error).toBe("native_duty_temporary_exclusion");
  expect(integration.service.status(registrationId).phase).toBe("watching");
  proofPatch={};expect((await call("/heartbeat",{registrationId})).status).toBe(200);
  proofPatch={lifecycleReserved:true,generation:"wrong"};
  expect((await call("/heartbeat",{registrationId})).body.error).toBe("native_duty_proof_mismatch");
  expect(integration.service.status(registrationId).phase).toBe("held");
 });
 it("unavailable native observation refuses effects but preserves the same watching registration for a fresh proof",async()=>{
  const registrationId=await grantAndRegister();
  const registration=()=>db.prepare("SELECT * FROM native_duty_registrations WHERE registration_id=?").get(registrationId);
  const before=registration();proofMissing=true;
  const unavailable=await call("/heartbeat",{registrationId});
  expect(unavailable.status).toBe(409);expect(unavailable.body.error).toBe("native_duty_proof_unavailable");
  expect(integration.service.status(registrationId).phase).toBe("watching");expect(registration()).toEqual(before);
  const resume=request("no-proof-must-not-prepare");
  const refused=await call("/prepare",{registrationId,request:resume});
  expect(refused.status).toBe(409);expect(refused.body.error).toBe("native_duty_proof_unavailable");
  expect(db.prepare("SELECT * FROM native_duty_intents").all()).toEqual([]);expect(registration()).toEqual(before);

  proofMissing=false;now+=1000;vi.setSystemTime(now);
  const fresh=await call("/heartbeat",{registrationId});
  expect(fresh.status).toBe(200);expect(fresh.body.registrationId).toBe(registrationId);expect(fresh.body.phase).toBe("watching");
  expect(integration.service.status(registrationId).phase).toBe("watching");
  expect((registration() as any).last_heartbeat_at).toBe(now);expect(db.prepare("SELECT count(*) n FROM native_duty_registrations").get()).toEqual({n:1});
 });
 it("temporary exclusion preserves prepared bytes and never reopens an in-flight send grant",async()=>{
  const registrationId=await grantAndRegister(),r=request();
  expect((await call("/prepare",{registrationId,request:r})).status).toBe(201);
  const original=db.prepare("SELECT * FROM native_duty_intents").get();lifecycle=true;
  expect((await call("/in-flight",{registrationId,operationId:r.operationId})).body.error).toBe("native_duty_temporary_exclusion");
  expect(db.prepare("SELECT * FROM native_duty_intents").get()).toEqual(original);
  expect(integration.service.status(registrationId).phase).toBe("watching");
  lifecycle=false;expect((await call("/in-flight",{registrationId,operationId:r.operationId})).body.maySendEffect).toBe(true);
  lifecycle=true;const inFlight=db.prepare("SELECT * FROM native_duty_intents").get();
  expect((await call("/heartbeat",{registrationId})).body.error).toBe("native_duty_temporary_exclusion");
  expect(db.prepare("SELECT * FROM native_duty_intents").get()).toEqual(inFlight);
  lifecycle=false;expect((await call("/in-flight",{registrationId,operationId:r.operationId})).body.maySendEffect).toBe(false);
 });
 it.each(["revoked","expired","generation","configuration","work","stopped"])("permanent %s remains terminal even while temporarily busy",async(kind)=>{
  const registrationId=await grantAndRegister();lifecycle=true;
  if(kind==="revoked")await call("/revoke",{scopeId:scope.scopeId},operator);
  if(kind==="expired"){now=scope.validUntil;vi.setSystemTime(now);}
  if(kind==="generation")db.prepare("UPDATE occupant_tenures SET generation_uuid='retired-holder' WHERE node_id='lead@xv'").run();
  if(kind==="configuration")db.prepare("UPDATE nodes SET model='changed' WHERE id='lead@xv'").run();
  if(kind==="work")db.prepare("UPDATE nodes SET model='changed' WHERE id IN ('builder@xv','architect@xv')").run();
  if(kind==="stopped")await call("/stop",{registrationId,reason:"retained-stop"});
  expect((await call("/heartbeat",{registrationId})).status).toBeGreaterThanOrEqual(400);
  expect(integration.service.status(registrationId).phase).toBe("stopped");
  lifecycle=false;expect((await call("/heartbeat",{registrationId})).status).toBeGreaterThanOrEqual(400);
  expect(integration.service.status(registrationId).phase).toBe("stopped");
 });
 it("waits before grant and recovers a lost registration response through read-only enrollment without duplication",async()=>{
  expect(await call(enrollmentUrl())).toEqual({status:200,body:{state:"waiting"}});
  expect(db.prepare("SELECT count(*) n FROM native_duty_grants").get()).toEqual({n:0});
  expect((await call("/grant",scope,operator)).status).toBe(201);
  expect(await call(enrollmentUrl())).toEqual({status:200,body:{state:"ready"}});
  await call("/register",enrollment); // treat the response as lost
  const existing=db.prepare("SELECT registration_id FROM native_duty_registrations").get() as {registration_id:string};
  expect(await call(enrollmentUrl())).toEqual({status:200,body:{state:"ready",registrationId:existing.registration_id}});
  expect(db.prepare("SELECT count(*) n FROM native_duty_registrations").get()).toEqual({n:1});
  expect((await call(enrollmentUrl(),undefined,{...holder,generation:"wrong-generation"})).body.state).toBe("held");
  expect((await call(enrollmentUrl()+"&extra=not-allowed")).status).toBe(400);
  expect((await call(enrollmentUrl(),undefined,holder,false)).status).toBe(401);
 });

 it.each(["scope-expiry","holder-generation","holder-configuration","worker-configuration","lifecycle-memory","lifecycle-reservation"])("holds %s at authorization and registration boundaries",async(kind)=>{
  expect((await call("/grant",scope,operator)).status).toBe(201);
  if(kind==="scope-expiry"){now=scope.validUntil;vi.setSystemTime(now);}
  if(kind==="holder-generation")db.prepare("UPDATE occupant_tenures SET generation_uuid='lead-g2' WHERE node_id='lead@xv'").run();
  if(kind==="holder-configuration")db.prepare("UPDATE nodes SET model='changed-model' WHERE id='lead@xv'").run();
  if(kind==="worker-configuration")db.prepare("UPDATE nodes SET model='changed-model' WHERE id IN ('builder@xv','architect@xv')").run();
  if(kind==="lifecycle-memory")lifecycle=true;
  if(kind==="lifecycle-reservation")db.prepare("INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES ('reservation','migration','lead@xv','lead@xv','lead-g1','prior-native','operator-agent@kernel','operator-agent-g1','hash','{}','{}','reserved',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  expect((await call(enrollmentUrl())).body.state).toBe("held");
  expect((await call("/register",enrollment)).status).toBeGreaterThanOrEqual(400);
  expect((await call("/grant",{...scope,scopeId:"new-scope"},operator)).body.error).toBe(kind.startsWith("lifecycle-")?"native_duty_temporary_exclusion":"native_duty_scope_not_approved");
  expect(db.prepare("SELECT count(*) n FROM native_duty_registrations").get()).toEqual({n:0});
 });

 it.each(["stale","identity","configuration"])("does not advertise ready or register with %s native evidence",async(kind)=>{
  expect((await call("/grant",scope,operator)).status).toBe(201);
  proofPatch=kind==="stale"?{observedAt:now-3001}:kind==="identity"?{generation:"other-generation"}:{configurationDigest:"different-config"};
  expect((await call(enrollmentUrl())).body.state).not.toBe("ready");
  expect((await call("/register",enrollment)).status).toBeGreaterThanOrEqual(400);
 });

 it("preserves in-flight receipt debt through an unavailable proof and stops only when its scope expires",async()=>{
  const registrationId=await grantAndRegister(),resume=request();
  expect((await call("/prepare",{registrationId,request:resume})).body.phase).toBe("prepared");
  await call("/in-flight",{registrationId,operationId:resume.operationId});
  repo.coordinatorAuthority.resumeOwned(holder.session,holder.generation,resume); // real durable receipt, response lost
  proofMissing=true;expect((await call("/heartbeat",{registrationId})).status).toBe(409);
  expect(integration.service.status(registrationId).phase).toBe("watching");
  const intentBefore=db.prepare("SELECT * FROM native_duty_intents WHERE registration_id=?").get(registrationId);
  expect((await call(enrollmentUrl())).body).toEqual({state:"ready",registrationId});
  now=scope.validUntil+1;vi.setSystemTime(now);
  expect((await call(enrollmentUrl())).body).toEqual({state:"ready",registrationId});
  expect((await call("/heartbeat",{registrationId})).body.error).toBe("native_duty_scope_expired");
  expect(integration.service.status(registrationId).phase).toBe("stopped");
  expect((await call("/reconcile",{registrationId,operationId:resume.operationId})).body.phase).toBe("receipt-confirmed");
  expect(integration.service.status(registrationId).phase).toBe("stopped");
  expect(db.prepare("SELECT * FROM native_duty_intents WHERE registration_id=?").get(registrationId)).toEqual({...intentBefore,phase:"receipt-confirmed"});
  expect((await call("/prepare",{registrationId,request:request("new-effect")})).status).toBeGreaterThanOrEqual(400);
  expect(db.prepare("SELECT count(*) n FROM native_duty_registrations").get()).toEqual({n:1});
 });

 it("pins staged migration target digest before launch and equals committed current configuration without granting early scope",async()=>{
  // The real peer is zero-custody; production migration authorization/native effects are
  // covered by the accepted migration suite. These rows model its durable begin/commit boundaries.
  const session="peer@xv",generation="peer-successor-g2",launch={nodeId:session,generation,runtime:"codex"};
  db.prepare("UPDATE nodes SET runtime='pi',model='old-pi-model',codex_config_profile=NULL WHERE id=?").run(session);
  const previous=recovery.configurationDigest(session);
  expect(recovery.configurationDigest(session,launch)).toBeNull(); // no approved staged target
  const packet=parseRuntimeMigration({operationId:"digest-peer-migration",target:{runtime:"codex",provider:"openai",model:"gpt-6-luna",effort:"high",codexConfigProfile:"conveyor-luna-high"},
   expected:{nodeId:session,sessionName:session,sessionId:session,generation:"peer-g1",pane:"%42",runtime:"pi",configSha256:"a".repeat(64),nativeFingerprintSha256:"b".repeat(64)}});
  const prepared={protocol:"runtime-migration-v1",...packet},at=new Date(now).toISOString();
  db.prepare(`INSERT INTO seat_dispatch_reservations
   (reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,
    request_hash,expected_json,frozen_snapshot,state,performer_session,performer_generation,successor_generation,created_at,updated_at)
   VALUES (?,?,?,?,?,?,?,?,?,?,?,'started',?,?,?,?,?)`).run("digest-reservation",packet.operationId,session,session,"peer-g1","retained-pi-history.jsonl",
    operator.session,operator.generation,digest(canonical(packet)),JSON.stringify(prepared),"retained-custody-digest",operator.session,operator.generation,generation,at,at);
  const projected=recovery.configurationDigest(session,launch);
  expect(projected).toMatch(/^[a-f0-9]{64}$/);expect(projected).not.toBe(previous);
  expect(recovery.configurationDigest(session)).toBe(previous);
  expect(repo.coordinatorAuthority.generation(session)).toBe("peer-g1");
  expect(db.prepare("SELECT runtime,model,codex_config_profile FROM nodes WHERE id=?").get(session)).toEqual({runtime:"pi",model:"old-pi-model",codex_config_profile:null});
  for(const mismatch of [{...launch,generation:"wrong-successor"},{...launch,nodeId:"other-node"},{...launch,runtime:"pi"}])expect(recovery.configurationDigest(session,mismatch)).toBeNull();
  for(const expected of [{...packet.expected!,sessionName:"wrong@xv"},{...packet.expected!,nodeId:"wrong-node"}]){
   db.prepare("UPDATE seat_dispatch_reservations SET expected_json=? WHERE reservation_id='digest-reservation'").run(JSON.stringify({...prepared,expected}));
   expect(recovery.configurationDigest(session,launch)).toBeNull();
  }
  db.prepare("UPDATE seat_dispatch_reservations SET expected_json=? WHERE reservation_id='digest-reservation'").run(JSON.stringify(prepared));
  const prospective={...scope,scopeId:"projected-peer-scope",nodeId:session,sessionName:session,generation,runtime:"codex",configurationDigest:projected};
  expect((await call("/grant",prospective,operator)).body.error).toBe("native_duty_scope_not_approved");
  expect(db.prepare("SELECT count(*) n FROM native_duty_grants WHERE scope_id=?").get(prospective.scopeId)).toEqual({n:0});
  // Same fields and atomic reservation release as the already-tested migration commit.
  db.transaction(()=>{
   db.prepare("UPDATE nodes SET runtime=?,model=?,effort=?,codex_config_profile=? WHERE id=?").run(packet.target.runtime,packet.target.model,packet.target.effort,packet.target.codexConfigProfile,session);
   db.prepare("UPDATE occupant_tenures SET generation_uuid=? WHERE node_id=?").run(generation,session);
   db.prepare("UPDATE seat_dispatch_reservations SET state='released',successor_native_id='fresh-codex-thread',release_receipt='exact-commit' WHERE reservation_id='digest-reservation'").run();
  }).immediate();
  expect(recovery.configurationDigest(session)).toBe(projected);
  expect(recovery.configurationDigest(session,launch)).toBe(projected);
  expect(recovery.configurationDigest(session,{...launch,runtime:"pi"})).toBeNull();
  // Runtime migration alone never transfers coordinator authority to this peer.
  expect(repo.coordinatorAuthority.get("xv")!.owner_session).toBe(holder.session);
  expect((await call("/grant",prospective,operator)).body.error).toBe("native_duty_scope_not_approved");
 });

 it("rejects authority expiry and changed obligations before native intent preparation",async()=>{
  const registrationId=await grantAndRegister(),stale=request();
  expect((await call("/prepare",{registrationId,request:{...stale,expectedObligationsDigest:"f".repeat(64)}})).body.error).toBe("native_duty_authority_changed");
  db.prepare("UPDATE coordinator_authority SET lease_until=? WHERE rig_id='xv'").run(now);
  expect((await call("/prepare",{registrationId,request:stale})).body.error).toBe("native_duty_authority_changed");
  expect(db.prepare("SELECT count(*) n FROM native_duty_intents").get()).toEqual({n:0});
 });
});
