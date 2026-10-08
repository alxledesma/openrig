import { RigRepository } from "../src/domain/rig-repository.js";
import { RestoreOrchestrator } from "../src/domain/restore-orchestrator.js";
import {afterEach,beforeEach,describe,expect,it,vi} from "vitest";
import {mkdtempSync,realpathSync,writeFileSync,appendFileSync,rmSync,readFileSync,mkdirSync,symlinkSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import type Database from "better-sqlite3";
import {createDb} from "../src/db/connection.js";
import {seed,token} from "./helpers/coordinator-fixture.js";
import {EventBus} from "../src/domain/event-bus.js";
import {QueueRepository} from "../src/domain/queue-repository.js";
import {OutboxHandler} from "../src/domain/outbox-handler.js";
import {CoordinationRecoveryService} from "../src/domain/coordination-recovery-service.js";
import {digest,legacyProposalDigest,type LegacyEnrollment} from "../src/domain/coordinator-authority-service.js";
import {HistoricalEffectDispositionService,historicalDigest,type HistoricalPlan} from "../src/domain/historical-effect-disposition.js";
import {SeatDeliveryGuard,resolveGuardTarget} from "../src/domain/seat-delivery-guard.js";
import {NativeDutyLaunchStore} from "../src/domain/native-duty-launch.js";
import * as launches from "../src/domain/native-duty-launch.js";
import * as processes from "../src/domain/native-process-lineage.js";
import * as piLaunch from "../src/domain/pi-rotation-launch-proof.js";
import * as piFacts from "../src/domain/pi-rotation-facts-resolver.js";
import * as rotation from "../src/domain/rotation-facts-resolver.js";
import {contextRefreshDigest} from "../src/domain/context-refresh-service.js";
import {ContextRefreshIntegration,type ContextRefreshIntegrationDeps,type ContextRefreshCheckpointPacket} from "../src/domain/context-refresh-integration.js";
import {DEFAULT_CONTEXT_REFRESH_POLICY,type ContextRefreshGrant} from "../src/domain/context-refresh-contract.js";

const operator={session:"operator-agent@kernel",generation:"operator-agent-g1"},seat="builder@xv",targetActor={session:seat,generation:"builder-g1"};
const packet:ContextRefreshCheckpointPacket={current_work:"bounded work",decisions:[],memory:[],constraints:[],standing_duties:[],evidence:[],next_action:"continue",outstanding_effects:[]};
const contract={runtime:"codex",model:"gpt-6-luna",provider:"openai",profile:"refresh-profile",effort:"high",permissions:{sandbox:{type:"workspace-write"},approval:"never"}};
describe("actual context refresh composition",()=>{
 let db:Database.Database,dir:string,file:string,repo:QueueRepository,outbox:OutboxHandler,guard:SeatDeliveryGuard,integration:ContextRefreshIntegration,deps:ContextRefreshIntegrationDeps,grant:ContextRefreshGrant;
 let now:number,native:boolean,supervisor:boolean,activity:string,activityAt:number,handoverCount:number,failHandover:boolean,whoamiResolve:ReturnType<typeof vi.fn>;
 const selection=()=>({grantId:grant.grantId,nodeId:seat});
 const line=(type:string,payload:unknown)=>JSON.stringify({timestamp:new Date(now).toISOString(),type,payload})+"\n";
 const usage=(used=86)=>line("event_msg",{type:"token_count",info:{last_token_usage:{total_tokens:used},model_context_window:100}});
 const compact=()=>appendFileSync(file,line("compacted",{message:"Actual successful native summary"}));
 const target=()=>({nodeId:seat,sessionName:seat,generation:repo.coordinatorAuthority.generation(seat)!,runtime:"codex" as const,nativeId:(db.prepare("SELECT resume_token FROM sessions WHERE node_id=?").get(seat) as any).resume_token,configurationDigest:deps.configurationDigest(seat)!});
 function piTarget() {
  const agent=join(dir,"pi-agent");mkdirSync(agent,{mode:0o700});
  writeFileSync(join(agent,"settings.json"),JSON.stringify({defaultProvider:"provider",defaultModel:"model",defaultThinkingLevel:"high"}));
  writeFileSync(join(agent,"models.json"),'{}');
  db.prepare("UPDATE nodes SET runtime='pi',model='provider/model',codex_config_profile=NULL,policy_launch_posture='floor' WHERE id=?").run(seat);
  db.prepare("UPDATE sessions SET resume_type='pi_session_file',resume_token=? WHERE node_id=?").run(file,seat);
  let leaf="thinking",launch="pi-launch-old",present=true,failures:any[]=[],used:number|null=86;
  const writeHistory=(header:string)=>writeFileSync(file,[{type:"session",id:header,timestamp:new Date(now).toISOString()},
   {type:"model_change",id:"model",provider:"provider",modelId:"model",timestamp:new Date(now).toISOString()},
   {type:"thinking_level_change",id:"thinking",thinkingLevel:"high",timestamp:new Date(now).toISOString()}].map(r=>JSON.stringify(r)+"\n").join(""));
  writeHistory("pi-old-header");
  const generation=()=>repo.coordinatorAuthority.generation(seat)!;
  // Exercise the actual shared policy resolver with this fixture's real DB;
  // no launch/restore machinery is constructed or run.
  const resolver = Object.assign(Object.create(RestoreOrchestrator.prototype), { rigRepo: new RigRepository(db) }) as RestoreOrchestrator;
  deps.piRotation={agentDir:()=>agent,runnerEntryPath:join(dir,"pi-runner.js"),resolvePosture:(nodeId,rigId)=>resolver.resolveRestorePosture(nodeId,rigId)};
  deps.piProof=async()=>({state:"present",generation:generation(),launchId:launch,fingerprint:"real-pi-proof-fixture",lastEntryId:leaf,quiescence:{settled:activity==="idle-at-prompt",observedAt:new Date(activityAt).toISOString()}});
  deps.piState=async()=>({ready:true,launchId:launch,sessionFile:file,lastEntryId:leaf,model:{provider:"provider",id:"model",contextWindow:100},
   quiescence:{launchId:launch,generation:generation(),sessionFile:file,lastEntryId:leaf,settled:activity==="idle-at-prompt",observedAt:new Date(activityAt).toISOString()},
   rpcSessionFileProof:{launchId:launch,generation:generation(),childPid:22,sessionFile:file,responseId:"pi-runner-get-state",observedAt:new Date(activityAt).toISOString()},
   runtimeReadiness:{launchId:launch,generation:generation(),sessionFile:file,thinkingLevel:"high",model:{provider:"provider",id:"model",contextWindow:100},observedAt:new Date(activityAt).toISOString(),failures,
    ...(used===null?{}:{context:{source:"assistant_usage",usedTokens:used,remainingTokens:100-used,observedAt:new Date(activityAt).toISOString()}})}});
  vi.spyOn(piLaunch,"observePiRotationLaunch").mockImplementation(async input=>present?{generation:input.generation,launchId:launch,sessionFile:file,pid:22,startFingerprint:"kernel-fixture-start",trustFlag:"no-approve"}:null);
  deps.handoverFactory=({dispatchReservations})=>({handover:async(input:any)=>{
   handoverCount++;const r=dispatchReservations.assertHandover(input.rotationExpected.reservationId,input.rotationExpected.operationId,seat,operator.session,operator.generation,input.rotationExpected);
   dispatchReservations.start(r,operator.session,operator.generation);if(failHandover)throw Error("native result unknown");
   file=join(dir,"fresh-pi.jsonl");leaf="thinking";launch="pi-launch-new";writeHistory("pi-new-header");
   db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('successor',?,2,'builder-g2','fresh')").run(seat);
   db.prepare("UPDATE nodes SET handover_result='complete' WHERE id=?").run(seat);db.prepare("UPDATE sessions SET resume_token=? WHERE node_id=?").run(file,seat);guard.rebindLifecycle(seat);
   dispatchReservations.committed(r.reservation_id,operator.session,operator.generation);return{ok:true};
  }}) as any;
  integration=new ContextRefreshIntegration(deps);grant={...grant,targets:[{...target(),runtime:"pi",nativeId:file}]};
  return {agent,absent:()=>{present=false;},fail:()=>{failures=[{code:"compaction_failed",observedAt:new Date(now).toISOString()}];},
   compact:(id:string)=>{appendFileSync(file,JSON.stringify({type:"compaction",id,timestamp:new Date(now).toISOString(),summary:"Completed native summary",firstKeptEntryId:"model",tokensBefore:86})+"\n");leaf=id;used=null;}};
 }
 async function boundGrant(){await integration.grant(operator,grant);}
 async function checkpoint(){await boundGrant();return integration.recordCheckpoint(targetActor,{...selection(),packet});}
 beforeEach(()=>{
  vi.useFakeTimers({toFake:["Date"]});now=Date.UTC(2026,9,7,23);vi.setSystemTime(now);native=true;supervisor=true;activity="idle-at-prompt";activityAt=now;handoverCount=0;failHandover=false;
  dir=realpathSync(mkdtempSync(join(tmpdir(),"context-refresh-integration-")));file=join(dir,"history.jsonl");
  writeFileSync(file,line("session_meta",{id:"native-old"})+line("turn_context",{model:"gpt-6-luna"})+usage(),{mode:0o600});
  writeFileSync(join(dir,"refresh-profile.config.toml"),'model="gpt-6-luna"\nmodel_provider="openai"\nmodel_reasoning_effort="high"\n',{mode:0o600});
  writeFileSync(join(dir,"supervisor.js"),'// isolated installed entry\n',{mode:0o600});vi.stubEnv("CODEX_HOME",dir);
  db=createDb();seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(now).toISOString(),new Date(now).toISOString());
  db.prepare("UPDATE nodes SET runtime='codex',model='gpt-6-luna',codex_config_profile='refresh-profile'").run();
  db.prepare("UPDATE sessions SET status='running',startup_status='ready',resume_type='codex_id',resume_token='native-old'").run();
  db.prepare("UPDATE sessions SET resume_token='native-'||session_name WHERE node_id<>?").run(seat);
  for(const name of [seat,operator.session])db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES (?,?,?,?)").run(name,name,name,name===seat?'%2':'%1');
  guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));
  repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});
  outbox=new OutboxHandler(db);repo.attachOutbox(outbox);
  const recovery=new CoordinationRecoveryService(repo,()=>null,()=>now);repo.coordinatorAuthority.coordinationRecovery=recovery;
  const store=new NativeDutyLaunchStore({root:join(dir,"launches"),nodeExecutable:realpathSync(process.execPath),supervisorEntry:join(dir,"supervisor.js")});
  store.prepare({scopeId:"operator-launch-intent",launchId:"operator-launch",nodeId:operator.session,sessionName:operator.session,generation:operator.generation,runtime:"codex",configurationDigest:recovery.configurationDigest(operator.session)!,harness:{executable:"/fixture/codex",args:["--no-daemon"],cwd:dir},pollMs:1000});
  whoamiResolve=vi.fn(({nodeId}:any)=>{
   if(!nodeId)throw new Error("Exact node lookup required");
   const row=db.prepare("SELECT n.id nodeId,n.runtime,s.session_name sessionName,s.resume_token nativeId FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1").get(nodeId) as any;
   return row?{identity:{nodeId:row.nodeId,sessionName:row.sessionName,runtime:row.runtime},contextUsage:{sessionId:row.nativeId,transcriptPath:file,fresh:true}}:null;
  });
  deps={db,queue:repo,guard,store,rotationRoot:dir,configurationDigest:s=>recovery.configurationDigest(s),
   tmux:{getPanePid:async(pane:string)=>pane==='%1'?10:20} as any,
   whoami:{resolve:whoamiResolve} as any,
   activity:{pollSeat:async()=>{},getRotationActivityWitness:()=>({activity,observedAt:new Date(activityAt).toISOString()})} as any,piState:async()=>null,piProof:async()=>null,
   handoverFactory:({dispatchReservations})=>({handover:async(input:any)=>{
    handoverCount++;expect(guard.ownsLifecycle(seat)).toBe(true);
    const r=dispatchReservations.assertHandover(input.rotationExpected.reservationId,input.rotationExpected.operationId,seat,operator.session,operator.generation,input.rotationExpected);
    dispatchReservations.start(r,operator.session,operator.generation);
    if(failHandover)throw Error("native result unknown");
    // The adapter boundary is simulated; real reservation/tenure/receipt code is exercised.
    db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES('successor',?,2,'builder-g2','fresh')").run(seat);
    db.prepare("UPDATE nodes SET handover_result='complete' WHERE id=?").run(seat);db.prepare("UPDATE sessions SET resume_token='native-new' WHERE node_id=?").run(seat);guard.rebindLifecycle(seat);
    dispatchReservations.committed(r.reservation_id,operator.session,operator.generation);return{ok:true};
   }}) as any};
  vi.spyOn(processes,"listNativeProcesses").mockResolvedValue([{pid:10,ppid:1,command:"shell"},{pid:11,ppid:10,command:"supervisor",executableName:"node"}]);
  vi.spyOn(processes,"verifyCodexPaneProcess").mockImplementation(async()=>native?{process:{pid:21},fingerprint:"actual-native-fixture"} as any:null);
  vi.spyOn(launches,"verifyNativeDutyProcessIdentity").mockImplementation(async()=>native);
  vi.spyOn(launches,"observeNativeDutyLaunch").mockImplementation(async(_store,input,options)=>{
    const binding=await options.currentBinding(input.scope.nodeId);return supervisor&&binding?{...input.scope,supervisorPid:input.supervisorPid,observedAt:Date.now(),fingerprint:"fixture-supervisor-os",nativePresent:true,supervisorIsNativeAncestor:true,lifecycleReserved:false}:null;
  });
  vi.spyOn(rotation,"resolveCodexNativeState").mockImplementation(async(_deps,session)=>({who:{identity:{nodeId:session}},usage:{sessionId:(db.prepare("SELECT resume_token FROM sessions WHERE session_name=?").get(session) as any)?.resume_token,transcriptPath:file,fresh:true},runtimeContract:contract}) as any);
  integration=new ContextRefreshIntegration(deps);
  grant={grantId:"finite-refresh",kind:"context-refresh",executor:{...operator,nodeId:operator.session,launchId:"operator-launch",configurationDigest:deps.configurationDigest(operator.session)!},targets:[target()],policy:{...DEFAULT_CONTEXT_REFRESH_POLICY},policyRevision:"approved-v1",validUntil:now+3600000,validator:{session:"reviewer@xv",generation:"reviewer-g1"},recoveryOwner:operator};
 });
 afterEach(()=>{vi.restoreAllMocks();vi.unstubAllEnvs();vi.useRealTimers();db?.close();rmSync(dir,{recursive:true,force:true});});

 async function adoptContainedUnknownEffect(){
  const lead="lead@xv",peer="peer@xv",history=new HistoricalEffectDispositionService(db),body="valuable ongoing work";
  for(const name of [lead,peer])if(!db.prepare("SELECT 1 FROM bindings WHERE node_id=?").get(name))db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES(?,?,?,?)").run(name,name,name,"%3");
  await repo.create({qitemId:"refresh-baton",sourceSession:operator.session,destinationSession:lead,body:"coordinate",nudge:false,identityProvenance:"transport:v1"});
  repo.claim({qitemId:"refresh-baton",destinationSession:lead,actorGeneration:"lead-g1",identityProvenance:"transport:v1"});
  await repo.create({qitemId:"refresh-work",sourceSession:lead,destinationSession:seat,body,nudge:false,identityProvenance:"transport:v1"});
  outbox.record({outboxId:"wake-intent-refresh-contained",senderSession:lead,destinationSession:seat,body:"UNKNOWN retained input",auditPointer:"refresh-work"});outbox.markIndeterminate("wake-intent-refresh-contained");
  const plan:HistoricalPlan={rigId:"xv",leadBatonId:"refresh-baton",leadGeneration:"lead-g1",operatorGeneration:operator.generation,operationId:"refresh-hold",authorizationId:"refresh-hold-auth",expiresAt:now+600000,effects:history.inspect("xv",["wake-intent-refresh-contained"])};
  await repo.create({qitemId:plan.authorizationId,sourceSession:lead,destinationSession:operator.session,body:JSON.stringify({kind:"outbox-historical-quarantine-authorization",requestDigest:historicalDigest({actor:operator.session,generation:operator.generation,input:plan})}),nudge:false,identityProvenance:"transport:v1"});
  repo.claim({qitemId:plan.authorizationId,destinationSession:operator.session,actorGeneration:operator.generation,identityProvenance:"transport:v1"});history.quarantine(operator.session,operator.generation,plan);
  const inventory=repo.coordinatorAuthority.legacyInventory("xv","refresh-migration-auth",true),deadline=now+600000;
  await repo.create({qitemId:"refresh-recovery",sourceSession:lead,destinationSession:operator.session,body:JSON.stringify({kind:"coordinator-held-history-recovery.v1",rigId:"xv",operationId:"refresh-migration",owner:operator.session,generation:operator.generation,lead,leadGeneration:"lead-g1",effects:inventory.heldHistory!.map(h=>h.outboxId),action:"reconcile-preserved-unknown-history",deadline,returnPath:{session:lead,queueId:"refresh-recovery"}}),expiresAt:new Date(deadline).toISOString(),nudge:false,identityProvenance:"transport:v1"});
  repo.claim({qitemId:"refresh-recovery",destinationSession:operator.session,actorGeneration:operator.generation,identityProvenance:"transport:v1"});
  const currentInventory=repo.coordinatorAuthority.legacyInventory("xv","refresh-migration-auth",true),workContract={inputDigest:digest("refresh-inputs"),destination:seat,bodyHash:digest(body),resources:["source/a"],returnContract:{destination:lead,evidenceRequired:["tests"]}};
  const input:LegacyEnrollment={rigId:"xv",batonId:"refresh-baton",owner:lead,ownerGeneration:"lead-g1",coordinators:[lead,peer],leaseMs:60000,operationId:"refresh-migration",authorizationId:"refresh-migration-auth",inventory:currentInventory,heldHistoryRecovery:{queueId:"refresh-recovery",rowHash:historicalDigest(db.prepare("SELECT * FROM queue_items WHERE qitem_id='refresh-recovery'").get())},obligations:currentInventory.rows.map(q=>q.queueId==="refresh-work"?{queueId:q.queueId,kind:"work",evidenceRef:"exact-work-contract",packageKey:"refresh-work-package",resourceScope:"exclusive",contract:workContract}:{queueId:q.queueId,kind:"coordination",evidenceRef:"exact-control-return"})};
  await repo.create({qitemId:input.authorizationId,sourceSession:lead,destinationSession:operator.session,body:JSON.stringify({kind:"coordinator-legacy-enrollment",proposalDigest:legacyProposalDigest(input)}),nudge:false,identityProvenance:"transport:v1"});
  repo.claim({qitemId:input.authorizationId,destinationSession:operator.session,actorGeneration:operator.generation,identityProvenance:"transport:v1"});
  repo.coordinatorAuthority.migrateLegacy(operator.session,operator.generation,input);
  repo.coordinatorAuthority.acknowledge(lead,token,{operationId:"refresh-ack",obligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv")});
  const row=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-refresh-contained'").get() as Record<string,unknown>;
  expect(repo.coordinatorAuthority.isAdoptedHistoryContained("xv",row)).toBe(true);
  return row;
 }

 it("history lookup remains bound to the current node when another rig reuses its session name",async()=>{
  db.prepare("UPDATE sessions SET session_name=? WHERE node_id='worker@other'").run(seat);
  await boundGrant();const observed=await integration.observe(operator,selection());
  expect(observed.identity).toMatchObject({nodeId:seat,sessionName:seat,nativeId:"native-old"});
  expect(observed.holds).not.toContain("identity-unavailable");
  expect(whoamiResolve).toHaveBeenCalledWith({nodeId:seat,compact:false});
 });
 it.each(["node","session"])("history lookup rejects a mismatched Whoami %s",async kind=>{
  await boundGrant();whoamiResolve.mockReturnValue({identity:{nodeId:kind==="node"?"worker@other":seat,
    sessionName:kind==="session"?"different-session":seat,runtime:"codex"},contextUsage:{sessionId:"native-old",transcriptPath:file,fresh:true}});
  const observed=await integration.observe(operator,selection());expect(observed.holds).toContain("identity-unavailable");
 });
 it("derives actual native model/window and raw usage timestamp with current OS and activity proof",async()=>{
  await boundGrant();const observed=await integration.observe(operator,selection());expect(observed.holds).toEqual([]);expect(observed.usage).toMatchObject({usedPercent:86,observedAt:now});
  now+=121000;vi.setSystemTime(now);activityAt=now;
  const aged=await integration.observe(operator,selection());expect(aged.usage!.observedAt).toBe(now-121000);expect(aged.holds).toContain("usage-stale");
 });
 it("uses actual CR2 successful-compaction delta without post-compaction usage and holds below threshold",async()=>{
  await boundGrant();expect((await integration.evaluate(operator,selection())).baselineCompactions).toBe(0);
  compact();expect(await integration.evaluate(operator,selection())).toMatchObject({action:"none",compactionsSinceBaseline:1,holds:["usage-unavailable"]});
  compact();expect(await integration.evaluate(operator,selection())).toMatchObject({action:"request-checkpoint",compactionsSinceBaseline:2,rotateThreshold:true,holds:["checkpoint-required"]});
  activity="working";expect((await integration.evaluate(operator,selection())).holds).toContain("busy");
 });
 it("requires real current supervised Operator and keeps Pi rotation unsupported",async()=>{
  supervisor=false;await expect(boundGrant()).rejects.toThrow("ancestry");supervisor=true;await boundGrant();
  expect(await integration.enrollment(operator,{launchId:"wrong",supervisorPid:11})).toMatchObject({state:"held"});
  expect(await integration.enrollment(operator,{launchId:"operator-launch",supervisorPid:11})).toMatchObject({state:"ready"});
  await expect(integration.enrollment(targetActor,{launchId:"operator-launch",supervisorPid:11})).rejects.toThrow();
  db.prepare("UPDATE nodes SET runtime='pi' WHERE id=?").run(seat);expect((await integration.observe(operator,selection())).capability).toBe("unsupported");
 });
 it("issues only fixed finite administrative checkpoint through an enrolled worker while ordinary assignment remains fenced",async()=>{
  await repo.create({qitemId:"baton",sourceSession:operator.session,destinationSession:"lead@xv",body:"coordinate",nudge:false});
  repo.coordinatorAuthority.enable(operator.session,operator.generation,{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:60000,operationId:"enable"});
  repo.coordinatorAuthority.acknowledge("lead@xv",token,{operationId:"ack",obligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv")});
  await expect(repo.create({sourceSession:operator.session,destinationSession:seat,body:"generic bypass",nudge:false})).rejects.toThrow();
  await boundGrant();const outcome=await integration.step(operator,{...selection(),operationId:"step-checkpoint"});expect(outcome.effect).toBe("checkpoint");expect(outcome.checkpointRequest?.phase).toBe("receipt-confirmed");
  const q=repo.getById(outcome.checkpointRequest!.qitemId)!;expect(q.sourceSession).toBe(operator.session);expect(q.expiresAt).toBe(new Date(grant.validUntil).toISOString());
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual({n:0});
  const replay=await integration.step(operator,{...selection(),operationId:"step-checkpoint"});expect(replay.effect).toBe("none");
  expect(await integration.reconcile(operator,{...selection(),operationId:"step-checkpoint"})).toMatchObject({invocation:{operationId:"step-checkpoint",state:"completed"}});
  await expect(repo.createContextRefreshCheckpoint(outcome.checkpointRequest!.operationId)).rejects.toThrow();
 });
 it("freezes target-authored history then reserves, hands over and requires actual successor ACK plus independent acceptance",async()=>{
  await checkpoint();
  const reserved=await integration.step(operator,{...selection(),operationId:"reserve-call"});
  expect(JSON.parse(readFileSync(join(dir,"frozen",reserved.attempt!.checkpoint.checkpointId+".json"),"utf8")).receipt.packet).toEqual(packet);expect(reserved.attempt?.phase).toBe("reserved");expect(handoverCount).toBe(0);
  const committed=await integration.step(operator,{...selection(),operationId:"handover-call"});expect(committed.attempt?.phase).toBe("committed-awaiting-acceptance");expect(handoverCount).toBe(1);
  await expect(integration.step(operator,{...selection(),operationId:"too-soon-release"})).rejects.toThrow("acceptance");
  const attemptId=committed.attempt!.attemptId;
  await integration.attest({session:seat,generation:"builder-g2"},{attemptId,kind:"successor_ack",evidenceRef:"actual-successor-readback"});
  await expect(integration.attest(operator,{attemptId,kind:"independent_acceptance",evidenceRef:"not-independent"})).rejects.toThrow("validator");
  await integration.attest(grant.validator,{attemptId,kind:"independent_acceptance",evidenceRef:"independent-readback"});
  expect((await integration.step(operator,{...selection(),operationId:"release-call"})).attempt?.phase).toBe("refreshed");
  expect((await integration.reconcile(operator,{...selection(),operationId:"release-call"})).invocation?.state).toBe("completed");expect(handoverCount).toBe(1);
 });
 it("retains started uncertain native outcomes and never replays the handover",async()=>{
  await checkpoint();await integration.step(operator,{...selection(),operationId:"reserve-call"});failHandover=true;
  await expect(integration.step(operator,{...selection(),operationId:"lost-handover"})).rejects.toThrow("unknown");
  expect((await integration.status(operator,{...selection(),operationId:"lost-handover"})).invocation?.state).toBe("completed");
  expect((await integration.step(operator,{...selection(),operationId:"next-poll"})).effect).toBe("none");expect(handoverCount).toBe(1);
  expect(db.prepare("SELECT state FROM seat_dispatch_reservations").get()).toEqual({state:"started"});
  expect(db.prepare("SELECT count(*) n FROM context_refresh_events WHERE operation_id='lost-handover' AND phase='step-cancelled-before-invocation'").get()).toEqual({n:0});
 });
 it("holds expired grant, model/config drift and typing guard before any permit",async()=>{
  await boundGrant();now=grant.validUntil;vi.setSystemTime(now);activityAt=now;expect((await integration.evaluate(operator,selection())).phase).toBe("scope-ended");
  expect(db.prepare("SELECT count(*) n FROM context_refresh_attempts").get()).toEqual({n:0});
  now-=1000;vi.setSystemTime(now);activityAt=now;await guard.set(seat,true,"operator","hold");expect((await integration.observe(operator,selection())).holds).toContain("typing-guard-enabled");
  await guard.set(seat,false,"operator","release");db.prepare("UPDATE nodes SET model='different' WHERE id=?").run(seat);expect((await integration.evaluate(operator,selection())).holds).toContain("binding-changed");
 });
 it("preserves unrelated UNKNOWN and authority custody as explicit holds",async()=>{
  await boundGrant();db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,delivery_state,ts_dispatched) VALUES('unknown',?,?,'private retained','indeterminate',?)").run(seat,operator.session,new Date(now).toISOString());
  expect((await integration.evaluate(operator,selection())).holds).toContain("effects-unresolved");
  expect(db.prepare("SELECT delivery_state,body FROM outbox_entries WHERE outbox_id='unknown'").get()).toEqual({delivery_state:"indeterminate",body:"private retained"});
 });
 it("accepts only exact adopted UNKNOWN containment when checking effects debt",async()=>{
  const original=await adoptContainedUnknownEffect();
  db.prepare("INSERT INTO nodes(id,rig_id,logical_id) VALUES ('retained-builder','other','builder')").run();
  db.prepare("INSERT INTO sessions(id,node_id,session_name) VALUES ('zz-retained-builder','retained-builder',?)").run(seat);
  db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES ('retained-builder-tenure','retained-builder',99,'retained-builder-g1','fresh')").run();
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('retained-builder-binding','retained-builder',?,'%99')").run(seat);
  db.prepare("UPDATE rigs SET archived_at=? WHERE id='other'").run(new Date(now).toISOString());
  expect(resolveGuardTarget(db,seat)).toMatchObject({nodeId:seat,session:seat,occupant:"builder-g1"});
  await boundGrant();
  expect((await integration.observe(operator,selection())).holds).not.toContain("effects-unresolved");
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get(original.outbox_id)).toEqual(original);
  outbox.record({outboxId:"wake-intent-refresh-mismatch",senderSession:"lead@xv",destinationSession:seat,body:"separate UNKNOWN input",auditPointer:"refresh-work"});outbox.markIndeterminate("wake-intent-refresh-mismatch");
  const unknownBefore=db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-refresh-mismatch'").get();
  expect((await integration.observe(operator,selection())).holds).toContain("effects-unresolved");
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id=?").get(original.outbox_id)).toEqual(original);
  expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-refresh-mismatch'").get()).toEqual(unknownBefore);
 });
 it("holds exact unfinished invocation debt across new IDs and grants after a server crash",async()=>{
  await boundGrant();const input={...selection(),operationId:"crashed-server-invocation"};
  db.prepare("INSERT INTO context_refresh_events(grant_id,node_id,operation_id,phase,evidence_digest,observed_at) VALUES(?,?,?,'step-invocation',?,?)").run(grant.grantId,seat,input.operationId,contextRefreshDigest({actor:operator,input}),now);
  expect(await integration.status(operator,input)).toMatchObject({invocation:{operationId:input.operationId,state:"in-flight"}});
  expect((await integration.step(operator,input)).effect).toBe("none");
  expect((await integration.status(operator,input)).invocation?.state).toBe("in-flight");
  await integration.reconcile(operator,input);
  expect((await integration.status(operator,input)).invocation?.state).toBe("in-flight");
  expect(db.prepare("SELECT count(*) n FROM context_refresh_events WHERE operation_id=? AND phase='step-cancelled-before-invocation'").get(input.operationId)).toEqual({n:0});
  await expect(integration.step(operator,{...selection(),operationId:"bypass-attempt"})).rejects.toThrow("unfinished invocation");
  await expect(integration.grant(operator,{...grant,grantId:"bypass-grant"})).rejects.toThrow("unfinished invocation");
  expect(db.prepare("SELECT count(*) n FROM context_refresh_checkpoint_requests").get()).toEqual({n:0});
 });
 it("records a guard refusal before lifecycle acquisition and does not create an effect",async()=>{
  await boundGrant();await guard.set(seat,true,"fixture","pre-effect guard refusal");
  const input={...selection(),operationId:"guard-refusal-invocation"};
  await expect(integration.step(operator,input)).rejects.toThrow();
  expect(await integration.status(operator,input)).toMatchObject({invocation:{operationId:input.operationId,state:"completed"}});
  expect(db.prepare("SELECT phase FROM context_refresh_events WHERE operation_id=? ORDER BY id").all(input.operationId)).toEqual([
   {phase:"step-invocation"},{phase:"step-completed"}
  ]);
  expect(db.prepare("SELECT count(*) n FROM context_refresh_attempts").get()).toEqual({n:0});
  expect(db.prepare("SELECT count(*) n FROM context_refresh_checkpoint_requests").get()).toEqual({n:0});
  expect(handoverCount).toBe(0);
 });
 it("turns an exact missing invocation into a no-effect tombstone that a late step cannot replay",async()=>{
  await boundGrant();const input={...selection(),operationId:"missing-invocation-reconciled"};
  expect((await integration.status(operator,input)).invocation).toBeNull();
  expect(await integration.reconcile(operator,input)).toMatchObject({invocation:{operationId:input.operationId,state:"completed"}});
  expect(db.prepare("SELECT phase FROM context_refresh_events WHERE operation_id=? ORDER BY id").all(input.operationId)).toEqual([
   {phase:"step-invocation"},{phase:"step-cancelled-before-invocation"},{phase:"step-completed"}
  ]);
  expect((await integration.step(operator,input)).effect).toBe("none");
  expect(db.prepare("SELECT count(*) n FROM context_refresh_attempts").get()).toEqual({n:0});
  expect(db.prepare("SELECT count(*) n FROM context_refresh_checkpoint_requests").get()).toEqual({n:0});
  expect(handoverCount).toBe(0);
 });
 it("lets exact reconciliation cancel a request waiting before claim and fences its late continuation",async()=>{
  await boundGrant();const input={...selection(),operationId:"concurrent-missing-invocation"};
  const original=integration.status.bind(integration);let blocked=false,entered!:()=>void,release!:()=>void;
  const atStatus=new Promise<void>(resolve=>{entered=resolve;}),barrier=new Promise<void>(resolve=>{release=resolve;});
  vi.spyOn(integration,"status").mockImplementation(async(actor,inputArg)=>{
   if(inputArg.operationId===input.operationId&&!blocked){blocked=true;entered();await barrier;}
   return original(actor,inputArg);
  });
  const lateStep=integration.step(operator,input);await atStatus;
  const reconciled=await integration.reconcile(operator,input);
  expect(reconciled.invocation).toMatchObject({operationId:input.operationId,state:"completed"});
  release();expect((await lateStep).effect).toBe("none");
  expect(db.prepare("SELECT phase FROM context_refresh_events WHERE operation_id=? ORDER BY id").all(input.operationId)).toEqual([
   {phase:"step-invocation"},{phase:"step-cancelled-before-invocation"},{phase:"step-completed"}
  ]);
  expect(db.prepare("SELECT count(*) n FROM context_refresh_attempts").get()).toEqual({n:0});
  expect(db.prepare("SELECT count(*) n FROM context_refresh_checkpoint_requests").get()).toEqual({n:0});
  expect(handoverCount).toBe(0);
 });
 it("refuses another actor and a conflicting immutable operation instead of tombstoning",async()=>{
  await boundGrant();const input={...selection(),operationId:"operation-conflict-no-tombstone"};
  await expect(integration.reconcile({session:"other-operator",generation:"other-g1"},input)).rejects.toThrow();
  expect(db.prepare("SELECT count(*) n FROM context_refresh_events WHERE operation_id=?").get(input.operationId)).toEqual({n:0});
  const foreignActor={session:"other-operator",generation:"other-g1"};
  db.prepare("INSERT INTO context_refresh_events(grant_id,node_id,operation_id,phase,evidence_digest,observed_at) VALUES(?,?,?,'step-invocation',?,?)")
   .run(grant.grantId,seat,input.operationId,contextRefreshDigest({actor:foreignActor,input}),now);
  await expect(integration.reconcile(operator,input)).rejects.toThrow("Step invocation ID is immutable");
  expect(db.prepare("SELECT phase FROM context_refresh_events WHERE operation_id=? ORDER BY id").all(input.operationId)).toEqual([{phase:"step-invocation"}]);
 });
 it("reconciles a lost actual queue response without repeating creation or its wake",async()=>{
  await boundGrant();const create=repo.createContextRefreshCheckpoint.bind(repo);
  const effect=vi.spyOn(repo,"createContextRefreshCheckpoint").mockImplementation(async op=>{await create(op);throw Error("response lost");});
  const input={...selection(),operationId:"lost-checkpoint-response"};
  await expect(integration.step(operator,input)).rejects.toThrow("response lost");
  expect(await integration.reconcile(operator,input)).toMatchObject({invocation:{operationId:input.operationId,state:"completed"},checkpointRequest:{phase:"receipt-confirmed"}});
  expect((await integration.step(operator,input)).effect).toBe("none");expect(effect).toHaveBeenCalledTimes(1);
 });

 it("retains a genuine busy target draft, exposes target readback and freezes only on a later actual idle step",async()=>{
  await boundGrant();activity="working";
  const draft=await integration.submitCheckpoint(targetActor,{...selection(),packet});
  expect(draft.phase).toBe("submitted");
  expect((await integration.submitCheckpoint(targetActor,{...selection(),packet})).draftId).toBe(draft.draftId);
  await expect(integration.submitCheckpoint(targetActor,{...selection(),packet:{...packet,next_action:"different"}})).rejects.toThrow("different immutable draft");
  expect(await integration.status(targetActor,selection())).toMatchObject({checkpointDraft:{draftId:draft.draftId,phase:"submitted"}});
  expect(db.prepare("SELECT count(*) n FROM context_refresh_events WHERE phase='checkpoint-authored'").get()).toEqual({n:0});
  expect((await integration.step(operator,{...selection(),operationId:"still-busy"})).effect).toBe("none");
  expect(db.prepare("SELECT count(*) n FROM context_refresh_attempts").get()).toEqual({n:0});
  await expect(integration.submitCheckpoint(operator,{...selection(),packet})).rejects.toThrow("genuine current target");
  activity="idle-at-prompt";
  const frozen=await integration.step(operator,{...selection(),operationId:"now-idle"});
  expect(frozen.effect).toBe("reserve");expect(frozen.attempt!.checkpoint.authoredBy).toEqual(targetActor);
  expect(await integration.status(targetActor,selection())).toMatchObject({checkpointDraft:{draftId:draft.draftId,phase:"frozen"}});
 });
 it("Pi genuine busy draft promotes on idle and real reservation requires fresh successor plus ACK and independent acceptance",async()=>{
  piTarget();const predecessor=file,before=readFileSync(file);await boundGrant();
  expect(await integration.observe(operator,selection())).toMatchObject({capability:"pi-reserved-fresh",usage:{usedPercent:86},holds:[]});
  activity="working";const draft=await integration.submitCheckpoint(targetActor,{...selection(),packet});expect(draft.phase).toBe("submitted");
  expect((await integration.step(operator,{...selection(),operationId:"pi-busy"})).effect).toBe("none");
  activity="idle-at-prompt";const reserved=await integration.step(operator,{...selection(),operationId:"pi-reserve"});
  expect(reserved.effect).toBe("reserve");expect(reserved.attempt?.phase).toBe("reserved");expect(reserved.attempt?.checkpoint.authoredBy).toEqual(targetActor);
  const started=await integration.step(operator,{...selection(),operationId:"pi-handover"});expect(started.effect).toBe("handover");
  expect(started.attempt?.phase).toBe("committed-awaiting-acceptance");expect(readFileSync(predecessor)).toEqual(before);expect(file).not.toBe(predecessor);
  await expect(integration.step(operator,{...selection(),operationId:"pi-no-ack"})).rejects.toThrow("Actual successor ACK");
  await integration.attest({session:seat,generation:"builder-g2"},{attemptId:reserved.attempt!.attemptId,kind:"successor_ack",evidenceRef:"fixture:actual-successor"});
  await integration.attest({session:"reviewer@xv",generation:"reviewer-g1"},{attemptId:reserved.attempt!.attemptId,kind:"independent_acceptance",evidenceRef:"fixture:independent"});
  const done=await integration.step(operator,{...selection(),operationId:"pi-release"});expect(done.effect).toBe("release");expect(done.attempt?.phase).toBe("refreshed");
  expect(handoverCount).toBe(1);
 });
 it("Pi actual two completed compactions trigger without usage; missing OS binding, header replacement and native failures remain held",async()=>{
  const p=piTarget();await boundGrant();expect((await integration.evaluate(operator,selection())).baselineCompactions).toBe(0);
  p.compact("first");expect((await integration.evaluate(operator,selection())).action).toBe("none");
  p.compact("second");expect(await integration.evaluate(operator,selection())).toMatchObject({action:"request-checkpoint",compactionsSinceBaseline:2});
  p.fail();expect((await integration.evaluate(operator,selection())).holds).toContain("runtime-not-ready");
  p.absent();expect((await integration.observe(operator,selection())).native.verified).toBe(false);
 });
 it("Pi effective posture inherits rig/default policy and respects explicit member precedence",async()=>{
  piTarget();
  db.prepare("UPDATE nodes SET policy_launch_posture=NULL,policy_origin=NULL WHERE id=?").run(seat);
  expect((await piFacts.resolvePiRotationNativeState(deps,seat)).runtimeContract.trust).toBe("no-approve");
  const rig=(db.prepare("SELECT rig_id rig FROM nodes WHERE id=?").get(seat) as {rig:string}).rig;
  db.prepare("UPDATE rigs SET rig_policy_origin='builtin',rig_policy_launch_posture='full_bypass' WHERE id=?").run(rig);
  await expect(piFacts.resolvePiRotationNativeState(deps,seat)).rejects.toThrow("binding-changed"); // Actual no-approve cannot prove bypass.
  db.prepare("UPDATE nodes SET policy_origin='builtin',policy_launch_posture='floor' WHERE id=?").run(seat);
  expect((await piFacts.resolvePiRotationNativeState(deps,seat)).runtimeContract.trust).toBe("no-approve");
  db.prepare("UPDATE nodes SET policy_launch_posture=NULL,policy_origin=NULL WHERE id=?").run(seat);
  db.prepare("UPDATE rigs SET rig_policy_launch_posture='floor' WHERE id=?").run(rig);
  expect((await piFacts.resolvePiRotationNativeState(deps,seat)).runtimeContract.trust).toBe("no-approve");
 });
 it("Pi effective posture changes during identity observation refuse and missing/invalid resolver cannot upgrade",async()=>{
  piTarget();const resolve=deps.piRotation!.resolvePosture;let calls=0;
  deps.piRotation!.resolvePosture=(nodeId,rigId)=>++calls===1?resolve(nodeId,rigId):"full_bypass";
  await expect(piFacts.resolvePiRotationNativeState(deps,seat)).rejects.toThrow("binding changed");
  deps.piRotation!.resolvePosture=undefined as any;
  await expect(piFacts.resolvePiRotationNativeState(deps,seat)).rejects.toThrow("effective launch policy unavailable");
  deps.piRotation!.resolvePosture=()=>"invalid" as any;
  await expect(piFacts.resolvePiRotationNativeState(deps,seat)).rejects.toThrow("effective launch policy unavailable");
 });
 it("Pi canonical file stays mandatory while defaults do not override native selection",async()=>{
  const p=piTarget();await boundGrant();
  writeFileSync(join(p.agent,"settings.json"),JSON.stringify({defaultProvider:"provider",defaultModel:"model",defaultThinkingLevel:"low"}));
  expect((await integration.observe(operator,selection())).capability).toBe("pi-reserved-fresh");
  const link=join(dir,"aliased-pi.jsonl");symlinkSync(file,link);db.prepare("UPDATE sessions SET resume_token=? WHERE node_id=?").run(link,seat);
  await expect(piFacts.resolvePiRotationNativeState(deps,seat)).rejects.toThrow("canonical Pi");
  expect(db.prepare("SELECT count(*) n FROM context_refresh_attempts").get()).toEqual({n:0});
 });
 it("Pi changed retained predecessor history or launch configuration keeps successor acceptance fenced",async()=>{
  const p=piTarget(),old=file;await checkpoint();const r=await integration.step(operator,{...selection(),operationId:"pi-freeze"});
  await integration.step(operator,{...selection(),operationId:"pi-change"});
  const retained=readFileSync(old),alias=join(dir,"old-copy.jsonl");writeFileSync(alias,retained);rmSync(old);symlinkSync(alias,old);
  await expect(integration.attest({session:seat,generation:"builder-g2"},{attemptId:r.attempt!.attemptId,kind:"successor_ack",evidenceRef:"fixture:alias"})).rejects.toThrow("continuity");
  rmSync(old);writeFileSync(old,retained);appendFileSync(old,'{"retained":"tampered"}\n');
  await expect(integration.attest({session:seat,generation:"builder-g2"},{attemptId:r.attempt!.attemptId,kind:"successor_ack",evidenceRef:"fixture:unacceptable"})).rejects.toThrow("continuity");
  expect(db.prepare("SELECT state FROM seat_dispatch_reservations").get()).toEqual({state:"committed"});
  writeFileSync(join(p.agent,"models.json"),'{"changed":true}');
  expect((await integration.reconcile(operator,selection())).attempt?.phase).toBe("uncertainty-held");
 });

 it("Pi administrative checkpoint uses actual canonical FILE binding through the existing enrolled-worker fence",async()=>{
  piTarget();await repo.create({qitemId:"pi-baton",sourceSession:operator.session,destinationSession:"lead@xv",body:"coordinate",nudge:false});
  repo.coordinatorAuthority.enable(operator.session,operator.generation,{rigId:"xv",batonId:"pi-baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:60000,operationId:"pi-enable"});
  repo.coordinatorAuthority.acknowledge("lead@xv",token,{operationId:"pi-ack",obligationsDigest:repo.coordinatorAuthority.reconciliationDigest("xv")});
  await expect(repo.create({sourceSession:operator.session,destinationSession:seat,body:"generic bypass",nudge:false})).rejects.toThrow();
  await boundGrant();const result=await integration.step(operator,{...selection(),operationId:"pi-prepare"});expect(result.effect).toBe("checkpoint");
  expect(repo.getById(result.checkpointRequest!.qitemId)).toMatchObject({sourceSession:operator.session,destinationSession:seat,expiresAt:new Date(grant.validUntil).toISOString()});
  expect(db.prepare("SELECT count(*) n FROM coordinator_assignments").get()).toEqual({n:0});
 });
 it("Pi claimed coordinator authority remains an explicit transfer prerequisite",async()=>{
  piTarget();await repo.create({qitemId:"pi-owner-baton",sourceSession:operator.session,destinationSession:seat,body:"coordinate",nudge:false});
  repo.coordinatorAuthority.enable(operator.session,operator.generation,{rigId:"xv",batonId:"pi-owner-baton",owner:seat,ownerGeneration:targetActor.generation,coordinators:[seat,"peer@xv"],leaseMs:60000,operationId:"pi-owner-enable"});
  await boundGrant();expect((await integration.evaluate(operator,selection())).holds).toContain("authority-transfer-required");
  expect((await integration.step(operator,{...selection(),operationId:"pi-owner-held"})).effect).toBe("none");expect(handoverCount).toBe(0);
 });
 it("Pi UNKNOWN after durable start never replays a native handover or grants successor acceptance",async()=>{
  piTarget();await checkpoint();await integration.step(operator,{...selection(),operationId:"pi-unknown-reserve"});failHandover=true;
  await expect(integration.step(operator,{...selection(),operationId:"pi-unknown-start"})).rejects.toThrow("unknown");
  expect((await integration.step(operator,{...selection(),operationId:"pi-never-retry"})).effect).toBe("none");
  expect(db.prepare("SELECT state,successor_generation FROM seat_dispatch_reservations").get()).toEqual({state:"started",successor_generation:null});expect(handoverCount).toBe(1);
 });

 it("does not promote another grant's retained draft",async()=>{
  await checkpoint();const second={...grant,grantId:"distinct-grant"};await integration.grant(operator,second);
  expect(await integration.status(targetActor,{grantId:second.grantId,nodeId:seat})).toMatchObject({checkpointDraft:null});
  expect(await integration.evaluate(operator,{grantId:second.grantId,nodeId:seat})).toMatchObject({action:"request-checkpoint"});
 });

});
