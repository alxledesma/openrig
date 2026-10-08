import { createHash } from "node:crypto";
import type Database from "better-sqlite3";
import type { EventBus } from "./event-bus.js";
import { NativeRecoveryCompletionStore, type NativeRecoveryCompletionPublisher } from "./native-recovery-completion.js";
import type { NativeRecoveryCompletion, NativeRecoveryContinuationRuntime, NativeRecoveryGuardResult, NativeRecoveryObservation, NativeSettledObservation } from "./native-recovery-continuation-contract.js";
import { readPinnedLegacyPiRecovery, type PinnedLegacyRecovery, type LegacyPiRecovery } from "./native-recovery-legacy.js";
import { nativeRecoverySourceValid } from "./native-recovery-source-proof.js";
import { observeNativeDutyLaunch, verifyNativeDutyProcessIdentity, type NativeDutyLaunchStore } from "./native-duty-launch.js";
import { listNativeProcesses } from "./native-process-lineage.js";
import { DeliveryGuardError, type SeatDeliveryGuard } from "./seat-delivery-guard.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";

const digest=(value:string)=>createHash("sha256").update(value).digest("hex");
const same=(a:unknown,b:unknown):boolean=>JSON.stringify(a)===JSON.stringify(b);

/** Reuses the native-duty kernel observer and the ordinary delivery serialization
 * domain. A receipt never grants delivery, claim, or recovery authority. */
export function createNativeRecoveryContinuation(deps:{
  db:Database.Database; eventBus:EventBus; guard:SeatDeliveryGuard;
  store:NativeDutyLaunchStore|null|undefined; tmux:{getPanePid(pane:string):Promise<number|null>};
  piProve:(session:string)=>Promise<PiNativeProof|null>;
  configurationDigest:(session:string)=>string|null|undefined;
  legacyReceipts?:PinnedLegacyRecovery[];
  roots:{piDetached:string;codex:string}; enabled?:(nodeId:string)=>boolean; now?:()=>number;
}):NativeRecoveryContinuationRuntime&{recordNativeRecoveryCompletion:NativeRecoveryCompletionPublisher} {
  const completions=new NativeRecoveryCompletionStore(deps.eventBus),now=deps.now??Date.now;
  const binding=(session:string)=>{
    const target=deps.guard.maybeTarget(session);
    if(!target?.occupant||!target.pane||target.session!==session)return null;
    const row=deps.db.prepare("SELECT s.id,s.resume_token,n.rig_id,n.runtime FROM sessions s JOIN nodes n ON n.id=s.node_id WHERE s.node_id=? ORDER BY s.id DESC LIMIT 1").get(target.nodeId) as {id:string;resume_token:string|null;rig_id:string;runtime:string}|undefined;
    const configurationDigest=deps.configurationDigest(session);
    if(!row?.resume_token||!configurationDigest||(row.runtime!=="pi"&&row.runtime!=="codex"))return null;
    return {nodeId:target.nodeId,sessionName:session,generation:target.occupant,pane:target.pane,
      sessionId:row.id,rigId:row.rig_id,runtime:row.runtime as "pi"|"codex",resumeToken:row.resume_token,
      nativeIdentityHash:digest(row.resume_token),configurationDigest};
  };
  const observe=async(session:string,producing=false)=>{
    const b=binding(session),store=deps.store;if(!b||!store||(deps.enabled&&!deps.enabled(b.nodeId)))return null;
    // Only the recovery's existing async lease may observe its own replacement
    // while its typing guard remains on. Consumer observations get no exemption.
    const currentBinding=async(nodeId:string)=>{
      const current=binding(session);if(!current||current.nodeId!==nodeId)return null;
      const own=producing&&deps.guard.ownsRunnerRehost(nodeId);
      return {...current,lifecycleReserved:!own&&(deps.guard.lifecycleActive(nodeId)||deps.guard.protectionFacts(nodeId)!==null)};
    };
    const latest=store.latest(b.nodeId,b.generation);if(!latest||latest.intent.configurationDigest!==b.configurationDigest)return null;
    const panePid=await deps.tmux.getPanePid(b.pane);if(!panePid)return null;
    const rows=await listNativeProcesses(),candidates:number[]=[];
    const byPid=new Map(rows.map(row=>[row.pid,row]));
    const inPane=(pid:number)=>{const seen=new Set<number>();while(!seen.has(pid)){if(pid===panePid)return true;seen.add(pid);const row=byPid.get(pid);if(!row)return false;pid=row.ppid;}return false;};
    const expected={OPENRIG_NODE_ID:b.nodeId,OPENRIG_SESSION_NAME:b.sessionName,OPENRIG_OCCUPANT_GENERATION:b.generation,OPENRIG_RUNTIME:b.runtime};
    for(const row of rows){
      if(row.executableName!=="node"||!inPane(row.pid))continue;
      if(await verifyNativeDutyProcessIdentity(row.pid,expected,[latest.intent.installedNode.path,latest.intent.installedSupervisor.path,"--supervise",latest.intent.configPath]))candidates.push(row.pid);
    }
    if(candidates.length!==1)return null;
    const proof=await observeNativeDutyLaunch(store,{scope:latest.intent,launchId:latest.intent.launchId,supervisorPid:candidates[0]!},{currentBinding,tmux:deps.tmux,piProve:deps.piProve});
    if(!proof?.processIdentity||!same(b,binding(session)))return null;
    const processes=proof.processIdentity;
    if(b.runtime==="pi"&&!processes.runtimeLaunchId)return null;
    const fields={native:processes.native,supervisor:processes.supervisor,supervisorLaunchId:proof.launchId,
      ...(processes.runtimeLaunchId?{runtimeLaunchId:processes.runtimeLaunchId}:{})};
    const incarnation={key:digest(JSON.stringify({nodeId:b.nodeId,generation:b.generation,nativeIdentityHash:b.nativeIdentityHash,...fields})),...fields};
    return {binding:b,incarnation,proof};
  };
  // A settled turn does not require a recovery receipt. Pair native kernel
  // identity with two cursor/quiescence samples; quiet refresh timestamps are
  // freshness evidence only and never part of the turn/incarnation identity.
  const settled = async (session:string):Promise<NativeSettledObservation|null> => {
    const b=binding(session);if(!b||b.runtime!=="pi")return null;
    const before=await deps.piProve(session);
    const current=await observe(session);if(!current)return null;
    const after=await deps.piProve(session),at=now();
    const fresh=(p:PiNativeProof|null)=>{
      const qAt=Date.parse(p?.quiescence?.observedAt??"");
      // The prover fingerprint binds its native runner launch independently of
      // the sidecar's launch field. Never accept contradictory proof metadata.
      const nativeLaunch=p?.state==="present"?JSON.parse(p.fingerprint).launchId:null;
      return p?.state==="present"&&p.generation===b.generation
        &&p.launchId===nativeLaunch&&p.launchId===current.incarnation.runtimeLaunchId&&p.fingerprint===current.proof.nativeFingerprint
        &&typeof p.lastEntryId==="string"&&p.lastEntryId.trim().length>0
        &&p.quiescence?.settled===true&&Number.isFinite(qAt)&&qAt<=at&&at-qAt<=3000;
    };
    if(!fresh(before)||!fresh(after)||before!.lastEntryId!==after!.lastEntryId
      ||!same(b,current.binding)||!same(b,binding(session)))return null;
    const latest=deps.store?.latest(b.nodeId,b.generation);
    if(!latest||latest.intent.launchId!==current.incarnation.supervisorLaunchId
      ||latest.intent.configurationDigest!==b.configurationDigest)return null;
    return {schema:"native-settled-observation.v1",rigId:b.rigId,nodeId:b.nodeId,sessionId:b.sessionId,
      sessionName:b.sessionName,generation:b.generation,runtime:b.runtime,nativeIdentityHash:b.nativeIdentityHash,
      configurationDigest:b.configurationDigest,incarnation:current.incarnation,lastEntryId:after!.lastEntryId!,
      quiescenceObservedAt:Date.parse(after!.quiescence!.observedAt!),observedAt:at};
  };
  const settledBindingMatches=(o:NativeSettledObservation,b:ReturnType<typeof binding>)=>
    !!b&&o?.schema==="native-settled-observation.v1"&&o.rigId===b.rigId&&o.nodeId===b.nodeId
      &&o.sessionId===b.sessionId&&o.sessionName===b.sessionName&&o.generation===b.generation&&o.runtime===b.runtime
      &&o.nativeIdentityHash===b.nativeIdentityHash&&o.configurationDigest===b.configurationDigest;
  const matches=(completion:NativeRecoveryCompletion,current:NonNullable<Awaited<ReturnType<typeof observe>>>)=>{
    const b=current.binding;
    return completion.nodeId===b.nodeId&&completion.sessionId===b.sessionId&&completion.sessionName===b.sessionName
      &&completion.rigId===b.rigId&&completion.generation===b.generation&&completion.runtime===b.runtime
      &&completion.nativeIdentityHash===b.nativeIdentityHash&&completion.configurationDigest===b.configurationDigest
      &&completion.incarnation.key===current.incarnation.key;
  };
  const legacyMatches=(c:NativeRecoveryCompletion,r:LegacyPiRecovery)=>c.producer==="pi-detached-resume"&&c.runtime==="pi"
    &&c.recoveryId===r.attemptId&&c.nodeId===r.nodeId&&c.sessionName===r.sessionName&&c.sessionId===r.sessionId
    &&c.generation===r.generation&&c.nativeIdentityHash===r.nativeIdHash&&c.completedAt===r.at
    &&c.incarnation.supervisorLaunchId===r.supervisorLaunchId;
  const sourceValid=(completion:NativeRecoveryCompletion,publicationComplete:boolean)=>{
    if(nativeRecoverySourceValid(deps.db,completion,{publicationComplete,roots:deps.roots}))return true;
    const pin=deps.legacyReceipts?.find(p=>p.ref===completion.source.ref&&p.digest===completion.source.digest);
    const receipt=pin&&readPinnedLegacyPiRecovery(deps.roots.piDetached,pin);
    return !!receipt&&legacyMatches(completion,receipt);
  };
  const corroborateLegacy=async(session:string):Promise<NativeRecoveryCompletion|null>=>{
    if(!deps.legacyReceipts?.length)return null;
    return deps.guard.operation(session,async()=>{
      const current=await observe(session);if(!current||current.binding.runtime!=="pi")return null;
      const {binding:b,incarnation,proof}=current;
      if(deps.enabled&&!deps.enabled(b.nodeId))return null;
      const existing=completions.latest(b.nodeId,b.generation);if(existing)return existing;
      const candidates=deps.legacyReceipts!.map(pin=>({pin,receipt:readPinnedLegacyPiRecovery(deps.roots.piDetached,pin)}))
        .filter(v=>v.receipt?.nodeId===b.nodeId&&v.receipt.generation===b.generation);
      if(candidates.length!==1)return null;
      const {pin,receipt}=candidates[0]!;if(!receipt)return null;
      const native=await deps.piProve(session);
      const starts=[proof.processIdentity!.nativeStartedAt,proof.processIdentity!.supervisorStartedAt].map(v=>Date.parse(v));
      if(native?.state!=="present"||native.generation!==b.generation||native.launchId!==incarnation.runtimeLaunchId
        ||native.fingerprint!==receipt.nativeFingerprint||starts.some(t=>!Number.isFinite(t)||t>receipt.at)||receipt.at>now())return null;
      const completion:NativeRecoveryCompletion={schema:"native-recovery-completion.v1",producer:"pi-detached-resume",
        recoveryId:receipt.attemptId,rigId:b.rigId,nodeId:b.nodeId,sessionName:b.sessionName,sessionId:b.sessionId,
        generation:b.generation,runtime:"pi",nativeIdentityHash:b.nativeIdentityHash,configurationDigest:b.configurationDigest,
        completedAt:receipt.at,source:{...pin},incarnation,custodyPreserved:true,generationUnchanged:true};
      if(!legacyMatches(completion,receipt)||!sourceValid(completion,true)||!same(b,binding(session)))return null;
      // Append a current observation of an explicitly pinned historical success.
      // No legacy receipt, claim, UNKNOWN effect, or native process is rewritten.
      return completions.record(completion);
    });
  };
  return {
    observeSettledClaimant:async session=>{try{return await settled(session);}catch{return null;}},
    withSettledClaimant:async<T>(observation:NativeSettledObservation,send:(current:NativeSettledObservation)=>Promise<T>):Promise<NativeRecoveryGuardResult<T>>=>{
      let sendStarted=false;
      try{return await deps.guard.operation(observation.sessionName,async()=>{
        if(!settledBindingMatches(observation,binding(observation.sessionName)))return {state:"invalid",reason:"settled-binding-changed"};
        const current=await settled(observation.sessionName);
        if(!current)return {state:"held",reason:"native-settled-observation-unavailable"};
        if(!settledBindingMatches(observation,binding(observation.sessionName))
          ||!same(observation.incarnation,current.incarnation))
          return {state:"invalid",reason:"settled-incarnation-changed"};
        // No await from the final synchronous binding check to the consumer's
        // atomic queue/CAS callback. Errors after callback starts stay UNKNOWN.
        sendStarted=true;
        return {state:"performed",value:await send(current)};
      });}catch(error){
        if(!sendStarted&&error instanceof DeliveryGuardError)return {state:error.code==="guard_target_changed"?"invalid":"held",reason:error.code};
        throw error;
      }
    },
    recordNativeRecoveryCompletion:async evidence=>{
      if(deps.enabled&&!deps.enabled(evidence.nodeId))return;
      const current=await observe(evidence.sessionName,true);
      if(!current)throw new Error("Native recovery kernel incarnation unavailable");
      const {binding:b,incarnation,proof}=current;
      if(evidence.runtimeLaunchId&&evidence.runtimeLaunchId!==incarnation.runtimeLaunchId)throw new Error("Native recovery runner changed");
      if(evidence.supervisorLaunchId&&evidence.supervisorLaunchId!==incarnation.supervisorLaunchId)throw new Error("Native recovery supervisor changed");
      if(evidence.nativeFingerprint){
        const currentFingerprint=proof.nativeFingerprint;
        if(currentFingerprint!==evidence.nativeFingerprint)throw new Error("Native recovery producer proof changed");
      }
      const {runtimeLaunchId:_runtime,supervisorLaunchId:_supervisor,nativeFingerprint:_fingerprint,...facts}=evidence;
      const completion:NativeRecoveryCompletion={schema:"native-recovery-completion.v1",...facts,configurationDigest:b.configurationDigest,
        completedAt:now(),incarnation,custodyPreserved:true,generationUnchanged:true};
      if(!matches(completion,current)||!sourceValid(completion,false)||!same(b,binding(b.sessionName)))throw new Error("Native recovery completion binding changed");
      completions.record(completion);
    },
    observeRecoveredIncarnation:async session=>{
      try{
        const b=binding(session);if(!b)return null;
        const completion=completions.latest(b.nodeId,b.generation)??await corroborateLegacy(session);
        if(!completion||!sourceValid(completion,true))return null;
        const current=await observe(session);
        if(!current||!matches(completion,current)||!sourceValid(completion,true))return null;
        return {completion,observedAt:now()};
      }catch{return null;}
    },
    withRecoveredIncarnation:async<T>(observation:NativeRecoveryObservation,send:()=>Promise<T>):Promise<NativeRecoveryGuardResult<T>>=>{
      let sendStarted=false;
      try{return await deps.guard.operation(observation.completion.sessionName,async()=>{
        const c=observation.completion,b=binding(c.sessionName);
        if(!b||b.nodeId!==c.nodeId||b.generation!==c.generation||b.sessionId!==c.sessionId||b.configurationDigest!==c.configurationDigest||b.nativeIdentityHash!==c.nativeIdentityHash)return {state:"invalid",reason:"recovered-binding-changed"};
        const latest=completions.latest(c.nodeId,c.generation);
        if(!latest||!same(latest,c))return {state:"invalid",reason:"recovery-completion-changed"};
        if(!sourceValid(c,true))return {state:"held",reason:"recovery-publication-unproved"};
        const current=await observe(c.sessionName);
        if(!current)return {state:"held",reason:"native-recovery-observation-unavailable"};
        if(!matches(c,current))return {state:"invalid",reason:"recovered-incarnation-changed"};
        // No await between final source/binding check and the consumer's synchronous
        // DB guard + pending-to-sending transition. Transport errors remain UNKNOWN.
        if(!same(b,binding(c.sessionName))||!sourceValid(c,true))return {state:"held",reason:"recovery-evidence-changed"};
        sendStarted=true;
        return {state:"performed",value:await send()};
      });}catch(error){
        if(!sendStarted&&error instanceof DeliveryGuardError)return {state:error.code==="guard_target_changed"?"invalid":"held",reason:error.code};
        throw error;
      }
    },
  };
}
