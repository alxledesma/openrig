import {it,expect} from "vitest";
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {createHash,randomUUID} from "node:crypto";
import {spawn} from "node:child_process";
import {once} from "node:events";
import {readHistoricalFailure,censusAttemptNative} from "../src/domain/failed-precommit-proof.js";
import type {DispatchReservation} from "../src/domain/seat-dispatch-reservation.js";
const r={reservation_id:"exact-reservation",operation_id:"exact-operation",node_id:"node",session_name:"worker@rig",predecessor_generation:"pred-managed",predecessor_native_id:"pred-native",performer_session:"operator-agent@kernel",performer_generation:"operator-g1"} as DispatchReservation;
it("historical import requires frozen hash-bound original failure and exact effect attribution",()=>{
 const root=mkdtempSync(join(tmpdir(),"historical-failure-"));mkdirSync(join(root,"frozen"));
 const effects={preparedGeneration:"prepared-g2",discoveredId:"discovered",nativeId:"native-failed",replacementStarted:true};
 const tuple={fullSnapshotSha256:"a".repeat(64),predecessorRowsSha256:"b".repeat(64),reservationId:r.reservation_id,operationId:r.operation_id,nodeId:r.node_id,predecessorGeneration:r.predecessor_generation,predecessorNativeId:r.predecessor_native_id,performerSession:r.performer_session,performerGeneration:r.performer_generation};
 const write=(name:string,value:unknown)=>{const path=`frozen/${name}.json`;writeFileSync(join(root,path),JSON.stringify(value));return {path,sha256:createHash("sha256").update(readFileSync(join(root,path))).digest("hex")};};
 try {
  const failure=write("failure",{httpStatus:500,result:{ok:false,code:"handover_commit_failed"}}),attribution=write("attribution",{...tuple,effects});
  const proof=write("proof",{protocol:"failed-precommit-history-v1",...tuple,effects,failure,attribution});
  expect(readHistoricalFailure(root,proof.path,proof.sha256,r).effects).toEqual(effects);
  expect(()=>readHistoricalFailure(root,proof.path,"0".repeat(64),r)).toThrow("hash");
  expect(()=>readHistoricalFailure(root,proof.path,proof.sha256,{...r,operation_id:"other"})).toThrow("attribution");
  writeFileSync(join(root,"frozen/attribution.json"),JSON.stringify({...tuple,effects:{...effects,preparedGeneration:"fabricated"}}));
  expect(()=>readHistoricalFailure(root,proof.path,proof.sha256,r)).toThrow("hash");
  const outside=write("other",{protocol:"failed-precommit-history-v1"});writeFileSync(join(root,"outside.json"),readFileSync(join(root,outside.path)));
  expect(()=>readHistoricalFailure(root,"outside.json",outside.sha256,r)).toThrow("outside frozen");
 }finally{rmSync(root,{recursive:true,force:true});}
});
it("real native effect census detects an escaped prepared-generation process and never retains its environment",async()=>{
 const generation=randomUUID(), child=spawn(process.execPath,['-e',"console.log('ready');setInterval(()=>{},1000)"],{env:{...process.env,OPENRIG_OCCUPANT_GENERATION:generation,PRIVATE_TEST_SENTINEL:"do-not-retain"},stdio:['ignore','pipe','ignore']});
 try {
  await once(child.stdout!,"data");const census=await censusAttemptNative(r,{preparedGeneration:generation,discoveredId:null,nativeId:null,replacementStarted:true});
  expect(census.remainingPids).toContain(child.pid);expect(JSON.stringify(census)).not.toContain("do-not-retain");
 }finally{child.kill();await once(child,"exit");}
 const cleaned=await censusAttemptNative(r,{preparedGeneration:generation,discoveredId:null,nativeId:null,replacementStarted:true});expect(cleaned.remainingPids).toEqual([]);
});
