import { lstatSync, readFileSync, realpathSync, existsSync } from "node:fs";
import path from "node:path";
import { recoveryReceiptDigest } from "./native-recovery-completion.js";

export interface PinnedLegacyRecovery {ref:string;digest:string}
export interface LegacyPiRecovery {
  ok:true;runtime:"pi";nodeId:string;sessionName:string;sessionId:string;generation:string;
  generationUnchanged:true;nativeIdHash:string;attemptId:string;supervisorLaunchId:string;
  nativeFingerprint:string;custodyPreserved:true;at:number;
}
/** Explicit rollout input, never automatic trust of arbitrary pre-upgrade files.
 * The pinned successful receipt is historical evidence; current kernel proof is
 * separately required before it can be normalized or used for continuation. */
export function readPinnedLegacyPiRecovery(root:string,pin:PinnedLegacyRecovery):LegacyPiRecovery|null {
  try {
    if(!/^[a-f0-9]{64}$/.test(pin.digest)||!path.isAbsolute(pin.ref)||path.normalize(pin.ref)!==pin.ref)return null;
    const resolvedRoot=realpathSync(root),relative=path.relative(resolvedRoot,pin.ref),parts=relative.split(path.sep);
    if(parts.length!==3||!/^[a-f0-9]{64}$/.test(parts[0]!)||!/^[a-zA-Z0-9-]+$/.test(parts[1]!)||parts[2]!=="completed.json")return null;
    let current=resolvedRoot;
    for(const part of ["",...parts]){
      if(part)current=path.join(current,part);
      const stat=lstatSync(current);
      if(stat.isSymbolicLink()||stat.uid!==process.getuid?.()||(stat.mode&0o077)!==0||realpathSync(current)!==current)return null;
      if(current===pin.ref?(!stat.isFile()||stat.size>262144):!stat.isDirectory())return null;
    }
    const bytes=readFileSync(pin.ref);if(recoveryReceiptDigest(bytes)!==pin.digest)return null;
    const receipt=JSON.parse(bytes.toString("utf8")) as LegacyPiRecovery;
    if(receipt.ok!==true||receipt.runtime!=="pi"||receipt.custodyPreserved!==true||receipt.generationUnchanged!==true
      ||receipt.attemptId!==parts[1]||!Number.isSafeInteger(receipt.at)||receipt.at<=0
      ||![receipt.nodeId,receipt.sessionName,receipt.sessionId,receipt.generation,receipt.nativeIdHash,receipt.supervisorLaunchId,receipt.nativeFingerprint].every(v=>typeof v==="string"&&v.length>0))return null;
    if(recoveryReceiptDigest(JSON.stringify([receipt.nodeId,receipt.generation]))!==parts[0])return null;
    // A new-protocol partial publication can never be downgraded into legacy.
    const directory=path.dirname(pin.ref);
    if(["began","completed","unknown"].some(kind=>existsSync(path.join(directory,`completion-publication-${kind}.json`))))return null;
    return receipt;
  }catch{return null;}
}

export function parsePinnedLegacyRecoveries(raw:string|undefined):PinnedLegacyRecovery[] {
  if(!raw)return [];
  const values:unknown=JSON.parse(raw);
  if(!Array.isArray(values)||values.length>8||values.some(v=>!v||typeof v!=="object"||Object.keys(v).sort().join(",")!=="digest,ref"||typeof v.ref!=="string"||!path.isAbsolute(v.ref)||typeof v.digest!=="string"||!/^[a-f0-9]{64}$/.test(v.digest)))throw new Error("Invalid pinned legacy native recovery configuration");
  if(new Set(values.map(v=>v.ref)).size!==values.length)throw new Error("Duplicate pinned legacy native recovery");
  return values;
}
