import { readFileSync } from "node:fs";
export function codexRotationContract(path:string, generation:string, launchArgv:string[], pinnedModel:string|null, pinnedProfile:string|null):Record<string,unknown> {
  let meta:Record<string,unknown>|null=null, turn:Record<string,unknown>|null=null;
  for(const line of readFileSync(path,"utf8").split("\n")) { if(!line.trim()) continue; let row;try { row=JSON.parse(line); } catch { continue; }
    if(row.type==="session_meta") meta=row.payload;
    if(row.type==="turn_context") turn=row.payload;
  }
  if(!meta || !turn || (meta.id??meta.session_id)!==generation) throw new Error("Native generation evidence missing or mismatched");
  const provider=turn.model_provider??meta.model_provider;
  const model=turn.model;
  if(typeof provider!=="string" || typeof model!=="string" || !turn.sandbox_policy || typeof turn.approval_policy!=="string") throw new Error("Native provider/model/posture evidence unavailable");
  const index=launchArgv.findIndex(a=>a==="-p" || a==="--profile");
  const profile=index>=0?launchArgv[index+1]:null;
  if(!profile || profile!==pinnedProfile || model!==pinnedModel) throw new Error("Live launch profile or model differs from persistent successor pin");
  return {runtime:"codex",model,provider,profile,permissions:{sandbox:turn.sandbox_policy,approval:turn.approval_policy},effort:turn.effort??null};
}
