import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { Command } from "commander";
import { DaemonClient, terminalAuthHeaders } from "../client.js";

/** Exact JSON contracts keep authority and evidence explicit; no implied automatic takeover. */
export function coordinatorCommand():Command {
 const cmd=new Command("coordinator").description("Rig-scoped durable coordinator authority and admitted packages");
 cmd.command("show <rigId>").action(async(rigId:string)=>{
   const res=await new DaemonClient().get(`/api/coordinator/${encodeURIComponent(rigId)}`, { headers: terminalAuthHeaders() });
   console.log(JSON.stringify(res.data,null,2));if(res.status>=400)process.exitCode=1;
 });
 // resume-owned takes a rigId, not a contract file: the daemon derives the caller's token
 // and current obligations digest, so the native Lead supplies no digest and no epoch.
 cmd.command("resume-owned <rigId>")
  .description("Continue your OWN live coordinator authority: atomically acknowledge a reconciling owner and renew it. Derives the token and current obligations digest server-side; never recovers an expired lease.")
  .option("--operation-id <id>","Stable durable receipt id for CONTROLLED replay. Omitted by default, which generates a fresh id so an operator never reuses one. Pass the printed exact id back to replay deliberately. The daemon always requires an exact id.")
  .option("--lease-ms <ms>","Renewed lease in milliseconds (1000-3600000)","1200000")
  .action(async(rigId:string,opts:{operationId?:string;leaseMs:string})=>{
   const leaseMs=Number(opts.leaseMs);
   if(!Number.isSafeInteger(leaseMs)||leaseMs<1000||leaseMs>3600000){process.stderr.write("leaseMs must be an integer 1000-3600000\n");process.exitCode=1;return;}
   const generated=!opts.operationId;
   // A fresh generated id removes the reused-operation-id failure at its source. There is NO
   // automatic retry: a failed call is retried deliberately by passing the printed exact id back.
   const operationId=opts.operationId??randomUUID();
   const res=await new DaemonClient().post(`/api/coordinator/resume-owned`,{rigId,leaseMs,operationId},{ headers: terminalAuthHeaders() });
   const receipt:unknown=res.data;
   console.log(JSON.stringify({...(receipt&&typeof receipt==="object"?receipt:{}),operationId,operationIdSource:generated?"generated":"supplied"},null,2));if(res.status>=400)process.exitCode=1;
  });
for(const op of ["active-expiry-recover","held-history-adopt","held-history-recovery-bind","outbox-abandon-evidence","outbox-abandon-continue","outcome-qualification-refresh","outcome-recovery-bind","outbox-abandon-authorize","outbox-abandon-notify","enable","transfer","acknowledge","renew","admit","dispose","recover","legacy-inventory","migrate-legacy","diagnostic-wake-dispose","coordination-plan","coordination-reconcile","coordination-accept","coordination-continue-custody","outcome-configure","resilience-materialize","reconciliation-recover","coordination-worker-probe","coordination-return-successor","coordination-return-continue","coordination-return-retire","coordination-return-intake-refresh","coordination-lifecycle-recovery","coordination-frontier-plan","coordination-frontier-admit","coordination-frontier-confirm","coordination-frontier-boundary"]){
   cmd.command(`${op} <contractFile>`).description(op==="dispose"?'Submit {"rigId":"...","packageKey":"original admitted package key","dispositionId":"new worker-authored JSON return queue ID"}. The original worker may dispose its own terminal return; holder role is not required.':"Submit exact frozen JSON contract; caller identity/generation derive from seat environment")
    .action(async(file:string)=>{
     const contract=JSON.parse(fs.readFileSync(file,"utf8"));
     if(op==="dispose"&&["rigId","packageKey","dispositionId"].some(k=>typeof contract?.[k]!=="string"||!contract[k].trim()))throw new Error("dispose requires JSON {rigId,packageKey,dispositionId}; dispositionId is the NEW worker-authored typed-return queue item, not the original assignment or duty ID");
     const res=await new DaemonClient().post(`/api/coordinator/${op}`,contract, { headers: terminalAuthHeaders() });
     console.log(JSON.stringify(res.data,null,2));if(res.status>=400)process.exitCode=1;
    });
 }
 return cmd;
}
