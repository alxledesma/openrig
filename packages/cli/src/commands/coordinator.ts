import fs from "node:fs";
import { Command } from "commander";
import { DaemonClient, terminalAuthHeaders } from "../client.js";

/** Exact JSON contracts keep authority and evidence explicit; no implied automatic takeover. */
export function coordinatorCommand():Command {
 const cmd=new Command("coordinator").description("Rig-scoped durable coordinator authority and admitted packages");
 cmd.command("show <rigId>").action(async(rigId:string)=>{
   const res=await new DaemonClient().get(`/api/coordinator/${encodeURIComponent(rigId)}`, { headers: terminalAuthHeaders() });
   console.log(JSON.stringify(res.data,null,2));if(res.status>=400)process.exitCode=1;
 });
 for(const op of ["enable","transfer","acknowledge","renew","admit","dispose","recover","legacy-inventory","migrate-legacy","coordination-worker-probe","coordination-return-successor","coordination-return-continue","coordination-return-retire","coordination-lifecycle-recovery"]){
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
