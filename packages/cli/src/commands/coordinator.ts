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
 for(const op of ["enable","transfer","acknowledge","renew","admit","dispose","recover","legacy-inventory","migrate-legacy"]){
   cmd.command(`${op} <contractFile>`).description("Submit exact frozen JSON contract; caller identity/generation derive from seat environment")
    .action(async(file:string)=>{
     const res=await new DaemonClient().post(`/api/coordinator/${op}`,JSON.parse(fs.readFileSync(file,"utf8")), { headers: terminalAuthHeaders() });
     console.log(JSON.stringify(res.data,null,2));if(res.status>=400)process.exitCode=1;
    });
 }
 return cmd;
}
