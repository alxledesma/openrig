import { afterEach, expect, it, vi } from "vitest";
import { Command } from "commander";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { handoverCommand, seatCommand } from "../src/commands/seat.js";
import { STATE_FILE } from "../src/daemon-lifecycle.js";
const dirs:string[]=[];afterEach(()=>{for(const dir of dirs.splice(0))fs.rmSync(dir,{recursive:true,force:true});vi.restoreAllMocks();vi.unstubAllEnvs();process.exitCode=undefined;});
function setup(response:any={status:200,data:{runtimeMigration:{operationId:"prepared"}}}){
 const post=vi.fn(async()=>response),get=vi.fn(async()=>response);
 const deps:any={lifecycleDeps:{readFile:(p:string)=>p===STATE_FILE?JSON.stringify({pid:123,port:7433,db:"fixture",startedAt:"2026-10-07T00:00:00Z"}):null,exists:(p:string)=>p===STATE_FILE,isProcessAlive:()=>true,fetch:async()=>({ok:true})},clientFactory:()=>({post,get})};
 const program=new Command();program.exitOverride();program.addCommand(seatCommand(deps));program.addCommand(handoverCommand(deps));vi.spyOn(console,"log").mockImplementation(()=>{});vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN","fixture-token");return {program,post,get};
}
it.each(["seat", "top-level"])("%s handover sends exact JSON packet with auth in one request",async kind=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),"migration-cli-"));dirs.push(dir);const file=path.join(dir,"packet.json");const packet={operationId:"op-1",target:{runtime:"codex",provider:"openai",model:"gpt-6-luna",effort:"high",codexConfigProfile:"peer"}};fs.writeFileSync(file,JSON.stringify(packet));const {program,post}=setup();
 await program.parseAsync(["node","rig",...(kind==="seat"?["seat"]:[]),"handover","peer@xv","--reason","safe peer migration","--runtime-migration",file,"--dry-run","--json"]);
 expect(post).toHaveBeenCalledTimes(1);expect(post).toHaveBeenCalledWith("/api/seat/handover/peer%40xv",expect.objectContaining({runtimeMigration:packet,dryRun:true}),{headers:{Authorization:"Bearer fixture-token"},timeoutMs:60_000});
});
it("exposes read-only one-shot outcome inspection",async()=>{const {program,get,post}=setup();await program.parseAsync(["node","rig","seat","runtime-migration-status","op-1"]);expect(get).toHaveBeenCalledWith("/api/seat/runtime-migration/op-1",{headers:{Authorization:"Bearer fixture-token"}});expect(post).not.toHaveBeenCalled();});
