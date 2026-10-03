import { describe,it,expect } from "vitest";
import { spawn } from "node:child_process";
import { mkdtempSync,rmSync,writeFileSync } from "node:fs";
import { join,resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { createDb } from "../src/db/connection.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { CoordinatorAuthorityService,digest } from "../src/domain/coordinator-authority-service.js";
import { seed,token } from "./helpers/coordinator-fixture.js";

async function fixture(){
 const dir=mkdtempSync(join(tmpdir(),"r07-race-")),file=join(dir,"db.sqlite"),db=createDb(file);seed(db);
 const bus=new EventBus(db),repo=new QueueRepository(db,bus),svc=repo.coordinatorAuthority;repo.attachOutbox(new OutboxHandler(db));
 await repo.create({qitemId:"baton",sourceSession:"operator-agent@kernel",destinationSession:"lead@xv",body:"dispatch",nudge:false});
 svc.enable("operator-agent@kernel","operator-agent-g1",{rigId:"xv",batonId:"baton",owner:"lead@xv",ownerGeneration:"lead-g1",coordinators:["lead@xv","peer@xv"],leaseMs:3600000,operationId:"enable"});
 svc.acknowledge("lead@xv",token,{operationId:"ack",obligationsDigest:svc.reconciliationDigest("xv")});
 return {dir,file,db,repo,svc,close(){db.close();rmSync(dir,{recursive:true,force:true});}};
}
const moduleUrl=(file:string)=>JSON.stringify(new URL(`../dist/${file}`,import.meta.url).href);
function child(file:string,op:string){
 const code=`import { createDb } from ${moduleUrl("db/connection.js")};import { EventBus } from ${moduleUrl("domain/event-bus.js")};import { QueueRepository } from ${moduleUrl("domain/queue-repository.js")};
 const db=createDb(${JSON.stringify(file)}),repo=new QueueRepository(db,new EventBus(db));console.log('READY');process.stdin.once('data',()=>{try { const r=repo.coordinatorAuthority.transfer('lead@xv','lead-g1',{expected:{rigId:'xv',epoch:1,generation:'lead-g1'},oldOwner:'lead@xv',recipient:'peer@xv',recipientGeneration:'peer-g1',leaseMs:3600000,operationId:${JSON.stringify(op)}});console.log(JSON.stringify({ok:true,epoch:r.epoch})); } catch(e){console.log(JSON.stringify({ok:false,code:e.code,message:e.message}));}finally{db.close();process.exit(0)}});`;
 const proc=spawn(process.execPath,["--input-type=module","-e",code],{stdio:["pipe","pipe","pipe"]});let output="",errors="";
 let readyResolve:()=>void;const ready=new Promise<void>(r=>readyResolve=r);
 proc.stdout.on("data",d=>{output+=d.toString();if(output.includes("READY"))readyResolve();});proc.stderr.on("data",d=>errors+=d.toString());
 const result=new Promise<{ok:boolean;code?:string;epoch?:number} >((res,rej)=>proc.on("close",code=>{if(code!==0)return rej(new Error(`child exit ${code}: ${errors}`));try{res(JSON.parse(output.trim().split("\n").at(-1)!));}catch(e){rej(e)}}));
 return {ready,result,go(){proc.stdin.write("GO\n");}};
}
describe("real commit ordering across independent processes/connections",()=>{
 it("two simultaneous successor CAS attempts commit one winner and one unchanged loser",async()=>{
  const f=await fixture();try{
   const before={authority:f.svc.get("xv"),baton:f.repo.getById("baton"),assignments:f.svc.obligations("xv")};
   const a=child(f.file,"a"),b=child(f.file,"b");await Promise.all([a.ready,b.ready]);a.go();b.go();const outcomes=await Promise.all([a.result,b.result]);
   expect(outcomes.filter(r=>r.ok)).toHaveLength(1);expect(outcomes.filter(r=>r.code==="coordinator_cas_lost")).toHaveLength(1);
   expect(f.svc.get("xv")?.epoch).toBe(2);expect(f.repo.getById("baton")?.destinationSession).toBe("peer@xv");
   expect(f.db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='transfer'").get()).toEqual({n:1});
   writeFileSync(new URL("../../../artifacts/r07-cas-records.json",import.meta.url),JSON.stringify({before,outcomes,after:{authority:f.svc.get("xv"),baton:f.repo.getById("baton"),assignments:f.svc.obligations("xv"),operations:f.db.prepare("SELECT * FROM coordinator_operations ORDER BY operation_id").all()}},null,2));
  }finally{f.close();}
 });
 it("crash-equivalent failure immediately before baton/authority commit rolls everything back",async()=>{
  const f=await fixture();try{
   f.db.exec("CREATE TRIGGER interrupt_transfer BEFORE UPDATE ON queue_items WHEN NEW.destination_session='peer@xv' BEGIN SELECT RAISE(ABORT,'injected before commit'); END");
   expect(()=>f.svc.transfer("lead@xv","lead-g1",{expected:token,oldOwner:"lead@xv",recipient:"peer@xv",recipientGeneration:"peer-g1",leaseMs:3600000,operationId:"interrupted"})).toThrow("injected");
   const reopen=createDb(f.file);try{const recovered=new CoordinatorAuthorityService(reopen);expect(recovered.get("xv")?.epoch).toBe(1);expect(recovered.get("xv")?.state).toBe("active");expect(reopen.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='transfer'").get()).toEqual({n:0});}finally{reopen.close();}
  }finally{f.close();}
 });
 it("post-commit process exit retains excluded predecessor and unacknowledged successor across restart",async()=>{
  const f=await fixture();try{
   const p=child(f.file,"commit-exit");await p.ready;p.go();expect((await p.result).ok).toBe(true);
   const other=createDb(f.file);try{const repo=new QueueRepository(other,new EventBus(other));expect(repo.coordinatorAuthority.get("xv")?.state).toBe("reconciling");
    await expect(repo.create({sourceSession:"lead@xv",destinationSession:"builder@xv",body:"old",dispatch:{token,packageKey:"p"},nudge:false})).rejects.toThrow("no longer holds");
    await expect(repo.create({sourceSession:"peer@xv",destinationSession:"builder@xv",body:"new",dispatch:{token:{rigId:"xv",epoch:2,generation:"peer-g1"},packageKey:"p"},nudge:false})).rejects.toThrow("not reconciled");
   }finally{other.close();}
  }finally{f.close();}
 });
 it("committed-before exclusion retains one queue row; stale request ordered after exclusion has no effects",async()=>{
  const f=await fixture();try{
   f.svc.admit("operator-agent@kernel","operator-agent-g1","xv","p",{inputDigest:digest("i"),destination:"builder@xv",bodyHash:digest("build"),resources:["a"],returnContract:{destination:"lead@xv",evidenceRequired:["proof"]}});
   const row=await f.repo.create({qitemId:"before",sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p"},nudge:false});
   const second=createDb(f.file);try{const svc=new CoordinatorAuthorityService(second);svc.transfer("lead@xv","lead-g1",{expected:token,oldOwner:"lead@xv",recipient:"peer@xv",recipientGeneration:"peer-g1",leaseMs:3600000,operationId:"exclude"});}finally{second.close();}
   const events=f.db.prepare("SELECT count(*) n FROM events").get();await expect(f.repo.create({qitemId:"after",sourceSession:"lead@xv",destinationSession:"builder@xv",body:"build",dispatch:{token,packageKey:"p"},nudge:false})).rejects.toThrow("no longer holds");
   expect(f.repo.getById("before")).toEqual(row);expect(f.repo.getById("after")).toBeNull();expect(f.db.prepare("SELECT count(*) n FROM events").get()).toEqual(events);
  }finally{f.close();}
 });
});
