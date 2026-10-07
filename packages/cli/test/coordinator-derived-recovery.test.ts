import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { Command } from "commander";
import { DaemonClient } from "../src/client.js";
import { coordinatorCommand } from "../src/commands/coordinator.js";

describe("derived recovery executor",()=>{
 const digest="a".repeat(64);
 const state=()=>({authority:{rig_id:"rig-1",epoch:7,owner_session:"lead@rig-1",owner_generation:"lead-g7",state:"reconciling",lease_until:Date.now()-1000},obligationsDigest:digest});
 const dirs=new Set<string>();
 let stderr:ReturnType<typeof vi.spyOn>,output:ReturnType<typeof vi.spyOn>;
 const parse=(args:string[])=>new Command().exitOverride().addCommand(coordinatorCommand()).parseAsync(["node","rig","coordinator","recover-window","rig-1",...args]);
 const prepared=()=>{
  const line=stderr.mock.calls.map(c=>String(c[0])).join("").match(/prepared request: ([^\n]+)/);
  expect(line).not.toBeNull();dirs.add(path.dirname(line![1]));return line![1];
 };
 const reported=()=>JSON.parse(String(output.mock.calls.at(-1)![0]));
 const mocks=()=>({get:vi.spyOn(DaemonClient.prototype,"get").mockResolvedValue({status:200,data:state()}),post:vi.spyOn(DaemonClient.prototype,"post").mockResolvedValue({status:200,data:{state:"reconciling",epoch:7}})});
 beforeEach(()=>{
  vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN","fixture-only-token");
  stderr=vi.spyOn(process.stderr,"write").mockImplementation(()=>true);output=vi.spyOn(console,"log").mockImplementation(()=>{});
 });
 afterEach(()=>{
  for(const c of stderr.mock.calls){const match=String(c[0]).match(/prepared request: ([^\n]+)/);if(match)dirs.add(path.dirname(match[1]));}
  for(const dir of dirs)fs.rmSync(dir,{recursive:true,force:true});dirs.clear();
  vi.unstubAllGlobals();vi.unstubAllEnvs();vi.restoreAllMocks();process.exitCode=undefined;
 });

 it.each(["reconciliation","expired"] as const)("derives exact %s contract and persists private intent/UNKNOWN before its single POST",async kind=>{
  const {get,post}=mocks();
  post.mockImplementation(async(endpoint,body,options)=>{
   const file=prepared(),envelope=JSON.parse(fs.readFileSync(file,"utf8"));
   expect(fs.statSync(file).mode&0o777).toBe(0o600);expect(fs.statSync(path.dirname(file)).mode&0o777).toBe(0o700);
   expect(envelope).toMatchObject({schema:"coordinator-recovery-envelope.v1",rigId:"rig-1",kind,endpoint,request:body});
   const receipt=path.join(path.dirname(file),"submission-receipt.json");expect(fs.statSync(receipt).mode&0o777).toBe(0o600);
   expect(JSON.parse(fs.readFileSync(receipt,"utf8"))).toMatchObject({status:"UNKNOWN",phase:"prepared-before-post",operationId:envelope.request.operationId});
   expect(options).toEqual({headers:{Authorization:"Bearer fixture-only-token"},timeoutMs:12000});
   return {status:200,data:{state:"reconciling",epoch:kind==="expired"?8:7}};
  });
  await parse(["--kind",kind,"--window-ms","10000","--request-timeout-ms","12000","--read-timeout-ms","45000",...(kind==="expired"?["--conflict-operation-id","original-spent"]:[])]);
  expect(get).toHaveBeenCalledExactlyOnceWith("/api/coordinator/rig-1",{headers:{Authorization:"Bearer fixture-only-token"},timeoutMs:45000});expect(post).toHaveBeenCalledTimes(1);
  const request=post.mock.calls[0][1] as Record<string,unknown>;expect(request.operationId).toMatch(/^[a-f0-9-]{36}$/);
  expect(request).toEqual(kind==="reconciliation"
   ?{token:{rigId:"rig-1",epoch:7,generation:"lead-g7"},operationId:request.operationId,obligationsDigest:digest,windowMs:10000}
   :{rigId:"rig-1",operationId:request.operationId,windowMs:10000,expectedEpoch:7,expectedOwnerGeneration:"lead-g7",expectedCustodyDigest:digest,conflictOperationId:"original-spent",conflictKind:"reconciliation-recover"});
  const result=reported();expect(result.status).toBe("COMMITTED");expect(JSON.parse(fs.readFileSync(result.receiptPath,"utf8"))).toMatchObject({status:"COMMITTED"});
  expect(fs.readFileSync(prepared(),"utf8")).not.toContain("fixture-only-token");
 });
 it.each([
  ["--kind","automatic"], ["--kind","expired"], ["--kind","reconciliation","--conflict-operation-id","wrong"],
  ["--kind","reconciliation","--window-ms","9999"], ["--kind","reconciliation","--window-ms","900001"],
  ["--kind","reconciliation","--window-ms","10000.5"], ["--kind","reconciliation","--request-timeout-ms","60001"],
  ["--kind","reconciliation","--read-timeout-ms","0"],
 ].map(args=>[args]))("invalid options %j send nothing",async args=>{
  const {get,post}=mocks();await parse(args);expect(get).not.toHaveBeenCalled();expect(post).not.toHaveBeenCalled();expect(process.exitCode).toBe(1);
 });
 it("requires an explicit kind through real command registration",async()=>{
  const {get,post}=mocks();await expect(parse([])).rejects.toThrow();expect(get).not.toHaveBeenCalled();expect(post).not.toHaveBeenCalled();
 });
 it.each(["active","live-window","missing-generation","missing-digest","wrong-rig","bad-epoch"])("rejects %s authority before preparing or posting",async kind=>{
  const {get,post}=mocks();const current=state();
  if(kind==="active")current.authority.state="active";
  if(kind==="live-window")current.authority.lease_until=Date.now()+60000;
  if(kind==="missing-generation")current.authority.owner_generation="";
  if(kind==="missing-digest")current.obligationsDigest="";
  if(kind==="wrong-rig")current.authority.rig_id="other";
  if(kind==="bad-epoch")current.authority.epoch=1.5;
  get.mockResolvedValue({status:200,data:current});await parse(["--kind","reconciliation"]);
  expect(get).toHaveBeenCalledTimes(1);expect(post).not.toHaveBeenCalled();expect(stderr.mock.calls.join("")).not.toContain("prepared request:");
 });
 it.each(["read-error","read-refusal"])("%s performs no POST",async kind=>{
  const {get,post}=mocks();if(kind==="read-error")get.mockRejectedValue(new Error("secret must not be printed"));else get.mockResolvedValue({status:404,data:{} as any});
  await parse(["--kind","reconciliation"]);expect(post).not.toHaveBeenCalled();expect(stderr.mock.calls.join("")).not.toContain("secret");
 });
 it.each(["reconciliation","expired"] as const)("UNKNOWN %s retains intent and deliberate replay is exact with no GET or new operation",async kind=>{
  const {get,post}=mocks();post.mockRejectedValue(new Error("credential-like diagnostic must not be printed"));
  const args=["--kind",kind,...(kind==="expired"?["--conflict-operation-id","original-spent"]:[])];await parse(args);
  const originalFile=prepared(),originalBytes=fs.readFileSync(originalFile),body=post.mock.calls[0][1];const originalReceipt=path.join(path.dirname(originalFile),"submission-receipt.json"),receiptBytes=fs.readFileSync(originalReceipt);
  expect(reported().status).toBe("UNKNOWN");expect(post).toHaveBeenCalledTimes(1);expect(JSON.parse(receiptBytes.toString()).status).toBe("UNKNOWN");
  expect(output.mock.calls.join("")).not.toContain("credential-like");get.mockClear();post.mockClear();stderr.mockClear();post.mockResolvedValue({status:409,data:{error:"coordinator_retired"}});
  await parse(["--kind",kind,"--replay-contract",originalFile]);
  expect(get).not.toHaveBeenCalled();expect(post).toHaveBeenCalledTimes(1);expect(JSON.stringify(post.mock.calls[0][1])).toBe(JSON.stringify(body));
  expect(fs.readFileSync(originalFile)).toEqual(originalBytes);expect(fs.readFileSync(originalReceipt)).toEqual(receiptBytes);
  expect(reported()).toMatchObject({status:"REFUSED",replayedFrom:originalFile});expect(reported().guidance).toContain("does not prove the original effect absent");
 });
 it.each(["endpoint","rig","kind","operation","incomplete","overlong-id","window","digest","extra-proof"])("refuses altered replay %s with no request",async kind=>{
  const {get,post}=mocks();const dir=fs.mkdtempSync(path.join(os.tmpdir(),"derived-replay-test-"));dirs.add(dir);const file=path.join(dir,"prepared.json");
  const value:any={schema:"coordinator-recovery-envelope.v1",kind:"reconciliation",operation:"reconciliation-recover",endpoint:"/api/coordinator/reconciliation-recover",rigId:"rig-1",request:{token:{rigId:"rig-1",epoch:7,generation:"lead-g7"},operationId:"original-op",obligationsDigest:digest,windowMs:900000}};
  if(kind==="endpoint")value.endpoint="/api/queue/create";if(kind==="rig")value.request.token.rigId="other";if(kind==="kind")value.kind="expired";
  if(kind==="operation")value.operation="renew";if(kind==="incomplete")delete value.request.operationId;if(kind==="overlong-id")value.request.operationId="x".repeat(161);
  if(kind==="window")value.request.windowMs=900001;if(kind==="digest")value.request.obligationsDigest="invented";if(kind==="extra-proof")value.request.ready=true;
  fs.writeFileSync(file,JSON.stringify(value));await parse(["--kind","reconciliation","--replay-contract",file]);expect(get).not.toHaveBeenCalled();expect(post).not.toHaveBeenCalled();expect(process.exitCode).toBe(1);
 });
 it("a backend refusal is recorded once without retry or changing recovery kind",async()=>{
  const {post}=mocks();post.mockResolvedValue({status:409,data:{error:"coordinator_reconciliation_recovery_exhausted"}});
  await parse(["--kind","reconciliation"]);expect(post).toHaveBeenCalledTimes(1);expect(reported().status).toBe("REFUSED");
  expect(JSON.parse(fs.readFileSync(reported().receiptPath,"utf8"))).toMatchObject({status:"REFUSED",response:{error:"coordinator_reconciliation_recovery_exhausted"}});
 });
 it("actual client transport stamps the genuine seat generation and terminal auth, without body identity overrides",async()=>{
  vi.stubEnv("OPENRIG_URL","http://fixture.invalid");vi.stubEnv("OPENRIG_SESSION_NAME","operator-agent@kernel@fixture-instance");vi.stubEnv("OPENRIG_OCCUPANT_GENERATION","operator-current-generation");
  const seen:{url:string;init:RequestInit|undefined}[]=[];
  vi.stubGlobal("fetch",vi.fn(async(url,init)=>{seen.push({url:String(url),init});if(init?.method==="POST")prepared();return new Response(JSON.stringify(init?.method==="GET"?state():{state:"reconciling"}),{status:200});}));
  await parse(["--kind","reconciliation"]);expect(seen).toHaveLength(2);
  for(const call of seen)expect(call.init?.headers).toMatchObject({Authorization:"Bearer fixture-only-token","X-OpenRig-Session":"operator-agent@kernel@fixture-instance","X-OpenRig-Occupant-Generation":"operator-current-generation"});
  expect(seen[1].init?.body).not.toContain("operator-current-generation");expect(reported().status).toBe("COMMITTED");
 });
});
