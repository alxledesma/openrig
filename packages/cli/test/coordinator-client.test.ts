import { describe,it,expect,vi,afterEach } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Command } from "commander";
import { DaemonClient,senderIdentityHeaders } from "../src/client.js";
import { coordinatorCommand } from "../src/commands/coordinator.js";

afterEach(()=>{vi.unstubAllEnvs();vi.restoreAllMocks();process.exitCode=undefined;});
describe("immutable coordinator caller generation plumbing",()=>{
 it("derives generation from managed seat environment",()=>{
  vi.stubEnv("OPENRIG_SESSION_NAME","lead@xv");vi.stubEnv("OPENRIG_OCCUPANT_GENERATION","old-generation");
  expect(senderIdentityHeaders()).toMatchObject({"X-OpenRig-Session":"lead@xv","X-OpenRig-Occupant-Generation":"old-generation"});
 });
 it("caller header options cannot override the saved seat generation",async()=>{
  vi.stubEnv("OPENRIG_SESSION_NAME","lead@xv");vi.stubEnv("OPENRIG_OCCUPANT_GENERATION","old-generation");let seen:Record<string,string>={};
  const client=new DaemonClient("http://localhost:7433",{fetchImpl:async(_url,init)=>{seen=init?.headers as Record<string,string>;return new Response('{}',{status:200});}});
  await client.post("/api/queue/create",{}, {headers:{"X-OpenRig-Occupant-Generation":"new-generation"}});
  expect(seen["X-OpenRig-Occupant-Generation"]).toBe("old-generation");
 });
});

describe("coordinator resume-owned read phase",()=>{
 const parse=(args:string[])=>new Command().addCommand(coordinatorCommand()).parseAsync(["node","rig","coordinator",...args]);
 const response=(data:unknown)=>({status:200,data});
 const preparedDirs=()=>fs.readdirSync(os.tmpdir()).filter(name=>name.startsWith("openrig-resume-owned-"));

 it("rejects an invalid read timeout before any request",async()=>{
  const get=vi.spyOn(DaemonClient.prototype,"get").mockResolvedValue(response({}));
  const post=vi.spyOn(DaemonClient.prototype,"post").mockResolvedValue(response({}));
  await parse(["resume-owned","rig-1","--read-timeout-ms","1000.5"]);
  expect(get).not.toHaveBeenCalled();expect(post).not.toHaveBeenCalled();expect(process.exitCode).toBe(1);
 });

 it("passes the bounded timeout to the initial GET and reports its failure before preparation or POST",async()=>{
  const before=preparedDirs();
  const get=vi.spyOn(DaemonClient.prototype,"get").mockRejectedValue(new Error("read timed out"));
  const post=vi.spyOn(DaemonClient.prototype,"post").mockResolvedValue(response({}));
  const output=vi.spyOn(console,"log").mockImplementation(()=>{});
  await parse(["resume-owned","rig-1","--read-timeout-ms","45000"]);
  expect(get).toHaveBeenCalledWith("/api/coordinator/rig-1",expect.objectContaining({timeoutMs:45000}));
  expect(post).not.toHaveBeenCalled();expect(preparedDirs()).toEqual(before);expect(process.exitCode).toBe(1);
  expect(JSON.parse(output.mock.calls[0][0])).toMatchObject({status:"PRE_EFFECT_READ_FAILED",phase:"initial_authority_read",postAttempts:0,preparedRequest:false});
  expect(output.mock.calls[0][0]).toContain("start a fresh resume-owned command");
  expect(output.mock.calls[0][0]).not.toContain("UNKNOWN");
 });

 it("keeps the exact prepared request and one-POST unknown outcome behavior after POST timeout",async()=>{
  vi.spyOn(DaemonClient.prototype,"get").mockResolvedValue(response({authority:{epoch:8},obligationsDigest:"digest-8"}));
  const post=vi.spyOn(DaemonClient.prototype,"post").mockRejectedValue(new Error("write timed out"));
  const stderr=vi.spyOn(process.stderr,"write").mockImplementation(()=>true);
  await expect(parse(["resume-owned","rig-1","--lease-ms","5000","--read-timeout-ms","12000"])).rejects.toThrow("write timed out");
  expect(post).toHaveBeenCalledTimes(1);
  expect(post.mock.calls[0][2]).not.toHaveProperty("timeoutMs"); // the read-only timeout is never forwarded to POST
  const pathLine=stderr.mock.calls.map(call=>String(call[0])).join("").match(/prepared request: ([^\n]+)/);
  expect(pathLine).not.toBeNull();
  const preparedPath=pathLine![1];
  try{
   const expected=JSON.parse(fs.readFileSync(preparedPath,"utf8"));
   expect(expected).toMatchObject({rigId:"rig-1",leaseMs:5000,expectedEpoch:8,expectedObligationsDigest:"digest-8"});
   expect(post.mock.calls[0][1]).toEqual(expected);
   expect(stderr.mock.calls.map(call=>String(call[0])).join("")).toContain(`--replay-contract ${preparedPath}`);
  }finally{fs.rmSync(path.dirname(preparedPath),{recursive:true,force:true});}
 });

 it("replays the exact prepared contract without a GET or reconstructed body",async()=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),"coordinator-replay-test-"));
  const file=path.join(dir,"prepared.json"),contract={rigId:"rig-1",leaseMs:5000,operationId:"op-fixed",expectedEpoch:8,expectedObligationsDigest:"digest-8"};
  fs.writeFileSync(file,JSON.stringify(contract));
  const get=vi.spyOn(DaemonClient.prototype,"get").mockResolvedValue(response({}));
  const post=vi.spyOn(DaemonClient.prototype,"post").mockResolvedValue(response({accepted:true}));
  try{
   await parse(["resume-owned","rig-1","--replay-contract",file]);
   expect(get).not.toHaveBeenCalled();expect(post).toHaveBeenCalledTimes(1);
   expect(post.mock.calls[0][1]).toEqual(contract);
   expect(post.mock.calls[0][2]).not.toHaveProperty("timeoutMs");
  }finally{fs.rmSync(dir,{recursive:true,force:true});}
 });

 it("applies the same bounded read option to coordinator show",async()=>{
  const get=vi.spyOn(DaemonClient.prototype,"get").mockResolvedValue(response({ok:true}));
  await parse(["show","rig-1","--read-timeout-ms","60000"]);
  expect(get).toHaveBeenCalledWith("/api/coordinator/rig-1",expect.objectContaining({timeoutMs:60000}));
 });
});
