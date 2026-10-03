import { describe,it,expect,vi,afterEach } from "vitest";
import { DaemonClient,senderIdentityHeaders } from "../src/client.js";

afterEach(()=>vi.unstubAllEnvs());
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
