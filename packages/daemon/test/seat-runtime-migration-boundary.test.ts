import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { Hono } from "hono";
const seam=vi.hoisted(()=>({address:"127.0.0.1",handover:vi.fn()}));
vi.mock("@hono/node-server/conninfo",()=>({getConnInfo:()=>({remote:{address:seam.address}})}));
vi.mock("../src/domain/seat-handover-service.js",()=>({SeatHandoverService:class{handover=seam.handover;}}));
import { seatRoutes } from "../src/routes/seat.js";
let app:Hono;
beforeEach(()=>{seam.handover.mockReset();seam.address="127.0.0.1";app=new Hono();app.use("*",async(c,next)=>{
 c.set("terminalBearerToken" as never,"fixture-token" as never);c.set("rigRepo" as never,{db:{}} as never);c.set("tmuxAdapter" as never,{} as never);await next();});app.route("/api/seat",seatRoutes);});
afterEach(()=>vi.unstubAllEnvs());
const packet={operationId:"op-1",target:{runtime:"codex",model:"gpt-6-luna",provider:"openai",effort:"high",codexConfigProfile:"luna-high"}};
function post(headers:Record<string,string>={},body:Record<string,unknown>={runtimeMigration:packet}){return app.request("/api/seat/handover/peer%40xv",{method:"POST",headers:{"Content-Type":"application/json",...headers},body:JSON.stringify(body)});}
it("requires authenticated local non-browser migration transport before the service",async()=>{
 expect((await post()).status).toBe(401);
 expect((await post({Authorization:"Bearer fixture-token",Origin:"https://example.invalid"})).status).toBe(403);
 seam.address="192.0.2.1";expect((await post({Authorization:"Bearer fixture-token"})).status).toBe(403);expect(seam.handover).not.toHaveBeenCalled();
});
it("uses transport actor and generation, passes only explicit migration packet, maps unknown to 503",async()=>{
 seam.handover.mockResolvedValue({ok:false,code:"runtime_migration_unknown",blindRetryAllowed:false});
 const response=await post({Authorization:"Bearer fixture-token","X-OpenRig-Session":"operator-agent@kernel","X-OpenRig-Occupant-Generation":"actual-g"},{runtimeMigration:packet,operator:"fake@rig",preparedMigration:{started:true},rotationActor:"fake@rig"});
 expect(response.status).toBe(503);expect(seam.handover).toHaveBeenCalledWith(expect.objectContaining({runtimeMigration:packet,rotationActor:"operator-agent@kernel",rotationActorGeneration:"actual-g"}));expect(seam.handover.mock.calls[0][0]).not.toHaveProperty("preparedMigration");
});
