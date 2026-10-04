import { it, expect, vi } from "vitest";
import { createProgram } from "../src/index.js";
import { DaemonClient } from "../src/client.js";
import type { QueueDeps } from "../src/commands/queue.js";
vi.mock("../src/daemon-lifecycle.js", async () => ({
  ...await vi.importActual<Record<string, unknown>>("../src/daemon-lifecycle.js"),
  getDaemonStatus: vi.fn(async () => ({state:"running",healthy:true,pid:1234,port:7433})),
  getDaemonUrl: vi.fn(() => "http://localhost:7433"),
}));
it("inspection reaches an authenticated HTTP boundary through the real DaemonClient", async () => {
  vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN", "synthetic-fixture-not-a-credential");
  const requests: {url:string;body:unknown;authenticated:boolean}[]=[];
  const fetchImpl: typeof fetch=async (input, init) => {
    const authenticated=new Headers(init?.headers).get("Authorization")==="Bearer synthetic-fixture-not-a-credential";
    requests.push({url:String(input),body:JSON.parse(String(init?.body)),authenticated});
    return new Response(JSON.stringify(authenticated ? {created:true,parentId:"fixture-parent"} : {error:"unauthorized",reason:"missing Authorization header"}),{status:authenticated?200:401,headers:{"Content-Type":"application/json"}});
  };
  const deps: QueueDeps={lifecycleDeps:{} as QueueDeps["lifecycleDeps"],clientFactory:()=>new DaemonClient("http://localhost:7433",{fetchImpl})};
  const output=vi.spyOn(console,"log").mockImplementation(()=>{});
  try {
    await createProgram({queueDeps:deps}).parseAsync(["node","rig","queue","inspect-recovery","fixture-q","--source-facts-hash","a".repeat(64),"--operation-id","fixture-op","--authorization-id","fixture-auth","--json"]);
    expect(requests).toEqual([{url:"http://localhost:7433/api/queue/fixture-q/inspect-recovery",body:{sourceFactsHash:"a".repeat(64),operationId:"fixture-op",authorizationId:"fixture-auth"},authenticated:true}]);
  } finally {output.mockRestore();vi.unstubAllEnvs();}
});
