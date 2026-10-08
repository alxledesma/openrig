import { afterEach, expect, it, vi } from "vitest";
import { Command } from "commander";
import { seatCommand } from "../src/commands/seat.js";
import { DaemonClient } from "../src/client.js";
import { STATE_FILE, type DaemonState, type LifecycleDeps } from "../src/daemon-lifecycle.js";
import type { StatusDeps } from "../src/commands/status.js";

const originalExit = process.exitCode;
const generation = "operator-generation-exact";
const bearer = "test-only-detached-resume-token";
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); process.exitCode = originalExit; });

function makeCli() {
  vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN", bearer);
  vi.stubEnv("OPENRIG_SESSION_NAME", "operator-agent@kernel");
  vi.stubEnv("OPENRIG_OCCUPANT_GENERATION", generation);
  const lifecycle: LifecycleDeps = {
    spawn: vi.fn(() => ({ pid: 1, unref: vi.fn() }) as never), fetch: vi.fn(async () => ({ok:true})), kill: vi.fn(() => true),
    readFile: vi.fn((p: string) => p === STATE_FILE ? JSON.stringify({pid:1,port:17538,db:"fixture.sqlite",startedAt:"2026-10-08T00:00:00Z"} as DaemonState) : null),
    writeFile: vi.fn(), removeFile: vi.fn(), exists: vi.fn((p: string) => p === STATE_FILE), mkdirp: vi.fn(), openForAppend: vi.fn(() => 3), isProcessAlive: vi.fn(() => true),
  };
  const requests: Array<{path:string; body:unknown; options?:unknown}> = [];
  const response = { status: 200, data: {ok:true, generation, custodyPreserved:true} };
  const client = {
    post: vi.fn(async (path:string, body:unknown, options?:unknown) => { requests.push({path,body,options}); return response; }),
    postOperatorMaintenance: vi.fn(async (body:unknown) => { requests.push({path:"/api/seat/operator-maintenance/rehost-runner",body}); return response; }),
  };
  const deps = { lifecycleDeps: lifecycle, clientFactory: vi.fn(() => client as unknown as DaemonClient) } as unknown as StatusDeps;
  const program = new Command().addCommand(seatCommand(deps));
  const override = (cmd:Command) => { cmd.exitOverride(); for (const sub of cmd.commands) override(sub); };
  override(program);
  const logs:string[]=[]; vi.spyOn(console,"log").mockImplementation((...v)=>logs.push(v.join(" "))); vi.spyOn(console,"error").mockImplementation((...v)=>logs.push(v.join(" ")));
  process.exitCode=undefined;
  return { program, requests, logs, client };
}

it("parses detached resume under rehost-runner into one authenticated strict-boolean request", async () => {
  const f=makeCli();
  await f.program.parseAsync(["node","rig","seat","rehost-runner","lead@xv","--reason","restore exact detached thread","--codex-detached-resume","--json"]);
  expect(f.requests).toHaveLength(1);
  expect(f.requests[0]).toMatchObject({path:"/api/seat/rehost-runner/lead%40xv",body:{reason:"restore exact detached thread",operator:undefined,legacyNativeWitness:false,codexDetachedResume:true}});
  expect(f.requests[0]!.options).toMatchObject({timeoutMs:60000});
  expect(new Headers((f.requests[0]!.options as any).headers).get("Authorization")).toBe(`Bearer ${bearer}`);
  expect(process.exitCode).toBeUndefined();
});

it("parses detached resume under operator-maintenance without caller agent identity", async () => {
  const f=makeCli();
  await f.program.parseAsync(["node","rig","seat","operator-maintenance","--reason","repair exact local Operator","--expected-node","operator-agent@kernel","--expected-generation",generation,"--codex-detached-resume","--json"]);
  expect(f.requests).toHaveLength(1);
  expect(f.requests[0]!.body).toEqual({reason:"repair exact local Operator",expected:{nodeId:"operator-agent@kernel",generation},codexDetachedResume:true});
  expect(f.client.postOperatorMaintenance).toHaveBeenCalledOnce();
  expect(process.exitCode).toBeUndefined();
});

it("refuses detached mode conflicts in both command parsers before any POST", async () => {
  const f=makeCli();
  await f.program.parseAsync(["node","rig","seat","rehost-runner","lead@xv","--reason","restore","--codex-detached-resume","--stopped-target-recovery","--accept-unpersisted-turn-loss","owner-ref"]);
  expect(f.requests).toHaveLength(0); expect(process.exitCode).toBe(1);
  process.exitCode=undefined;
  await f.program.parseAsync(["node","rig","seat","operator-maintenance","--reason","repair","--expected-node","operator-agent@kernel","--expected-generation",generation,"--codex-detached-resume","--attempt-id","abcdef12-1234-1234-1234-123456789abc","--began-sha256","a".repeat(64)]);
  expect(f.requests).toHaveLength(0); expect(process.exitCode).toBe(1);
});
