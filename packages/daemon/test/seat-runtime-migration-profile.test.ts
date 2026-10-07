import { beforeEach, afterEach, expect, it, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
const proc=vi.hoisted(()=>({run:vi.fn()}));
vi.mock("node:child_process",async importOriginal=>({...await importOriginal<typeof import("node:child_process")>(),execFile:proc.run}));
import { CodexRuntimeAdapter } from "../src/adapters/codex-runtime-adapter.js";
let dir:string,adapter:CodexRuntimeAdapter,binding:any;
beforeEach(()=>{
 dir=fs.mkdtempSync(path.join(os.tmpdir(),"migration-profile-"));fs.writeFileSync(path.join(dir,"peer.config.toml"),'model="gpt-6-luna"\nmodel_provider="openai"\nmodel_reasoning_effort="high"\napproval_policy="on-request"\nsandbox_mode="workspace-write"\n');
 proc.run.mockReset().mockImplementation((_file,_args,_opts,callback)=>callback(null,"opaque status", ""));
 adapter=new CodexRuntimeAdapter({tmux:{} as any,fsOps:{readFile:p=>fs.readFileSync(p,"utf8"),exists:fs.existsSync,writeFile:()=>{},mkdirp:()=>{}},codexHome:dir,launchPath:"/fixture/native/path"});
 binding={id:"b",nodeId:"peer",tmuxSession:"peer@xv",tmuxWindow:null,tmuxPane:"%0",cmuxWorkspace:null,cmuxSurface:null,updatedAt:"",cwd:dir,model:"gpt-6-luna",effort:"high",codexConfigProfile:"peer",launchPosture:"floor"};
});
afterEach(()=>fs.rmSync(dir,{recursive:true,force:true}));
it("uses the adapter native home, PATH and cwd; records only digest and boolean auth result",async()=>{
 const result=await adapter.preflightRuntimeMigration(binding);expect(result).toMatchObject({authenticated:true,profileSha256:expect.stringMatching(/^[a-f0-9]{64}$/),effective:{model:"gpt-6-luna",effort:"high",provider:"openai",sandbox:"workspace-write"}});
 expect(proc.run.mock.calls.map(c=>c.slice(0,2))).toEqual([["codex",["--version"]],["codex",["-p","peer","mcp","list"]],["codex",["login","status"]]]);
 for(const call of proc.run.mock.calls)expect(call[2]).toMatchObject({cwd:dir,env:{CODEX_HOME:dir,PATH:"/fixture/native/path"}});
 expect(JSON.stringify(result)).not.toContain("opaque status");
});
it("refuses permission escalation before probing and strips native auth errors",async()=>{
 binding.launchPosture="full_bypass";await expect(adapter.preflightRuntimeMigration(binding)).rejects.toThrow("posture");expect(proc.run).not.toHaveBeenCalled();
 binding.launchPosture="floor";proc.run.mockImplementation((_f,_a,_o,callback)=>callback(new Error("private auth material"),"", "private auth material"));
 await expect(adapter.preflightRuntimeMigration(binding)).rejects.toThrow("authentication could not be verified");
});
