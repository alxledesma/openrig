import { mkdtempSync, realpathSync, writeFileSync, appendFileSync, rmSync, renameSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NativeModelReader } from "../src/domain/context-refresh-integration.js";
import { createHash } from "node:crypto";
import type { PiRotationContract } from "../src/domain/pi-rotation-native-proof.js";
import { describe, it, expect } from "vitest";
import { ContextRefreshObserver, readContextRefreshTranscript, startContextRefreshCatchup, type ContextRefreshBinding, type ContextRefreshObserverSources } from "../src/domain/context-refresh-observer.js";
const NOW = Date.parse("2026-10-07T23:00:00Z");
const stamp = (n: number) => new Date(NOW - 10000 + n).toISOString();
const meta = { type: "session_meta", timestamp: stamp(0), payload: { id: "thread" } };
const turn = (model = "model") => ({ type: "turn_context", timestamp: stamp(1), payload: { model } });
const usage = (n = 2, window = 1000, total = 850) => ({ type: "event_msg", timestamp: stamp(n), payload: {
  type: "token_count", info: { model_context_window: window, last_token_usage: { total_tokens: total } },
} });
const compact = (n = 3, count?: number) => ({ type: "compacted", timestamp: stamp(n), payload: { message: "native compacted history", ...(count === undefined ? {} : { count }) } });
function fixture(rows: unknown[] = [meta, turn(), usage()]) {
  let binding: ContextRefreshBinding = { nodeId: "node", sessionName: "lead@rig", generation: "gen", runtime: "codex", nativeId: "thread", configurationDigest: "digest", model: "model", contextWindow: 1000 };
  let bytes = Buffer.from(rows.map(r => JSON.stringify(r) + "\n").join(""));
  let revision = 0; let identity = "inode-1", pi: unknown = null, clock = NOW;
  const offsets: number[] = [];
  const sources: ContextRefreshObserverSources = {
    binding: async () => ({ ...binding }),
    native: async b => ({ nodeId: b.nodeId, sessionName: b.sessionName, generation: b.generation, nativeId: b.nativeId, verified: true, observedAt: clock, launchId: "launch", fingerprint: "kernel-proof" }),
    activity: async () => ({ value: "idle", observedAt: clock }),
    transcriptPath: async () => "/private/native.jsonl", piState: async () => pi,
    readTranscript: (_file, offset, limit) => { offsets.push(offset); return { bytes: bytes.subarray(offset, offset + limit), size: bytes.length, identity, revision: String(revision) }; },
  };
  return { sources, offsets, observer: () => new ContextRefreshObserver(sources, { now: () => clock }),
    bytes: () => bytes,
    append: (row: unknown) => { revision++; bytes = Buffer.concat([bytes, Buffer.from(JSON.stringify(row) + "\n")]); },
    raw: (text: string) => { revision++; bytes = Buffer.from(text); },
    replace: () => { identity = "inode-2"; },
    setBinding: (b: Partial<ContextRefreshBinding>) => { binding = { ...binding, ...b }; },
    setPi: (v: unknown) => { pi = v; }, advance: (n: number) => { clock += n; },
  };
}
describe("source-bound context refresh observation", () => {
  it("validates asynchronous native samples at completion without rewriting raw timestamps", async () => {
    const f = fixture(), native = f.sources.native;
    f.sources.native = async b => { await Promise.resolve(); f.advance(25); return native(b); };
    f.sources.activity = async () => { await Promise.resolve(); return { value: "idle", observedAt: NOW + 25 }; };
    f.sources.transcriptPath = async () => { f.advance(25); return "/private/native.jsonl"; };
    const result = await f.observer().observe("node");
    expect(result.holds).toEqual([]);
    expect(result.native.observedAt).toBe(NOW + 25);
    expect(result.activity.observedAt).toBe(NOW + 25);
    expect(result.observedAt).toBe(NOW + 50);
    expect(result.usage?.observedAt).toBe(Date.parse(stamp(2)));
  });
  it("refuses native proof aged during asynchronous final binding read", async () => {
    const f = fixture(), binding = f.sources.binding; let calls = 0;
    f.sources.binding = async id => { if (++calls === 2) f.advance(5001); return binding(id); };
    const result = await f.observer().observe("node");
    expect(result.holds).toContain("native-proof-unavailable");
    expect(result.holds).toContain("activity-stale");
    expect(result.native.verified).toBe(false);
    expect(result.usage).toBeNull();
  });
  it("preserves original samples and complete zero count without rescanning unchanged history", async () => {
    const f = fixture(), o = f.observer(), a = await o.observe("node");
    expect(a.holds).toEqual([]); expect(a.usage?.usedPercent).toBe(85); expect(a.compactions?.count).toBe(0);
    f.advance(1000); const b = await o.observe("node");
    expect(b.usage).toEqual(a.usage); expect(b.compactions).toEqual(a.compactions);
    expect(f.offsets.slice(1).every(offset => offset > 0)).toBe(true);
  });
  it("withholds an incomplete cold scan and resumes from byte offsets", async () => {
    const f = fixture(), o = new ContextRefreshObserver(f.sources, { now: () => NOW, maxBytesPerObservation: 64 });
    expect((await o.observe("node")).compactions).toBeNull();
    let result = await o.observe("node");
    for (let i = 0; i < 30 && !result.compactions; i++) result = await o.observe("node");
    expect(result.compactions?.count).toBe(0); expect(result.usage?.usedPercent).toBe(85);
    expect(f.offsets[1]).toBe(64);
  });
  it("counts only successful native compactions; restart recomputes same absolute count", async () => {
    const f = fixture([meta, turn(), usage(), compact(), { type: "event_msg", timestamp: stamp(4), payload: { type: "compaction_failed" } }, compact(5), usage(6)]);
    const a = await f.observer().observe("node"), b = await f.observer().observe("node");
    expect(a.compactions?.count).toBe(2); expect(b.compactions).toEqual(a.compactions); expect(a.usage?.observedAt).toBe(Date.parse(stamp(6)));
  });
  it("invalidates precompaction usage and accepts only a later native sample", async () => {
    const f = fixture(), o = f.observer(); await o.observe("node"); f.append(compact());
    expect((await o.observe("node")).usage).toBeNull(); f.append(usage(4));
    expect((await o.observe("node")).usage?.observedAt).toBe(Date.parse(stamp(4)));
  });
  it("rejects equal-time precompaction usage", async () => {
    const f = fixture([meta, turn(), compact(3), usage(3)]); expect((await f.observer().observe("node")).usage).toBeNull();
  });
  it("invalidates old model usage even with the same denominator", async () => {
    const f = fixture(), o = f.observer(); await o.observe("node");
    f.append({ ...turn("other"), timestamp: stamp(4) }); f.setBinding({ model: "other" });
    expect((await o.observe("node")).usage).toBeNull(); f.append(usage(5));
    expect((await o.observe("node")).usage?.usedPercent).toBe(85);
  });
  it.each([2000, -1, 0])("rejects model-denominator mismatch/invalid window %s", async window => {
    const f = fixture([meta, turn(), usage(2, window)]); expect((await f.observer().observe("node")).usage).toBeNull();
  });
  it("holds old thread and native-generation evidence", async () => {
    const f = fixture(); f.setBinding({ nativeId: "new-thread" }); const a = await f.observer().observe("node");
    expect(a.compactions).toBeNull(); expect(a.holds).toContain("compaction-evidence-invalid");
    f.sources.native = async b => ({ ...b, generation: "old", verified: true, observedAt: NOW, launchId: null, fingerprint: "old" });
    const b = await f.observer().observe("node"); expect(b.native.verified).toBe(false); expect(b.usage).toBeNull();
  });
  it.each([-1, 2, 0.5])("never credits invalid/nonmonotone explicit compact count %s as zero", async count => {
    const f = fixture([meta, turn(), compact(3, count)]), r = await f.observer().observe("node");
    expect(r.compactions).toBeNull(); expect(r.holds).toContain("compaction-evidence-invalid");
  });
  it("rejects nonmonotone timestamps and malformed records", async () => {
    const f = fixture([meta, turn(), compact(4), compact(3)]); expect((await f.observer().observe("node")).compactions).toBeNull();
    f.raw(JSON.stringify(meta) + "\n{bad}\n"); expect((await f.observer().observe("node")).compactions).toBeNull();
  });
  it("withholds partial records until completed, and holds truncation/replacement", async () => {
    const f = fixture(), o = f.observer(); await o.observe("node");
    f.raw([meta, turn(), usage()].map(r => JSON.stringify(r) + "\n").join("") + '{"type":');
    expect((await o.observe("node")).compactions).toBeNull();
    f.raw(JSON.stringify(meta) + "\n"); expect((await o.observe("node")).compactions).toBeNull();
    const g = fixture(), p = g.observer(); await p.observe("node"); g.replace();
    expect((await p.observe("node")).compactions).toBeNull();
  });
  it("revalidates current binding after source reads and refuses stale usage/activity", async () => {
    const f = fixture(); let reads = 0; const original = f.sources.binding;
    f.sources.binding = async id => { const b = await original(id); return ++reads > 1 && b ? { ...b, generation: "next" } : b; };
    expect((await f.observer().observe("node")).holds).toContain("binding-changed");
    const g = fixture(); g.advance(130000); g.sources.activity = async () => ({ value: "idle", observedAt: NOW });
    const r = await g.observer().observe("node"); expect(r.holds).toContain("usage-stale"); expect(r.holds).toContain("activity-stale");
  });
  it("reports Pi unsupported, missing usage and persistent negative readiness honestly", async () => {
    const f = fixture(); f.setBinding({ runtime: "pi", nativeId: "/pi/session", model: "provider/model" });
    const model = { provider: "provider", id: "model", contextWindow: 1000 };
    const raw = { ready: true, launchId: "launch", sessionFile: "/pi/session", lastEntryId: "leaf", model,
      quiescence: { generation: "gen", launchId: "launch", sessionFile: "/pi/session", lastEntryId: "leaf", settled: true, observedAt: stamp(2) },
      runtimeReadiness: { generation: "gen", launchId: "launch", sessionFile: "/pi/session", model, observedAt: stamp(2), failures: [{ code: "compaction_failed", observedAt: stamp(1) }] } };
    f.setPi(raw); const o = f.observer(), a = await o.observe("node");
    expect(a.capability).toBe("unsupported"); expect(a.holds).toContain("runtime-not-ready"); expect(a.holds).toContain("usage-unavailable"); expect(a.compactions).toBeNull();
    f.advance(130000); const b = await o.observe("node"); expect(b.holds).toContain("runtime-not-ready");
    f.setPi({ ...raw, runtimeReadiness: { ...raw.runtimeReadiness, failures: [], context: { source: "assistant_usage", usedTokens: 800, remainingTokens: 200, observedAt: stamp(2) } } });
    const c = await o.observe("node"); expect(c.usage?.usedPercent).toBe(80); expect(c.usage?.observedAt).toBe(Date.parse(stamp(2))); expect(c.holds).toContain("usage-stale"); expect(c.holds).toContain("runtime-unsupported");
  });
});

function piFixture() {
 const f=fixture(),file="/private/pi/session.jsonl",header="pi-header";
 const rows:any[]=[{type:"session",id:header,timestamp:stamp(0)},{type:"model_change",id:"m",timestamp:stamp(1),provider:"provider",modelId:"model"},
  {type:"thinking_level_change",id:"t",timestamp:stamp(2),thinkingLevel:"high"}];
 f.raw(rows.map(r=>JSON.stringify(r)+"\n").join(""));f.setBinding({runtime:"pi",nativeId:file,model:"provider/model"});
 let baseline:string|null=null,proof=true;
 const state:any={ready:true,launchId:"launch",sessionFile:file,lastEntryId:"t",model:{provider:"provider",id:"model",contextWindow:1000},
  quiescence:{launchId:"launch",generation:"gen",sessionFile:file,lastEntryId:"t",settled:true,observedAt:new Date(NOW).toISOString()},
  runtimeReadiness:{launchId:"launch",generation:"gen",sessionFile:file,model:{provider:"provider",id:"model",contextWindow:1000},observedAt:new Date(NOW).toISOString(),failures:[]}};
 f.setPi(state);
 f.sources.piContract=async()=>proof?{runtime:"pi",provider:"provider",model:"model",thinkingLevel:"high",trust:"no-approve",agentDir:"/private/pi",generation:"gen",launchId:"launch",
  nativeId:header+"\n"+file,sessionFile:file,sessionHeaderId:header,sessionSha256:createHash("sha256").update(f.bytes()).digest("hex"),configurationDigest:"digest"}:null;
 f.sources.piBaseline=()=>baseline;
 const add=(id:string,type="compaction")=>{const row={type,id,timestamp:stamp(3+rows.length),summary:"completed native summary",firstKeptEntryId:"m",tokensBefore:800};rows.push(row);f.append(row);state.lastEntryId=id;state.quiescence.lastEntryId=id;};
 return {...f,rows,state,add,setBaseline:(v:string)=>{baseline=v;},deny:()=>{proof=false;}};
}
describe("Pi complete native history and bounded cache",()=>{
 it("Pi counts only completed native entries with file/header/id/line hashes and preserves raw sample times",async()=>{
  const f=piFixture(),o=f.observer();const start=await o.observe("node");expect(start.capability).toBe("pi-reserved-fresh");expect(start.compactions?.count).toBe(0);
  f.setBaseline(start.compactions!.cursor);f.add("failed","compaction_failed");f.add("one");f.add("two");
  const r=await o.observe("node");expect(r.compactions?.count).toBe(2);expect(r.holds).toEqual(["usage-unavailable"]);
  const c=JSON.parse(r.compactions!.cursor);expect(c.sessionFile).toBe("/private/pi/session.jsonl");expect(c.sessionHeaderId).toBe("pi-header");expect(c.compactions.map((v:any)=>v.entryId)).toEqual(["one","two"]);
  expect(c.compactions.every((v:any)=>/^[a-f0-9]{64}$/.test(v.lineSha256))).toBe(true);
  f.advance(1000);expect((await o.observe("node")).compactions).toEqual(r.compactions);
  expect((await f.observer().observe("node")).compactions).toEqual(r.compactions);
 });
 it("Pi refuses same-inode prefix edits across warm cache and cold durable baseline, duplicate success and partial history",async()=>{
  const f=piFixture(),o=f.observer();f.add("one");const first=await o.observe("node");f.setBaseline(first.compactions!.cursor);
  f.raw(f.bytes().toString().replace("completed native summary","replaced native summary"));f.add("two");
  expect((await o.observe("node")).compactions).toBeNull();expect((await f.observer().observe("node")).compactions).toBeNull();
  const g=piFixture();g.add("same");g.add("same");expect((await g.observer().observe("node")).compactions).toBeNull();
  const h=piFixture();h.raw(h.bytes().toString()+'{"type":');expect((await h.observer().observe("node")).compactions).toBeNull();
 });
 it("Pi cold scans remain unknown until complete and absent OS proof never grants capability",async()=>{
  const f=piFixture();f.add("one");const o=new ContextRefreshObserver(f.sources,{now:()=>NOW,maxBytesPerObservation:64});
  expect((await o.observe("node")).compactions).toBeNull();let r=await o.observe("node");
  for(let i=0;i<30&&!r.compactions;i++)r=await o.observe("node");expect(r.compactions?.count).toBe(1);
  f.deny();expect(await o.observe("node")).toMatchObject({capability:"unsupported",compactions:null});
 });
 it("Pi history cache is capped at64 and evicted history cold-rescans under its durable baseline",async()=>{
  const f=piFixture(),o=f.observer();const first=await o.observe("node");f.setBaseline(first.compactions!.cursor);
  // Drive real distinct keys with matching contracts; map size is not an admission signal.
  const base=f.sources.piContract!;f.sources.piContract=async b=>({...await base(b),generation:b.generation} as PiRotationContract);
  for(let i=0;i<65;i++){f.setBinding({generation:`g${i}`});await o.observe("node");}
  expect((o as any).piScans.size).toBe(64);f.setBinding({generation:"gen"});
  f.raw(f.bytes().toString().replace('"id":"t"','"id":"x"'));
  expect((await o.observe("node")).compactions).toBeNull();
 });

 it("samples native activity after a slow current-contract read",async()=>{
  const f=piFixture(),contract=f.sources.piContract!;
  f.sources.piContract=async b=>{f.advance(6000);return contract(b);};
  const result=await f.observer().observe("node");
  expect(result.native.verified).toBe(true);
  expect(result.native.observedAt).toBe(NOW+6000);
  expect(result.activity.observedAt).toBe(NOW+6000);
  expect(result.holds).not.toContain("native-proof-unavailable");
  expect(result.holds).not.toContain("activity-stale");
 });

 it("classifies Pi contract read failure as unsupported runtime, not corrupt history",async()=>{
  const f=piFixture();
  f.sources.piContract=async()=>{throw new Error("native contract unavailable");};
  const result=await f.observer().observe("node");
  expect(result.holds).toContain("runtime-unsupported");
  expect(result.holds).not.toContain("compaction-evidence-invalid");
  expect(result.compactions).toBeNull();
 });
});

it("cold catchup drains both real model metadata and observer history responsively without duplicate jobs, probes or partial decisions",async()=>{
 const tick=()=>new Promise<void>(resolve=>setImmediate(resolve));
 // Earlier cold-scan fixtures have finished parsing, but their scheduled
 // cleanup needs an event-loop turn before this test owns both job slots.
 await tick();
 // The shared limiter deduplicates keys, caps concurrent jobs and stops at its
 // finite work budget. Steps represent already bounded parser chunks.
 const a={},b={},c={};let aCalls=0,bCalls=0,cCalls=0,duplicate=0;
 startContextRefreshCatchup(a,()=>{aCalls++;return{bytes:512*1024*1024,complete:false};});
 startContextRefreshCatchup(a,()=>{duplicate++;return{bytes:0,complete:true};});
 startContextRefreshCatchup(b,()=>{bCalls++;return{bytes:1,complete:true};});
 startContextRefreshCatchup(c,()=>{cCalls++;return{bytes:1,complete:true};});
 for(let i=0;i<6;i++)await tick();expect([aCalls,bCalls,cCalls,duplicate]).toEqual([2,1,0,0]);
 const dir=realpathSync(mkdtempSync(join(tmpdir(),"refresh-cold-catchup-"))),file=join(dir,"native.jsonl");
 try {
  const row=(r:unknown)=>JSON.stringify(r)+"\n";
  writeFileSync(file,row(meta)+row(turn()));
  const filler=row({type:"response_item",payload:{type:"message",text:"x".repeat(60000)}});
  for(let i=0;i<350;i++)appendFileSync(file,filler);
  appendFileSync(file,row(compact(3))+row(usage(4)));
  const size=statSync(file).size;expect(size).toBeGreaterThan(2*8*1024*1024);
  const reader=new NativeModelReader();expect(reader.read(file,"thread","binding-one")).toBeNull();
  const f=fixture(),originalNative=f.sources.native;let probes=0,activityCalls=0,maxRead=0;
  f.sources.native=async binding=>{probes++;return originalNative(binding);};
  f.sources.activity=async()=>{activityCalls++;return{value:"idle",observedAt:NOW};};
  f.sources.transcriptPath=async()=>file;
  f.sources.readTranscript=(path,offset,limit)=>{maxRead=Math.max(maxRead,limit);return readContextRefreshTranscript(path,offset,limit);};
  const observer=new ContextRefreshObserver(f.sources,{now:()=>NOW,maxBytesPerObservation:64*1024});
  const incomplete=await observer.observe("node");expect(incomplete.compactions).toBeNull();expect(incomplete.usage).toBeNull();
  let responsiveTicks=0;
  for(;responsiveTicks<1000;responsiveTicks++) {
   const scan=[...(observer as any).scans.values()][0] as any;
   const model=[...(reader as any).entries.values()][0] as any;
   if(scan.complete&&model.offset===size)break;
   await tick();
  }
  expect(responsiveTicks).toBeGreaterThan(2);expect(responsiveTicks).toBeLessThan(1000);
  expect([probes,activityCalls]).toEqual([1,1]);expect(maxRead).toBeLessThanOrEqual(128*1024);
  expect(reader.read(file,"thread","binding-one")).toEqual({model:"model",contextWindow:1000});
  const complete=await observer.observe("node");expect(complete.compactions?.count).toBe(1);expect(complete.usage?.observedAt).toBe(Date.parse(stamp(4)));
  // A superseded metadata binding cannot receive completion from the old job.
  const another=new NativeModelReader();another.read(file,"thread","old-binding");
  const old=[...(another as any).entries.values()][0] as any,oldOffset=old.offset;
  another.read(file,"thread","new-binding");for(let i=0;i<8;i++)await tick();
  expect(old.offset).toBe(oldOffset);expect(another.read(file,"thread","new-binding")).toEqual({model:"model",contextWindow:1000});
  // A changed daemon binding cancels work even without another observation.
  let current=true;const invalidated=new NativeModelReader();invalidated.read(file,"thread","bound",()=>current);
  const cancelled=[...(invalidated as any).entries.values()][0] as any,cancelledOffset=cancelled.offset;current=false;
  for(let i=0;i<4;i++)await tick();expect(cancelled.offset).toBe(cancelledOffset);
  // A replacement inode during catchup cannot publish completed source facts.
  const replaced=new ContextRefreshObserver(f.sources,{now:()=>NOW,maxBytesPerObservation:64*1024});
  expect((await replaced.observe("node")).compactions).toBeNull();
  renameSync(file,file+".retained");writeFileSync(file,row(meta)+row(turn())+row(usage()));
  for(let i=0;i<4;i++)await tick();
  expect((await replaced.observe("node")).compactions).toBeNull();
 } finally {rmSync(dir,{recursive:true,force:true});}
});
