import { ContextUsageStore } from "../src/domain/context-usage-store.js";
import {describe,it,expect,beforeEach,afterEach} from "vitest";
import {spawnSync} from "node:child_process";
import {mkdtempSync,readFileSync,rmSync,existsSync,writeFileSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
const collector=resolve("assets/claude-statusline-context.cjs");
const session="00000000-0000-7000-8000-000000000001",generation="11111111-2222-3333-4444-555555555555";let dir:string;
beforeEach(()=>dir=mkdtempSync(join(tmpdir(),"claude-receipt-")));afterEach(()=>rmSync(dir,{recursive:true,force:true}));
function payload(){return {session_id:session,session_name:"reviewer@pilot",model:{id:"claude-sonnet-5-5"},effort:{level:"high"},context_window:{context_window_size:200000,used_percentage:25,remaining_percentage:75,total_input_tokens:1000,total_output_tokens:50,current_usage:{input_tokens:900}},rate_limits:{five_hour:{used_percentage:12,resets_at:1791050000}}};}
function collect(input:unknown,gen:string|null=generation,target=join(dir,"context.json")){
 const env={...process.env};delete env.OPENRIG_OCCUPANT_GENERATION;delete env.RIGGED_OCCUPANT_GENERATION;env.CLAUDE_CODE_EFFORT_LEVEL="max";if(gen!==null)env.OPENRIG_OCCUPANT_GENERATION=gen;
 const r=spawnSync(process.execPath,[collector,target,join(dir,"provider.json")],{input:typeof input==="string"?input:JSON.stringify(input),encoding:"utf8",env});
 expect(r.status).toBe(0);expect(r.stdout).toBe("");expect(r.stderr).not.toContain("PRIVATE_MARKER");return r;
}
const read=()=>JSON.parse(readFileSync(join(dir,"context.json"),"utf8"));
describe("allowlisted native statusline receipt",()=>{
 it("retains exact complete receipt and legacy context/provider values atomically",()=>{
  const before=Date.now(),p=payload();collect(p);const sample=read(),r=sample.runtime_metadata;
  expect(Object.keys(r).sort()).toEqual(["schema_version","source","sampled_at","session_id","session_name","model_id","effort_level","occupant_generation"].sort());
  expect(r.schema_version).toBe(1);expect(r.source).toBe("claude_statusline_json");expect(r.session_id).toEqual({value:session,availability:"available",reason:"observed"});expect(r.model_id.value).toBe(p.model.id);expect(r.effort_level.value).toBe("high");
  expect(r.occupant_generation).toEqual({value:generation,availability:"available",reason:"observed",provenance:"inherited_environment"});
  expect(Date.parse(r.sampled_at)).toBeGreaterThanOrEqual(before);expect(r.sampled_at).toBe(sample.sampled_at);expect(Date.parse(r.sampled_at)).toBeLessThanOrEqual(Date.now());expect(sample.context_window).toEqual(p.context_window);expect(existsSync(join(dir,"context.json.tmp"))).toBe(false);
  expect(JSON.parse(readFileSync(join(dir,"provider.json"),"utf8")).rateLimits.five_hour).toEqual({usedPercent:12,resetsAt:new Date(1791050000*1000).toISOString()});
 });
 it("legacy normalizer ignores additive receipt without changing usage",()=>{
  collect(payload());const sample=read();const store=new ContextUsageStore(null as never,{stateDir:dir});
  const without={...sample};delete without.runtime_metadata;
  expect(store.normalizeSample(sample)).toEqual(store.normalizeSample(without));
 });
 it("records model and effort changes without changing session",()=>{collect(payload());const p=payload();p.model.id="claude-opus-5-5";p.effort.level="max";collect(p);const r=read().runtime_metadata;expect(r.session_id.value).toBe(session);expect(r.model_id.value).toBe(p.model.id);expect(r.effort_level.value).toBe("max");});
 it("missing context preserves runtime receipt and provider usage",()=>{const p:any=payload();delete p.context_window;collect(p);expect(read().runtime_metadata.model_id.value).toBe(p.model.id);expect(read().context_window).toBeUndefined();expect(existsSync(join(dir,"provider.json"))).toBe(true);});
 it("absence stays missing without invented defaults",()=>{const p:any=payload();delete p.model;delete p.effort;delete p.session_name;collect(p,null);const r=read().runtime_metadata;expect(r.model_id).toEqual({value:null,availability:"unavailable",reason:"missing"});expect(r.effort_level).toEqual(r.model_id);expect(r.session_name).toBeUndefined();expect(r.occupant_generation.value).toBeNull();});
 it.each([{value:null,reason:"null"},{value:3,reason:"malformed"},{value:{level:"ultra"},reason:"unsupported"},{value:{level:"high\nPRIVATE_MARKER"},reason:"malformed"},{value:{level:"x".repeat(300)},reason:"malformed"}])("validates effort %j",({value,reason})=>{const p:any=payload();p.effort=value;collect(p);expect(read().runtime_metadata.effort_level).toEqual({value:null,availability:"unavailable",reason});});
 it.each([{model:null,reason:"null"},{model:7,reason:"malformed"},{model:{},reason:"missing"},{model:{id:42},reason:"malformed"}])("validates model availability %j",({model,reason})=>{const p:any=payload();p.model=model;collect(p);expect(read().runtime_metadata.model_id).toEqual({value:null,availability:"unavailable",reason});});
 it("rejects malformed identity/model/generation and private extras",()=>{const p:any=payload();p.session_id=7;p.session_name="bad\nPRIVATE_MARKER";p.model={id:"bad\nPRIVATE_MARKER",secret:"PRIVATE_MARKER"};p.settings={auth:"PRIVATE_MARKER"};p.transcript="PRIVATE_MARKER";p.tool={body:"PRIVATE_MARKER"};p.effort.extra="PRIVATE_MARKER";collect(p,"bad\nPRIVATE_MARKER");const r=read().runtime_metadata;expect(JSON.stringify(r)).not.toContain("PRIVATE_MARKER");expect(r.model_id.reason).toBe("malformed");expect(r.session_id.reason).toBe("malformed");expect(r.occupant_generation.value).toBeNull();});
 it("old inherited generation is not relabelled current",()=>{collect(payload());expect(read().runtime_metadata.occupant_generation.value).toBe(generation);expect(read().runtime_metadata.occupant_generation.provenance).toBe("inherited_environment");});
 it("malformed JSON logs fixed diagnostic and preserves old timestamp",()=>{collect(payload());const old=readFileSync(join(dir,"context.json"),"utf8"),r=collect('{"PRIVATE_MARKER":');expect(r.stderr.trim()).toBe("[openrig][collector] failed to collect Claude status line");expect(readFileSync(join(dir,"context.json"),"utf8")).toBe(old);});
 it("output errors never leak path/exception text",()=>{const blocked=join(dir,"PRIVATE_MARKER");writeFileSync(blocked,"file");const r=collect(payload(),generation,join(blocked,"out.json"));expect(r.stderr.trim()).toBe("[openrig][collector] failed to collect Claude status line");});
});
