import {describe,it,expect} from 'vitest';
import {validatePiDetachedHistory} from '../src/domain/pi-detached-resume-integration.js';
import type {PiDetachedBinding} from '../src/domain/pi-detached-resume.js';
const binding={nodeId:'lead',sessionId:'s',sessionName:'lead@rig',generation:'g',runtime:'pi',nativeId:'/saved/session.jsonl',cwd:'/project',model:'openrouter/declared',effort:null,profile:'default',launchPosture:null} as PiDetachedBinding;
const bytes=(rows:unknown[])=>Buffer.from(rows.map(x=>JSON.stringify(x)).join('\n')+'\n');
const header={type:'session',id:'native',cwd:'/project'};
describe('Pi detached retained-history parser',()=>{
 it('preserves legitimate model fallback history independently of declared launch pin',()=>{const data=bytes([header,{type:'model_change',id:'a',parentId:null,provider:'openrouter',modelId:'fallback'},{type:'thinking_level_change',id:'b',parentId:'a',thinkingLevel:'max'}]);expect(validatePiDetachedHistory(binding,data)).toEqual({nativeIdentity:'native',lastEntryId:'b'});expect(binding.model).toBe('openrouter/declared');});
 it.each(['truncated','wrong-cwd','duplicate','missing-parent','invalid-json'] as const)('holds malformed %s retained history',kind=>{let data=bytes([header,{type:'message',id:'a',parentId:null}]);if(kind==='truncated')data=data.subarray(0,-1);if(kind==='wrong-cwd')data=bytes([{...header,cwd:'/other'},{id:'a',parentId:null}]);if(kind==='duplicate')data=bytes([header,{id:'a',parentId:null},{id:'a',parentId:'a'}]);if(kind==='missing-parent')data=bytes([header,{id:'a',parentId:'not-present'}]);if(kind==='invalid-json')data=Buffer.from('not-json\n');expect(()=>validatePiDetachedHistory(binding,data)).toThrow();});
});

import {readFileSync,mkdtempSync,mkdirSync,rmSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createPiDetachedResumeIntegration} from '../src/domain/pi-detached-resume-integration.js';
it('Pi global census holds stripped native/file aliases and unreadable or ambiguous processes',()=>{
 const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8'),program=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!;
 const root=mkdtempSync(path.join(tmpdir(),'pi-census-')),home=path.join(root,'home'),foreign=path.join(root,'foreign');mkdirSync(home);mkdirSync(foreign);
 const fixture=String.raw`
def argv_env(pid):
 c=json.loads(os.environ['PI_CENSUS_FIXTURE'])
 if c.get('unreadable'): raise ValueError()
 return c['argv'],[x.encode() for x in c['env']],c['exe']
subprocess.check_output=lambda *a,**k: ('4321 '+str(os.getuid())+' S\n').encode()
os.kill=lambda *a: None
`;
 const run=(argv:string[],env:string[]=[],exe='/bin/node',extra={})=>execFileSync('python3',['-c',program.replace('ok=False',fixture+'\nok=False'),JSON.stringify({OPENRIG_NODE_ID:'lead',OPENRIG_SESSION_NAME:'lead@rig',OPENRIG_HOME:home}),'0','/saved/session.jsonl','pi'],{encoding:'utf8',env:{...process.env,PI_CENSUS_FIXTURE:JSON.stringify({argv,env,exe,...extra})}}).trim();
 try{expect(run(['/bin/node','pi-runner.js','--session','/saved/session.jsonl'])).toBe('0');expect(run(['/bin/pi','--session=/saved/session.jsonl'],[],'/bin/pi')).toBe('0');expect(run(['/bin/node','pi-runner.js','--session','/saved/other'])).toBe('1');expect(run(['/bin/node','pi-runner.js'])).toBe('0');expect(run(['/bin/zsh'],['OPENRIG_NODE_ID=lead'])).toBe('0');expect(run(['/bin/zsh'],['OPENRIG_SESSION_NAME=lead@rig'])).toBe('0');expect(run(['/bin/zsh'],['OPENRIG_NODE_ID=other','OPENRIG_SESSION_NAME=lead@rig','OPENRIG_HOME='+foreign])).toBe('1');expect(run(['/bin/zsh'],[], '/bin/zsh',{unreadable:true})).toBe('0');expect(run(['/bin/zsh'],['OPENRIG_NODE_ID=x','OPENRIG_NODE_ID=y'])).toBe('0');}finally{rmSync(root,{recursive:true,force:true});}
});
it('production preflight freezes explicit restore policy and declared model without consulting absent historical posture',async()=>{
 const runner=path.resolve('packages/daemon/src/adapters/pi-runner.ts');let posture:'floor'|'full_bypass'='floor';
 const deps={db:{},guard:{ownsRunnerRehost:()=>true,target:()=>({session:binding.sessionName,occupant:binding.generation,pane:'%1'})},tmux:{},resume:{},store:{assertReady:()=>{}},launchEnvironment:{usesNativeDuty:async()=>true},launchPath:process.env.PATH!,stateRoot:'/tmp/pi-test-state',runnerEntryPath:runner,piProve:async()=>null,piRunnerState:()=>null,configurationDigest:()=> 'frozen-config',resolvePosture:()=>posture,snapshotRoot:'/tmp/pi-test-snapshot'};
 const engine=createPiDetachedResumeIntegration(deps as unknown as Parameters<typeof createPiDetachedResumeIntegration>[0]);const options=(engine as unknown as {deps:{preflight:(b:PiDetachedBinding,d:boolean)=>Promise<{digest:string;posture:string}>}}).deps;
 const floor=await options.preflight(binding,true);expect(floor.posture).toBe('floor');expect(await options.preflight(binding,false)).toEqual(floor);posture='full_bypass';expect((await options.preflight(binding,true)).digest).not.toBe(floor.digest);expect(binding.model).toBe('openrouter/declared');
});

import {piSupervisorCandidates} from '../src/domain/pi-detached-resume-integration.js';
it('Pi supervisor candidate filter checks only node descendants of exact pane',()=>{
 const rows=[{pid:1,ppid:0,executableName:'node'},{pid:10,ppid:1,executableName:'zsh'},{pid:12,ppid:11,executableName:'node'},{pid:11,ppid:10,executableName:'node'},{pid:13,ppid:11,executableName:'pi'},{pid:20,ppid:1,executableName:'node'}].map(row=>({...row,command:row.executableName,startedAt:'start'}));
 expect(piSupervisorCandidates(rows,10).map(row=>row.pid)).toEqual([12,11]);expect(piSupervisorCandidates(rows,99)).toEqual([]);
});
it('Pi native CLI script census holds relative missing malformed or duplicate session arguments',()=>{
 const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8'),program=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!;
 const fixture=String.raw`
def argv_env(pid):
 return json.loads(os.environ['PI_CENSUS_ARGV']),[], '/usr/bin/node'
subprocess.check_output=lambda *a,**k: ('4321 '+str(os.getuid())+' S\n').encode()
`;
 const run=(args:string[])=>execFileSync('python3',['-c',program.replace('ok=False',fixture+'\nok=False'),JSON.stringify({OPENRIG_NODE_ID:'lead',OPENRIG_SESSION_NAME:'lead@rig',OPENRIG_HOME:null}),'0','/saved/session.jsonl','pi'],{encoding:'utf8',env:{...process.env,PI_CENSUS_ARGV:JSON.stringify(['/usr/bin/node','/opt/node_modules/pi-coding-agent/dist/cli.js',...args])}}).trim();
 for(const args of [[],['--session'],['--session','relative.jsonl'],['--session='],['--session','--help'],['--session','/other','--session','/third']])expect(run(args)).toBe('0');
 expect(run(['--session','/saved/session.jsonl'])).toBe('0');expect(run(['--session','/other/session.jsonl'])).toBe('1');
});

it('Pi census distinguishes unrelated absolute Oh My Pi runtime while preserving exact identity and selector holds',()=>{
 const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8'),program=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!;
 const fixture=String.raw`
def argv_env(pid):
 c=json.loads(os.environ['PI_RUNTIME_FIXTURE'])
 return c['argv'],[x.encode() for x in c['env']],c['exe']
subprocess.check_output=lambda *a,**k: ('30717 '+str(os.getuid())+' S\n').encode()
`;
 const omp='/opt/node_modules/@oh-my-pi/pi-coding-agent/dist/cli.js',pi='/opt/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js';
 const run=(script:string,args:string[]=[],env:string[]=[],exe='/opt/bin/bun')=>execFileSync('python3',['-c',program.replace('ok=False',fixture+'\nok=False'),JSON.stringify({OPENRIG_NODE_ID:'lead',OPENRIG_SESSION_NAME:'lead@rig',OPENRIG_HOME:null}),'0','/saved/session.jsonl','pi'],{encoding:'utf8',env:{...process.env,PI_RUNTIME_FIXTURE:JSON.stringify({argv:[exe,script,...args],env,exe})}}).trim();
 expect(run(omp)).toBe('1');expect(run(omp,[],['OPENRIG_NODE_ID=lead'])).toBe('0');expect(run(omp,['--session','/saved/session.jsonl'])).toBe('0');
 for(const args of [['--session','relative.jsonl'],['--session'],['--session='],['--session','--help'],['--session','/other','--session','/third'],['--continue'],['--resume'],['--session-dir','/other'],['--mode','rpc']])expect(run(omp,args),JSON.stringify(args)).toBe('0');
 expect(run(pi,[],[],'/usr/bin/node')).toBe('0');expect(run(pi,['--session','relative'],[],'/usr/bin/node')).toBe('0');expect(run('node_modules/pi-coding-agent/dist/cli.js',[],[],'/usr/bin/node')).toBe('0');
});
