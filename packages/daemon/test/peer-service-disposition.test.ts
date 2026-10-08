import { describe,it,expect } from 'vitest';
import { execFileSync,spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync,realpathSync,writeFileSync,readFileSync,rmSync,mkdirSync,chmodSync,readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { PeerServiceDispositionStore,SERVICE_PROCESS_PY,sampleServiceProcess,type ServiceProcessSample } from '../src/domain/peer-service-disposition.js';
const hash=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8');
const census=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!;
const home='/private/service-test-home',identity={OPENRIG_NODE_ID:'service-test-node',OPENRIG_SESSION_NAME:'service-test@rig',OPENRIG_HOME:home,OPENRIG_OCCUPANT_GENERATION:'old',OPENRIG_RUNTIME:'pi'};
const pythonHash=(v:unknown)=>hash(JSON.stringify(v));
const root:ServiceProcessSample={pid:4321,ppid:1,uid:process.getuid!(),boot:hash('boot'),start:'100',image:{path:'/compiled/service-test',device:'1',inode:'2',size:50,mtime:'1',ctime:'1',sha256:hash('service'),compiled:true},argvHash:pythonHash(['/compiled/service-test']),identityHash:pythonHash(identity),loader:false,hazard:false,identity};
const clone=<T>(v:T):T=>JSON.parse(JSON.stringify(v));
const disposition=(sample=root)=>({schema:'peer-service-disposition-v1',nodeId:identity.OPENRIG_NODE_ID,sessionName:identity.OPENRIG_SESSION_NAME,home,approvedArtifactSha256:sample.image.sha256,process:sample});
function fakeCensus(samples:ServiceProcessSample[],records:unknown[]=[disposition()],changes:Record<string,unknown>={}){
 const fixture=String.raw`
c=json.loads(os.environ['SERVICE_CENSUS_FIXTURE'])
counts={}
def kernel_instance(pid):
 if c.get('kernelUnknown')==pid or c.get('kernelDenied')==pid: raise ValueError()
 row=next((x for x in c['samples'] if x['pid']==pid),None)
 if row is None: raise ProcessLookupError()
 return {k:row[k] for k in ['pid','ppid','uid','boot','start']}
def sample_process(pid):
 counts[pid]=counts.get(pid,0)+1
 if c.get('unreadable')==pid: raise ValueError()
 row=next(x for x in c['samples'] if x['pid']==pid).copy()
 if c.get('rootRace') and pid==4321 and counts[pid]>1: row['start']='999'
 if c.get('childRace') and pid!=4321: raise ValueError()
 return row
def argv_env(pid):
 if c.get('argvUnreadable')==pid: raise ValueError()
 row=next(x for x in c['samples'] if x['pid']==pid)
 args=c.get('argv',{}).get(str(pid),[row['image']['path']])
 env=[(k+'='+v).encode() for k,v in row['identity'].items() if v is not None]
 if row['loader']: env.append(b'LD_PRELOAD=private')
 return args,env,row['image']['path']
def image_stamp(executable): return next(x['image'] for x in c['samples'] if x['image']['path']==executable)
def ps_output(args,**kwargs):
 if args[1]=='-p':
  if c.get('psUnreadable'): raise ValueError()
  return c.get('psUid','').encode()
 return ''.join(str(x['pid'])+' '+str(x['uid'])+' S\n' for x in c['samples']).encode()
subprocess.check_output=ps_output
def kill_probe(pid,*a):
 if c.get('killDenied')==pid: raise PermissionError()
 if not any(x['pid']==pid for x in c['samples']): raise ProcessLookupError()
os.kill=kill_probe
`;
 return execFileSync('python3',['-c',SERVICE_PROCESS_PY+census.replace('ok=False',fixture+'\nok=False'),JSON.stringify({OPENRIG_NODE_ID:identity.OPENRIG_NODE_ID,OPENRIG_SESSION_NAME:identity.OPENRIG_SESSION_NAME,OPENRIG_HOME:home}),'0','/saved/native.jsonl','pi',JSON.stringify(records)],{encoding:'utf8',env:{...process.env,SERVICE_CENSUS_FIXTURE:JSON.stringify({samples,...changes})}}).trim();
}
describe('exact peer service census disposition',()=>{
 it('requires independently confirmed foreign UID after kernel and signal permission failures',()=>{
  const foreign={...clone(root),uid:root.uid+1};
  const denied={kernelDenied:root.pid,killDenied:root.pid};
  expect(fakeCensus([foreign],[disposition()],{...denied,psUid:String(foreign.uid),argvUnreadable:root.pid})).toBe('1');
  for(const psUid of [String(root.uid),'','not-a-uid',String(foreign.uid)+'\n'+String(foreign.uid)]){
   expect(fakeCensus([foreign],[disposition()],{...denied,psUid})).toBe('0');
  }
  expect(fakeCensus([foreign],[disposition()],{...denied,psUnreadable:true})).toBe('0');
  expect(fakeCensus([root],[disposition()],{...denied,psUid:String(root.uid)})).toBe('0');
  // A stale record never exempts a replacement carrying the target identity.
  expect(fakeCensus([{...clone(root),start:'200'}],[disposition()])).toBe('0');
 });
 it('ignores exited disposition instances without granting a waiver',()=>{
  expect(fakeCensus([])).toBe('1');
 });
 it.each(['start','boot','uid'] as const)('ignores stale %s instances before inaccessible argv sampling',kind=>{
  const observed=clone(root);observed.identity={...observed.identity,OPENRIG_NODE_ID:null,OPENRIG_SESSION_NAME:null};
  if(kind==='start')observed.start='200';if(kind==='boot')observed.boot=hash('other boot');if(kind==='uid')observed.uid++;
  expect(fakeCensus([observed],[disposition()],{unreadable:observed.pid,...(kind==='uid'?{argvUnreadable:observed.pid}:{})})).toBe('1');
  if(kind!=='uid')expect(fakeCensus([{...observed,identity:root.identity}],[disposition()],{unreadable:observed.pid})).toBe('0');
 });
 it('holds an exact unreadable instance and unknown kernel observation',()=>{
  expect(fakeCensus([root],[disposition()],{unreadable:root.pid})).toBe('0');
  expect(fakeCensus([root],[disposition()],{kernelUnknown:root.pid})).toBe('0');
  const reused={...clone(root),start:'200'};
  expect(fakeCensus([reused],[disposition()],{argvUnreadable:root.pid})).toBe('0');
 });
 it('requires explicit evidence for each instance; never covers identity-bearing descendants',()=>{
  const child={...clone(root),pid:4322,ppid:4321,start:'101'};
  expect(fakeCensus([root],[])).toBe('0');expect(fakeCensus([root])).toBe('1');expect(fakeCensus([root,child])).toBe('0');
 });
 it.each(['different-image','grandchild','early-start','different-identity','loader','unreadable','child-race'] as const)('holds %s child',kind=>{
  const child={...clone(root),pid:4322,ppid:4321,start:'101'};let changes={};
  if(kind==='different-image')child.image={...child.image,inode:'other'};
  if(kind==='grandchild')child.ppid=4320;
  if(kind==='early-start')child.start='99';
  if(kind==='different-identity'){child.identity.OPENRIG_OCCUPANT_GENERATION='different';child.identityHash=pythonHash(child.identity);}
  if(kind==='loader')child.loader=true;
  if(kind==='unreadable')changes={unreadable:child.pid};
  if(kind==='child-race')changes={childRace:true};
  expect(fakeCensus([root,child],[disposition()],changes)).toBe('0');
 });
 it.each(['start','boot','image','root-race','conflict','scope','wrapper','loader','native-file'] as const)('holds %s root',kind=>{
  const observed=clone(root);let records:unknown[]=[disposition()],changes={};
  if(kind==='start')observed.start='200';if(kind==='boot')observed.boot=hash('reboot');if(kind==='image')observed.image.sha256=hash('changed');
  if(kind==='root-race')changes={rootRace:true};if(kind==='conflict')records=[disposition(),disposition()];if(kind==='scope')records=[{...disposition(),home:'/other'}];
  if(kind==='wrapper'){observed.hazard=true;records=[disposition(observed)];}
  if(kind==='loader'){observed.loader=true;records=[disposition(observed)];}
  if(kind==='native-file'){observed.argvHash=pythonHash([observed.image.path,'/saved/native.jsonl']);records=[disposition(observed)];changes={argv:{[observed.pid]:[observed.image.path,'/saved/native.jsonl']}};}
  expect(fakeCensus([observed],records,changes)).toBe('0');
 });
});

describe('private immutable peer-service producer',()=>{
 it('re-samples, re-authorizes and binds private immutable evidence, refusing drift/tamper',async()=>{
  const dir=realpathSync(mkdtempSync(path.join(tmpdir(),'peer-service-store-')));const evidence=path.join(dir,'evidence.json');writeFileSync(evidence,'{"approved":true}');
  const processSample={...clone(root),identity:{...identity,OPENRIG_HOME:dir}};processSample.identityHash=pythonHash(processSample.identity);
  const input={nodeId:identity.OPENRIG_NODE_ID,sessionName:identity.OPENRIG_SESSION_NAME,actor:'operator-agent@kernel',actorGeneration:'operator-generation',purpose:'independently approved test artifact',evidencePath:evidence,evidenceSha256:hash(readFileSync(evidence)),approvedArtifactSha256:root.image.sha256,pid:root.pid};
  try{const store=new PeerServiceDispositionStore(dir);let auth=0;await store.record(input,()=>{auth++;},async()=>clone(processSample));expect(auth).toBe(2);expect(store.read(input.nodeId,input.sessionName)).toHaveLength(1);
   await store.record(input,()=>{},async()=>clone(processSample));expect(readdirSync(store.root)).toHaveLength(1);
   await expect(store.record({...input,purpose:'conflicting judgment'},()=>{},async()=>clone(processSample))).rejects.toThrow(/conflicting/);
   await expect(store.record({...input,approvedArtifactSha256:hash('wrong')},()=>{},async()=>clone(processSample))).rejects.toThrow();
   let count=0;await expect(store.record(input,()=>{},async()=>({...clone(processSample),start:count++?'201':'100'}))).rejects.toThrow(/changed/);
   await expect(store.record(input,()=>{throw new Error('not peer');},async()=>clone(processSample))).rejects.toThrow(/not peer/);
   const file=path.join(store.root,readdirSync(store.root)[0]!);chmodSync(file,0o644);expect(()=>store.read(input.nodeId,input.sessionName)).toThrow(/private/);chmodSync(file,0o600);
   writeFileSync(evidence,'changed');expect(store.read(input.nodeId,input.sessionName)).toHaveLength(1);
   await expect(store.record(input,()=>{},async()=>clone(processSample))).rejects.toThrow(/evidence-changed/);
   rmSync(evidence);expect(store.read(input.nodeId,input.sessionName)).toHaveLength(1);
  }finally{rmSync(dir,{recursive:true,force:true});}
 });
});

it('samples a real compiled service and direct fork through actual platform kernel APIs',async()=>{
 const dir=realpathSync(mkdtempSync(path.join(tmpdir(),'peer-service-kernel-'))),file=path.join(dir,'service-proof'),c=path.join(dir,'proof.c');let processHandle:ChildProcess|undefined,child=0;
 writeFileSync(c,'#include <unistd.h>\n#include <stdio.h>\nint main(){pid_t p=fork(); if(p<0)return 1; if(p==0){while(1)pause();} printf("%d\\n",p);fflush(stdout);while(1)pause();}\n');
 try{
  execFileSync('cc',[c,'-o',file],{stdio:'pipe'});
  processHandle=spawn(file,[],{env:{PATH:process.env.PATH,...identity,OPENRIG_HOME:dir},stdio:['ignore','pipe','pipe']});
  child=await new Promise<number>((resolve,reject)=>{let text='';const timeout=setTimeout(()=>reject(new Error('helper timeout')),3000);processHandle!.once('error',reject);processHandle!.stdout!.on('data',b=>{text+=b.toString();if(text.includes('\n')){clearTimeout(timeout);resolve(Number(text.trim()));}});});
  const sampled=await sampleServiceProcess(processHandle.pid!),fork=await sampleServiceProcess(child);
  expect(sampled.image.compiled).toBe(true);expect(fork.ppid).toBe(sampled.pid);expect(fork.image).toEqual(sampled.image);expect(fork.identityHash).toBe(sampled.identityHash);
  const expected={OPENRIG_NODE_ID:identity.OPENRIG_NODE_ID,OPENRIG_SESSION_NAME:identity.OPENRIG_SESSION_NAME,OPENRIG_HOME:dir};
  const fixture=`subprocess.check_output=lambda *a,**k: ${JSON.stringify(sampled.pid+' '+sampled.uid+' S\n'+child+' '+sampled.uid+' S\n')}.encode()\n`;
  const run=(records:unknown[])=>execFileSync('python3',['-c',SERVICE_PROCESS_PY+census.replace('ok=False',fixture+'\nok=False'),JSON.stringify(expected),'0','/saved/native.jsonl','pi',JSON.stringify(records)],{encoding:'utf8'}).trim();
  expect(run([])).toBe('0');expect(run([{...disposition(sampled),home:dir}])).toBe('0');
  // Narrow the fake census enumeration to the exact real root instance.
  const rootOnly=`subprocess.check_output=lambda *a,**k: ${JSON.stringify(sampled.pid+' '+sampled.uid+' S\n')}.encode()\n`;
  expect(execFileSync('python3',['-c',SERVICE_PROCESS_PY+census.replace('ok=False',rootOnly+'\nok=False'),JSON.stringify(expected),'0','/saved/native.jsonl','pi',JSON.stringify([{...disposition(sampled),home:dir}])],{encoding:'utf8'}).trim()).toBe('1');
 }finally{if(child)try{process.kill(child,'SIGKILL');}catch{}processHandle?.kill('SIGKILL');rmSync(dir,{recursive:true,force:true});}
});
