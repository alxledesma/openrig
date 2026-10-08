import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { mkdtempSync, realpathSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { capturePiManagedSpawnProof, managedPiChildArgs, piLaunchArtifact, readPiLaunchProcess, validatePiManagedSpawnProof, type PiLaunchProcess } from '../src/adapters/pi-managed-launch-proof.js';
import { buildPiRunnerArgs, piSeatPaths } from '../src/adapters/pi-runner-protocol.js';
const clone = <T>(v:T):T => JSON.parse(JSON.stringify(v));
it('binds real Node title erasure to exact spawn artifacts/instance and rejects contradictions', async () => {
 const dir=realpathSync(mkdtempSync(path.join(tmpdir(),'pi-launch-proof-')));
 const runnerEntry=path.join(dir,'pi-runner.js'),piEntry=path.join(dir,'pi.js');
 writeFileSync(runnerEntry,'// trusted test runner\n');
 writeFileSync(piEntry,'#!/usr/bin/env node\nprocess.title="pi"; console.log("ready"); setInterval(()=>{},1000);\n');
 const intent={runnerEntryPath:runnerEntry,sessionName:'test@isolated',stateRoot:dir,cwd:dir,launchId:'test-launch',trust:'no-approve' as const};
 const dirs=piSeatPaths(dir,intent.sessionName),childArgs=managedPiChildArgs(intent);
 const identity={OPENRIG_NODE_ID:'test-only',OPENRIG_SESSION_NAME:intent.sessionName,OPENRIG_OCCUPANT_GENERATION:'test-generation',OPENRIG_RUNTIME:'pi'};
 const child=spawn(process.execPath,[piEntry,...childArgs],{cwd:dir,env:{PATH:'/usr/bin:/bin',...identity,PI_CODING_AGENT_DIR:dirs.agentDir,PI_CODING_AGENT_SESSION_DIR:dirs.sessionsDir},stdio:['ignore','pipe','pipe']});
 try {
  await new Promise<void>((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('startup timeout')),3000);child.stdout.once('data',()=>{clearTimeout(timer);resolve();});child.once('error',reject);});
  const observed=readPiLaunchProcess(child.pid!);
  expect(observed.argv.filter(Boolean)).toEqual(['pi']);
  const parent:PiLaunchProcess={...clone(observed),instance:{...observed.instance,pid:process.pid,ppid:1},identity:{...observed.identity,...identity},argv:[process.execPath,...buildPiRunnerArgs(intent)]};
  observed.instance.ppid=process.pid;
  const read=(pid:number)=>clone(pid===process.pid?parent:observed);
  const input={nodeId:identity.OPENRIG_NODE_ID,generation:identity.OPENRIG_OCCUPANT_GENERATION,intent,childPid:child.pid!,piEntry:piLaunchArtifact(piEntry),interpreter:piLaunchArtifact(process.execPath),childArgs};
  const proof=capturePiManagedSpawnProof(input,read);expect(proof).not.toBeNull();
  const expected={...identity,nodeId:input.nodeId,generation:input.generation,...intent,agentDir:dirs.agentDir,sessionFile:path.join(dirs.sessionsDir,'current.jsonl')};
  const check=(p=proof!,r=parent,c=observed)=>validatePiManagedSpawnProof(p,expected,r,c);
  expect(check()).toBe(true);
  const original={...clone(observed),argv:[process.execPath,piEntry,...childArgs]};expect(check(proof!,parent,original)).toBe(true);
  for(const argv of [['pi','--approve'],['zsh'],[process.execPath,piEntry,'--mode','text']])expect(check(proof!,parent,{...clone(observed),argv})).toBe(false);
  for(const field of ['start','boot','uid','ppid'] as const){const c=clone(observed);(c.instance as Record<string,unknown>)[field]=field==='uid'||field==='ppid'?999:'changed';expect(check(proof!,parent,c)).toBe(false);}
  expect(check(proof!,parent,{...clone(observed),agentDir:'/wrong'})).toBe(false);
  expect(check(proof!,parent,{...clone(observed),sessionsDir:'/wrong'})).toBe(false);
  expect(check(proof!,parent,{...clone(observed),identity:{...observed.identity,OPENRIG_OCCUPANT_GENERATION:'wrong'}})).toBe(false);
  expect(check({...clone(proof!),intent:{...intent,trust:'approve'}})).toBe(false);
  expect(check(proof!,{...clone(parent),argv:[process.execPath,runnerEntry,'--no-approve']})).toBe(false);
  expect(check({...clone(proof!),childArgs:['--approve']})).toBe(false);
  expect(check({...clone(proof!),intent:{...intent,sessionFile:path.join(dirs.sessionsDir,'wrong.jsonl')}})).toBe(false);
  expect(check({...clone(proof!),childImage:{...proof!.childImage,sha256:'0'.repeat(64)}})).toBe(false);
  expect(capturePiManagedSpawnProof(input,()=>{throw Error('unreadable');})).toBeNull();
  let reads=0;expect(capturePiManagedSpawnProof(input,(pid)=>{const row=read(pid);if(++reads>2)row.instance.start='reused';return row;})).toBeNull();
  writeFileSync(piEntry,'#!/usr/bin/env node\n// changed\n');expect(check()).toBe(false);
 } finally {child.kill();await new Promise<void>(resolve=>{if(child.exitCode!==null||child.signalCode!==null)resolve();else child.once('exit',()=>resolve());});rmSync(dir,{recursive:true,force:true});}
});
