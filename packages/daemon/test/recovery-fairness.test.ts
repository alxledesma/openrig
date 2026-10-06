import {describe,it,expect} from 'vitest';
import {WatchdogScheduler} from '../src/domain/watchdog-scheduler.js';
import {makeCoordinatorContinuityPolicy} from '../src/domain/policies/coordinator-continuity.js';
describe('recovery I/O fairness',()=>{
 it('serves an event-loop turn between jobs without overlapping ticks',async()=>{
  const trace:string[]=[];
  const s=new WatchdogScheduler({jobsRepo:{listActive:()=>[{jobId:'a'},{jobId:'b'}],getById:(jobId:string)=>({jobId,state:'active'})} as any,policyEngine:{evaluate:async(j:any)=>{trace.push(j.jobId);if(j.jobId==='a')setImmediate(()=>trace.push('io'));}} as any});
  await Promise.all([s.runTickNow(),s.runTickNow()]);expect(trace).toEqual(['a','io','b']);
 });
 it('does not evaluate a job stopped during the yield',async()=>{
  let n=0;const s=new WatchdogScheduler({jobsRepo:{listActive:()=>[{jobId:'a'}],getById:()=>({jobId:'a',state:'stopped'})} as any,policyEngine:{evaluate:async()=>{n++;}} as any});await s.runTickNow();expect(n).toBe(0);
 });
 it('checks shutdown again after yielding',async()=>{
  let release!:()=>void,entered!:()=>void,n=0;const atYield=new Promise<void>(r=>{entered=r;});
  const s=new WatchdogScheduler({jobsRepo:{listActive:()=>[{jobId:'a'}],getById:(jobId:string)=>({jobId,state:'active'})} as any,policyEngine:{evaluate:async()=>{n++;}} as any,setTimer:()=>({}) as any,clearTimer:()=>{},yieldToEventLoop:()=>{entered();return new Promise<void>(r=>{release=r;});}});
  s.start();const tick=s.runTickNow();await atYield;const stopped=s.stop();release();await Promise.all([tick,stopped]);expect(n).toBe(0);expect(s.isRunning()).toBe(false);
 });
 it('preserves phase order and yields only between complete units',async()=>{
  const trace:string[]=[];const phase=(name:string)=>()=>{trace.push(name);};
  const p=makeCoordinatorContinuityPolicy({refreshRuntimeAvailability:phase('availability'),coordinationRecovery:{refreshActivity:phase('activity'),supervise:phase('supervise'),deliverCommitted:phase('deliver')},resumeAdministrativeDuties:phase('admin'),runtimeOutcomeAssessment:{stagePolicyBoundary:phase('policy'),drain:phase('drain'),stageRecoveryBoundary:phase('recovery')},observeContinuity:phase('observe')} as any,async()=>{trace.push('yield');});
  await p.evaluate({registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'rig'},jobId:'job'} as any);
  expect(trace).toEqual(['availability','yield','activity','yield','supervise','yield','admin','yield','policy','yield','drain','yield','recovery','yield','deliver','yield','observe']);
 });
 it('does not continue after a refused authority phase',async()=>{
  const trace:string[]=[];const p=makeCoordinatorContinuityPolicy({refreshRuntimeAvailability:async()=>{},coordinationRecovery:{refreshActivity:async()=>{},supervise:()=>{throw Error('authority refused');},deliverCommitted:()=>trace.push('send')},resumeAdministrativeDuties:()=>trace.push('admin')} as any,async()=>{});
  await expect(p.evaluate({registeredBySession:'operator-agent@kernel',target:{session:'operator-agent@kernel'},context:{rigId:'rig'},jobId:'job'} as any)).rejects.toThrow('authority refused');expect(trace).toEqual([]);
 });
});
