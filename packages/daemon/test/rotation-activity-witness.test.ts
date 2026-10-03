import {test,expect} from 'vitest';
import {SeatActivityService}from'../src/domain/seat-activity-service.js';
import {assertRotationPrecondition,isRotationLoopback}from'../src/domain/rotation-precondition.js';
import type{TmuxAdapter}from'../src/adapters/tmux.js';
function fixture(){const now=100000;const svc=new SeatActivityService({tmux:{} as TmuxAdapter,now:()=>new Date(now)});
 svc.declareRungInventory({seatNodeId:'seat',sessionName:'seat@rig'},{adapterId:'test',runtime:'codex',rungs:[{rung:'self-report',lifecycleCoverage:'full',initialTrust:'authoritative'},{rung:'needs-input-chrome',lifecycleCoverage:'full',initialTrust:'authoritative'},{rung:'window-sampling',lifecycleCoverage:'full',initialTrust:'authoritative'}]});return{svc,now};}
const expected={protocol:'generation-queue-runtime-idle-v1',generation:'old',queue:[],runtimeContract:{model:'live'},checkpointHash:'bytes'};
test('fresh needs-input evidence in deciding rung cannot refresh stale idle activity',()=>{
 const{svc,now}=fixture();svc.reportEvidence({seatNodeId:'seat',sessionName:'seat@rig',rung:'self-report',sourceId:'idle',seq:1,activity:'idle-at-prompt',observedAt:new Date(now-10000).toISOString()});
 svc.reportEvidence({seatNodeId:'seat',sessionName:'seat@rig',rung:'self-report',sourceId:'question',seq:1,needsInput:{count:0,reason:null},observedAt:new Date(now).toISOString()});
 const witness=svc.getRotationActivityWitness('seat');expect(witness?.sourceId).toBe('idle');expect(Date.parse(witness!.observedAt)).toBe(now-10000);
 expect(()=>assertRotationPrecondition(expected,{generation:'old',queue:[],runtimeContract:{model:'live'},checkpointHash:'bytes',activity:witness!.activity!,observedAt:Date.parse(witness!.observedAt)},now)).toThrow('stale');
});
test('fresh unrelated rung cannot refresh stale higher-priority idle',()=>{
 const{svc,now}=fixture();svc.reportEvidence({seatNodeId:'seat',sessionName:'seat@rig',rung:'self-report',sourceId:'idle',seq:1,activity:'idle-at-prompt',observedAt:new Date(now-10000).toISOString()});
 svc.reportEvidence({seatNodeId:'seat',sessionName:'seat@rig',rung:'window-sampling',sourceId:'sampling',seq:1,activity:'idle-at-prompt',observedAt:new Date(now).toISOString()});
 expect(svc.getRotationActivityWitness('seat')?.sourceId).toBe('idle');expect(Date.parse(svc.getRotationActivityWitness('seat')!.observedAt)).toBe(now-10000);
});
test('unknown witness fails closed',()=>{const{svc}=fixture();expect(svc.getRotationActivityWitness('seat')).toBeNull();});
test('guarded rotation accepts only explicit loopback transport addresses',()=>{for(const address of ['127.0.0.1','::1','::ffff:127.0.0.1'])expect(isRotationLoopback(address)).toBe(true);for(const address of [undefined,'','198.51.100.2','0.0.0.0'])expect(isRotationLoopback(address)).toBe(false);});
