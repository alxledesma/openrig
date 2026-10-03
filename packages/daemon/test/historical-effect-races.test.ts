import { it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createDb } from '../src/db/connection.js';
import { seed } from './helpers/coordinator-fixture.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { EventBus } from '../src/domain/event-bus.js';
import { SessionRegistry } from '../src/domain/session-registry.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { HistoricalEffectDispositionService, historicalDigest, type HistoricalPlan, type HistoricalDisposition } from '../src/domain/historical-effect-disposition.js';

const actor='operator-agent@kernel',generation='operator-agent-g1';
async function fixture() {
 const dir=mkdtempSync(join(tmpdir(),'openrig-historical-race-')),file=join(dir,'db.sqlite'),db=createDb(file);seed(db);const sessions=new SessionRegistry(db),repo=new QueueRepository(db,new EventBus(db),{resolveOccupantGeneration:s=>sessions.currentOccupantGenerationForSession(s)}),svc=new HistoricalEffectDispositionService(db),outbox=new OutboxHandler(db);
 await repo.create({qitemId:'baton',sourceSession:actor,destinationSession:'lead@xv',body:'canonical valuable baton',nudge:false});repo.claim({qitemId:'baton',destinationSession:'lead@xv'});
 await repo.create({qitemId:'valuable',sourceSession:'lead@xv',destinationSession:'builder@xv',body:'valuable obligation',nudge:false});outbox.record({outboxId:'wake-intent-old',senderSession:'lead@xv',destinationSession:'builder@xv',body:'synthetic historical intent',auditPointer:'valuable'});
 const base={rigId:'xv',leadBatonId:'baton',leadGeneration:'lead-g1',operatorGeneration:generation,expiresAt:Date.now()+600000};
 const plan=(op:string):HistoricalPlan=>({...base,operationId:op,authorizationId:`auth-${op}`,effects:svc.inspect('xv',['wake-intent-old'])});
 const disposition=(op:string):HistoricalDisposition=>{const {effects,...p}=plan(op);return {...p,quarantineOperationId:'hold',effect:effects[0]!,action:'withdraw-obsolete-wake',reason:'Attributable obsolete synthetic intent',evidenceRef:'independent current native inspection'};};
 const authorize=async(input:HistoricalPlan|HistoricalDisposition)=>{if(!('effects' in input)&&!outbox.isHistoricalQuarantined(input.effect.outboxId)){const hold=plan('hold');await authorize(hold);svc.quarantine(actor,generation,hold);}await repo.create({qitemId:input.authorizationId,sourceSession:'lead@xv',destinationSession:actor,body:JSON.stringify({kind:'effects' in input?'outbox-historical-quarantine-authorization':'outbox-historical-disposition-authorization',requestDigest:historicalDigest({actor,generation,input})}),nudge:false,identityProvenance:'transport:v1'});repo.claim({qitemId:input.authorizationId,destinationSession:actor});};
 return {db,file,plan,disposition,authorize,close(){db.close();rmSync(dir,{recursive:true,force:true});}};
}
const moduleUrl=(f:string)=>JSON.stringify(new URL(`../dist/${f}`,import.meta.url).href);
function child(file:string,input:HistoricalPlan|HistoricalDisposition,kind:'dispose'|'quarantine'|'claim') {
 const code=`import {createDb} from ${moduleUrl('db/connection.js')};import {HistoricalEffectDispositionService} from ${moduleUrl('domain/historical-effect-disposition.js')};import {OutboxHandler} from ${moduleUrl('domain/outbox-handler.js')};import {SeatDeliveryGuard,resolveGuardTarget} from ${moduleUrl('domain/seat-delivery-guard.js')};const db=createDb(${JSON.stringify(file)}),service=new HistoricalEffectDispositionService(db),guard=new SeatDeliveryGuard(db,n=>resolveGuardTarget(db,n));console.log('READY');process.stdin.once('data',async()=>{try{const input=${JSON.stringify(input)};const receipt=${kind==='dispose'?`await service.dispose(${JSON.stringify(actor)},${JSON.stringify(generation)},input,guard)`:kind==='quarantine'?`service.quarantine(${JSON.stringify(actor)},${JSON.stringify(generation)},input)`:`new OutboxHandler(db).claimForDelivery('wake-intent-old')`};console.log(JSON.stringify({ok:true,receipt}));}catch(e){console.log(JSON.stringify({ok:false,code:e.code,message:e.message}));}finally{db.close();process.exit(0);}});`;
 const proc=spawn(process.execPath,['--input-type=module','-e',code],{stdio:['pipe','pipe','pipe']});let output='',errors='';let readyResolve!:()=>void;const ready=new Promise<void>(r=>readyResolve=r);proc.stdout.on('data',d=>{output+=d.toString();if(output.includes('READY'))readyResolve();});proc.stderr.on('data',d=>errors+=d.toString());
 const result=new Promise<{ok:boolean;code?:string;receipt?:boolean|unknown}>((resolve,reject)=>proc.on('close',code=>{if(code!==0)return reject(new Error(`Child exit ${code}: ${errors}`));try{resolve(JSON.parse(output.trim().split('\n').at(-1)!));}catch(e){reject(e);}}));return {ready,result,go(){proc.stdin.end('GO\n');}};
}
it('independent processes commit exactly one disposition and retain the original obligation claim',async()=>{
 const f=await fixture();try{const a=f.disposition('a'),b=f.disposition('b');await f.authorize(a);await f.authorize(b);const before=f.db.prepare('SELECT * FROM queue_items ORDER BY qitem_id').all();const x=child(f.file,a,'dispose'),y=child(f.file,b,'dispose');await Promise.all([x.ready,y.ready]);x.go();y.go();const results=await Promise.all([x.result,y.result]);expect(results.filter(r=>r.ok)).toHaveLength(1);expect(results.filter(r=>r.code==='historical_quarantine_required')).toHaveLength(1);expect(f.db.prepare("SELECT count(*) n FROM outbox_historical_operations WHERE kind='dispose'").get()).toEqual({n:1});expect(f.db.prepare('SELECT * FROM queue_items ORDER BY qitem_id').all()).toEqual(before);expect(f.db.prepare("SELECT delivery_state,delivered_at FROM outbox_entries WHERE outbox_id='wake-intent-old'").get()).toEqual({delivery_state:'retired',delivered_at:null});}finally{f.close();}
});
it('quarantine versus independent delivery claim serializes: no held intention is claimed',async()=>{
 const f=await fixture();try{const p=f.plan('hold');await f.authorize(p);const x=child(f.file,p,'quarantine'),y=child(f.file,p,'claim');await Promise.all([x.ready,y.ready]);x.go();y.go();const [quarantine,claim]=await Promise.all([x.result,y.result]);const held=!!f.db.prepare("SELECT 1 FROM outbox_historical_quarantines WHERE state='held'").get();expect(held&&claim.receipt===true).toBe(false);if(held){expect(quarantine.ok).toBe(true);expect(claim.receipt).toBe(false);}else{expect(quarantine.code).toBe('historical_effect_drift');expect(claim.receipt).toBe(true);}expect(f.db.prepare("SELECT delivery_state FROM outbox_entries WHERE outbox_id='wake-intent-old'").get()).toEqual({delivery_state:held?'pending':'sending'});}finally{f.close();}
});
