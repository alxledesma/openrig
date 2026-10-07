import {describe,it,expect,beforeEach,afterEach,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';
import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {archiveAgedTerminalTransitions} from '../src/domain/queue-retention.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

describe('native assignment claim independent clock proof',()=>{

 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,outbox:OutboxHandler,clock:number,samples:Map<string,CoordinationActivity>;
 const sessions=['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv','operator-agent@kernel'];
 const task=(key:string,owner='builder@xv',more:Partial<CoordinationTask>={}):CoordinationTask=>({key,packageKey:key,owner,action:'Perform '+key+' and return bounded evidence',deadline:clock+20000,body:key,predecessors:[],admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:'approved/non-subject/'+key,capacityRef:'current/provider/'+key,effortRef:'current/effort/'+key,validUntil:clock+60000},...more});
 const plan=(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}):CoordinationPlan=>({rigId:'xv',revision:'r1',operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,...more,tasks});
 function sample(session:string):CoordinationActivity{const generation=repo.coordinatorAuthority.generation(session)!;return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:new Date(clock).toISOString(),rungs:[],lastSwap:{generation,at:new Date(clock).toISOString()}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:new Date(clock).toISOString(),activity:'idle-at-prompt'}};}
 function refresh(){for(const s of sessions)samples.set(s,sample(s));}
 function admit(tasks:CoordinationTask[]){for(const t of tasks)repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.key),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});}
 function configure(tasks:CoordinationTask[],more:Partial<CoordinationPlan>={}){admit(tasks);return svc.configure('operator-agent@kernel','operator-agent-g1',plan(tasks,more));}
 function nativeClaim(qitemId:string,owner:string){return repo.claim({qitemId,destinationSession:owner,actorGeneration:repo.coordinatorAuthority.generation(owner)!,identityProvenance:'transport:v1'});}
 function normal(){return [task('product'),task('repair','architect@xv',{recoveryFor:'product'})];}
 const reconcile=()=>svc.reconcile('lead@xv','lead-g1','xv');
 const notice=(id:string)=>db.prepare('SELECT * FROM outbox_entries WHERE outbox_id=?').get('wake-intent-'+id) as any;
 beforeEach(async()=>{
  vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'shared-repair-'));db=createDb(join(dir,'db'));seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});outbox=new OutboxHandler(db);repo.attachOutbox(outbox);
  for(const s of sessions)db.prepare('INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES (?,?,?,?)').run('binding-'+s,s,s,'%1');
  repo.attachTransport({send:async()=>({ok:true,verified:true})});
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});
 async function finishTyped(packageKey:string,owner:string,queueId:string,returnId:string,state:'done'|'failed'='done'){
  if(repo.getById(queueId)!.state==='pending')nativeClaim(queueId,owner);
  repo.update({qitemId:queueId,actorSession:owner,actorGeneration:repo.coordinatorAuthority.generation(owner)!,identityProvenance:'transport:v1',state,...(state==='done'?{closureReason:'no-follow-on' as const}:{})});
  await repo.create({qitemId:returnId,sourceSession:owner,destinationSession:'lead@xv',body:JSON.stringify({packageKey,inputDigest:digest(packageKey),evidence:[{kind:'report',ref:'actual/'+packageKey+'.md'}]}),nudge:false});
  repo.coordinatorAuthority.dispose(owner,repo.coordinatorAuthority.generation(owner)!,'xv',packageKey,returnId);
  await svc.deliverCommitted();
 }

 async function assignment(verified=false){configure(normal());const first=reconcile().find(r=>r.key==='product')!;expect(first.state).toBe('pending-pickup');repo.attachTransport({send:async()=>({ok:true,verified})});await svc.deliverCommitted();return first.queueId!;}
 function outcome(id:string){return db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND operation_id=? AND kind='assignment-wake-outcome'").get('assignment-wake-outcome:'+id) as {receipt:string}|undefined;}
 function independentClockClaim(id:string){
  const append=repo.transitionLog.appendNativeCustody.bind(repo.transitionLog);
  const spy=vi.spyOn(repo.transitionLog,'appendNativeCustody').mockImplementation((...args)=>{vi.setSystemTime(clock+1);return append(...args);});
  try{nativeClaim(id,'builder@xv');}finally{spy.mockRestore();clock+=1;vi.setSystemTime(clock);refresh();}
  const claim=repo.transitionLog.listForQitem(id).find(t=>t.transitionNote==='claimed')!;
  expect(claim.ts).not.toBe(repo.getById(id)!.claimedAt);
  return claim;
 }
 function protectedHistory(id:string){return {queue:db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(id),notice:notice(id),evidence:db.prepare('SELECT * FROM queue_native_custody_evidence WHERE qitem_id=? ORDER BY transition_id').all(id),transitions:repo.transitionLog.listForQitem(id)};}
 it.each(['in-progress','done','archived-done'] as const)('contains exact UNKNOWN assignment after real %s custody despite independent clock stamps',async state=>{
  const id=await assignment(),claim=independentClockClaim(id);
  if(state!=='in-progress')repo.update({qitemId:id,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  if(state==='archived-done'){
   archiveAgedTerminalTransitions(db,{nowIso:new Date(clock+40*86400000).toISOString(),transitionsRetentionDays:30,terminalStates:['done']});
   expect(db.prepare('SELECT * FROM queue_transitions WHERE qitem_id=?').all(id)).toEqual([]);
  }
  const before=protectedHistory(id),results=reconcile();
  expect(JSON.parse(outcome(before.notice.outbox_id)!.receipt)).toMatchObject({claimTransitionId:claim.transitionId,recipientGeneration:'builder-g1',deliveryConclusion:'unknown',originalMutations:0,outcomeOnly:true,grantsAuthority:false});
  expect(svc.noticeOutcomeContained('xv',before.notice)).toBe(true);expect(protectedHistory(id)).toEqual(before);
  if(state!=='in-progress')expect(results.find(r=>r.key==='terminal-return:product')).toMatchObject({state:'pending-native-terminal-return'});
 });
 // Fault injection only in this disposable test DB: production receipts reject UPDATE/DELETE.
 function corruptReceipt(id:string,mutate:(r:any)=>void,missing=false){
  const rows=db.prepare('SELECT * FROM queue_native_custody_evidence').all() as Array<{transition_id:number;qitem_id:string;receipt:string}>;
  db.exec('DROP TABLE queue_native_custody_evidence; CREATE TABLE queue_native_custody_evidence(transition_id INTEGER PRIMARY KEY,qitem_id TEXT NOT NULL,receipt TEXT NOT NULL)');
  for(const row of rows){if(row.qitem_id===id){if(missing)continue;const r=JSON.parse(row.receipt);mutate(r);row.receipt=JSON.stringify(r);}db.prepare('INSERT INTO queue_native_custody_evidence VALUES(?,?,?)').run(row.transition_id,row.qitem_id,row.receipt);}
 }
 it.each(['current-generation','body','missing','actor-generation','transition','before-state','before-body','after-destination','claim-time','claim-generation'] as const)('holds %s integrity failure and preserves UNKNOWN/history',async kind=>{
  const id=await assignment();independentClockClaim(id);
  if(kind==='current-generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
  else if(kind==='body')db.prepare("UPDATE queue_items SET body='drift' WHERE qitem_id=?").run(id);
  else corruptReceipt(id,r=>{
   if(kind==='actor-generation')r.actorGeneration='builder-g2';
   if(kind==='transition')r.transition.ts=new Date(clock+1000).toISOString();
   if(kind==='before-state')r.beforeQueue.state='done';
   if(kind==='before-body')r.beforeQueue.body='drift';
   if(kind==='after-destination')r.afterQueue.destination_session='reviewer@xv';
   if(kind==='claim-time')r.afterQueue.claimed_at=new Date(clock+1000).toISOString();
   if(kind==='claim-generation')r.afterQueue.claimed_by_generation_uuid='builder-g2';
  },kind==='missing');
  const before=protectedHistory(id);reconcile();expect(outcome(before.notice.outbox_id)).toBeUndefined();expect(svc.noticeOutcomeContained('xv',before.notice)).toBe(false);expect(protectedHistory(id)).toEqual(before);
 });
});
