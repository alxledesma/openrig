import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type Database from 'better-sqlite3';
import {createDb} from '../src/db/connection.js';
import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {CoordinationRecoveryService,type CoordinationActivity,type CoordinationTask,type CoordinationPlan} from '../src/domain/coordination-recovery-service.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import {seed,token} from './helpers/coordinator-fixture.js';

describe('explicit returned predecessor milestone',()=>{
 let dir:string,db:Database.Database,repo:QueueRepository,svc:CoordinationRecoveryService,clock:number,samples:Map<string,CoordinationActivity>;
 const sessions=['lead@xv','peer@xv','builder@xv','reviewer@xv','architect@xv','operator-agent@kernel'];
 const activity=(session:string):CoordinationActivity=>{const generation=repo.coordinatorAuthority.generation(session)!;const at=new Date(clock).toISOString();return {generation,identityVerified:true,state:{seatNodeId:session,activity:'idle-at-prompt',needsInput:{count:0,reason:null},decidedBy:'window-sampling',seq:1,changedAt:at,rungs:[],lastSwap:{generation,at}},witness:{seatNodeId:session,sessionName:session,rung:'window-sampling',sourceId:'tmux',seq:1,observedAt:at,activity:'idle-at-prompt'}};};
 function refresh(){for(const session of sessions)samples.set(session,activity(session));}
 const task=(key:string,owner:string,body:string,predecessors:CoordinationTask['predecessors']=[],recoveryFor?:string):CoordinationTask=>({key,packageKey:key,owner,action:`Review ${key}`,deadline:clock+30000,body,predecessors,...(recoveryFor?{recoveryFor}:{}),admission:{generation:repo.coordinatorAuthority.generation(owner)!,configurationDigest:svc.configurationDigest(owner)!,qualificationRef:`approved/${key}`,capacityRef:`capacity/${key}`,effortRef:`effort/${key}`,validUntil:clock+60000}});
 function admit(t:CoordinationTask){repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv',t.packageKey,{inputDigest:digest(t.packageKey),destination:t.owner,bodyHash:digest(t.body),resources:[],returnContract:{destination:'lead@xv',evidenceRequired:['report']}});}
 const plan=(tasks:CoordinationTask[],revision='r1'):CoordinationPlan=>({rigId:'xv',revision,operatorGeneration:'operator-agent-g1',stallMs:10000,allowIdlePeerTransfer:true,tasks});
 const configure=(tasks:CoordinationTask[],revision='r1')=>{
  const expanded=[...tasks];
  for(const t of tasks.filter(task=>!task.recoveryFor)){
   if(!tasks.some(candidate=>candidate.recoveryFor===t.packageKey))expanded.push(task(`${t.packageKey}-recovery`,'architect@xv',`Recover ${t.packageKey}`,[],t.packageKey));
  }
  for(const t of expanded)if(!db.prepare('SELECT 1 FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv',t.packageKey))admit(t);
  return svc.configure('operator-agent@kernel','operator-agent-g1',plan(expanded,revision));
 };
 const claim=(queueId:string,owner='builder@xv')=>repo.claim({qitemId:queueId,destinationSession:owner,actorGeneration:repo.coordinatorAuthority.generation(owner)!,identityProvenance:'transport:v1'});
 const queueId=(packageKey:string)=>`qitem-coordination-${digest(`xv:${packageKey}`).slice(0,24)}`;

 beforeEach(async()=>{
  vi.useFakeTimers({toFake:['Date']});clock=Date.now();dir=mkdtempSync(join(tmpdir(),'returned-milestone-'));db=createDb(join(dir,'db'));seed(db);
  db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(),new Date(clock).toISOString());
  const bus=new EventBus(db);repo=new QueueRepository(db,bus,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));
  for(const session of sessions)db.prepare('INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES (?,?,?,?)').run(`binding-${session}`,session,session,'%1');
  await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:'lead@xv',body:'coordinate',nudge:false});
  repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:'lead@xv',ownerGeneration:'lead-g1',coordinators:['lead@xv','peer@xv'],leaseMs:60000,operationId:'enable'});
  repo.coordinatorAuthority.acknowledge('lead@xv',token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
  samples=new Map();refresh();svc=new CoordinationRecoveryService(repo,s=>samples.get(s)??null,()=>clock);repo.coordinatorAuthority.coordinationRecovery=svc;
 });
 afterEach(()=>{db.close();rmSync(dir,{recursive:true,force:true});vi.useRealTimers();});

 async function scenario(milestone:'returned'|'accepted'|null='returned'){
  const prior=task('prior','builder@xv','inspect the original result');admit(prior);
  const admitted=db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv','prior') as {contract_hash:string};
  const successor=task('independent-review','reviewer@xv','independently inspect the returned result',[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:queueId('prior'),...(milestone?{milestone}:{})} as CoordinationTask['predecessors'][number]]);admit(successor);
  configure([prior,successor]);
  let priorResult=svc.reconcile('lead@xv','lead-g1','xv').find(result=>result.key==='prior')!;
  const original=priorResult.queueId!;claim(original);
  repo.update({qitemId:original,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  const typed={packageKey:'prior',inputDigest:digest('prior'),evidence:[{kind:'report',ref:'evidence/prior.md'}]};
  await repo.create({qitemId:'typed-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify(typed),nudge:false,identityProvenance:'transport:v1'});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','prior','typed-return');
  return {prior,successor,original,contractHash:admitted.contract_hash,typed,svc,repo,db};
 }

 it('dispatches a dependent review after a genuine typed return, before acceptance, and freezes the returned proof',async()=>{
  const s=await scenario('returned');
  expect(s.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='xv' AND kind='coordination-accept' AND json_extract(receipt,'$.queueId')=?").get(s.original)).toBeUndefined();
  const result=s.svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review');
  expect(result?.state).toBe('pending-pickup');
  expect(result?.queueId).toBe(queueId('independent-review'));
  const frozen=s.db.prepare("SELECT receipt FROM coordinator_operations WHERE rig_id='xv' AND kind='coordination-predecessor-resolution' AND operation_id=?").get('coordination-predecessor-resolution:xv:independent-review') as {receipt:string};
  const proof=JSON.parse(frozen.receipt).predecessors[0];
  expect(proof).toMatchObject({packageKey:'prior',contractHash:s.contractHash,queueId:s.original,milestone:'returned',dispositionId:'typed-return'});
  expect(proof).not.toHaveProperty('acceptOperationId');
  expect(s.db.prepare("SELECT 1 FROM coordinator_operations WHERE rig_id='xv' AND operation_id='coordination-accept:prior'").get()).toBeUndefined();
 });

 it.each(['missing-proof','changed-proof','missing-recovery-service'] as const)('final managed-send fence holds for %s',async failure=>{
  const s=await scenario('returned');
  const result=s.svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review')!;
  expect(result.queueId).toBe(queueId('independent-review'));
  expect(()=>s.repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv',result.queueId)).not.toThrow();
  const operationId='coordination-predecessor-resolution:xv:independent-review';
  if(failure==='missing-proof')s.db.prepare('DELETE FROM coordinator_operations WHERE rig_id=? AND operation_id=?').run('xv',operationId);
  if(failure==='changed-proof')s.db.prepare("UPDATE coordinator_operations SET receipt='{}' WHERE rig_id=? AND operation_id=?").run('xv',operationId);
  if(failure==='missing-recovery-service')s.repo.coordinatorAuthority.coordinationRecovery=undefined;
  expect(()=>s.repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv',result.queueId)).toThrow();
 });

 it.each(['valid-proof','missing-proof','changed-proof','missing-recovery-service'] as const)('final send returned proof while destination is coordinator member: %s',async failure=>{
  const s=await scenario('returned');
  const result=s.svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review')!;
  expect(result.queueId).toBe(queueId('independent-review'));
  // This fixture represents the independently admitted reviewer as the second
  // current coordinator; coordinator-to-coordinator sends have no ordinary scope.
  s.db.prepare('UPDATE coordinator_authority SET coordinators=? WHERE rig_id=?').run(JSON.stringify(['lead@xv','reviewer@xv']),'xv');
  expect(s.repo.coordinatorAuthority.scope('lead@xv','reviewer@xv')).toBeUndefined();
  const operationId='coordination-predecessor-resolution:xv:independent-review';
  if(failure==='missing-proof')s.db.prepare('DELETE FROM coordinator_operations WHERE rig_id=? AND operation_id=?').run('xv',operationId);
  if(failure==='changed-proof')s.db.prepare("UPDATE coordinator_operations SET receipt='{}' WHERE rig_id=? AND operation_id=?").run('xv',operationId);
  if(failure==='missing-recovery-service')s.repo.coordinatorAuthority.coordinationRecovery=undefined;
  if(failure==='valid-proof')expect(()=>s.repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv',result.queueId)).not.toThrow();
  else expect(()=>s.repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv',result.queueId)).toThrow();
 });

 it('preserves unmanaged queue sends when the coordinator schema is unavailable',()=>{
  vi.spyOn(repo.coordinatorAuthority,'available').mockReturnValue(false);
  expect(()=>repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv','legacy-qitem')).not.toThrow();
 });

 it('keeps omitted accepted milestone and legacy queue/disposition references acceptance-gated',async()=>{
  const omitted=await scenario(null);
  expect(omitted.svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review')?.state).toBe('held');
  const legacy=task('legacy-review','reviewer@xv','review prior result',[{queueId:omitted.original,dispositionId:'typed-return'}]);
  admit(legacy);configure([omitted.prior,omitted.successor,legacy],'r2');
  expect(omitted.svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='legacy-review')?.state).toBe('held');
 });

 it.each(['missing-return','wrong-source','wrong-generation','incomplete-evidence','wrong-instance'] as const)('does not release a successor for %s',async failure=>{
  const prior=task('prior','builder@xv','inspect the original result');admit(prior);
  const admitted=db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv','prior') as {contract_hash:string};
  const successor=task('independent-review','reviewer@xv','independently inspect',[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:queueId('prior'),milestone:'returned'} as CoordinationTask['predecessors'][number]]);admit(successor);configure([prior,successor]);
  const original=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='prior')!.queueId!;claim(original);
  repo.update({qitemId:original,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  if(failure!=='missing-return'){
   const body={packageKey:failure==='wrong-instance'?'other':'prior',inputDigest:digest('prior'),evidence:failure==='incomplete-evidence'?[]:[{kind:'report',ref:'evidence/prior.md'}]};
   const source= failure==='wrong-source'?'peer@xv':'builder@xv';
   await repo.create({qitemId:'typed-return',sourceSession:source,destinationSession:'lead@xv',body:JSON.stringify(body),nudge:false,identityProvenance:'transport:v1'});
   if(failure==='wrong-generation')db.prepare("UPDATE queue_items SET minting_generation_uuid='builder-g2' WHERE qitem_id='typed-return'").run();
   expect(()=>repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','prior','typed-return')).toThrow();
  }
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review')?.state).toBe('held');
  expect(db.prepare("SELECT 1 FROM coordinator_assignments WHERE rig_id='xv' AND package_key='independent-review'").get()).toBeUndefined();
 });

 it('holds when the original assigned instance drifts after a genuine return',async()=>{
  const prior=task('prior','builder@xv','inspect the original result');admit(prior);
  const admitted=db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv','prior') as {contract_hash:string};
  const successor=task('independent-review','reviewer@xv','independently inspect',[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:queueId('prior'),milestone:'returned'} as CoordinationTask['predecessors'][number]]);admit(successor);configure([prior,successor]);
  const original=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='prior')!.queueId!;claim(original);
  repo.update({qitemId:original,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:'typed-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'prior',inputDigest:digest('prior'),evidence:[{kind:'report',ref:'evidence/prior.md'}]}),nudge:false,identityProvenance:'transport:v1'});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','prior','typed-return');
  const assigned=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review')!;
  expect(assigned.state).toBe('pending-pickup');
  expect(()=>repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv',assigned.queueId)).not.toThrow();
  db.prepare('UPDATE coordinator_assignments SET body_hash=? WHERE rig_id=? AND package_key=?').run(digest('drifted-original-body'),'xv','prior');
  expect(svc.assignmentPredecessorsReady(assigned.queueId!)).toBe(false);
  expect(()=>repo.coordinatorAuthority.assertManagedSend('lead@xv','reviewer@xv',assigned.queueId)).toThrow();
 });

 it('rejects malformed milestone, wrong instance references, and changed bindings after assignment',async()=>{
  const prior=task('prior','builder@xv','inspect the original result');admit(prior);
  const admitted=db.prepare('SELECT contract_hash FROM coordinator_packages WHERE rig_id=? AND package_key=?').get('xv','prior') as {contract_hash:string};
  const bad=task('bad-review','reviewer@xv','review',[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:'wrong-instance',milestone:'returned'} as CoordinationTask['predecessors'][number]]);admit(bad);
  expect(()=>configure([prior,bad])).toThrow();
  const unknown=task('unknown-review','reviewer@xv','review',[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:queueId('prior'),milestone:'returned-ish'} as unknown as CoordinationTask['predecessors'][number]]);admit(unknown);
  expect(()=>configure([prior,unknown])).toThrow();
  const valid=task('independent-review','reviewer@xv','review',[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:queueId('prior'),milestone:'returned'} as CoordinationTask['predecessors'][number]]);admit(valid);configure([prior,valid]);
  const original=svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='prior')!.queueId!;claim(original);
  repo.update({qitemId:original,actorSession:'builder@xv',actorGeneration:'builder-g1',identityProvenance:'transport:v1',state:'done',closureReason:'no-follow-on'});
  await repo.create({qitemId:'typed-return',sourceSession:'builder@xv',destinationSession:'lead@xv',body:JSON.stringify({packageKey:'prior',inputDigest:digest('prior'),evidence:[{kind:'report',ref:'evidence/prior.md'}]}),nudge:false,identityProvenance:'transport:v1'});
  repo.coordinatorAuthority.dispose('builder@xv','builder-g1','xv','prior','typed-return');
  expect(svc.reconcile('lead@xv','lead-g1','xv').find(r=>r.key==='independent-review')?.state).toBe('pending-pickup');
  const changed={...valid,predecessors:[{packageKey:'prior',contractHash:admitted.contract_hash,queueId:queueId('prior')}] as CoordinationTask['predecessors']};
  expect(()=>configure([prior,changed],'r2')).toThrow();
 });
});
