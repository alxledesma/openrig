import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync,existsSync,mkdirSync,symlinkSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import type Database from 'better-sqlite3';
import {Hono} from 'hono';
import {createDb} from '../src/db/connection.js';
import {seed} from './helpers/coordinator-fixture.js';
import {SeatDeliveryGuard,resolveGuardTarget} from '../src/domain/seat-delivery-guard.js';
import {CodexSameGenerationRehost,parseCodexStoppedRecovery,type CodexRehostOptions,type CodexStoppedRecoveryInput} from '../src/domain/codex-rehost.js';
import {stoppedCodexContract} from '../src/domain/codex-rehost-integration.js';
import {seatRoutes} from '../src/routes/seat.js';
import {SeatLifecycleService} from '../src/domain/seat-lifecycle-service.js';
import {RigRepository} from '../src/domain/rig-repository.js';
import {SessionRegistry} from '../src/domain/session-registry.js';
import {EventBus} from '../src/domain/event-bus.js';
import {QueueRepository} from '../src/domain/queue-repository.js';
import {OutboxHandler} from '../src/domain/outbox-handler.js';
import {digest} from '../src/domain/coordinator-authority-service.js';
import type {TmuxAdapter} from '../src/adapters/tmux.js';
import type {NativeProcessRow} from '../src/domain/native-process-lineage.js';
const sha=(b:string|Buffer)=>createHash('sha256').update(b).digest('hex');
const seat='lead@xv',nativeId='original-exact-native-thread',generation='lead-g1';
const input={nodeId:seat,sessionName:seat,reason:'recover verified late stop without a second signal',operator:'operator-agent@kernel'};
const tree=(pid:number):NativeProcessRow[]=>[{pid:10,ppid:1,command:'/bin/zsh',executableName:'zsh',startedAt:'stable-pane',pgid:10,tpgid:pid},
 {pid,ppid:10,command:`/usr/bin/codex --no-daemon -p exact resume ${nativeId}`,executableName:'codex',startedAt:'native-'+pid,pgid:pid,tpgid:pid}];
describe('durable stopped Codex rehost continuation',()=>{
 let db:Database.Database,dir:string,file:string,attempt:string,beganBytes:Buffer,unknownBytes:Buffer,options:CodexRehostOptions,service:CodexSameGenerationRehost,guard:SeatDeliveryGuard,request:CodexStoppedRecoveryInput;
 let processes:NativeProcessRow[],signal:ReturnType<typeof vi.fn>,resume:ReturnType<typeof vi.fn>,absence:ReturnType<typeof vi.fn>;
 const snapshot=()=>JSON.stringify(Object.fromEntries(['nodes','sessions','bindings','occupant_tenures','queue_items','coordinator_authority','coordinator_resources','outbox_entries'].map(t=>[t,db.prepare('SELECT * FROM '+t).all()])));
 beforeEach(async context=>{
  dir=realpathSync(mkdtempSync(path.join(tmpdir(),'stopped-codex-recovery-')));file=path.join(dir,'native.jsonl');
  writeFileSync(file,JSON.stringify({type:'session_meta',payload:{id:nativeId,model_provider:'openai'}})+'\n'+JSON.stringify({type:'turn_context',payload:{model:'gpt-6-luna',effort:'high',sandbox_policy:{type:'workspace-write'},approval_policy:'never'}})+'\n',{mode:0o600});
  db=createDb();seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host',?,?)").run(new Date().toISOString(),new Date().toISOString());db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,seat);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready'").run();db.prepare("UPDATE sessions SET origin='claimed',resume_type='codex_id',resume_token=? WHERE node_id=?").run(nativeId,seat);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding',?,?,'%1')").run(seat,seat);
  guard=new SeatDeliveryGuard(db,s=>resolveGuardTarget(db,s));await guard.set(seat,true,'operator','isolated test');
  processes=tree(20);signal=vi.fn();absence=vi.fn(async()=>true);
  resume=vi.fn(async()=>{expect(guard.ownsRunnerRehost(seat)).toBe(true);expect(existsSync(path.join(attempt,'recovery-began.json'))).toBe(true);processes=tree(40);return {ok:true as const};});
  const now=Date.now();options={db,guard,tmux:{getPanePid:async()=>10},snapshotRoot:path.join(dir,'private'),resume:{resume},
   nativeState:async()=>({nodeId:seat,sessionName:seat,nativeId,transcriptPath:file,runtimeContract:{runtime:'codex',model:'gpt-6-luna',provider:'openai',profile:'exact',effort:'high',permissions:{sandbox:{type:'workspace-write'},approval:'never'}}}),
   activityWitness:async()=>({seatNodeId:seat,sessionName:seat,rung:'window-sampling',sourceId:'test',seq:1,observedAt:new Date(now).toISOString(),activity:'idle'}),
   preflightSupervisedLaunch:async()=>({posture:'floor',effective:{model:'gpt-6-luna',provider:'openai',effort:'high',approval:'never',sandbox:'workspace-write'},evidenceDigest:'a'.repeat(64)}),
   observeSupervisedReplacement:async()=>({launchId:'real-seam-launch',fingerprint:'independent-kernel-proof'}),
   stoppedNativeState:async b=>({nodeId:b.nodeId,sessionName:b.sessionName,nativeId:b.nativeId,transcriptPath:file,runtimeContract:stoppedCodexContract(file,b)}),
   proveStoppedIdentityAbsent:absence,listProcesses:async()=>processes,verifyProcessIdentity:async()=>true,signal,now:()=>now,sleep:async()=>{},waitMs:1,pollMs:1};
  if(context.task.name.startsWith('continues')||context.task.name.startsWith('D1')){
   const events=new EventBus(db),repo=new QueueRepository(db,events,{resolveOccupantGeneration:s=>repo.coordinatorAuthority.generation(s)});repo.attachOutbox(new OutboxHandler(db));
   await repo.create({qitemId:'baton',sourceSession:'operator-agent@kernel',destinationSession:seat,body:'coordinate',nudge:false});
   repo.coordinatorAuthority.enable('operator-agent@kernel','operator-agent-g1',{rigId:'xv',batonId:'baton',owner:seat,ownerGeneration:generation,coordinators:[seat,'peer@xv'],leaseMs:60000,operationId:'enable'});
   const token={rigId:'xv',epoch:1,generation};repo.coordinatorAuthority.acknowledge(seat,token,{operationId:'ack',obligationsDigest:repo.coordinatorAuthority.reconciliationDigest('xv')});
   repo.coordinatorAuthority.admit('operator-agent@kernel','operator-agent-g1','xv','owned',{inputDigest:'input',destination:seat,bodyHash:digest('retained claim'),resources:['file:x'],returnContract:{destination:'peer@xv',evidenceRequired:['report']}});
   await repo.create({qitemId:'work',sourceSession:seat,destinationSession:seat,body:'retained claim',dispatch:{token,packageKey:'owned'},nudge:false});repo.claim({qitemId:'work',destinationSession:seat,identityProvenance:'transport:v1'});
   new OutboxHandler(db).record({outboxId:'unknown',senderSession:seat,destinationSession:'peer@xv',body:'ambiguous effect'});db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate'").run();
  }
  service=new CodexSameGenerationRehost(options);const stopped=await service.rehost(input);expect(stopped).toMatchObject({ok:false,code:'codex_rehost_stop_unknown',effectAttempted:true});
  if(stopped.ok||!stopped.receiptPath)throw new Error('missing retained attempt');attempt=path.dirname(stopped.receiptPath);
  // Deployed legacy receipt has no new nativeEvidence field. No production receipt is touched.
  const began=JSON.parse(readFileSync(stopped.receiptPath,'utf8'));delete began.nativeEvidence;writeFileSync(stopped.receiptPath,JSON.stringify(began)+'\n');
  beganBytes=readFileSync(stopped.receiptPath);unknownBytes=readFileSync(path.join(attempt,'unknown.json'));request={...input,actorGeneration:'operator-agent-g1',attemptId:began.attemptId,beganSha256:sha(beganBytes)};
  processes=[tree(20)[0]!];signal.mockClear();resume.mockClear();
 });
 afterEach(()=>{db?.close();rmSync(dir,{recursive:true,force:true});});
 it('continues a legacy stopped attempt once without signal or custody changes and writes genuine completion',async()=>{
  const before=snapshot();const lifecycle=new SeatLifecycleService({db,rigRepo:new RigRepository(db),sessionRegistry:new SessionRegistry(db),eventBus:new EventBus(db),tmuxAdapter:{deliveryGuard:guard} as unknown as TmuxAdapter,codexRehost:service});
  expect(await lifecycle.rehostRunner({seatRef:seat,reason:request.reason,operator:request.operator,actorGeneration:request.actorGeneration,codexStoppedRecovery:{attemptId:request.attemptId,beganSha256:request.beganSha256}})).toMatchObject({ok:true,generation,generationUnchanged:true,custodyPreserved:true,authorityRepaired:false});
  expect(signal).not.toHaveBeenCalled();expect(resume).toHaveBeenCalledExactlyOnceWith(seat,'codex_id',nativeId,dir,'exact','floor','gpt-6-luna','high',generation);expect(snapshot()).toBe(before);
  expect(readFileSync(path.join(attempt,'began.json'))).toEqual(beganBytes);expect(readFileSync(path.join(attempt,'unknown.json'))).toEqual(unknownBytes);expect(JSON.parse(readFileSync(path.join(attempt,'completed.json'),'utf8'))).toMatchObject({ok:true,recoveryProtocol:'codex-stopped-recovery-v1',beganSha256:request.beganSha256});
  expect(await service.recoverStopped(request)).toMatchObject({ok:false,code:'codex_rehost_recovery_replay',effectAttempted:false});expect(resume).toHaveBeenCalledTimes(1);expect(guard.ownsRunnerRehost(seat)).toBe(false);expect(guard.preference(seat)).toMatchObject({desired:true,effective:true});
 });
 const addDelayedArrivals=()=>{
  new OutboxHandler(db).record({outboxId:'late-held',senderSession:'operator-agent@kernel',destinationSession:seat,body:'held while stopped'});
  db.prepare("UPDATE outbox_entries SET delivery_state='retained' WHERE outbox_id='late-held'").run();
  db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body) VALUES('late-pending','later','later','operator-agent@kernel',?,'pending','pending while stopped')").run(seat);
  db.prepare("UPDATE sessions SET last_seen_at='later' WHERE node_id=?").run(seat);
 };
 it('D1 preserves delayed held deliveries, pending queue and session metadata with both custody baselines',async()=>{
  addDelayedArrivals();const before=snapshot(),backup=readFileSync(path.join(attempt,'transcript.jsonl'));
  expect(await service.recoverStopped(request)).toMatchObject({ok:true,custodyPreserved:true});
  expect(snapshot()).toBe(before);expect(signal).not.toHaveBeenCalled();expect(resume).toHaveBeenCalledTimes(1);
  const journal=JSON.parse(readFileSync(path.join(attempt,'recovery-began.json'),'utf8')),original=JSON.parse(beganBytes.toString()).custody;
  expect(journal.originalCustody).toEqual(original);
  for(const key of ['node','bindings','tenures','permissions','authority','assignments','staged','resources'])expect(journal.recoveryBaselineCustody[key]).toEqual(original[key]);
  for(const key of ['sessions','queue','outbox'])expect(journal.recoveryBaselineCustody[key]).not.toEqual(original[key]);
  const completed=JSON.parse(readFileSync(path.join(attempt,'completed.json'),'utf8'));expect(completed.custodyAfter).toEqual(journal.recoveryBaselineCustody);
  expect(readFileSync(path.join(attempt,'began.json'))).toEqual(beganBytes);expect(readFileSync(path.join(attempt,'unknown.json'))).toEqual(unknownBytes);expect(readFileSync(path.join(attempt,'transcript.jsonl'))).toEqual(backup);
 });
 it.each(['node','bindings','tenures','permissions','authority','assignments','staged','resources','sending'])('D1 still refuses original %s drift before recovery effects',async kind=>{
  addDelayedArrivals();
  if(kind==='node')db.prepare("UPDATE nodes SET role='changed' WHERE id=?").run(seat);
  if(kind==='bindings')db.prepare("UPDATE bindings SET tmux_window='changed' WHERE node_id=?").run(seat);
  if(kind==='tenures')db.prepare("UPDATE occupant_tenures SET boot_at='changed' WHERE node_id=?").run(seat);
  if(kind==='permissions')db.prepare("INSERT INTO node_permission_selections(node_id,runtime,mode,actor,reason) VALUES(?,'codex','floor','operator-agent@kernel','changed')").run(seat);
  if(kind==='authority')db.prepare("UPDATE coordinator_authority SET lease_until=lease_until+1 WHERE rig_id='xv'").run();
  if(kind==='assignments')db.prepare("UPDATE coordinator_assignments SET body_hash='changed' WHERE rig_id='xv'").run();
  if(kind==='staged')db.prepare("INSERT INTO coordinator_stage_assignments(rig_id,package_key,source,destination,body_hash,queue_id,source_generation) VALUES('xv','owned',?,?,'new','new-stage','lead-g1')").run(seat,seat);
  if(kind==='resources')db.prepare("UPDATE coordinator_resources SET resource_key='file:changed' WHERE rig_id='xv'").run();
  if(kind==='sending')db.prepare("UPDATE outbox_entries SET delivery_state='sending' WHERE outbox_id='late-held'").run();
  const before=snapshot();expect(await service.recoverStopped(request)).toMatchObject({ok:false,effectAttempted:false,code:kind==='sending'?'codex_rehost_sending':'codex_rehost_recovery_receipt'});
  expect(signal).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();expect(existsSync(path.join(attempt,'recovery-began.json'))).toBe(false);expect(snapshot()).toBe(before);
 });
 it.each(['sessions','queue','outbox'])('D1 retains UNKNOWN for %s drift during recovery',async kind=>{
  addDelayedArrivals();resume.mockImplementation(async()=>{
   processes=tree(40);
   if(kind==='sessions')db.prepare("UPDATE sessions SET last_seen_at='during-resume' WHERE node_id=?").run(seat);
   if(kind==='queue')db.prepare("UPDATE queue_items SET ts_updated='during-resume' WHERE qitem_id='late-pending'").run();
   if(kind==='outbox')db.prepare("UPDATE outbox_entries SET delivery_state='indeterminate' WHERE outbox_id='late-held'").run();
   return {ok:true};
  });
  expect(await service.recoverStopped(request)).toMatchObject({ok:false,effectAttempted:true,code:'codex_rehost_custody_changed'});
  expect(existsSync(path.join(attempt,'recovery-unknown.json'))).toBe(true);expect(existsSync(path.join(attempt,'completed.json'))).toBe(false);
  expect(readFileSync(path.join(attempt,'began.json'))).toEqual(beganBytes);expect(readFileSync(path.join(attempt,'unknown.json'))).toEqual(unknownBytes);
  expect(await service.recoverStopped(request)).toMatchObject({ok:false,effectAttempted:false});expect(resume).toHaveBeenCalledTimes(1);expect(signal).not.toHaveBeenCalled();
 });
 it.each(['live','reparented','unreadable-global','wrong-digest','wrong-code','binding','custody','backup','prefix','profile','actor','generation','other-debt','guard','pane-replaced'])("holds %s before recovery intent/effect",async kind=>{
  if(kind==='live')processes=tree(20);if(kind==='reparented'){processes=[tree(20)[0]!,{...tree(20)[1]!,ppid:1}];absence.mockResolvedValue(false);}if(kind==='unreadable-global')absence.mockResolvedValue(false);
  if(kind==='wrong-digest')request.beganSha256='0'.repeat(64);if(kind==='wrong-code')writeFileSync(path.join(attempt,'unknown.json'),JSON.stringify({attemptId:request.attemptId,code:'codex_rehost_resume_unknown',effectAttempted:true,blindRetryAllowed:false}));
  if(kind==='binding')db.prepare("UPDATE sessions SET resume_token='different' WHERE node_id=?").run(seat);if(kind==='custody')db.prepare("UPDATE nodes SET role='different' WHERE id=?").run(seat);
  if(kind==='backup')writeFileSync(path.join(attempt,'transcript.jsonl'),readFileSync(file)+'{}\n');if(kind==='prefix')writeFileSync(file,readFileSync(file,'utf8').replace('workspace-write','danger-full-access'));
  if(kind==='profile'){const base=options.preflightSupervisedLaunch;options.preflightSupervisedLaunch=async(...a)=>({...await base(...a),evidenceDigest:'b'.repeat(64)});}if(kind==='actor')request.operator='peer@xv';if(kind==='generation')request.actorGeneration='expired-operator';
  if(kind==='other-debt'){const other=path.join(path.dirname(attempt),'other-attempt');mkdirSync(other,{mode:0o700});writeFileSync(path.join(other,'began.json'),'{}',{mode:0o600});}if(kind==='guard')await guard.set(seat,false,'operator','test');if(kind==='pane-replaced'){let n=0;options.listProcesses=async()=>[{...tree(20)[0]!,startedAt:String(++n)}];}
  expect(await new CodexSameGenerationRehost(options).recoverStopped(request)).toMatchObject({ok:false,effectAttempted:false});expect(signal).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();expect(existsSync(path.join(attempt,'recovery-began.json'))).toBe(false);expect(existsSync(path.join(attempt,'completed.json'))).toBe(false);
 });
 it.each(['resume-throw','resume-false','supervisor','post-custody','post-history'])("retains one-shot UNKNOWN after %s and never replays",async kind=>{
  if(kind==='resume-throw')resume.mockImplementation(async()=>{throw new Error('transport lost');});if(kind==='resume-false')resume.mockResolvedValue({ok:false});if(kind==='supervisor')options.observeSupervisedReplacement=async()=>null;
  if(kind==='post-custody')resume.mockImplementation(async()=>{processes=tree(40);db.prepare("UPDATE nodes SET role='changed' WHERE id=?").run(seat);return {ok:true};});if(kind==='post-history')resume.mockImplementation(async()=>{processes=tree(40);writeFileSync(file,readFileSync(file,'utf8').replace('workspace-write','danger-full-access'));return {ok:true};});
  service=new CodexSameGenerationRehost(options);expect(await service.recoverStopped(request)).toMatchObject({ok:false,effectAttempted:true});expect(existsSync(path.join(attempt,'recovery-began.json'))).toBe(true);expect(existsSync(path.join(attempt,'recovery-unknown.json'))).toBe(true);expect(existsSync(path.join(attempt,'completed.json'))).toBe(false);
  expect(await service.recoverStopped(request)).toMatchObject({ok:false,effectAttempted:false});expect(resume).toHaveBeenCalledTimes(1);expect(signal).not.toHaveBeenCalled();expect(readFileSync(path.join(attempt,'unknown.json'))).toEqual(unknownBytes);
 });
 it('rejects caller native facts and untrusted route actor before effects',async()=>{
  expect(()=>parseCodexStoppedRecovery({...request,pid:20})).toThrow();const app=new Hono();app.use('*',async(c,next)=>{c.set('terminalBearerToken' as never,'isolated-test-token');await next();});app.route('/api/seat',seatRoutes);
  const res=await app.request('/api/seat/rehost-runner/lead%40xv',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({reason:'recover',operator:'operator-agent@kernel',codexStoppedRecovery:{attemptId:request.attemptId,beganSha256:request.beganSha256}})});
  expect(res.status).toBe(401);expect(signal).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
 });
});

// Exercise the exact shipped Python program with an isolated kernel boundary,
// including an argv that merely mentions the thread in unrelated shell text.
it('global census parses actual Codex resume argv and holds unreadable or reparented seat identity',()=>{
 const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8');
 const program=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!;
 const fixture=String.raw`
def argv_env(pid):
 case=json.loads(os.environ['RECOVERY_KERNEL_FIXTURE'])
 if case.get('unreadable'): raise ValueError()
 return case['argv'],[x.encode() for x in case['env']],case['exe']
subprocess.check_output=lambda *args,**kwargs: ('4321 '+str(os.getuid())+' S\n').encode()
os.kill=lambda *args: None
`;
 const run=(f:unknown)=>execFileSync('python3',['-c',program.replace('ok=False',fixture+'\nok=False'),JSON.stringify({OPENRIG_NODE_ID:seat,OPENRIG_SESSION_NAME:seat}),'10',nativeId],{env:{...process.env,RECOVERY_KERNEL_FIXTURE:JSON.stringify(f)},encoding:'utf8'}).trim();
 const base={argv:['/usr/bin/codex','--no-daemon','-p','exact','resume',nativeId],env:[],exe:'/usr/bin/codex'};
 expect(run(base)).toBe('0');
 expect(run({...base,argv:['/usr/bin/codex','--dangerously-bypass-hook-trust','resume',nativeId]})).toBe('0');
 expect(run({...base,argv:['/usr/bin/codex','--dangerously-bypass-hook-trust','--yolo','resume','different-thread']})).toBe('1');
 expect(run({...base,argv:['/usr/bin/codex','resume','--image','one.png','two.png',nativeId]})).toBe('0');
 expect(run({...base,argv:['/usr/bin/codex','exec-server']})).toBe('1');
 expect(run({...base,argv:['/usr/bin/codex','resume','--model','gpt-6-luna','--add-dir','/tmp',nativeId]})).toBe('0');
 expect(run({...base,argv:['/usr/bin/codex','resume','--unknown','value',nativeId]})).toBe('0');
 expect(run({...base,argv:['/usr/bin/codex','resume','--model']})).toBe('0');
 expect(run({...base,argv:['/bin/sh','-c',`echo codex resume ${nativeId}`],exe:'/bin/sh'})).toBe('1');
 expect(run({...base,argv:['/usr/bin/codex','-c',`note=${nativeId}`,'resume','different-thread']})).toBe('1');
 expect(run({...base,argv:['/usr/bin/node','wrapper.js'],exe:'/usr/bin/node',env:[`OPENRIG_NODE_ID=${seat}`]})).toBe('0');
 expect(run({...base,unreadable:true})).toBe('0');
});

it('kernel executable-path fallback accepts complete absolute identity and refuses conflicts or malformed fields',()=>{
 const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8');
 const program=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!.split('def argv_env(pid):')[0]!;
 const check=String.raw`
assert executable_path('/private/fixture/codex',None)=='/private/fixture/codex'
assert executable_path('/private/fixture/codex','/private/fixture/codex')=='/private/fixture/codex'
assert executable_path('/System/WebContent','/Cryptex/WebContent')=='/Cryptex/WebContent'
for kernel,native in [('',None),('codex',None),('/absolute/with\ncontrol',None),('/private/codex','/private/b'),('/private/a','relative')]:
 try: executable_path(kernel,native)
 except (ValueError,OSError): continue
 raise AssertionError('unproven executable accepted')
print('PASS')`;
 expect(execFileSync('python3',['-c',program+check],{encoding:'utf8'}).trim()).toBe('PASS');
});

it('global census exempts a matching session name only for a proven different node and realpath-distinct home',()=>{
 const source=readFileSync(new URL('../src/domain/codex-rehost-integration.ts',import.meta.url),'utf8');
 const program=source.match(/const STOPPED_CENSUS_PY = String.raw`([\s\S]*?)`;/)![1]!;
 const root=mkdtempSync(path.join(tmpdir(),'census-home-namespace-'));
 const currentHome=path.join(root,'current'),otherHome=path.join(root,'other'),aliasHome=path.join(root,'current-alias');
 mkdirSync(currentHome);mkdirSync(otherHome);symlinkSync(currentHome,aliasHome,'dir');
 const pythonFixture=String.raw`
def argv_env(pid):
 case=json.loads(os.environ['RECOVERY_KERNEL_FIXTURE'])
 if case.get('unreadable'): raise ValueError()
 return case['argv'],[x.encode() for x in case['env']],case['exe']
subprocess.check_output=lambda *args,**kwargs: ('4321 '+str(os.getuid())+' S\n').encode()
os.kill=lambda *args: None
`;
 const originalHome=process.env['OPENRIG_HOME'];
 const fixture=(env:string[],argv:string[]=['/bin/zsh'],exe='/bin/zsh')=>({env,argv,exe});
 const foreignSameSession=(home:string,extra:string[]=[]):string[]=>[
  'OPENRIG_NODE_ID=peer@xv',`OPENRIG_SESSION_NAME=${seat}`,`OPENRIG_HOME=${home}`,'OPENRIG_OCCUPANT_GENERATION=peer-g1',...extra,
 ];
 const run=(value:unknown,current: string|null)=>{
  const environment:NodeJS.ProcessEnv={...process.env,RECOVERY_KERNEL_FIXTURE:JSON.stringify(value)};
  if(current===null)delete environment['OPENRIG_HOME'];else environment['OPENRIG_HOME']=current;
  const expected={OPENRIG_NODE_ID:seat,OPENRIG_SESSION_NAME:seat,OPENRIG_HOME:current};
  return execFileSync('python3',['-c',program.replace('ok=False',pythonFixture+'\nok=False'),JSON.stringify(expected),'10',nativeId],{env:environment,encoding:'utf8'}).trim();
 };
 try {
  // Only a different node with two existing, absolute, realpath-distinct homes
  // makes this same display name unrelated to the target.
  expect(run(fixture(foreignSameSession(otherHome)),currentHome)).toBe('1');
  expect(run(fixture(foreignSameSession(currentHome)),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(aliasHome)),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession('relative-home')),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(path.join(root,'missing-home'))),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome).filter(v=>!v.startsWith('OPENRIG_NODE_ID=')),),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome,[`OPENRIG_NODE_ID=${seat}`])),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome,[`OPENRIG_NODE_ID=third@xv`])),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome).filter(v=>!v.startsWith('OPENRIG_HOME=')),),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome,[`OPENRIG_HOME=${otherHome}`])),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome,[`OPENRIG_OCCUPANT_GENERATION=stale-generation`])),currentHome)).toBe('1');
  expect(run(fixture(['OPENRIG_NODE_ID='+seat,`OPENRIG_SESSION_NAME=${seat}`,`OPENRIG_HOME=${otherHome}`,'OPENRIG_OCCUPANT_GENERATION=stale-generation']),currentHome)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome)),null)).toBe('0');
  expect(run(fixture(foreignSameSession(otherHome)),'' )).toBe('0');
  // The identity exemption never hides a process running the exact native Codex thread.
  expect(run(fixture(['OPENRIG_NODE_ID=peer@xv','OPENRIG_SESSION_NAME=other@xv',`OPENRIG_HOME=${otherHome}`,'OPENRIG_OCCUPANT_GENERATION=peer-g1'],['/usr/bin/codex','--no-daemon','resume',nativeId],'/usr/bin/codex'),currentHome)).toBe('0');
  expect(run(fixture(['OPENRIG_NODE_ID=peer@xv','OPENRIG_SESSION_NAME=other@xv',`OPENRIG_HOME=${otherHome}`,'OPENRIG_OCCUPANT_GENERATION=peer-g1'],['/usr/bin/codex','--no-daemon','resume','unrelated-thread'],'/usr/bin/codex'),currentHome)).toBe('1');
 } finally {
  if(originalHome===undefined)delete process.env['OPENRIG_HOME'];else process.env['OPENRIG_HOME']=originalHome;
  rmSync(root,{recursive:true,force:true});
 }
});
