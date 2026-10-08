import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {createHash} from 'node:crypto';
import {mkdtempSync,readFileSync,realpathSync,rmSync,writeFileSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import type Database from 'better-sqlite3';
import {Hono} from 'hono';
const transport=vi.hoisted(()=>({address:'127.0.0.1'}));
vi.mock('@hono/node-server/conninfo',()=>({getConnInfo:()=>({remote:{address:transport.address}})}));
import {createDb} from '../src/db/connection.js';
import {seed} from './helpers/coordinator-fixture.js';
import {SeatDeliveryGuard,resolveGuardTarget} from '../src/domain/seat-delivery-guard.js';
import {CodexSameGenerationRehost,createOperatorMaintenanceAuthority,type CodexRehostOptions,type OperatorMaintenanceAuthority} from '../src/domain/codex-rehost.js';
import {stoppedCodexContract} from '../src/domain/codex-rehost-integration.js';
import {seatRoutes} from '../src/routes/seat.js';
import {RigRepository} from '../src/domain/rig-repository.js';
import {SessionRegistry} from '../src/domain/session-registry.js';
import {EventBus} from '../src/domain/event-bus.js';
import type {NativeProcessRow} from '../src/domain/native-process-lineage.js';
const sha=(b:Buffer)=>createHash('sha256').update(b).digest('hex');
const seat='operator-agent@kernel',generation='operator-agent-g1',nativeId='original-operator-thread';
const reason='Independent local terminal maintenance of exact Operator';
const tree=(pid:number):NativeProcessRow[]=>[{pid:10,ppid:1,command:'/bin/zsh',executableName:'zsh',startedAt:'stable-pane',pgid:10,tpgid:pid},
 {pid,ppid:10,command:`/usr/bin/codex --no-daemon -p exact resume ${nativeId}`,executableName:'codex',startedAt:'native-'+pid,pgid:pid,tpgid:pid}];
describe('independent Operator terminal maintenance',()=>{
 let db:Database.Database,dir:string,file:string,app:Hono,guard:SeatDeliveryGuard,options:CodexRehostOptions,service:CodexSameGenerationRehost;
 let processes:NativeProcessRow[],signal:ReturnType<typeof vi.fn>,resume:ReturnType<typeof vi.fn>,configuredToken:string|null;
 const packet=()=>({reason,expected:{nodeId:seat,generation}});
 const post=(body:unknown=packet(),headers:Record<string,string>={Authorization:'Bearer fixture-token'})=>app.request('/api/seat/operator-maintenance/rehost-runner',{method:'POST',headers:{'Content-Type':'application/json',...headers},body:JSON.stringify(body)});
 const snapshot=()=>JSON.stringify(Object.fromEntries(['nodes','sessions','bindings','occupant_tenures','queue_items','coordinator_authority','coordinator_resources','outbox_entries'].map(t=>[t,db.prepare('SELECT * FROM '+t).all()])));
 beforeEach(async()=>{
  dir=realpathSync(mkdtempSync(path.join(tmpdir(),'operator-maintenance-')));file=path.join(dir,'native.jsonl');
  writeFileSync(file,JSON.stringify({type:'session_meta',payload:{id:nativeId,model_provider:'openai'}})+'\n'+JSON.stringify({type:'turn_context',payload:{model:'gpt-6-luna',effort:'high',sandbox_policy:{type:'workspace-write'},approval_policy:'never'}})+'\n',{mode:0o600});
  db=createDb();seed(db);db.prepare("INSERT INTO self_host_identity VALUES(1,'test-host',?,?)").run(new Date().toISOString(),new Date().toISOString());
  db.prepare("UPDATE nodes SET runtime='codex',cwd=?,model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(dir,seat);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token=? WHERE node_id=?").run(nativeId,seat);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('binding',?,?,'%1')").run(seat,seat);
  guard=new SeatDeliveryGuard(db,s=>resolveGuardTarget(db,s));await guard.set(seat,true,'operator','isolated test');
  processes=tree(20);signal=vi.fn(()=>{processes=[tree(20)[0]!];});resume=vi.fn(async()=>{expect(guard.ownsRunnerRehost(seat)).toBe(true);processes=tree(40);return {ok:true as const};});
  const now=Date.now();options={db,guard,tmux:{getPanePid:async()=>10},snapshotRoot:path.join(dir,'private'),resume:{resume},
   nativeState:async()=>({nodeId:seat,sessionName:seat,nativeId,transcriptPath:file,runtimeContract:{runtime:'codex',model:'gpt-6-luna',provider:'openai',profile:'exact',effort:'high',permissions:{sandbox:{type:'workspace-write'},approval:'never'}}}),
   activityWitness:async()=>({seatNodeId:seat,sessionName:seat,rung:'window-sampling',sourceId:'fixture',seq:1,observedAt:new Date(now).toISOString(),activity:'idle'}),
   preflightSupervisedLaunch:async()=>({posture:'floor',effective:{model:'gpt-6-luna',provider:'openai',effort:'high',approval:'never',sandbox:'workspace-write'},evidenceDigest:'a'.repeat(64)}),
   observeSupervisedReplacement:async()=>({launchId:'verified-launch',fingerprint:'independent-proof'}),
   stoppedNativeState:async b=>({nodeId:b.nodeId,sessionName:b.sessionName,nativeId:b.nativeId,transcriptPath:file,runtimeContract:stoppedCodexContract(file,b)}),
   proveStoppedIdentityAbsent:async()=>true,listProcesses:async()=>processes,verifyProcessIdentity:async()=>true,signal,now:()=>now,sleep:async()=>{},waitMs:1,pollMs:1};
  service=new CodexSameGenerationRehost(options);configuredToken='fixture-token';transport.address='127.0.0.1';
  const context={rigRepo:new RigRepository(db),sessionRegistry:new SessionRegistry(db),eventBus:new EventBus(db),tmuxAdapter:{deliveryGuard:guard},codexRehost:service};
  app=new Hono();app.use('*',async(c,next)=>{for(const [k,v] of Object.entries(context))c.set(k as never,v as never);c.set('terminalBearerToken' as never,configuredToken as never);await next();});app.route('/api/seat',seatRoutes);
 });
 afterEach(()=>{db?.close();rmSync(dir,{recursive:true,force:true});});
 async function stopped(maintenance=false){
  signal.mockImplementation(()=>{});
  const result=maintenance?await (await post()).json():await service.rehost({nodeId:seat,sessionName:seat,reason,operator:seat});
  expect(result).toMatchObject({ok:false,code:'codex_rehost_stop_unknown',effectAttempted:true});
  const attempt=path.dirname(result.receiptPath),began=readFileSync(result.receiptPath),unknown=readFileSync(path.join(attempt,'unknown.json'));
  processes=[tree(20)[0]!];signal.mockClear();resume.mockClear();
  return {attempt,began,unknown,recovery:{attemptId:JSON.parse(began.toString()).attemptId,beganSha256:sha(began)}};
 }
 it('performs normal rehost with maintenance provenance and no caller agent identity',async()=>{
  const before=snapshot(),response=await post(),result=await response.json();expect(response.status).toBe(200);expect(result).toMatchObject({ok:true,generation,generationUnchanged:true,custodyPreserved:true});
  expect(JSON.parse(readFileSync(result.receiptPath,'utf8'))).toMatchObject({actor:'local-terminal-maintenance',maintenanceProvenance:{principal:'local-terminal-maintenance',nodeId:seat,sessionName:seat,generation,mode:'rehost'}});
  expect(signal).toHaveBeenCalledTimes(1);expect(resume).toHaveBeenCalledTimes(1);expect(snapshot()).toBe(before);expect(guard.ownsRunnerRehost(seat)).toBe(false);
 });
 it.each([false,true])('continues stop UNKNOWN with native Operator absent, original maintenance=%s, without repeated signal',async maintenance=>{
  const saved=await stopped(maintenance),before=snapshot();const response=await post({...packet(),codexStoppedRecovery:saved.recovery});expect(await response.json()).toMatchObject({ok:true,generation,custodyPreserved:true});expect(response.status).toBe(200);
  const journal=JSON.parse(readFileSync(path.join(saved.attempt,'recovery-began.json'),'utf8'));expect(journal).toMatchObject({actor:'local-terminal-maintenance',maintenanceProvenance:{nodeId:seat,generation,mode:'stopped-recovery'}});expect(journal).not.toHaveProperty('actorGeneration');
  expect(readFileSync(path.join(saved.attempt,'began.json'))).toEqual(saved.began);expect(readFileSync(path.join(saved.attempt,'unknown.json'))).toEqual(saved.unknown);expect(snapshot()).toBe(before);
  expect(signal).not.toHaveBeenCalled();expect(resume).toHaveBeenCalledExactlyOnceWith(seat,'codex_id',nativeId,dir,'exact','floor','gpt-6-luna','high',generation);
  expect(await (await post({...packet(),codexStoppedRecovery:saved.recovery})).json()).toMatchObject({ok:false,code:'codex_rehost_recovery_replay',effectAttempted:false});expect(resume).toHaveBeenCalledTimes(1);
 });
 it.each(['missing-token','wrong-token','unconfigured','remote','origin','empty-origin','agent-header','operator-body','authority-body','proof-body','path-body','non-operator','wrong-generation','recovery-extra'])('refuses %s before native effects',async kind=>{
  let body:Record<string,unknown>=packet(),headers:Record<string,string>={Authorization:'Bearer fixture-token'};
  if(kind==='missing-token')headers={};if(kind==='wrong-token')headers.Authorization='Bearer wrong';if(kind==='unconfigured')configuredToken=null;if(kind==='remote')transport.address='192.0.2.1';
  if(kind==='origin')headers.Origin='https://example.invalid';if(kind==='empty-origin')headers.Origin='';if(kind==='agent-header')headers['X-OpenRig-Session']=seat;
  if(kind==='operator-body')body.operator=seat;if(kind==='authority-body')body.maintenanceAuthority={principal:'local-terminal-maintenance'};if(kind==='proof-body')body.nativeProof={pid:20};if(kind==='path-body')body.path=file;
  if(kind==='non-operator')body.expected={nodeId:'lead@xv',generation:'lead-g1'};if(kind==='wrong-generation')body.expected={nodeId:seat,generation:'old'};if(kind==='recovery-extra')body.codexStoppedRecovery={attemptId:'a',beganSha256:'a'.repeat(64),pid:20};
  expect((await post(body,headers)).status).toBeGreaterThanOrEqual(400);expect(signal).not.toHaveBeenCalled();expect(resume).not.toHaveBeenCalled();
 });
 it('rejects structurally forged capability and genuine capability for another target',async()=>{
  const authority=createOperatorMaintenanceAuthority({nodeId:seat,generation});
  expect(await service.rehost({nodeId:seat,sessionName:seat,reason,maintenanceAuthority:{...authority} as OperatorMaintenanceAuthority})).toMatchObject({ok:false,code:'codex_rehost_maintenance_identity',effectAttempted:false});
  await guard.set('lead@xv',true,'operator','isolated target refusal');
  expect(await service.rehost({nodeId:'lead@xv',sessionName:'lead@xv',reason,maintenanceAuthority:authority})).toMatchObject({ok:false,code:'codex_rehost_maintenance_identity',effectAttempted:false});expect(signal).not.toHaveBeenCalled();
 });
 it.each(['actor','digest','custody','profile','durable-stopped','not-ready','generation','live','lost-resume'])('preserves %s recovery fence',async kind=>{
  const saved=await stopped();
  if(kind==='actor'){const b=JSON.parse(saved.began.toString());b.actor='peer@xv';writeFileSync(path.join(saved.attempt,'began.json'),JSON.stringify(b)+'\n');saved.recovery.beganSha256=sha(readFileSync(path.join(saved.attempt,'began.json')));}
  if(kind==='digest')saved.recovery.beganSha256='0'.repeat(64);if(kind==='custody')db.prepare("UPDATE nodes SET role='changed' WHERE id=?").run(seat);
  if(kind==='profile')options.preflightSupervisedLaunch=async()=>({posture:'floor',effective:{model:'gpt-6-luna',provider:'openai',effort:'high',approval:'never',sandbox:'workspace-write'},evidenceDigest:'b'.repeat(64)});
  if(kind==='durable-stopped')db.prepare("UPDATE sessions SET status='exited' WHERE node_id=?").run(seat);if(kind==='not-ready')db.prepare("UPDATE sessions SET startup_status='failed' WHERE node_id=?").run(seat);
  if(kind==='generation')db.prepare("UPDATE occupant_tenures SET generation_uuid='new' WHERE node_id=?").run(seat);if(kind==='live')processes=tree(20);
  if(kind==='lost-resume')resume.mockImplementation(async()=>{throw new Error('lost transport');});
  const result=await (await post({...packet(),codexStoppedRecovery:saved.recovery})).json();expect(result.ok).toBe(false);expect(signal).not.toHaveBeenCalled();
  if(kind==='lost-resume'){expect(result.effectAttempted).toBe(true);expect(existsSync(path.join(saved.attempt,'recovery-unknown.json'))).toBe(true);expect(await (await post({...packet(),codexStoppedRecovery:saved.recovery})).json()).toMatchObject({ok:false,code:'codex_rehost_recovery_replay'});expect(resume).toHaveBeenCalledTimes(1);}
  else{expect(resume).not.toHaveBeenCalled();expect(existsSync(path.join(saved.attempt,'recovery-began.json'))).toBe(false);}
 });
});
