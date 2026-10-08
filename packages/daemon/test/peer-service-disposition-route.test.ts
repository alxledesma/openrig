import { afterEach,beforeEach,describe,expect,it,vi } from 'vitest';
import { createDb } from '../src/db/connection.js';
import { seed } from './helpers/coordinator-fixture.js';
import { SeatDeliveryGuard,resolveGuardTarget } from '../src/domain/seat-delivery-guard.js';
import { createTestApp,mockTmuxAdapter } from './helpers/test-app.js';
const control=vi.hoisted(()=>({address:'127.0.0.1',writes:0,late:false,change:()=>{}}));
vi.mock('@hono/node-server/conninfo',()=>({getConnInfo:()=>({remote:{address:control.address}})}));
vi.mock('../src/domain/peer-service-disposition.js',()=>({PeerServiceDispositionStore:class{
 async record(input:unknown,authorize:()=>void){authorize();if(control.late)control.change();authorize();control.writes++;return input;}
}}));
const op='operator-agent@kernel',peer='lead@xv',token='fixture-service-test-token';
describe('authenticated peer-service disposition route',()=>{
 let db:ReturnType<typeof createDb>,app:ReturnType<typeof createTestApp>['app'];
 const headers=()=>({Authorization:`Bearer ${token}`,'X-OpenRig-Session':op,'X-OpenRig-Occupant-Generation':'operator-agent-g1'});
 const body={pid:4321,purpose:'Root approved exact service artifact',evidencePath:'/private/provenance.json',evidenceSha256:'a'.repeat(64),approvedArtifactSha256:'b'.repeat(64)};
 const post=(extra={},h:Record<string,string>=headers(),seat=peer)=>app.request('/api/seat/peer-service-disposition/'+encodeURIComponent(seat),{method:'POST',headers:{'Content-Type':'application/json',...h},body:JSON.stringify({...body,...extra})});
 beforeEach(()=>{
  control.address='127.0.0.1';control.writes=0;control.late=false;db=createDb();seed(db);
  db.prepare("UPDATE sessions SET status='running',startup_status='ready' WHERE node_id=?").run(op);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('operator-service-binding',?,?, '%9')").run(op,op);
  db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('peer-service-binding',?,?, '%78')").run(peer,peer);
  const guard=new SeatDeliveryGuard(db,name=>resolveGuardTarget(db,name));
  ({app}=createTestApp(db,{tmux:Object.assign(mockTmuxAdapter(),{deliveryGuard:guard}),appDeps:{terminalBearerToken:token}}));
  control.change=()=>db.prepare("UPDATE sessions SET status='detached' WHERE node_id=?").run(op);
 });afterEach(()=>db.close());
 it('binds genuine current ready peer Operator and revalidates before durable write',async()=>{
  const r=await post();expect(r.status).toBe(200);expect(control.writes).toBe(1);expect((await r.json()).disposition).toMatchObject({actor:op,actorGeneration:'operator-agent-g1',nodeId:peer,sessionName:peer});
  control.late=true;expect((await post()).status).toBe(409);expect(control.writes).toBe(1);
 });
 it('refuses wrong transport/generation, self target, unknown origin and authentication',async()=>{
  for(const h of [{...headers(),'X-OpenRig-Session':peer},{...headers(),'X-OpenRig-Occupant-Generation':'stale'},{...headers(),'X-OpenRig-Origin-Unknown':'true'}])expect((await post({},h)).status).toBe(409);
  expect((await post({},headers(),op)).status).toBe(409);expect((await post({},{...headers(),Authorization:'Bearer wrong'})).status).toBe(401);expect(control.writes).toBe(0);
 });
 it('refuses remote/origin and malformed, arbitrary added actor fields',async()=>{
  control.address='10.1.1.1';expect((await post()).status).toBe(403);control.address='127.0.0.1';expect((await post({},{...headers(),Origin:'https://example.invalid'})).status).toBe(403);
  for(const extra of [{pid:1},{evidencePath:'relative'},{evidenceSha256:'invalid'},{actor:'body-forgery'}])expect((await post(extra)).status).toBe(400);expect(control.writes).toBe(0);
 });
});
