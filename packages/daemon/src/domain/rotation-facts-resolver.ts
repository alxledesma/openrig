import { parse as parseToml } from "smol-toml";
import { homedir } from "node:os";
import type Database from 'better-sqlite3';
import {readFileSync,realpathSync} from 'node:fs';import {resolve,sep} from 'node:path';import {createHash} from 'node:crypto';import {execFile} from 'node:child_process';import{promisify}from'node:util';
import type{WhoamiService}from'./whoami-service.js';import type{SeatActivityService}from'./seat-activity-service.js';import type{TmuxAdapter}from'../adapters/tmux.js';import{codexRotationContract}from'./rotation-native-proof.js';import{assertRotationPrecondition,assertManagedUnattended}from'./rotation-precondition.js';
import { rotationLocalAddresses, rotationActiveQueueRows } from './rotation-local-custody.js';
const exec=promisify(execFile);
export function rotationFactsResolver(deps:{db:Database.Database;whoami:WhoamiService;activity:SeatActivityService;tmux:TmuxAdapter;root:string}) {
 return async(seat:string,expected:Record<string,unknown>):Promise<void>=>{
  const policy=JSON.parse(readFileSync(resolve(deps.root,"policy.json"),"utf8"));
  const root=realpathSync(deps.root);if(typeof expected.checkpointPath!=='string')throw new Error('Checkpoint path required');
  const path=realpathSync(resolve(root,expected.checkpointPath));if(!path.startsWith(root+sep+'frozen'+sep))throw new Error('Checkpoint outside immutable attempt directory');
  const bytes=readFileSync(path);const checkpointHash=createHash('sha256').update(bytes).digest('hex');const receipt=JSON.parse(bytes.toString());
  assertManagedUnattended(policy,seat,receipt);
  assertCoordinatorRotationSafe(deps.db,seat);
  const {who,usage,runtimeContract}=await resolveRotationNativeState(deps,seat);
  if(receipt.generation!==usage.sessionId)throw new Error("Checkpoint native generation differs");
  const queue=rotationActiveQueueRows(deps.db,seat);
  const witness=deps.activity.getRotationActivityWitness(who.identity.nodeId);
  assertRotationPrecondition(expected,{generation:usage.sessionId??null,queue,runtimeContract,activity:witness?.activity??null,observedAt:witness?Date.parse(witness.observedAt):NaN,checkpointHash});
 };
}

/** Rotation cannot silently orphan the sole coordinator authority generation. */
export function assertCoordinatorRotationSafe(db:Database.Database,seat:string):void {
 if(!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_authority'").get())return;
 const [canonicalSeat,localAlias]=rotationLocalAddresses(db,seat);
 if(db.prepare("SELECT 1 FROM coordinator_authority WHERE owner_session IN (?,?)").get(canonicalSeat,localAlias))throw new Error("Current coordinator authority holder must transfer and reconcile its baton before automatic rotation");
}

/** Keep comm as the final ps column; args is separately queried for the selected native PID. */
export function parseProcessInventory(text:string):RegExpExecArray[]{return text.split("\n").map(line=>/^\s*(\d+)\s+(\d+)\s+(.+?)\s*$/.exec(line)).filter(Boolean) as RegExpExecArray[];}
export function assertSuccessorProfileContinuity(config:Record<string,unknown>,profile:Record<string,unknown>,contract:Record<string,unknown>,argv:string[]):void {
 const arg=(short:string,long:string)=>{const i=argv.findIndex(a=>a===short||a===long);return i>=0?argv[i+1]:undefined;};
 const approval=arg("-a","--ask-for-approval")??profile["approval_policy"]??config["approval_policy"];
 const provider=profile["model_provider"]??config["model_provider"]??"openai";
 const effort=profile["model_reasoning_effort"]??config["model_reasoning_effort"]??null;
 if(provider!==contract.provider||effort!==contract.effort||approval!==(contract.permissions as {approval:string}).approval)throw new Error("Successor native profile would change provider, effort or approval policy");
}

export async function resolveRotationNativeState(deps:{db:Database.Database;whoami:WhoamiService;tmux:TmuxAdapter},seat:string) {
  const who=deps.whoami.resolve({sessionName:seat,compact:false});if(!who || who.identity.runtime!=='codex')throw new Error('Native rotation proof currently supports Codex only');
  const usage=who.contextUsage as {sessionId?:string;transcriptPath?:string;fresh?:boolean}|undefined;
  if(!usage?.fresh || !usage.sessionId || !usage.transcriptPath)throw new Error('Current native generation/checkpoint unavailable');
  const node=deps.db.prepare('SELECT model,codex_config_profile FROM nodes WHERE id=?').get(who.identity.nodeId) as {model:string|null;codex_config_profile:string|null};
  const binding=deps.db.prepare('SELECT tmux_pane FROM bindings WHERE node_id=?').get(who.identity.nodeId) as {tmux_pane:string|null}|undefined;
  if(!binding?.tmux_pane)throw new Error('Managed pane unavailable');
  const pid=await deps.tmux.getPanePid(binding.tmux_pane);if(!pid)throw new Error('Managed process PID unavailable');
  const result=await exec('/bin/ps',['-axo','pid=,ppid=,comm='],{maxBuffer:16*1024*1024});
  const processes=parseProcessInventory(result.stdout);
  const descendants=new Set([pid]);for(let i=0;i<processes.length;i++){let added=false;for(const p of processes)if(descendants.has(Number(p[2]))&&!descendants.has(Number(p[1]))){descendants.add(Number(p[1]));added=true;}if(!added)break;}
  const native=processes.filter(p=>descendants.has(Number(p[1]))&&/(^|\/)codex$/.test(p[3]!));if(native.length!==1)throw new Error('Exactly one native Codex process required');
  const args=await exec('/bin/ps',['-p',native[0]![1]!,'-o','args='],{maxBuffer:1024*1024});
  const argv=args.stdout.trim().split(/\s+/);const runtimeContract=codexRotationContract(usage.transcriptPath,usage.sessionId,argv,node.model,node.codex_config_profile);
  const config=parseToml(readFileSync(resolve(process.env["CODEX_HOME"]??resolve(homedir(),".codex"),"config.toml"),"utf8")) as Record<string,unknown>;
  if(!node.codex_config_profile || !/^[a-zA-Z0-9_-]+$/.test(node.codex_config_profile))throw new Error("Safe native successor profile required");
  const profilePath=resolve(process.env["CODEX_HOME"]??resolve(homedir(),".codex"),`${node.codex_config_profile}.config.toml`);
  const profile=parseToml(readFileSync(profilePath,"utf8")) as Record<string,unknown>;
  assertSuccessorProfileContinuity(config,profile,runtimeContract,argv);
  return {who,usage,runtimeContract};
}
