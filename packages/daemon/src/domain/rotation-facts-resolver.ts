import { parse as parseToml } from "smol-toml";
import { homedir } from "node:os";
import type Database from 'better-sqlite3';
import {readFileSync,realpathSync} from 'node:fs';import {resolve,sep} from 'node:path';import {createHash} from 'node:crypto';import {execFile} from 'node:child_process';import{promisify}from'node:util';
import type{WhoamiService}from'./whoami-service.js';import type{SeatActivityService}from'./seat-activity-service.js';import type{TmuxAdapter}from'../adapters/tmux.js';import{codexRotationContract}from'./rotation-native-proof.js';import{assertRotationPrecondition,assertManagedUnattended}from'./rotation-precondition.js';
import { rotationLocalAddresses, rotationActiveQueueRows } from './rotation-local-custody.js';
import { proveCodexNativeThread } from "./codex-native-thread-proof.js";
import { resolveGuardTarget } from "./seat-delivery-guard.js";
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

type CodexNativeStateDeps = { db: Database.Database; whoami: WhoamiService; tmux: TmuxAdapter };

/** Rotation's capacity/checkpoint policy still requires fresh token telemetry. */
export async function resolveRotationNativeState(deps: CodexNativeStateDeps, seat: string) {
  return resolveNativeState(deps, seat, true);
}

/** Native history/identity is independent of token-count age. This does not
 * grant fresh usage, idle, kernel identity or custody: callers retain those gates. */
export async function resolveCodexNativeState(deps: CodexNativeStateDeps, seat: string) {
  return resolveNativeState(deps, seat, false);
}

async function resolveNativeState(deps: CodexNativeStateDeps, seat: string, requireFreshUsage: boolean) {
  // Resolve the current managed identity once; retained rigs may share its name.
  const target = resolveGuardTarget(deps.db, seat);
  if (!target || target.session !== seat) throw new Error("Current managed Codex identity unavailable");
  const who = deps.whoami.resolve({ nodeId: target.nodeId, compact: false });
  if (!who || who.identity.nodeId !== target.nodeId || who.identity.sessionName !== target.session || who.identity.runtime !== "codex") throw new Error("Native rotation proof currently supports Codex only");
  const usage = who.contextUsage as { sessionId?: string; transcriptPath?: string; fresh?: boolean } | undefined;
  if (!usage?.sessionId || !usage.transcriptPath || (requireFreshUsage && !usage.fresh)) {
    throw new Error("Current native generation/checkpoint unavailable");
  }
  const currentSession = () => deps.db.prepare(`SELECT n.runtime,n.model,n.codex_config_profile,
    s.id sessionId,s.session_name sessionName,s.resume_type resumeType,s.resume_token nativeId
    FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1`)
    .get(who.identity.nodeId) as { runtime: string; model: string | null; codex_config_profile: string | null;
      sessionId: string; sessionName: string; resumeType: string; nativeId: string | null } | undefined;
  const node = currentSession();
  if (!node || node.runtime !== "codex" || node.sessionName !== who.identity.sessionName
    || node.resumeType !== "codex_id" || !node.nativeId) throw new Error("Current saved Codex session unavailable");
  if (usage.sessionId !== node.nativeId) throw new Error("Current native thread differs from saved session");
  const binding = deps.db.prepare("SELECT tmux_pane FROM bindings WHERE node_id=?").get(who.identity.nodeId) as { tmux_pane: string | null } | undefined;
  if (!binding?.tmux_pane) throw new Error("Managed pane unavailable");
  const pid = await deps.tmux.getPanePid(binding.tmux_pane);
  if (!pid) throw new Error("Managed process PID unavailable");
  const result = await exec("/bin/ps", ["-axo", "pid=,ppid=,comm="], { maxBuffer: 16 * 1024 * 1024 });
  const processes = parseProcessInventory(result.stdout);
  const descendants = new Set([pid]);
  for (let i = 0; i < processes.length; i++) {
    let added = false;
    for (const p of processes) if (descendants.has(Number(p[2])) && !descendants.has(Number(p[1]))) {
      descendants.add(Number(p[1])); added = true;
    }
    if (!added) break;
  }
  const native = processes.filter(p => descendants.has(Number(p[1])) && /(^|\/)codex$/.test(p[3]!));
  if (native.length !== 1) throw new Error("Exactly one native Codex process required");
  const args = await exec("/bin/ps", ["-p", native[0]![1]!, "-o", "args="], { maxBuffer: 1024 * 1024 });
  const revalidateThread = requireFreshUsage ? undefined
    : await proveCodexNativeThread(Number(native[0]![1]), usage.transcriptPath, args.stdout);
  const argv = args.stdout.trim().split(/\s+/);
  const runtimeContract = codexRotationContract(usage.transcriptPath, usage.sessionId, argv, node.model, node.codex_config_profile);
  const configHome = process.env["CODEX_HOME"] ?? resolve(homedir(), ".codex");
  const config = parseToml(readFileSync(resolve(configHome, "config.toml"), "utf8")) as Record<string, unknown>;
  if (!node.codex_config_profile || !/^[a-zA-Z0-9_-]+$/.test(node.codex_config_profile)) throw new Error("Safe native successor profile required");
  const profile = parseToml(readFileSync(resolve(configHome, `${node.codex_config_profile}.config.toml`), "utf8")) as Record<string, unknown>;
  assertSuccessorProfileContinuity(config, profile, runtimeContract, argv);
  if (JSON.stringify(currentSession()) !== JSON.stringify(node)
    || (deps.db.prepare("SELECT tmux_pane FROM bindings WHERE node_id=?").get(who.identity.nodeId) as { tmux_pane: string | null } | undefined)?.tmux_pane !== binding.tmux_pane
    || await deps.tmux.getPanePid(binding.tmux_pane) !== pid) throw new Error("Current Codex binding changed during native observation");
  await revalidateThread?.();
  return { who, usage, runtimeContract };
}
