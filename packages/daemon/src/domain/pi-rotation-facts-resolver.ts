import type Database from "better-sqlite3";
import { lstatSync, realpathSync } from "node:fs";
import { isAbsolute, normalize } from "node:path";
import type { TmuxAdapter } from "../adapters/tmux.js";
import type { PiRunnerState } from "../adapters/pi-runner-protocol.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";
import { parseNativeModelWindow } from "./model-window.js";
import { observePiRotationLaunch } from "./pi-rotation-launch-proof.js";
import { piRotationContract, type PiRotationNode } from "./pi-rotation-native-proof.js";

export interface PiRotationFactsDeps {
  db: Database.Database; tmux: TmuxAdapter;
  piState(session: string): Promise<unknown>;
  piProof(session: string, generation: string): Promise<PiNativeProof | null>;
  piRotation?: { agentDir(session: string): string; runnerEntryPath: string };
}
export function canonicalPiSessionFile(value: unknown): value is string {
  if (typeof value !== "string" || !isAbsolute(value) || normalize(value) !== value || /[\x00-\x1f\x7f]/.test(value)) return false;
  try { const stat = lstatSync(value); return stat.isFile() && !stat.isSymbolicLink() && realpathSync(value) === value; } catch { return false; }
}
/** Typed projection of daemon-owned JSON. Consumers still validate every field used. */
export function piRotationSidecar(value: unknown): PiRunnerState | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as PiRunnerState : null;
}
interface CurrentPiRow extends PiRotationNode { nodeId: string; sessionName: string; pane: string }
function current(deps: PiRotationFactsDeps, seat: string): CurrentPiRow | null {
  return deps.db.prepare(`SELECT n.id nodeId,n.runtime,n.model,n.policy_launch_posture launchPosture,
    s.session_name sessionName,s.status sessionStatus,s.startup_status startupStatus,s.resume_type resumeType,s.resume_token resumeToken,
    t.generation_uuid generation,b.tmux_pane pane FROM nodes n JOIN sessions s ON s.node_id=n.id
    JOIN occupant_tenures t ON t.node_id=n.id JOIN bindings b ON b.node_id=n.id
    WHERE s.session_name=? ORDER BY s.id DESC,t.generation_ordinal DESC LIMIT 1`).get(seat) as CurrentPiRow | undefined ?? null;
}
/** Native presence is independent of idle. This is also used when the genuine
 * target submits a checkpoint draft while its own authenticated tool is busy. */
export async function observePiRotationIdentity(deps: PiRotationFactsDeps, seat: string) {
  const row = current(deps, seat), config = deps.piRotation;
  if (!row || !config || row.runtime !== "pi" || !row.generation || row.sessionStatus !== "running" || row.startupStatus !== "ready"
    || row.resumeType !== "pi_session_file" || !canonicalPiSessionFile(row.resumeToken)) throw Error("Current canonical Pi native binding unavailable");
  const raw = piRotationSidecar(await deps.piState(seat)), proof = await deps.piProof(seat, row.generation);
  if (!raw || !proof || proof.state !== "present") throw Error("Current Pi native proof unavailable");
  const agentDir = config.agentDir(seat), launch = await observePiRotationLaunch({nodeId:row.nodeId,sessionName:seat,generation:row.generation,
    sessionFile:row.resumeToken,agentDir,runnerEntryPath:config.runnerEntryPath,proof}, {
    tmux:deps.tmux, sidecar:async session=>piRotationSidecar(await deps.piState(session)),
    currentBinding:async nodeId=>{const now=current(deps,seat);return now && now.nodeId===nodeId && now.generation ? {
      nodeId,sessionName:now.sessionName,generation:now.generation,runtime:now.runtime!,pane:now.pane,sessionFile:now.resumeToken} : null;},
  });
  const after = piRotationSidecar(await deps.piState(seat));
  const identity = (s: PiRunnerState | null) => s && ({ready:s.ready,exited:s.exited,launchId:s.launchId,sessionFile:s.sessionFile,lastEntryId:s.lastEntryId,
    model:s.model,generation:s.quiescence?.generation,failures:s.runtimeReadiness?.failures});
  if (!launch || JSON.stringify(current(deps,seat))!==JSON.stringify(row) || !canonicalPiSessionFile(row.resumeToken)
    || JSON.stringify(identity(after))!==JSON.stringify(identity(raw))) throw Error("Pi native binding changed during OS observation");
  const model = parseNativeModelWindow(raw.model);
  if (!model || row.model!==`${model.provider}/${model.id}`) throw Error("Actual Pi model differs from pinned launch contract");
  return {row,state:raw,proof:{...proof,verifiedLaunch:launch},model,agentDir};
}
/** Full fresh idle history/header/model/effort/trust proof for reserved rotation.
 * No caller-provided PID, session path or readiness assertion is accepted. */
export async function resolvePiRotationNativeState(deps: PiRotationFactsDeps, seat: string) {
  const found = await observePiRotationIdentity(deps,seat), {state,proof,row} = found, r = state.runtimeReadiness, q = state.quiescence;
  const at = Date.parse(q?.observedAt ?? ""), now = Date.now();
  if (!r || !q || r.generation!==row.generation || r.launchId!==state.launchId || r.sessionFile!==row.resumeToken
    || q.launchId!==state.launchId || q.generation!==row.generation || q.sessionFile!==row.resumeToken || q.lastEntryId!==state.lastEntryId
    || !Number.isFinite(at) || at>now || now-at>5000 || JSON.stringify(parseNativeModelWindow(r.model))!==JSON.stringify(found.model)) {
    throw Error("Fresh exact Pi readiness/model binding unavailable");
  }
  const result = piRotationContract(row.resumeToken!,{ready:state.ready===true&&!state.exited,launchId:state.launchId,generation:r.generation,
    sessionFile:state.sessionFile,lastEntryId:state.lastEntryId,model:found.model,observedAt:r.observedAt,failures:r.failures},proof,row,found.agentDir,now);
  if (!result.ok) throw Error(`Pi rotation proof held: ${result.hold}`);
  return {...found,usage:{sessionId:row.resumeToken!,transcriptPath:row.resumeToken!},runtimeContract:result.contract};
}
