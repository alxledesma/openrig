import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import type { CodexRuntimeAdapter } from "../adapters/codex-runtime-adapter.js";
import type { CodexResumeAdapter } from "../adapters/codex-resume.js";
import type { TmuxAdapter } from "../adapters/tmux.js";
import type { SeatActivityService } from "./seat-activity-service.js";
import type { WhoamiService } from "./whoami-service.js";
import type { SeatDeliveryGuard } from "./seat-delivery-guard.js";
import type { Binding } from "./types.js";
import type { CodexDaemonSupportDetector } from "./codex-daemon-support.js";
import { CodexSameGenerationRehost, type CodexRehostNativeState } from "./codex-rehost.js";
import { NativeDutyLaunchStore, observeNativeDutyLaunch, verifyNativeDutyProcessIdentity } from "./native-duty-launch.js";
import { resolveRotationNativeState } from "./rotation-facts-resolver.js";
import { listNativeProcesses } from "./native-process-lineage.js";
import { SeatLaunchEnvironment, structuredNativeExecutable } from "./seat-launch-environment.js";

/** Production composition only. Every effect remains inside the guarded rehost
 * service; this factory cannot grant or renew coordinator authority. */
export function createCodexRehostIntegration(deps: {
  db: Database.Database; guard: SeatDeliveryGuard; tmux: TmuxAdapter;
  whoami: WhoamiService; activity: SeatActivityService;
  adapter: CodexRuntimeAdapter; resume: CodexResumeAdapter;
  launchEnvironment: SeatLaunchEnvironment; store?: NativeDutyLaunchStore;
  launchPath: string; snapshotRoot: string; detectDaemonSupport: CodexDaemonSupportDetector;
  configurationDigest(session: string): string | null | undefined;
}) {
  const currentBinding = async (nodeId: string) => {
    if (!deps.guard.ownsRunnerRehost(nodeId)) return null;
    const target = deps.guard.maybeTarget(nodeId);
    if (!target?.occupant || !target.pane) return null;
    // Only this exact rehost lease and its required guard are exempted for
    // read-only process proof. Durable reservations still refuse the proof.
    if (deps.guard.protectionFacts(nodeId)?.code === "seat_dispatch_reserved") return null;
    const row = deps.db.prepare("SELECT n.runtime,s.resume_token FROM nodes n JOIN sessions s ON s.node_id=n.id WHERE n.id=? ORDER BY s.id DESC LIMIT 1")
      .get(nodeId) as { runtime: string; resume_token: string | null } | undefined;
    const configurationDigest = deps.configurationDigest(target.session);
    if (row?.runtime !== "codex" || !row.resume_token || !configurationDigest) return null;
    return { nodeId, sessionName: target.session, generation: target.occupant,
      runtime: "codex" as const, configurationDigest, pane: target.pane,
      resumeToken: row.resume_token, lifecycleReserved: false };
  };
  return new CodexSameGenerationRehost({
    db: deps.db, guard: deps.guard, tmux: deps.tmux, resume: deps.resume, snapshotRoot: deps.snapshotRoot,
    nativeState: async session => {
      const state = await resolveRotationNativeState(deps, session);
      const contract = state.runtimeContract as CodexRehostNativeState["runtimeContract"];
      if (!state.usage.sessionId || !state.usage.transcriptPath || contract.runtime !== "codex") throw new Error("Exact Codex native history unavailable");
      return { nodeId: state.who.identity.nodeId, sessionName: session, nativeId: state.usage.sessionId,
        transcriptPath: state.usage.transcriptPath, runtimeContract: contract };
    },
    activityWitness: async (nodeId, pane) => {
      if (!deps.guard.ownsRunnerRehost(nodeId)) return null;
      const target = deps.guard.maybeTarget(nodeId);
      if (!target || target.pane !== pane) return null;
      await deps.activity.pollSeat(target.session);
      return deps.activity.getRotationActivityWitness(nodeId);
    },
    preflightSupervisedLaunch: async (binding, native) => {
      if (!deps.store || !deps.guard.ownsRunnerRehost(binding.nodeId)
        || !await deps.launchEnvironment.usesNativeDuty(binding.sessionName, binding.nodeId)) throw new Error("Supervised Codex rehost is not enabled");
      deps.store.assertReady();
      const current = await currentBinding(binding.nodeId);
      if (!current || current.generation !== binding.generation || current.sessionName !== binding.sessionName
        || current.resumeToken !== binding.nativeId) throw new Error("Codex rehost binding changed");
      const stored = deps.db.prepare(`SELECT id,node_id AS nodeId,attachment_type AS attachmentType,
        tmux_session AS tmuxSession,tmux_window AS tmuxWindow,tmux_pane AS tmuxPane,
        external_session_name AS externalSessionName,cmux_workspace AS cmuxWorkspace,
        cmux_surface AS cmuxSurface,updated_at AS updatedAt FROM bindings WHERE node_id=?`)
        .get(binding.nodeId) as Binding | undefined;
      const node = deps.db.prepare("SELECT policy_launch_posture FROM nodes WHERE id=?")
        .get(binding.nodeId) as { policy_launch_posture: string | null } | undefined;
      const sandbox = native.runtimeContract.permissions.sandbox;
      const sandboxType = sandbox && typeof sandbox === "object" ? (sandbox as { type?: unknown }).type : sandbox;
      if (sandboxType !== "workspace-write" && sandboxType !== "danger-full-access") throw new Error("Codex rehost cannot preserve this sandbox posture");
      const posture = sandboxType === "danger-full-access" ? "full_bypass" : "floor";
      if (!stored || stored.tmuxPane !== current.pane || (node?.policy_launch_posture && node.policy_launch_posture !== posture)) throw new Error("Codex rehost persisted posture or pane mismatch");
      const verified = await deps.adapter.preflightRuntimeMigration({ ...stored, cwd: binding.cwd,
        model: binding.model, effort: binding.effort ?? undefined, codexConfigProfile: binding.codexConfigProfile,
        launchPosture: posture });
      if (verified.effective.sandbox !== sandboxType || verified.effective.approval !== native.runtimeContract.permissions.approval
        || verified.effective.provider !== native.runtimeContract.provider) throw new Error("Codex rehost would change native permissions or provider");
      const daemon = await deps.detectDaemonSupport(binding.cwd);
      if (daemon.kind !== "supported") throw new Error("Supervised Codex rehost requires proven --no-daemon support");
      const harness = structuredNativeExecutable("codex", [], deps.launchPath, binding.cwd);
      // Neither the pane nor an API caller supplies the successor identity.
      for (const [key, expected] of Object.entries({ OPENRIG_NODE_ID: binding.nodeId,
        OPENRIG_SESSION_NAME: binding.sessionName, OPENRIG_OCCUPANT_GENERATION: binding.generation, OPENRIG_RUNTIME: "codex" })) {
        if (await deps.tmux.getSessionEnv(binding.sessionName, key) !== expected) throw new Error("Codex rehost native launch environment mismatch");
      }
      return { posture, effective: verified.effective, evidenceDigest: createHash("sha256").update(JSON.stringify({
        configurationDigest: current.configurationDigest, profileSha256: verified.profileSha256,
        effective: verified.effective, posture, harness, daemon: daemon.kind,
      })).digest("hex") };
    },
    observeSupervisedReplacement: async binding => {
      if (!deps.store) return null;
      const current = await currentBinding(binding.nodeId);
      const latest = deps.store.latest(binding.nodeId, binding.generation);
      if (!current || !latest || current.resumeToken !== binding.nativeId || current.sessionName !== binding.sessionName
        || latest.intent.configurationDigest !== current.configurationDigest) return null;
      const panePid = await deps.tmux.getPanePid(current.pane);
      if (!panePid) return null;
      const rows = await listNativeProcesses(), descendants = new Set([panePid]);
      for (let i = 0; i < rows.length; i++) {
        let changed = false;
        for (const row of rows) if (descendants.has(row.ppid) && !descendants.has(row.pid)) { descendants.add(row.pid); changed = true; }
        if (!changed) break;
      }
      const identity = { OPENRIG_NODE_ID: binding.nodeId, OPENRIG_SESSION_NAME: binding.sessionName,
        OPENRIG_OCCUPANT_GENERATION: binding.generation, OPENRIG_RUNTIME: "codex" };
      const supervisors: number[] = [];
      for (const row of rows) if (descendants.has(row.pid) && row.executableName === "node"
        && await verifyNativeDutyProcessIdentity(row.pid, identity,
          [latest.intent.installedNode.path, latest.intent.installedSupervisor.path, "--supervise", latest.intent.configPath])) supervisors.push(row.pid);
      if (supervisors.length !== 1) return null;
      const proof = await observeNativeDutyLaunch(deps.store, { scope: latest.intent,
        launchId: latest.intent.launchId, supervisorPid: supervisors[0]! }, { currentBinding, tmux: deps.tmux });
      return proof ? { launchId: proof.launchId, fingerprint: proof.fingerprint } : null;
    },
  });
}
