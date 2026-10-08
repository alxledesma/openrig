import { createOperatorMaintenanceAuthority, parseCodexStoppedRecovery, type CodexStoppedRecovery } from "../domain/codex-rehost.js";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { SeatDispatchReservationService } from "../domain/seat-dispatch-reservation.js";
import { getConnInfo } from "@hono/node-server/conninfo";
import { isRotationLoopback } from "../domain/rotation-precondition.js";
import { rotationFactsResolver } from "../domain/rotation-facts-resolver.js";
import { OutboxHandler } from "../domain/outbox-handler.js";
import { Hono } from "hono";
import type { RigRepository } from "../domain/rig-repository.js";
import type { SessionRegistry } from "../domain/session-registry.js";
import type { DiscoveryRepository } from "../domain/discovery-repository.js";
import type { EventBus } from "../domain/event-bus.js";
import type { TmuxAdapter } from "../adapters/tmux.js";
import { observeClaudePaneStartedAt } from "../domain/native-process-lineage.js";
import { SeatStatusService } from "../domain/seat-status-service.js";
import { SeatRuntimeMigration, RuntimeMigrationRefusal } from "../domain/seat-runtime-migration.js";
import { SeatHandoverService } from "../domain/seat-handover-service.js";
import { SeatSwitchClientService } from "../domain/seat-switch-client-service.js";
import { SeatLifecycleService, type SeatRefusal } from "../domain/seat-lifecycle-service.js";
import { makePredecessorRecapResolver } from "../domain/predecessor-recap-resolver.js";
import type { ContextUsageStore } from "../domain/context-usage-store.js";
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { resolveAuthoredRecapPointer } from "../domain/context-packs/seat-recap-store.js";
import { buildRebuildPrimingChain } from "../domain/rebuild-priming-chain.js";
import { OPENRIG_HOME } from "../openrig-compat.js";
import { SettingsStore } from "../domain/user-settings/settings-store.js";
import { transportSenderSession } from "./require-sender-identity.js";
import { PiResumeAdapter } from "../adapters/pi-resume.js";
import { piSeatPaths, parsePiRunnerState } from "../adapters/pi-runner-protocol.js";
import { makeLegacyPiNativeWitness, makeNativeLegacyPiTransportSource } from "../domain/legacy-pi-native-witness.js";
import { resolvePiInstallationModule } from "../domain/pi-installation-module-resolver.js";
import { observeLegacyPiEnvironment } from "../domain/legacy-pi-native-provenance.js";
/** Bounded positional tail window for the idle witness; matches the native 64 KiB evidence. */
const PI_SESSION_TAIL_WINDOW_BYTES = 65536;

import { makePiNativeProver } from "../domain/coordinator-runtime-availability.js";
import { execCommand } from "../adapters/tmux-exec.js";
import { listNativeProcesses } from "../domain/native-process-lineage.js";
import { resolve as resolvePath } from "node:path";
import { fileURLToPath } from "node:url";

export const seatRoutes = new Hono();

// S09 is an independent delivery preference, never a lifecycle or permission change.
seatRoutes.post("/set-typing-guard/:seatRef", async c => {
  const guard = (c.get("tmuxAdapter" as never) as TmuxAdapter).deliveryGuard;
  if (!guard) return c.json({ error: "Delivery guard unavailable" }, 503);
  const body = await c.req.json<Record<string, unknown>>();
  if (typeof body.enabled !== "boolean" || typeof body.reason !== "string" || !body.reason.trim()) {
    return c.json({ error: "enabled boolean and reason required" }, 400);
  }
  const actor = transportSenderSession(c);
  if (!actor) return c.json({ error: "Sender identity required for preference audit" }, 400);
  try {
    const target = guard.target(decodeURIComponent(c.req.param("seatRef")));
    const preference = await guard.set(target.nodeId, body.enabled, actor, body.reason);
    return c.json({ ...preference, tradeoff: "Automatic terminal input is paused while enabled, even at an empty prompt. Disabling does not replay retained messages." }, preference.pending ? 202 : 200);
  } catch (error) { return c.json({ error: (error as Error).message }, 409); }
});

seatRoutes.get("/held-messages/:seatRef", c => {
  const guard = (c.get("tmuxAdapter" as never) as TmuxAdapter).deliveryGuard;
  if (!guard) return c.json({ error: "Delivery guard unavailable" }, 503);
  try {
    const target = guard.target(decodeURIComponent(c.req.param("seatRef")));
    const outbox = new OutboxHandler(guard.db);
    const id = c.req.query("id");
    if (id) {
      const entry = outbox.getById(id);
      if (entry?.guardBinding?.nodeId !== target.nodeId) return c.json({ error: "No retained history for this node and ID" }, 404);
      return c.json({ entry });
    }
    return c.json(outbox.heldForNode(target.nodeId, Number(c.req.query("limit") ?? 100), Number(c.req.query("offset") ?? 0)));
  } catch (error) { return c.json({ error: (error as Error).message }, 400); }
});

seatRoutes.post("/retire-held-message/:seatRef/:id", async c => {
  const guard = (c.get("tmuxAdapter" as never) as TmuxAdapter).deliveryGuard;
  if (!guard) return c.json({ error: "Delivery guard unavailable" }, 503);
  const body = await c.req.json<Record<string, unknown>>();
  const actor = transportSenderSession(c);
  if (!actor || typeof body.reason !== "string" || !body.reason.trim()) return c.json({ error: "Sender identity and reason required" }, 400);
  try {
    const target = guard.target(decodeURIComponent(c.req.param("seatRef")));
    const outbox = new OutboxHandler(guard.db); const id = c.req.param("id");
    if (outbox.getById(id)?.guardBinding?.nodeId !== target.nodeId) return c.json({ error: "No held message for this node and ID" }, 404);
    return c.json({ entry: outbox.retire(id, actor, body.reason), effect: "Retired from active quota; evidence preserved. No delivery, native consumption or work closure is asserted." });
  } catch (error) { return c.json({ error: (error as Error).message }, 409); }
});

seatRoutes.get("/status/:seatRef", (c) => {
  const rigRepo = c.get("rigRepo" as never) as RigRepository;
  const service = new SeatStatusService({ rigRepo });
  const result = service.getStatus(decodeURIComponent(c.req.param("seatRef")!));

  if (result.ok) {
    const guard = (c.get("tmuxAdapter" as never) as TmuxAdapter | undefined)?.deliveryGuard;
    const target = guard?.maybeTarget(decodeURIComponent(c.req.param("seatRef")!));
    return c.json({ ...result.status, ...(guard && target ? { typingGuard: {
      ...guard.preference(target.nodeId), heldCount: new OutboxHandler(guard.db).heldForNode(target.nodeId, 1).total,
    } } : {}) });
  }

  if (result.code === "seat_ambiguous") {
    return c.json(result, 409);
  }
  if (result.code === "seat_ref_required") {
    return c.json(result, 400);
  }
  return c.json(result, 404);
});

seatRoutes.get("/runtime-migration/:operationId", async c => {
  const token = c.get("terminalBearerToken" as never) as string | null;
  if (!token) return c.json({ok:false,code:"runtime_migration_authenticated_control_required"},503);
  const authResponse = await authBearerTokenMiddleware({expectedToken:token})(c,async()=>{});
  if (authResponse) return authResponse;
  let address:string|undefined; try { address=getConnInfo(c).remote.address; } catch {}
  if (c.req.header("Origin") || !isRotationLoopback(address)) return c.json({ok:false,code:"runtime_migration_local_only"},403);
  const rigRepo = c.get("rigRepo" as never) as RigRepository;
  try {
    const migration = new SeatRuntimeMigration({db:rigRepo.db,rigRepo,
      sessionRegistry:c.get("sessionRegistry" as never) as SessionRegistry,eventBus:c.get("eventBus" as never) as EventBus,
      tmuxAdapter:c.get("tmuxAdapter" as never) as TmuxAdapter});
    return c.json(migration.inspect(c.req.param("operationId"),transportSenderSession(c)??"",c.req.header("X-OpenRig-Occupant-Generation")??""));
  } catch(error) { return c.json({ok:false,code:error instanceof RuntimeMigrationRefusal?error.code:"runtime_migration_status_unavailable"},409); }
});

seatRoutes.post("/handover/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  if(body["rotationExpected"] || body["runtimeMigration"] !== undefined){
    const token = c.get("terminalBearerToken" as never) as string | null;
    if (!token) return c.json({ok:false,code:"rotation_authenticated_control_required"},503);
    const authResponse = await authBearerTokenMiddleware({expectedToken:token})(c, async()=>{});
    if (authResponse) return authResponse;
    let address:string|undefined;
    try{address=getConnInfo(c).remote.address;}catch{}
    if(c.req.header("Origin") || !isRotationLoopback(address))return c.json({ok:false,code:"rotation_local_only",message:"Automatic rotation requires local non-browser transport"},403);
  }
  const rigRepo = c.get("rigRepo" as never) as RigRepository;
  const rotationRoot=process.env["OPENRIG_ROTATION_ROOT"];
  const rotationPrecondition=rotationRoot ? rotationFactsResolver({db:rigRepo.db,root:rotationRoot,
    whoami:c.get("whoamiService" as never) as import("../domain/whoami-service.js").WhoamiService,
    activity:c.get("seatActivityService" as never) as import("../domain/seat-activity-service.js").SeatActivityService,
    tmux:c.get("tmuxAdapter" as never) as TmuxAdapter}) : undefined;
  const service = new SeatHandoverService({
    migrationPiSkillRoot: session => join(piSeatPaths(join(OPENRIG_HOME,"state","pi"),session).agentDir,"skills"),
    migrationPiProve: makePiNativeProver(rigRepo.db, execCommand, { fs: { readFile: (p: string) => readFileSync(p, "utf-8") }, piStateRoot: join(OPENRIG_HOME, "state", "pi") }),
    rotationPrecondition,
    dispatchReservations: rotationPrecondition && (c.get("tmuxAdapter" as never) as TmuxAdapter).deliveryGuard ? new SeatDispatchReservationService({db:rigRepo.db,guard:(c.get("tmuxAdapter" as never) as TmuxAdapter).deliveryGuard!,verifyPredecessor:rotationPrecondition,observeSuccessor:async()=>{throw new Error("Successor observation uses authenticated reservation route");}}) : undefined,
    db: rigRepo.db,
    rigRepo,
    sessionRegistry: c.get("sessionRegistry" as never) as SessionRegistry,
    discoveryRepo: c.get("discoveryRepo" as never) as DiscoveryRepository,
    eventBus: c.get("eventBus" as never) as EventBus,
    tmuxAdapter: c.get("tmuxAdapter" as never) as TmuxAdapter,
    sessionEnv: (c.get("sessionEnv" as never) as Record<string, string | undefined> | undefined) ?? undefined,
    runtimeSessionEnv: (c.get("runtimeSessionEnv" as never) as Record<string, Record<string, string | undefined>> | undefined) ?? undefined,
    // B1 — launch a fresh successor into a live agent via the runtime adapters.
    runtimeAdapters: (c.get("runtimeAdapters" as never) as Record<string, import("../domain/runtime-adapter.js").RuntimeAdapter> | undefined) ?? undefined,
    // OPR.0.4.6.02 S1 — the shared tmux option-defaults applier, so a FRESH
    // handover successor gets the same mouse/status/clipboard defaults as a
    // launched seat (orch C1 scope ruling).
    tmuxOptionDefaults: (c.get("tmuxOptionDefaults" as never) as import("../domain/tmux-option-defaults.js").TmuxOptionDefaultsApplier | undefined) ?? undefined,
    // B2 — discovered-mode resume-token capture derive-helper deps.
    contextUsageStore: (c.get("contextUsageStore" as never) as import("../domain/resume-token-capture.js").ResumeTokenCaptureDeps["contextUsageStore"]) ?? undefined,
    resumeTokenCapturer: (c.get("resumeMetadataRefresher" as never) as import("../domain/resume-token-capture.js").ResumeTokenCaptureDeps["resumeTokenCapturer"]) ?? undefined,
    // #421 — the pane's current Claude process start time, so capture skips an older sidecar.
    claudeProcessStartedAt: (sessionName: string) => observeClaudePaneStartedAt({ target: sessionName, tmux: c.get("tmuxAdapter" as never) as TmuxAdapter }),
    // Wire the predecessor-recap resolver so the successor boot packet fires
    // with a bounded from-record recap. Reuses the full ContextUsageStore from context (readAndNormalize
    // = claude transcript_path; readCodexAndNormalize = codex rollout_path) + a resume-token lookup for
    // the codex thread id; parseJsonlExchanges is the resolver's default. Absent store → resolver omitted
    // (recap sections omitted honestly). Firing proven live in the money-proof e2e.
    predecessorRecapResolver: (() => {
      const store = c.get("contextUsageStore" as never) as ContextUsageStore | undefined;
      if (!store) return undefined;
      const db = rigRepo.db;
      return makePredecessorRecapResolver({
        // B16 — session_id rides the read so the resolver can verify the name-keyed sidecar is the
        // PREDECESSOR's (canonical-name reuse means a booted successor overwrites it).
        readClaudeRecord: (sessionName) => {
          const usage = store.readAndNormalize(sessionName);
          return { transcriptPath: usage.transcriptPath, sessionId: usage.sessionId ?? null };
        },
        readCodexTranscriptPath: (args) => store.readCodexAndNormalize(args).transcriptPath,
        lookupResumeToken: (nodeId, sessionName) => {
          const row = db
            .prepare("SELECT resume_token FROM sessions WHERE node_id = ? AND session_name = ? ORDER BY id DESC LIMIT 1")
            .get(nodeId, sessionName) as { resume_token: string | null } | undefined;
          return row?.resume_token ?? null;
        },
      });
    })(),
    // OPR.0.5.3.5 mini-req 7 — the AUTHORED recap pointer for the successor
    // packet: seat dir from topology.root CONFIG (slice-06 D1 layout); every
    // outcome labeled — present with chain depth, or a named absence carrying
    // the path tried (a seat-id/directory mapping gap surfaces loudly at the
    // door drive instead of silently).
    authoredRecapResolver: (seatRef: string) => {
      const topologyRoot = String(new SettingsStore().resolveOne("topology.root").value);
      return resolveAuthoredRecapPointer(seatRef, topologyRoot);
    },
    // OPR.0.5.5.5 (fix B3) — the ONE production rebuild priming chain builder
    // (rebuild-priming-chain.ts): RECAP.md, LEARNED.md, latest restore packet
    // when the seat's restore-pending marker names one, superseded recaps
    // newest-first. Declares only; the service existence-filters (named gaps).
    rebuildPrimingResolver: (seatRef: string) => buildRebuildPrimingChain(seatRef, {
      topologyRoot: String(new SettingsStore().resolveOne("topology.root").value),
      openrigHome: OPENRIG_HOME,
    }),
    // OPR.0.4.6.PI1 FR-6 — the Pi adapter in the runtime-adapter map exposes
    // the pi-runner sidecar reader; reuse it structurally (no new context var).
    piRunnerStateStore: (() => {
      const adapters = c.get("runtimeAdapters" as never) as Record<string, unknown> | undefined;
      const pi = adapters?.["pi"] as { readSessionFile?: (sessionName: string) => { ok: true; sessionFile: string } | { ok: false; reason: string } } | undefined;
      return typeof pi?.readSessionFile === "function"
        ? { readSessionFile: pi.readSessionFile.bind(pi) as (sessionName: string) => { ok: true; sessionFile: string } | { ok: false; reason: string } }
        : undefined;
    })(),
    ompRunnerStateStore: (() => {
      const adapters = c.get("runtimeAdapters" as never) as Record<string, unknown> | undefined;
      const omp = adapters?.["omp"] as { readSessionFile?: (sessionName: string) => { ok: true; sessionFile: string } | { ok: false; reason: string } } | undefined;
      return typeof omp?.readSessionFile === "function"
        ? { readSessionFile: omp.readSessionFile.bind(omp) }
        : undefined;
    })(),
    // GHOST-STAGE (e/Class-B) — the canonical OccupantInvalidator so commit()'s re-key call fires
    // (invalidate the retiring occupant's seat-name-keyed stores before the successor accumulates any).
    occupantInvalidator: (c.get("occupantInvalidator" as never) as import("../domain/occupant-invalidator.js").OccupantInvalidator | undefined) ?? undefined,
    // WAVE-O B1 (R2 508e383d) — the daemon's ONE SeatActivityService rides every
    // production handover construction, so a real committed swap reaches
    // declareOccupantSwap and the successor never inherits the retiree's evidence or
    // promoted rung authority. Optional in the deps contract; ALWAYS wired here.
    activityOracle: (c.get("seatActivityService" as never) as import("../domain/seat-activity-service.js").SeatActivityService | undefined) ?? undefined,
  });
  const result = await service.handover({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    reason: typeof body["reason"] === "string" ? body["reason"] : null,
    source: typeof body["source"] === "string" ? body["source"] : null,
    operator: typeof body["operator"] === "string" ? body["operator"] : null,
    dryRun: body["dryRun"] === true,
    rotationActor: transportSenderSession(c) ?? undefined,
    rotationActorGeneration: c.req.header("X-OpenRig-Occupant-Generation"),
    runtimeMigration: body["runtimeMigration"],
    rotationExpected: body["rotationExpected"] && typeof body["rotationExpected"] === "object" ? body["rotationExpected"] as Record<string, unknown> : undefined,
  });

  if (result.ok) {
    return c.json("plan" in result ? result.plan : result.result);
  }

  if (result.code === "runtime_migration_refused") return c.json(result, 409);
  if (result.code === "runtime_migration_unknown") return c.json(result, 503);
  if (result.code === "missing_reason" || result.code === "invalid_source") {
    return c.json(result, 400);
  }
  if (result.code === "seat_ambiguous") {
    return c.json(result, 409);
  }
  if (result.code === "successor_creation_not_implemented" ||
    result.code === "source_not_supported") {
    return c.json(result, 501);
  }
  if (result.code === "tmux_probe_failed") {
    return c.json(result, 502);
  }
  if (result.code === "current_occupant_required" ||
    result.code === "discovered_not_active" ||
    result.code === "successor_tmux_absent" ||
    result.code === "successor_already_managed" ||
    result.code === "successor_is_current" ||
    result.code === "runtime_mismatch") {
    return c.json(result, 409);
  }
  if (result.code === "seat_ref_required") {
    return c.json(result, 400);
  }
  if (result.code === "handover_commit_failed" ||
    result.code === "successor_create_failed" ||
    result.code === "context_delivery_failed") {
    return c.json(result, 500);
  }
  return c.json(result, 404);
});

/** The exact seat identity the intent audit attributes a signal to. Taken from the plan's
 *  already-verified seat binding; never empty and never caller-supplied. */
export interface LegacySignalIntentSeatIdentity { rigId: string; nodeId: string; logicalId: string }

/** PURE construction of the durable before-signal intent event.
 *
 *  G1: the role is derived from the BOUND runner pid, never from ppid. A runner's ppid is its
 *  pane shell and is never 0, so the previous ppid test labelled BOTH targets "child" and wrote a
 *  false fact into the audit. Everything here is a direct read of already-verified inputs: no
 *  clock read, no I/O, no environment. `at` is passed in so the caller owns the timestamp.
 *
 *  F3: the record must distinguish two targets that start in the same second, so it names the
 *  pid, its role, the shared start and the endpoint it is about to be told about.
 */
export function buildLegacySignalIntentEvent(input: {
  seat: LegacySignalIntentSeatIdentity;
  binding: { runner: { pid: number } };
  target: { pid: number; startedAt: string };
  endpoint: { host: "127.0.0.1"; port: number };
  at: string;
}) {
  return {
    type: "seat.runner_rehost_legacy_signal_intent" as const,
    rigId: input.seat.rigId, nodeId: input.seat.nodeId, logicalId: input.seat.logicalId,
    reason: "legacy_pi_native_witness", operator: null, at: input.at,
    signalDelivered: false as const, targetPid: input.target.pid,
    targetRole: input.target.pid === input.binding.runner.pid ? "runner" as const : "child" as const,
    targetStartedAt: input.target.startedAt, endpointPort: input.endpoint.port,
  };
}

// S5 (OPR.0.5.4.7) — the seat-lifecycle verb surface: set-model / stop / clean.
// One service, one resolution path, one status mapping shared by all three verbs.
export function seatLifecycleService(c: { get(key: never): unknown }): SeatLifecycleService {
  const rigRepo = c.get("rigRepo" as never) as RigRepository;
  return new SeatLifecycleService({
    db: rigRepo.db,
    codexRehost: c.get("codexRehost" as never) as import("../domain/codex-rehost.js").CodexSameGenerationRehost | undefined,
    rigRepo,
    sessionRegistry: c.get("sessionRegistry" as never) as SessionRegistry,
    eventBus: c.get("eventBus" as never) as EventBus,
    tmuxAdapter: c.get("tmuxAdapter" as never) as TmuxAdapter,
    nodeLauncher: c.get("nodeLauncher" as never) as import("../domain/node-launcher.js").NodeLauncher,
    startupOrchestrator: (c.get("startupOrchestrator" as never) as import("../domain/startup-orchestrator.js").StartupOrchestrator | undefined) ?? undefined,
    runtimeAdapters: (c.get("runtimeAdapters" as never) as Record<string, import("../domain/runtime-adapter.js").RuntimeAdapter> | undefined) ?? undefined,
    occupantInvalidator: (c.get("occupantInvalidator" as never) as import("../domain/occupant-invalidator.js").OccupantInvalidator | undefined) ?? undefined,
    activityOracle: (c.get("seatActivityService" as never) as import("../domain/seat-activity-service.js").SeatActivityService | undefined) ?? undefined,
  });
}

/** Keys a caller may NEVER author for the legacy native-witness option. The option
 *  carries a strict boolean and NOTHING else: the daemon builds the witness itself,
 *  from its own census and module bindings. A request carrying any of these is
 *  refused outright rather than stripped, so an authored proof can never be silent. */
export const LEGACY_WITNESS_FORBIDDEN_REQUEST_KEYS = [
  "witness", "proof", "nativeWitness", "nativeLeaf", "leaf", "leafId",
  "lastEntryId", "cursor", "modules", "moduleUrl", "runnerModuleUrl", "piModuleUrl",
  "modulePath", "scriptPath", "endpoint", "inspectorPort", "port", "pid",
  "runnerPid", "childPid", "ppid", "sessionFile", "launchId", "generation",
  "path", "sessionPath", "snapshotPath", "recoverySnapshot", "stoppedTargetLeaf", "leafSource",
  "startedAt", "runnerStartedAt", "childStartedAt", "timestamp", "timing", "timeout", "waitMs",
] as const;

/** Parse the EXPLICIT legacy native-witness option. Absent means the ordinary rehost;
 *  present it must be a real boolean; nothing else about the bridge is settable. */
export function parseLegacyNativeWitnessRequest(body: Record<string, unknown>): { ok: true; legacyNativeWitness: boolean } | { ok: false; error: string } {
  for (const key of LEGACY_WITNESS_FORBIDDEN_REQUEST_KEYS) {
    if (key in body) return { ok: false, error: `${key} is not accepted: process identity, witness and recovery paths are built by the daemon, never supplied by a caller` };
  }
  const raw = body["legacyNativeWitness"];
  if (raw === undefined) return { ok: true, legacyNativeWitness: false };
  if (typeof raw !== "boolean") return { ok: false, error: "legacyNativeWitness must be a boolean when present" };
  return { ok: true, legacyNativeWitness: raw };
}

export function parseStoppedTargetRecoveryRequest(body: Record<string, unknown>): { ok: true; legacyNativeWitness: boolean; stoppedTargetRecovery: boolean; codexDetachedResume?: true; stoppedTargetAcceptanceReference?: string } | { ok: false; error: string } {
  const legacy = parseLegacyNativeWitnessRequest(body);
  if (!legacy.ok) return legacy;
  const rawMode = body["stoppedTargetRecovery"];
  if (rawMode !== undefined && typeof rawMode !== "boolean") return { ok: false, error: "stoppedTargetRecovery must be a boolean when present" };
  const stoppedTargetRecovery = rawMode === true;
  const rawDetached = body["codexDetachedResume"];
  if (rawDetached !== undefined && typeof rawDetached !== "boolean") return { ok: false, error: "codexDetachedResume must be a boolean when present" };
  const codexDetachedResume = rawDetached === true;
  const rawAcceptance = body["stoppedTargetAcceptanceReference"];
  if (rawAcceptance !== undefined && typeof rawAcceptance !== "string") return { ok: false, error: "stoppedTargetAcceptanceReference must be a string when present" };
  const acceptance = typeof rawAcceptance === "string" ? rawAcceptance.trim() : "";
  if (legacy.legacyNativeWitness && stoppedTargetRecovery) return { ok: false, error: "legacyNativeWitness and stoppedTargetRecovery are mutually exclusive" };
  if (codexDetachedResume && (legacy.legacyNativeWitness || stoppedTargetRecovery || body["stoppedTargetAcceptanceReference"] !== undefined || body["codexStoppedRecovery"] !== undefined || body["legacyCodexProfile"] !== undefined)) return { ok: false, error: "codexDetachedResume is exclusive with Codex stopped recovery, legacy Codex profile/witness, and Pi recovery modes" };
  if (stoppedTargetRecovery && !acceptance) return { ok: false, error: "stoppedTargetAcceptanceReference is required and must acknowledge possible loss of an unpersisted in-flight turn" };
  if (!stoppedTargetRecovery && rawAcceptance !== undefined) return { ok: false, error: "stoppedTargetAcceptanceReference requires stoppedTargetRecovery" };
  return { ok: true, legacyNativeWitness: legacy.legacyNativeWitness, stoppedTargetRecovery, ...(codexDetachedResume ? { codexDetachedResume: true as const } : {}), ...(acceptance ? { stoppedTargetAcceptanceReference: acceptance } : {}) };
}

// Same-generation Pi runner rehost. The shipped resume primitive, the shipped pi
// native prover and the shipped runner-state parser are constructed here
// unchanged; startup does not expose them on the request context. Paths derive
// from the same OPENRIG_HOME the launch path uses, so the session file identity
// is the RECORDED one and never a reconstructed guess.
// Independent local maintenance can repair the Operator without impersonating
// a running agent. This route has no caller-selected target or actor.
seatRoutes.post("/operator-maintenance/rehost-runner",async c=>{
  const token=c.get("terminalBearerToken" as never) as string|null;
  if(!token)return c.json({ok:false,code:"operator_maintenance_authenticated_control_required"},503);
  const auth=await authBearerTokenMiddleware({expectedToken:token})(c,async()=>{});if(auth)return auth;
  let address:string|undefined;try{address=getConnInfo(c).remote.address;}catch{}
  if(c.req.raw.headers.has("Origin")||!isRotationLoopback(address))return c.json({ok:false,code:"operator_maintenance_local_only"},403);
  if(["X-OpenRig-Session","X-OpenRig-Occupant-Generation","X-OpenRig-Origin-Unknown"].some(k=>c.req.raw.headers.has(k)))
    return c.json({ok:false,code:"operator_maintenance_agent_identity_not_accepted"},400);
  const body=await c.req.json<Record<string,unknown>>().catch(()=>null);
  if(!body||typeof body!=="object"||Array.isArray(body)||Object.keys(body).some(k=>!["reason","expected","codexStoppedRecovery","codexDetachedResume","legacyCodexProfile","enableGuard"].includes(k))
    ||typeof body.reason!=="string"||!body.reason.trim()||!body.expected||typeof body.expected!=="object"||Array.isArray(body.expected)
    ||Object.keys(body.expected).sort().join(',')!=="generation,nodeId")return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);
  const expected=body.expected as {nodeId:unknown;generation:unknown};
  if(typeof expected.nodeId!=="string"||!expected.nodeId||typeof expected.generation!=="string"||!expected.generation)return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);
  let recovery:CodexStoppedRecovery|undefined;
  try{if(body.codexStoppedRecovery!==undefined)recovery=parseCodexStoppedRecovery(body.codexStoppedRecovery);}catch{return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);}
  if(body.codexDetachedResume!==undefined&&typeof body.codexDetachedResume!=="boolean")return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);
  const detachedResume=body.codexDetachedResume===true;
  const legacyCodexProfile=body.legacyCodexProfile;
  if(legacyCodexProfile!==undefined&&(typeof legacyCodexProfile!=="string"||!/^[a-zA-Z0-9_-]+$/.test(legacyCodexProfile)||recovery||detachedResume))
    return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);
  if(detachedResume&&recovery)return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);
  if(body.enableGuard!==undefined&&body.enableGuard!==true)return c.json({ok:false,code:"operator_maintenance_request_invalid"},400);
  const guard=(c.get("tmuxAdapter" as never) as TmuxAdapter)?.deliveryGuard;
  if(!guard)return c.json({ok:false,code:"operator_maintenance_unavailable"},503);
  try{
    const target=guard.target("operator-agent@kernel");
    if(target.session!=="operator-agent@kernel"||target.nodeId!==expected.nodeId||target.occupant!==expected.generation)
      return c.json({ok:false,code:"operator_maintenance_binding_changed"},409);
    const maintenanceAuthority=createOperatorMaintenanceAuthority({nodeId:target.nodeId,generation:target.occupant,recovery,...(detachedResume?{detachedResume:true as const}:{}),
      ...(typeof legacyCodexProfile==="string"?{legacyCodexProfile}:{}),...(body.enableGuard===true?{enableGuard:true as const}:{})});
    const result=await seatLifecycleService(c).rehostRunner({seatRef:"operator-agent@kernel",reason:body.reason,maintenanceAuthority,
      ...(recovery?{codexStoppedRecovery:recovery}:{}),...(detachedResume?{codexDetachedResume:true,actorGeneration:target.occupant}:{})});
    return c.json(result,result.ok?200:seatLifecycleStatus(result.code));
  }catch{return c.json({ok:false,code:"operator_maintenance_binding_unproven"},409);}
});

seatRoutes.post("/rehost-runner/:seatRef", async c => {
  const body = await c.req.json<Record<string, unknown>>();
  if (typeof body.reason !== "string" || !body.reason.trim()) return c.json({ error: "reason required" }, 400);
  if (body.operator !== undefined && typeof body.operator !== "string") return c.json({ error: "operator must be a string when present" }, 400);
  const rehostRequest = parseStoppedTargetRecoveryRequest(body);
  if (!rehostRequest.ok) return c.json({ error: rehostRequest.error }, 400);
  let codexStoppedRecovery: CodexStoppedRecovery | undefined;
  if (body.codexStoppedRecovery !== undefined || rehostRequest.codexDetachedResume) {
    const token = c.get("terminalBearerToken" as never) as string | null;
    if (!token) return c.json({ok:false,code:"codex_rehost_recovery_authenticated_control_required"},503);
    const authResponse = await authBearerTokenMiddleware({expectedToken:token})(c,async()=>{});
    if (authResponse) return authResponse;
    let address:string|undefined;try { address=getConnInfo(c).remote.address; } catch {}
    if(c.req.header("Origin")||!isRotationLoopback(address))return c.json({ok:false,code:"codex_rehost_recovery_local_only"},403);
    if (body.codexStoppedRecovery !== undefined) {
      try { codexStoppedRecovery = parseCodexStoppedRecovery(body.codexStoppedRecovery); } catch (error) { return c.json({error:(error as Error).message},400); }
    }
    if (rehostRequest.legacyNativeWitness || rehostRequest.stoppedTargetRecovery || rehostRequest.stoppedTargetAcceptanceReference) return c.json({error:"Codex and Pi recovery modes are exclusive"},400);
    const actorGeneration=c.req.header("X-OpenRig-Occupant-Generation");
    const guard=(c.get("tmuxAdapter" as never) as TmuxAdapter | undefined)?.deliveryGuard;
    let operatorTarget:import("../domain/seat-delivery-guard.js").GuardTarget|undefined;try{operatorTarget=guard?.target("operator-agent@kernel");}catch{}
    if (transportSenderSession(c) !== "operator-agent@kernel" || !actorGeneration || actorGeneration!==operatorTarget?.occupant || c.req.header("X-OpenRig-Origin-Unknown") === "true") return c.json({error:"Current Operator transport identity and generation required"},403);
    if(rehostRequest.codexDetachedResume){
      let target:import("../domain/seat-delivery-guard.js").GuardTarget|undefined;try{target=guard?.target(decodeURIComponent(c.req.param("seatRef")));}catch{}
      if(!target||target.nodeId===operatorTarget?.nodeId)return c.json({error:"Detached resume requires a peer target; Operator self-target is forbidden"},403);
    }
  }
  const rigRepo = c.get("rigRepo" as never) as RigRepository;
  const tmuxAdapter = c.get("tmuxAdapter" as never) as TmuxAdapter;
  const stateRoot = join(OPENRIG_HOME, "state", "pi");
  const runnerEntryPath = resolvePath(import.meta.dirname, "../adapters/pi-runner.js");
  const fsOps = {
    readFile: (p: string) => readFileSync(p, "utf-8"),
    writeFile: (p: string, content: string) => writeFileSync(p, content, "utf-8"),
    exists: (p: string) => existsSync(p),
    mkdirp: (p: string) => mkdirSync(p, { recursive: true }),
  };
  // F1 collector: collects only reduced reason codes from this request's proof
  // observations. It is scoped to the request closure, holds no argv, env, path
  // or pid text, and is read once when the pre-effect identity refusal is built.
  const proofReasons: import("../domain/coordinator-runtime-availability.js").PiProofReason[] = [];
  /** Bounded 64 KiB POSITIONAL tail entry id, shared by the ordinary idle gate and
   *  the legacy witness so both credit the very same read of the very same file. */
  const boundedTailEntryId = (p: string): string | null => {
    try {
      const size = statSync(p).size;
      const window = Math.min(size, PI_SESSION_TAIL_WINDOW_BYTES);
      const start = size - window;
      const buffer = Buffer.alloc(window);
      const fd = openSync(p, "r");
      try { readSync(fd, buffer, 0, window, start); } finally { closeSync(fd); }
      const text = buffer.toString("utf-8");
      const lines = text.split("\n").filter(line => line.trim().length > 0);
      const tail = lines[lines.length - 1];
      if (!tail) return null;
      // A single entry larger than the window means we did not see its start.
      if (start > 0 && lines.length === 1) return null;
      // Refuse a truncated final entry: the tail must end at a record boundary.
      if (start > 0 && !text.endsWith("\n")) return null;
      const parsed = JSON.parse(tail) as { id?: unknown };
      return typeof parsed.id === "string" && /^[0-9a-f]{8}$/i.test(parsed.id) ? parsed.id : null;
    } catch { return null; }
  };
  const lifecycle = new SeatLifecycleService({
    db: rigRepo.db,
    rigRepo,
    codexRehost: c.get("codexRehost" as never) as import("../domain/codex-rehost.js").CodexSameGenerationRehost | undefined,
    sessionRegistry: c.get("sessionRegistry" as never) as SessionRegistry,
    eventBus: c.get("eventBus" as never) as EventBus,
    tmuxAdapter,
    nodeLauncher: c.get("nodeLauncher" as never) as import("../domain/node-launcher.js").NodeLauncher,
    startupOrchestrator: (c.get("startupOrchestrator" as never) as import("../domain/startup-orchestrator.js").StartupOrchestrator | undefined) ?? undefined,
    runtimeAdapters: (c.get("runtimeAdapters" as never) as Record<string, import("../domain/runtime-adapter.js").RuntimeAdapter> | undefined) ?? undefined,
    occupantInvalidator: (c.get("occupantInvalidator" as never) as import("../domain/occupant-invalidator.js").OccupantInvalidator | undefined) ?? undefined,
    activityOracle: (c.get("seatActivityService" as never) as import("../domain/seat-activity-service.js").SeatActivityService | undefined) ?? undefined,
    listProcesses: () => listNativeProcesses(),
    piResume: new PiResumeAdapter(tmuxAdapter, fsOps, { stateRoot, runnerEntryPath }, {
      seatLaunchEnvironment: c.get("seatLaunchEnvironment" as never) as import("../domain/seat-launch-environment.js").SeatLaunchEnvironment | undefined,
    }),
    piProve: makePiNativeProver(rigRepo.db, execCommand, { fs: { readFile: (p: string) => readFileSync(p, "utf-8") }, piStateRoot: stateRoot, diagnose: reasons => { proofReasons.push(...reasons); } }),
    piRunnerState: (sessionName: string) => {
      const p = piSeatPaths(stateRoot, sessionName).runnerStatePath;
      return existsSync(p) ? parsePiRunnerState(readFileSync(p, "utf-8")) : null;
    },
    piSessionFileExists: (p: string) => existsSync(p),
    piRecoverySnapshotDirectory: join(stateRoot, "rehost-recovery-snapshots"),
    // Bounded 64 KiB POSITIONAL tail: read only the last window, never the whole file.
    // An entry larger than the window, or a final line without its terminating newline
    // (a partial/in-progress append), returns null so rehost refuses instead of
    // parsing an optimistic value. No transcript content is ever returned.
    // R3-B3: the identity digest is BOUNDED too. The whole session file may be very
    // large, so only a bounded positional window is hashed and the receipt states which.
    piSessionFileDigestPrefix: (p: string) => {
      try {
        const size = statSync(p).size;
        const window = Math.min(size, PI_SESSION_TAIL_WINDOW_BYTES);
        const buffer = Buffer.alloc(window);
        const fd = openSync(p, "r");
        try { readSync(fd, buffer, 0, window, size - window); } finally { closeSync(fd); }
        return `w${window}:${createHash("sha256").update(buffer).digest("hex").slice(0, 16)}`;
      } catch { return null; }
    },
    piSessionTailEntryId: boundedTailEntryId,
    // EXPLICIT legacy native-witness option only. The seam constructs the REAL
    // collector per call, from the module binding the service derived from the exact
    // native process commands, and passes the SAME bounded tail reader this route
    // already trusts for the ordinary idle gate. Nothing is constructed, signalled or
    // opened unless the explicit option asks for it.
    legacyPiWitness: {
      witness: request => makeLegacyPiNativeWitness({
        source: {
          ...makeNativeLegacyPiTransportSource(request.modules),
          // C2 durable BEFORE-signal intent audit. Written before any delivery; if it throws the
          // collector delivers ZERO signals and refuses, so a delivery is never unrecorded.
          recordSignalIntent: async (target, endpoint) => {
            // The seat identity is not in scope at this seam; the intent record is keyed by the
            // target's own bound start identity, which is what the audit must attest anyway.
            // The event body is built by an extracted PURE helper so the audit facts can be
            // asserted directly, without standing up a router, a database or a witness run.
            rigRepo.db.transaction(() => (c.get("eventBus" as never) as EventBus).persistWithinTransaction(
              buildLegacySignalIntentEvent({ seat: request.seat, binding: request.binding, target, endpoint, at: new Date().toISOString() }),
            ))();
          },
        },
        modules: request.modules,
        signal: "SIGUSR1",
        tailEntryId: boundedTailEntryId,
      }).witness(request.binding),
    },
    // The CURRENT fallback-capable runner this route's resume launches. The legacy
    // path refuses without it: it must be able to prove the replacement is this one.
    currentPiRunnerEntryPath: runnerEntryPath,
    // The Pi cached-chunk module comes from TRUSTED INSTALLATION PROVENANCE: the
    // executable the daemon launches as the Pi runtime, resolved through that
    // installation's own static import graph to the ONE module both required classes
    // are exported from. It is deliberately NOT derived from the child's argv, which
    // Pi overwrites with its own process title. The child's argv is the bare string
    // "pi", so no census-derived binding could ever be sound. An unresolvable
    // installation returns null and the option refuses with a typed blocker.
    // ONE resolver call per request: the module and the entry it was reached from are
    // the same resolved installation, so they can never come from different graphs.
    legacyPiCachedModuleUrl: (targetStartedAtMs?: number) => {
      const resolved = resolvePiInstallationModule({
        executable: process.env["OPENRIG_PI_EXECUTABLE"]?.trim() || "pi",
        pathEnv: process.env["PATH"],
        // Every file of the walked graph must predate the child that loaded it. Passing the
        // target start here is what makes that a graph-wide bound instead of two picked files.
        ...(targetStartedAtMs !== undefined ? { targetStartedAtMs } : {}),
        // Refuse a graph that could change or intercept the inspector itself.
        scanForbiddenTokens: true,
      });
      return resolved.ok ? { modulePath: fileURLToPath(resolved.value.moduleUrl), entryPath: fileURLToPath(resolved.value.entryUrl) } : null;
    },
    // Daemon-set of PROVEN legacy runner script hashes. Daemon-owned configuration;
    // an unmatched live script refuses rather than being treated as this build.
    legacyRunnerHashes: (process.env["OPENRIG_LEGACY_RUNNER_HASHES"] ?? "")
      .split(",").map(entry => entry.trim().toLowerCase()).filter(entry => /^[0-9a-f]{64}$/.test(entry)),
    // Reduced kernel-region environment provenance per target: the seat's occupant
    // generation positive control, plus whether NODE_OPTIONS is present. No values.
    legacyEnvironmentObserver: input => observeLegacyPiEnvironment(input),
  });
  const result = await lifecycle.rehostRunner({
    seatRef: decodeURIComponent(c.req.param("seatRef")),
    reason: body.reason,
    operator: codexStoppedRecovery || rehostRequest.codexDetachedResume ? transportSenderSession(c) : (body.operator as string | undefined) ?? null,
    ...(codexStoppedRecovery ? {codexStoppedRecovery,actorGeneration:c.req.header("X-OpenRig-Occupant-Generation")} : {}),
    ...(rehostRequest.codexDetachedResume ? {codexDetachedResume:true,actorGeneration:c.req.header("X-OpenRig-Occupant-Generation")} : {}),
    legacyNativeWitness: rehostRequest.legacyNativeWitness,
    stoppedTargetRecovery: rehostRequest.stoppedTargetRecovery,
    ...(rehostRequest.stoppedTargetAcceptanceReference ? { stoppedTargetAcceptanceReference: rehostRequest.stoppedTargetAcceptanceReference } : {}),
    onPreEffectRefusal: refusal => (refusal.code === "rehost_process_identity_unknown" ? { ...refusal, observed: { ...(refusal.observed ?? {}), reasons: [...new Set([...proofReasons.map(r => r.code), ...((refusal.observed?.reasons as string[] | undefined) ?? [])])] } } : refusal),
  });
  return c.json(result, result.ok ? 200 : seatLifecycleStatus(result.code));
});

function seatLifecycleStatus(code: string): 400 | 404 | 409 | 500 | 502 {
  if (code === "seat_ref_required" || code === "missing_model" || code === "missing_reason" || code === "missing_actor" || code === "fresh_required" || code === "invalid_cwd" || code === "invalid_codex_profile" || code === "profile_not_installed") return 400;
  if (code === "seat_not_found") return 404;
  if (code === "tmux_probe_failed") return 502;
  if (code === "launch_unavailable" || code === "runtime_adapter_missing" || code === "launch_failed" || code === "startup_failed" || code === "profile_guard_unavailable") return 500;
  // seat_ambiguous / session_live / session_not_live / no_session / claimed_session /
  // nothing_to_clean — state conflicts, not client syntax errors.
  return 409;
}

seatRoutes.post("/set-permissions/:seatRef", async (c) => {
  const body = await c.req.json<Record<string, unknown>>().catch(() => ({} as Record<string, unknown>));
  const actor = transportSenderSession(c);
  if (!actor || !body || Array.isArray(body) || typeof body.mode !== "string" || typeof body.reason !== "string") {
    return c.json({ error: "Sender identity, mode and reason are required" }, 400);
  }
  const result = await seatLifecycleService(c).setPermissions({
    seatRef: decodeURIComponent(c.req.param("seatRef")), mode: body.mode, reason: body.reason, actor,
  });
  return c.json(result, result.ok ? 200 : seatLifecycleStatus(result.code));
});

seatRoutes.post("/set-model/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  const result = await seatLifecycleService(c).setModel({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    model: typeof body["model"] === "string" ? body["model"] : "",
    reason: typeof body["reason"] === "string" ? body["reason"] : "",
    operator: typeof body["operator"] === "string" ? body["operator"] : null,
  });
  if (result.ok) return c.json(result);
  return c.json(result, seatLifecycleStatus(result.code));
});

seatRoutes.post("/set-cwd/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  const actor = transportSenderSession(c);
  if (!actor) return c.json({ ok: false, code: "missing_actor", message: "Transport sender identity is required for directory audit." }, 400);
  const result = await seatLifecycleService(c).setCwd({ seatRef: decodeURIComponent(c.req.param("seatRef")),
    cwd: typeof body["cwd"] === "string" ? body["cwd"] : "", reason: typeof body["reason"] === "string" ? body["reason"] : "", actor });
  return c.json(result, result.ok ? 200 : seatLifecycleStatus(result.code));
});

seatRoutes.post("/set-codex-profile/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  // This is an attributed local transport audit, not an occupant-generation
  // proof or a new Operator-authorization grant. Existing caller policy applies.
  const actor = transportSenderSession(c);
  if (!actor) return c.json({ ok: false, code: "missing_actor", message: "A transport-derived seat identity is required for profile pin audit." }, 400);
  const result = await seatLifecycleService(c).setCodexProfile({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    profile: typeof body["profile"] === "string" ? body["profile"] : "",
    reason: typeof body["reason"] === "string" ? body["reason"] : "",
    actor,
  });
  if (result.ok) return c.json(result);
  return c.json(result, seatLifecycleStatus(result.code));
});

seatRoutes.post("/launch/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  const result = await seatLifecycleService(c).launchFresh({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    fresh: body["fresh"] === true,
    reason: typeof body["reason"] === "string" ? body["reason"] : "",
    stop: body["stop"] === true,
    operator: typeof body["operator"] === "string" ? body["operator"] : null,
  });
  if (result.ok) return c.json(result);
  return c.json(result, seatLifecycleStatus(result.code));
});

seatRoutes.post("/stop/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  const result = await seatLifecycleService(c).stopSeat({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    reason: typeof body["reason"] === "string" ? body["reason"] : "",
    operator: typeof body["operator"] === "string" ? body["operator"] : null,
  });
  if (result.ok) return c.json(result);
  return c.json(result, seatLifecycleStatus(result.code));
});

seatRoutes.post("/clean/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  const result = await seatLifecycleService(c).cleanSeat({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    reason: typeof body["reason"] === "string" ? body["reason"] : "",
    operator: typeof body["operator"] === "string" ? body["operator"] : null,
  });
  if (result.ok) return c.json(result);
  return c.json(result, seatLifecycleStatus(result.code));
});

// OPR.0.4.3.26 — seat-recovery VIEW retarget. Points an attached tmux client at
// the seat's canonical session/window. VIEW-ONLY: it resolves the seat READ-ONLY
// (SeatStatusService) and only probes/switches via the tmux adapter (already in
// context). It does NOT construct SeatHandoverService / SessionRegistry writes /
// ClaimService and never routes through converge/reconcile — no routing,
// binding, session, transcript, or identity mutation is possible here.
seatRoutes.post("/switch-client/:seatRef", async (c) => {
  const body: Record<string, unknown> = await c.req.json().catch(() => ({}));
  const rigRepo = c.get("rigRepo" as never) as RigRepository;
  const service = new SeatSwitchClientService({
    rigRepo,
    tmuxAdapter: c.get("tmuxAdapter" as never) as TmuxAdapter,
  });

  const rawWindow = body["toWindow"];
  const toWindow = typeof rawWindow === "number" && Number.isInteger(rawWindow) ? rawWindow : null;

  const result = await service.switchClient({
    seatRef: decodeURIComponent(c.req.param("seatRef")!),
    client: typeof body["client"] === "string" && body["client"] !== "" ? body["client"] : null,
    toWindow,
  });

  if (result.ok) {
    return c.json(result.result);
  }

  if (result.code === "seat_ref_required") {
    return c.json(result, 400);
  }
  if (result.code === "seat_not_found" ||
    result.code === "client_not_found" ||
    result.code === "window_not_found") {
    return c.json(result, 404);
  }
  if (result.code === "seat_ambiguous" ||
    result.code === "missing_canonical_session" ||
    result.code === "session_not_found" ||
    result.code === "no_client" ||
    result.code === "ambiguous_client") {
    return c.json(result, 409);
  }
  // switch_failed / tmux_probe_failed — a tmux-layer failure, not a client error.
  return c.json(result, 502);
});
