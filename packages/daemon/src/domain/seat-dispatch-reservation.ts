import type Database from "better-sqlite3";
import { createHash, randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { homedir } from "node:os";
import { SeatDeliveryGuard, resolveGuardTarget } from "./seat-delivery-guard.js";
import { rotationLocalAddresses } from "./rotation-local-custody.js";
import type { AttemptEffects, HistoricalFailureProof } from "./failed-precommit-proof.js";

export class DispatchReservationError extends Error {
  constructor(readonly code: string, message: string) { super(message); }
  getResponse(): Response { return Response.json({ ok: false, code: this.code, message: this.message, retryable: this.code === "seat_dispatch_reserved" }, { status: 409 }); }
}
export const canonical = (x: unknown): string => JSON.stringify(x, (_k, v) => v && !Array.isArray(v) && typeof v === "object" ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a.localeCompare(b))) : v);
const digest = (x: unknown): string => createHash("sha256").update(canonical(x)).digest("hex");
export interface DispatchReservation {
  reservation_id: string; operation_id: string; node_id: string; session_name: string;
  predecessor_generation: string; predecessor_native_id: string; actor_session: string; actor_generation: string;
  request_hash: string; expected_json: string; frozen_snapshot: string;
  state: "reserved" | "started" | "committed" | "released"; performer_session: string | null; performer_generation: string | null;
  successor_generation: string | null; successor_native_id: string | null;
  release_receipt: string | null;
}
export interface ReservationRequest {
  reservationId: string; operationId: string; nodeId: string; generation: string; reason: string;
  expected: Record<string, unknown>; profileSha256: string;
}
export interface ReservationNativeState { nativeId: string; runtimeContract: unknown; }
interface ClaimReleaseRow {
  qitem_id: string; predecessor_generation: string; preimage_ts_updated: string;
  preimage_claimed_at: string | null; preimage_closure_required_at: string | null;
  post_ts_updated: string | null;
}
export interface ReservationDeps {
  db: Database.Database; guard: SeatDeliveryGuard;
  verifyPredecessor: (seat: string, expected: Record<string, unknown>) => Promise<void>;
  observeSuccessor: (seat: string) => Promise<ReservationNativeState>;
  censusFailedAttempt?: (reservation: DispatchReservation, effects: AttemptEffects) => Promise<{remainingPids:number[];observedAt:string}>;
  historicalFailure?: (reference: string, sha256: string, reservation: DispatchReservation) => HistoricalFailureProof;
}

/** Same-DB durable exclusion; a crash retains the active fence and all custody. */
export class SeatDispatchReservationService {
  private readonly attemptScope = new AsyncLocalStorage<{id:string;active:boolean}>();
  ownsAttemptLock(id:string):boolean {const scope=this.attemptScope.getStore();return scope?.id===id && scope.active;}
  constructor(private readonly deps: ReservationDeps) {}
  private fail(code: string, message: string): never { throw new DispatchReservationError(code, message); }
  get(id: string): DispatchReservation {
    const r = this.deps.db.prepare("SELECT * FROM seat_dispatch_reservations WHERE reservation_id=?").get(id) as DispatchReservation | undefined;
    return r ?? this.fail("reservation_not_found", "No such durable dispatch reservation");
  }
  private actor(actor: string, generation: string) {
    const target = resolveGuardTarget(this.deps.db, actor);
    if (!actor || !generation || !target || target.session !== actor || target.occupant !== generation) this.fail("reservation_actor_stale", "Actual current local actor/generation required");
    const active = this.deps.db.prepare("SELECT status,startup_status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(target.nodeId) as {status:string;startup_status:string}|undefined;
    if (active?.status !== "running" || active.startup_status !== "ready") this.fail("reservation_actor_stale", "Current actor session must be running and ready");
    return target;
  }
  private profileHash(nodeId: string): string {
    const n = this.deps.db.prepare("SELECT codex_config_profile FROM nodes WHERE id=?").get(nodeId) as { codex_config_profile: string | null } | undefined;
    if (!n?.codex_config_profile || !/^[a-zA-Z0-9_-]+$/.test(n.codex_config_profile)) this.fail("reservation_profile_unknown", "Exact named Codex profile required");
    try { return createHash("sha256").update(readFileSync(resolve(process.env["CODEX_HOME"] ?? resolve(homedir(), ".codex"), `${n.codex_config_profile}.config.toml`))).digest("hex"); }
    catch { return this.fail("reservation_profile_unknown", "Named profile bytes unavailable"); }
  }
  snapshot(nodeId: string, session: string, legacyTemplate?:string): string {
    const db = this.deps.db;
    const n = db.prepare("SELECT id,rig_id,runtime,model,cwd,codex_config_profile,permission_policy,policy_launch_posture FROM nodes WHERE id=?").get(nodeId) as { rig_id: string } | undefined;
    if (!n) this.fail("reservation_target_unknown", "Node unavailable");
    const [canonicalSeat, localAlias] = rotationLocalAddresses(db, session);
    const queue = db.prepare("SELECT * FROM queue_items WHERE destination_session IN (?,?) ORDER BY qitem_id").all(canonicalSeat, localAlias) as Array<Record<string, unknown>>;
    for (const q of queue) { q["bodyHash"] = createHash("sha256").update(String(q["body"])).digest("hex"); delete q["body"]; }
    // Existing durable records retain their exact snapshot contract on upgrade. New
    // reservations capture every queue column; historical recovery additionally requires
    // an attributed full snapshot hash, so omitted legacy columns are never fresh proof.
    if(legacyTemplate){const keys=Object.keys((JSON.parse(legacyTemplate) as {queue:Array<Record<string,unknown>>}).queue[0]??{});if(keys.length)for(const q of queue)for(const key of Object.keys(q))if(!keys.includes(key))delete q[key];}
    const resources = db.prepare(`SELECT resource_key,package_key FROM coordinator_resources WHERE rig_id=? AND package_key IN (
      SELECT package_key FROM coordinator_assignments WHERE rig_id=? AND destination IN (?,?) UNION
      SELECT package_key FROM coordinator_stage_assignments WHERE rig_id=? AND (source IN (?,?) OR destination IN (?,?))) ORDER BY resource_key`)
      .all(n.rig_id, n.rig_id, canonicalSeat, localAlias, n.rig_id, canonicalSeat, localAlias, canonicalSeat, localAlias);
    return canonical({ node: n, permission: db.prepare("SELECT * FROM node_permission_selections WHERE node_id=?").get(nodeId) ?? null, profileSha256: this.profileHash(nodeId), queue, resources });
  }
  private audit(r: DispatchReservation, action: string, actor: string, generation: string, evidence: unknown) {
    this.deps.db.prepare("INSERT INTO seat_dispatch_reservation_audit(reservation_id,action,actor_session,actor_generation,evidence_json,created_at) VALUES(?,?,?,?,?,?)")
      .run(r.reservation_id, action, actor, generation, canonical(evidence), new Date().toISOString());
  }
  /** Same-DB multi-process exclusion for reserved handover/recovery. Dead PID removal
   * removes only an operation lock: durable started fence and custody remain intact. */
  async withAttemptLock<T>(id: string, actor: string, generation: string, fn: () => Promise<T>): Promise<T> {
    if (this.ownsAttemptLock(id)) return fn();
    const r = this.get(id), caller = this.actor(actor,generation), token = randomUUID();
    const ids = [...new Set([r.node_id,caller.nodeId])].sort();
    this.deps.db.transaction(() => {
      this.actor(actor,generation);
      for (const node of ids) {
        const lock = this.deps.db.prepare("SELECT owner_pid FROM seat_dispatch_attempt_locks WHERE node_id=?").get(node) as {owner_pid:number}|undefined;
        if (lock) {
          let gone = false; try { process.kill(lock.owner_pid,0); } catch (error) { gone = (error as NodeJS.ErrnoException).code === "ESRCH"; }
          if (!gone) this.fail("reservation_attempt_busy","Exact attempt lifecycle active or owner liveness unknown; fence retained");
          this.deps.db.prepare("DELETE FROM seat_dispatch_attempt_locks WHERE node_id=? AND owner_pid=?").run(node,lock.owner_pid);
        }
        this.deps.db.prepare("INSERT INTO seat_dispatch_attempt_locks VALUES(?,?,?,?,?)").run(node,id,token,process.pid,new Date().toISOString());
      }
    }).immediate();
    const scope={id,active:true};
    try { return await this.attemptScope.run(scope,fn); }
    finally {scope.active=false;this.deps.db.prepare("DELETE FROM seat_dispatch_attempt_locks WHERE token=? AND owner_pid=?").run(token,process.pid);}
  }
  private predecessorRows(r: DispatchReservation): string {
    const db=this.deps.db;
    return canonical({binding:db.prepare("SELECT * FROM bindings WHERE node_id=?").all(r.node_id),sessions:db.prepare("SELECT * FROM sessions WHERE node_id=? ORDER BY id").all(r.node_id),tenures:db.prepare("SELECT * FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal").all(r.node_id)});
  }
  recordPrepared(r: DispatchReservation,actor:string,generation:string,preparedGeneration:string) {
    if (!this.deps.guard.ownsLifecycle(r.node_id) || this.get(r.reservation_id).state!=="started" || !preparedGeneration) this.fail("reservation_recovery_required","Started lifecycle required for prepared effect receipt");
    this.audit(this.get(r.reservation_id),"successor_prepared",actor,generation,{preparedGeneration,predecessorRows:this.predecessorRows(r)});
  }
  recordFailedPrecommit(id:string,actor:string,generation:string,effects:AttemptEffects,code:string,cleanup:"completed"|"uncertain") {
    const r=this.get(id);
    if (!this.deps.guard.ownsLifecycle(r.node_id) || r.state!=="started" || r.performer_session!==actor || r.performer_generation!==generation) this.fail("reservation_recovery_required","Exact started performer/lifecycle required for failure receipt");
    this.audit(r,"failed_precommit",actor,generation,{operationId:r.operation_id,effects,code,cleanup,deliveryOrQualificationCredit:false});
  }
  recordLaunched(id:string,actor:string,generation:string,effects:AttemptEffects) {
    const r=this.get(id);
    if(!this.deps.guard.ownsLifecycle(r.node_id)||r.state!=="started"||r.performer_session!==actor||r.performer_generation!==generation)this.fail("reservation_recovery_required","Exact started performer required for launch effect receipt");
    this.audit(r,"successor_launched",actor,generation,{operationId:r.operation_id,effects});
  }
  recordUnexpectedFailure(id:string,actor:string,generation:string) {
    const r=this.get(id);
    if(r.state!=="started")return;
    const prepared=this.deps.db.prepare("SELECT evidence_json FROM seat_dispatch_reservation_audit WHERE reservation_id=? AND action='successor_prepared' ORDER BY id DESC LIMIT 1").get(id) as {evidence_json:string}|undefined;
    const launched=this.deps.db.prepare("SELECT evidence_json FROM seat_dispatch_reservation_audit WHERE reservation_id=? AND action='successor_launched' ORDER BY id DESC LIMIT 1").get(id) as {evidence_json:string}|undefined;
    this.recordFailedPrecommit(id,actor,generation,launched?JSON.parse(launched.evidence_json).effects:{preparedGeneration:prepared?JSON.parse(prepared.evidence_json).preparedGeneration:"",discoveredId:null,nativeId:null,replacementStarted:true},"unexpected_precommit_exception","uncertain");
  }
  /** Recovery is an explicit abandon, never retry or started->reserved reset. */
  async abandonFailedPrecommit(actor:string,generation:string,id:string,input:{operationId:string;reason:string;mode:"abandon_failed_precommit";historicalProofRef?:string;historicalProofSha256?:string}):Promise<DispatchReservation> {
    const initial=this.get(id),caller=this.actor(actor,generation);
    if(actor!=="operator-agent@kernel")this.fail("reservation_actor_forbidden","Current genuine Kernel Operator required for failed precommit recovery");
    if(input.mode!=="abandon_failed_precommit" || input.operationId!==initial.operation_id || !input.reason?.trim())this.fail("reservation_release_mismatch","Explicit failed precommit disposition, exact operation and reason required");
    const releaseHash=digest({actor,generation,input});
    if(initial.state==="released") {if(initial.release_receipt!==releaseHash)this.fail("reservation_replay_changed","Recovery replay differs from original caller generation/request");return initial;}
    return this.withAttemptLock(id,actor,generation,()=>this.deps.guard.lifecycle([initial.node_id,caller.nodeId],async()=>{
      const r=this.get(id);this.actor(actor,generation);
      const validate=()=>{
        const now=this.get(id),target=this.deps.guard.target(r.node_id);
        if(now.state!=="started" || now.successor_generation!==null || now.successor_native_id!==null || !now.performer_session || !now.performer_generation || now.performer_session!==r.performer_session || now.performer_generation!==r.performer_generation || target.occupant!==r.predecessor_generation || target.session!==r.session_name)this.fail("reservation_recovery_required","Exact started attempt and unchanged predecessor with zero successor fields required");
        if(!this.deps.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='seat_dispatch_claim_releases'").get() || this.deps.db.prepare("SELECT 1 FROM seat_dispatch_claim_releases WHERE reservation_id=?").get(id))this.fail("reservation_recovery_required","Zero committed claim-release ledger required; missing migration is unknown");
        if(this.snapshot(r.node_id,r.session_name,r.frozen_snapshot)!==r.frozen_snapshot)this.fail("reservation_snapshot_changed","Frozen canonical/alias queue, claims, bodies, resources, config or profile changed");
      };
      validate();
      const failures=this.deps.db.prepare("SELECT evidence_json FROM seat_dispatch_reservation_audit WHERE reservation_id=? AND action='failed_precommit' ORDER BY id DESC LIMIT 1").get(id) as {evidence_json:string}|undefined;
      const prepared=this.deps.db.prepare("SELECT evidence_json FROM seat_dispatch_reservation_audit WHERE reservation_id=? AND action='successor_prepared' ORDER BY id DESC LIMIT 1").get(id) as {evidence_json:string}|undefined;
      let effects:AttemptEffects,historical:HistoricalFailureProof|undefined;
      if(failures && prepared){
        const failure=JSON.parse(failures.evidence_json), prep=JSON.parse(prepared.evidence_json);
        if(failure.operationId!==r.operation_id || !["handover_commit_failed","context_delivery_failed","successor_create_failed"].includes(failure.code) || failure.cleanup!=="completed" || failure.effects?.preparedGeneration!==prep.preparedGeneration || prep.predecessorRows!==this.predecessorRows(r))this.fail("reservation_recovery_required","Failure/cleanup attribution or predecessor session history uncertain");
        effects=failure.effects;
      }else{
        if(!input.historicalProofRef || !input.historicalProofSha256 || !this.deps.historicalFailure)this.fail("reservation_historical_reconciliation_required","No durable failure/effect proof: explicit attributed historical reconciliation required; no forced reset");
        try{historical=this.deps.historicalFailure(input.historicalProofRef,input.historicalProofSha256,r);effects=historical.effects;}catch{this.fail("reservation_historical_reconciliation_required","Historical failure/effect attribution unavailable or changed");}
        if(historical.fullSnapshotSha256!==createHash("sha256").update(this.snapshot(r.node_id,r.session_name)).digest("hex") || historical.predecessorRowsSha256!==createHash("sha256").update(this.predecessorRows(r)).digest("hex"))this.fail("reservation_historical_reconciliation_required","Historical full custody/session census cannot be attributed to unchanged original predecessor");
      }
      const persistedBefore=this.predecessorRows(r);
      if(!effects.preparedGeneration || effects.preparedGeneration===r.predecessor_generation || typeof effects.replacementStarted!=="boolean" || (effects.discoveredId!==null && typeof effects.discoveredId!=="string") || (effects.nativeId!==null && typeof effects.nativeId!=="string"))this.fail("reservation_recovery_required","Exact prepared/discovery/native effect census unavailable");
      if(this.deps.db.prepare("SELECT 1 FROM occupant_tenures WHERE generation_uuid=?").get(effects.preparedGeneration))this.fail("reservation_recovery_required","Prepared successor has a managed tenure");
      const discoveryBefore=this.deps.db.prepare("SELECT id,status,claimed_node_id FROM discovered_sessions WHERE tmux_session=? ORDER BY id").all(r.session_name);
      if((discoveryBefore as Array<{id:string;status:string;claimed_node_id:string|null}>).some(d=>d.status!=="vanished" || d.claimed_node_id!==null) || (effects.discoveredId && !(discoveryBefore as Array<{id:string}>).some(d=>d.id===effects.discoveredId)))this.fail("reservation_recovery_required","Prepared/discovery effects not completely cleaned or attributed");
      try {await this.deps.verifyPredecessor(r.session_name,JSON.parse(r.expected_json));}
      catch {this.fail("reservation_recovery_required","Fresh exact predecessor native/session/profile/permission proof unavailable; fence retained");}
      if(!this.deps.censusFailedAttempt)this.fail("reservation_recovery_required","Fresh native attempt census unavailable");
      let native:{remainingPids:number[];observedAt:string};
      try {native=await this.deps.censusFailedAttempt(r,effects);}
      catch {this.fail("reservation_recovery_required","Fresh native successor-effect census unavailable; fence retained");}
      if(!Array.isArray(native.remainingPids) || native.remainingPids.length || !Number.isFinite(Date.parse(native.observedAt)) || Date.now()-Date.parse(native.observedAt)>5000 || Date.parse(native.observedAt)>Date.now())this.fail("reservation_recovery_required","Successor native effects remain or cleanup census is stale/unknown");
      return this.deps.db.transaction(()=>{
        this.actor(actor,generation);validate();
        if(this.predecessorRows(r)!==persistedBefore || canonical(this.deps.db.prepare("SELECT id,status,claimed_node_id FROM discovered_sessions WHERE tmux_session=? ORDER BY id").all(r.session_name))!==canonical(discoveryBefore))this.fail("reservation_recovery_required","Predecessor or discovery census drifted during native proof");
        if(this.deps.db.prepare("UPDATE seat_dispatch_reservations SET state='released',release_receipt=?,updated_at=? WHERE reservation_id=? AND state='started' AND successor_generation IS NULL AND successor_native_id IS NULL").run(releaseHash,new Date().toISOString(),id).changes!==1)this.fail("reservation_state_changed","Attempt changed during recovery; fence retained");
        this.audit(r,historical?"historical_failed_precommit_reconciled":"failed_precommit_recovered",actor,generation,{...input,effects,native,historical:historical??null,performerSession:r.performer_session,performerGeneration:r.performer_generation,predecessorRowsHash:digest(persistedBefore),deliveryOrQualificationCredit:false,continuityCredit:false});
        this.audit(r,"released",actor,generation,{...input,deliveryOrQualificationCredit:false,continuityCredit:false});return this.get(id);
      }).immediate();
    },id));
  }
  async reserve(actor: string, actorGeneration: string, input: ReservationRequest): Promise<DispatchReservation> {
    if (!input.reason?.trim() || !/^[A-Za-z0-9._-]{1,160}$/.test(input.reservationId) || !/^[A-Za-z0-9._-]{1,160}$/.test(input.operationId) || !/^[a-f0-9]{64}$/.test(input.profileSha256)) this.fail("reservation_contract_invalid", "Exact IDs, reason and profile hash required");
    if (input.expected?.["reservationId"] !== input.reservationId || input.expected["operationId"] !== input.operationId) this.fail("reservation_contract_invalid", "Frozen expected contract must carry exact reservation/operation IDs");
    const caller = this.actor(actor, actorGeneration), target = this.deps.guard.target(input.nodeId);
    if (actor !== target.session && actor !== "operator-agent@kernel") this.fail("reservation_actor_forbidden", "Only current incumbent or Kernel Operator may reserve this cutover");
    const requestHash = digest({ actor, actorGeneration, input });
    const old = this.deps.db.prepare("SELECT * FROM seat_dispatch_reservations WHERE reservation_id=? OR (node_id=? AND operation_id=?)").get(input.reservationId, input.nodeId, input.operationId) as DispatchReservation | undefined;
    if (old) {
      if (old.state === "released" || old.request_hash !== requestHash || old.reservation_id !== input.reservationId || old.predecessor_generation !== target.occupant) this.fail("reservation_replay_changed", "Reservation replay differs, was released or incumbent changed; new attempt requires fresh IDs/preflight");
      return old;
    }
    return this.deps.guard.lifecycle([target.nodeId, caller.nodeId], async () => {
      this.actor(actor, actorGeneration);
      if (this.deps.guard.target(input.nodeId).occupant !== input.generation) this.fail("reservation_generation_changed", "Incumbent changed before reservation");
      if (this.profileHash(input.nodeId) !== input.profileSha256) this.fail("reservation_profile_changed", "Profile bytes changed");
      const before = this.snapshot(input.nodeId, target.session);
      await this.deps.verifyPredecessor(target.session, input.expected);
      return this.deps.db.transaction(() => {
        this.actor(actor, actorGeneration);
        const now = this.deps.guard.target(input.nodeId);
        if (now.occupant !== input.generation || now.session !== target.session || this.snapshot(input.nodeId, target.session) !== before) this.fail("reservation_snapshot_changed", "Generation/config/queue/resource state changed during preflight");
        const at = new Date().toISOString();
        this.deps.db.prepare(`INSERT INTO seat_dispatch_reservations(reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,'reserved',?,?)`)
          .run(input.reservationId, input.operationId, input.nodeId, target.session, input.generation, String(input.expected["generation"] ?? ""), actor, actorGeneration, requestHash, canonical(input.expected), before, at, at);
        const r = this.get(input.reservationId); this.audit(r, "reserved", actor, actorGeneration, { reason: input.reason, requestHash }); return r;
      }).immediate();
    });
  }
  assertHandover(id: string, operationId: string, nodeId: string, actor: string, generation: string, expected: Record<string, unknown>): DispatchReservation {
    const r = this.get(id); this.actor(actor, generation);
    if (!this.deps.guard.ownsLifecycle(nodeId) || r.node_id !== nodeId || r.operation_id !== operationId || r.state !== "reserved" || r.predecessor_generation !== this.deps.guard.target(nodeId).occupant || r.expected_json !== canonical(expected) || this.snapshot(nodeId, r.session_name,r.frozen_snapshot) !== r.frozen_snapshot) this.fail("reservation_handover_mismatch", "Exact active reservation/lifecycle/predecessor/frozen state required");
    if ((actor !== r.actor_session || generation !== r.actor_generation) && actor !== "operator-agent@kernel") this.fail("reservation_actor_forbidden", "Handover actor differs from admitted reserver");
    return r;
  }
  start(r: DispatchReservation, actor: string, generation: string) {
    if (!this.deps.guard.ownsLifecycle(r.node_id)) this.fail("reservation_lifecycle_required", "Lifecycle lease required");
    this.deps.db.transaction(() => {
      this.actor(actor, generation);
      if (this.deps.db.prepare("UPDATE seat_dispatch_reservations SET state='started',performer_session=?,performer_generation=?,updated_at=? WHERE reservation_id=? AND state='reserved' AND performer_session IS NULL AND performer_generation IS NULL").run(actor, generation, new Date().toISOString(), r.reservation_id).changes !== 1) this.fail("reservation_state_changed", "Reservation state changed");
      this.audit(r, "replacement_started", actor, generation, { frozenSnapshotHash: digest(r.frozen_snapshot) });
    }).immediate();
  }
  /** Called within the seat-swap transaction before the retiring occupant is invalidated. */
  prepareClaimRelease(id: string, retiringGeneration: string | undefined) {
    const r = this.get(id);
    if (!this.deps.db.inTransaction || !this.deps.guard.ownsLifecycle(r.node_id) || r.state !== "started" ||
        !retiringGeneration || retiringGeneration !== r.predecessor_generation ||
        this.snapshot(r.node_id, r.session_name,r.frozen_snapshot) !== r.frozen_snapshot) {
      this.fail("reservation_claim_release_mismatch", "Exact frozen predecessor custody required before claim release");
    }
    const frozen = JSON.parse(r.frozen_snapshot) as { queue: Array<{qitem_id:string;state:string;claimed_by_generation_uuid:string|null}> };
    const expected = frozen.queue.filter(q => q.state === "in-progress" && q.claimed_by_generation_uuid === retiringGeneration)
      .map(q => q.qitem_id).sort();
    // The ordinary invalidator releases by generation across the DB. Refuse if that generation
    // owns any row outside this target's frozen local custody rather than altering unseen work.
    const rows = this.deps.db.prepare(`SELECT qitem_id,ts_updated,claimed_at,closure_required_at FROM queue_items
      WHERE state='in-progress' AND claimed_by_generation_uuid=? ORDER BY qitem_id`).all(retiringGeneration) as Array<{
      qitem_id:string;ts_updated:string;claimed_at:string|null;closure_required_at:string|null;
    }>;
    if (canonical(rows.map(row => row.qitem_id)) !== canonical(expected)) {
      this.fail("reservation_claim_release_mismatch", "Retiring generation has custody outside the exact frozen target");
    }
    const insert = this.deps.db.prepare(`INSERT INTO seat_dispatch_claim_releases
      (reservation_id,qitem_id,predecessor_generation,preimage_ts_updated,preimage_claimed_at,preimage_closure_required_at)
      VALUES(?,?,?,?,?,?)`);
    for (const row of rows) insert.run(id,row.qitem_id,retiringGeneration,row.ts_updated,row.claimed_at,row.closure_required_at);
  }
  /** Exact persisted release ledger is the only allowed difference from the immutable preimage. */
  private committedSnapshotMatches(r: DispatchReservation): boolean {
    const frozen = JSON.parse(r.frozen_snapshot) as {queue:Array<Record<string,unknown>>};
    const expected = structuredClone(frozen);
    const rows = this.deps.db.prepare("SELECT * FROM seat_dispatch_claim_releases WHERE reservation_id=? ORDER BY qitem_id")
      .all(r.reservation_id) as ClaimReleaseRow[];
    const expectedIds = expected.queue.filter(q => q["state"] === "in-progress" && q["claimed_by_generation_uuid"] === r.predecessor_generation)
      .map(q => q["qitem_id"]).sort();
    if (canonical(rows.map(row => row.qitem_id)) !== canonical(expectedIds)) return false;
    for (const row of rows) {
      const q = expected.queue.find(item => item["qitem_id"] === row.qitem_id);
      if (!q || !row.post_ts_updated || q["ts_updated"] !== row.preimage_ts_updated ||
          q["claimed_at"] !== row.preimage_claimed_at) return false;
      q["state"] = "pending";
      q["claimed_by_generation_uuid"] = null;
      q["claimed_at"] = null;
      if("closure_required_at" in q)q["closure_required_at"] = null;
      q["ts_updated"] = row.post_ts_updated;
    }
    return canonical(expected) === this.snapshot(r.node_id, r.session_name,r.frozen_snapshot);
  }
  committed(id: string, actor: string, generation: string) {
    const r = this.get(id), target = this.deps.guard.target(r.node_id);
    if (!this.deps.guard.ownsLifecycle(r.node_id) || r.state !== "started" || r.performer_session !== actor || r.performer_generation !== generation || !target.occupant || target.occupant === r.predecessor_generation) this.fail("reservation_successor_unknown", "Same performing actor and committed new managed generation required");
    const n = this.deps.db.prepare("SELECT handover_result FROM nodes WHERE id=?").get(r.node_id) as { handover_result: string | null };
    const session = this.deps.db.prepare("SELECT resume_token FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1").get(r.node_id) as { resume_token: string | null } | undefined;
    if (n.handover_result !== "complete") this.fail("reservation_successor_unknown", "Handover not complete; fence retained");
    if (!this.committedSnapshotMatches(r)) this.fail("reservation_claim_release_mismatch", "Exact predecessor claim release and all other frozen custody required");
    this.deps.db.transaction(() => {
      this.actor(actor, generation);
      if (this.deps.db.prepare("UPDATE seat_dispatch_reservations SET state='committed',successor_generation=?,successor_native_id=?,updated_at=? WHERE reservation_id=? AND state='started'").run(target.occupant, session?.resume_token ?? null, new Date().toISOString(), id).changes !== 1) this.fail("reservation_state_changed", "Reservation changed before successor commit");
      this.audit(r, "successor_committed", actor, generation, { successorGeneration: target.occupant, successorNativeId: session?.resume_token ?? null });
    }).immediate();
  }
  private materialActors(r: DispatchReservation): Set<string> {
    const actors = new Set([r.session_name, r.actor_session, "operator-agent@kernel"]);
    if (r.performer_session) actors.add(r.performer_session);
    const performers = this.deps.db.prepare("SELECT actor_session FROM seat_dispatch_reservation_audit WHERE reservation_id=? AND action IN ('replacement_started','successor_committed')")
      .all(r.reservation_id) as Array<{actor_session:string}>;
    for (const performer of performers) actors.add(performer.actor_session);
    return actors;
  }
  private async verifySuccessor(r: DispatchReservation) {
    const target = this.deps.guard.target(r.node_id), native = await this.deps.observeSuccessor(r.session_name);
    if (r.state !== "committed" || !r.successor_generation || target.occupant !== r.successor_generation || !native.nativeId || native.nativeId === r.predecessor_native_id || (r.successor_native_id && r.successor_native_id !== native.nativeId) || canonical(native.runtimeContract) !== canonical(JSON.parse(r.expected_json)["runtimeContract"]) || !this.committedSnapshotMatches(r)) this.fail("reservation_successor_mismatch", "Current fresh successor/config/queue continuity proof unavailable; fence retained");
    return native;
  }
  async attest(actor: string, generation: string, id: string, input: { operationId: string; checkpointHash: string; kind: "successor_ack" | "independent_acceptance"; evidenceRef: string }): Promise<DispatchReservation> {
    const r = this.get(id), caller = this.actor(actor, generation);
    return this.deps.guard.lifecycle([r.node_id, caller.nodeId], async () => {
      this.actor(actor, generation);
      if (r.operation_id !== input.operationId || JSON.parse(r.expected_json)["checkpointHash"] !== input.checkpointHash || !input.evidenceRef?.trim()) this.fail("reservation_receipt_mismatch", "Exact operation/checkpoint/evidence required");
      if (input.kind === "successor_ack" ? actor !== r.session_name : this.materialActors(r).has(actor)) this.fail("reservation_receipt_actor", "Successor acknowledgment and distinct independent acceptance required");
      const native = await this.verifySuccessor(r);
      this.deps.db.transaction(() => { this.actor(actor, generation); this.audit(r, input.kind, actor, generation, { ...input, successorGeneration: r.successor_generation, nativeId: native.nativeId }); }).immediate();
      return this.get(id);
    }, id);
  }
  async release(actor: string, generation: string, id: string, input: { operationId: string; reason: string; mode: "accepted_successor" | "cancel_before_replacement" }): Promise<DispatchReservation> {
    const r = this.get(id), caller = this.actor(actor, generation);
    if (!input.reason?.trim() || input.operationId !== r.operation_id) this.fail("reservation_release_mismatch", "Exact operation/reason required");
    const releaseHash = digest({ actor, generation, input });
    if (r.state === "released") { if (r.release_receipt !== releaseHash) this.fail("reservation_replay_changed", "Release replay changed"); return r; }
    return this.deps.guard.lifecycle([r.node_id, caller.nodeId], async () => {
      this.actor(actor, generation);
      if (input.mode === "cancel_before_replacement") {
        if (r.state !== "reserved" || this.deps.guard.target(r.node_id).occupant !== r.predecessor_generation || (actor !== "operator-agent@kernel" && (actor !== r.actor_session || generation !== r.actor_generation))) this.fail("reservation_recovery_required", "Replacement may have occurred; pre-cutover cancellation refused");
        await this.deps.verifyPredecessor(r.session_name, JSON.parse(r.expected_json));
      } else if (input.mode === "accepted_successor") {
        if (actor !== "operator-agent@kernel") this.fail("reservation_actor_forbidden", "Current Kernel Operator releases accepted successor");
        await this.verifySuccessor(r);
        const receipts = this.deps.db.prepare("SELECT action,actor_session,actor_generation,evidence_json FROM seat_dispatch_reservation_audit WHERE reservation_id=? AND action IN ('successor_ack','independent_acceptance')").all(id) as Array<{ action: string; actor_session: string; actor_generation: string; evidence_json: string }>;
        const materialActors = this.materialActors(r);
        for (const kind of ["successor_ack", "independent_acceptance"]) if (!receipts.some(x => x.action === kind && (kind !== "independent_acceptance" || !materialActors.has(x.actor_session)) && resolveGuardTarget(this.deps.db, x.actor_session)?.occupant === x.actor_generation && JSON.parse(x.evidence_json).successorGeneration === r.successor_generation)) this.fail("reservation_acceptance_missing", "Current-generation successor and independent receipts required");
      } else this.fail("reservation_release_mismatch", "Explicit disposition required");
      return this.deps.db.transaction(() => {
        this.actor(actor, generation);
        if (input.mode === "accepted_successor" ? !this.committedSnapshotMatches(r) : this.snapshot(r.node_id, r.session_name,r.frozen_snapshot) !== r.frozen_snapshot) this.fail("reservation_snapshot_changed", "Frozen queue/config/resources changed before release");
        if (this.deps.db.prepare("UPDATE seat_dispatch_reservations SET state='released',release_receipt=?,updated_at=? WHERE reservation_id=? AND state=?").run(releaseHash, new Date().toISOString(), id, r.state).changes !== 1) this.fail("reservation_state_changed", "Reservation changed before release");
        this.audit(r, "released", actor, generation, { ...input, deliveryOrQualificationCredit: false }); return this.get(id);
      }).immediate();
    }, id);
  }
}
