import { historicalQuarantineExists, isHistoricalQuarantined } from "./historical-effect-disposition.js";
import { resolveGuardTarget, type SeatDeliveryGuard } from "./seat-delivery-guard.js";
import { QueueTransitionLog } from "./queue-transition-log.js";
import { EventBus } from "./event-bus.js";
import { createHash } from "node:crypto";
import type Database from "better-sqlite3";

/**
 * The EXECUTABLE wake-intent namespace. A durable wake intent is written by the
 * daemon (QueueRepository.stageWakeIntent) with an id under this prefix, and the
 * startup drain EXECUTES every pending row under it as a real wake.
 *
 * NOT a route reservation: the public `/outbox/record` audit route no longer
 * refuses caller-supplied ids under this prefix (the W4-era MF5 guard was unbuilt —
 * founder ruling, over-engineering audit: its justification required an adversary
 * inside this trust domain, where the only caller is the daemon's own localhost
 * client). A caller CAN now record an id under this prefix and the drain will
 * select it. Single source of truth for the drain query only.
 */
export const WAKE_INTENT_PREFIX = "wake-intent-";

// W1 (transactional closure): `indeterminate` is the ambiguous-delivery outcome —
// a send that landed on the wire but whose render could not be CONFIRMED (transport
// res.ok && !verified). It is never silently promoted to `delivered` (unconfirmed)
// nor demoted to `failed` (it may have landed); the CAS transitions gate on 'pending',
// so an indeterminate row is TERMINAL-BY-CAS (reconciliation is an out-of-scope
// follow-on, not a W1 transition).
// `sending` (MF3) is a transient CLAIM state: a drainer atomically moves a wake
// intent pending→sending BEFORE the external send, so an overlapping drainer finds
// nothing to claim and cannot double-send. A crash mid-send leaves the row visibly
// `sending`; the recovery boundary (`reconcileAbandonedSending`, run once at
// startup) reconciles it to `indeterminate` — the send is never blindly re-driven.
export const OUTBOX_DELIVERY_STATES = ["pending", "sending", "delivered", "failed", "indeterminate", "retained", "retired"] as const;
export type OutboxDeliveryState = (typeof OUTBOX_DELIVERY_STATES)[number];

/**
 * Validate a raw `delivery_state` cell against the closed union before it is typed
 * as an {@link OutboxDeliveryState}. Replaces the prior unchecked `as` cast: a cast
 * never fails, so a stray DB value (corruption, a newer daemon's state read by an
 * older one) would silently masquerade as a typed value and make every downstream
 * narrowing unsound. We control every writer, so an unknown value is a real defect —
 * fail loud rather than fabricate a type.
 */
export function parseDeliveryState(raw: string): OutboxDeliveryState {
  if ((OUTBOX_DELIVERY_STATES as readonly string[]).includes(raw)) {
    return raw as OutboxDeliveryState;
  }
  throw new OutboxHandlerError(
    "invalid_delivery_state",
    `outbox row has unknown delivery_state ${JSON.stringify(raw)} (expected one of ${OUTBOX_DELIVERY_STATES.join(", ")})`,
  );
}

export interface OutboxEntry {
  outboxId: string;
  senderSession: string;
  destinationSession: string;
  body: string;
  tags: string[] | null;
  urgency: string;
  tsDispatched: string;
  deliveryState: OutboxDeliveryState;
  deliveredAt: string | null;
  auditPointer: string | null;
  guardBinding?: { nodeId: string; session: string; occupant: string | null; pane: string | null } | null;
  retiredAt?: string | null;
  retiredBy?: string | null;
  retirementReason?: string | null;
}

interface OutboxEntryRow {
  outbox_id: string;
  sender_session: string;
  destination_session: string;
  body: string;
  tags: string | null;
  urgency: string;
  ts_dispatched: string;
  delivery_state: string;
  delivered_at: string | null;
  audit_pointer: string | null;
  guard_binding?: string | null;
  retired_at?: string | null;
  retired_by?: string | null;
  retirement_reason?: string | null;
}

export interface OutboxRecordInput {
  outboxId?: string;
  senderSession: string;
  destinationSession: string;
  body: string;
  tags?: string[];
  urgency?: string;
  auditPointer?: string;
  /** P21 §4 era-stamp: the route passes `transport:v1` (senderSession derived from the transport
   *  header chokepoint). Written onto the outbox row; absence = claimed-era. */
  identityProvenance?: string | null;
}

export class OutboxHandlerError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

function newOutboxId(): string {
  const ts = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0, 14);
  const hex = Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, "0");
  return `outbox-${ts}-${hex}`;
}

/**
 * Sender-side outbox. Symmetric to InboxHandler. Records what a sender
 * dispatched independent of receiver behavior. Idempotent on outbox_id.
 *
 * No event-bus events emitted in Phase A — outbox is pure audit. If a future
 * phase wants delivery-tracking events, add them through this surface.
 */
export class OutboxHandler {
  readonly db: Database.Database;
  /** P21 §4: detected once — a curated-migration test DB (or a pre-067 daemon) may lack the
   *  era-stamp column, so the writer degrades (omits it) instead of throwing. */
  private readonly hasIdentityProvenanceColumn: boolean;

  constructor(db: Database.Database) {
    this.db = db;
    this.hasIdentityProvenanceColumn = (
      this.db.prepare("PRAGMA table_info(outbox_entries)").all() as Array<{ name: string }>
    ).some((col) => col.name === "identity_provenance");
  }

  record(input: OutboxRecordInput): OutboxEntry {
    const id = input.outboxId ?? newOutboxId();
    const existing = this.getByIdRaw(id);
    if (existing) return this.rowToEntry(existing);

    const ts = new Date().toISOString();
    const tags = input.tags ? JSON.stringify(input.tags) : null;
    const urgency = input.urgency ?? "routine";

    if (this.hasIdentityProvenanceColumn) {
      this.db
        .prepare(
          `INSERT INTO outbox_entries (
            outbox_id, sender_session, destination_session, body, tags, urgency, ts_dispatched, audit_pointer, identity_provenance
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          id,
          input.senderSession,
          input.destinationSession,
          input.body,
          tags,
          urgency,
          ts,
          input.auditPointer ?? null,
          input.identityProvenance ?? null
        );
    } else {
      this.db
        .prepare(
          `INSERT INTO outbox_entries (
            outbox_id, sender_session, destination_session, body, tags, urgency, ts_dispatched, audit_pointer
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          id,
          input.senderSession,
          input.destinationSession,
          input.body,
          tags,
          urgency,
          ts,
          input.auditPointer ?? null
        );
    }

    return this.getByIdOrThrow(id);
  }

  /** Retention shares the existing outbox ID. Committed wakes preserve each member,
   * even if guard activation happened after staging and quota is now exhausted. */
  retain(input: OutboxRecordInput & { outboxId: string }, binding: NonNullable<OutboxEntry["guardBinding"]>, precommitted = false): OutboxEntry {
    return this.db.transaction(() => {
      const existing = this.getById(input.outboxId);
      if (existing) {
        if (existing.senderSession !== input.senderSession || existing.destinationSession !== input.destinationSession || existing.body !== input.body ||
            (existing.guardBinding && JSON.stringify(existing.guardBinding) !== JSON.stringify(binding))) {
          throw new OutboxHandlerError("delivery_identity_conflict", "The delivery ID already names different content or target identity.");
        }
        if (existing.deliveryState === "retained" || existing.deliveryState === "retired") return existing;
        if (existing.deliveryState !== "pending" && existing.deliveryState !== "sending") {
          throw new OutboxHandlerError("delivery_already_attempted", "This delivery already has a terminal outcome; it cannot be retained or retried.");
        }
      } else if (precommitted) {
        throw new OutboxHandlerError("outbox_not_found", "Precommitted delivery is missing; no replacement custody is created.");
      }
      if (!precommitted) this.assertRetentionCapacity(binding.nodeId, input.body);
      if (!existing) this.record(input);
      this.db.prepare(`UPDATE outbox_entries SET delivery_state='retained', guard_binding=?, delivered_at=NULL
        WHERE outbox_id=? AND delivery_state IN ('pending','sending')`)
        .run(JSON.stringify(binding), input.outboxId);
      return this.getByIdOrThrow(input.outboxId);
    })();
  }

  assertRetentionCapacity(nodeId: string, body: string): void {
    // Provisional bounded defaults. Historical/retired rows and already committed
    // overflow are deliberately not evicted to make these active quotas fit.
    const usage = this.db.prepare(`SELECT count(*) AS count, coalesce(sum(length(CAST(body AS BLOB))),0) AS bytes
      FROM outbox_entries WHERE delivery_state='retained' AND json_extract(guard_binding,'$.nodeId')=?`)
      .get(nodeId) as { count: number; bytes: number };
    const size = Buffer.byteLength(body, "utf8");
    if (size > 1024 * 1024 || usage.count >= 100 || usage.bytes + size > 8 * 1024 * 1024) {
      throw new OutboxHandlerError("retained_quota_full", "Held-message quota is full. Retire a specific held message to release active quota; no input was written.");
    }
  }

  retire(outboxId: string, actor: string, reason: string): OutboxEntry {
    if (!actor.trim() || !reason.trim()) throw new OutboxHandlerError("retirement_reason_required", "Actor and reason are required.");
    const entry = this.getByIdOrThrow(outboxId);
    if (entry.deliveryState === "retired") return entry;
    if (entry.deliveryState !== "retained") throw new OutboxHandlerError("delivery_not_retained", "Only retained messages can be retired.");
    this.db.prepare(`UPDATE outbox_entries SET delivery_state='retired', retired_at=?, retired_by=?, retirement_reason=?
      WHERE outbox_id=? AND delivery_state='retained'`).run(new Date().toISOString(), actor, reason, outboxId);
    return this.getByIdOrThrow(outboxId);
  }

  heldForNode(nodeId: string, limit = 100, offset = 0) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000 || !Number.isInteger(offset) || offset < 0) {
      throw new OutboxHandlerError("invalid_pagination", "Use limit 1–1000 and a nonnegative offset.");
    }
    const total = (this.db.prepare(`SELECT count(*) AS n FROM outbox_entries
      WHERE delivery_state='retained' AND json_extract(guard_binding,'$.nodeId')=?`).get(nodeId) as { n: number }).n;
    const rows = this.db.prepare(`SELECT * FROM outbox_entries
      WHERE delivery_state='retained' AND json_extract(guard_binding,'$.nodeId')=? ORDER BY ts_dispatched,outbox_id LIMIT ? OFFSET ?`)
      .all(nodeId, limit, offset) as OutboxEntryRow[];
    return { items: rows.map(r => this.rowToEntry(r)), total, limit, offset, truncated: offset + rows.length < total };
  }

  /** Explicitly abandon an unresolved historical direct effect, without asserting delivery/failure. */
  async abandonUncertain(input: {outboxId:string;bodySha256:string;expectedState:"pending"|"indeterminate";operationId:string;
    authorizationId:string;reason:string;evidenceRef:string;actor:string;generation:string}, guard: SeatDeliveryGuard | undefined): Promise<OutboxEntry> {
    const refuse = (code:string,message:string):never => {throw new OutboxHandlerError(code,message);};
    if (!guard || guard.db !== this.db) refuse("outbox_lifecycle_required","Same-database lifecycle guard required.");
    if (!["pending","indeterminate"].includes(input.expectedState) || !input.operationId?.trim() || !input.reason?.trim()
      || !input.evidenceRef?.trim() || !input.authorizationId || !/^[a-f0-9]{64}$/.test(input.bodySha256)) refuse("outbox_abandon_contract_required","Exact effect, operation, reason and evidence required.");
    const entry = this.getById(input.outboxId);
    if (!entry || entry.senderSession !== input.actor || entry.outboxId.startsWith(WAKE_INTENT_PREFIX) || entry.guardBinding) {
      refuse("outbox_abandon_refused","Only actual sender's unguarded non-executable direct effect can be abandoned.");
    }
    const local = (session:string) => {
      const parts = session.split("@");
      if (parts.length !== 2) return undefined;
      const row = this.db.prepare(`SELECT s.node_id FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN rigs r ON r.id=n.rig_id
        WHERE s.session_name=? AND r.name=? ORDER BY s.id DESC LIMIT 1`).get(session,parts[1]) as {node_id:string}|undefined;
      if (!row) return undefined;
      const t = this.db.prepare("SELECT generation_uuid FROM occupant_tenures WHERE node_id=? ORDER BY generation_ordinal DESC LIMIT 1").get(row.node_id) as {generation_uuid:string}|undefined;
      return t?.generation_uuid ? {nodeId:row.node_id,generation:t.generation_uuid} : undefined;
    };
    const operator = "operator-agent@kernel";
    const sessions = [input.actor,entry!.destinationSession,operator];
    const nodes = sessions.map(local);
    if (nodes.some(n=>!n) || nodes[0]!.generation !== input.generation) refuse("outbox_generation_unknown","Current local sender, recipient and Kernel Operator identities required.");
    const snapshot = JSON.stringify(entry);
    const requestDigest = createHash("sha256").update(JSON.stringify([input.outboxId,input.bodySha256,input.expectedState,input.operationId,
      input.authorizationId,input.reason,input.evidenceRef,input.actor,input.generation,nodes])).digest("hex");
    return guard!.lifecycle(nodes.map(n=>n!.nodeId), async()=>this.db.transaction(()=>{
      if (JSON.stringify(sessions.map(local)) !== JSON.stringify(nodes) || nodes.some(n=>!guard!.ownsLifecycle(n!.nodeId))) {
        refuse("outbox_generation_changed","Seat identity changed while waiting for lifecycle boundary.");
      }
      const prior = this.db.prepare("SELECT payload FROM events WHERE type='outbox.uncertain_abandoned' AND json_extract(payload,'$.outboxId')=? AND json_extract(payload,'$.operationId')=? ORDER BY seq LIMIT 1")
        .get(input.outboxId,input.operationId) as {payload:string}|undefined;
      if (prior) {
        const event = JSON.parse(prior.payload) as {requestDigest:string};
        if (event.requestDigest !== requestDigest) refuse("outbox_operation_conflict","Abandonment operation reused with changed contract or identities.");
        const result = this.getById(input.outboxId);
        if (result?.deliveryState !== "retired") refuse("outbox_abandon_drift","Retirement receipt and current effect disagree.");
        return result!;
      }
      const current = this.getById(input.outboxId);
      if (JSON.stringify(current) !== snapshot || current?.deliveryState !== input.expectedState
        || createHash("sha256").update(current.body).digest("hex") !== input.bodySha256) {
        refuse("outbox_abandon_drift","Frozen effect body/state changed; no abandonment.");
      }
      const auth = this.db.prepare("SELECT source_session,destination_session,minting_generation_uuid,claimed_by_generation_uuid,state,body,expires_at FROM queue_items WHERE qitem_id=?")
        .get(input.authorizationId) as {source_session:string;destination_session:string;minting_generation_uuid:string|null;claimed_by_generation_uuid:string|null;state:string;body:string;expires_at:string|null}|undefined;
      const creation = this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1")
        .get(input.authorizationId) as {actor_session:string;identity_provenance:string|null}|undefined;
      const contract = {kind:"outbox-abandon-authorization",outboxId:input.outboxId,bodySha256:input.bodySha256,expectedState:input.expectedState,
        operationId:input.operationId,senderGeneration:input.generation,reason:input.reason,evidenceRef:input.evidenceRef};
      let proof:Record<string,unknown>|undefined;try{proof=auth?JSON.parse(auth.body):undefined;}catch{}
      if (!auth || auth.source_session !== operator || auth.destination_session !== input.actor || auth.minting_generation_uuid !== nodes[2]!.generation
        || auth.claimed_by_generation_uuid !== input.generation || auth.state !== "in-progress" || creation?.actor_session !== operator
        || creation.identity_provenance !== "transport:v1" || !auth.expires_at || !Number.isFinite(Date.parse(auth.expires_at)) || Date.parse(auth.expires_at)<=Date.now() || !proof || Object.keys(proof).length !== Object.keys(contract).length
        || Object.entries(contract).some(([k,v])=>proof![k]!==v)) {
        refuse("outbox_operator_authorization_required","Current Kernel Operator's exact transport-authored authorization must be claimed by the current sender.");
      }
      const issued=this.db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id=? AND kind='outbox-abandon-authorization'").get('outbox-abandon-authorization:'+input.authorizationId) as {receipt:string}|undefined;
      if(issued){const r=JSON.parse(issued.receipt),hash=(v:string)=>createHash('sha256').update(v).digest('hex');if(r.authorizationId!==input.authorizationId||r.outboxId!==input.outboxId||r.deadline!==Date.parse(auth!.expires_at!)||r.bodyHash!==hash(auth!.body)||r.effectSnapshotHash!==hash(JSON.stringify(current))||r.sender!==input.actor||r.senderGeneration!==input.generation||r.recipient!==current!.destinationSession||r.recipientGeneration!==nodes[1]!.generation||r.operator!==operator||r.operatorGeneration!==nodes[2]!.generation)refuse('outbox_operator_authorization_required','Issued authorization differs from frozen effect or current native identities');}
      const changed = this.db.prepare("UPDATE outbox_entries SET delivery_state='retired',retired_at=?,retired_by=?,retirement_reason=? WHERE outbox_id=? AND delivery_state=?")
        .run(new Date().toISOString(),input.actor,`Uncertain effect explicitly abandoned: ${input.reason}`,input.outboxId,input.expectedState);
      if (changed.changes !== 1) refuse("outbox_abandon_drift","Concurrent effect disposition won; no abandonment.");
      new EventBus(this.db).persistWithinTransaction({type:"outbox.uncertain_abandoned",schemaVersion:1,outboxId:input.outboxId,operationId:input.operationId,
        requestDigest,originalState:input.expectedState,deliveryConclusion:"unknown",sender:input.actor,senderGeneration:input.generation,
        operatorGeneration:nodes[2]!.generation,recipientGeneration:nodes[1]!.generation,authorizationId:input.authorizationId,
        bodySha256:input.bodySha256,reason:input.reason,evidenceRef:input.evidenceRef,originalAuditPointer:current!.auditPointer});
      return this.getByIdOrThrow(input.outboxId);
    }).immediate());
  }

  /** Internal transport seam: atomic audit origin and observed outcome, never exposed by /outbox/record. */
  recordDirectAttempt(input: OutboxRecordInput, outcome: "delivered" | "failed" | "indeterminate"): OutboxEntry {
    return this.db.transaction(() => {
      // Caller-supplied IDs cannot turn ordinary records into transport evidence.
      const entry = this.record({...input, outboxId: undefined, identityProvenance: "transport:v1"});
      new EventBus(this.db).persistWithinTransaction({type:"outbox.direct_attempt",schemaVersion:1,outboxId:entry.outboxId,
        sender:entry.senderSession,destination:entry.destinationSession,bodySha256:createHash("sha256").update(entry.body).digest("hex"),
        outcome,dispatchedAt:entry.tsDispatched});
      return outcome === "delivered" ? this.markDelivered(entry.outboxId) : outcome === "failed" ? this.markFailed(entry.outboxId) : this.markIndeterminate(entry.outboxId);
    }).immediate();
  }

  private currentRecipientGeneration(actor: string): string | null {
    const target = resolveGuardTarget(this.db, actor);
    return target?.session === actor ? target.occupant : null;
  }

  /** Read-only current recipient evidence; displaying bytes is not testimony or execution. */
  recipientAcknowledgmentContract(actor:string,generation:string,outboxId:string):{body:string;contract:{outboxId:string;bodySha256:string;effectSnapshotSha256:string;expectedState:"pending"|"indeterminate";acknowledged:true;reason:string}} {
    const entry=this.getById(outboxId),hash=(v:string)=>createHash('sha256').update(v).digest('hex');
    // Reject executable and contained history before any origin-history lookup.
    if(!entry||!generation||entry.destinationSession!==actor||entry.outboxId.startsWith(WAKE_INTENT_PREFIX)||entry.guardBinding||this.isHistoricalQuarantined(outboxId)||!['pending','indeterminate'].includes(entry.deliveryState))throw new OutboxHandlerError('outbox_ack_evidence_required','Current exact recipient and unresolved real non-executable unquarantined direct attempt required');
    const current = this.currentRecipientGeneration(actor);
    const attempt=this.db.prepare("SELECT payload FROM events WHERE type='outbox.direct_attempt' AND json_extract(payload,'$.outboxId')=? ORDER BY seq LIMIT 1").get(outboxId) as {payload:string}|undefined;let origin:any;try{origin=attempt?JSON.parse(attempt.payload):null;}catch{}
    if(!entry||!generation||current!==generation||entry.destinationSession!==actor||entry.outboxId.startsWith(WAKE_INTENT_PREFIX)||entry.guardBinding||this.isHistoricalQuarantined(outboxId)||!['pending','indeterminate'].includes(entry.deliveryState)||origin?.schemaVersion!==1||origin.sender!==entry.senderSession||origin.destination!==entry.destinationSession||origin.bodySha256!==hash(entry.body)||origin.dispatchedAt!==entry.tsDispatched||origin.outcome!=='indeterminate')throw new OutboxHandlerError('outbox_ack_evidence_required','Current exact recipient and unresolved real non-executable unquarantined direct attempt required');
    return {body:entry.body,contract:{outboxId,bodySha256:hash(entry.body),effectSnapshotSha256:hash(JSON.stringify(entry)),expectedState:entry.deliveryState as 'pending'|'indeterminate',acknowledged:true,reason:'I actually read this exact direct message; acknowledgment grants no work or acceptance authority'}};
  }
  /** Read-only internal receipt proof; never creates an acknowledgment or replays transport. */
  recipientAcknowledgmentProof(actor:string,generation:string,outboxId:string,receiptId:string):boolean {
    const hash=(v:string)=>createHash('sha256').update(v).digest('hex'),entry=this.getById(outboxId);
    const current = this.currentRecipientGeneration(actor);
    if(!entry||!generation||current!==generation||entry.destinationSession!==actor||entry.deliveryState!=='delivered'||entry.guardBinding||entry.outboxId.startsWith(WAKE_INTENT_PREFIX)||this.isHistoricalQuarantined(outboxId)||receiptId!=='qitem-outbox-recipient-ack-'+hash(JSON.stringify([outboxId,actor,generation])))return false;
    const saved=this.db.prepare("SELECT payload FROM events WHERE type='outbox.recipient_acknowledged' AND json_extract(payload,'$.receiptId')=? ORDER BY seq LIMIT 1").get(receiptId) as {payload:string}|undefined;
    const receipt=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(receiptId) as any;
    const creation=this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1").get(receiptId) as any;
    const attempt=this.db.prepare("SELECT payload FROM events WHERE type='outbox.direct_attempt' AND json_extract(payload,'$.outboxId')=? ORDER BY seq LIMIT 1").get(outboxId) as {payload:string}|undefined;
    let proof:any,origin:any;try{proof=JSON.parse(saved?.payload??'null');origin=JSON.parse(attempt?.payload??'null');}catch{return false;}
    return !!proof&&proof.schemaVersion===1&&proof.outboxId===outboxId&&proof.receiptId===receiptId&&proof.actor===actor&&proof.generation===generation&&proof.bodySha256===hash(entry.body)&&!!receipt&&proof.receiptSnapshotSha256===hash(JSON.stringify(receipt))&&receipt.state==='done'&&receipt.source_session===actor&&receipt.destination_session===entry.senderSession&&receipt.minting_generation_uuid===generation&&receipt.body===JSON.stringify({kind:'outbox-delivery-ack',outboxId,bodySha256:hash(entry.body)})&&creation?.actor_session===actor&&creation.identity_provenance==='transport:v1'&&origin?.schemaVersion===1&&origin.outboxId===outboxId&&origin.sender===entry.senderSession&&origin.destination===actor&&origin.bodySha256===hash(entry.body)&&origin.dispatchedAt===entry.tsDispatched&&origin.outcome==='indeterminate';
  }
  onRecipientAcknowledgment?:(actor:string,generation:string,outboxId:string,receiptId:string)=>void;

  /** Fixed native recipient receipt; terminal evidence only, no generic queue dispatch or transport. */
  acknowledgeRecipientDelivery(actor:string,generation:string,input:{outboxId:string;bodySha256:string;effectSnapshotSha256:string;expectedState:"pending"|"indeterminate";acknowledged:true;reason:string}):{receiptId:string;entry:OutboxEntry} {
    return this.db.transaction(()=>{
      const hash=(v:string)=>createHash("sha256").update(v).digest("hex"),refuse=(code:string,message:string):never=>{throw new OutboxHandlerError(code,message);};
      if(!input||Object.keys(input).sort().join(',')!=='acknowledged,bodySha256,effectSnapshotSha256,expectedState,outboxId,reason'||input.acknowledged!==true||!["pending","indeterminate"].includes(input.expectedState)||[input.outboxId,input.reason].some(v=>typeof v!=='string'||!v.trim())||[input.bodySha256,input.effectSnapshotSha256].some(v=>typeof v!=='string'||! /^[a-f0-9]{64}$/.test(v)))refuse('outbox_ack_contract_required','Exact frozen effect and explicit actual-reading acknowledgment required');
      const tenure = this.currentRecipientGeneration(actor);
      const entry=this.getById(input.outboxId);
      if(!entry||!actor||!generation||tenure!==generation||entry.destinationSession!==actor||entry.outboxId.startsWith(WAKE_INTENT_PREFIX)||entry.guardBinding||this.isHistoricalQuarantined(entry.outboxId))refuse('outbox_ack_recipient_required','Current actual native recipient and non-executable unquarantined direct effect required');
      const receiptId='qitem-outbox-recipient-ack-'+hash(JSON.stringify([input.outboxId,actor,generation]));
      const requestDigest=hash(JSON.stringify({actor,generation,input:{outboxId:input.outboxId,bodySha256:input.bodySha256,effectSnapshotSha256:input.effectSnapshotSha256,expectedState:input.expectedState,acknowledged:input.acknowledged,reason:input.reason}}));
      const saved=this.db.prepare("SELECT payload FROM events WHERE type='outbox.recipient_acknowledged' AND json_extract(payload,'$.receiptId')=? ORDER BY seq LIMIT 1").get(receiptId) as {payload:string}|undefined;
      const receipt=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(receiptId) as Record<string,unknown>|undefined;
      if(saved){let proof:any;try{proof=JSON.parse(saved.payload);}catch{}if(!proof||proof.requestDigest!==requestDigest||!receipt||proof.receiptSnapshotSha256!==hash(JSON.stringify(receipt))||entry!.deliveryState!=='delivered')refuse('outbox_ack_conflict','Acknowledgment replay differs from immutable saved request/receipt');const result=this.reconcileRecipientDelivery({outboxId:input.outboxId,receiptId,actor,generation,reason:input.reason});this.onRecipientAcknowledgment?.(actor,generation,input.outboxId,receiptId);return {receiptId,entry:result};}
      if(receipt)refuse('outbox_ack_conflict','Deterministic receipt ID already exists without internal acknowledgment provenance');
      if(entry!.deliveryState!==input.expectedState||hash(entry!.body)!==input.bodySha256||hash(JSON.stringify(entry))!==input.effectSnapshotSha256)refuse('outbox_ack_drift','Exact effect snapshot/body/state changed; no receipt or reconciliation');
      const ts=new Date().toISOString(),body=JSON.stringify({kind:'outbox-delivery-ack',outboxId:input.outboxId,bodySha256:input.bodySha256});
      this.db.prepare("INSERT INTO queue_items(qitem_id,ts_created,ts_updated,source_session,destination_session,state,body,minting_generation_uuid,closure_reason,summary) VALUES (?,?,?,?,?,'done',?,?,'no-follow-on','Exact recipient delivery acknowledgment; terminal evidence only')").run(receiptId,ts,ts,actor,entry!.senderSession,body,generation);
      new QueueTransitionLog(this.db).append({qitemId:receiptId,state:'done',actorSession:actor,transitionNote:'created',identityProvenance:'transport:v1',closureReason:'no-follow-on'});
      // Existing origin/recipient/receipt checks run inside this same transaction. Any refusal rolls back the fixed receipt.
      const result=this.reconcileRecipientDelivery({outboxId:input.outboxId,receiptId,actor,generation,reason:input.reason});
      const fixed=this.db.prepare('SELECT * FROM queue_items WHERE qitem_id=?').get(receiptId);
      new EventBus(this.db).persistWithinTransaction({type:'outbox.recipient_acknowledged',schemaVersion:1,outboxId:input.outboxId,receiptId,actor,generation,requestDigest,receiptSnapshotSha256:hash(JSON.stringify(fixed)),originalState:input.expectedState,bodySha256:input.bodySha256});
      this.onRecipientAcknowledgment?.(actor,generation,input.outboxId,receiptId);
      return {receiptId,entry:result};
    }).immediate();
  }

  /** Reconcile an old direct effect only from a current recipient's durable exact acknowledgment.
   * No queue mutations, sends, native input or executable wake-intent retirement. */
  reconcileRecipientDelivery(input: { outboxId: string; receiptId: string; actor: string; generation: string; reason: string }): OutboxEntry {
    return this.db.transaction(() => {
      const entry = this.getById(input.outboxId);
      if (!entry) throw new OutboxHandlerError("outbox_not_found", "Exact outbox entry not found.");
      if (!input.reason?.trim() || !input.actor || !input.generation || entry.outboxId.startsWith(WAKE_INTENT_PREFIX)
        || entry.destinationSession !== input.actor || entry.guardBinding) {
        throw new OutboxHandlerError("outbox_reconciliation_refused", "Attributed recipient, reason and non-executable direct effect required.");
      }
      const current = this.currentRecipientGeneration(input.actor);
      const attempt = this.db.prepare("SELECT payload FROM events WHERE type='outbox.direct_attempt' AND json_extract(payload,'$.outboxId')=? ORDER BY seq LIMIT 1").get(entry.outboxId) as {payload:string}|undefined;
      let origin: {schemaVersion?:number;sender?:string;destination?:string;bodySha256?:string;outcome?:string;dispatchedAt?:string}|undefined;
      try { origin = attempt ? JSON.parse(attempt.payload) : undefined; } catch {}
      const hash = createHash("sha256").update(entry.body).digest("hex");
      if (origin?.schemaVersion !== 1 || origin.sender !== entry.senderSession || origin.destination !== entry.destinationSession
        || origin.bodySha256 !== hash || origin.dispatchedAt !== entry.tsDispatched || origin.outcome !== "indeterminate") {
        throw new OutboxHandlerError("outbox_direct_attempt_required", "Actual unresolved direct transport attempt evidence required; ordinary records cannot prove delivery.");
      }
      const receipt = this.db.prepare("SELECT source_session,destination_session,minting_generation_uuid,body,ts_created FROM queue_items WHERE qitem_id=?").get(input.receiptId) as {source_session:string;destination_session:string;minting_generation_uuid:string|null;body:string;ts_created:string}|undefined;
      let proof: {kind?:string;outboxId?:string;bodySha256?:string}|undefined;
      try { proof = receipt ? JSON.parse(receipt.body) : undefined; } catch {}
      const creation = this.db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1").get(input.receiptId) as {actor_session:string;identity_provenance:string|null}|undefined;
      if (creation?.actor_session !== input.actor || creation.identity_provenance !== "transport:v1") {
        throw new OutboxHandlerError("outbox_recipient_receipt_required", "Receipt creation must be attributed to the recipient's managed transport.");
      }
      if (current !== input.generation || !receipt || receipt.source_session !== input.actor
        || receipt.destination_session !== entry.senderSession || receipt.minting_generation_uuid !== input.generation
        || !Number.isFinite(Date.parse(entry.tsDispatched)) || !Number.isFinite(Date.parse(receipt.ts_created)) || Date.parse(receipt.ts_created) < Date.parse(entry.tsDispatched)
        || proof?.kind !== "outbox-delivery-ack" || proof.outboxId !== entry.outboxId || proof.bodySha256 !== hash) {
        throw new OutboxHandlerError("outbox_recipient_receipt_required", "Current recipient's durable exact delivery acknowledgment required.");
      }
      if (entry.deliveryState === "delivered") {
        let audit: {receiptId?:string}|undefined;try { audit = JSON.parse(entry.auditPointer ?? ""); } catch {}
        if (audit?.receiptId !== input.receiptId) throw new OutboxHandlerError("outbox_reconciliation_conflict", "Existing delivery has another evidence receipt.");
        return entry;
      }
      if (!["pending", "indeterminate"].includes(entry.deliveryState)) throw new OutboxHandlerError("outbox_reconciliation_conflict", "Only unresolved direct effects can be reconciled.");
      const audit = JSON.stringify({kind:"recipient-delivery-reconciliation",receiptId:input.receiptId,actor:input.actor,
        generation:input.generation,reason:input.reason.trim(),priorAuditPointer:entry.auditPointer});
      this.db.prepare("UPDATE outbox_entries SET delivery_state='delivered',delivered_at=?,audit_pointer=? WHERE outbox_id=? AND delivery_state=?")
        .run(new Date().toISOString(),audit,input.outboxId,entry.deliveryState);
      return this.getByIdOrThrow(input.outboxId);
    }).immediate();
  }

  markDelivered(outboxId: string): OutboxEntry {
    const ts = new Date().toISOString();
    const result = this.db
      .prepare(
        `UPDATE outbox_entries
           SET delivery_state = 'delivered', delivered_at = ?
         WHERE outbox_id = ? AND delivery_state = 'pending'`
      )
      .run(ts, outboxId);
    if (result.changes === 0) {
      const entry = this.getById(outboxId);
      if (!entry) throw new OutboxHandlerError("outbox_not_found", `outbox ${outboxId} not found`);
      return entry;
    }
    return this.getByIdOrThrow(outboxId);
  }

  markFailed(outboxId: string): OutboxEntry {
    const result = this.db
      .prepare(
        `UPDATE outbox_entries
           SET delivery_state = 'failed'
         WHERE outbox_id = ? AND delivery_state = 'pending'`
      )
      .run(outboxId);
    if (result.changes === 0) {
      const entry = this.getById(outboxId);
      if (!entry) throw new OutboxHandlerError("outbox_not_found", `outbox ${outboxId} not found`);
      return entry;
    }
    return this.getByIdOrThrow(outboxId);
  }

  /**
   * W1 (transactional closure): record an AMBIGUOUS delivery outcome — the send
   * landed but its render could not be confirmed (transport res.ok && !verified).
   * Same compare-and-set shape as markFailed/markDelivered (guards on 'pending'),
   * so it is idempotent and NEVER clobbers a row that already resolved. An
   * indeterminate row is terminal-by-CAS: it is never silently promoted to
   * delivered nor demoted to failed by the drain.
   */
  markIndeterminate(outboxId: string): OutboxEntry {
    const result = this.db
      .prepare(
        `UPDATE outbox_entries
           SET delivery_state = 'indeterminate'
         WHERE outbox_id = ? AND delivery_state = 'pending'`
      )
      .run(outboxId);
    if (result.changes === 0) {
      const entry = this.getById(outboxId);
      if (!entry) throw new OutboxHandlerError("outbox_not_found", `outbox ${outboxId} not found`);
      return entry;
    }
    return this.getByIdOrThrow(outboxId);
  }

  /**
   * MF3: atomically CLAIM a pending wake intent for delivery (pending→sending)
   * BEFORE the external send. Returns true iff this caller won the claim. An
   * overlapping drainer's claim finds the row no longer `pending` and returns
   * false, so exactly one caller performs the external send.
   */
  isHistoricalQuarantined(outboxId: string): boolean { return isHistoricalQuarantined(this.db,outboxId); }

  claimForDelivery(outboxId: string): boolean {
    const exclude = historicalQuarantineExists(this.db) ? " AND NOT EXISTS (SELECT 1 FROM outbox_historical_quarantines h WHERE h.outbox_id=outbox_entries.outbox_id AND h.state='held')" : "";
    const result = this.db
      .prepare(
        `UPDATE outbox_entries SET delivery_state = 'sending'
          WHERE outbox_id = ? AND delivery_state = 'pending'${exclude}`
      )
      .run(outboxId);
    return result.changes === 1;
  }

  /**
   * MF3: finalize a CLAIMED wake intent (sending→delivered|indeterminate|failed)
   * after its external send resolved. CAS-guarded on `sending` so it only ever
   * finalizes a row this drainer claimed. `delivered_at` is stamped only for a
   * confirmed delivery.
   */
  finalizeDelivery(outboxId: string, state: "delivered" | "indeterminate" | "failed" | "retained"): OutboxEntry {
    const deliveredAt = state === "delivered" ? new Date().toISOString() : null;
    const result = this.db
      .prepare(
        `UPDATE outbox_entries SET delivery_state = ?, delivered_at = ?
          WHERE outbox_id = ? AND delivery_state = 'sending'`
      )
      .run(state, deliveredAt, outboxId);
    if (result.changes === 0) {
      const entry = this.getById(outboxId);
      if (!entry) throw new OutboxHandlerError("outbox_not_found", `outbox ${outboxId} not found`);
      return entry;
    }
    return this.getByIdOrThrow(outboxId);
  }

  getById(outboxId: string): OutboxEntry | null {
    const row = this.getByIdRaw(outboxId);
    return row ? this.rowToEntry(row) : null;
  }

  listForSender(senderSession: string, limit = 100): OutboxEntry[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM outbox_entries WHERE sender_session = ? ORDER BY ts_dispatched DESC, rowid DESC LIMIT ?`
      )
      .all(senderSession, limit) as OutboxEntryRow[];
    return rows.map((r) => this.rowToEntry(r));
  }

  /**
   * W1 (transactional closure): list still-`pending` rows whose outbox_id begins
   * with `idPrefix`, oldest first. The drain uses this to recover intents a crash
   * left committed-but-undelivered. `idPrefix` is a trusted compile-time constant
   * (e.g. "wake-intent-") with no LIKE wildcards. Oldest-first + bounded `limit`
   * so a caller can page and terminate on a served short batch (never a silent cap).
   */
  listPending(idPrefix: string, limit = 200): OutboxEntry[] {
    const exclude = historicalQuarantineExists(this.db) ? " AND NOT EXISTS (SELECT 1 FROM outbox_historical_quarantines h WHERE h.outbox_id=outbox_entries.outbox_id AND h.state='held')" : "";
    // EXACT-CASE prefix match. SQLite `LIKE` is case-insensitive by default, so a
    // `LIKE 'wake-intent-%'` selector would also execute `WAKE-INTENT-…` variants.
    // `substr(...) = ?` uses the binary collation (case-sensitive), so exactly one
    // spelling is executable. With the route-side prefix refusal unbuilt, this
    // narrowness is the ONLY thing keeping a recorded case variant out of the
    // executable drain — widen it and variants become executable.
    const rows = this.db
      .prepare(
        `SELECT * FROM outbox_entries
          WHERE delivery_state = 'pending' AND substr(outbox_id, 1, ?) = ?${exclude}
          ORDER BY ts_dispatched ASC, rowid ASC LIMIT ?`
      )
      .all(idPrefix.length, idPrefix, limit) as OutboxEntryRow[];
    return rows.map((r) => this.rowToEntry(r));
  }

  /**
   * BLOCKING 1 (guard re-seal): at a process/recovery boundary, atomically
   * reconcile ABANDONED claims — rows a crashed process left in the transient
   * `sending` state — to `indeterminate` WITHOUT re-sending. A `sending` row is
   * ambiguous (the external send may or may not have landed after the claim), so
   * it records `indeterminate`, never a forever-transient claim and never a blind
   * re-send. EXACT-CASE prefix, matching the executable selector. Returns the
   * count reconciled.
   */
  reconcileAbandonedSending(idPrefix: string): number {
    const result = this.db
      .prepare(
        `UPDATE outbox_entries SET delivery_state = 'indeterminate'
          WHERE delivery_state = 'sending' AND substr(outbox_id, 1, ?) = ?`
      )
      .run(idPrefix.length, idPrefix);
    return result.changes;
  }

  private getByIdRaw(outboxId: string): OutboxEntryRow | undefined {
    return this.db
      .prepare("SELECT * FROM outbox_entries WHERE outbox_id = ?")
      .get(outboxId) as OutboxEntryRow | undefined;
  }

  private getByIdOrThrow(outboxId: string): OutboxEntry {
    const entry = this.getById(outboxId);
    if (!entry) throw new OutboxHandlerError("outbox_not_found", `outbox ${outboxId} not found after write`);
    return entry;
  }

  private rowToEntry(row: OutboxEntryRow): OutboxEntry {
    return {
      outboxId: row.outbox_id,
      senderSession: row.sender_session,
      destinationSession: row.destination_session,
      body: row.body,
      tags: row.tags ? (JSON.parse(row.tags) as string[]) : null,
      urgency: row.urgency,
      tsDispatched: row.ts_dispatched,
      deliveryState: parseDeliveryState(row.delivery_state),
      deliveredAt: row.delivered_at,
      auditPointer: row.audit_pointer,
      ...(row.guard_binding !== undefined ? {
        guardBinding: row.guard_binding ? JSON.parse(row.guard_binding) : null,
        retiredAt: row.retired_at ?? null,
        retiredBy: row.retired_by ?? null,
        retirementReason: row.retirement_reason ?? null,
      } : {}),
    };
  }
}
