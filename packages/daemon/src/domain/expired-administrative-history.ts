import { createHash } from "node:crypto";
import type Database from "better-sqlite3";

type Row = Record<string, unknown>;

const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const isRecord = (value: unknown): value is Row => !!value && typeof value === "object" && !Array.isArray(value);
const exactKeys = (value: Row, keys: readonly string[]): boolean =>
  Object.keys(value).sort().join(",") === [...keys].sort().join(",");
const parseRecord = (value: unknown): Row | undefined => {
  if (typeof value !== "string") return undefined;
  try {
    const parsed: unknown = JSON.parse(value);
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
};
const sha256 = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);

const contractKeys = ["kind", "outboxId", "bodySha256", "expectedState", "operationId", "senderGeneration", "reason", "evidenceRef"] as const;
const receiptKeys = ["authorizationId", "outboxId", "bodyHash", "effectSnapshotHash", "deadline", "sender", "senderGeneration", "recipient", "recipientGeneration", "operator", "operatorGeneration"] as const;
const eventKeys = ["type", "schemaVersion", "outboxId", "operationId", "requestDigest", "originalState", "deliveryConclusion", "sender", "senderGeneration", "operatorGeneration", "recipientGeneration", "authorizationId", "bodySha256", "reason", "evidenceRef", "originalAuditPointer"] as const;

/**
 * Returns true only when an exact, expired, never-claimed native administrative
 * authorization has become immutable history because its exact direct effect
 * was retired with an authentic UNKNOWN outcome. This is a pure queue-watch
 * classification; it never changes the queue, outbox, claims, or receipts.
 *
 * False means the caller must keep the row actionable. Operational authorization
 * and all other dispatch fences remain owned by their existing callers.
 */
export function isContainedExpiredAdministrativeHistory(
  db: Database.Database,
  rigId: string,
  qitemId: string,
  nowMs: number,
): boolean {
  try {
    if (!rigId || !qitemId || !Number.isSafeInteger(nowMs)) return false;

    const queue = db.prepare(`
      SELECT qitem_id,source_session,destination_session,state,claimed_at,claimed_by_generation_uuid,
             body,expires_at,minting_generation_uuid
      FROM queue_items WHERE qitem_id=?
    `).get(qitemId) as (Row & {
      qitem_id: string; source_session: string; destination_session: string; state: string;
      claimed_at: string | null; claimed_by_generation_uuid: string | null; body: string;
      expires_at: string | null; minting_generation_uuid: string | null;
    }) | undefined;
    if (!queue || queue.state !== "pending" || queue.claimed_at || queue.claimed_by_generation_uuid ||
        !queue.expires_at || !Number.isFinite(Date.parse(queue.expires_at)) || Date.parse(queue.expires_at) > nowMs) return false;

    const operationId = `outbox-abandon-authorization:${qitemId}`;
    const authorization = db.prepare(`
      SELECT rig_id,receipt,request_hash FROM coordinator_operations
      WHERE operation_id=? AND kind='outbox-abandon-authorization'
    `).get(operationId) as { rig_id: string; receipt: string; request_hash: string } | undefined;
    if (!authorization || authorization.rig_id !== rigId || !sha256(authorization.request_hash)) return false;
    const receipt = parseRecord(authorization.receipt);
    const contract = parseRecord(queue.body);
    if (!receipt || !exactKeys(receipt, receiptKeys) || !contract || !exactKeys(contract, contractKeys)) return false;
    if (contract.kind !== "outbox-abandon-authorization" || typeof contract.outboxId !== "string" ||
        contract.outboxId.startsWith("wake-intent-") || !sha256(contract.bodySha256) ||
        !["pending", "indeterminate"].includes(String(contract.expectedState)) ||
        typeof contract.operationId !== "string" || !contract.operationId ||
        typeof contract.senderGeneration !== "string" || !contract.senderGeneration ||
        typeof contract.reason !== "string" || !contract.reason ||
        typeof contract.evidenceRef !== "string" || !contract.evidenceRef) return false;

    const deadline = Date.parse(queue.expires_at);
    if (receipt.authorizationId !== qitemId || receipt.outboxId !== contract.outboxId ||
        receipt.bodyHash !== hash(queue.body) || !sha256(receipt.effectSnapshotHash) || receipt.deadline !== deadline ||
        receipt.sender !== queue.destination_session || receipt.senderGeneration !== contract.senderGeneration ||
        receipt.operator !== queue.source_session || receipt.operator !== "operator-agent@kernel" ||
        receipt.operatorGeneration !== queue.minting_generation_uuid || !queue.minting_generation_uuid ||
        typeof receipt.recipient !== "string" || !receipt.recipient ||
        typeof receipt.recipientGeneration !== "string" || !receipt.recipientGeneration) return false;

    const creation = db.prepare(`
      SELECT actor_session,identity_provenance FROM queue_transitions
      WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1
    `).get(qitemId) as { actor_session: string; identity_provenance: string | null } | undefined;
    if (!creation || creation.actor_session !== queue.source_session || creation.identity_provenance !== "transport:v1") return false;

    // A current empty claim is insufficient if a prior claim was later handed off.
    // Native claim/update transitions leave a non-pending state in the transition log.
    if (db.prepare("SELECT 1 FROM queue_transitions WHERE qitem_id=? AND state!='pending' LIMIT 1").get(qitemId)) return false;

    const effect = db.prepare(`
      SELECT outbox_id,sender_session,destination_session,body,delivery_state,retired_by,
             retired_at,audit_pointer,guard_binding
      FROM outbox_entries WHERE outbox_id=?
    `).get(contract.outboxId) as (Row & {
      outbox_id: string; sender_session: string; destination_session: string; body: string;
      delivery_state: string; retired_by: string | null; retired_at: string | null;
      audit_pointer: string | null; guard_binding: string | null;
    }) | undefined;
    if (!effect || effect.outbox_id !== contract.outboxId || effect.delivery_state !== "retired" ||
        effect.retired_by !== queue.destination_session || !effect.retired_at ||
        !Number.isFinite(Date.parse(effect.retired_at)) || effect.guard_binding ||
        effect.sender_session !== queue.destination_session || hash(effect.body) !== contract.bodySha256 ||
        receipt.recipient !== effect.destination_session) return false;

    const events = db.prepare(`
      SELECT payload FROM events WHERE type='outbox.uncertain_abandoned'
        AND json_extract(payload,'$.outboxId')=?
      ORDER BY seq
    `).all(contract.outboxId) as Array<{ payload: string }>;
    if (events.length !== 1) return false;
    const event = parseRecord(events[0]?.payload);
    if (!event || !exactKeys(event, eventKeys) || event.type !== "outbox.uncertain_abandoned" || event.schemaVersion !== 1 ||
        event.outboxId !== contract.outboxId || typeof event.operationId !== "string" || !event.operationId ||
        !sha256(event.requestDigest) || event.originalState !== contract.expectedState ||
        event.deliveryConclusion !== "unknown" || event.bodySha256 !== contract.bodySha256 ||
        event.sender !== effect.sender_session || event.senderGeneration !== contract.senderGeneration ||
        event.originalAuditPointer !== effect.audit_pointer || typeof event.authorizationId !== "string" ||
        event.authorizationId === qitemId || event.reason === undefined || event.evidenceRef === undefined) return false;

    // The event's own authorization must be a different genuine, claimed native
    // authorization for the same immutable effect. This allows one valid retirement
    // to contain sibling expired authorizations without asserting they succeeded.
    const retiredId = event.authorizationId;
    const retiredOperation = db.prepare(`
      SELECT rig_id,receipt,request_hash FROM coordinator_operations
      WHERE operation_id=? AND kind='outbox-abandon-authorization'
    `).get(`outbox-abandon-authorization:${retiredId}`) as { rig_id: string; receipt: string; request_hash: string } | undefined;
    const retiredQueue = db.prepare(`
      SELECT qitem_id,source_session,destination_session,state,claimed_at,claimed_by_generation_uuid,
             body,expires_at,minting_generation_uuid
      FROM queue_items WHERE qitem_id=?
    `).get(retiredId) as (Row & {
      qitem_id: string; source_session: string; destination_session: string; state: string;
      claimed_at: string | null; claimed_by_generation_uuid: string | null; body: string;
      expires_at: string | null; minting_generation_uuid: string | null;
    }) | undefined;
    if (!retiredOperation || retiredOperation.rig_id !== rigId || !sha256(retiredOperation.request_hash) ||
        !retiredQueue || !retiredQueue.claimed_at || retiredQueue.claimed_by_generation_uuid !== event.senderGeneration ||
        retiredQueue.destination_session !== event.sender || retiredQueue.source_session !== "operator-agent@kernel") return false;
    const retiredReceipt = parseRecord(retiredOperation.receipt);
    const retiredContract = parseRecord(retiredQueue.body);
    const retiredCreation = db.prepare(`
      SELECT actor_session,identity_provenance FROM queue_transitions
      WHERE qitem_id=? AND transition_note='created' ORDER BY transition_id LIMIT 1
    `).get(retiredId) as { actor_session: string; identity_provenance: string | null } | undefined;
    if (!retiredReceipt || !exactKeys(retiredReceipt, receiptKeys) || !retiredContract ||
        !exactKeys(retiredContract, contractKeys) || retiredContract.kind !== "outbox-abandon-authorization" ||
        retiredContract.outboxId !== contract.outboxId || retiredContract.bodySha256 !== contract.bodySha256 ||
        retiredContract.expectedState !== contract.expectedState || retiredContract.operationId !== event.operationId ||
        retiredContract.senderGeneration !== event.senderGeneration || retiredContract.reason !== event.reason ||
        retiredContract.evidenceRef !== event.evidenceRef ||
        retiredReceipt.authorizationId !== retiredId || retiredReceipt.outboxId !== contract.outboxId ||
        retiredReceipt.bodyHash !== hash(retiredQueue.body) || !sha256(retiredReceipt.effectSnapshotHash) ||
        retiredReceipt.effectSnapshotHash !== receipt.effectSnapshotHash ||
        retiredReceipt.sender !== event.sender || retiredReceipt.senderGeneration !== event.senderGeneration ||
        retiredReceipt.recipient !== effect.destination_session || retiredReceipt.recipientGeneration !== event.recipientGeneration ||
        retiredReceipt.operator !== "operator-agent@kernel" || retiredReceipt.operatorGeneration !== event.operatorGeneration ||
        retiredQueue.minting_generation_uuid !== event.operatorGeneration || !retiredCreation ||
        retiredCreation.actor_session !== "operator-agent@kernel" || retiredCreation.identity_provenance !== "transport:v1") return false;

    // Reservations and delivery guards are separate live custody fences. A contained
    // historical row cannot mask them even if the caller later adds this predicate.
    const sessions = [queue.destination_session, queue.source_session, effect.destination_session];
    for (const session of new Set(sessions)) {
      const reservation = db.prepare(`
        SELECT 1 FROM seat_dispatch_reservations r
        WHERE r.state!='released' AND (r.session_name=? OR r.node_id IN
          (SELECT s.node_id FROM sessions s WHERE s.session_name=?)) LIMIT 1
      `).get(session, session);
      const guard = db.prepare(`
        SELECT 1 FROM seat_delivery_guards g WHERE (g.desired=1 OR g.effective=1)
          AND g.node_id IN (SELECT s.node_id FROM sessions s WHERE s.session_name=?) LIMIT 1
      `).get(session);
      if (reservation || guard) return false;
    }

    return true;
  } catch {
    // Missing migrations, unexpected schema, or malformed rows remain actionable.
    return false;
  }
}
