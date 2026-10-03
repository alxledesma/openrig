import type { Migration } from "../migrate.js";

// All queue columns except the five fields changed by retiring-generation claim release.
// The exception is installed only for an exact row preimage in the same SQLite transaction
// that commits the successor. Other connections retain the 091 dispatch fence.
const unchanged = [
  "qitem_id", "ts_created", "source_session", "destination_session", "priority", "tier",
  "tags", "blocked_on", "handed_off_to", "handed_off_from", "expires_at",
  "chain_of_record", "body", "closure_reason", "closure_target", "last_nudge_attempt",
  "last_nudge_result", "last_heartbeat", "resolution", "target_repo", "summary",
  "evidence_ref", "minting_generation_uuid", "human_intent", "human_detail",
];
const unchangedSql = unchanged.map((column) => `NEW.${column} IS OLD.${column}`).join(" AND ");

/** Compatible with databases that already applied 091; never relaxes the general fence. */
export const reservedClaimReleaseSchema: Migration = {
  name: "092_reserved_claim_release.sql",
  sql: `
CREATE TABLE seat_dispatch_claim_releases (
  reservation_id TEXT NOT NULL REFERENCES seat_dispatch_reservations(reservation_id),
  qitem_id TEXT NOT NULL REFERENCES queue_items(qitem_id),
  predecessor_generation TEXT NOT NULL,
  preimage_ts_updated TEXT NOT NULL,
  preimage_claimed_at TEXT,
  preimage_closure_required_at TEXT,
  post_ts_updated TEXT,
  PRIMARY KEY(reservation_id,qitem_id)
);
DROP TRIGGER seat_dispatch_queue_update;
CREATE TRIGGER seat_dispatch_queue_update BEFORE UPDATE ON queue_items
 WHEN EXISTS(SELECT 1 FROM seat_dispatch_reservations r WHERE r.state!='released' AND
   (NEW.destination_session=r.session_name OR OLD.destination_session=r.session_name OR
    NEW.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1) OR
    OLD.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1)))
 AND NOT EXISTS (
   SELECT 1 FROM seat_dispatch_claim_releases p
   JOIN seat_dispatch_reservations r ON r.reservation_id=p.reservation_id
   WHERE p.qitem_id=OLD.qitem_id AND r.state='started'
     AND r.predecessor_generation=p.predecessor_generation
     AND (OLD.destination_session=r.session_name OR OLD.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1))
     AND OLD.state='in-progress' AND OLD.claimed_by_generation_uuid=p.predecessor_generation
     AND OLD.ts_updated=p.preimage_ts_updated
     AND OLD.claimed_at IS p.preimage_claimed_at
     AND OLD.closure_required_at IS p.preimage_closure_required_at
     AND p.post_ts_updated IS NULL
     AND NEW.state='pending' AND NEW.claimed_by_generation_uuid IS NULL
     AND NEW.claimed_at IS NULL AND NEW.closure_required_at IS NULL
     AND NEW.ts_updated IS NOT NULL
     AND ${unchangedSql}
 )
 BEGIN SELECT RAISE(ABORT,'seat_dispatch_reserved'); END;
CREATE TRIGGER seat_dispatch_claim_release_record AFTER UPDATE ON queue_items
 WHEN OLD.state='in-progress' AND NEW.state='pending' AND OLD.claimed_by_generation_uuid IS NOT NULL
 BEGIN
   UPDATE seat_dispatch_claim_releases SET post_ts_updated=NEW.ts_updated
   WHERE qitem_id=OLD.qitem_id AND post_ts_updated IS NULL AND predecessor_generation=OLD.claimed_by_generation_uuid
     AND EXISTS(SELECT 1 FROM seat_dispatch_reservations r WHERE r.reservation_id=seat_dispatch_claim_releases.reservation_id AND r.state='started');
 END;
`,
};
