import type { Migration } from "../migrate.js";

/** Persistent cutover fences survive daemon/process loss. No time-based deletion/unlock. */
export const seatDispatchReservationsSchema: Migration = {
  name: "091_seat_dispatch_reservations.sql",
  sql: `
CREATE TABLE seat_dispatch_reservations (
 reservation_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, node_id TEXT NOT NULL REFERENCES nodes(id),
 session_name TEXT NOT NULL, predecessor_generation TEXT NOT NULL, predecessor_native_id TEXT NOT NULL,
 actor_session TEXT NOT NULL, actor_generation TEXT NOT NULL, request_hash TEXT NOT NULL,
 expected_json TEXT NOT NULL, frozen_snapshot TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('reserved','started','committed','released')),
 performer_session TEXT, performer_generation TEXT,
 successor_generation TEXT, successor_native_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 release_receipt TEXT, UNIQUE(node_id,operation_id),
 CHECK ((performer_session IS NULL AND performer_generation IS NULL) OR
        (performer_session IS NOT NULL AND performer_generation IS NOT NULL))
);
CREATE UNIQUE INDEX seat_dispatch_one_active ON seat_dispatch_reservations(node_id) WHERE state!='released';
CREATE TRIGGER seat_dispatch_performer_immutable BEFORE UPDATE OF performer_session,performer_generation ON seat_dispatch_reservations
 WHEN OLD.performer_session IS NOT NULL AND
      (NEW.performer_session IS NOT OLD.performer_session OR NEW.performer_generation IS NOT OLD.performer_generation)
 BEGIN SELECT RAISE(ABORT,'seat_dispatch_performer_immutable'); END;
CREATE TABLE seat_dispatch_reservation_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT, reservation_id TEXT NOT NULL REFERENCES seat_dispatch_reservations(reservation_id),
 action TEXT NOT NULL, actor_session TEXT NOT NULL, actor_generation TEXT NOT NULL,
 evidence_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TRIGGER seat_dispatch_queue_insert BEFORE INSERT ON queue_items
 WHEN EXISTS(SELECT 1 FROM seat_dispatch_reservations r WHERE r.state!='released' AND
   (NEW.destination_session=r.session_name OR NEW.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1)))
 BEGIN SELECT RAISE(ABORT,'seat_dispatch_reserved'); END;
CREATE TRIGGER seat_dispatch_queue_update BEFORE UPDATE ON queue_items
 WHEN EXISTS(SELECT 1 FROM seat_dispatch_reservations r WHERE r.state!='released' AND
   (NEW.destination_session=r.session_name OR OLD.destination_session=r.session_name OR
    NEW.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1) OR
    OLD.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1)))
 BEGIN SELECT RAISE(ABORT,'seat_dispatch_reserved'); END;
CREATE TRIGGER seat_dispatch_queue_delete BEFORE DELETE ON queue_items
 WHEN EXISTS(SELECT 1 FROM seat_dispatch_reservations r WHERE r.state!='released' AND
   (OLD.destination_session=r.session_name OR OLD.destination_session=r.session_name||'@'||(SELECT host_id FROM self_host_identity WHERE singleton=1)))
 BEGIN SELECT RAISE(ABORT,'seat_dispatch_reserved'); END;
`,
};
