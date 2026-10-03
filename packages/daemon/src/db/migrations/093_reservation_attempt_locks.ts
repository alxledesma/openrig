import type { Migration } from "../migrate.js";

/** Process exclusion supplements the in-process lifecycle guard; never releases a reservation. */
export const reservationAttemptLocksSchema: Migration = {
  name: "093_reservation_attempt_locks.sql",
  sql: `CREATE TABLE seat_dispatch_attempt_locks (
    node_id TEXT PRIMARY KEY REFERENCES nodes(id),
    reservation_id TEXT NOT NULL REFERENCES seat_dispatch_reservations(reservation_id),
    token TEXT NOT NULL, owner_pid INTEGER NOT NULL, created_at TEXT NOT NULL
  );`,
};
