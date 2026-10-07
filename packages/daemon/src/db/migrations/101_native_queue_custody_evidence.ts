import type { Migration } from "../migrate.js";

/** Immutable outcome evidence is independent of live/archive transition storage.
 * No legacy backfill: session provenance alone does not establish a generation. */
export const nativeQueueCustodyEvidenceSchema: Migration = {
  name: "101_native_queue_custody_evidence.sql",
  sql: `
    CREATE TABLE queue_native_custody_evidence (
      transition_id INTEGER PRIMARY KEY,
      qitem_id TEXT NOT NULL,
      receipt TEXT NOT NULL
    );
    CREATE INDEX idx_native_custody_qitem ON queue_native_custody_evidence(qitem_id, transition_id);
    CREATE TRIGGER native_custody_no_update BEFORE UPDATE ON queue_native_custody_evidence
      BEGIN SELECT RAISE(ABORT, 'native_custody_evidence_immutable'); END;
    CREATE TRIGGER native_custody_no_delete BEFORE DELETE ON queue_native_custody_evidence
      BEGIN SELECT RAISE(ABORT, 'native_custody_evidence_immutable'); END;
    CREATE TABLE coordinator_held_history_outcome_bases (
      rig_id TEXT NOT NULL,
      outbox_id TEXT NOT NULL,
      receipt TEXT NOT NULL,
      PRIMARY KEY(rig_id, outbox_id),
      FOREIGN KEY(rig_id, outbox_id) REFERENCES coordinator_held_history(rig_id, outbox_id)
    );
    CREATE TRIGGER held_outcome_base_no_update BEFORE UPDATE ON coordinator_held_history_outcome_bases
      BEGIN SELECT RAISE(ABORT, 'held_history_outcome_base_immutable'); END;
    CREATE TRIGGER held_outcome_base_no_delete BEFORE DELETE ON coordinator_held_history_outcome_bases
      BEGIN SELECT RAISE(ABORT, 'held_history_outcome_base_immutable'); END;
  `,
};
