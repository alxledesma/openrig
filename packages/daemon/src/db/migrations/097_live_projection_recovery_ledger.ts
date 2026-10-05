import type {Migration} from '../migrate.js';
/** Generic same-generation live-projection recovery receipts live in their own
 * immutable ledger. Reusing coordinator_operations here would make recovery
 * fail (FK -> coordinator_authority) on legitimate rigs that were never
 * enrolled in coordinator authority; manufacturing enrollment to satisfy a
 * receipt is forbidden instead. The subject identifiers are deliberately plain
 * audit TEXT columns WITHOUT REFERENCES: an immutable receipt must never block
 * supported node removal or rig teardown (an FK plus abort triggers here would
 * make a recovered rig undeletable). */
export const liveProjectionRecoveryLedgerSchema:Migration={name:'097_live_projection_recovery_ledger.sql',sql:`
CREATE TABLE live_projection_recovery_operations (
 operation_id TEXT PRIMARY KEY,
 rig_id TEXT NOT NULL, node_id TEXT NOT NULL, session_id TEXT NOT NULL,
 request_hash TEXT NOT NULL, receipt TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TRIGGER live_projection_recovery_no_delete BEFORE DELETE ON live_projection_recovery_operations BEGIN SELECT RAISE(ABORT,'live_projection_recovery_ledger_immutable'); END;
CREATE TRIGGER live_projection_recovery_no_update BEFORE UPDATE ON live_projection_recovery_operations BEGIN SELECT RAISE(ABORT,'live_projection_recovery_ledger_immutable'); END;
`};
