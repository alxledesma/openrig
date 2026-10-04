import type {Migration} from '../migrate.js';
/** Acknowledged UNKNOWN audit debt cannot become executable by removing a hold. */
export const coordinatorHeldHistorySchema:Migration={name:'095_coordinator_held_history.sql',sql:`
CREATE TABLE coordinator_held_history (
 rig_id TEXT NOT NULL REFERENCES rigs(id), outbox_id TEXT NOT NULL REFERENCES outbox_entries(outbox_id),
 migration_operation_id TEXT NOT NULL, original_row_hash TEXT NOT NULL, quarantine_hash TEXT NOT NULL,
 quarantine_operation_hash TEXT NOT NULL, pre_custody_hash TEXT NOT NULL, post_custody_hash TEXT NOT NULL,
 recovery_queue_id TEXT NOT NULL REFERENCES queue_items(qitem_id), receipt TEXT NOT NULL,
 PRIMARY KEY(rig_id,outbox_id)
);
CREATE TRIGGER coordinator_held_no_delete BEFORE DELETE ON coordinator_held_history BEGIN SELECT RAISE(ABORT,'coordinator_held_history_immutable'); END;
CREATE TRIGGER coordinator_held_no_update BEFORE UPDATE ON coordinator_held_history BEGIN SELECT RAISE(ABORT,'coordinator_held_history_immutable'); END;
CREATE TRIGGER coordinator_held_quarantine_no_delete BEFORE DELETE ON outbox_historical_quarantines
 WHEN EXISTS(SELECT 1 FROM coordinator_held_history d WHERE d.outbox_id=OLD.outbox_id)
 BEGIN SELECT RAISE(ABORT,'coordinator_held_history_protected'); END;
CREATE TRIGGER coordinator_held_quarantine_no_drift BEFORE UPDATE ON outbox_historical_quarantines
 WHEN EXISTS(SELECT 1 FROM coordinator_held_history d WHERE d.outbox_id=OLD.outbox_id)
 AND (NEW.outbox_id IS NOT OLD.outbox_id OR NEW.rig_id IS NOT OLD.rig_id OR NEW.original_hash IS NOT OLD.original_hash
 OR NEW.operation_id IS NOT OLD.operation_id OR NEW.authorization_id IS NOT OLD.authorization_id
 OR NEW.admitted_until IS NOT OLD.admitted_until OR NEW.created_at IS NOT OLD.created_at
 OR (NEW.state IS NOT OLD.state AND NOT(NEW.state='disposed' AND (SELECT delivery_state FROM outbox_entries WHERE outbox_id=OLD.outbox_id)='retired')))
 BEGIN SELECT RAISE(ABORT,'coordinator_held_history_protected'); END;
CREATE TRIGGER coordinator_held_operation_no_delete BEFORE DELETE ON outbox_historical_operations
 WHEN EXISTS(SELECT 1 FROM outbox_historical_quarantines h JOIN coordinator_held_history d ON d.outbox_id=h.outbox_id WHERE h.rig_id=OLD.rig_id AND h.operation_id=OLD.operation_id)
 BEGIN SELECT RAISE(ABORT,'coordinator_held_history_protected'); END;
CREATE TRIGGER coordinator_held_operation_no_update BEFORE UPDATE ON outbox_historical_operations
 WHEN EXISTS(SELECT 1 FROM outbox_historical_quarantines h JOIN coordinator_held_history d ON d.outbox_id=h.outbox_id WHERE h.rig_id=OLD.rig_id AND h.operation_id=OLD.operation_id)
 BEGIN SELECT RAISE(ABORT,'coordinator_held_history_protected'); END;
CREATE TRIGGER coordinator_held_effect_no_delete BEFORE DELETE ON outbox_entries
 WHEN EXISTS(SELECT 1 FROM coordinator_held_history d WHERE d.outbox_id=OLD.outbox_id)
 BEGIN SELECT RAISE(ABORT,'coordinator_held_history_protected'); END;
CREATE TRIGGER coordinator_held_effect_no_replay BEFORE UPDATE ON outbox_entries
 WHEN EXISTS(SELECT 1 FROM coordinator_held_history d WHERE d.outbox_id=OLD.outbox_id)
 AND (NEW.outbox_id IS NOT OLD.outbox_id OR NEW.sender_session IS NOT OLD.sender_session OR NEW.destination_session IS NOT OLD.destination_session
 OR NEW.body IS NOT OLD.body OR NEW.tags IS NOT OLD.tags OR NEW.urgency IS NOT OLD.urgency
 OR NEW.ts_dispatched IS NOT OLD.ts_dispatched OR NEW.audit_pointer IS NOT OLD.audit_pointer
 OR NEW.identity_provenance IS NOT OLD.identity_provenance OR NEW.guard_binding IS NOT OLD.guard_binding
 OR NEW.delivered_at IS NOT OLD.delivered_at OR (NEW.delivery_state IS NOT OLD.delivery_state AND NEW.delivery_state!='retired'))
 BEGIN SELECT RAISE(ABORT,'coordinator_held_history_protected'); END;
`};
