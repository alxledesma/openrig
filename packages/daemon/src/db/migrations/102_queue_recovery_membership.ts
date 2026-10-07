import type { Migration } from "../migrate.js";

/** Derived decoded tag membership only; no custody or recovery authority.
 * Backfill and triggers run in the migration/queue mutation transaction. Preserve
 * json_each semantics for arrays, object values and scalar strings, including
 * escapes, duplicates and malformed legacy tags (which contribute no members).
 * Ranking is mirrored solely to support an ordered tag seek; reads join the
 * canonical queue row and retain the existing transition/failed-attempt fences. */
export const queueRecoveryMembershipSchema: Migration = {
  name: "102_queue_recovery_membership.sql",
  sql: `
    CREATE TABLE queue_recovery_membership (
      tag TEXT NOT NULL,
      qitem_id TEXT NOT NULL,
      live_rank INTEGER NOT NULL,
      ts_updated TEXT NOT NULL,
      PRIMARY KEY(tag, qitem_id)
    ) WITHOUT ROWID;
    CREATE INDEX idx_queue_recovery_membership_rank
      ON queue_recovery_membership(tag, live_rank, ts_updated DESC, qitem_id DESC);

    INSERT INTO queue_recovery_membership(tag, qitem_id, live_rank, ts_updated)
      SELECT DISTINCT j.value, q.qitem_id,
        CASE WHEN q.state IN ('pending','in-progress','blocked') THEN 0 ELSE 1 END, q.ts_updated
      FROM queue_items q, json_each(CASE WHEN json_valid(q.tags) THEN q.tags ELSE '[]' END) j
      WHERE j.type='text' AND substr(j.value,1,13)='recovery-for:';

    CREATE TRIGGER queue_recovery_membership_insert AFTER INSERT ON queue_items
    BEGIN
      -- REPLACE need not fire delete triggers when recursive_triggers is off.
      DELETE FROM queue_recovery_membership WHERE qitem_id=NEW.qitem_id;
      INSERT INTO queue_recovery_membership(tag, qitem_id, live_rank, ts_updated)
        SELECT DISTINCT value, NEW.qitem_id,
          CASE WHEN NEW.state IN ('pending','in-progress','blocked') THEN 0 ELSE 1 END, NEW.ts_updated
        FROM json_each(CASE WHEN json_valid(NEW.tags) THEN NEW.tags ELSE '[]' END)
        WHERE type='text' AND substr(value,1,13)='recovery-for:';
    END;
    CREATE TRIGGER queue_recovery_membership_update AFTER UPDATE OF tags, state, ts_updated, qitem_id ON queue_items
    WHEN OLD.tags IS NOT NEW.tags OR OLD.state IS NOT NEW.state
      OR OLD.ts_updated IS NOT NEW.ts_updated OR OLD.qitem_id IS NOT NEW.qitem_id
    BEGIN
      DELETE FROM queue_recovery_membership WHERE qitem_id=OLD.qitem_id;
      INSERT INTO queue_recovery_membership(tag, qitem_id, live_rank, ts_updated)
        SELECT DISTINCT value, NEW.qitem_id,
          CASE WHEN NEW.state IN ('pending','in-progress','blocked') THEN 0 ELSE 1 END, NEW.ts_updated
        FROM json_each(CASE WHEN json_valid(NEW.tags) THEN NEW.tags ELSE '[]' END)
        WHERE type='text' AND substr(value,1,13)='recovery-for:';
    END;
    CREATE TRIGGER queue_recovery_membership_delete AFTER DELETE ON queue_items
    BEGIN
      DELETE FROM queue_recovery_membership WHERE qitem_id=OLD.qitem_id;
    END;
    CREATE INDEX idx_queue_recovery_membership_qitem ON queue_recovery_membership(qitem_id);
  `,
};
