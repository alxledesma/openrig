import type { Migration } from "../migrate.js";

export const contextRefreshSchema: Migration = {
  name: "108_context_refresh.sql",
  sql: `
    CREATE TABLE context_refresh_grants (
      grant_id TEXT PRIMARY KEY, grant_json TEXT NOT NULL, grant_digest TEXT NOT NULL,
      created_at INTEGER NOT NULL, revoked_at INTEGER
    ) WITHOUT ROWID;
    CREATE TRIGGER context_refresh_grant_immutable BEFORE UPDATE OF grant_id,grant_json,grant_digest,created_at ON context_refresh_grants
      BEGIN SELECT RAISE(ABORT,'context refresh grant is immutable'); END;
    CREATE TRIGGER context_refresh_grant_revoke_once BEFORE UPDATE OF revoked_at ON context_refresh_grants
      WHEN OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
      BEGIN SELECT RAISE(ABORT,'context refresh revocation is one way'); END;
    CREATE TRIGGER context_refresh_grant_retained BEFORE DELETE ON context_refresh_grants
      BEGIN SELECT RAISE(ABORT,'context refresh grant is retained'); END;

    CREATE TABLE context_refresh_baselines (
      node_id TEXT NOT NULL, generation TEXT NOT NULL, native_id TEXT NOT NULL,
      baseline_count INTEGER NOT NULL CHECK(baseline_count>=0), highest_count INTEGER NOT NULL,
      created_at INTEGER NOT NULL, source TEXT NOT NULL, cursor TEXT NOT NULL,
      PRIMARY KEY(node_id,generation,native_id), CHECK(highest_count>=baseline_count)
    ) WITHOUT ROWID;
    CREATE TRIGGER context_refresh_baseline_immutable BEFORE UPDATE OF node_id,generation,native_id,baseline_count,created_at,source,cursor ON context_refresh_baselines
      BEGIN SELECT RAISE(ABORT,'context refresh baseline is immutable'); END;
    CREATE TRIGGER context_refresh_baseline_monotone BEFORE UPDATE OF highest_count ON context_refresh_baselines
      WHEN NEW.highest_count<OLD.highest_count BEGIN SELECT RAISE(ABORT,'compaction count regressed'); END;
    CREATE TRIGGER context_refresh_baseline_retained BEFORE DELETE ON context_refresh_baselines
      BEGIN SELECT RAISE(ABORT,'context refresh baseline is retained'); END;

    CREATE TABLE context_refresh_checkpoint_requests (
      operation_id TEXT PRIMARY KEY, grant_id TEXT NOT NULL REFERENCES context_refresh_grants(grant_id),
      node_id TEXT NOT NULL, request_json TEXT NOT NULL, request_digest TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('prepared','effect-in-flight','uncertainty-held','receipt-confirmed','cancelled-before-effect')),
      created_at INTEGER NOT NULL, receipt_digest TEXT
    );
    CREATE UNIQUE INDEX context_refresh_checkpoint_per_grant ON context_refresh_checkpoint_requests(grant_id,node_id) WHERE phase!='cancelled-before-effect';
    CREATE UNIQUE INDEX context_refresh_checkpoint_node_debt ON context_refresh_checkpoint_requests(node_id)
      WHERE phase NOT IN ('receipt-confirmed','cancelled-before-effect');
    CREATE TRIGGER context_refresh_checkpoint_immutable BEFORE UPDATE OF operation_id,grant_id,node_id,request_json,request_digest,created_at ON context_refresh_checkpoint_requests
      BEGIN SELECT RAISE(ABORT,'context refresh request is immutable'); END;
    CREATE TRIGGER context_refresh_checkpoint_phase BEFORE UPDATE OF phase ON context_refresh_checkpoint_requests
      WHEN NOT (OLD.phase=NEW.phase OR (OLD.phase='prepared' AND NEW.phase IN ('effect-in-flight','cancelled-before-effect'))
        OR (OLD.phase IN ('effect-in-flight','uncertainty-held') AND NEW.phase IN ('uncertainty-held','receipt-confirmed')))
      BEGIN SELECT RAISE(ABORT,'context refresh request cannot replay'); END;
    CREATE TRIGGER context_refresh_checkpoint_receipt BEFORE UPDATE OF receipt_digest ON context_refresh_checkpoint_requests
      WHEN OLD.receipt_digest IS NOT NULL AND NEW.receipt_digest IS NOT OLD.receipt_digest
      BEGIN SELECT RAISE(ABORT,'context refresh request receipt is immutable'); END;
    CREATE TRIGGER context_refresh_checkpoint_retained BEFORE DELETE ON context_refresh_checkpoint_requests
      BEGIN SELECT RAISE(ABORT,'context refresh request is retained'); END;

    CREATE TABLE context_refresh_attempts (
      attempt_id TEXT PRIMARY KEY, grant_id TEXT NOT NULL REFERENCES context_refresh_grants(grant_id),
      node_id TEXT NOT NULL, attempt_json TEXT NOT NULL, attempt_digest TEXT NOT NULL,
      operation_id TEXT NOT NULL UNIQUE, reservation_id TEXT NOT NULL UNIQUE,
      phase TEXT NOT NULL CHECK(phase IN ('prepared','effect-in-flight','reserved','replacement-started','committed-awaiting-acceptance','uncertainty-held','refreshed','cancelled-before-effect')),
      pending_action TEXT CHECK(pending_action IN ('reserve','handover','release')),
      created_at INTEGER NOT NULL, receipt_digest TEXT
    );
    CREATE UNIQUE INDEX context_refresh_attempt_node_debt ON context_refresh_attempts(node_id)
      WHERE phase NOT IN ('refreshed','cancelled-before-effect');
    CREATE TRIGGER context_refresh_attempt_immutable BEFORE UPDATE OF attempt_id,grant_id,node_id,attempt_json,attempt_digest,operation_id,reservation_id,created_at ON context_refresh_attempts
      BEGIN SELECT RAISE(ABORT,'context refresh attempt is immutable'); END;
    CREATE TRIGGER context_refresh_attempt_phase BEFORE UPDATE OF phase ON context_refresh_attempts
      WHEN NOT (OLD.phase=NEW.phase
        OR (OLD.phase='prepared' AND NEW.phase IN ('effect-in-flight','cancelled-before-effect'))
        OR (OLD.phase='effect-in-flight' AND NEW.phase IN ('reserved','replacement-started','committed-awaiting-acceptance','uncertainty-held','refreshed','cancelled-before-effect'))
        OR (OLD.phase='reserved' AND NEW.phase IN ('replacement-started','committed-awaiting-acceptance','uncertainty-held','refreshed','cancelled-before-effect'))
        OR (OLD.phase='replacement-started' AND NEW.phase IN ('committed-awaiting-acceptance','uncertainty-held','refreshed','cancelled-before-effect'))
        OR (OLD.phase='committed-awaiting-acceptance' AND NEW.phase IN ('uncertainty-held','refreshed'))
        OR (OLD.phase='uncertainty-held' AND NEW.phase IN ('reserved','replacement-started','committed-awaiting-acceptance','refreshed','cancelled-before-effect')))
      BEGIN SELECT RAISE(ABORT,'context refresh attempt cannot replay'); END;
    CREATE TRIGGER context_refresh_attempt_terminal BEFORE UPDATE ON context_refresh_attempts
      WHEN OLD.phase IN ('refreshed','cancelled-before-effect')
      BEGIN SELECT RAISE(ABORT,'context refresh terminal attempt is immutable'); END;
    CREATE TRIGGER context_refresh_attempt_retained BEFORE DELETE ON context_refresh_attempts
      BEGIN SELECT RAISE(ABORT,'context refresh attempt is retained'); END;

    CREATE TABLE context_refresh_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, grant_id TEXT NOT NULL REFERENCES context_refresh_grants(grant_id),
      node_id TEXT NOT NULL, operation_id TEXT NOT NULL, phase TEXT NOT NULL,
      evidence_digest TEXT NOT NULL, observed_at INTEGER NOT NULL
    );
    CREATE TRIGGER context_refresh_events_immutable BEFORE UPDATE ON context_refresh_events
      BEGIN SELECT RAISE(ABORT,'context refresh events are immutable'); END;
    CREATE TRIGGER context_refresh_events_retained BEFORE DELETE ON context_refresh_events
      BEGIN SELECT RAISE(ABORT,'context refresh events are retained'); END;
  `,
};
