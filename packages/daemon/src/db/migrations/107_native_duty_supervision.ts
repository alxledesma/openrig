import type { Migration } from "../migrate.js";

/** Durable opt-in holder-continuation grants and one-shot native effect intents. */
export const nativeDutySupervisionSchema: Migration = {
  name: "107_native_duty_supervision.sql",
  sql: `
    CREATE TABLE native_duty_grants (
      scope_id TEXT PRIMARY KEY,
      scope_digest TEXT NOT NULL,
      scope_json TEXT NOT NULL,
      granted_by_session TEXT NOT NULL,
      granted_by_generation TEXT NOT NULL,
      granted_at INTEGER NOT NULL,
      revoked_at INTEGER
    ) WITHOUT ROWID;

    CREATE TRIGGER native_duty_grant_identity_immutable
    BEFORE UPDATE OF scope_id, scope_digest, scope_json, granted_by_session,
      granted_by_generation, granted_at ON native_duty_grants
    BEGIN SELECT RAISE(ABORT, 'native duty grant identity is immutable'); END;
    CREATE TRIGGER native_duty_grant_no_delete BEFORE DELETE ON native_duty_grants
    BEGIN SELECT RAISE(ABORT, 'native duty grants are retained'); END;
    CREATE TRIGGER native_duty_grant_revoke_once
    BEFORE UPDATE OF revoked_at ON native_duty_grants
    WHEN OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
    BEGIN SELECT RAISE(ABORT, 'native duty grant revocation is one-way'); END;

    CREATE TABLE native_duty_registrations (
      registration_id TEXT PRIMARY KEY,
      scope_id TEXT NOT NULL REFERENCES native_duty_grants(scope_id),
      launch_id TEXT NOT NULL,
      supervisor_pid INTEGER NOT NULL,
      node_id TEXT NOT NULL,
      session_name TEXT NOT NULL,
      generation TEXT NOT NULL,
      runtime TEXT NOT NULL CHECK(runtime IN ('codex','pi')),
      configuration_digest TEXT NOT NULL,
      proof_fingerprint TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('awaiting-native-proof','watching','held','stopped')),
      last_heartbeat_at INTEGER NOT NULL,
      observer_deadline INTEGER NOT NULL,
      reason TEXT,
      created_at INTEGER NOT NULL,
      UNIQUE(scope_id, launch_id)
    );
    CREATE INDEX native_duty_registrations_scope ON native_duty_registrations(scope_id);
    CREATE TRIGGER native_duty_registration_identity_immutable
    BEFORE UPDATE OF registration_id, scope_id, launch_id, supervisor_pid, node_id,
      session_name, generation, runtime, configuration_digest, created_at
    ON native_duty_registrations
    BEGIN SELECT RAISE(ABORT, 'native duty registration identity is immutable'); END;
    CREATE TRIGGER native_duty_registration_no_delete BEFORE DELETE ON native_duty_registrations
    BEGIN SELECT RAISE(ABORT, 'native duty registrations are retained'); END;
    CREATE TRIGGER native_duty_registration_phase_monotone
    BEFORE UPDATE OF phase ON native_duty_registrations
    WHEN NOT (
      OLD.phase = NEW.phase OR
      (OLD.phase = 'awaiting-native-proof' AND NEW.phase IN ('watching','held','stopped')) OR
      (OLD.phase = 'watching' AND NEW.phase IN ('held','stopped')) OR
      (OLD.phase = 'held' AND NEW.phase = 'stopped')
    )
    BEGIN SELECT RAISE(ABORT, 'native duty registration cannot be revived'); END;

    CREATE TABLE native_duty_intents (
      registration_id TEXT NOT NULL REFERENCES native_duty_registrations(registration_id),
      operation_id TEXT NOT NULL,
      request_json TEXT NOT NULL,
      request_hash TEXT NOT NULL,
      body_digest TEXT NOT NULL,
      prepared_at INTEGER NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('prepared','effect-in-flight','receipt-confirmed','uncertainty-held')),
      PRIMARY KEY(registration_id, operation_id)
    ) WITHOUT ROWID;
    CREATE UNIQUE INDEX native_duty_one_unresolved_intent
      ON native_duty_intents(registration_id) WHERE phase != 'receipt-confirmed';
    CREATE TRIGGER native_duty_intent_identity_immutable
    BEFORE UPDATE OF registration_id, operation_id, request_json, request_hash,
      body_digest, prepared_at ON native_duty_intents
    BEGIN SELECT RAISE(ABORT, 'native duty intent is immutable'); END;
    CREATE TRIGGER native_duty_intent_no_delete BEFORE DELETE ON native_duty_intents
    BEGIN SELECT RAISE(ABORT, 'native duty intents are retained'); END;
    CREATE TRIGGER native_duty_intent_phase_monotone
    BEFORE UPDATE OF phase ON native_duty_intents
    WHEN NOT (
      OLD.phase = NEW.phase OR
      (OLD.phase = 'prepared' AND NEW.phase IN ('effect-in-flight','uncertainty-held')) OR
      (OLD.phase = 'effect-in-flight' AND NEW.phase IN ('receipt-confirmed','uncertainty-held')) OR
      (OLD.phase = 'uncertainty-held' AND NEW.phase = 'receipt-confirmed')
    )
    BEGIN SELECT RAISE(ABORT, 'native duty intent cannot be retried or revived'); END;
  `,
};
