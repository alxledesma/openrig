import type { Migration } from "../migrate.js";
/** Exact effect quarantine preserves original outbox bytes and never grants delivery. */
export const historicalEffectDispositionsSchema: Migration = {
 name: "094_historical_effect_dispositions.sql",
 sql: `CREATE TABLE outbox_historical_quarantines (
   outbox_id TEXT PRIMARY KEY REFERENCES outbox_entries(outbox_id), rig_id TEXT NOT NULL REFERENCES rigs(id),
   original_hash TEXT NOT NULL, operation_id TEXT NOT NULL, authorization_id TEXT NOT NULL,
   admitted_until INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('held','disposed')),
   created_at TEXT NOT NULL
 );
 CREATE TABLE outbox_historical_operations (
   rig_id TEXT NOT NULL REFERENCES rigs(id), operation_id TEXT NOT NULL, kind TEXT NOT NULL,
   request_hash TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY(rig_id,operation_id)
 );`,
};
