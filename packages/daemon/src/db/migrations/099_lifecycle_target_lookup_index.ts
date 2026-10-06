import type { Migration } from "../migrate.js";

/** Additive, nonunique lookup for lifecycle retirement/containment predicates.
 * Receipt and custody bytes are unchanged. Only lifecycle-control receipts are
 * indexed: unrelated historical operation receipts need not be JSON.
 */
export const lifecycleTargetLookupIndexSchema: Migration = {
  name: "099_lifecycle_target_lookup_index.sql",
  sql: `CREATE INDEX IF NOT EXISTS idx_coordinator_lifecycle_target_kind
    ON coordinator_operations(json_extract(receipt,'$.targetQueueId'),json_extract(receipt,'$.kind'))
    WHERE kind='coordinator-lifecycle-control';`,
};
