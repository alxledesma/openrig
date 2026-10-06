import type { Migration } from "../migrate.js";

/** Additive, nonunique exact-subject lookup for lifecycle duties. Every link of one duty chain carries the same
 * deterministic rootId in its control receipt, so apply-time completion capture can find the live link of an exact
 * subject without scanning the rig's operation history and without depending on which generation or epoch each link
 * was bound to; the second index serves the (duty kind, package) lookup used both when a duty is issued and when an
 * admission-refresh completes. Receipt and custody bytes are unchanged; only lifecycle-control receipts are indexed (the same rows the
 * 099 index already parses), so unrelated historical receipts need not be JSON.
 */
export const lifecycleRootLookupIndexSchema: Migration = {
  name: "100_lifecycle_root_lookup_index.sql",
  sql: `CREATE INDEX IF NOT EXISTS idx_coordinator_lifecycle_root
    ON coordinator_operations(json_extract(receipt,'$.rootId'))
    WHERE kind='coordinator-lifecycle-control';
  CREATE INDEX IF NOT EXISTS idx_coordinator_lifecycle_kind_package
    ON coordinator_operations(json_extract(receipt,'$.kind'),json_extract(receipt,'$.packageKey'))
    WHERE kind='coordinator-lifecycle-control';`,
};
