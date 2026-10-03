import type Database from "better-sqlite3";
import { createHash } from "node:crypto";

/** Use the daemon's durable self identity, exactly as migration 091's queue fence does.
 * A missing identity cannot prove which qualified address is local, so fail closed.
 * Foreign host suffixes are never included in local custody.
 */
export function rotationLocalAddresses(db: Database.Database, session: string): [string, string] {
  const row = db.prepare("SELECT host_id FROM self_host_identity WHERE singleton=1").get() as {host_id:string}|undefined;
  if (!row?.host_id || session.split("@").length !== 2) throw new Error("Current local host identity or canonical seat unavailable for rotation custody");
  return [session, `${session}@${row.host_id}`];
}

/** Exact active queue input for the lifecycle-locked native precondition. */
export function rotationActiveQueueRows(db: Database.Database, session: string): Array<Record<string, unknown>> {
  const [canonicalSeat, localAlias] = rotationLocalAddresses(db, session);
  const rows = db.prepare("SELECT qitem_id AS id,destination_session AS destinationSession,state,claimed_at AS claimedAt,ts_updated AS updated,claimed_by_generation_uuid AS claimGeneration,body FROM queue_items WHERE destination_session IN (?,?) AND state NOT IN ('done','cancelled') ORDER BY qitem_id")
    .all(canonicalSeat, localAlias) as Array<Record<string, unknown>>;
  for (const row of rows) {
    row["bodyHash"] = createHash("sha256").update(String(row["body"])).digest("hex");
    delete row["body"];
  }
  return rows;
}
