import type {Migration} from '../migrate.js';
/** Exact direct-origin proof lookup; preserves first immutable attempt by seq. */
export const outboxOriginIndexSchema:Migration={name:'096_outbox_origin_index.sql',sql:`
CREATE INDEX IF NOT EXISTS idx_events_outbox_origin_seq
 ON events(json_extract(payload,'$.outboxId'),seq)
 WHERE type='outbox.direct_attempt';
`};
