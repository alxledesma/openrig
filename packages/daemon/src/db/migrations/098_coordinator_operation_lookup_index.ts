import type {Migration} from '../migrate.js';
/** Measured repair for the coordinator operation lookup.
 *
 *  `SELECT rig_id,receipt FROM coordinator_operations WHERE operation_id=? AND kind=?`
 *  planned as `SCAN coordinator_operations` over 9668 rows on the authoritative backup: the only
 *  index present was the (rig_id,operation_id) primary key, which cannot serve a lookup keyed on
 *  operation_id alone. The single-row `.get` form was cheap only because it stops at the first
 *  match; the `.all` form had to walk the whole table per call and measured 5.7-8.2ms warm and
 *  221ms on first touch.
 *
 *  This index is NONUNIQUE on purpose. `operation_id` is only unique per rig, so the same operation
 *  id legitimately appears under different rigs and under different kinds; a UNIQUE index would
 *  either fail to apply or silently change which rows an ordered `.all` returns. Additive index
 *  only: no table, column, canonical hash or primary key is touched, and no authority, custody or
 *  UNKNOWN effect changes.
 */
export const coordinatorOperationLookupIndexSchema:Migration={name:'098_coordinator_operation_lookup_index.sql',sql:`
CREATE INDEX IF NOT EXISTS idx_coordinator_operations_operation_kind
 ON coordinator_operations(operation_id,kind);
`};