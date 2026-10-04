# Atomic multi-cohort historical startup quarantine

Dependency graph: actual per-rig custody/Lead+Operator authorization and exact snapshots → bounded manifest → independent source/package review → current pre-startup preservation/backup → atomic quarantine → normal fresh delivery verification. Source-only candidate does not authorize any production operation.

The existing `--wake-recovery-mode observe --wake-recovery-manifest /absolute/regular-file.json` accepts either the unchanged single HistoricalPlan or an explicit bundle:

```json
{"schema":"historical-startup-bundle.v1","cohorts":["each element must be a genuine typed HistoricalPlan, not this illustrative string"]}
```

Each actual cohort contains rigId, leadBatonId, leadGeneration, operatorGeneration, distinct operationId/authorizationId, finite expiresAt, and effects[{outboxId,rowHash,custodyHash}]. Existing exact-current Lead baton and transport-authored authorization actually claimed by genuine current Operator, row/custody hashes, scope and twenty-minute admission remain unchanged. No guessed actor or broad permission is introduced.

All bundle shapes/duplicate IDs/op/auth/Operator-generation consistency are checked before ledger writes. Maximum32 cohorts,2000 effects per cohort,32000 total and existing1MiB regular non-symlink absolute manifest limit. A single rig may have several separately authorized shards; no effect may appear twice. All cohorts use the same actual current Operator generation, but each retains its own rig's authentic current custodian/authorization. File limit may bind before total count; do not assume arbitrary fleet capacity.

One outer SQLite immediate transaction encloses every existing quarantine operation/savepoint. A later authority, hash, custody, authorization, generation or operation refusal rolls back all newly inserted holds and receipts. Startup already calls this seam before abandoned-send recovery and wake draining; no startup/transport order changes. Failure aborts startup rather than permitting partial protection and replay. Original outbox bytes, queue claims/body/resources and native processes are not modified; normal new unlisted effects continue delivery. Quarantine expiry does not release held ambiguous history.

Identical valid bundle replay returns existing attributed receipts; mixed already-held/new cohorts remain all-or-nothing for new changes. Replay preserves existing single-plan behavior and authentic current actor checks. A ledger held under some other operation is not silently imported. No disposition/delivery conclusion or work completion follows from quarantine.

Return path: aggregate receipt states cohort/effect count, nested original attributed receipts, deliveryConclusionunknown and outboxMutations0. Production custodian retains exact private manifest/hash, source/package binding, before/after protected rows and native census, and verifies all selected holds before fresh delivery. Any unsupported rig/canonical custody/ambiguous effect remains a concrete recovery boundary, not a reason to fabricate authorization or omit it from replay protection.

Source tests cover two genuine rigs, fresh independent delivery, legacy single path, idempotency, cross-cohort duplicate/identity refusal and rollback on later hash/custody/authority/authorization failures. No live rollout, credentials, runtime DB/API/native/provider access or public publication occurs in this slice.
