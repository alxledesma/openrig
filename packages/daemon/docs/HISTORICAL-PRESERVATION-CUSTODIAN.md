# Preservation-only compatibility contract

This generic extension addresses actual configured pod leads and archived-only delivery debt without inventing dispatcher authority. Existing active plans, genuine enrolled canonical authority and legacy Operator-issued literal-lead batons remain compatible. New names/suffixes/roles/spec labels never acquire authority automatically.

## Dependency graph and custody

Owner-approved configuration/delegation → actual current Operator issues exact typed custody queue → genuine selected current active custodian claims it → custodian snapshots/reviews exact enumerated effect rows/custody → custodian creates exact transport-auth digest queue → actual current Operator claims authorization → bounded quarantine/startup transaction → attributed receipt/current supported return.

No new API, registration table or source-writing owner is necessary. Use supported native queue create/claim/show/transitions and existing `/api/queue/outbox/historical/{snapshot,quarantine}`. Caller identity still comes from supported authenticated runtime transport, never body/header fabrication. Queue IDs below are examples, not defaults or imported admissions.

The Operator-issued custody queue body has **exactly** these keys:

```json
{
 "kind":"historical-preservation-custodian.v1",
 "rigId":"ACTUAL_ACTIVE_CUSTODY_RIG_ID",
 "nodeId":"ACTUAL_CURRENT_CUSTODIAN_NODE_ID",
 "session":"ACTUAL_CUSTODIAN_SESSION",
 "generation":"ACTUAL_CUSTODIAN_MANAGED_GENERATION",
 "operatorGeneration":"ACTUAL_CURRENT_OPERATOR_GENERATION",
 "purpose":"historical-quarantine",
 "configurationRef":"EXACT_CONFIGURATION_EVIDENCE_REFERENCE",
 "ownerDelegationRef":"EXACT_EXISTING_OWNER_SCOPE_REFERENCE",
 "returnPath":{"session":"operator-agent@kernel","queueId":"THIS_CUSTODY_QUEUE_ID"}
}
```

Creation must be genuine current `operator-agent@kernel`, minting generation current, initial transition `transport:v1`. Current custodian must belong that exact active rig/node/session/generation, actually hold in-progress queue claim. References are attributed Operator attestations under existing owner authorization, **not** a parser's independently verified configuration or role grant. Empty references refuse. Current enrolled coordinator authority always prevails: a stale/different custody queue cannot bypass it. Exact actor checks run before replay, retaining changed-generation refusal.

Plan `rigId` is the active custody rig. `leadBatonId` points to this queue and `leadGeneration` to actual selected custodian generation; legacy field names are kept for wire compatibility. Actual custodian issues existing exact two-key authorization `{kind:"outbox-historical-quarantine-authorization",requestDigest:historicalDigest({actor:"operator-agent@kernel",generation:CURRENT_OPERATOR_GENERATION,input:EXACT_PLAN})}` to Operator via transport:v1. Operator actually claims it. Admission remains finite within20minutes, current exact row/custody hashes,1..2000effects, distinct cohort identities and existing atomic startup bundle guards. Ordinary queue completion/failure of the custody queue, with receipt/evidence returned to current Operator, is the return path; a note alone is not effect disposition.

## Archived subjects: quarantine only

For an archived-only cohort append optional `archivedScope` to BOTH typed custody body and snapshot/plan, with byte-semantically identical canonical object/array order:

```json
{"subjects":[{"rigId":"IMMUTABLE_ARCHIVED_RIG_ID","name":"EXACT_ARCHIVED_NAME","archivedAt":"EXACT_ARCHIVE_MARKER","fingerprint":"SHA256_CANONICAL_ARCHIVE_DESCRIPTOR"}]}
```

Fingerprint is `historicalDigest({id:rigId,name,archived_at:archivedAt})`. Every subject must exist with matching immutable ID/name/non-null archive marker/fingerprint.1..32subjects, distinctIDs; enumerate **all** database IDs matching each endpoint name. A same-name active rig causes ambiguity refusal; never guess old endpoint lineage from name. Cross-archived endpoints require both original names' exhaustive subjects. Host-qualified/malformed addresses refuse. Neither endpoint may be outside explicitly delegated subjects. Active custody rig is separate; no archived node/session/current claimant is fabricated.

Quarantine records scope/unknown/quarantineOnly in receipt, holds only enumerated IDs in separate historical ledger and never edits original outbox/body/guard/provenance/queue/claim/resource/archive status. Unlisted fresh IDs continue existing delivery. Changing archive markers/subjects/custody/authorization/current actors refuses; later cohort failure rolls back all newly introduced earlier holds/operation receipts in the startup bundle.1MiB/32cohorts/32000total and legacy single-manifest bounds unchanged.

## Explicit limits

- The preservation-only baton and every archivedScope **refuse `/dispose`**. A separately issued active disposition capability below is required for new configured custodians. They confer no retirement, dispatch, enqueue admission, acceptance, transfer, unarchive, node replacement, provider/private/paid permission or native qualification.
- Existing legitimate active enrolled-holder/legacy contracts retain their supported disposition semantics. Registered direct effects still require genuine actualsender supported abandonment; eligible wake/missinglocal direct withdrawal remains separately authorized. No quarantine becomes delivered or accepted.
- `enable`/`migrateLegacy` uncertainty gates are unchanged: active held uncertainty can still prevent enrollment. Accountable authentic disposition/recovery is required; this patch never waives that debt. Archived protection keeps stale debt held, not merged into an active project's work.
- This source capability alone cannot authorize production cohorts. Root must reconcile current native authority, exact hashes/custody/current actors, backup, before-replay startup ordering, isolated real proof and protected comparisons. No live runtime or provider operation accompanies this source candidate.

## Separate active narrow-disposition capability

Under the owner-approved complete preservation fix, genuine current Operator may issue a **distinct** genuinely claimed custody queue with kind `historical-disposition-custodian.v1`, purpose `historical-disposition`, and required `expiresAt` future/current within20minutes. All other exact fields, current stable active rig/node/session/generations, transport:v1 mint, config/owner references and returnPath match the preservation contract, with this separate queue's own ID. It must omit archivedScope. Current enrolled canonical authority always prevails. No automatic conversion/reuse of a preservation-only queue.

An active withdrawal request names that separately claimed disposition queue as leadBatonId. The prior quarantine retains its original distinct operation; exact current custodian must separately issue Lead→Operator transport:v1 per-effect authorization, actually claimed by current Operator, binding the entire exact request digest. Existing lifecycle lock, current actor recheck, originalrow/custodyhashes, priorheldmembership, finite per-effect20minute admission and strict actions are unchanged. Only `withdraw-obsolete-wake` with preserved actual queue audit pointer, or `custodian-withdraw-unregistered-direct` for **unguarded nonwake missing-local-endpoint** qualifies. Registered direct traffic still refuses and must use the genuine actualsender path. ArchivedScope always refuses even with disposition capability; omitting it cannot bypass active endpoint scope.

This permits authentic narrow active withdrawal before enrollment, preserving unknown delivery and original obligations/claims/resources. It grants no dispatch/enrollment/acceptance/transfer permission, retires no unenumerated effect, and leaves migration's uncertain-effects refusal intact. Source regression fixtures are not live custody/delegation proof.
