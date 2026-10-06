# Fork coordination, guarded rotation and decision assessment

This fork's source branch extends the upstream OpenRig0.6.3 commit
`8b5e948807e258aa659862064258e41da9e3eaa9`. It is source for evaluation,
not a claim of deployment or qualification for an existing rig. It does not
rebase automatically onto later upstream changes.

## Deterministic runtime controls

Coordinator authority binds queue mutations to an admitted package, current
owner generation and epoch. Worker results retain their attributed disposition;
coordinator acceptance is a separate receipt. A coordination plan records each
task's concrete owner, action, deadline, exact predecessor receipts and dated
qualification, capacity and effort references. A blocked task activates only its
separately admitted recovery work. Unrelated ready work can continue.

The optional continuity controller requires fresh native identity and activity
observations. Idle does not establish admission or authority. Explicitly enabled
idle Peer takeover retains obligations and requires the new holder's actual
acknowledgment before dispatch. Working, unknown or human-input state holds the
transfer. Repeated reminder text does not count as work progress.

Guarded seat rotation freezes generation, native session, queue, alias, profile,
resource and checkpoint state. Persistent reservations fence ordinary dispatch
through cutover; the claim-release permit belongs only to the swap transaction.
Successor acknowledgment and independent acceptance precede accepted release.
Failed precommit recovery requires exact-attempt cleanup/effect evidence and a
fresh predecessor census. An old failed response alone cannot prove absence of
native effects or authorize a forced reset. Uncertain attempts remain fenced.

CLI coordinator control commands send the existing terminal bearer and current
caller generation. This remains the trusted local seat transport boundary, not
an OS sandbox or protection against a hostile process with the same credentials.

An owner whose lease is still live but whose reconciliation is unacknowledged
continues with `rig coordinator resume-owned <rigId>`. The daemon derives the
caller's own token and the current obligations digest from immutable seat
identity, so no digest or epoch is supplied by hand; acknowledgment and renewal
land in one transaction behind the canonical baton claim, and an active owner may
renew here under the same fences. The operation id is generated fresh unless one
is passed explicitly to replay deliberately, and a lapsed lease always refuses
here: expired authority is recovered only by the explicit expiry-recovery path.

Additional source changes cover managed cmux sizing, generation-bound native
session receipts, audited existing-seat profile/cwd selection and exact outbox
attempt/recipient reconciliation. Queue intent, transport acknowledgment,
implementation return, independent review and acceptance remain distinct.

## Optional decision service

The standalone [decision assessment library](../../scripts/decision-assessment/README.md)
normalizes LAYA, TypeSafe Jev and OpenRouter Decisions transports. Configuration
pins endpoint/model/schema/capabilities, secret references, data policy and
finite timeouts. Its default is disabled observation mode. Hosted paid/private
requests require explicit strict opt-ins; secrets are resolved at request time.
An unavailable or uncalibrated model abstains without changing deterministic
work disposition. Model output has no permission-escalation effect.

No daemon event subscription or return interceptor is installed by this library.
A future provider-neutral runtime adapter must establish actual custody and
current state before assessment, then recheck deterministic authority in the
commit transaction. Native hooks can contribute evidence but cannot replace
common state enforcement. Optional service lifecycle and a versioned normalized
interface allow provider changes independently of OpenRig workflow rules.

Tests use synthetic provider replies and bounded loopback HTTP fixtures. Those
checks do not establish live hosted access, model calibration, input coverage,
DGX capacity, provider retention agreements or cross-platform native parity.
Public examples use placeholder accounts and documentation addresses. Real
configuration, runtime state, profiles, private policies and credentials belong
outside this source repository.

## Upgrade and evaluation

Preserve the Apache2.0 license and upstream notices. Review this branch against
its pinned upstream base before adopting it. Migration092 adds guarded claim
release;093 adds attempt locks. Back up runtime state consistently and qualify
rollback separately before upgrading a live DB. Source test success does not
establish native actor continuity, independent acceptance or a safe live rollout.

For source checks, use the upstream build/typecheck/test commands and
`node --test scripts/decision-assessment/assessment.test.mjs`. Confirm mirrored
skills and generated context packs with the repository's existing check scripts.
Keep external inference and runtime activation explicit, bounded and separately
authorized for the installation being evaluated.
