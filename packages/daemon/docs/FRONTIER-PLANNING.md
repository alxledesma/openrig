# Frontier planning: exhausted authorized frontier to accountable next work

`src/domain/frontier-planning.ts` closes the gap where an empty frontier was indistinguishable from a
finished project, a protected hold, or a planning lapse. It adds three rows to the lifecycle duty kind
table and no second duty engine.

## Lifecycle

1. **Classification** (`frontier()`), a pure read of durable facts, in strict precedence:
   `ACTIVE` > `AWAITING-ACCEPTANCE` > `PROTECTED-HOLD` > `MATERIALIZABLE` > `EXHAUSTED`. Only an
   `EXHAUSTED` frontier raises the planning obligation. Dormant recovery backups are excluded using the
   same test as `dormantRecoveryHistory`; unplanned legacy-class packages keep their existing
   materialization duty and never support a completeness finding.
2. **Stabilization.** `EXHAUSTED` must hold across consecutive durable `frontier-observation` rows and
   past a minimum dwell. Both numbers are genuine Operator plan configuration (`plan.frontierPlanning`);
   the built-in defaults are the review's stated floor. Observations are bounded: rows are written only
   until the stabilization decision for a given `(digest, epoch, state)` is durable, so a permanently
   exhausted rig stops growing. This is not a retry budget and not a new duty window.
3. **Planning duty** (`frontier-planning`, binding `currentHolder`). The genuine current Lead claims it
   and records exactly one typed disposition through `POST /api/coordinator/coordination-frontier-plan`:
   `plan-proposal`, `frontier-complete` or `frontier-blocked`. Every citation must name a scope ref in
   the duty's frozen snapshot with a matching sha256 digest; a malformed, empty or unknown citation is
   refused, and an empty frozen scope admits no proposal at all. Prose, silence and an empty frontier
   are not completion.
4. **Operator admission duty** (`frontier-admission`, binding `currentOperator`). The proposer (Lead)
   and the admitter (Operator) differ and neither is the implementing worker. The Operator registers
   every cited candidate through the existing supported package admission, or refuses the whole proposal
   once with an attributed reason. Partial admission, a non-product work class and any divergence from
   the frozen proposal resources or return contract are refused. The proposal's own verified scope
   citations are bound into the admitted contract.
5. **Operator confirmation duty** (`frontier-confirmation`, binding `currentOperator`). A
   `frontier-complete` is Lead self-attestation until the genuine current Operator confirms that exact
   completion digest **on that exact planning duty** as a duty. A confirmation is bound to the planning
   duty that recorded it, by `planningQueueId` in the lookup and in the confirmation duty's semantic
   key, so it never carries across a reopen: after a reopen the successor planning duty has a distinct
   id and re-recording a byte-identical mapping still requires a fresh Operator confirmation. Every
   scope item must map to an **accepted product package whose contract cites that exact scope ref**, or
   to an explicit owner-attributed deferral. Legacy and administrative accepted work can never support
   completeness.
6. **Reopen.** A blocked, completed or declined disposition is never a silent permanent stall. The
   accountable boundary, its recorded unblock condition and the owning Operator are written durably once,
   and only the genuine current Operator recording the discharged disposition reopens planning, as a
   genuinely distinct successor duty.
7. **Existing chain.** A registered, unplanned product package now matches the existing
   `centralLifecyclePass` unplanned loop, so the unchanged materialization duty, plan write, qualified
   dispatch and `accept()` path carry the work forward.

## Operator contract

- **Scope sources.** `plan.scopeSources: [{ref, digest}]` is set only by the genuine current Operator
  through supported plan configuration. The runtime snapshots refs and digests and never reads or
  interprets their contents. A prepared snapshot is not permission.
- **Work class.** `PackageContract.workClass` is set at Operator registration. Absent means a legacy
  contract whose frozen bytes are never rewritten and which never supports a completeness finding.
- **Scope citations.** `PackageContract.scopeCitations` is bound at admission from the cited proposal.
  Absent means a package that can never support a completeness mapping.
- **Stabilization.** `plan.frontierPlanning = {stabilizationObservations, stabilizationMs}`.

## Invariants this preserves

- No package, plan task, qualification, acceptance, delivery or dispatch status is ever invented.
- **Missing scope has an accountable boundary.** With no configured `scopeSources`, `frontier-complete`
  is refused and `scope-source-missing` is the only acceptable `frontier-blocked` boundary.
- **Source drift fences stale completion.** Completeness is bound to the current `frontierDigest` and
  `scopeSourcesDigest`; re-measuring a source makes the record stale and issues a fresh duty.
- **Every record path passes the shared Act facet.** `recordFrontierPlan`, `admitFrontierProposal`,
  `recordFrontierConfirmation` and the Operator-owned reopen all consult `seam.actAllowed` in the same
  transaction as the write, so a recorded disposition cannot outlive the authority that issued the duty.
- **Replays are idempotent and monotone.** An exact replay returns the stored receipt before expiry is
  considered, and the Complete facet is evaluated against the duty's own frozen scope sources.
- Protected unknowns, quiescence, locks and immutable history are untouched. No facet mutates an outbox
  row, an UNKNOWN notice stays UNKNOWN, an expired duty is never extended, and accepted or dormant-backup
  task bytes are never rewritten.

## D10 integration seam

All planning logic lives behind `FrontierPlanningSeam`. The touch to the shared finite-duty mechanism:

| Location | Change |
|---|---|
| `lifecycleDuty` parameter type | three frontier kinds added to the kind union |
| `lifecycleDuty` action and instruction blocks | the three new action names and duty texts |
| `lifecycleControlCompleted` | three explicit branches before the recovery fallthrough |
| `validLifecycleControlFrame` | the matching Act-facet branches before the acceptance/recovery fallthrough |
| `centralLifecyclePass` | one `pass(rigId)` call |

Rebase onto the facet refactor:

1. `DutyKind` union gains `frontier-planning`, `frontier-admission`, `frontier-confirmation`.
2. `dutyKinds` gains `{binding:'currentHolder',effectClass:'state-changing'}` for planning and
   `{binding:'currentOperator',effectClass:'state-changing'}` for admission and confirmation.
3. `dutyPostcondition` needs explicit branches **before** its recovery-binding fallthrough.
4. `dutySubjectReady` needs explicit branches **before** its acceptance/recovery fallthrough.
5. `centralLifecyclePass` gains the same single `pass(rigId)` call.
6. `seam.actAllowed` is the one place the record paths consult the shared Act facet. Rebase it onto
   `lifecycleControlActAllowed` / `dutyFacts().act`; never reimplement the gates inside the planner.

Both duties are keyed by a frozen digest (`frontierDigest`, `proposalDigest`, `completionDigest`), never
by plan revision, so a revision bump cannot mint a competing duty. Duties are located by that digest in
`coordinator_operations`, never by predicting the mechanism's queue id. Routing an exhausted or
recipient-protected frontier duty into the shared task-hold intake stays D10's change; the frontier module
records a durable `frontier-boundary-intake` row naming the accountable boundary instead.

## Durable rows

| kind | meaning |
|---|---|
| `frontier-observation` | append-only census row, bounded to the stabilization decision |
| `frontier-plan-disposition` | the Lead's one attributed typed disposition |
| `frontier-admission-disposition` | the Operator's attributed admission or refusal |
| `frontier-confirmation-disposition` | the Operator's independent confirmation of a completion |
| `frontier-reopen` | the Operator's attributed discharge of a blocked or declined disposition |
| `frontier-boundary-intake` | the accountable boundary, unblock condition and owning Operator |