# Frontier planning: exhausted authorized frontier to accountable next work

## The gap this closes

`centralLifecyclePass` emits a materialization duty only for packages that are **already registered**
in `coordinator_packages`. Nothing above registration observes "this rig has run out of authorized
work", and nothing owned the decision "what does this project do next". An empty frontier was
therefore indistinguishable from a finished project, a protected hold, or a planning lapse. This
document and `src/domain/frontier-planning.ts` close that gap. It adds two rows to the lifecycle
duty kind table and no second duty engine.

## Lifecycle

1. **Classification** (`frontier()`), a pure read of durable facts, in strict precedence:
   `ACTIVE` (admitted work ready or in custody) > `AWAITING-ACCEPTANCE` (typed return unauthenticated,
   or owned recovery outstanding) > `PROTECTED-HOLD` (product work held by admission expiry, effect
   debt, quiescence, reservation or an owner boundary) > `MATERIALIZABLE` (registered, unplanned
   product package) > `EXHAUSTED`. Only an `EXHAUSTED` frontier raises the planning obligation.
2. **Stabilization.** `EXHAUSTED` must hold across consecutive durable `frontier-observation` rows
   *and* past a minimum dwell, so a transient gap between acceptance and the next reconcile never
   accumulates into an obligation. Both numbers are genuine Operator plan configuration
   (`plan.frontierPlanning`); the built-in defaults are the review's stated floor of two observations
   and five minutes. This is not a retry budget and not a new duty window: the duty itself reuses
   the shared finite-duty convention unchanged.
3. **Planning duty** (`frontier-planning`, binding `currentHolder`). The genuine current Lead claims
   it and records exactly one typed disposition through
   `POST /api/coordinator/coordination-frontier-plan`:
   - `plan-proposal` — candidate packages, each citing at least one scope ref from the duty's frozen
     snapshot with a matching digest, plus its resources and return contract.
   - `frontier-complete` — every frozen scope item mapped to an **accepted** package or to an explicit
     owner-attributed deferral carrying its authorization reference.
   - `frontier-blocked` — a named accountable boundary and its unblock condition.
   Prose, silence and an empty frontier are not completion.
4. **Operator admission duty** (`frontier-admission`, binding `currentOperator`). The proposer (Lead)
   and the admitter (Operator) differ and neither is the implementing worker. The Operator registers
   every cited candidate through the existing supported package admission, or refuses the whole
   proposal once with an attributed reason. Partial admission is refused.
5. **Existing chain.** A registered, unplanned product package now matches the existing
   `centralLifecyclePass` unplanned loop, so the unchanged materialization duty, plan write, qualified
   dispatch and `accept()` path carry the work forward. Nothing new is created automatically.

## Operator contract

- **Scope sources.** `plan.scopeSources: [{ref, digest}]` is set only by the genuine current Operator
  through supported plan configuration (`POST /api/coordinator/coordination-plan`). The runtime
  snapshots refs and digests, passes them to the Lead, and never reads or interprets their contents.
  A prepared snapshot is not permission.
- **Work class.** `PackageContract.workClass ∈ {product, recovery, administrative, inquiry}` is set
  at Operator registration. Absent means a legacy contract: its frozen bytes are never rewritten, an
  unplanned non-backup legacy package still receives its existing materialization duty, and it never
  supports a completeness finding.
- **Stabilization.** `plan.frontierPlanning = {stabilizationObservations, stabilizationMs}`.

## Invariants this preserves

- No package, plan task, qualification, acceptance, delivery or dispatch status is ever invented. The
  runtime issues obligations and records receipts; the Operator admits, the Lead decides, the worker
  executes, the Lead accepts.
- **Missing scope has an accountable boundary.** With no configured `scopeSources`, `frontier-complete`
  is refused and `scope-source-missing` is the only acceptable `frontier-blocked` boundary. A project
  is never declared complete against an empty scope.
- **Source drift fences stale completion.** `frontier-complete` is bound to the current
  `frontierDigest` and `scopeSourcesDigest`. When the Operator re-measures a source, the digest moves,
  the record is stale, and a fresh duty is issued under a new digest.
- **Protected unknowns, quiescence, locks and immutable history are untouched.** No facet mutates an
  outbox row, an UNKNOWN notice stays UNKNOWN and is never retried, an expired duty is never extended,
  and accepted or dormant-backup task bytes are never rewritten.
- A `frontier` result row is emitted by every reconcile: `stabilizing`, `pending-native-frontier-planning`,
  `held`, `frontier-complete`, `pending-native-frontier-admission`, `frontier-admission-complete`,
  `frontier-admission-declined` or `frontier-admission-incomplete`. Never `frontier-complete` is
  inferred from an empty frontier alone.

## D10 integration seam

All planning logic lives in `src/domain/frontier-planning.ts` behind `FrontierPlanningSeam`. The
entire touch to the shared finite-duty mechanism is six lines in
`src/domain/coordination-recovery-service.ts`:

| Location | Change |
|---|---|
| `lifecycleDuty` parameter type | `'frontier-planning'\|'frontier-admission'` added to the kind union |
| `lifecycleDuty` action ternary | the two new action names |
| `lifecycleDuty` follow-up block | the two duty instruction texts |
| `lifecycleControlCompleted` | `if(r.kind===PLANNING_DUTY_KIND) …` / `if(r.kind===ADMISSION_DUTY_KIND) …` before the recovery fallthrough |
| `validLifecycleControlFrame` | the matching Act-facet branches before the acceptance/recovery fallthrough |
| `centralLifecyclePass` | `result.push(...this.frontierPlanning().pass(rigId));` |

When the shared duty mechanism is replaced by its facet refactor, rebase exactly these five kinds:

1. `DutyKind` union gains `'frontier-planning'|'frontier-admission'`.
2. `dutyKinds` gains
   `{'frontier-planning':{binding:'currentHolder',effectClass:'state-changing'}, 'frontier-admission':{binding:'currentOperator',effectClass:'state-changing'}}`.
   The shared facets then pick both kinds up automatically.
3. `dutyPostcondition` needs explicit branches **before** its recovery-binding fallthrough:
   `planPostcondition(rigId, r)` and `admissionPostcondition(rigId, r)`.
4. `dutySubjectReady` needs explicit branches **before** its acceptance/recovery fallthrough:
   `planActAllowed(rigId, r)` and `admissionActAllowed(rigId, r)`.
5. `centralLifecyclePass` gains the same single `pass(rigId)` call.

The semantic key passed for both duties is the frozen digest (`frontierDigest` and `proposalDigest`),
never the plan revision, so a plan revision bump cannot mint a competing duty for the same frontier.

## Durable rows

| kind | meaning |
|---|---|
| `frontier-observation` | append-only census row per supervise observation |
| `frontier-plan-disposition` | the Lead's one attributed typed disposition |
| `frontier-admission-disposition` | the Operator's attributed admission or refusal |

`coordinator-lifecycle-control` rows of kind `frontier-planning` / `frontier-admission` are the duties
themselves and flow through the unchanged shared claim, send, act and close gates.