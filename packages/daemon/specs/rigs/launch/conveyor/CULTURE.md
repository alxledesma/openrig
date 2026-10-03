# Conveyor Culture

Conveyor is the small public starter for learning OpenRig workflow motion.
It is intentionally ordinary: one intake lead, one planner, one builder, and
one reviewer moving queued work through a clear handoff path.

## Responsibilities

- Keep every handoff explicit in the queue.
- Treat queue depth as the backpressure signal.
- Prefer small packets that can move from intake to review without extra
  coordination ceremony.
- Use review feedback to improve the next build packet instead of hiding
  defects in chat.
- Close terminal work with honest closure evidence.

## Principles

- The workflow is a teaching rig, not a private release factory.
- Runtime primitives stay generic: queue, workflow, watchdog, project, proof,
  and topology surfaces should all make sense without special background.
- A user should be able to run multiple conveyor packets at once and understand
  why one station has a deeper queue than another.
- If a packet is blocked, the owner records the blocker and target instead of
  silently waiting.

## Operating Notes

- The `conveyor` workflow is the default station pipeline.
- The `basic-loop` workflow is a slower walkthrough for watching one packet
  move end to end.
- The review seat may route follow-up work back to the build seat by ordinary
  queue handoff when rework is needed. The default workflow pass path moves
  review to close.


## OpenRig-managed project intake

Project policy: OpenRig-managed projects use the selected
OpenRig mission/slice, rigor, culture and durable queue. Retire inherited Software
Factory issue authorization and Factory-specific workflow ceremonies; do not
require a second Factory approval for authorized OpenRig work. Preserve product,
security and database constraints, useful tests/validation, independent exact
candidate review, exclusive scopes, actual-risk admission and owner external or
destructive boundaries. Do not disable GitHub server protections.

Before ingestion or migration, reconcile active workers/custody, retain original
guidance, classify inherited rules, and replace only Factory process. Record a
dependency graph, ready packages, checks and return paths; align project guidance
and startup projections. Verify real seat adoption and work return. One project
writer owns migration; do not interrupt writers or change non-OpenRig projects.
Ambiguous substantive rules go to the Architect while other ready work continues.
Consult the project’s own operating policy before migration or dependency recovery.

## Lead-owned dependency recovery

Within the project owner’s approved dependency scope, every Lead has authority and responsibility to acquire, download, install and configure ordinary dependencies/tools required by any worker or reviewer for approved work, without routine owner approval.

When a prerequisite is missing, the Lead records an owned recovery task, performs the setup or assigns a qualified executor, corrects the worker packet, and verifies the worker can run the required command. Use project-local or disposable environments and trusted pinned versions where practical; preserve shared environments and independent review. An ordinary missing dependency or an inherited blanket download ban must not become an ownerless hold. Continue unrelated ready work.

Escalate only an actual credential/access/security/protected-resource/destructive-action boundary, purchase beyond an authorized budget, deployment, or new scope; name its evidence, affected slice, owner and next action. This instruction does not create a broad dependency-install approval gate.

Future work packages must include dependency/tool versions and source, setup location and command, Lead/recovery executor, verification command and expected result, worker resumption/return path, and any concrete boundary. A missing Playwright browser is ordinary dependency recovery under this rule.


## Shared resilience defaults

This starter inherits `builtin:standard`. A blocked seat must return concrete recovery work while independent dependency-ready assignments continue. Kernel Operator owns actual current route/admission/enrollment/observer setup; Lead and Planner own the ready frontier, technical authority and independent review remain explicit. This compact starter has no Peer: selecting and enrolling an actual qualified Peer is an accountable Operator recovery assignment, not an implied role or a reason to park all work. Model assessment may request only bounded admitted recovery; it never grants acceptance. Public/private and paid provider permissions must be configured explicitly from current evidence, never inherited from this template.
