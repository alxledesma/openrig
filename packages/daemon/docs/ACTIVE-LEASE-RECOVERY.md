# Admitted automatic coordinator lease recovery

An expired lease is a dispatch fence, not an election. The prior native observer created an Operator recovery item but required its terminal completion before unavailable-owner transfer; an expired active, still-present holder could not renew. The shared observer now refreshes real native availability before supervision.

## Two distinct supported recovery paths

Existing `allowIdlePeerTransfer` admits a bounded unchanged-progress transfer only when both current occupants have fresh verified deciding-rung idle evidence. Positive native absence excludes this idle path: cached activity cannot bypass the unavailable-owner contract.

New strict boolean `allowUnavailablePeerTransfer: true` in a current genuine Operator's immutable coordination plan admits automatic unavailable-owner recovery. Omission/false does not inherit permission. A current generation-bound Operator watchdog, exact plan revision, expired active/recovery epoch and fresh positive generation-bound native absence must all match. A same-rig distinct registered Peer must have fresh verified idle evidence, no other open local/alias custody, current generation and nonexpired current configuration-bound task admissions. The exact canonical baton must still be claimed by the predecessor generation. Uncertain delivery effects, changed epoch/owner/generation, live lease, returning native owner, unknown/stale evidence, busy/reserved Peer or expired admission refuse transfer.

One immediate SQLite transaction moves only the canonical baton to pending Peer reconciliation, increments/fences the epoch, retains all worker bodies/claims/resources and commits a generation-bound Peer wake. Notice creation failure rolls back the entire transfer. The actual Peer claims its notice, reads the exact obligations and acknowledges through the existing API; only this genuine acknowledgment claims the canonical baton and allows admitted frontier dispatch. No synthetic terminal Operator evidence or acknowledgment is created.

Refusals and expired active states create deduplicated real Operator recovery custody with action/reason/deadline and an exact queue return path. Notice and intended-recipient-generation wake commit together; ambiguous effects are not retried. Existing expired-reconciliation recovery notices and idle-stall Peer notices also use committed wakes. The policy avoids a second parallel outage intake when accountable recovery custody already exists.

## Scope and limits

The mechanism is generic for every enrolled rig and explicit current plan. It does not install live policies or inherit qualifications, paid/private routing permission, a fabricated Peer or new product authority. Healthy busy/unknown owners are never automatically replaced or silently granted unlimited lease extensions. They retain a concrete Operator recovery obligation; supported voluntary handoff remains possible. Native absence is limited to the existing positively verified runtime observer's supported harnesses (currently Codex/Claude tmux process census); unsupported harness observations remain unknown and must use independently qualified idle recovery or bounded Operator action. Source fixtures do not prove live automatic recovery; an isolated native takeover/wake/ACK/custody-preservation receipt remains required before production.
