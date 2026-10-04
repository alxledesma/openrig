# Authenticated protection inspection

`rig queue inspect-recovery <qid> --source-facts-hash <sha256> --operation-id <id> --authorization-id <own-claimed-qid> --json` calls `POST /api/queue/:qid/inspect-recovery` with exactly `{sourceFactsHash,operationId,authorizationId}`. The existing daemon terminal bearer and actual managed caller/current generation are required; there is no caller-provided watchdog identity or failure code.

Before invocation, a current attributed author creates an ordinary queue authorization directed to the actual Kernel Operator. The current Operator may issue this authorization itself under standing recovery authority and must actually claim it through transport. No fresh human approval is implied. Supported Owner/human creation requires transport provenance; a claimed-only body is not promoted to genuine Owner authorization. A managed author must still have its recorded mint generation. Authorization stays its own claimed obligation; the inspection never disposes it.

Authorization body has exactly these keys:

```json
{
  "kind": "queue-recovery-inspection-authorization.v1",
  "purpose": "inspect-protected-obligation",
  "qitemId": "EXACT_EXISTING_OBLIGATION",
  "sourceFactsHash": "64_LOWERCASE_HEX",
  "operationId": "DISTINCT_OPERATION",
  "operatorGeneration": "ACTUAL_CURRENT_MANAGED_GENERATION",
  "expiresAt": 0
}
```

`expiresAt` is finite UTC milliseconds, strictly future and at most 20 minutes after the actual recorded authorization creation. Earlier queue `expiresAt` also applies. Invalid/future-created/expired or unclaimed authorization refuses, including replay. Operation IDs use `[A-Za-z0-9._-]`, length1–160. No permission to bypass a protected original is granted.

Source hash is SHA256 of UTF-8 `JSON.stringify([qitemId, sourceSession, destinationSession, SHA256(body), state, tsUpdated, claimedAt, closureTarget])` from the exact full current queue record, including null fields. This is the existing shared `sourceFacts` function, not a full-row or sorted-object hash. Retain separate original raw rows/resources/reservations for preservation comparisons.

The server serializes through the actual destination's existing delivery guard, without granting an input/lifecycle lease or sending to it. After any wait, an immediate SQLite transaction verifies actual current Operator, authorization claim/mint provenance, exact nonterminal source, full target node/session/occupant/pane tuple and freshly re-read real reservation/typing protection. A genuine active reservation or typing guard produces a new distinct server-authored `reconcile-refused-wake-ladder` parent with its finite60-second accountability deadline and generation-tagged intent through the same existing detector. Unprotected obligations return `unprotected` and no parent. Missing/stale/changed facts refuse; no synthetic transport failure or destructive actor stop is needed.

A persisted operation binding is in the typed parent/inspection receipt. Same-operation replay must match exact authorization/current source/current target/full protection fingerprints and the canonical watchdog→Operator parent plus attributed generation-tagged intent. It returns the original committed parent regardless of terminal state; no evidence timestamp remint, old indeterminate redrive, recursive child or original work mutation. Conflict refuses. Parent+intent are atomic; external delivery runs only after releasing the target lock. Receipt separates commitment, delivery attempt/error and unverified pickup; none implies native consumption or accepted completion.

The existing deadline scanner later produces its one correlated overdue child if the new parent remains genuinely unclaimed. This is the existing requested60-second deadline plus at-most60-second scan/execution, not a native pickup SLA. Original protected93 reservation/claimed4f155 notice are never modified by inspection. Actual native live proof, source integration and deployment remain separate from source tests.
