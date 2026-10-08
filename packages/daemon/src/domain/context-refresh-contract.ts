/** Context refresh is a separate finite effect scope, never a holder-renewal grant. */
export interface ContextRefreshActor { session: string; generation: string }
export interface ContextRefreshTarget {
  nodeId: string;
  sessionName: string;
  generation: string;
  runtime: "codex" | "pi";
  nativeId: string;
  configurationDigest: string;
}
export interface ContextRefreshPolicy {
  preparePercent: number;
  rotatePercent: number;
  successfulCompactions: number;
  pollMs: number;
  maxUsageAgeMs: number;
}
export const DEFAULT_CONTEXT_REFRESH_POLICY: Readonly<ContextRefreshPolicy> = Object.freeze({
  preparePercent: 75, rotatePercent: 85, successfulCompactions: 2,
  pollMs: 120_000, maxUsageAgeMs: 120_000,
});
export interface ContextRefreshGrant {
  grantId: string;
  kind: "context-refresh";
  executor: ContextRefreshActor & { nodeId: string; launchId: string; configurationDigest: string };
  targets: ContextRefreshTarget[];
  policy: ContextRefreshPolicy;
  policyRevision: string;
  validUntil: number;
  validator: ContextRefreshActor;
  recoveryOwner: ContextRefreshActor;
}
export type ContextRefreshHold =
  | "identity-unavailable" | "binding-changed" | "native-proof-unavailable"
  | "runtime-unsupported" | "runtime-not-ready" | "usage-unavailable" | "usage-stale"
  | "compaction-evidence-invalid" | "busy" | "activity-unknown" | "activity-stale"
  | "authority-transfer-required" | "effects-unresolved" | "lifecycle-reserved"
  | "typing-guard-enabled" | "checkpoint-required" | "checkpoint-changed"
  | "scope-ended" | "operator-self-refresh" | "attempt-unresolved";
/** CR2 supplies daemon-observed facts. No caller may submit these as native proof.
 * Raw native timestamps/cursors remain unchanged on ordinary observation refresh. */
export interface ContextRefreshObservation {
  identity: {
    nodeId: string; sessionName: string; generation: string | null;
    runtime: string | null; nativeId: string | null; configurationDigest: string | null;
  };
  observedAt: number;
  capability: "codex-reserved-fresh" | "pi-reserved-fresh" | "unsupported";
  native: { verified: boolean; observedAt: number; launchId: string | null; fingerprint: string | null };
  activity: { value: "idle" | "busy" | "unknown"; observedAt: number };
  usage: { usedPercent: number; observedAt: number; source: string; cursor: string } | null;
  compactions: { count: number; observedAt: number; source: string; cursor: string } | null;
  /** Negative readiness/custody facts persist until their actual source resolves them. */
  holds: ContextRefreshHold[];
}
export type ContextRefreshPhase = "watching" | "preparation-needed" | "checkpoint-requested"
  | "checkpoint-ready" | "prerequisite-held" | "reserved" | "replacement-started"
  | "committed-awaiting-acceptance" | "refreshed" | "uncertainty-held" | "scope-ended";
export interface ContextRefreshDecision {
  nodeId: string;
  phase: ContextRefreshPhase;
  action: "observe" | "request-checkpoint" | "reserve" | "reconcile" | "none";
  holds: ContextRefreshHold[];
  prepareThreshold: boolean;
  rotateThreshold: boolean;
  baselineCompactions: number | null;
  compactionsSinceBaseline: number | null;
  attemptId: string | null;
}
/** Server-verified target-authored checkpoint, derived from immutable actual evidence.
 * This is an injected receipt interface, never an API body accepted on trust. */
export interface ContextRefreshCheckpoint {
  target: ContextRefreshTarget;
  checkpointId: string;
  checkpointHash: string;
  queueDigest: string;
  authoredBy: ContextRefreshActor;
  outstandingEffects: 0;
}
export interface ContextRefreshAttempt {
  attemptId: string;
  grantId: string;
  target: ContextRefreshTarget;
  operationId: string;
  reservationId: string;
  checkpoint: ContextRefreshCheckpoint;
  phase: "prepared" | "effect-in-flight" | "reserved" | "replacement-started"
    | "committed-awaiting-acceptance" | "uncertainty-held" | "refreshed" | "cancelled-before-effect";
}
/** Only the existing reservation service's durable evidence can settle an attempt.
 * prepared/absent is not proof a request was never sent. */
export interface ContextRefreshReservationEvidence {
  reservationId: string;
  operationId: string;
  targetNodeId: string;
  predecessorGeneration: string;
  predecessorNativeId: string;
  checkpointHash: string;
  state: "reserved" | "started" | "committed" | "released";
  successor: { generation: string; nativeId: string; configurationDigest: string } | null;
  successorVerified: boolean;
  custodyVerified: boolean;
  successorAck: ContextRefreshActor | null;
  independentAcceptance: ContextRefreshActor | null;
  releasedBy: ContextRefreshActor | null;
  releaseMode: "accepted_successor" | "cancel_before_replacement" | null;
  receiptDigest: string;
}
