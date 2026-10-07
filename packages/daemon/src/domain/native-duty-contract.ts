/** Shared wire contract for opt-in, launch-bound software duties.
 * A registration is not coordinator authority. The existing authenticated
 * coordinator API remains the final fence on every effect.
 */
export type NativeDutyPhase =
  | "awaiting-native-proof" | "watching" | "prepared" | "effect-in-flight"
  | "receipt-confirmed" | "held" | "uncertainty-held" | "stopped";

export interface NativeDutyActor { session: string; generation: string }

/** Granted through authenticated Operator transport; never from a heartbeat.
 * All deadlines and lease limits come from the actual approved work scope.
 */
export interface NativeDutyScope {
  scopeId: string;
  nodeId: string;
  sessionName: string;
  generation: string;
  runtime: "codex" | "pi";
  rigId: string;
  configurationDigest: string;
  validUntil: number;
  maxLeaseMs: number;
  kind: "holder-continuation";
}

export interface NativeDutyGrant {
  scope: NativeDutyScope;
  scopeDigest: string;
  grantedBy: NativeDutyActor;
  grantedAt: number;
  revokedAt: number | null;
}

/** Independent server observation, not fields accepted as caller assertions.
 * Native runtime proof must include the supervisor's actual process ancestry.
 */
export interface NativeDutyProof {
  nodeId: string;
  sessionName: string;
  generation: string;
  runtime: "codex" | "pi";
  launchId: string;
  supervisorPid: number;
  configurationDigest: string;
  fingerprint: string;
  observedAt: number;
  nativePresent: boolean;
  supervisorIsNativeAncestor: boolean;
  lifecycleReserved: boolean;
}

export interface NativeDutyRegisterRequest {
  scopeId: string;
  launchId: string;
  supervisorPid: number;
}

/** Exact existing coordinator resume-owned body. No secret or arbitrary command. */
export interface NativeDutyResumeRequest {
  rigId: string;
  operationId: string;
  leaseMs: number;
  expectedEpoch: number;
  expectedObligationsDigest: string;
}

export interface NativeDutyPrepareRequest {
  registrationId: string;
  request: NativeDutyResumeRequest;
}

export interface NativeDutyOperationRequest {
  registrationId: string;
  operationId: string;
}

export interface NativeDutyIntent {
  operationId: string;
  request: NativeDutyResumeRequest;
  bodyDigest: string;
  preparedAt: number;
  phase: "prepared" | "effect-in-flight" | "receipt-confirmed" | "uncertainty-held";
}

export interface NativeDutyStatus {
  registrationId: string;
  scope: NativeDutyScope;
  scopeDigest: string;
  launchId: string;
  phase: NativeDutyPhase;
  lastHeartbeatAt: number;
  observerDeadline: number;
  reason: string | null;
  /** Retained after stop/retirement. A missing receipt is never a retry grant. */
  intent: NativeDutyIntent | null;
}

export const NATIVE_DUTY_API = "/api/native-duty";

export function nativeDutyUnresolved(intent: NativeDutyIntent | null): boolean {
  return intent !== null && intent.phase !== "receipt-confirmed";
}

/** Scheduling cannot enlarge the explicitly granted effect window. */
export function nativeDutyLeaseMs(scope: NativeDutyScope, now: number): number {
  if (!Number.isSafeInteger(now) || !Number.isSafeInteger(scope.validUntil)
    || !Number.isSafeInteger(scope.maxLeaseMs) || scope.maxLeaseMs <= 0) return 0;
  return Math.max(0, Math.min(scope.maxLeaseMs, scope.validUntil - now));
}
