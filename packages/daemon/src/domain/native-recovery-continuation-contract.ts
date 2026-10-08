/** Successful native recovery is evidence, never authority or work acceptance. */
export interface NativeRecoveryCompletion {
  schema: "native-recovery-completion.v1";
  recoveryId: string;
  producer: "pi-runner-rehost" | "pi-detached-resume" | "codex-rehost" | "codex-stopped-recovery" | "codex-detached-resume";
  rigId: string; nodeId: string; sessionId: string; sessionName: string;
  generation: string; runtime: "pi" | "codex";
  nativeIdentityHash: string; configurationDigest: string; completedAt: number;
  source: { ref: string; digest: string };
  incarnation: {
    key: string;
    runtimeLaunchId?: string;
    supervisorLaunchId?: string;
    native: { pid: number; startFingerprint: string };
    supervisor?: { pid: number; startFingerprint: string };
  };
  custodyPreserved: true; generationUnchanged: true;
}
export interface NativeRecoveryObservation {
  completion: NativeRecoveryCompletion;
  observedAt: number;
}
export type NativeRecoveryGuardResult<T> =
  | { state: "performed"; value: T }
  | { state: "held" | "invalid"; reason: string };
/** Native quiescence is observation only, never a task outcome or recovery receipt. */
export interface NativeSettledObservation {
  schema: "native-settled-observation.v1";
  rigId: string; nodeId: string; sessionId: string; sessionName: string;
  generation: string; runtime: "pi" | "codex";
  nativeIdentityHash: string; configurationDigest: string;
  incarnation: NativeRecoveryCompletion["incarnation"];
  lastEntryId: string;
  quiescenceObservedAt: number;
  observedAt: number;
}
export interface NativeRecoveryContinuationRuntime {
  observeSettledClaimant?(session: string): Promise<NativeSettledObservation | null>;
  withSettledClaimant?<T>(observation: NativeSettledObservation, send: (current: NativeSettledObservation) => Promise<T>): Promise<NativeRecoveryGuardResult<T>>;
  observeRecoveredIncarnation(session: string): Promise<NativeRecoveryObservation | null>;
  withRecoveredIncarnation<T>(observation: NativeRecoveryObservation, send: () => Promise<T>): Promise<NativeRecoveryGuardResult<T>>;
}
