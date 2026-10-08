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
export interface NativeRecoveryContinuationRuntime {
  observeRecoveredIncarnation(session: string): Promise<NativeRecoveryObservation | null>;
  withRecoveredIncarnation<T>(observation: NativeRecoveryObservation, send: () => Promise<T>): Promise<NativeRecoveryGuardResult<T>>;
}
