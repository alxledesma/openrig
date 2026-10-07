/** Readiness is additional negative evidence, never identity, admission or authority. */
export interface DispatchRuntimeRequirements {
  /** Explicit native provider/model binding, independent of a static launcher default. */
  model?: string;
  minimumContextTokens?: number;
}
export interface DispatchRuntimeHold {
  reason: 'runtime-not-ready';
  code: string;
  launchId: string;
  observedAt: string;
}
const object = (v: unknown): v is Record<string, any> => !!v && typeof v === 'object' && !Array.isArray(v);
const failures = new Set(['model_error', 'model_change_failed', 'compaction_failed', 'compaction_aborted', 'compaction_no_result']);
export function validRuntimeRequirements(v: unknown): boolean {
  return v === undefined || object(v) && Object.keys(v).every(k => k === 'model' || k === 'minimumContextTokens')
    && (v.model === undefined || typeof v.model === 'string' && /^[^\s/]+\/\S+$/.test(v.model) && v.model.length <= 512)
    && (v.minimumContextTokens === undefined || Number.isSafeInteger(v.minimumContextTokens) && v.minimumContextTokens >= 0);
}

/** Only a matching native launch can report a persistent unresolved failure. Aging an
 * observation never resolves that failure; the producer must record actual recovery. */
export function assessPiDispatchReadiness(raw: unknown, generation: string, now: number,
  requirements?: DispatchRuntimeRequirements): DispatchRuntimeHold | null {
  if (!object(raw) || raw.ready !== true || raw.exited || !object(raw.quiescence) || !object(raw.runtimeReadiness)) return null;
  const q = raw.quiescence, r = raw.runtimeReadiness;
  const observed = (v: unknown) => typeof v === 'string' && Number.isFinite(Date.parse(v)) && Date.parse(v) <= now;
  if (!generation || typeof raw.launchId !== 'string' || !raw.launchId || typeof raw.sessionFile !== 'string' || !raw.sessionFile
    || q.generation !== generation || r.generation !== generation || q.launchId !== raw.launchId || r.launchId !== raw.launchId
    || q.sessionFile !== raw.sessionFile || r.sessionFile !== raw.sessionFile || q.lastEntryId !== raw.lastEntryId
    || !observed(q.observedAt) || !observed(r.observedAt)) return null;
  const hold = (code: string): DispatchRuntimeHold => ({ reason: 'runtime-not-ready', code, launchId: raw.launchId, observedAt: r.observedAt });
  // The latest sidecar retains failures until native recovery. A delayed refresh or
  // stalled runner must not turn an unresolved failure into an eligible worker.
  if (Array.isArray(r.failures)) {
    const failure = r.failures.find((f: unknown) => object(f) && failures.has(f.code)
      && typeof f.observedAt === 'string' && Number.isFinite(Date.parse(f.observedAt)) && Date.parse(f.observedAt) <= Date.parse(r.observedAt));
    if (failure) return hold(failure.code);
  }
  // Model and context are likewise scoped to the latest unchanged native state.
  // The producer invalidates context when input, cursor or model changes. An old
  // negative observation is not positive recovery; native idle/identity freshness
  // remains enforced independently by the coordinator.
  if (!object(r.model) || !object(raw.model) || r.model.provider !== raw.model.provider || r.model.id !== raw.model.id) return null;
  if (requirements?.model && `${r.model.provider}/${r.model.id}` !== requirements.model) return hold('native-model-mismatch');
  const c = r.context;
  if (object(c) && c.source === 'assistant_usage' && Number.isSafeInteger(c.usedTokens) && c.usedTokens >= 0
    && Number.isSafeInteger(c.remainingTokens) && Number.isFinite(Date.parse(c.observedAt)) && Date.parse(c.observedAt) <= Date.parse(r.observedAt)
    && Number.isFinite(r.model.contextWindow) && r.model.contextWindow > 0
    && c.remainingTokens === r.model.contextWindow - c.usedTokens
    && c.remainingTokens < (requirements?.minimumContextTokens ?? 1)) return hold('context-headroom');
  return null;
}
