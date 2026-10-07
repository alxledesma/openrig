import { describe, expect, it } from 'vitest';
import { assessPiDispatchReadiness, validRuntimeRequirements } from '../src/domain/dispatch-runtime-readiness.js';

describe('SM06 native dispatch readiness', () => {
  const now = Date.parse('2026-10-07T20:00:00Z');
  const at = new Date(now).toISOString();
  const model = { provider: 'provider', id: 'model', contextWindow: 1000, maxTokens: 100 };
  const state = () => ({ ready: true, launchId: 'launch', sessionFile: '/session', lastEntryId: 'leaf', model,
    quiescence: { launchId: 'launch', generation: 'gen', sessionFile: '/session', lastEntryId: 'leaf', observedAt: at },
    runtimeReadiness: { launchId: 'launch', generation: 'gen', sessionFile: '/session', observedAt: at, model,
      failures: [{ code: 'compaction_failed', observedAt: new Date(now - 60000).toISOString() }] } });
  it('holds an unresolved native failure despite fresh idle observations, without expiring the failure', () => {
    expect(assessPiDispatchReadiness(state(), 'gen', now)).toMatchObject({ reason: 'runtime-not-ready', code: 'compaction_failed' });
    for (const delay of [3001, 4999, 5000, 60000])
      expect(assessPiDispatchReadiness(state(), 'gen', now + delay)?.code).toBe('compaction_failed');
    const s = state(); s.runtimeReadiness.failures = [];
    expect(assessPiDispatchReadiness(s, 'gen', now)).toBeNull();
  });
  it('does not apply old-generation, old-launch or future observations to a current owner', () => {
    expect(assessPiDispatchReadiness(state(), 'new-gen', now)).toBeNull();
    const s = state(); s.runtimeReadiness.launchId = 'old';
    expect(assessPiDispatchReadiness(s, 'gen', now)).toBeNull();
    const healthy = state(); healthy.runtimeReadiness.failures = [];
    expect(assessPiDispatchReadiness(healthy, 'gen', now + 60000, { model: 'provider/other' })?.code).toBe('native-model-mismatch');
    expect(assessPiDispatchReadiness(state(), 'gen', now - 1)).toBeNull();
    expect(assessPiDispatchReadiness({}, 'gen', now)).toBeNull();
  });
  it('uses an explicitly admitted native model rather than a static launcher default', () => {
    const s = state(); s.runtimeReadiness.failures = [];
    expect(assessPiDispatchReadiness(s, 'gen', now)).toBeNull();
    expect(assessPiDispatchReadiness(s, 'gen', now, { model: 'provider/other' })?.code).toBe('native-model-mismatch');
    expect(assessPiDispatchReadiness(s, 'gen', now, { model: 'provider/model' })).toBeNull();
  });
  it('holds a native context snapshot below the admitted floor; absent context remains unknown', () => {
    const s = state(); s.runtimeReadiness.failures = [];
    const snapshot = { ...s, runtimeReadiness: { ...s.runtimeReadiness,
      context: { usedTokens: 990, remainingTokens: 10, observedAt: at, source: 'assistant_usage' } } };
    expect(assessPiDispatchReadiness(snapshot, 'gen', now, { minimumContextTokens: 20 })?.code).toBe('context-headroom');
    expect(assessPiDispatchReadiness(snapshot, 'gen', now + 60000, { minimumContextTokens: 20 })?.code).toBe('context-headroom');
    expect(assessPiDispatchReadiness(snapshot, 'gen', now, { minimumContextTokens: 10 })).toBeNull();
    expect(assessPiDispatchReadiness(s, 'gen', now, { minimumContextTokens: 20 })).toBeNull();
    snapshot.runtimeReadiness.context.remainingTokens = 0; // Inconsistent telemetry is not a fact.
    expect(assessPiDispatchReadiness(snapshot, 'gen', now, { minimumContextTokens: 20 })).toBeNull();
  });
  it('validates finite explicit requirements without accepting extra policy fields', () => {
    expect(validRuntimeRequirements({ model: 'provider/model', minimumContextTokens: 0 })).toBe(true);
    for (const bad of [{ model: 'model' }, { minimumContextTokens: -1 }, { minimumContextTokens: Infinity }, { override: true }])
      expect(validRuntimeRequirements(bad)).toBe(false);
  });
});
