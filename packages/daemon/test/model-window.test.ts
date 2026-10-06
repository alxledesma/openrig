import { describe, expect, it } from 'vitest';
import { classifyModelChange, parseNativeModelWindow, parseNativeModelCatalog, type NativeModelWindow } from '../src/domain/model-window.js';
const model = (contextWindow: number): NativeModelWindow => ({ provider: 'native-provider', id: 'actual-model', contextWindow });
describe('native model-window classification', () => {
  it.each([[100, 100, 'known_same_or_larger'], [100, 200, 'known_same_or_larger'], [200, 100, 'known_downsize']] as const)(
    'classifies actual windows %s -> %s', (from, to, kind) => expect(classifyModelChange(model(from), model(to)).kind).toBe(kind));
  it.each([null, undefined, model(0), model(-1), model(Infinity), model(NaN), model(1.5), model(Number.MAX_SAFE_INTEGER + 1),
    { ...model(100), provider: '' }, { ...model(100), id: ' ' }, { ...model(100), maxTokens: 101 }])(
    'preserves unknown or invalid metadata on both sides', invalid => {
      expect(classifyModelChange(invalid, model(100))).toEqual({ kind: 'unknown', reason: 'current-model-window-unknown' });
      expect(classifyModelChange(model(100), invalid)).toEqual({ kind: 'unknown', reason: 'target-model-window-unknown' });
    });
  it('validates native RPC numbers and strips unrelated fields', () => {
    expect(parseNativeModelWindow({ ...model(100), contextWindow: '100' })).toBeNull();
    expect(parseNativeModelWindow({ ...model(100), maxTokens: Infinity })).toBeNull();
    expect(parseNativeModelWindow({ ...model(100), extra: 'excluded' })).toEqual(model(100));
    expect(parseNativeModelWindow([])).toBeNull();
  });
  it('returns a bounded detached projection without exporting extra native fields', () => {
    const current = { ...model(100), secret: 'excluded', maxTokens: 10 }, target = model(50);
    const decision = classifyModelChange(current, target);
    expect(decision).toEqual({ kind: 'known_downsize', current: modelWithTokens(), target: model(50) });
    current.contextWindow = 1000; target.contextWindow = 1000;
    if (decision.kind !== 'unknown') { expect(decision.current.contextWindow).toBe(100); expect(decision.target.contextWindow).toBe(50); }
    function modelWithTokens() { return { ...model(100), maxTokens: 10 }; }
  });
});

it("bounds native string and catalog sizes without invented defaults", () => {
 expect(parseNativeModelWindow({...model(100),provider:"x".repeat(257)})).toBeNull();
 expect(parseNativeModelWindow({...model(100),id:"x".repeat(257)})).toBeNull();
 expect(parseNativeModelCatalog(Array.from({length:513},()=>model(100)))).toBeNull();
 expect(parseNativeModelCatalog([{...model(100),secret:"excluded"}])).toEqual([model(100)]);
});
