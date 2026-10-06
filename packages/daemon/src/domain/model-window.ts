/** Native runner metadata only. Configured model pins are not serving-model evidence. */
export interface NativeModelWindow {
  provider: string;
  id: string;
  contextWindow: number;
  maxTokens?: number;
}
export type ModelWindowChange =
  | { kind: 'unknown'; reason: 'current-model-window-unknown' | 'target-model-window-unknown' }
  | { kind: 'known_same_or_larger' | 'known_downsize'; current: NativeModelWindow; target: NativeModelWindow };

function valid(value: NativeModelWindow | null | undefined): value is NativeModelWindow {
  return !!value && typeof value.provider === 'string' && value.provider.trim().length > 0 && value.provider.length <= 256
    && typeof value.id === 'string' && value.id.trim().length > 0 && value.id.length <= 256
    && Number.isSafeInteger(value.contextWindow) && value.contextWindow > 0
    && (value.maxTokens === undefined || (Number.isSafeInteger(value.maxTokens)
      && value.maxTokens > 0 && value.maxTokens <= value.contextWindow));
}

/** Bounded allowlisted projection of untrusted native RPC metadata. */
export function parseNativeModelWindow(value: unknown): NativeModelWindow | null {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !valid(value as NativeModelWindow)) return null;
  const v = value as NativeModelWindow;
  return { provider: v.provider, id: v.id, contextWindow: v.contextWindow,
    ...(v.maxTokens === undefined ? {} : { maxTokens: v.maxTokens }) };
}

/** Classifies limits; grants no launch, replacement, queue or recovery authority.
 * No inferred defaults or token reserve: unknown evidence remains unknown. */
export function classifyModelChange(
  current: NativeModelWindow | null | undefined,
  target: NativeModelWindow | null | undefined,
): ModelWindowChange {
  if (!valid(current)) return { kind: 'unknown', reason: 'current-model-window-unknown' };
  if (!valid(target)) return { kind: 'unknown', reason: 'target-model-window-unknown' };
  const project = (v: NativeModelWindow): NativeModelWindow => ({
    provider: v.provider, id: v.id, contextWindow: v.contextWindow,
    ...(v.maxTokens === undefined ? {} : { maxTokens: v.maxTokens }),
  });
  return { kind: target.contextWindow < current.contextWindow ? 'known_downsize' : 'known_same_or_larger',
    current: project(current), target: project(target) };
}

/** Reject oversized catalogs rather than silently truncate native capability evidence. */
export function parseNativeModelCatalog(value: unknown): NativeModelWindow[] | null {
  if (!Array.isArray(value) || value.length > 512) return null;
  const models = value.map(parseNativeModelWindow).filter((m): m is NativeModelWindow => m !== null);
  return models.length ? models : null;
}
