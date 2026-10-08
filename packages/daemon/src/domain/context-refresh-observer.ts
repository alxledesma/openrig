import { performance } from "node:perf_hooks";
import { createHash, type Hash } from "node:crypto";
import type { PiRotationContract } from "./pi-rotation-native-proof.js";
import { closeSync, lstatSync, openSync, readSync, fstatSync, constants } from "node:fs";
import type { ContextRefreshObservation, ContextRefreshHold } from "./context-refresh-contract.js";
import { assessPiDispatchReadiness } from "./dispatch-runtime-readiness.js";

/** These seams are daemon-owned observations, never request-body attestations.
 * Binding supplies the currently observed model/window, not a launcher default. */
export interface ContextRefreshBinding {
  nodeId: string; sessionName: string; generation: string; runtime: "codex" | "pi";
  nativeId: string; configurationDigest: string; model: string; contextWindow: number;
}
export interface ContextRefreshNativeProof {
  nodeId: string; sessionName: string; generation: string; nativeId: string;
  verified: boolean; observedAt: number; launchId: string | null; fingerprint: string | null;
}
export interface ContextRefreshTranscriptChunk {
  bytes: Buffer; size: number; identity: string; revision?: string;
}
export interface ContextRefreshObserverSources {
  binding(nodeId: string): Promise<ContextRefreshBinding | null>;
  /** Cheap daemon DB/config binding check for cache-only work; no native probe. */
  bindingCurrent?(binding: ContextRefreshBinding): boolean;
  native(binding: ContextRefreshBinding): Promise<ContextRefreshNativeProof | null>;
  activity(binding: ContextRefreshBinding): Promise<ContextRefreshObservation["activity"] | null>;
  transcriptPath(binding: ContextRefreshBinding): Promise<string | null>;
  piState(binding: ContextRefreshBinding): Promise<unknown>;
  /** Full daemon-derived kernel/file/header/config proof. Missing on older runners => held. */
  piContract?(binding: ContextRefreshBinding): Promise<PiRotationContract | null>;
  /** Immutable CR1 baseline cursor, scoped to node + generation + FILE. */
  piBaseline?(binding: ContextRefreshBinding): string | null;
  /** Optional filesystem seam for deterministic fixtures. Offset is bytes, never lines. */
  readTranscript?(file: string, offset: number, limit: number): ContextRefreshTranscriptChunk;
}
interface Scan {
  identity: string; revision: string; size: number; offset: number; pending: Buffer; complete: boolean; invalid: boolean;
  metaId: string | null; metaAt: number; lastAt: number; model: string | null;
  count: number; compactAt: number; compactCursor: string;
  usage: ContextRefreshObservation["usage"]; usageModel: string | null; usageWindow: number | null;
}
const object = (v: unknown): v is Record<string, any> => !!v && typeof v === "object" && !Array.isArray(v);
const time = (v: unknown): number => typeof v === "string" ? Date.parse(v) : NaN;
const nonnegative = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) >= 0;

/** Positional reads do not load a whole native history. Symlinks and replacement
 * during a read refuse; append-only growth may be picked up on the next poll. */
export function readContextRefreshTranscript(file: string, offset: number, limit: number): ContextRefreshTranscriptChunk {
  const before = lstatSync(file);
  if (!before.isFile() || before.isSymbolicLink()) throw new Error("native-history-not-regular");
  const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = fstatSync(fd);
    if (stat.ino !== before.ino || stat.dev !== before.dev) throw new Error("native-history-replaced");
    const bytes = Buffer.alloc(Math.min(limit, Math.max(0, stat.size - offset)));
    const length = readSync(fd, bytes, 0, bytes.length, offset);
    const after = fstatSync(fd);
    if (after.ino !== stat.ino || after.dev !== stat.dev || after.size < stat.size) throw new Error("native-history-truncated");
    return { bytes: bytes.subarray(0, length), size: after.size, identity: `${stat.dev}:${stat.ino}`, revision: `${after.size}:${after.mtimeMs}:${after.ctimeMs}` };
  } finally { closeSync(fd); }
}

export interface PiContextRefreshCursor {
  version: 1; sessionFile: string; sessionHeaderId: string; byteLength: number; sha256: string;
  lastEntryId: string; compactions: Array<{ entryId: string; lineSha256: string }>;
}
interface PiScan {
  revision: string; identity: string; size: number; offset: number; pending: Buffer;
  header: string | null; lastEntryId: string; at: number; lastAt: number; invalid: boolean;
  hash: Hash; prefixChecks: Array<{ cursor: PiContextRefreshCursor; hash: Hash }>;
  compactions: PiContextRefreshCursor["compactions"]; ids: Set<string>;
}
interface PiCache { accepted?: PiContextRefreshCursor; revision?: string; identity?: string; at?: number; scan?: PiScan }
export function parsePiContextRefreshCursor(raw: string): PiContextRefreshCursor | null {
  try {
    const c = JSON.parse(raw);
    return c?.version === 1 && typeof c.sessionFile === "string" && typeof c.sessionHeaderId === "string" && c.sessionHeaderId
      && Number.isSafeInteger(c.byteLength) && c.byteLength > 0 && /^[a-f0-9]{64}$/.test(c.sha256)
      && typeof c.lastEntryId === "string" && Array.isArray(c.compactions) && c.compactions.length <= 4096
      && c.compactions.every((e: any) => typeof e.entryId === "string" && e.entryId && /^[a-f0-9]{64}$/.test(e.lineSha256)) ? c : null;
  } catch { return null; }
}

const catchupJobs = new Map<object, Promise<void>>();
/** Cache-only work: never awaited by an HTTP observation/effect request. Two jobs
 * process-wide drain bounded parser chunks with an event-loop yield each time.
 * Callers retain exact cache/incarnation ownership; this creates no fresh proof. */
export function startContextRefreshCatchup(key: object, step: (remainingBytes: number) => { bytes: number; complete: boolean }): void {
  if (catchupJobs.has(key) || catchupJobs.size >= 2) return;
  const started = performance.now();
  const job = (async () => {
    let remaining = 1024 * 1024 * 1024;
    while (remaining > 0 && performance.now() - started < 30_000) {
      await new Promise<void>(resolve => setImmediate(resolve));
      if (performance.now() - started >= 30_000) break;
      const result = step(remaining);
      if (!Number.isSafeInteger(result.bytes) || result.bytes < 0 || result.bytes > remaining) break;
      remaining -= result.bytes;
      if (result.complete || result.bytes === 0) break;
    }
  })().catch(() => { /* Unknown/missing history stays unavailable; no retry/effect. */ });
  catchupJobs.set(key, job);
  void job.finally(() => { if (catchupJobs.get(key) === job) catchupJobs.delete(key); });
}

/** Absolute successful-compaction count, scoped to one native thread. CR1 owns
 * durable baselines; a cold/restarted observer must finish scanning before it
 * reports any count. Partial/malformed history never becomes an invented zero. */
export class ContextRefreshObserver {
  private readonly scans = new Map<string, Scan>();
  private readonly piScans = new Map<string, PiCache>();
  private readonly bindings = new Map<string, string>();
  constructor(private readonly sources: ContextRefreshObserverSources, private readonly options: {
    now?: () => number; maxBytesPerObservation?: number; maxRecordBytes?: number;
    maxUsageAgeMs?: number; maxProofAgeMs?: number;
  } = {}) {}

  async observe(nodeId: string): Promise<ContextRefreshObservation> {
    const clock = this.options.now ?? Date.now;
    const holds = new Set<ContextRefreshHold>();
    const result: ContextRefreshObservation = {
      identity: { nodeId, sessionName: "", generation: null, runtime: null, nativeId: null, configurationDigest: null },
      observedAt: clock(), capability: "unsupported",
      native: { verified: false, observedAt: 0, launchId: null, fingerprint: null },
      activity: { value: "unknown", observedAt: 0 }, usage: null, compactions: null, holds: [],
    };
    let b: ContextRefreshBinding | null;
    try { b = await this.sources.binding(nodeId); } catch { b = null; }
    if (!b || b.nodeId !== nodeId || !b.generation || !b.nativeId || !b.configurationDigest) {
      this.bindings.delete(nodeId); result.holds = ["identity-unavailable"]; return result;
    }
    const bindingKey = JSON.stringify(b);
    this.bindings.delete(nodeId); this.bindings.set(nodeId, bindingKey);
    while (this.bindings.size > 64) this.bindings.delete(this.bindings.keys().next().value!);
    result.identity = { nodeId, sessionName: b.sessionName, generation: b.generation, runtime: b.runtime,
      nativeId: b.nativeId, configurationDigest: b.configurationDigest };
    result.capability = b.runtime === "codex" ? "codex-reserved-fresh" : "unsupported";
    let piContractLaunchId: string | null = null;
    const sample = () => Promise.all([
      this.sources.native(b!).catch(() => null), this.sources.activity(b!).catch(() => null),
    ]);
    // Preserve Codex's bounded catch-up scheduling. Pi's native contract also
    // reads history, so its short-lived proof is sampled after that work.
    let [native, activity] = b.runtime === "codex" ? await sample() : [null, null];
    if (b.runtime === "codex") {
      try {
        const file = await this.sources.transcriptPath(b);
        if (!file) throw new Error("missing-history");
        const s = this.scan(file, b.nativeId);
        if (!s.invalid && !s.complete && s.offset < s.size) {
          const key = `${file}\0${b.nativeId}`, revision = s.revision, size = s.size;
          startContextRefreshCatchup(s, remaining => {
            if (this.scans.get(key) !== s || this.bindings.get(nodeId) !== bindingKey || this.sources.bindingCurrent?.(b!) === false) return { bytes: 0, complete: true };
            const stat = (this.sources.readTranscript ?? readContextRefreshTranscript)(file, 0, 0);
            if (stat.identity !== s.identity || stat.size < s.offset) { s.invalid = true; return { bytes: 0, complete: true }; }
            if (stat.size !== size || (stat.revision ?? `${stat.identity}:${stat.size}`) !== revision) return { bytes: 0, complete: true };
            const before = s.offset, scanned = this.scan(file, b!.nativeId, remaining);
            return { bytes: scanned.offset - before, complete: scanned.invalid || scanned.complete || scanned.offset >= scanned.size };
          });
        }
        if (s.invalid || !s.complete || s.metaId !== b.nativeId) holds.add("compaction-evidence-invalid");
        else {
          result.compactions = { count: s.count, observedAt: s.compactAt || s.metaAt,
            source: "codex_compacted_jsonl", cursor: s.compactCursor || `byte:${s.offset}` };
          if (s.usage && s.model === b.model && s.usageModel === b.model && s.usageWindow === b.contextWindow) result.usage = { ...s.usage };
        }
      } catch { holds.add("compaction-evidence-invalid"); }
    } else {
      let state: unknown;
      try { state = await this.sources.piState(b); this.pi(b, state, clock(), result, holds); }
      catch { holds.add("usage-unavailable"); }
      // A contract refusal is not evidence of malformed compaction history.
      let contract: PiRotationContract | null = null;
      try { contract = await this.sources.piContract?.(b) ?? null; }
      catch { holds.add("runtime-unsupported"); }
      if (!contract || contract.runtime !== "pi" || contract.generation !== b.generation || contract.sessionFile !== b.nativeId
        || `${contract.provider}/${contract.model}` !== b.model) holds.add("runtime-unsupported");
      else {
        piContractLaunchId = contract.launchId;
        result.capability = "pi-reserved-fresh";
        try {
          const scanned = this.scanPi(b, contract), key = `${b.nativeId}\0${b.generation}`, cache = this.piScans.get(key), scan = cache?.scan;
          if (!scanned && scan && !scan.invalid && scan.offset < scan.size) startContextRefreshCatchup(scan, remaining => {
            if (this.piScans.get(key) !== cache || cache!.scan !== scan || this.bindings.get(nodeId) !== bindingKey || this.sources.bindingCurrent?.(b!) === false) return { bytes: 0, complete: true };
            const stat = (this.sources.readTranscript ?? readContextRefreshTranscript)(b!.nativeId, 0, 0);
            if (stat.identity !== scan.identity || stat.size !== scan.size || (stat.revision ?? `${stat.identity}:${stat.size}`) !== scan.revision) return { bytes: 0, complete: true };
            const before = scan.offset, result = this.scanPi(b!, contract!, remaining);
            return { bytes: scan.offset - before, complete: !!result || scan.invalid || scan.offset >= scan.size || cache!.scan !== scan };
          });
          if (!scanned || !object(state) || state.lastEntryId !== scanned.cursor.lastEntryId) holds.add("compaction-evidence-invalid");
          else result.compactions = { count: scanned.cursor.compactions.length, observedAt: scanned.at,
            source: "pi_compaction_jsonl", cursor: JSON.stringify(scanned.cursor) };
        } catch { holds.add("compaction-evidence-invalid"); }
      }
      if (result.capability === "unsupported") holds.add("runtime-unsupported");
    }
    // Sample short-lived native/activity evidence after potentially expensive
    // history/contract work; never extend the freshness bound to hide latency.
    if (b.runtime === "pi") [native, activity] = await sample();
    if (b.runtime === "pi" && piContractLaunchId !== native?.launchId) {
      result.capability = "unsupported"; holds.add("runtime-unsupported");
    }
    let after: ContextRefreshBinding | null;
    try { after = await this.sources.binding(nodeId); } catch { after = null; }
    // Validate against completion time: native sampling happens after observation
    // starts, and slow history/binding reads can age an initially fresh proof.
    const now = clock(); result.observedAt = now;
    const timely = (at: number) => Number.isFinite(at) && at <= now && now - at <= (this.options.maxProofAgeMs ?? 5000);
    if (native && native.nodeId === nodeId && native.sessionName === b.sessionName && native.generation === b.generation
      && native.nativeId === b.nativeId && native.verified && native.fingerprint && timely(native.observedAt)) {
      result.native = { verified: true, observedAt: native.observedAt, launchId: native.launchId, fingerprint: native.fingerprint };
    } else holds.add("native-proof-unavailable");
    if (activity) result.activity = activity;
    if (!activity || activity.value === "unknown") holds.add("activity-unknown");
    else if (!timely(activity.observedAt)) holds.add("activity-stale");
    else if (activity.value === "busy") holds.add("busy");
    if (!result.usage) holds.add("usage-unavailable");
    else if (result.usage.observedAt > now || now - result.usage.observedAt > (this.options.maxUsageAgeMs ?? 120000)) holds.add("usage-stale");
    if (JSON.stringify(after) !== JSON.stringify(b)) {
      if (this.bindings.get(nodeId) === bindingKey) this.bindings.delete(nodeId);
      holds.add("binding-changed"); result.native.verified = false; result.usage = null; result.compactions = null;
    }
    if (!result.native.verified) { result.usage = null; result.compactions = null; }
    result.holds = [...holds]; return result;
  }

  private scan(file: string, nativeId: string, remainingBytes = Infinity): Scan {
    const key = `${file}\0${nativeId}`;
    const read = this.sources.readTranscript ?? readContextRefreshTranscript;
    const budget = Math.min(remainingBytes, this.options.maxBytesPerObservation ?? 8 * 1024 * 1024);
    const recordLimit = this.options.maxRecordBytes ?? 1024 * 1024;
    let s = this.scans.get(key);
    if (!s) {
      s = { identity: "", revision: "", size: 0, offset: 0, pending: Buffer.alloc(0), complete: false, invalid: false,
        metaId: null, metaAt: 0, lastAt: 0, model: null, count: 0, compactAt: 0, compactCursor: "",
        usage: null, usageModel: null, usageWindow: null };
      this.scans.set(key, s);
    }
    this.scans.delete(key); this.scans.set(key, s);
    while (this.scans.size > 64) this.scans.delete(this.scans.keys().next().value!);
    let remaining = budget;
    do {
      const chunk = read(file, s.offset, Math.min(128 * 1024, remaining));
      if (chunk.size < s.offset || (s.identity && s.identity !== chunk.identity)) { s.invalid = true; s.complete = false; break; }
      s.identity = chunk.identity; s.size = chunk.size; s.revision = chunk.revision ?? `${chunk.identity}:${chunk.size}`;
      if (!chunk.bytes.length) { s.complete = s.offset === chunk.size && !s.pending.length; break; }
      const start = s.offset - s.pending.length;
      const bytes = Buffer.concat([s.pending, chunk.bytes]);
      s.offset += chunk.bytes.length; remaining -= chunk.bytes.length;
      let from = 0, end: number;
      while ((end = bytes.indexOf(10, from)) >= 0) {
        const line = bytes.subarray(from, end); const cursor = `byte:${start + end + 1}`;
        if (line.length > recordLimit) s.invalid = true;
        else if (line.length) {
          try { this.record(s, JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(line)), cursor, (this.options.now ?? Date.now)()); }
          catch { s.invalid = true; }
        }
        from = end + 1;
      }
      s.pending = bytes.subarray(from);
      if (s.pending.length > recordLimit) { s.invalid = true; s.pending = Buffer.alloc(0); }
      s.complete = s.offset === chunk.size && !s.pending.length;
      if (s.offset >= chunk.size || s.invalid) break;
    } while (remaining > 0);
    return s;
  }

  private record(s: Scan, row: unknown, cursor: string, now: number): void {
    if (!object(row) || typeof row.type !== "string") { s.invalid = true; return; }
    if (row.type === "session_meta") {
      if (s.metaId || !object(row.payload) || typeof row.payload.id !== "string") { s.invalid = true; return; }
      s.metaId = row.payload.id; s.metaAt = time(row.timestamp);
      if (!Number.isFinite(s.metaAt) || s.metaAt > now) s.invalid = true;
      return;
    }
    const token = row.type === "event_msg" && object(row.payload) && row.payload.type === "token_count";
    if (row.type !== "turn_context" && row.type !== "compacted" && !token) return;
    const at = time(row.timestamp);
    if (!Number.isFinite(at) || at > now || at < s.lastAt || !object(row.payload)) { s.invalid = true; return; }
    s.lastAt = at;
    if (row.type === "turn_context") {
      if (typeof row.payload.model !== "string") { s.invalid = true; return; }
      if (s.model !== row.payload.model) { s.usage = null; s.usageModel = null; s.usageWindow = null; }
      s.model = row.payload.model;
    } else if (row.type === "compacted") {
      // Native `compacted` records are completed compactions; requests/failures
      // and event_msg markers receive no success credit.
      if (typeof row.payload.message !== "string" || (row.payload.count !== undefined
        && (!nonnegative(row.payload.count) || row.payload.count !== s.count + 1))) { s.invalid = true; return; }
      s.count++; s.compactAt = at; s.compactCursor = cursor; s.usage = null;
    } else {
      const info = row.payload.info;
      if (!object(info) || !object(info.last_token_usage) || !s.model) { s.usage = null; return; }
      const used = info.last_token_usage.total_tokens, window = info.model_context_window;
      if (!nonnegative(used) || !Number.isSafeInteger(window) || window <= 0 || at <= s.compactAt) { s.usage = null; return; }
      s.usage = { usedPercent: Math.min(100, used / window * 100), observedAt: at, source: "codex_token_count_jsonl", cursor };
      s.usageModel = s.model; s.usageWindow = window;
    }
  }

  /** Re-scan changed Pi history in bounded chunks. Previous accepted and durable
   * baseline prefixes must both survive byte-for-byte; same-inode edits are held.
   * Eviction/restart discards only cache, never the CR1 baseline. */
  private scanPi(b: ContextRefreshBinding, contract: PiRotationContract, remainingBytes = Infinity): { cursor: PiContextRefreshCursor; at: number } | null {
    const key = `${b.nativeId}\0${b.generation}`, read = this.sources.readTranscript ?? readContextRefreshTranscript;
    const first = read(b.nativeId, 0, 0), revision = first.revision ?? `${first.identity}:${first.size}`;
    let cache = this.piScans.get(key);
    if (!cache) { cache = {}; this.piScans.set(key, cache); }
    this.piScans.delete(key); this.piScans.set(key, cache);
    while (this.piScans.size > 64) this.piScans.delete(this.piScans.keys().next().value!);
    const retainedRaw = this.sources.piBaseline?.(b), retained = retainedRaw ? parsePiContextRefreshCursor(retainedRaw) : null;
    if (retainedRaw && !retained) return null;
    if (retained && (retained.sessionFile !== b.nativeId || retained.sessionHeaderId !== contract.sessionHeaderId)) return null;
    if (cache.accepted && cache.revision === revision && cache.identity === first.identity) {
      return cache.accepted.sha256 === contract.sessionSha256 && cache.accepted.sessionHeaderId === contract.sessionHeaderId
        ? { cursor: cache.accepted, at: cache.at! } : null;
    }
    if (!cache.scan || cache.scan.revision !== revision || cache.scan.identity !== first.identity) {
      const prefixes = [retained, cache.accepted].filter((v): v is PiContextRefreshCursor => !!v);
      cache.scan = { revision, identity: first.identity, size: first.size, offset: 0, pending: Buffer.alloc(0),
        header: null, lastEntryId: "", at: 0, lastAt: 0, invalid: false, hash: createHash("sha256"),
        prefixChecks: prefixes.map(cursor => ({ cursor, hash: createHash("sha256") })), compactions: [], ids: new Set() };
    }
    const s = cache.scan;
    if (s.invalid || s.prefixChecks.some(p => p.cursor.byteLength > s.size)) return null;
    let budget = Math.min(remainingBytes, this.options.maxBytesPerObservation ?? 8 * 1024 * 1024);
    while (budget > 0 && s.offset < s.size) {
      const chunk = read(b.nativeId, s.offset, Math.min(128 * 1024, budget));
      if (chunk.identity !== s.identity || chunk.size !== s.size || (chunk.revision ?? `${chunk.identity}:${chunk.size}`) !== s.revision || !chunk.bytes.length) {
        cache.scan = undefined; return null;
      }
      s.hash.update(chunk.bytes);
      for (const p of s.prefixChecks) if (s.offset < p.cursor.byteLength) p.hash.update(chunk.bytes.subarray(0, p.cursor.byteLength - s.offset));
      const bytes = Buffer.concat([s.pending, chunk.bytes]); s.offset += chunk.bytes.length; budget -= chunk.bytes.length;
      let from = 0, end: number;
      while ((end = bytes.indexOf(10, from)) >= 0) {
        const line = bytes.subarray(from, end); from = end + 1;
        try {
          if (!line.length || line.length > (this.options.maxRecordBytes ?? 1024 * 1024)) throw Error("invalid-record");
          const row = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(line)), at = time(row.timestamp);
          if (!object(row) || typeof row.id !== "string" || !row.id || !Number.isFinite(at) || at > (this.options.now ?? Date.now)()) throw Error("invalid-native-record");
          if (!s.header) {
            if (row.type !== "session") throw Error("header-required");
            s.header = row.id; s.at = at;
          } else {
            if (row.type === "session" || typeof row.type !== "string" || at < s.lastAt) throw Error("history-replaced");
            s.lastEntryId = row.id;
            if (row.type === "compaction") {
              if (s.ids.has(row.id) || s.compactions.length >= 4096 || typeof row.summary !== "string" || !row.summary.trim()
                || !(row.firstKeptEntryId === null || typeof row.firstKeptEntryId === "string") || !nonnegative(row.tokensBefore)) throw Error("invalid-compaction");
              s.ids.add(row.id); s.compactions.push({ entryId: row.id, lineSha256: createHash("sha256").update(line).digest("hex") }); s.at = at;
            }
          }
          s.lastAt = at;
        } catch { s.invalid = true; }
      }
      s.pending = bytes.subarray(from);
      if (s.pending.length > (this.options.maxRecordBytes ?? 1024 * 1024)) s.invalid = true;
      if (s.invalid) return null;
    }
    if (s.offset !== s.size || s.pending.length || !s.header || s.header !== contract.sessionHeaderId) return null;
    const digest = s.hash.copy().digest("hex");
    if (digest !== contract.sessionSha256 || s.prefixChecks.some(p => p.cursor.sessionHeaderId !== s.header
      || p.hash.copy().digest("hex") !== p.cursor.sha256)) { s.invalid = true; return null; }
    const cursor: PiContextRefreshCursor = { version: 1, sessionFile: b.nativeId, sessionHeaderId: s.header,
      byteLength: s.size, sha256: digest, lastEntryId: s.lastEntryId, compactions: s.compactions };
    cache.accepted = cursor; cache.at = s.at; cache.identity = s.identity; cache.revision = s.revision; cache.scan = undefined;
    return { cursor, at: s.at };
  }

  private pi(b: ContextRefreshBinding, raw: unknown, now: number, out: ContextRefreshObservation, holds: Set<ContextRefreshHold>): void {
    if (!object(raw) || raw.ready !== true || raw.exited || !object(raw.quiescence) || !object(raw.runtimeReadiness)) return;
    const q = raw.quiescence, r = raw.runtimeReadiness;
    if (raw.sessionFile !== b.nativeId || q.generation !== b.generation || r.generation !== b.generation
      || q.launchId !== raw.launchId || r.launchId !== raw.launchId || q.sessionFile !== b.nativeId || r.sessionFile !== b.nativeId
      || q.lastEntryId !== raw.lastEntryId || !Number.isFinite(time(r.observedAt)) || time(r.observedAt) > now) return;
    if (assessPiDispatchReadiness(raw, b.generation, now)) holds.add("runtime-not-ready");
    const model = r.model, current = raw.model, c = r.context;
    if (!object(model) || !object(current) || model.provider !== current.provider || model.id !== current.id
      || `${model.provider}/${model.id}` !== b.model || model.contextWindow !== b.contextWindow || !object(c)
      || c.source !== "assistant_usage" || !nonnegative(c.usedTokens) || !Number.isSafeInteger(c.remainingTokens)
      || c.remainingTokens !== b.contextWindow - c.usedTokens || !Number.isFinite(time(c.observedAt))
      || time(c.observedAt) > time(r.observedAt) || b.contextWindow <= 0) return;
    out.usage = { usedPercent: Math.min(100, c.usedTokens / b.contextWindow * 100), observedAt: time(c.observedAt),
      source: "pi_assistant_usage", cursor: String(raw.lastEntryId ?? "") };
  }
}
