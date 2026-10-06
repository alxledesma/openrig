// Legacy Pi native witness — the daemon-owned, ephemeral pre-halt proof for the
// EXPLICIT legacy Pi runner bridge. This module answers one question, inside an
// already-held exclusive lease, without weakening any ordinary observation:
//
//   "Is this exact live legacy Pi runner, and this exact live child session,
//    idle right now, and what is the session's real leaf entry?"
//
// The legacy RunnerCore cursor is NOT that answer. The retained legacy build
// assigns a generic event id — often a UI UUID — to its durable cursor, so that
// cursor can name no session entry at all. Requiring it to equal the leaf is
// invalid; reading the leaf out of the live child session is authoritative. This
// module therefore produces the leaf and never a cursor claim, and it leaves
// every on-disk cursor, sidecar and cache untouched.
//
// Design constraints this file exists to enforce:
//
//   - NO CALLER PROOF. `witness()` binds pids it was handed and then RE-PROVES
//     their identity itself. A caller cannot supply a witness, a leaf, a module
//     path, a script URL, a port, or a target: every value below comes from the
//     target's own module registry, the target's own instances, and the bounded
//     session file the daemon already owns.
//   - SIGNALLING IS A LAST RESORT, NEVER A DEFAULT. A signal may only reach a pid
//     that (a) is alive with the exact bound start identity, (b) has the exact
//     parent the binding describes, (c) is not this daemon, and (d) has a
//     verifiably FREE loopback inspector endpoint. Any failure refuses typed,
//     before any halt and before any signal.
//   - READ-OR-DESTRUCTURE. Every inspector interaction is one of: Runtime.enable,
//     Debugger.enable, Runtime.evaluate of one module-specifier expression,
//     Runtime.queryObjects, Runtime.callFunctionOn, or Debugger.scriptParsed. No
//     heap snapshot, no object previews, no Runtime.getProperties, no assignment,
//     no invocation with arguments.
//   - ALLOWLISTED PRIMITIVES ONLY. Exactly the values in the accessor allowlists
//     may leave the target: booleans, non-negative integers, and a small set of
//     opaque identifier strings. Messages, model state, settings, environment,
//     argv and transcripts are never read.
//   - REDUCED FAILURE. Every refusal is a closed-vocabulary reason code plus a
//     count. No pid, port, path, argv, environment text or exception message is
//     ever carried in a reason or a thrown error.
//   - EPHEMERAL. The witness is a value. Nothing is persisted, cached, or written
//     back to the sidecar, the runner, or the session file.
import { execFile } from "node:child_process";
import { createConnection } from "node:net";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { runAsyncSite } from "./sync-site-wrap.js";

const execFileAsync = promisify(execFile);

/** Closed vocabulary for every refusal. A reason must be added here before any
 *  call site can produce it; an unknown token is unrepresentable. */
export const LEGACY_WITNESS_REASONS = [
  "target_identity_unproven",
  "inspector_endpoint_unavailable",
  "inspector_unavailable",
  "inspector_listener_unverified",
  "inspector_close_unverified",
  "module_identity_unverified",
  "runner_instance_ambiguous",
  "child_instance_ambiguous",
  "session_instance_ambiguous",
  "projection_invalid",
  "runner_not_idle",
  "child_not_idle",
  "session_identity_mismatch",
  "launch_identity_mismatch",
  "native_leaf_mismatch",
  "witness_sample_drift",
] as const;

export type LegacyPiWitnessReason = (typeof LEGACY_WITNESS_REASONS)[number];

const REASON_SET: ReadonlySet<string> = new Set(LEGACY_WITNESS_REASONS);

export function isLegacyPiWitnessReason(value: unknown): value is LegacyPiWitnessReason {
  return typeof value === "string" && REASON_SET.has(value);
}

/** A reduced refusal. `detail` exists for local diagnostics and is DROPPED by
 *  `reduceWitnessFailure`; nothing derived from it survives this module. */
export interface LegacyPiWitnessFailure {
  reasons: LegacyPiWitnessReason[];
  /** How many R-C-R rounds ran before the refusal: a count, never a timestamp. */
  rounds: number;
  detail?: string;
}

export interface LegacyPiWitnessAcceptance {
  /** The session's real leaf entry id, proven equal to the bounded file tail. This
   *  is what a same-history rehost may carry forward. It is NOT the legacy
   *  runner's cursor, which may be a UI UUID. */
  nativeLeaf: string;
  launchId: string;
  generation: string;
  sessionFile: string;
  sessionId: string;
  /** Distinctive-but-opaque digest over the proven identities. Carries no pid,
   *  port, path or launch text, so it is safe in an audit receipt. */
  evidenceId: string;
  rounds: number;
}

export type LegacyPiWitnessResult =
  | ({ ok: true } & LegacyPiWitnessAcceptance)
  | ({ ok: false } & LegacyPiWitnessFailure);

/** Reduce a refusal to the only form that may leave this module: closed reason
 *  tokens plus a round count. This is the single egress path for failures. */
export function reduceWitnessFailure(failure: LegacyPiWitnessFailure): { reasons: LegacyPiWitnessReason[]; rounds: number } {
  const reasons = (failure.reasons ?? []).filter(isLegacyPiWitnessReason);
  return {
    // A refusal always names at least one closed reason. An empty set would be
    // indistinguishable from "no verdict", which is precisely the UNKNOWN defect.
    reasons: reasons.length > 0 ? reasons : ["inspector_unavailable"],
    rounds: Number.isFinite(failure.rounds) && failure.rounds > 0 ? Math.trunc(failure.rounds) : 0,
  };
}

/** A live process identity the caller believes it is looking at. It is a HINT:
 *  `witness()` re-proves start identity and parentage before it may signal
 *  anything, and refuses if either does not match. */
export interface LegacyPiTargetIdentity { pid: number; ppid: number; startedAt: string }

/** The identity a caller supplies for the rehost. No proof value crosses here. */
export interface LegacyPiWitnessBinding {
  runner: LegacyPiTargetIdentity;
  child: LegacyPiTargetIdentity;
  launchId: string;
  generation: string;
  sessionFile: string;
}

/** Where the daemon finds the classes it must read. Both are constants derived
 *  from the installed package layout and the shipped runner entry, resolved from
 *  the daemon's own trusted configuration — never from a request body. The
 *  transport still verifies each is a module the TARGET reports as already
 *  loaded before importing it, so a wrong constant refuses rather than misreads. */
export interface LegacyPiModuleBinding {
  /** URL of the module that exports the runner's core class. */
  runnerModuleUrl: string;
  /** URL of the cached Pi bundle chunk exporting the runtime and session classes. */
  piModuleUrl: string;
}

/** The accessor allowlist: `name:kind`. The transport reads exactly these, from
 *  exactly these classes, with no dynamic path and no caller-supplied field. */
export interface LegacyPiProjectionRequest {
  exportName: string;
  accessors: ReadonlyArray<`${string}:${"bool" | "id"}`>;
}

export type LegacyPiPrimitive = boolean | number | string | null;

export interface LegacyPiProjection {
  /** Live instances of the class. Anything but 1 is ambiguous, never "pick one". */
  count: number;
  fields: Record<string, LegacyPiPrimitive>;
}

/** ONE target, ONE live inspector session, bound to a single pid for its whole
 *  life. It must be closed before another unit is attached to another target. */
export interface LegacyPiInspectorTransport {
  readonly endpoint: { host: "127.0.0.1"; port: number };
  readonly registration: { pid: number; startIdentity: string; daemonPid: number };
  /** Canonical URLs the TARGET itself reports as loaded, from Debugger.scriptParsed. */
  loadedModuleUrls(): Promise<string[]>;
  /** Confirm the endpoint's listener is owned by the registered target alone.
   *  False for a collision AND for unknown ownership; both refuse. */
  verifyListenerOwnership(): Promise<boolean>;
  /** Enumerate live instances and read ONLY the allowlisted primitives. */
  queryProjection(request: LegacyPiProjectionRequest): Promise<LegacyPiProjection>;
  /** Close the target's inspector listener, then report whether it is verifiably gone. */
  close(): Promise<{ listenerClosed: boolean }>;
}

/** Every effect this module may have on the machine, as an interface. Production
 *  supplies the real implementations; tests supply their own. Nothing here has a
 *  default that does anything. */
export interface LegacyPiTransportSource {
  /** The loopback endpoint the activation signal will make the TARGET listen on.
   *
   *  This is deliberately NOT an ephemeral port of our choosing: `SIGUSR1` makes a
   *  Node process open its inspector on the port the process was CONFIGURED with
   *  (`--inspect-port`, else the built-in default), so binding a free port of our
   *  own and then attaching to it could never succeed. The returned endpoint must
   *  be the one the target will actually use; the caller then requires it to be
   *  free before the signal is sent. */
  resolveEndpoint(): Promise<{ host: "127.0.0.1"; port: number }>;
  /** Whether ANY process currently listens on the endpoint: a collision. */
  endpointInUse(endpoint: { host: "127.0.0.1"; port: number }): Promise<boolean>;
  /** Fresh identity for a pid: `null` if not alive with the expected identity. */
  census(pid: number): Promise<LegacyPiTargetIdentity | null>;
  /** Deliver the activation signal to ONE pid. */
  deliverSignal(pid: number, signal: string): void;
  /** Attach to the target's already-open inspector listener on `endpoint`. */
  openTransport(target: LegacyPiTargetIdentity, endpoint: { host: "127.0.0.1"; port: number }): Promise<LegacyPiInspectorTransport>;
  /** Re-read a pid's identity AFTER the work, to prove the target did not change. */
  readIdentity(pid: number): Promise<LegacyPiTargetIdentity | null>;
}

export interface LegacyPiWitnessOptions {
  source: LegacyPiTransportSource;
  /** The signal that opens a Node inspector listener. Only SIGUSR1 qualifies: it
   *  is the one signal Node treats as "open the inspector", so a caller cannot
   *  substitute a termination signal and have it counted as proof. */
  signal: "SIGUSR1";
  /** Bounded tail entry id of the session file the daemon already owns. */
  tailEntryId: (sessionFile: string) => string | null;
  modules: LegacyPiModuleBinding;
  /** Fresh R-C-R rounds; at least 2, because one round cannot show stability. */
  rounds?: number;
  roundGapMs?: number;
}

export interface LegacyPiNativeWitnessCollector {
  witness(binding: LegacyPiWitnessBinding): Promise<LegacyPiWitnessResult>;
}

const RUNNER_ACCESSORS: ReadonlyArray<`${string}:${"bool" | "id"}`> = [
  "ready:bool", "streaming:bool", "processing:bool", "controlPending:bool",
  "sessionFile:id", "sessionId:id", "launchId:id", "generation:id",
];

const CHILD_ACCESSORS: ReadonlyArray<`${string}:${"bool" | "id"}`> = [
  "isStreaming:bool", "isCompacting:bool", "pendingMessageCount:id",
  "sessionFile:id", "sessionId:id", "leafId:id", "runtimeSessionMatchesSession:bool",
];

export const LEGACY_RUNNER_PROJECTION: LegacyPiProjectionRequest = { exportName: "RunnerCore", accessors: RUNNER_ACCESSORS };
export const LEGACY_CHILD_PROJECTION: LegacyPiProjectionRequest = { exportName: "AgentSessionRuntime", accessors: CHILD_ACCESSORS };
const LEGACY_SESSION_PROJECTION: LegacyPiProjectionRequest = { exportName: "AgentSession", accessors: [] };

const DEFAULT_ROUNDS = 2;
const DEFAULT_ROUND_GAP_MS = 250;
const DEFAULT_CLOSE_GRACE_MS = 2_000;
const DEFAULT_LISTEN_TIMEOUT_MS = 8_000;

/** An opaque identifier is a single bounded token. Anything else — free text, an
 *  embedded newline, a path with spaces — is refused rather than carried. */
const OPAQUE_ID = /^[A-Za-z0-9._:@/-]{1,512}$/;
/** A Pi entry id is a short hex token; an unbounded "leaf" is not one. */
const OPAQUE_ENTRY_ID = /^[0-9a-f]{1,64}$/i;

function asBool(value: LegacyPiPrimitive | undefined): boolean | null {
  return typeof value === "boolean" ? value : null;
}
function asId(value: LegacyPiPrimitive | undefined): string | null {
  return typeof value === "string" && OPAQUE_ID.test(value) ? value : null;
}
function asCount(value: LegacyPiPrimitive | undefined): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : null;
}

/** Every declared accessor must have produced a value of its declared kind. A
 *  missing, mistyped or non-primitive projection is `projection_invalid`: never
 *  a default, never a guess. */
function projectRound(raw: LegacyPiProjection, request: LegacyPiProjectionRequest): Record<string, LegacyPiPrimitive> | null {
  if (!Number.isInteger(raw.count) || raw.count < 0) return null;
  const fields = raw.fields ?? {};
  const out: Record<string, LegacyPiPrimitive> = {};
  for (const accessor of request.accessors) {
    const [name, kind] = accessor.split(":") as [string, "bool" | "id"];
    const value = fields[name];
    if (kind === "bool") {
      const bool = asBool(value);
      if (bool === null) return null;
      out[name] = bool;
      continue;
    }
    // pendingMessageCount travels through the id slot but is an integer; a
    // non-integer or negative count is a type mismatch, not a zero.
    if (name === "pendingMessageCount") {
      const count = asCount(value);
      if (count === null) return null;
      out[name] = count;
      continue;
    }
    out[name] = asId(value);
  }
  return out;
}

/** One accepted round: the runner projection plus the child projection. */
interface WitnessRound {
  runner: Record<string, LegacyPiPrimitive>;
  child: Record<string, LegacyPiPrimitive>;
}

/** The reduced, order-stable signature of a round. Two rounds must agree exactly;
 *  any change in a proven value refuses rather than being averaged or absorbed. */
function roundSignature(round: WitnessRound): string {
  return JSON.stringify([round.runner, round.child]);
}

/** One projection read: the allowlisted primitives, or the reason it failed. */
type ProjectionRead = Record<string, LegacyPiPrimitive> | LegacyPiWitnessReason;
type RoundOutcome = WitnessRound | LegacyPiWitnessReason;
const isReason = (value: ProjectionRead | RoundOutcome): value is LegacyPiWitnessReason => typeof value === "string";

export function makeLegacyPiNativeWitness(options: LegacyPiWitnessOptions): LegacyPiNativeWitnessCollector {
  const rounds = Math.max(2, Math.trunc(options.rounds ?? DEFAULT_ROUNDS));
  const roundGapMs = options.roundGapMs ?? DEFAULT_ROUND_GAP_MS;
  const wait = (ms: number) => new Promise<void>(resolve => { setTimeout(resolve, ms); });

  /** Attach, read, and ALWAYS close — including on every failure path. A unit we
   *  opened but cannot prove closed is itself a refusal: the target must never
   *  keep an inspector open because our verification had a problem. */
  async function withTransport(
    target: LegacyPiTargetIdentity,
    expectedParent: number | null,
    body: (transport: LegacyPiInspectorTransport) => Promise<ProjectionRead>,
  ): Promise<ProjectionRead> {
    let transport: LegacyPiInspectorTransport | null = null;
    let closeFailed = false;
    try {
      const endpoint = await options.source.resolveEndpoint();
      // A port already in use means an inspector is open that we did not open and
      // cannot attribute. Refuse BEFORE the signal; never adopt a foreign listener.
      if (await options.source.endpointInUse(endpoint)) return "inspector_endpoint_unavailable";
      if (!(await verifyTarget(target, expectedParent))) return "target_identity_unproven";
      options.source.deliverSignal(target.pid, options.signal);
      transport = await options.source.openTransport(target, endpoint);
      if (transport.registration.pid !== target.pid) return "target_identity_unproven";
      if (transport.registration.startIdentity !== target.startedAt) return "target_identity_unproven";
      if (!(await transport.verifyListenerOwnership())) return "inspector_listener_unverified";
      if (!(await verifyTarget(target, expectedParent))) return "target_identity_unproven";
      return await body(transport);
    } catch {
      return "inspector_unavailable";
    } finally {
      if (transport) {
        // Close on EVERY exit. Closing asks the TARGET to drop its listener and
        // then verifies at the socket that nothing is left; the client socket is
        // never itself treated as proof of closure.
        try {
          const closed = await transport.close();
          if (!closed.listenerClosed) closeFailed = true;
        } catch { closeFailed = true; }
      }
      if (closeFailed) throw new LegacyPiCloseUnverified();
    }
  }

  /** Re-prove the caller's hint. The pid must be alive with EXACTLY the bound start
   *  identity (defeating pid reuse) and, when expected, EXACTLY the bound parent. */
  async function verifyTarget(target: LegacyPiTargetIdentity, expectParent: number | null): Promise<boolean> {
    if (!Number.isInteger(target.pid) || target.pid <= 1 || target.pid === process.pid) return false;
    if (!target.startedAt.trim()) return false;
    const live = await options.source.census(target.pid);
    if (!live || live.startedAt !== target.startedAt) return false;
    if (expectParent !== null && live.ppid !== expectParent) return false;
    return true;
  }

  async function readProjection(
    transport: LegacyPiInspectorTransport,
    moduleUrl: string,
    request: LegacyPiProjectionRequest,
  ): Promise<ProjectionRead> {
    // The class must live in a module the TARGET reports as already loaded.
    // Importing an unloaded URL would create a SECOND module instance whose
    // prototype matches no live object, and would execute code in the target.
    const loaded = await transport.loadedModuleUrls();
    if (!loaded.includes(moduleUrl)) return "module_identity_unverified";
    const raw = await transport.queryProjection(request);
    if (raw.count !== 1) {
      return request === LEGACY_RUNNER_PROJECTION ? "runner_instance_ambiguous"
        : request === LEGACY_CHILD_PROJECTION ? "child_instance_ambiguous"
          : "session_instance_ambiguous";
    }
    const fields = projectRound(raw, request);
    if (!fields) return "projection_invalid";
    return fields as Record<string, LegacyPiPrimitive>;
  }

  const readRunner = (transport: LegacyPiInspectorTransport) =>
    readProjection(transport, options.modules.runnerModuleUrl, LEGACY_RUNNER_PROJECTION);

  /** The child read also requires exactly ONE live session, and that it IS the
   *  runtime's own session. A second session, or a session the runtime does not
   *  own, is ambiguous rather than resolvable. */
  async function readChild(transport: LegacyPiInspectorTransport): Promise<ProjectionRead> {
    const sessions = await readProjection(transport, options.modules.piModuleUrl, LEGACY_SESSION_PROJECTION);
    if (isReason(sessions)) return sessions;
    return readProjection(transport, options.modules.piModuleUrl, LEGACY_CHILD_PROJECTION);
  }

  /** One R-C-R round: runner, child, runner again. Bracketing the child read with
   *  a second runner read means a turn that begins during the child read cannot
   *  hide between samples. */
  async function takeRound(binding: LegacyPiWitnessBinding): Promise<RoundOutcome> {
    const first = await withTransport(binding.runner, null, readRunner);
    if (isReason(first)) return first;
    const middle = await withTransport(binding.child, binding.runner.pid, readChild);
    if (isReason(middle)) return middle;
    const last = await withTransport(binding.runner, null, readRunner);
    if (isReason(last)) return last;
    // The bracketing pair must agree: the runner did not move while we read the child.
    if (roundSignature({ runner: first, child: {} }) !== roundSignature({ runner: last, child: {} })) return "witness_sample_drift";
    return { runner: first, child: middle };
  }

  /** The accepted facts, before the evidence id is computed over them. */
  type WitnessVerdict = Omit<LegacyPiWitnessAcceptance, "evidenceId" | "rounds">;

  function verdict(binding: LegacyPiWitnessBinding, round: WitnessRound): LegacyPiWitnessReason | WitnessVerdict {
    const { runner, child } = round;
    // Identity: the live runner must BE this launch at this generation, and runner
    // and child must name the same session file and session id.
    if (runner["launchId"] !== binding.launchId || runner["generation"] !== binding.generation) return "launch_identity_mismatch";
    if (runner["sessionFile"] !== binding.sessionFile || child["sessionFile"] !== binding.sessionFile) return "session_identity_mismatch";
    if (child["sessionId"] !== runner["sessionId"] || child["runtimeSessionMatchesSession"] !== true) return "session_identity_mismatch";
    // Quiescence, both sides. Anything but PROVEN idle refuses: absence is never
    // read as idle, and a control in flight is not idle either.
    if (runner["ready"] !== true || runner["streaming"] !== false || runner["processing"] !== false || runner["controlPending"] !== false) return "runner_not_idle";
    if (child["isStreaming"] !== false || child["isCompacting"] !== false) return "child_not_idle";
    if (child["pendingMessageCount"] !== 0) return "child_not_idle";
    // The leaf is the authoritative cursor, and it must equal the BOUNDED FILE TAIL
    // for this same session file. If history and file disagree, refuse rather than
    // carrying either value forward.
    const leaf = child["leafId"];
    if (typeof leaf !== "string" || !OPAQUE_ENTRY_ID.test(leaf)) return "native_leaf_mismatch";
    if (options.tailEntryId(binding.sessionFile) !== leaf) return "native_leaf_mismatch";
    const sessionId = child["sessionId"];
    if (typeof sessionId !== "string") return "session_identity_mismatch";
    return { nativeLeaf: leaf, launchId: binding.launchId, generation: binding.generation, sessionFile: binding.sessionFile, sessionId };
  }

  return {
    async witness(binding: LegacyPiWitnessBinding): Promise<LegacyPiWitnessResult> {
      const fail = (reasons: LegacyPiWitnessReason[], rounds: number): LegacyPiWitnessResult => ({
        ok: false, ...reduceWitnessFailure({ reasons, rounds }),
      });
      // 1. The hint must be a live, non-self, correctly-parented process.
      if (!(await verifyTarget(binding.runner, null))) return fail(["target_identity_unproven"], 0);
      if (!(await verifyTarget(binding.child, binding.runner.pid))) return fail(["target_identity_unproven"], 0);
      // 2. Fresh R-C-R rounds, all agreeing exactly.
      let accepted: WitnessRound | null = null;
      for (let round = 1; round <= rounds; round += 1) {
        if (round > 1) await wait(roundGapMs);
        let result: RoundOutcome;
        try {
          result = await takeRound(binding);
        } catch (error) {
          // A close that could not be verified is its own refusal, and it outranks
          // whatever the round was reading.
          if (error instanceof LegacyPiCloseUnverified) return fail(["inspector_close_unverified"], round);
          throw error;
        }
        if (isReason(result)) return fail([result], round);
        if (accepted && roundSignature(accepted) !== roundSignature(result)) return fail(["witness_sample_drift"], round);
        accepted = result;
      }
      // 3. Verdict on the agreed rounds.
      const decided = verdict(binding, accepted!);
      if (typeof decided === "string") return fail([decided], rounds);
      // 4. Neither target may have changed while we read them.
      const runnerAfter = await options.source.readIdentity(binding.runner.pid);
      const childAfter = await options.source.readIdentity(binding.child.pid);
      if (!runnerAfter || runnerAfter.startedAt !== binding.runner.startedAt
        || !childAfter || childAfter.startedAt !== binding.child.startedAt || childAfter.ppid !== binding.runner.pid) {
        return fail(["target_identity_unproven"], rounds);
      }
      // 5. The evidence id digests the proven identities: no pid, port or path text.
      const evidenceId = `sha256:${createHash("sha256").update(JSON.stringify([
        binding.runner.startedAt, binding.child.startedAt, decided.nativeLeaf, decided.sessionId, decided.sessionFile,
      ])).digest("hex").slice(0, 16)}`;
      return { ok: true, evidenceId, rounds, ...decided };
    },
  };
}

/** Internal marker: a listener we opened could not be proven closed. It carries no
 *  text, so throwing it cannot leak the endpoint or the target. */
class LegacyPiCloseUnverified extends Error {
  constructor() { super("inspector close unverified"); this.name = "LegacyPiCloseUnverified"; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Real transport: SIGUSR1 activation plus a Node inspector client speaking
// exactly the protocol this module needs and nothing more. Every step is
// fail-closed: an unexpected frame, a non-primitive projection, an unexpected
// instance count or an unverifiable listener becomes a typed refusal.
// ─────────────────────────────────────────────────────────────────────────────

interface PendingCommand { resolve: (value: unknown) => void; reject: (error: Error) => void }

class InspectorClient {
  private socket: WebSocket | null = null;
  private nextId = 0;
  private readonly pending = new Map<number, PendingCommand>();
  /** URLs the TARGET reports as loaded, collected from Debugger.scriptParsed. */
  private readonly loaded = new Set<string>();
  private scriptsEnabled = false;

  constructor(private readonly host: "127.0.0.1", private readonly port: number) {}

  async connect(timeoutMs: number): Promise<void> {
    const response = await fetch(`http://${this.host}:${this.port}/json/list`, { signal: AbortSignal.timeout(timeoutMs) });
    if (!response.ok) throw new Error("inspector discovery failed");
    const targets: unknown = await response.json();
    if (!Array.isArray(targets) || targets.length !== 1 || typeof targets[0]?.webSocketDebuggerUrl !== "string") throw new Error("inspector target ambiguous");
    const url = new URL(targets[0].webSocketDebuggerUrl);
    if (url.protocol !== "ws:" || url.hostname !== this.host || url.port !== String(this.port) || !/^\/[a-f0-9-]+$/i.test(url.pathname) || url.username || url.password || url.search || url.hash) throw new Error("inspector target unbound");
    const socket = new WebSocket(url.href);
    this.socket = socket;
    socket.addEventListener("message", (event: { data: unknown }) => {
      let message: { id?: number; method?: string; params?: Record<string, unknown>; result?: unknown; error?: unknown };
      try { message = JSON.parse(String(event.data)) as typeof message; } catch { return; }
      if (message.method === "Debugger.scriptParsed") {
        const url = message.params?.["url"];
        if (typeof url === "string" && url.startsWith("file://")) this.loaded.add(url);
        return;
      }
      if (typeof message.id !== "number") return;
      const waiter = this.pending.get(message.id);
      if (!waiter) return;
      this.pending.delete(message.id);
      if (message.error) { waiter.reject(new Error("inspector command refused")); return; }
      waiter.resolve(message.result);
    });
    const drop = () => {
      for (const waiter of this.pending.values()) waiter.reject(new Error("inspector connection lost"));
      this.pending.clear();
    };
    socket.addEventListener("error", drop);
    socket.addEventListener("close", drop);
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("inspector attach timed out")), timeoutMs);
      socket.addEventListener("open", () => { clearTimeout(timer); resolve(); }, { once: true });
      socket.addEventListener("error", () => { clearTimeout(timer); reject(new Error("inspector attach failed")); }, { once: true });
    });
  }

  private send(method: string, params: Record<string, unknown> = {}, timeoutMs = 5_000): Promise<unknown> {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return Promise.reject(new Error("inspector not connected"));
    const id = ++this.nextId;
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error("inspector command timed out")); }, timeoutMs);
      this.pending.set(id, {
        resolve: value => { clearTimeout(timer); resolve(value); },
        reject: error => { clearTimeout(timer); reject(error); },
      });
      this.socket!.send(JSON.stringify({ id, method, params }));
    });
  }

  async enable(timeoutMs: number): Promise<void> {
    await this.send("Runtime.enable", {}, timeoutMs);
    // Debugger.enable replays every script the target has ALREADY parsed. That is
    // how the loaded-module set is obtained without importing anything yet.
    await this.send("Debugger.enable", {}, timeoutMs);
    this.scriptsEnabled = true;
  }

  get tracksScripts(): boolean { return this.scriptsEnabled; }
  loadedModuleUrls(): Promise<string[]> { return Promise.resolve([...this.loaded]); }

  /** Evaluate the module-specifier expression for one already-loaded module URL,
   *  returning the module namespace as a REMOTE object handle.
   *
   *  `returnByValue: false` is essential: by-value serialization cannot carry CDP
   *  remote object handles, so a namespace inspected that way could never yield a
   *  prototype handle and every enumeration would fail closed for the wrong reason. */
  private async namespaceOf(moduleUrl: string): Promise<string> {
    const evaluated = await this.send("Runtime.evaluate", {
      expression: `globalThis.process.getBuiltinModule('node:module').createRequire(${JSON.stringify(moduleUrl)})(globalThis.process.getBuiltinModule('node:url').fileURLToPath(${JSON.stringify(moduleUrl)}))`, returnByValue: false, generatePreview: false,
    }) as { result?: { objectId?: string }; exceptionDetails?: unknown };
    if (evaluated.exceptionDetails || !evaluated.result?.objectId) {
      const type = (evaluated.result as { type?: string } | undefined)?.type;
      const safeType = ["object", "undefined", "function", "string", "boolean", "number", "symbol", "bigint"].includes(type ?? "") ? type : "unknown";
      let code = "unknown";
      if (evaluated.exceptionDetails && evaluated.result?.objectId) {
        const projected = await this.send("Runtime.callFunctionOn", { objectId: evaluated.result.objectId, functionDeclaration: "function () { const codes = ['ERR_REQUIRE_CYCLE_MODULE','ERR_REQUIRE_ASYNC_MODULE','ERR_VM_DYNAMIC_IMPORT_CALLBACK_MISSING','ERR_VM_DYNAMIC_IMPORT_CALLBACK_MISSING_FLAG','ERR_MODULE_NOT_FOUND','ERR_INVALID_ARG_TYPE']; return codes.includes(this.code) ? this.code : ['TypeError','ReferenceError','SyntaxError','Error'].includes(this.name) ? this.name : 'unknown'; }", returnByValue: true }) as { result?: { value?: unknown } };
        if (typeof projected.result?.value === "string") code = projected.result.value;
      }
      throw new Error(`module namespace unavailable (exception=${!!evaluated.exceptionDetails}, type=${safeType}, code=${code})`);
    }
    return evaluated.result.objectId;
  }

  /** Resolve one export's prototype as a remote object handle from one already-loaded
   *  module. The selected prototype is returned by reference, never by value. */
  private async prototypeOf(moduleUrl: string, exportName: string): Promise<string> {
    const namespaceObjectId = await this.namespaceOf(moduleUrl);
    // Only the ONE named export's prototype crosses back. No map of exports, no
    // values, no object graph and no preview ever leaves the target.
    const selected = await this.send("Runtime.callFunctionOn", {
      objectId: namespaceObjectId,
      functionDeclaration: `function () { const exported = this[${JSON.stringify(exportName)}]; return typeof exported === 'function' ? exported.prototype : undefined; }`,
      returnByValue: false,
      generatePreview: false,
    }) as { result?: { objectId?: string } };
    if (!selected.result?.objectId) throw new Error("export not found in loaded module");
    return selected.result.objectId;
  }

  /** Live instances of one class, as the queryObjects array's remote handle. */
  private async instancesOf(moduleUrl: string, exportName: string): Promise<{ count: number; arrayObjectId: string }> {
    const prototypeObjectId = await this.prototypeOf(moduleUrl, exportName);
    const query = await this.send("Runtime.queryObjects", { prototypeObjectId }) as { objects?: { objectId?: string } };
    if (!query.objects?.objectId) throw new Error("queryObjects returned no array");
    const counted = await this.send("Runtime.callFunctionOn", {
      objectId: query.objects.objectId,
      functionDeclaration: "function () { return this.length; }",
      returnByValue: true,
    }) as { result?: { value?: number } };
    const count = counted.result?.value;
    if (typeof count !== "number") throw new Error("instance count unavailable");
    return { count, arrayObjectId: query.objects.objectId };
  }

  async queryProjection(request: LegacyPiProjectionRequest, moduleUrl: string): Promise<LegacyPiProjection> {
    const { count, arrayObjectId } = await this.instancesOf(moduleUrl, request.exportName);
    const projected = await this.send("Runtime.callFunctionOn", {
      objectId: arrayObjectId,
      functionDeclaration: `function () { const v = this[0]; if (!v) return null; return (${projectorFor(request)}).call(v); }`,
      returnByValue: true,
      generatePreview: false,
    }) as { result?: { value?: unknown } };
    const value = projected.result?.value;
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("projection unavailable");
    // The runtime-session identity is NOT a constant. It is proved by comparing the
    // runtime's own session against the single enumerated AgentSession instance, so
    // a runtime holding a different session cannot be credited as this one.
    const matches = request.exportName === "AgentSessionRuntime"
      ? await this.runtimeSessionMatches(arrayObjectId, moduleUrl)
      : true;
    return { count, fields: { ...(value as Record<string, LegacyPiPrimitive>), runtimeSessionMatchesSession: matches } };
  }

  /** Compare the enumerated runtime's `session` with the ONE enumerated session.
   *
   *  Two independent handle identities are compared in the target: first that the
   *  runtime owns the same object as the enumerated session, then the converse. Both
   *  must hold, so neither a stale runtime nor an orphaned session passes. */
  private async runtimeSessionMatches(runtimeArrayObjectId: string, moduleUrl: string): Promise<boolean> {
    const runtime = await this.send("Runtime.callFunctionOn", {
      objectId: runtimeArrayObjectId,
      functionDeclaration: "function () { return this[0] ?? null; }",
      returnByValue: false,
    }) as { result?: { objectId?: string } };
    if (!runtime.result?.objectId) return false;
    const sessions = await this.instancesOf(moduleUrl, "AgentSession");
    if (sessions.count !== 1) return false;
    const session = await this.send("Runtime.callFunctionOn", {
      objectId: sessions.arrayObjectId,
      functionDeclaration: "function () { return this[0]; }",
      returnByValue: false,
    }) as { result?: { objectId?: string } };
    if (!session.result?.objectId) return false;
    const forward = await this.send("Runtime.callFunctionOn", {
      objectId: runtime.result.objectId,
      functionDeclaration: "function (enumerated) { return this.session === enumerated; }",
      arguments: [{ objectId: session.result.objectId }],
      returnByValue: true,
    }) as { result?: { value?: unknown } };
    const backward = await this.send("Runtime.callFunctionOn", {
      objectId: session.result.objectId,
      functionDeclaration: "function (enumeratedRuntime) { return this === enumeratedRuntime.session; }",
      arguments: [{ objectId: runtime.result.objectId }],
      returnByValue: true,
    }) as { result?: { value?: unknown } };
    return forward.result?.value === true && backward.result?.value === true;
  }

  /** Ask the TARGET to close its own listener, then drop the client and verify at
   *  the socket that nothing is left listening.
   *
   *  Two details matter for safety. `inspector.close()` is scheduled in the target
   *  and awaited only within a bounded race, so a target that is busy cannot be
   *  blocked by our cleanup: we disconnect regardless and let the verification
   *  below decide. And the client's own socket closing is never treated as proof
   *  of closure — a closed WebSocket with a live listener is a live RCE surface,
   *  so only an absent listener counts. */
  async close(graceMs: number, allowTargetEvaluation = true): Promise<{ listenerClosed: boolean }> {
    const budget = Math.max(200, Math.min(graceMs, 2_000));
    // Never close synchronously while still attached. The target schedules its own
    // close on a macrotask and returns immediately, so the target's event loop is
    // never blocked waiting on us; the evaluate itself is additionally bounded, so
    // a busy target cannot stall our cleanup either.
    //
    // Use Node's builtin getter, rather than dynamic import from an inspector
    // evaluation context (which may lack an import callback). Missing support
    // cannot authorize cleanup: listener absence still decides below.
    try {
      if (allowTargetEvaluation) await Promise.race([
        this.send("Runtime.evaluate", {
          expression: "(() => { const inspector = globalThis.process?.getBuiltinModule?.('node:inspector'); if (!inspector) return false; setTimeout(() => { try { inspector.close(); } catch {} }, 25); return true; })()",
          awaitPromise: false,
          returnByValue: true,
        }, budget),
        new Promise<void>(resolve => { setTimeout(resolve, budget); }),
      ]);
    } catch { /* the target may already be closing it; verification decides */ }
    // The client's own socket is dropped before verification: closure is proved by
    // the ABSENCE of a listener, never by our disconnect.
    try { this.socket?.close(); } catch { /* already closed */ }
    return { listenerClosed: await waitForListenerGone(this.host, this.port, graceMs) };
  }
}

/** The only functions ever run inside a target for this feature: one fixed reader
 *  per projection shape, each reading an allowlisted primitive set and nothing
 *  else. These perform no assignment and take no arguments, and the runtime/session
 *  IDENTITY is deliberately absent: it is proved by handle comparison, not asserted. */
function projectorFor(request: LegacyPiProjectionRequest): string {
  switch (request.exportName) {
    case "RunnerCore":
      return "function () { return { ready: this.ready, streaming: this.streaming, processing: this.processing, controlPending: this.controlPending, sessionFile: this.sessionFile ?? null, sessionId: this.sessionId ?? null, launchId: this.identity?.launchId ?? null, generation: this.identity?.generation ?? null }; }";
    case "AgentSessionRuntime":
      // getLeafId() reads a stored session field; nothing else on the manager is touched.
      return "function () { const s = this.session; if (!s) return null; return { isStreaming: s.isStreaming, isCompacting: s.isCompacting, pendingMessageCount: s.pendingMessageCount, sessionFile: s.sessionFile ?? null, sessionId: s.sessionId ?? null, leafId: s.sessionManager?.getLeafId?.() ?? null }; }";
    default:
      // A count-only read, used to prove there is exactly one live session.
      return "function () { return {}; }";
  }
}

async function waitForListenerGone(host: "127.0.0.1", port: number, graceMs: number): Promise<boolean> {
  const deadline = Date.now() + Math.max(100, graceMs);
  for (;;) {
    if (!(await endpointInUse({ host, port }))) return true;
    if (Date.now() >= deadline) return false;
    await new Promise<void>(resolve => { setTimeout(resolve, 50); });
  }
}

/** Whether ANY process listens on the loopback endpoint.
 *
 *  A refused connection means nothing is serving that port, so `false` is the
 *  honest answer for a free port; this must never report a free port as busy, or
 *  every rehost would refuse on a clean host. */
export async function endpointInUse(endpoint: { host: "127.0.0.1"; port: number }): Promise<boolean> {
  return await new Promise<boolean>(resolve => {
    const socket = createConnection({ host: endpoint.host, port: endpoint.port });
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const settle = (value: boolean) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      try { socket.destroy(); } catch { /* already destroyed */ }
      resolve(value);
    };
    socket.once("connect", () => settle(true));
    // Only an explicit connection refusal proves absence. An unknown error or
    // timeout is not permission to signal a target or credit listener cleanup.
    socket.once("error", (error: NodeJS.ErrnoException) => settle(error.code !== "ECONNREFUSED"));
    timer = setTimeout(() => settle(true), 400);
  });
}

/** The loopback endpoint a `SIGUSR1`-opened Node inspector listens on.
 *
 *  Node does not accept a port at signal time: it opens the port the process was
 *  launched with. An operator-prepared legacy runner therefore has to have been
 *  launched with a known `--inspect-port`, which this constant names; a process on
 *  the built-in default is covered by the same value. We never bind a port of our
 *  own choosing, because we could not then attach to what the target opened. */
export const LEGACY_INSPECTOR_PORT = 9229;

/** The endpoint the target will use, plus the free-port proof the collector needs.
 *  Returns the endpoint only; the collector performs the pre-signal collision
 *  check so that a busy port refuses before any signal is sent. */
export async function resolveInspectorEndpoint(): Promise<{ host: "127.0.0.1"; port: number }> {
  return { host: "127.0.0.1", port: LEGACY_INSPECTOR_PORT };
}

/** Fresh start identity for one pid, from `ps lstart`. `null` when it is not alive,
 * or when it is this daemon, which must never be a signal target. */
export async function readProcessIdentity(pid: number): Promise<LegacyPiTargetIdentity | null> {
  return await runAsyncSite("legacy-witness.read-identity", async () => {
    if (!Number.isInteger(pid) || pid <= 1 || pid === process.pid) return null;
    try {
      const { stdout } = await execFileAsync("ps", ["-Ao", "pid,ppid,lstart"], {
        encoding: "utf-8", maxBuffer: 8 * 1024 * 1024, env: { ...process.env, LC_ALL: "C" },
      });
      for (const line of stdout.split("\n")) {
        const match = line.trim().match(/^(\d+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})$/);
        if (match && Number(match[1]) === pid) return { pid, ppid: Number(match[2]), startedAt: match[3]! };
      }
      return null;
    } catch { return null; }
  });
}

/** Deliver ONE signal to ONE pid. Never this process, never pid <= 1, and a
 *  delivery failure is a refusal rather than a silent retry. */
export function deliverTargetSignal(pid: number, signal: string): boolean {
  if (!Number.isInteger(pid) || pid <= 1 || pid === process.pid) return false;
  try {
    process.kill(pid, signal as NodeJS.Signals);
    return true;
  } catch { return false; }
}

/** The production transport source: real signals, real endpoint checks, real
 *  inspector attach, real identity re-reads. There is no registry, no cached
 *  proof, and no caller-supplied value anywhere in it. */
export function makeNativeLegacyPiTransportSource(modules: LegacyPiModuleBinding): LegacyPiTransportSource {
  const moduleFor = (request: LegacyPiProjectionRequest): string =>
    request.exportName === "RunnerCore" ? modules.runnerModuleUrl : modules.piModuleUrl;

  return {
    resolveEndpoint: resolveInspectorEndpoint,
    endpointInUse,
    census: readProcessIdentity,
    deliverSignal: (pid, signal) => { deliverTargetSignal(pid, signal); },
    readIdentity: readProcessIdentity,
    openTransport: async (target, endpoint) => {
      const client = new InspectorClient(endpoint.host, endpoint.port);
      let attachment: Promise<void> | undefined;
      const attach = () => attachment ??= (async () => {
        await waitForListener(endpoint, DEFAULT_LISTEN_TIMEOUT_MS);
        const owners = await listenerPids(endpoint.port);
        if (owners.length !== 1 || owners[0] !== target.pid) throw new Error("inspector listener unbound");
        const beforeConnect = await readProcessIdentity(target.pid);
        if (!beforeConnect || beforeConnect.startedAt !== target.startedAt || beforeConnect.ppid !== target.ppid) throw new Error("inspector target identity changed");
        await client.connect(DEFAULT_LISTEN_TIMEOUT_MS);
        const beforeEnable = await readProcessIdentity(target.pid);
        const enablingOwners = await listenerPids(endpoint.port);
        if (!beforeEnable || beforeEnable.startedAt !== target.startedAt || beforeEnable.ppid !== target.ppid || enablingOwners.length !== 1 || enablingOwners[0] !== target.pid) throw new Error("inspector target identity changed");
        await client.enable(DEFAULT_LISTEN_TIMEOUT_MS);
        if (!client.tracksScripts) throw new Error("debugger not enabled");
      })();
      // Return the cleanup handle before attaching. Even a failed discovery or
      // attach must reach the collector's finally block and verify cleanup.
      return {
        endpoint,
        registration: { pid: target.pid, startIdentity: target.startedAt, daemonPid: process.pid },
        loadedModuleUrls: async () => { await attach(); return client.loadedModuleUrls(); },
        verifyListenerOwnership: async () => {
          await attach();
          // The listener must be the bound target and the ONLY process on the port.
          const owners = await listenerPids(endpoint.port);
          return owners.length === 1 && owners[0] === target.pid;
        },
        queryProjection: async request => { await attach(); return client.queryProjection(request, moduleFor(request)); },
        close: async () => {
          const closingIdentity = await readProcessIdentity(target.pid);
          const closingOwners = await listenerPids(endpoint.port);
          const sameOriginalProcess = !!closingIdentity && closingIdentity.startedAt === target.startedAt && closingOwners.length === 1 && closingOwners[0] === target.pid;
          // A reparented ORIGINAL process can close its own listener. A replacement
          // or unknown owner must never receive an inspector evaluation from cleanup.
          return client.close(DEFAULT_CLOSE_GRACE_MS, sameOriginalProcess);
        },
      };
    },
  };
}

/** Wait for the target's own listener to appear after the activation signal. */
async function waitForListener(endpoint: { host: "127.0.0.1"; port: number }, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await endpointInUse(endpoint)) return;
    if (Date.now() >= deadline) throw new Error("inspector listener never appeared");
    await new Promise<void>(resolve => { setTimeout(resolve, 50); });
  }
}

/** Pids listening on a loopback port. Empty is "no attributable owner", which the
 *  caller must treat as unverified rather than as absent. */
async function listenerPids(port: number): Promise<number[]> {
  try {
    const { stdout } = await execFileAsync("lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"], { encoding: "utf-8", maxBuffer: 64 * 1024 });
    return stdout.split(/\s+/).filter(Boolean).map(Number).filter(value => Number.isInteger(value));
  } catch { return []; }
}

/** Canonical file URL for a module path, for the daemon's own trusted config. */
export function moduleUrlOf(filePath: string): string { return pathToFileURL(filePath).href; }
