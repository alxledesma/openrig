// Bounded native provenance for the EXPLICIT legacy Pi bridge.
//
// Everything the live pair cannot show through the ordinary census is recovered here
// from the kernel or the filesystem, and every read is REDUCED to a boolean or a small
// closed classification. No value is ever returned: no generation token, no
// NODE_OPTIONS text, no environment entry, no path.
//
//   - Pi's bundle entry overwrites its own process title, so the census command is the
//     bare string `pi` and `ps`-rendered environment text is empty. The kernel's stored
//     region (KERN_PROCARGS2) is NOT affected by a title rewrite, so this module reads
//     it directly, in the same shape as the shipped occupant-generation extractor.
//   - That read carries a POSITIVE CONTROL: the seat's own
//     OPENRIG_OCCUPANT_GENERATION must be present exactly once and match. A readable
//     control is what makes an "unset" answer meaningful; without it the answer is
//     UNKNOWN and refuses.
//   - NODE_OPTIONS is the one environment-borne way a Node target can be configured
//     with an inspector port. It is reported as PRESENT or UNSET only, and ANY present
//     value refuses — the value itself is never read, logged or compared.
import { execFile } from "node:child_process";
import { closeSync, openSync, readSync, statSync } from "node:fs";
import { promisify } from "node:util";
import { resolveNativeTool } from "./codex-session-file-proof.js";

const execFileAsync = promisify(execFile);

/** Closed vocabulary for every refusal this module can produce. */
export const LEGACY_PI_PROVENANCE_REASONS = [
  "environment_region_unreadable",
  "occupant_generation_control_absent",
  "occupant_generation_mismatch",
  "node_options_present",
  "native_tool_unavailable",
  "listener_baseline_unreadable",
  "graph_forbidden_token",
] as const;
export type LegacyPiProvenanceReason = (typeof LEGACY_PI_PROVENANCE_REASONS)[number];

/** Tokens that would let a loaded module change or intercept the inspector itself.
 *  The Pi bundle and the runner are scanned for these at RESOLVE time, keyed to the
 *  exact graph that was walked. */
export const FORBIDDEN_GRAPH_TOKENS = ["SIGUSR1", "debugPort", "node:inspector"] as const;

export type LegacyPiRun = (command: string, args: readonly string[]) => Promise<{ stdout: string }>;

export interface LegacyPiEnvironmentObservation {
  /** True when the kernel region was readable at all. The positive control. */
  regionReadable: boolean;
  /** True only when the seat's generation was present exactly once AND matched. */
  occupantGenerationMatches: boolean;
  /** Classification only. No value is carried, under any condition. */
  nodeOptions: "unset" | "present";
  /** No environment entry, value, name or path can appear here. */
  reasons: LegacyPiProvenanceReason[];
}

/**
 * The sanitized kernel extractor.
 *
 * Emits, per pid, only three fields: a readability control, whether the seat's
 * generation matches, and whether NODE_OPTIONS is present. It never prints a value:
 * the generation is compared in-process against the expected token and reduced to a
 * verdict, and NODE_OPTIONS is reduced to presence.
 */
const PROC_ENV_PY = [
  "import ctypes,sys,re",
  "libc=ctypes.CDLL(None)",
  "def region(pid):",
  "    mib=(ctypes.c_int*3)(1,49,pid); n=ctypes.c_size_t(0)",
  "    if libc.sysctl(mib,3,None,ctypes.byref(n),None,0)!=0: return None",
  "    b=ctypes.create_string_buffer(n.value)",
  "    if libc.sysctl(mib,3,b,ctypes.byref(n),None,0)!=0: return None",
  "    return b.raw[:n.value].split(b'\\0')",
  "def verdict(pid,expected):",
  "    parts=region(pid)",
  "    if parts is None:",
  "        sys.stdout.write('%d\\tunreadable\\tnomatch\\tunknown\\n' % pid); return",
  "    gens=[s[len(b'OPENRIG_OCCUPANT_GENERATION='):].decode('utf-8','ignore') for s in parts if s.startswith(b'OPENRIG_OCCUPANT_GENERATION=')]",
  "    control=len(gens)==1 and re.fullmatch(r'[A-Za-z0-9._:-]{1,128}',gens[0]) is not None",
  "    match=control and gens[0]==expected",
  "    node_opts=any(s.startswith(b'NODE_OPTIONS=') for s in parts)",
  "    sys.stdout.write('%d\\t%s\\t%s\\t%s\\n' % (pid,'control' if control else 'nocontrol','match' if match else 'nomatch','present' if node_opts else 'unset'))",
"args=sys.argv[1:]",
  "for i in range(0,len(args),2):",
  // sys.argv entries are STRINGS, and ctypes rejects a str where a C int is
  // required. int() is mandatory: without it the real extractor raised
  // TypeError on every run and every genuine observation refused as
  // environment_region_unreadable. Kept as a Python comment in the emitted
  // script so the script stays self-describing.
  "# sys.argv entries are strings; ctypes requires an int",
  "    verdict(int(args[i]),args[i+1])",
].join("\n");

const defaultRun: LegacyPiRun = async (command, args) => {
  const { stdout } = await execFileAsync(command, [...args], { encoding: "utf-8", maxBuffer: 64 * 1024, timeout: 2_000 });
  return { stdout };
};

export interface LegacyPiEnvironmentInput {
  pid: number;
  /** The seat's own occupant generation: the positive control this read is bracketed to. */
  generation: string;
  platform?: NodeJS.Platform;
  run?: LegacyPiRun;
}

/**
 * Observe ONE target's environment, reduced to verdicts.
 *
 * An unreadable region, a missing or mismatched control, or a present NODE_OPTIONS all
 * refuse. Nothing here can report "unset" for a target whose region was not actually
 * read, which is exactly the distinction the census `ps` render could not make.
 */
export async function observeLegacyPiEnvironment(input: LegacyPiEnvironmentInput): Promise<LegacyPiEnvironmentObservation> {
  const platform = input.platform ?? process.platform;
  const refuse = (reason: LegacyPiProvenanceReason, regionReadable = false): LegacyPiEnvironmentObservation => ({
    regionReadable, occupantGenerationMatches: false, nodeOptions: "present", reasons: [reason],
  });
  // The kernel region extractor exists only for Darwin; elsewhere the control cannot be
  // established, so the answer is UNKNOWN rather than an optimistic "unset".
  if (platform !== "darwin") return refuse("environment_region_unreadable");
  const python = resolveNativeTool("python3", platform);
  if (!python) return refuse("native_tool_unavailable");
  const run = input.run ?? defaultRun;
  let stdout: string;
  try {
    ({ stdout } = await run(python, ["-c", PROC_ENV_PY, String(input.pid), input.generation]));
  } catch {
    return refuse("environment_region_unreadable");
  }
  const line = stdout.split("\n").find(candidate => candidate.startsWith(`${input.pid}\t`));
  if (!line) return refuse("environment_region_unreadable");
  const [, control, match, nodeOptions] = line.split("\t");
  const regionReadable = control === "control";
  // No readable control means "unset" was never observed, so it is not reported as unset.
  if (!regionReadable) return refuse("occupant_generation_control_absent", true);
  if (match !== "match") return refuse("occupant_generation_mismatch", true);
  // ANY present value refuses. The classification is the whole answer.
  if (nodeOptions !== "unset") return refuse("node_options_present", true);
  return { regionReadable: true, occupantGenerationMatches: true, nodeOptions: "unset", reasons: [] };
}

/** Parse the `lsof -a -p PID -iTCP -sTCP:LISTEN -Fn` listing into loopback endpoints. */
export function parsePidScopedListeners(stdout: string): { ports: number[]; public: number[] } {
  const ports: number[] = [];
  const publicEndpoints: number[] = [];
  for (const line of stdout.split("\n")) {
    if (!line.startsWith("n")) continue;
    const address = line.slice(1).trim();
    const match = address.match(/^(?:(.*):)?(\d+)(?:\s*\(LISTEN\))?$/);
    if (!match) continue;
    const host = match[1] ?? "";
    const port = Number(match[2]);
    if (!Number.isInteger(port) || port <= 0) continue;
    // A loopback listener (and bare `*:port`/`0.0.0.0` is public) is classified apart:
    // a target that opens a public listener is containment failure, not our endpoint.
    if (host === "127.0.0.1" || host === "localhost" || host === "[::1]" || host === "::1") ports.push(port);
    else publicEndpoints.push(port);
  }
  return { ports: [...new Set(ports)].sort((a, b) => a - b), public: [...new Set(publicEndpoints)].sort((a, b) => a - b) };
}

export interface LegacyPiListenerSnapshot {
  ok: boolean;
  ports: number[];
  public: number[];
  reason?: LegacyPiProvenanceReason;
}

/** The pid-scoped listener set for ONE target, or a typed refusal. Bare `lsof` is
 *  never used: the daemon does not inherit an operator PATH, so an unresolvable tool
 *  must refuse before any signal rather than leave an inspector open. */
export async function readPidScopedListeners(input: { pid: number; platform?: NodeJS.Platform; run?: LegacyPiRun }): Promise<LegacyPiListenerSnapshot> {
  const platform = input.platform ?? process.platform;
  const lsof = resolveNativeTool("lsof", platform);
  if (!lsof) return { ok: false, ports: [], public: [], reason: "native_tool_unavailable" };
  const run = input.run ?? defaultRun;
  try {
    const { stdout } = await run(lsof, ["-a", "-p", String(input.pid), "-iTCP", "-sTCP:LISTEN", "-Fn"]);
    const parsed = parsePidScopedListeners(stdout);
    return { ok: true, ...parsed };
  } catch (error) {
    // `lsof` exits 1 when NOTHING matched, which for this query is the ordinary,
    // expected answer: the target holds no listener. It is an empty baseline, not an
    // unreadable one. Any other failure stays a refusal.
    const code = (error as { code?: unknown }).code;
    if (code === 1 || code === "1") return { ok: true, ports: [], public: [] };
    return { ok: false, ports: [], public: [], reason: "listener_baseline_unreadable" };
  }
}

/** Whether a file's content predates the target that loaded it. A file replaced AFTER a
 *  process started would otherwise let today's bytes describe different code than the
 *  code that is running. */
export function filePredatesStart(path: string, targetStartedAtMs: number): boolean {
  try {
    const stats = statSync(path);
    return stats.ctimeMs < targetStartedAtMs && stats.mtimeMs < targetStartedAtMs;
  } catch {
    return false;
  }
}

/** Whether any of the loaded sources could change or intercept the inspector itself. */
export function containsForbiddenGraphToken(source: string): LegacyPiProvenanceReason | null {
  return FORBIDDEN_GRAPH_TOKENS.some(token => source.includes(token)) ? "graph_forbidden_token" : null;
}

/** The shebang interpreter line of an entry file, or null when it has none. Only the
 *  interpreter NAME is returned; a shebang carrying flags is a refusal for the caller
 *  to detect, never something this module interprets. */
export function entryShebang(path: string): string | null {
  try {
    const head = statSync(path);
    if (!head.isFile()) return null;
    const handle = openSync(path, "r");
    let first = "";
    try {
      const buffer = Buffer.alloc(256);
      const read = readSync(handle, buffer, 0, 256, 0);
      first = buffer.subarray(0, read).toString("utf-8").split("\n", 1)[0] ?? "";
    } finally { closeSync(handle); }
    return first.startsWith("#!") ? first.slice(2).trim() : null;
  } catch {
    return null;
  }
}