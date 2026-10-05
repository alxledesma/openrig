import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execute = promisify(execFile);

/**
 * Read-only proof that a live native Codex process is the SAME native conversation
 * that produced a stored session token, for a seat launched WITHOUT resume argv.
 *
 * This is a witness for one native conversation, never a way to mint, rotate or
 * accept a new identity. The strict resume-token path remains authoritative
 * whenever it applies; this proof is consulted only after it declines.
 *
 * Identity is established ONLY by the conjunction of:
 *   1. exact pane-root descendant lineage to one native Codex binary,
 *   2. the kernel-stored current occupant generation,
 *   3. exactly one open read/write rollout file with a stable descriptor and inode,
 *   4. that file's own first record, whose session_meta.id equals the stored token,
 *   5. stability of all of the above across an independent second observation.
 *
 * Never used as identity: the rollout filename, the process cwd, the profile or
 * the model. A matching filename with a different session_meta.id is refused, and
 * a matching cwd proves nothing at all.
 *
 * No transcript text is read or returned (one bounded metadata prefix only), and
 * no environment is ever read raw: the generation witness returns a single
 * allowlisted token or nothing.
 */

export interface CodexSessionFileProofDeps {
  run(command: string, args: string[]): Promise<string>;
  /** Bounded metadata read: at most `bytes` from offset 0. Never the transcript body. */
  readPrefix(file: string, bytes: number): Promise<string>;
  /** Sanitized kernel witness: the single OPENRIG_OCCUPANT_GENERATION token, or "". */
  occupantGeneration(pid: number): Promise<string>;
  /** Direct ancestor chain, nearest parent first, as the kernel reports it. */
  ancestry(pid: number): Promise<number[]>;
}

export interface CodexSessionFileProofResult {
  state: "proven" | "absent" | "refused";
  code?: string;
  pid?: number;
  fd?: number;
  inode?: string;
  sessionId?: string;
  generation?: string;
  paneRootPid?: number;
  /** Identifiers only. Never transcript text, argv text, cwd or environment. */
  fingerprint?: string;
}

export const CODEX_SESSION_FILE_PROOF_METADATA_BYTES = 65536;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const nativeCodexBinary = "codex";

/**
 * The sanitized kernel extractor for the occupant generation. Reads the kernel's
 * stored args+env (sysctl KERN_PROCARGS2) and emits ONLY the allowlisted
 * generation token when exactly one occurrence matches the token charset; on
 * Darwin this is authoritative because a child that rewrites its process title
 * wipes the env region that `ps` renders.
 */
const PROC_ARGS_PY = [
  "import ctypes,sys,re",
  "libc=ctypes.CDLL(None)",
  "def tok(pid):",
  "    mib=(ctypes.c_int*3)(1,49,pid); n=ctypes.c_size_t(0)",
  "    if libc.sysctl(mib,3,None,ctypes.byref(n),None,0)!=0: return ''",
  "    b=ctypes.create_string_buffer(n.value)",
  "    if libc.sysctl(mib,3,b,ctypes.byref(n),None,0)!=0: return ''",
  "    t=[s.decode('utf-8','ignore').split('=',1)[1] for s in b.raw[:n.value].split(b'\\0') if s.startswith(b'OPENRIG_OCCUPANT_GENERATION=')]",
  "    return t[0] if len(t)==1 and re.fullmatch(r'[A-Za-z0-9._:-]{1,128}',t[0]) else ''",
  "print(''.join(f'{i}\\t{tok(i)}\\n' for i in map(int,sys.argv[1:])))",
].join("\n");

export const defaultDeps: CodexSessionFileProofDeps = {
  async run(command, args) {
    return (await execute(command, args, { timeout: 2000, maxBuffer: 8 * 1024 * 1024 })).stdout;
  },
  async readPrefix(file, bytes) {
    const { open } = await import("node:fs/promises");
    const handle = await open(file, "r");
    try {
      const buffer = Buffer.alloc(bytes);
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
      return buffer.subarray(0, bytesRead).toString("utf8");
    } finally {
      await handle.close();
    }
  },
  async occupantGeneration(pid) {
    if (process.platform !== "darwin") return "";
    try {
      const { stdout } = await execute("python3", ["-c", PROC_ARGS_PY, String(pid)], { timeout: 2000, maxBuffer: 64 * 1024 });
      const line = stdout.split("\n").find((l: string) => l.startsWith(`${pid}\t`));
      if (!line) return "";
      const token = line.slice(line.indexOf("\t") + 1).trim();
      return /^[A-Za-z0-9._:-]{1,128}$/.test(token) ? token : "";
    } catch {
      return "";
    }
  },
  async ancestry(pid) {
    const raw = await execute("ps", ["-o", "ppid=", "-p", String(pid)], { timeout: 2000, maxBuffer: 1024 * 1024 }).then(r => r.stdout).catch(() => "");
    const parent = Number(raw.trim());
    return Number.isSafeInteger(parent) && parent > 0 ? [parent] : [];
  },
};

interface OpenRollout { fd: number; inode: string; device: string; path: string }

export interface CodexInitialLaunchProofOptions {
  paneRootPid: number;
  pid: number;
  expectedToken: string;
  expectedGeneration: string;
  /** Minimum strict descendant depth below the pane root before the native binary. */
  minAncestryDepth?: number;
}

/**
 * Prove, or refuse, that `pid` is the same native Codex conversation identified by
 * `expectedToken`, for a pane-root descendant. Returns `proven` only when every
 * condition holds on two independent observations.
 */
export async function proveCodexInitialLaunch(
  options: CodexInitialLaunchProofOptions,
  deps: CodexSessionFileProofDeps = defaultDeps,
): Promise<CodexSessionFileProofResult> {
  const { paneRootPid, pid, expectedToken, expectedGeneration } = options;
  const minAncestryDepth = options.minAncestryDepth ?? 1;
  if (!Number.isSafeInteger(pid) || pid <= 1) return { state: "refused", code: "pid_invalid" };
  if (!Number.isSafeInteger(paneRootPid) || paneRootPid <= 1) return { state: "refused", code: "pane_root_invalid" };
  if (!uuid.test(expectedToken)) return { state: "refused", code: "token_invalid" };

  const lineageToRoot = async (): Promise<number[] | null> => {
    const chain: number[] = [];
    let cursor = pid;
    const seen = new Set<number>();
    while (cursor > 1 && cursor !== paneRootPid) {
      if (seen.has(cursor)) return null;
      seen.add(cursor);
      const parents = await deps.ancestry(cursor);
      if (parents.length !== 1) return null;
      chain.push(parents[0]!);
      cursor = parents[0]!;
      if (chain.length > 64) return null;
    }
    if (cursor !== paneRootPid) return null;
    return chain.length >= minAncestryDepth ? chain : null;
  };

  const observe = async (): Promise<
    | { ok: true; generation: string; rollouts: OpenRollout[]; lineage: number[]; comm: string }
    | { ok: false; code: string }
  > => {
    const lineage = await lineageToRoot();
    if (!lineage) return { ok: false, code: "lineage_broken" };
    const comm = (await deps.run("ps", ["-p", String(pid), "-o", "comm="]).catch(() => "")).trim();
    if (!comm) return { ok: false, code: "process_absent" };
    if (comm.split("/").pop() !== nativeCodexBinary) return { ok: false, code: "not_native_codex_binary" };
    const generation = (await deps.occupantGeneration(pid)).trim();
    if (!generation) return { ok: false, code: "generation_witness_absent" };
    const rollouts = await readWriteRollouts(pid, deps);
    if (rollouts.length === 0) return { ok: false, code: "no_open_rollout" };
    if (rollouts.length !== 1) return { ok: false, code: "writer_ambiguous" };
    return { ok: true, generation, rollouts, lineage, comm };
  };

  const first = await observe();
  if (!first.ok) return first.code === "process_absent" ? { state: "absent", code: first.code } : { state: "refused", code: first.code };
  // A witness that never equals the current generation is a mismatch; a witness
  // that matched and then moved is drift. Both refuse, but the cause is exact.
  if (first.generation !== expectedGeneration) return { state: "refused", code: "generation_mismatch" };

  const bound = first.rollouts[0]!;
  // N1: the descriptor's inode must still name this path immediately before the
  // read, so a swapped or replaced path cannot be read as the bound file.
  const beforeRead = await statOfPath(bound.path, deps);
  if (!beforeRead || beforeRead.inode !== bound.inode || beforeRead.device !== bound.device)
    return { state: "refused", code: "descriptor_binding_mismatch" };
  let sessionId: string;
  try {
    const prefix = await deps.readPrefix(bound.path, CODEX_SESSION_FILE_PROOF_METADATA_BYTES);
    const record = JSON.parse(prefix.split("\n", 1)[0] ?? "");
    const payload = record?.payload;
    if (record?.type !== "session_meta") return { state: "refused", code: "metadata_not_session_meta" };
    if (typeof payload?.id !== "string" || !uuid.test(payload.id)) return { state: "refused", code: "metadata_id_invalid" };
    // The binding is the file's OWN session_meta.id against the stored token.
    // The filename and the cwd are deliberately not consulted here.
    if (payload.id !== expectedToken) return { state: "refused", code: "token_mismatch" };
    sessionId = payload.id;
  } catch {
    return { state: "refused", code: "metadata_unreadable" };
  }
  // ...and still the same file after the read completes.
  const afterRead = await statOfPath(bound.path, deps);
  if (!afterRead || afterRead.inode !== bound.inode || afterRead.device !== bound.device)
    return { state: "refused", code: "descriptor_binding_mismatch" };

  const second = await observe();
  if (!second.ok) return { state: "refused", code: second.code === "process_absent" ? "process_absent_drift" : second.code };
  const rebound = second.rollouts[0]!;
  if (second.generation !== first.generation) return { state: "refused", code: "generation_drift" };
  if (rebound.fd !== bound.fd || rebound.inode !== bound.inode || rebound.path !== bound.path)
    return { state: "refused", code: "descriptor_drift" };
  if (second.lineage.join(",") !== first.lineage.join(",")) return { state: "refused", code: "lineage_drift" };

  return {
    state: "proven",
    pid,
    paneRootPid,
    fd: bound.fd,
    inode: bound.inode,
    sessionId,
    generation: first.generation,
    // Fingerprint carries identifiers only; the session id is a credential-shaped
    // token, so it appears as a digest and never in the clear.
    fingerprint: `codex-session-file-proof:${pid}:${bound.fd}:${bound.inode}:${createHash("sha256").update(sessionId).digest("hex")}`,
  };
}

/** Parse `lsof -F` output into the unique open read/write rollout files of one pid. */
/** One open-file record as real `lsof -F` emits it. */
export interface LsofFieldRecord {
  fd: number;
  mode: string;
  inode: string | null;
  device: string | null;
  path: string;
}

/**
 * Parse the REAL `lsof -F` field format. On Darwin the fields are separate
 * lines: `p<pid>`, `f<fd>` with NO mode appended, `a<mode>` (for example `ar`,
 * `aw`, `au`), `i<inode>`, optionally `D<device>`, then `n<path>`. The mode is
 * never part of the `f` line, so a parser expecting `f43u` matches nothing.
 * Records we cannot parse completely are dropped rather than guessed.
 */
export function parseLsofFieldRecords(raw: string): LsofFieldRecord[] {
  const records: LsofFieldRecord[] = [];
  let fd: number | null = null;
  let mode = "";
  let inode: string | null = null;
  let device: string | null = null;
  for (const line of raw.split("\n")) {
    if (!line) continue;
    if (line.startsWith("p")) { fd = null; mode = ""; inode = null; device = null; continue; }
    if (line.startsWith("f")) {
      const m = line.match(/^f(\d+)$/);
      fd = m ? Number(m[1]) : null;
      mode = ""; inode = null; device = null;
      continue;
    }
    if (line.startsWith("a")) { mode = line.slice(1); continue; }
    if (line.startsWith("i")) { inode = line.slice(1); continue; }
    if (line.startsWith("D")) { device = line.slice(1); continue; }
    if (line.startsWith("n")) {
      if (fd !== null && mode) records.push({ fd, mode, inode, device, path: line.slice(1) });
      fd = null; mode = ""; inode = null; device = null;
    }
  }
  return records;
}

/**
 * The unique open read/write rollout files of one pid, using the HELD
 * descriptor's own inode rather than a path lookup alone.
 */
async function readWriteRollouts(pid: number, deps: CodexSessionFileProofDeps): Promise<OpenRollout[]> {
  const raw = await deps.run("lsof", ["-a", "-p", String(pid), "-d", "0-999", "-FaniD"]);
  const records = parseLsofFieldRecords(raw).filter(r => /[uw]/.test(r.mode) && /\/rollout-[^/]+\.jsonl$/.test(r.path));
  const unique = new Map<string, OpenRollout>();
  for (const record of records) {
    // N1: bind the descriptor the writer holds to the path we will read. A path
    // renamed or replaced after the writer opened it must refuse, never be read
    // as though it were the bound file.
    // A descriptor without a usable device number cannot be bound to a path and
    // must be refused rather than trusted on its inode alone.
    if (!record.inode || record.device === null) continue;
    const descriptorDevice = normalizeDevice(record.device);
    if (!descriptorDevice) continue;
    const onDisk = await statOfPath(record.path, deps);
    if (!onDisk || onDisk.inode !== record.inode || onDisk.device !== descriptorDevice) continue;
    const key = `${record.fd}:${record.inode}:${descriptorDevice}`;
    if (!unique.has(key)) unique.set(key, { fd: record.fd, inode: record.inode, device: descriptorDevice, path: record.path });
  }
  return [...unique.values()];
}

/**
 * Resolve the single native Codex descendant of a pane root. The pane PID is the
 * pane's own shell; the native agent is its descendant. Exactly one descendant whose
 * executable basename is the native codex binary must exist, otherwise the proof
 * refuses rather than guessing.
 */
export async function findNativeCodexDescendant(paneRootPid: number, deps: CodexSessionFileProofDeps = defaultDeps): Promise<number | null> {
  if (!Number.isSafeInteger(paneRootPid) || paneRootPid <= 1) return null;
  const raw = await deps.run("ps", ["-axo", "pid=,ppid=,comm="]).catch((_r: unknown) => "");
  const rows = raw.split("\n").map(line => line.trim()).filter(Boolean).map(line => {
    const m = line.match(/^(\d+)\s+(\d+)\s+(.+)$/);
    return m ? { pid: Number(m[1]), ppid: Number(m[2]), comm: m[3]! } : null;
  }).filter((r): r is { pid: number; ppid: number; comm: string } => r !== null);
  const childrenOf = new Map<number, number[]>();
  for (const r of rows) {
    if (!childrenOf.has(r.ppid)) childrenOf.set(r.ppid, []);
    childrenOf.get(r.ppid)!.push(r.pid);
  }
  const descendants = new Set<number>();
  const queue = [...(childrenOf.get(paneRootPid) ?? [])];
  while (queue.length) {
    const pid = queue.shift()!;
    if (pid === paneRootPid || descendants.has(pid)) continue;
    descendants.add(pid);
    queue.push(...(childrenOf.get(pid) ?? []));
  }
  const native = rows.filter(r => descendants.has(r.pid) && r.comm.split("/").pop() === nativeCodexBinary).map(r => r.pid);
  // Exactly one native codex binary under this pane root, or no proof at all.
  return native.length === 1 ? native[0]! : null;
}

/**
 * lsof reports the device as a hex `dev_t` (for example `0x1000011`) while
 * `stat -f %d` reports it in decimal. Normalize both to a decimal string so the
 * descriptor's own device number can be compared with the path's.
 */
export function normalizeDevice(value: string): string | null {
  const trimmed = value.trim();
  if (/^0x[0-9a-f]+$/i.test(trimmed)) {
    const parsed = Number.parseInt(trimmed, 16);
    return Number.isSafeInteger(parsed) ? String(parsed) : null;
  }
  return /^\d+$/.test(trimmed) ? trimmed : null;
}

/** inode + decimal device for one path, so a recycled filename cannot masquerade as stable. */
async function statOfPath(file: string, deps: CodexSessionFileProofDeps): Promise<{ inode: string; device: string } | null> {
  try {
    const out = (await deps.run("stat", ["-f", "%d %i", file])).trim().split(/\s+/);
    const device = out[0];
    const inode = out[1];
    return device && inode && /^\d+$/.test(device) && /^\d+$/.test(inode) ? { device, inode } : null;
  } catch {
    return null;
  }
}