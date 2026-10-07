import type Database from "better-sqlite3";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { piSeatPaths, parsePiRunnerState } from "../adapters/pi-runner-protocol.js";
const run = promisify(execFile);
export interface RuntimeAvailability { session: string; generation: string; state: "present" | "absent" | "unknown"; observedAt: number; fingerprint: string; /** Optional native idle/busy evidence, carried through from the Pi native
   *  proof. `settled: null` (or an absent field) is UNKNOWN, never idle.
   *  Evidence only — no authority, recovery, queue, guard or send effect. */
  quiescence?: PiQuiescenceProof }
/** Proof that the SAME managed Pi occupant is live: runner argv binds
 * --session-name/--launch-id, the runner's typed sidecar binds launchId and
 * the exact native session-file token via the per-launch launch-id instance binding, and the
 * target-process environment carries the genuine OPENRIG_OCCUPANT_GENERATION
 * equal to the node's latest occupant tenure. Environment text never leaves
 * this module except the single extracted generation token; fingerprints carry
 * only identifiers. */
export interface PiNativeProof { lastEntryId?: string | null; state: "present" | "absent"; generation: string; launchId: string | null; fingerprint: string; quiescence?: PiQuiescenceProof }
/** Native idle/busy evidence carried alongside an identity proof. Evidence
 *  ONLY — nothing here grants authority, recovery, queue, guard, qualification
 *  or send rights.
 *
 *  `settled: null` is the honest UNKNOWN case and must never be read as idle:
 *  it means the sidecar carried no quiescence record, the record was malformed,
 *  or its bindings no longer match this exact launch/generation/session/cursor.
 *  Adapters that cannot produce native evidence omit the field entirely. */
export interface PiQuiescenceProof { settled: boolean | null; observedAt: string | null }
/** Reduced, non-sensitive reason a proof came back UNKNOWN (null).
 *  Every value is a closed-vocabulary token, a count, or a boolean. Argument
 *  vectors, environment text, filesystem paths, pids and error messages are
 *  structurally excluded: an exception contributes only its constructor name. */
export type PiProofReasonCode =
  | "binding_rows"
  | "no_pane_target"
  | "tmux_probe_failed"
  | "census_empty"
  | "census_invalid"
  | "duplicate_pids"
  | "runner_count"
  | "pi_child_count"
  | "sidecar_unreadable"
  | "sidecar_exited"
  | "sidecar_not_ready"
  | "sidecar_launch_mismatch"
  | "sidecar_session_file_mismatch"
  | "kernel_probe_failed"
  | "generation_unverified_runner"
  | "generation_unverified_child"
  | "unstable_between_samples"
  | "binding_changed"
  | "exception";
export interface PiProofReason { code: PiProofReasonCode; count?: number; detail?: "ok" | "timeout" | "error" | "empty" | "absent"; }
/** Receives exactly one reason at each null exit. Default is a no-op so every
 *  existing consumer is unchanged unless it opts in. */
export type PiProofDiagnose = (reasons: PiProofReason[]) => void;
export interface PiNativeProverOptions { fs: { readFile(path: string): string }; piStateRoot: string; argvCensus?: () => Promise<string>; envProbe?: (pids: number[]) => Promise<string>; procArgs?: (pids: number[]) => Promise<Map<number, string | null>>; diagnose?: PiProofDiagnose }
// Reads the kernel's stored process args+env (sysctl KERN_PROCARGS2, mib
// {CTL_KERN=1, KERN_PROCARGS2=49, pid}) and prints ONLY `<pid>\t<token>` for
// the allowlisted OPENRIG_OCCUPANT_GENERATION when exactly one sanitized
// occurrence exists, else `<pid>\t`. No other byte is ever emitted.
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
export function makePiNativeProver(db: Database.Database, exec: (command: string) => Promise<string>, opts: PiNativeProverOptions) {
  const quote = (s: string) => "'" + s.replace(/'/g, "'\\''") + "'";
  const argvCensus = opts.argvCensus ?? (async () => (await run("ps", ["-axo", "pid=,ppid=,command="], { timeout: 2000, maxBuffer: 8 * 1024 * 1024 })).stdout);
  const envProbe = opts.envProbe ?? (async (pids: number[]) => (await run("/bin/ps", ["eww", "-p", pids.join(",")], { timeout: 2000, maxBuffer: 8 * 1024 * 1024 })).stdout);
  const GENERATION_ENV = "OPENRIG_OCCUPANT_GENERATION=";
  // Darwin: the kernel copy is authoritative because a Pi child rewrites its
  // process title, wiping the env region `ps eww` renders. Elsewhere (or on any
  // probe failure) the map stays empty and callers fall back to ps evidence.
  const procArgs = opts.procArgs ?? (process.platform !== "darwin" ? async () => new Map<number, string | null>() : async (pids: number[]) => {
    const map = new Map<number, string | null>();
    try {
      const { stdout } = await run("python3", ["-c", PROC_ARGS_PY, ...pids.map(String)], { timeout: 2000, maxBuffer: 64 * 1024 });
      for (const line of stdout.split("\n")) { const m = line.match(/^(\d+)\t(.*)$/); if (m) map.set(Number(m[1]), m[2] === "" ? null : m[2]!); }
    } catch { /* unavailable: ps fallback below decides */ }
    return map;
  });
  const diagnose = opts.diagnose ?? (() => {});
  // One null exit emits exactly one reason code. Counts and closed-vocabulary
  // detail only: never argv, env, path or pid text.
  const unknown = (code: PiProofReasonCode, extra?: { count?: number; detail?: PiProofReason["detail"] }): null => {
    diagnose([{ code, ...(extra?.count === undefined ? {} : { count: extra.count }), ...(extra?.detail === undefined ? {} : { detail: extra.detail }) }]);
    return null;
  };
  // Read the runner's own native quiescence record WITHOUT weakening the
  // identity proof: a missing, malformed or stale-binding record yields
  // settled: null (UNKNOWN), never true. Identity is still proven; only the
  // idle claim is withheld. Failures emit NO reason — an unknown quiescence is
  // not a proof anomaly, and the existing reason vocabulary stays closed.
  const readQuiescence = (state: PiRunnerSide, launchFlag: string, resumeToken: string | null, generation: string): PiQuiescenceProof => {
    const q = state?.quiescence;
    const unknownQuiescence: PiQuiescenceProof = { settled: null, observedAt: null };
    if (!q || typeof q !== "object") return unknownQuiescence;
    if (typeof q.settled !== "boolean") return unknownQuiescence;
    // Every binding is REQUIRED for idle credit, not merely checked when
    // present: a record missing its launch, generation, native session file or
    // cursor is unattributable, so it cannot be credited to this seat.
    if (q.launchId !== launchFlag) return unknownQuiescence;
    if (q.generation !== generation) return unknownQuiescence;
    if (q.sessionFile !== resumeToken) return unknownQuiescence;
    if ((q.lastEntryId ?? undefined) !== (state.lastEntryId ?? undefined)) return unknownQuiescence;
    // A timestamp alone proves nothing, and an unusable one is not evidence at
    //  all: require a parseable, well-formed, real-calendar instant. Date.parse
    //  silently normalises impossible dates (Feb 30 becomes Mar 2) rather than
    //  rejecting them, so the calendar triple is checked separately.
    if (typeof q.observedAt !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(q.observedAt)) return unknownQuiescence;
    const calendar = q.observedAt.slice(0, 10).split("-").map(Number) as [number, number, number];
    const canonical = new Date(Date.UTC(calendar[0], calendar[1] - 1, calendar[2]));
    if (canonical.getUTCFullYear() !== calendar[0] || canonical.getUTCMonth() !== calendar[1] - 1 || canonical.getUTCDate() !== calendar[2]) return unknownQuiescence;
    if (!Number.isFinite(Date.parse(q.observedAt))) return unknownQuiescence;
    return { settled: q.settled, observedAt: q.observedAt };
  };
  return async (session: string): Promise<PiNativeProof | null> => {
    const BINDING_SQL = `SELECT n.id AS nodeId,n.runtime,b.tmux_pane,b.tmux_session,t.generation_uuid,s.resume_token FROM sessions s JOIN nodes n ON n.id=s.node_id LEFT JOIN bindings b ON b.node_id=n.id JOIN occupant_tenures t ON t.node_id=n.id WHERE s.session_name=? AND n.runtime='pi' AND s.id=(SELECT MAX(s2.id) FROM sessions s2 WHERE s2.node_id=n.id) AND t.generation_ordinal=(SELECT MAX(x.generation_ordinal) FROM occupant_tenures x WHERE x.node_id=n.id)`;
    const bindingRows = (): Array<{ nodeId: string; runtime: string; tmux_pane: string | null; tmux_session: string | null; generation_uuid: string; resume_token: string | null }> =>
      db.prepare(BINDING_SQL).all(session) as Array<{ nodeId: string; runtime: string; tmux_pane: string | null; tmux_session: string | null; generation_uuid: string; resume_token: string | null }>;
    const read = () => { const rows = bindingRows(); return rows.length === 1 ? rows[0] : undefined; };
    const binding = read();
    if (!binding) {
      let count = -1;
      try { count = bindingRows().length; } catch { count = -1; }
      return unknown("binding_rows", { count });
    }
    const target = binding.tmux_pane ?? binding.tmux_session;
    if (!target) return unknown("no_pane_target");
    try {
      const sample = async (): Promise<PiNativeProof | null> => {
        const pane = (await exec(`tmux display-message -p -t ${quote(target)} '#{pane_id}|#{pane_pid}|#{pane_dead}'`)).trim();
        const match = pane.match(/^(%\d+)\|(\d+)\|([01])$/);
        if (!match) return unknown("tmux_probe_failed");
        const lines = (await argvCensus()).trim().split("\n");
        if (!lines.length) return unknown("census_empty");
        if (lines.some(l => !/^\d+\s+\d+\s+\S/.test(l.trim()))) return unknown("census_invalid");
        const parsed = lines.map(l => { const m = l.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/); return { pid: Number(m![1]), ppid: Number(m![2]), argv: m![3]! }; });
        if (new Set(parsed.map(r => r.pid)).size !== parsed.length) return unknown("duplicate_pids");
        const descendants = new Set<number>([Number(match[2])]); let changed = true;
        while (changed) { changed = false; for (const r of parsed) if (descendants.has(r.ppid) && !descendants.has(r.pid)) { descendants.add(r.pid); changed = true; } }
        const runners = parsed.filter(r => descendants.has(r.pid) && r.argv.includes("pi-runner.js") && new RegExp(`--session-name\\s+'?${session.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}'?(\\s|$)`).test(r.argv));
        if (!runners.length) {
          const root = parsed.find(r => r.pid === Number(match[2]));
          // Positive absence only: live shell root with no runner descendant,
          // or a dead retained pane. Anything ambiguous stays unknown.
          if (root && ['zsh', 'bash', 'sh', 'fish'].includes(path.basename(root.argv.split(" ")[0]!).replace(/^-/, "")) && !parsed.some(r => descendants.has(r.pid) && r.pid !== root.pid)) return { state: "absent", generation: binding.generation_uuid, launchId: null, fingerprint: JSON.stringify({ pane: match[1], rootShell: true }) };
          if (match[3] === "1") return { state: "absent", generation: binding.generation_uuid, launchId: null, fingerprint: JSON.stringify({ pane: match[1], deadPane: true }) };
          return unknown("census_invalid", { detail: "absent" });
        }
        if (runners.length !== 1) return unknown("runner_count", { count: runners.length });
        const runner = runners[0]!;
        const launchFlag = runner.argv.match(/--launch-id\s+(\S+)/)?.[1] ?? null;
        // The Pi child runs as node|bun executing the pi-coding-agent CLI entry
        // (argv[0] basename is NOT 'pi' in practice), always beneath the matched
        // runner specifically — never any pi process in the pane subtree.
        const childrenOf = new Map<number, typeof parsed>(); for (const r of parsed) { if (!childrenOf.has(r.ppid)) childrenOf.set(r.ppid, []); childrenOf.get(r.ppid)!.push(r); }
        const underRunner = new Set<number>([runner.pid]); let grew = true;
        while (grew) { grew = false; for (const pid of [...underRunner]) for (const c of childrenOf.get(pid) ?? []) if (!underRunner.has(c.pid)) { underRunner.add(c.pid); grew = true; } }
        const isPiEntrypoint = (argv: string) => /\bpi-coding-agent\S*\s/.test(argv + " ") && argv.includes("--mode") && /--mode\s+rpc/.test(argv) || path.basename(argv.split(" ")[0]!) === "pi";
        const piProcs = parsed.filter(r => underRunner.has(r.pid) && r.pid !== runner.pid && isPiEntrypoint(r.argv));
        if (piProcs.length !== 1) return unknown("pi_child_count", { count: piProcs.length });
        const piProc = piProcs[0]!;
        let state: PiRunnerSide; try { state = parsePiRunnerState(opts.fs.readFile(piSeatPaths(opts.piStateRoot, session).runnerStatePath)); } catch { return unknown("sidecar_unreadable"); }
        if (!state) return unknown("sidecar_unreadable");
        // A live runner AND live Pi child contradicting an exited sidecar marker
        // (e.g. a previous instance's marker during replacement startup) is
        // UNKNOWN, never positive absence: false absent would feed authority
        // logic exclusion evidence about a live coordinator. Genuine absence is
        // only the bare-shell / dead-pane forms established above.
        if (state.exited) return unknown("sidecar_exited");
        if (!state.ready) return unknown("sidecar_not_ready");
        if (!launchFlag || state.launchId !== launchFlag) return unknown("sidecar_launch_mismatch");
        if (state.sessionFile !== binding.resume_token || !binding.resume_token) return unknown("sidecar_session_file_mismatch");
        // No clock-freshness requirement: pi-runner writes the sidecar only on
        // state/event transitions, so a healthy IDLE runner legitimately carries
        // an old updatedAt. Identity binds through the per-launch minted launch-id
        // instead — argv and sidecar must show the SAME fresh instance uuid (a
        // prior or replacement process can never share it), plus double-stable
        // live lineage for runner and child and the get_state session token.
        // Targeted environment read: per selected process, ONLY the
        // occupant-generation token is extracted; every other byte is dropped.
        // Both the runner AND its Pi child must independently carry exactly one
        // generation equal to the node's latest tenure (allowlist confirmed at
        // adapters/pi-runner-protocol.ts PI_ENV_OPENRIG_VARS). Source order:
        // kernel KERN_PROCARGS2 (survives process-title rewriting), then the
        // legacy `ps eww` view. A kernel-confirmed absence is never contradicted
        // by ps; nothing is synthesized from parentage.
        const kernelGens = await procArgs([runner.pid, piProc.pid]);
        let psText: string | null = null;
        const genFor = async (pid: number): Promise<{ value: string | null; source: "kernel" | "ps" }> => {
          if (kernelGens.has(pid)) return { value: kernelGens.get(pid)!, source: "kernel" };
          if (psText === null) psText = await envProbe([runner.pid, piProc.pid]);
          const found = [...(psText.split("\n").find(l => l.trim().startsWith(pid + " ")) ?? "").matchAll(new RegExp(GENERATION_ENV + "(\\S+)", "g"))].map(m => m[1]!);
          return { value: found.length === 1 ? found[0]! : null, source: "ps" };
        };
        const runnerEv = await genFor(runner.pid), piEv = await genFor(piProc.pid);
        if (runnerEv.value !== binding.generation_uuid) return unknown("generation_unverified_runner", { detail: runnerEv.value === null ? "empty" : "ok" });
        if (piEv.value !== binding.generation_uuid) return unknown("generation_unverified_child", { detail: piEv.value === null ? "empty" : "ok" });
        return { state: "present", generation: binding.generation_uuid, launchId: launchFlag, lastEntryId: state.lastEntryId ?? null, fingerprint: JSON.stringify({ pane: match[1], runner: [runner.pid, runner.ppid], pi: [piProc.pid, piProc.ppid], launchId: launchFlag, sidecarUpdatedAt: state.updatedAt, genSources: [runnerEv.source, piEv.source] }), quiescence: readQuiescence(state, launchFlag, binding.resume_token, binding.generation_uuid) };
      };
      const first = await sample(), second = await sample();
      if (!first || !second) return unknown("unstable_between_samples");
      if (first.fingerprint !== second.fingerprint) return unknown("unstable_between_samples");
      if (JSON.stringify(read()) !== JSON.stringify(binding)) return unknown("binding_changed");
      return second;
    } catch (error) {
      // Only the error CLASS name; never the message, which can embed argv or env.
      const kind = (error as { constructor?: { name?: string } } | null | undefined)?.constructor?.name;
      diagnose([{ code: "exception", detail: kind === "TimeoutError" ? "timeout" : kind === "Error" ? "error" : "absent" }]);
      return null;
    }
  };
}
type PiRunnerSide = ReturnType<typeof parsePiRunnerState>;
export function makeCoordinatorRuntimeObserver(db: Database.Database, exec: (command: string) => Promise<string>, census = async () => (await run("ps", ["-axo", "pid=,ppid=,lstart=,comm="], { timeout: 2000, maxBuffer: 8 * 1024 * 1024 })).stdout, piProbe?: ((session: string) => Promise<PiNativeProof | null>) | null) {
 return async (session:string):Promise<RuntimeAvailability|null> => {
  const read=()=>{
   const rows=db.prepare(`SELECT DISTINCT n.id,n.runtime,b.tmux_pane,b.tmux_session,t.generation_uuid FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN bindings b ON b.node_id=n.id JOIN occupant_tenures t ON t.node_id=n.id WHERE s.session_name=? AND t.generation_ordinal=(SELECT MAX(x.generation_ordinal) FROM occupant_tenures x WHERE x.node_id=n.id)`).all(session) as Array<{id:string;runtime:string;tmux_pane:string|null;tmux_session:string|null;generation_uuid:string}>;
   return rows.length===1?rows[0]:undefined;
  };
  const binding=read();if(!binding)return null;
  // Pi seats prove through the shared native prover (the same proof serves the
  // coordinator recovery guard); an unconfigured prover stays unknown, never green.
  if (binding.runtime === 'pi') { if (!piProbe) return null; const proof = await piProbe(session); return proof ? { session, generation: proof.generation, state: proof.state, observedAt: Date.now(), fingerprint: proof.fingerprint, ...(proof.quiescence ? { quiescence: proof.quiescence } : {}) } : null; }
  if (!['codex','claude-code'].includes(binding.runtime)) return null;
  const target=binding.tmux_pane??binding.tmux_session;if(!target)return null;
  const quote=(s:string)=>"'"+s.replace(/'/g,"'\\''")+"'";
  try {
   const sample=async()=>{
    // Successful explicit pane metadata is required. Missing server/pane is unknown.
    const pane=(await exec(`tmux display-message -p -t ${quote(target)} '#{pane_id}|#{pane_pid}|#{pane_dead}'`)).trim();
    const match=pane.match(/^(%\d+)\|(\d+)\|([01])$/);if(!match)return null;
    const text=await census();const rows=text.trim().split('\n').map(line=>line.trim().match(/^(\d+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+(.+)$/));
    if(!rows.length||rows.some(row=>!row))return null;
    const parsed=rows.map(row=>({pid:Number(row![1]),ppid:Number(row![2]),started:row![3]!,comm:row![4]!}));
    if(new Set(parsed.map(r=>r.pid)).size!==parsed.length)return null;
    const descendants=new Set<number>([Number(match[2])]);let changed=true;while(changed){changed=false;for(const r of parsed)if(descendants.has(r.ppid)&&!descendants.has(r.pid)){descendants.add(r.pid);changed=true;}}
    const runtime=binding.runtime==='codex'?'codex':'claude';const native=parsed.filter(r=>descendants.has(r.pid)&&path.basename(r.comm)===runtime);
    // A live shell with no native descendant, or retained dead pane with no live
    // pane PID, is positive absence. Unknown root mismatch is not absence.
    const root=parsed.find(r=>r.pid===Number(match[2]));
    if(native.length===0&&parsed.some(r=>descendants.has(r.pid)&&r.pid!==Number(match[2])))return null;
    if(!root&&match[3]!=='1')return null;
    if(root&&native.length===0&&!['zsh','bash','sh','fish'].includes(path.basename(root.comm).replace(/^-(zsh|bash|sh|fish)$/, '$1')))return null;
    return {state:native.length?'present' as const:'absent' as const,fingerprint:JSON.stringify({pane,native: native.map(r=>[r.pid,r.ppid,r.started,r.comm]),root:root?[root.pid,root.ppid,root.started,root.comm]:null})};
   };
   const first=await sample(),second=await sample();if(!first||!second||first.fingerprint!==second.fingerprint||JSON.stringify(read())!==JSON.stringify(binding))return null;
   return {session,generation:binding.generation_uuid,state:second.state,observedAt:Date.now(),fingerprint:second.fingerprint};
  } catch {return null;}
 };
}
