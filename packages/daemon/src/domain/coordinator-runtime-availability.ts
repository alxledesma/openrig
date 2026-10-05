import type Database from "better-sqlite3";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { piSeatPaths, parsePiRunnerState } from "../adapters/pi-runner-protocol.js";
const run = promisify(execFile);
export interface RuntimeAvailability { session: string; generation: string; state: "present" | "absent" | "unknown"; observedAt: number; fingerprint: string }
/** Proof that the SAME managed Pi occupant is live: runner argv binds
 * --session-name/--launch-id, the runner's typed sidecar binds launchId and
 * the exact native session-file token with a current heartbeat, and the
 * target-process environment carries the genuine OPENRIG_OCCUPANT_GENERATION
 * equal to the node's latest occupant tenure. Environment text never leaves
 * this module except the single extracted generation token; fingerprints carry
 * only identifiers. */
export interface PiNativeProof { state: "present" | "absent"; generation: string; launchId: string | null; fingerprint: string }
export interface PiNativeProverOptions { fs: { readFile(path: string): string }; piStateRoot: string; argvCensus?: () => Promise<string>; envProbe?: (pids: number[]) => Promise<string> }
export function makePiNativeProver(db: Database.Database, exec: (command: string) => Promise<string>, opts: PiNativeProverOptions) {
  const quote = (s: string) => "'" + s.replace(/'/g, "'\\''") + "'";
  const argvCensus = opts.argvCensus ?? (async () => (await run("ps", ["-axo", "pid=,ppid=,command="], { timeout: 2000, maxBuffer: 8 * 1024 * 1024 })).stdout);
  const envProbe = opts.envProbe ?? (async (pids: number[]) => (await run("/bin/ps", ["eww", "-p", pids.join(",")], { timeout: 2000, maxBuffer: 8 * 1024 * 1024 })).stdout);
  const GENERATION_ENV = "OPENRIG_OCCUPANT_GENERATION=";
  return async (session: string): Promise<PiNativeProof | null> => {
    const read = () => {
      const rows = db.prepare(`SELECT n.id AS nodeId,n.runtime,b.tmux_pane,b.tmux_session,t.generation_uuid,s.resume_token FROM sessions s JOIN nodes n ON n.id=s.node_id LEFT JOIN bindings b ON b.node_id=n.id JOIN occupant_tenures t ON t.node_id=n.id WHERE s.session_name=? AND n.runtime='pi' AND s.id=(SELECT MAX(s2.id) FROM sessions s2 WHERE s2.node_id=n.id) AND t.generation_ordinal=(SELECT MAX(x.generation_ordinal) FROM occupant_tenures x WHERE x.node_id=n.id)`).all(session) as Array<{ nodeId: string; runtime: string; tmux_pane: string | null; tmux_session: string | null; generation_uuid: string; resume_token: string | null }>;
      return rows.length === 1 ? rows[0] : undefined;
    };
    const binding = read();
    if (!binding) return null;
    const target = binding.tmux_pane ?? binding.tmux_session;
    if (!target) return null;
    try {
      const sample = async (): Promise<PiNativeProof | null> => {
        const pane = (await exec(`tmux display-message -p -t ${quote(target)} '#{pane_id}|#{pane_pid}|#{pane_dead}'`)).trim();
        const match = pane.match(/^(%\d+)\|(\d+)\|([01])$/);
        if (!match) return null;
        const lines = (await argvCensus()).trim().split("\n");
        if (!lines.length || lines.some(l => !/^\d+\s+\d+\s+\S/.test(l.trim()))) return null;
        const parsed = lines.map(l => { const m = l.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/); return { pid: Number(m![1]), ppid: Number(m![2]), argv: m![3]! }; });
        if (new Set(parsed.map(r => r.pid)).size !== parsed.length) return null;
        const descendants = new Set<number>([Number(match[2])]); let changed = true;
        while (changed) { changed = false; for (const r of parsed) if (descendants.has(r.ppid) && !descendants.has(r.pid)) { descendants.add(r.pid); changed = true; } }
        const runners = parsed.filter(r => descendants.has(r.pid) && r.argv.includes("pi-runner.js") && new RegExp(`--session-name\\s+'?${session.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}'?(\\s|$)`).test(r.argv));
        if (!runners.length) {
          const root = parsed.find(r => r.pid === Number(match[2]));
          // Positive absence only: live shell root with no runner descendant,
          // or a dead retained pane. Anything ambiguous stays unknown.
          if (root && ['zsh', 'bash', 'sh', 'fish'].includes(path.basename(root.argv.split(" ")[0]!).replace(/^-/, "")) && !parsed.some(r => descendants.has(r.pid) && r.pid !== root.pid)) return { state: "absent", generation: binding.generation_uuid, launchId: null, fingerprint: JSON.stringify({ pane: match[1], rootShell: true }) };
          if (match[3] === "1") return { state: "absent", generation: binding.generation_uuid, launchId: null, fingerprint: JSON.stringify({ pane: match[1], deadPane: true }) };
          return null;
        }
        if (runners.length !== 1) return null;
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
        if (piProcs.length !== 1) return null;
        const piProc = piProcs[0]!;
        let state: PiRunnerSide; try { state = parsePiRunnerState(opts.fs.readFile(piSeatPaths(opts.piStateRoot, session).runnerStatePath)); } catch { return null; }
        if (!state) return null;
        // A live runner AND live Pi child contradicting an exited sidecar marker
        // (e.g. a previous instance's marker during replacement startup) is
        // UNKNOWN, never positive absence: false absent would feed authority
        // logic exclusion evidence about a live coordinator. Genuine absence is
        // only the bare-shell / dead-pane forms established above.
        if (state.exited) return null;
        if (!state.ready || state.launchId !== launchFlag || !launchFlag || state.sessionFile !== binding.resume_token || !binding.resume_token) return null;
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
        // adapters/pi-runner-protocol.ts PI_ENV_OPENRIG_VARS).
        const envText = await envProbe([runner.pid, piProc.pid]);
        const genFor = (pid: number): string | null => {
          const line = envText.split("\n").find(l => l.trim().startsWith(String(pid) + " "));
          if (!line) return null;
          const found = [...line.matchAll(new RegExp(GENERATION_ENV + "(\\S+)", "g"))].map(m => m[1]!);
          return found.length === 1 ? found[0]! : null;
        };
        const runnerGen = genFor(runner.pid), piGen = genFor(piProc.pid);
        if (runnerGen !== binding.generation_uuid || piGen !== binding.generation_uuid) return null;
        return { state: "present", generation: binding.generation_uuid, launchId: launchFlag, fingerprint: JSON.stringify({ pane: match[1], runner: [runner.pid, runner.ppid], pi: [piProc.pid, piProc.ppid], launchId: launchFlag, sidecarUpdatedAt: state.updatedAt }) };
      };
      const first = await sample(), second = await sample();
      if (!first || !second || first.fingerprint !== second.fingerprint || JSON.stringify(read()) !== JSON.stringify(binding)) return null;
      return second;
    } catch { return null; }
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
  if (binding.runtime === 'pi') { if (!piProbe) return null; const proof = await piProbe(session); return proof ? { session, generation: proof.generation, state: proof.state, observedAt: Date.now(), fingerprint: proof.fingerprint } : null; }
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
