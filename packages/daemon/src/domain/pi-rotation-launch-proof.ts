import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import type { NativeProcessRow } from "./native-process-lineage.js";
import { listNativeProcesses } from "./native-process-lineage.js";
import type { PiNativeProof } from "./coordinator-runtime-availability.js";
import type { PiRunnerState } from "../adapters/pi-runner-protocol.js";
import { resolveNativeTool } from "./codex-session-file-proof.js";
import type { PiVerifiedLaunch } from "./pi-rotation-native-proof.js";

const execFileAsync = promisify(execFile);
const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");
const isRecord = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const safePid = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 1;
const instant = (value: unknown): value is string => typeof value === "string"
  && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value)
  && Number.isFinite(Date.parse(value));

export interface PiRotationLaunchBinding {
  nodeId: string;
  sessionName: string;
  generation: string;
  runtime: string;
  pane: string;
  sessionFile: string | null;
}
export interface PiKernelLaunchObservation {
  publicIdentityMatches: boolean;
  runnerEntryMatches: boolean;
  runnerFlagsMatch: boolean;
  rpcChildMatches: boolean;
  sessionDirectoryMatches: boolean;
  agentDirectoryMatches: boolean;
  trustFlag: "approve" | "no-approve" | null;
}
export interface PiRotationLaunchExpected {
  nodeId: string;
  sessionName: string;
  generation: string;
  sessionFile: string;
  agentDir: string;
  runnerEntryPath: string;
  launchId: string;
}
export interface PiRotationLaunchDeps {
  tmux: { getPanePid(pane: string): Promise<number | null> };
  currentBinding(nodeId: string): Promise<PiRotationLaunchBinding | null>;
  /** Must be read from the daemon-owned runner sidecar, never from a request. */
  sidecar(sessionName: string): Promise<PiRunnerState | null>;
  processes?: () => Promise<NativeProcessRow[]>;
  /** Kernel-backed reader. Its result is a bounded allowlist, never raw argv/env. */
  kernelProcess?: (pid: number, role: "runner" | "child", expected: PiRotationLaunchExpected) => Promise<PiKernelLaunchObservation | null>;
  now?: () => number;
}

interface ProofFingerprint {
  pane: string;
  runner: [number, number];
  pi: [number, number];
  launchId: string;
}

function parseProofFingerprint(proof: PiNativeProof): ProofFingerprint | null {
  try {
    const value: unknown = JSON.parse(proof.fingerprint);
    if (!isRecord(value) || typeof value.pane !== "string" || typeof value.launchId !== "string"
      || !Array.isArray(value.runner) || value.runner.length !== 2 || !safePid(value.runner[0]) || !safePid(value.runner[1])
      || !Array.isArray(value.pi) || value.pi.length !== 2 || !safePid(value.pi[0]) || !safePid(value.pi[1])) return null;
    return { pane: value.pane, launchId: value.launchId, runner: [value.runner[0], value.runner[1]], pi: [value.pi[0], value.pi[1]] };
  } catch { return null; }
}

function matchesBinding(actual: PiRotationLaunchBinding | null, expected: PiRotationLaunchBinding): boolean {
  return !!actual && actual.nodeId === expected.nodeId && actual.sessionName === expected.sessionName
    && actual.generation === expected.generation && actual.runtime === "pi" && actual.pane === expected.pane
    && actual.sessionFile === expected.sessionFile;
}

function processByPid(rows: NativeProcessRow[], pid: number): NativeProcessRow | null {
  const matches = rows.filter((row) => row.pid === pid);
  return matches.length === 1 && matches[0]?.startedAt ? matches[0] : null;
}

function descendsFrom(rows: NativeProcessRow[], childPid: number, ancestorPid: number): boolean {
  const byPid = new Map(rows.map((row) => [row.pid, row]));
  if (byPid.size !== rows.length) return false;
  const seen = new Set<number>();
  let current = byPid.get(childPid);
  while (current && !seen.has(current.pid)) {
    if (current.pid === ancestorPid) return true;
    seen.add(current.pid);
    current = byPid.get(current.ppid);
  }
  return false;
}

function looksLikePiRpc(row: NativeProcessRow): boolean {
  const text = `${row.executableName ?? ""} ${row.command}`.toLowerCase();
  return /(?:^|\s|\/)pi(?:\s|$)/.test(text) || text.includes("pi-coding-agent");
}

function sameProcessSample(a: NativeProcessRow, b: NativeProcessRow): boolean {
  return a.pid === b.pid && a.ppid === b.ppid && a.startedAt === b.startedAt;
}

function stateMatches(input: {
  state: PiRunnerState | null;
  proof: PiNativeProof;
  expected: PiRotationLaunchExpected;
  childPid: number;
  nowMs: number;
}): boolean {
  const { state, proof, expected, childPid, nowMs } = input;
  if (!state || state.ready !== true || state.launchId !== expected.launchId || state.sessionFile !== expected.sessionFile) return false;
  if ((state.lastEntryId ?? null) !== (proof.lastEntryId ?? null)) return false;
  const rpc = state.rpcSessionFileProof;
  if (!rpc || rpc.launchId !== expected.launchId || rpc.generation !== expected.generation || !safePid(rpc.childPid) || rpc.childPid !== childPid
    || rpc.sessionFile !== expected.sessionFile || !/^(?:pi-runner-get-state|pi-runner-quiescence-refresh-\d+)$/.test(rpc.responseId)
    || !instant(rpc.observedAt)) return false;
  const responseAt = Date.parse(rpc.observedAt);
  if (responseAt > nowMs || nowMs - responseAt > 15_000) return false;
  const readiness = state.runtimeReadiness;
  if (!readiness || readiness.launchId !== expected.launchId || readiness.generation !== expected.generation
    || readiness.sessionFile !== expected.sessionFile || !instant(readiness.observedAt) || Date.parse(readiness.observedAt) > nowMs
    || Date.parse(readiness.observedAt) < responseAt
    || nowMs - Date.parse(readiness.observedAt) > 15_000 || !Array.isArray(readiness.failures)
    || readiness.failures.some(failure => !isRecord(failure) || typeof failure.code !== "string" || !instant(failure.observedAt))) return false;
  const quiescence = state.quiescence;
  if (!quiescence || quiescence.launchId !== expected.launchId || quiescence.generation !== expected.generation
    || quiescence.sessionFile !== expected.sessionFile || (quiescence.lastEntryId ?? null) !== (proof.lastEntryId ?? null)
    || typeof quiescence.settled !== "boolean" || quiescence.settled !== proof.quiescence?.settled
    || !instant(quiescence.observedAt) || quiescence.observedAt !== proof.quiescence?.observedAt) return false;
  const observedAt = Date.parse(quiescence.observedAt);
  return observedAt <= nowMs && nowMs - observedAt <= 15_000;
}

function sessionProofStateKey(state: PiRunnerState | null): string | null {
  if (!state) return null;
  return JSON.stringify({
    ready: state.ready, launchId: state.launchId, sessionFile: state.sessionFile, lastEntryId: state.lastEntryId,
    rpcSessionFileProof: state.rpcSessionFileProof,
    runtimeReadiness: state.runtimeReadiness ? {
      launchId: state.runtimeReadiness.launchId, generation: state.runtimeReadiness.generation,
      sessionFile: state.runtimeReadiness.sessionFile, observedAt: state.runtimeReadiness.observedAt,
      failures: state.runtimeReadiness.failures,
    } : null,
    quiescence: state.quiescence ? {
      launchId: state.quiescence.launchId, generation: state.quiescence.generation,
      sessionFile: state.quiescence.sessionFile, lastEntryId: state.quiescence.lastEntryId,
      settled: state.quiescence.settled, observedAt: state.quiescence.observedAt,
    } : null,
  });
}

const KERNEL_PI_PROOF = String.raw`import ctypes,json,os,struct,sys
def read(pid):
 if sys.platform=='darwin':
  libc=ctypes.CDLL(None); mib=(ctypes.c_int*3)(1,49,pid); n=ctypes.c_size_t(0)
  if libc.sysctl(mib,3,None,ctypes.byref(n),None,0)!=0 or n.value>8388608: raise ValueError()
  b=ctypes.create_string_buffer(n.value)
  if libc.sysctl(mib,3,b,ctypes.byref(n),None,0)!=0: raise ValueError()
  raw=b.raw[:n.value]; argc=struct.unpack_from('i',raw,0)[0]
  if argc<1 or argc>65536: raise ValueError()
  at=raw.index(b'\0',4)+1
  while at<len(raw) and raw[at]==0: at+=1
  argv=[]
  for _ in range(argc):
   end=raw.index(b'\0',at); argv.append(raw[at:end].decode('utf-8')); at=end+1
  env=raw[at:].split(b'\0')
 elif sys.platform.startswith('linux'):
  argv=[x.decode('utf-8') for x in open('/proc/'+str(pid)+'/cmdline','rb').read().rstrip(b'\0').split(b'\0')]
  env=open('/proc/'+str(pid)+'/environ','rb').read().split(b'\0')
 else: raise ValueError()
 return argv,env
def values(env,key):
 prefix=(key+'=').encode(); return [x[len(prefix):].decode('utf-8') for x in env if x.startswith(prefix)]
def oneflag(argv,flag):
 vals=[argv[i+1] for i,x in enumerate(argv[:-1]) if x==flag]
 return vals[0] if len(vals)==1 else None
try:
 pid=int(sys.argv[1]); role=sys.argv[2]; e=json.loads(sys.argv[3]); argv,env=read(pid)
 keys={'OPENRIG_NODE_ID':e['nodeId'],'OPENRIG_SESSION_NAME':e['sessionName'],'OPENRIG_OCCUPANT_GENERATION':e['generation'],'OPENRIG_RUNTIME':'pi'}
 identity=all(values(env,k)==[v] for k,v in keys.items())
 approve='--approve' in argv; noapprove='--no-approve' in argv
 trust='approve' if approve and not noapprove else ('no-approve' if noapprove and not approve else None)
 out={'publicIdentityMatches':identity,'runnerEntryMatches':False,'runnerFlagsMatch':False,'rpcChildMatches':False,'sessionDirectoryMatches':False,'agentDirectoryMatches':False,'trustFlag':trust}
 if role=='runner':
  out['runnerEntryMatches']=len(argv)>1 and argv[1]==e['runnerEntryPath']
  out['runnerFlagsMatch']=oneflag(argv,'--session-name')==e['sessionName'] and oneflag(argv,'--launch-id')==e['launchId'] and trust is not None
 elif role=='child':
  out['rpcChildMatches']=oneflag(argv,'--mode')=='rpc' and oneflag(argv,'--name')==e['sessionName'] and any('pi-coding-agent' in x for x in argv)
  out['sessionDirectoryMatches']=oneflag(argv,'--session-dir')==e['sessionDir'] and values(env,'PI_CODING_AGENT_SESSION_DIR')==[e['sessionDir']]
  out['agentDirectoryMatches']=values(env,'PI_CODING_AGENT_DIR')==[e['agentDir']]
 print(json.dumps(out,separators=(',',':')))
except:
 print('{}')`;

async function kernelProcessDefault(pid: number, role: "runner" | "child", expected: PiRotationLaunchExpected): Promise<PiKernelLaunchObservation | null> {
  if (!safePid(pid)) return null;
  try {
    const python = resolveNativeTool("python3");
    if (!python) return null;
    const { stdout } = await execFileAsync(python, ["-c", KERNEL_PI_PROOF, String(pid), role, JSON.stringify({
      ...expected, sessionDir: path.dirname(expected.sessionFile),
    })], { encoding: "utf8", timeout: 2_000, maxBuffer: 1_024 });
    const value: unknown = JSON.parse(stdout.trim());
    if (!isRecord(value) || typeof value.publicIdentityMatches !== "boolean" || typeof value.runnerEntryMatches !== "boolean"
      || typeof value.runnerFlagsMatch !== "boolean" || typeof value.rpcChildMatches !== "boolean"
      || typeof value.sessionDirectoryMatches !== "boolean" || typeof value.agentDirectoryMatches !== "boolean"
      || (value.trustFlag !== null && value.trustFlag !== "approve" && value.trustFlag !== "no-approve")) return null;
    return {
      publicIdentityMatches: value.publicIdentityMatches,
      runnerEntryMatches: value.runnerEntryMatches,
      runnerFlagsMatch: value.runnerFlagsMatch,
      rpcChildMatches: value.rpcChildMatches,
      sessionDirectoryMatches: value.sessionDirectoryMatches,
      agentDirectoryMatches: value.agentDirectoryMatches,
      trustFlag: value.trustFlag,
    };
  } catch { return null; }
}

/** Observe the exact managed Pi runner/child launch without mutating runtime state. */
export async function observePiRotationLaunch(
  input: { nodeId: string; sessionName: string; generation: string; sessionFile: string; agentDir: string; runnerEntryPath: string; proof: PiNativeProof },
  deps: PiRotationLaunchDeps,
): Promise<PiVerifiedLaunch | null> {
  const now = deps.now ?? Date.now;
  try {
    if (![input.nodeId, input.sessionName, input.generation, input.sessionFile, input.agentDir, input.runnerEntryPath].every(value => typeof value === "string" && value.length > 0)
      || !path.isAbsolute(input.sessionFile) || !path.isAbsolute(input.agentDir) || !path.isAbsolute(input.runnerEntryPath)) return null;
    const proof = input.proof;
    if (proof.state !== "present" || proof.generation !== input.generation || !proof.launchId) return null;
    const fingerprint = parseProofFingerprint(proof);
    if (!fingerprint || fingerprint.launchId !== proof.launchId) return null;
    const binding: PiRotationLaunchBinding = {
      nodeId: input.nodeId, sessionName: input.sessionName, generation: input.generation,
      runtime: "pi", pane: fingerprint.pane, sessionFile: input.sessionFile,
    };
    const sidecarBefore = await deps.sidecar(input.sessionName);
    const initialTime = now();
    if (!Number.isSafeInteger(initialTime) || !stateMatches({ state: sidecarBefore, proof, expected: { ...input, launchId: proof.launchId }, childPid: fingerprint.pi[0], nowMs: initialTime })) return null;
    const readProcesses = deps.processes ?? listNativeProcesses;
    const readKernel = deps.kernelProcess ?? kernelProcessDefault;
    const sample = async () => {
      const current = await deps.currentBinding(input.nodeId);
      if (!matchesBinding(current, binding)) return null;
      const panePid = await deps.tmux.getPanePid(binding.pane);
      if (!safePid(panePid)) return null;
      const rows = await readProcesses();
      const pane = processByPid(rows, panePid);
      const runner = processByPid(rows, fingerprint.runner[0]);
      const child = processByPid(rows, fingerprint.pi[0]);
      if (!pane || !runner || !child || runner.ppid !== fingerprint.runner[1] || child.ppid !== fingerprint.pi[1]
        || runner.pid !== fingerprint.runner[0] || child.pid !== fingerprint.pi[0]
        || !descendsFrom(rows, runner.pid, panePid) || child.ppid !== runner.pid
        || !looksLikePiRpc(child)) return null;
      const rpcCandidates = rows.filter(row => row.pid !== runner.pid && descendsFrom(rows, row.pid, runner.pid) && looksLikePiRpc(row));
      if (rpcCandidates.length !== 1 || rpcCandidates[0]?.pid !== child.pid) return null;
      const expected: PiRotationLaunchExpected = { ...input, launchId: proof.launchId! };
      const [runnerKernel, childKernel] = await Promise.all([
        readKernel(runner.pid, "runner", expected), readKernel(child.pid, "child", expected),
      ]);
      if (!runnerKernel || !childKernel || !runnerKernel.publicIdentityMatches || !childKernel.publicIdentityMatches
        || !runnerKernel.runnerEntryMatches || !runnerKernel.runnerFlagsMatch || !childKernel.rpcChildMatches
        || !childKernel.sessionDirectoryMatches || !childKernel.agentDirectoryMatches
        || !runnerKernel.trustFlag || runnerKernel.trustFlag !== childKernel.trustFlag) return null;
      return { panePid, pane, runner, child, trustFlag: childKernel.trustFlag };
    };
    const first = await sample();
    const second = await sample();
    if (!first || !second || first.panePid !== second.panePid || !sameProcessSample(first.pane, second.pane)
      || !sameProcessSample(first.runner, second.runner) || !sameProcessSample(first.child, second.child)
      || first.trustFlag !== second.trustFlag) return null;
    if (!matchesBinding(await deps.currentBinding(input.nodeId), binding)) return null;
    const state = await deps.sidecar(input.sessionName);
    const nowMs = now();
    if (!Number.isSafeInteger(nowMs) || sessionProofStateKey(sidecarBefore) !== sessionProofStateKey(state)
      || !stateMatches({ state, proof, expected: { ...input, launchId: proof.launchId }, childPid: second.child.pid, nowMs })) return null;
    const startFingerprint = sha256(JSON.stringify({
      pane: [second.pane.pid, second.pane.ppid, second.pane.startedAt],
      runner: [second.runner.pid, second.runner.ppid, second.runner.startedAt],
      child: [second.child.pid, second.child.ppid, second.child.startedAt],
    }));
    return {
      generation: input.generation, launchId: proof.launchId, sessionFile: input.sessionFile,
      pid: second.child.pid, startFingerprint, trustFlag: second.trustFlag,
    };
  } catch { return null; }
}
