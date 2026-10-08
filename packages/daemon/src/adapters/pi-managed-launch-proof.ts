/** Trusted-local managed Pi launch evidence. Never an independent JSON attestation. */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { lstatSync, readFileSync, realpathSync } from "node:fs";
import path from "node:path";
import { SERVICE_PROCESS_PY } from "../domain/peer-service-disposition.js";
import { buildPiChildArgs, buildPiRunnerArgs, piSeatPaths, type PiRunnerLaunchOpts } from "./pi-runner-protocol.js";

export interface PiLaunchArtifact {
  path: string; device: string; inode: string; size: number; mtime: string; ctime: string; sha256: string;
}
export interface PiLaunchProcess {
  instance: { pid: number; ppid: number; uid: number; boot: string; start: string };
  image: PiLaunchArtifact & { compiled: boolean };
  identity: Record<string, string | null>;
  agentDir: string | null; sessionsDir: string | null;
  argv: string[];
}
export interface PiManagedSpawnProof {
  version: 1;
  nodeId: string; generation: string;
  intent: PiRunnerLaunchOpts;
  runner: PiLaunchProcess["instance"];
  child: PiLaunchProcess["instance"];
  runnerImage: PiLaunchProcess["image"];
  childImage: PiLaunchProcess["image"];
  runnerEntry: PiLaunchArtifact;
  piEntry: PiLaunchArtifact;
  interpreter: PiLaunchArtifact;
  childArgs: string[];
}
const digest = (b: Buffer) => createHash("sha256").update(b).digest("hex");
export function piLaunchArtifact(file: string): PiLaunchArtifact {
  const canonical = realpathSync(file), before = lstatSync(canonical, { bigint: true });
  if (!path.isAbsolute(file) || !before.isFile() || (before.mode & 0o022n) !== 0n) throw Error("Untrusted launch artifact");
  const bytes = readFileSync(canonical), after = lstatSync(canonical, { bigint: true });
  if (before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size || before.mtimeNs !== after.mtimeNs || before.ctimeNs !== after.ctimeNs) throw Error("Launch artifact changed");
  return { path: canonical, device: String(after.dev), inode: String(after.ino), size: Number(after.size), mtime: String(after.mtimeNs), ctime: String(after.ctimeNs), sha256: digest(bytes) };
}
const PROCESS_PROJECTION = String.raw`
pid=int(sys.argv[1]); before=kernel_instance(pid)
argv,env,executable=service_argv_env(pid); image=image_stamp(executable); identity,loader=inherited_identity(env)
def directory(key):
 vals=[v[len(key)+1:].decode('utf8') for v in env if v.startswith((key+'=').encode())]
 if len(vals)>1: raise ValueError()
 return vals[0] if vals else None
if loader or kernel_instance(pid)!=before: raise ValueError()
print(json.dumps({'instance':before,'image':image,'identity':identity,'agentDir':directory('PI_CODING_AGENT_DIR'),'sessionsDir':directory('PI_CODING_AGENT_SESSION_DIR'),'argv':argv},separators=(',',':')))
`;
/** Internal only: raw argv is never persisted or exposed through an API. */
export function readPiLaunchProcess(pid: number): PiLaunchProcess {
  if (!Number.isSafeInteger(pid) || pid <= 1) throw Error("Invalid launch pid");
  const value = JSON.parse(execFileSync('/usr/bin/env', ['python3', '-c', SERVICE_PROCESS_PY + PROCESS_PROJECTION, String(pid)], {
    encoding: 'utf8', timeout: 2000, maxBuffer: 65536,
  })) as PiLaunchProcess;
  if (value.instance.pid !== pid || value.instance.uid !== process.getuid?.() || !value.image.compiled || !Array.isArray(value.argv)) throw Error("Invalid launch process");
  return value;
}
const equal = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);
function artifactMatches(image: PiLaunchArtifact, artifact: PiLaunchArtifact): boolean {
  return ['path','device','inode','size','mtime','ctime','sha256'].every(k => (image as unknown as Record<string,unknown>)[k] === (artifact as unknown as Record<string,unknown>)[k]);
}
export function managedPiChildArgs(intent: PiRunnerLaunchOpts): string[] {
  const dirs = piSeatPaths(intent.stateRoot, intent.sessionName);
  return [...buildPiChildArgs({ ...intent, sessionsDir: dirs.sessionsDir }), '--extension', path.join(path.dirname(intent.runnerEntryPath), 'pi-bounded-compaction-extension.ts')];
}
function identityMatches(row: PiLaunchProcess, proof: PiManagedSpawnProof): boolean {
  return row.identity.OPENRIG_NODE_ID === proof.nodeId && row.identity.OPENRIG_SESSION_NAME === proof.intent.sessionName
    && row.identity.OPENRIG_OCCUPANT_GENERATION === proof.generation && row.identity.OPENRIG_RUNTIME === 'pi';
}
/** Independently corroborate a producer record with live kernel observations and installed files. */
export function validatePiManagedSpawnProof(proof: PiManagedSpawnProof, expected: {
  nodeId: string; generation: string; sessionName: string; launchId: string; agentDir: string; sessionFile: string; runnerEntryPath: string;
}, runner: PiLaunchProcess, child: PiLaunchProcess): boolean {
  try {
    if (!proof || proof.version !== 1 || proof.nodeId !== expected.nodeId || proof.generation !== expected.generation
      || proof.intent.runtime === 'omp' || proof.intent.sessionName !== expected.sessionName || proof.intent.launchId !== expected.launchId
      || proof.intent.runnerEntryPath !== expected.runnerEntryPath || !path.isAbsolute(proof.intent.stateRoot) || !path.isAbsolute(proof.intent.cwd)
      || !['approve','no-approve'].includes(proof.intent.trust) || (proof.intent.sessionFile && proof.intent.forkRef)
      || (proof.intent.sessionFile && proof.intent.sessionFile !== expected.sessionFile)) return false;
    const dirs = piSeatPaths(proof.intent.stateRoot, proof.intent.sessionName);
    if (dirs.agentDir !== expected.agentDir || dirs.sessionsDir !== path.dirname(expected.sessionFile)
      || !equal(proof.runner, runner.instance) || !equal(proof.child, child.instance)
      || child.instance.ppid !== runner.instance.pid || child.instance.start === ''
      || !equal(proof.runnerImage, runner.image) || !equal(proof.childImage, child.image)
      || !identityMatches(runner, proof) || !identityMatches(child, proof)
      || child.agentDir !== dirs.agentDir || child.sessionsDir !== dirs.sessionsDir) return false;
    if (!equal(piLaunchArtifact(expected.runnerEntryPath), proof.runnerEntry)
      || !equal(piLaunchArtifact(proof.piEntry.path), proof.piEntry)
      || !equal(piLaunchArtifact(proof.interpreter.path), proof.interpreter)
      || !artifactMatches(child.image, proof.interpreter)
      || !/^#!\/usr\/bin\/env node\r?\n/.test(readFileSync(proof.piEntry.path, 'utf8'))) return false;
    const parentArgs = buildPiRunnerArgs(proof.intent);
    if (runner.argv.length !== parentArgs.length + 1 || !equal(runner.argv.slice(1), parentArgs)) return false;
    if (!equal(proof.childArgs, managedPiChildArgs(proof.intent))) return false;
    const nonempty = child.argv.filter(Boolean);
    const erased = nonempty.length === 1 && nonempty[0] === 'pi';
    const original = nonempty.length === proof.childArgs.length + 2
      && realpathSync(nonempty[0]!) === proof.interpreter.path && nonempty[1] === proof.piEntry.path
      && equal(nonempty.slice(2), proof.childArgs);
    return erased || original;
  } catch { return false; }
}
/** Called only after success on the current spawn handle's get_state pipe. */
export function capturePiManagedSpawnProof(input: {
  nodeId: string; generation: string; intent: PiRunnerLaunchOpts; childPid: number;
  piEntry: PiLaunchArtifact; interpreter: PiLaunchArtifact; childArgs: string[];
}, read = readPiLaunchProcess): PiManagedSpawnProof | null {
  try {
    const runner = read(process.pid), child = read(input.childPid);
    const proof: PiManagedSpawnProof = { version: 1, nodeId: input.nodeId, generation: input.generation, intent: input.intent,
      runner: runner.instance, child: child.instance, runnerImage: runner.image, childImage: child.image,
      runnerEntry: piLaunchArtifact(input.intent.runnerEntryPath), piEntry: input.piEntry, interpreter: input.interpreter, childArgs: input.childArgs };
    const dirs = piSeatPaths(input.intent.stateRoot, input.intent.sessionName);
    if (!validatePiManagedSpawnProof(proof, { ...input, ...input.intent, agentDir: dirs.agentDir,
      sessionFile: input.intent.sessionFile ?? path.join(dirs.sessionsDir, 'pending.jsonl') }, runner, child)) return null;
    if (!equal(runner, read(process.pid)) || !equal(child, read(input.childPid))) return null;
    return proof;
  } catch { return null; }
}
