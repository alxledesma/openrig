import { accessSync, closeSync, constants, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, readSync, realpathSync, statSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import type { TmuxAdapter } from "../adapters/tmux.js";
import { shellQuote } from "../adapters/shell-quote.js";

// Explicit public launch metadata, not a prefix/denylist over credential names.
// Provider keys and OPENRIG_ACTIVITY_HOOK_TOKEN keep their non-typed channel.
export const SEAT_PUBLIC_ENV_KEYS = [
  "OPENRIG_HOME", "OPENRIG_URL", "OPENRIG_HOST", "OPENRIG_PORT",
  "OPENRIG_NODE_ID", "OPENRIG_SESSION_NAME", "OPENRIG_RUNTIME",
  "OPENRIG_OCCUPANT_GENERATION", "OPENRIG_TRANSCRIPTS_LINES",
  "OPENRIG_TRANSCRIPTS_POLL_INTERVAL_SECONDS",
] as const;

export function publicSeatEnvironment(env: Readonly<Record<string, string | undefined>>): Record<string, string> {
  const result: Record<string, string> = {};
  for (const key of SEAT_PUBLIC_ENV_KEYS) if (env[key] !== undefined) result[key] = env[key]!;
  if (result.OPENRIG_URL) {
    try {
      const url = new URL(result.OPENRIG_URL);
      if (url.username || url.password || url.search || url.hash) throw new Error("Not a public URL");
    } catch {
      delete result.OPENRIG_URL;
      // The existing tmux environment channel still carries it. Never log its value.
      console.warn("[seat launch] URL omitted from typed metadata; retaining the existing environment channel.");
    }
  }
  return result;
}

/** Locate the CLI paired with this daemon, never a PATH-first global install.
 * The first layout is the bundled CLI; the second is the workspace build.
 */
function pairedCli(): string {
  for (const relative of ["../../../package.json", "../../../cli/package.json"]) {
    const manifest = path.resolve(import.meta.dirname, relative);
    try {
      const pkg = JSON.parse(readFileSync(manifest, "utf8"));
      if (pkg.name === "@openrig/cli" && typeof pkg.bin?.rig === "string") {
        return path.resolve(path.dirname(manifest), pkg.bin.rig);
      }
    } catch { /* try the other supported layout */ }
  }
  throw new Error("Paired CLI unavailable");
}

/** Pin npm Codex's interpreter as well as its entry file to the probe PATH.
 * Child tools still inherit the pane PATH. Unrecognised env shebang syntax
 * falls back to the old literal-PATH command instead of guessing its meaning.
 */
function codexEntry(searchPath: string, cwd: string): string {
  const executable = launchExecutable("codex", searchPath, cwd);
  const fd = openSync(executable, "r");
  let firstLine: string;
  try {
    const bytes = Buffer.alloc(512);
    firstLine = bytes.subarray(0, readSync(fd, bytes, 0, bytes.length, 0)).toString("utf8").split("\n")[0]!;
  } finally { closeSync(fd); }
  if (/^#!\s*\/usr\/bin\/env(?:\s|$)/.test(firstLine)) {
    if (!/^#![ \t]*\/usr\/bin\/env[ \t]+node[ \t]*\r?$/.test(firstLine)) throw new Error("Unsupported env shebang");
    return `${shellQuote(launchExecutable("node", searchPath, cwd))} ${shellQuote(executable)}`;
  }
  return shellQuote(executable);
}

/** Resolve exactly on the daemon launch PATH, without consulting the pane rc. */
export function launchExecutable(name: string, searchPath: string, cwd: string): string {
  for (const entry of searchPath.split(path.delimiter)) {
    const file = path.resolve(cwd, entry, name);
    try { accessSync(file, constants.X_OK); if (statSync(file).isFile()) return file; } catch { /* next entry */ }
  }
  throw new Error(`Seat launch requires ${name} on the daemon launch PATH.`);
}

export interface NativeDutyLaunchWrapper {
  enabled(nodeId: string): boolean;
  wrap(input: { nodeId: string; sessionName: string; generation: string; runtime: "codex" | "pi";
    harness: { executable: string; args: string[]; cwd: string } }): Promise<{ executable: string; args: string[]; cwd: string }>;
}

/** Resolve a native command without interpreting shell text. npm Codex's exact
 * env-node shebang pins BOTH its entry and daemon-selected interpreter.
 */
export function structuredNativeExecutable(name: string, args: string[], searchPath: string, cwd: string): { executable: string; args: string[]; cwd: string } {
  const executable = path.isAbsolute(name) ? name : launchExecutable(name, searchPath, cwd);
  accessSync(executable, constants.X_OK);
  if (!statSync(executable).isFile()) throw new Error("Native executable unavailable");
  if (name === "codex") {
    const fd = openSync(executable, "r"); let line: string;
    try { const bytes = Buffer.alloc(512); line = bytes.subarray(0, readSync(fd, bytes, 0, bytes.length, 0)).toString("utf8").split("\n")[0]!; }
    finally { closeSync(fd); }
    if (line.startsWith("#!")) {
      if (!/^#![ \t]*\/usr\/bin\/env[ \t]+node[ \t]*\r?$/.test(line)) throw new Error("Unsupported native shebang");
      return { executable: launchExecutable("node", searchPath, cwd), args: [executable, ...args], cwd };
    }
  }
  return { executable, args: [...args], cwd };
}

/** Reassert only public seat metadata after shell startup. Session identity is
 * read from tmux's launch environment; a successor's reserved identity wins.
 * Credentials stay in the inherited channel. An explicitly selected Codex home
 * is reasserted for Codex only; an unset selection retains the pane's defaults.
 */
export class SeatLaunchEnvironment {
  constructor(private readonly tmux: TmuxAdapter,
    private readonly sessionEnv: Readonly<Record<string, string | undefined>>,
    private readonly daemonCwd: string,
    private readonly cliPath?: string,
    private readonly codexHome?: string,
    private readonly nativeDuty?: NativeDutyLaunchWrapper) {}

  async usesNativeDuty(session: string, nodeId?: string): Promise<boolean> {
    if (!this.nativeDuty) return false;
    const id = nodeId ?? await this.tmux.getSessionEnv(session, "OPENRIG_NODE_ID");
    if (!id) throw new Error("Native duty opt-in requires actual node identity");
    return this.nativeDuty.enabled(id);
  }

  async structuredCommand(session: string, harness: { executable: string; args: string[]; cwd: string },
    target: { nodeId?: string; generation?: string; runtime: "codex" | "pi" }): Promise<string> {
    // This is an admission gate, deliberately outside command()'s best-effort catch.
    if (!this.nativeDuty || !await this.usesNativeDuty(session, target.nodeId)) throw new Error("Native duty opt-in required");
    const shell = path.basename(await this.tmux.getPaneCommand(session) ?? "").replace(/^-/, "");
    if (!["sh", "bash", "zsh"].includes(shell)) throw new Error("Unsupported native duty pane shell");
    const identity: Record<string, string | undefined> = {};
    for (const key of ["OPENRIG_NODE_ID", "OPENRIG_SESSION_NAME", "OPENRIG_RUNTIME", "OPENRIG_OCCUPANT_GENERATION"]) identity[key] = await this.tmux.getSessionEnv(session, key);
    if (!identity.OPENRIG_NODE_ID || !identity.OPENRIG_SESSION_NAME || (target.nodeId && target.nodeId !== identity.OPENRIG_NODE_ID)) throw new Error("Native duty identity unavailable");
    if (target.generation !== undefined) identity.OPENRIG_OCCUPANT_GENERATION = target.generation;
    identity.OPENRIG_RUNTIME = target.runtime;
    if (!identity.OPENRIG_OCCUPANT_GENERATION || !this.sessionEnv.PATH || !path.isAbsolute(harness.cwd)) throw new Error("Native duty launch binding unavailable");
    const resolved = structuredNativeExecutable(harness.executable, harness.args, this.sessionEnv.PATH, harness.cwd);
    const wrapped = await this.nativeDuty.wrap({ nodeId: identity.OPENRIG_NODE_ID, sessionName: identity.OPENRIG_SESSION_NAME,
      generation: identity.OPENRIG_OCCUPANT_GENERATION, runtime: target.runtime, harness: resolved });
    if (!path.isAbsolute(wrapped.executable) || wrapped.cwd !== harness.cwd || !Array.isArray(wrapped.args)
      || wrapped.args.some(arg => typeof arg !== "string" || arg.includes("\0"))) throw new Error("Native duty wrapper contract mismatch");
    const binDir = this.rigBin();
    const env = publicSeatEnvironment({ OPENRIG_TRANSCRIPTS_LINES: "", OPENRIG_TRANSCRIPTS_POLL_INTERVAL_SECONDS: "", ...this.sessionEnv, ...identity });
    if (target.runtime === "codex" && this.codexHome) env.CODEX_HOME = this.codexHome;
    const assignments = Object.entries(env).map(([key, value]) => shellQuote(`${key}=${value}`));
    // The managed Pi runner resolves its child toolchain after the pane's rc
    // has run. Keep that selection on the same PATH used to resolve the runner;
    // otherwise an rc-selected wrapper/interpreter defeats the launch contract.
    // Retain the pane's extra tools after the daemon-selected toolchain.
    const managedPath = target.runtime === "pi" ? `${binDir}${path.delimiter}${this.sessionEnv.PATH}` : binDir;
    return `/usr/bin/env ${assignments.join(" ")} PATH=${shellQuote(managedPath)}:"$PATH" ${[wrapped.executable, ...wrapped.args].map(shellQuote).join(" ")}`;
  }

  private rigBin(): string {
    const cli = realpathSync(this.cliPath ?? pairedCli());
    accessSync(cli, constants.X_OK);
    if (!statSync(cli).isFile() || !this.sessionEnv.OPENRIG_HOME) throw new Error("Paired CLI or instance home unavailable");
    const node = realpathSync(process.execPath);
    accessSync(node, constants.X_OK);
    if (!statSync(node).isFile()) throw new Error("Paired CLI interpreter unavailable");
    // Pin the running daemon's interpreter, not the tool shell's PATH. Version
    // the launcher semantics as well as both paths; old active aliases stay put.
    const launcher = `#!/bin/sh\nexec ${shellQuote(node)} ${shellQuote(cli)} "$@"\n`;
    const identity = JSON.stringify(["paired-node-cli-v1", node, cli, launcher]);
    const bin = path.resolve(this.daemonCwd, this.sessionEnv.OPENRIG_HOME, "run", "seat-bin", createHash("sha256").update(identity).digest("hex"));
    mkdirSync(bin, { recursive: true });
    const entry = path.join(bin, "rig");
    try { writeFileSync(entry, launcher, { flag: "wx", mode: 0o700 }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    const stat = lstatSync(entry);
    if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o022) !== 0
      || readFileSync(entry, "utf8") !== launcher || readdirSync(bin).some(name => name !== "rig")) throw new Error("Seat bin is not the paired rig-only launcher");
    accessSync(entry, constants.X_OK);
    return bin;
  }

  async command(session: string, command: string, target: { codexCwd?: string; nodeId?: string; generation?: string; runtime?: string } = {}): Promise<string> {
    if (await this.usesNativeDuty(session, target.nodeId)) throw new Error("Opted-in launch requires structured native argv");
    const searchPath = this.sessionEnv.PATH;
    const codexEnv = target.codexCwd !== undefined
      ? [searchPath ? `PATH=${shellQuote(searchPath)}` : "", this.codexHome ? `CODEX_HOME=${shellQuote(this.codexHome)}` : ""].filter(Boolean)
      : [];
    const fallback = codexEnv.length ? `env ${codexEnv.join(" ")} ${command}` : command;
    try {
      // Nushell does not expand "$PATH". Keep its pre-existing literal command.
      const shell = path.basename(await this.tmux.getPaneCommand(session) ?? "").replace(/^-/, "");
      if (shell === "nu" || shell === "nu.exe") throw new Error("Non-POSIX pane");
      const identity: Record<string, string | undefined> = {};
      for (const key of ["OPENRIG_NODE_ID", "OPENRIG_SESSION_NAME", "OPENRIG_RUNTIME", "OPENRIG_OCCUPANT_GENERATION"]) {
        identity[key] = await this.tmux.getSessionEnv(session, key);
      }
      if (!identity.OPENRIG_NODE_ID || !identity.OPENRIG_SESSION_NAME) {
        throw new Error("Seat launch requires the session's OpenRig identity.");
      }
      if (target.nodeId !== undefined && identity.OPENRIG_NODE_ID !== target.nodeId) {
        throw new Error("Seat launch identity differs from the intended node.");
      }
      // Handover respawns an existing pane with -e; tmux's session environment
      // still names the predecessor. The caller owns the reserved generation.
      if (target.runtime !== undefined) identity.OPENRIG_RUNTIME = target.runtime;
      if (target.generation !== undefined) identity.OPENRIG_OCCUPANT_GENERATION = target.generation;
      // Codex help/preflight uses the daemon PATH. Preserve its executable and
      // interpreter selection while child tools retain the user's PATH.
      if (target.codexCwd !== undefined) {
        if (!searchPath || !command.startsWith("codex ")) throw new Error("Expected a Codex launch command and PATH.");
        command = codexEntry(searchPath, target.codexCwd) + command.slice(5);
      }
      const binDir = this.rigBin();
      const env = publicSeatEnvironment({ OPENRIG_TRANSCRIPTS_LINES: "", OPENRIG_TRANSCRIPTS_POLL_INTERVAL_SECONDS: "", ...this.sessionEnv, ...identity });
      if (target.codexCwd !== undefined && this.codexHome) env.CODEX_HOME = this.codexHome;
      // Classic Claude may be an rc alias or function, not a PATH executable.
      // Its caller sources the staged command in a pane-shell subshell. Leading
      // assignments preserve shell lookup; /usr/bin/env would bypass it.
      if (target.runtime === "claude-code") {
        const assignments = Object.entries(env).map(([key, value]) => `${key}=${shellQuote(value)}`);
        return `${assignments.join(" ")} PATH=${shellQuote(binDir)}:"$PATH" ${command}`;
      }
      const assignments = Object.entries(env).map(([key, value]) => shellQuote(`${key}=${value}`));
      return `/usr/bin/env ${assignments.join(" ")} PATH=${shellQuote(binDir)}:"$PATH" ${command}`;
    } catch {
      // This best-effort correction is not a new launch admission gate. Do not
      // include error details: transport errors may contain environment values.
      console.warn("[seat launch] Environment correction unavailable; using the previous launch command.");
      return fallback;
    }
  }
}
