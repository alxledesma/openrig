// Daemon-owned Pi INSTALLATION provenance resolver for the explicit legacy bridge.
//
// The live Pi child cannot describe itself: its bundle entry calls
// `process.title = APP_NAME`, so the census command line is the bare string `pi`
// and every argv-derived binding is structurally impossible. This resolver
// therefore answers from TRUSTED INSTALLATION PROVENANCE instead — the executable
// the daemon itself launches, followed through that installation's own import
// graph — and returns exactly ONE canonical module URL that both required
// classes come from.
//
// What this file deliberately does NOT do:
//   - no hardcoded chunk name, hash, suffix or ordering; the module is FOUND by
//     walking the installation's static import graph and checking real exports;
//   - no caller path, argv, port or pid input, and no network, eval or target code;
//   - no "first plausible module" rule: zero or several qualifying modules both
//     refuse with a closed reason, because a wrong module cannot be corrected
//     later by the caller.
//   - no touching of the witness's own loaded-URL gate: this resolver only ever
//     proposes a URL, and the transport still proves the target already parsed it
//     and exports the required names.
import { accessSync, constants as fsConstants, readFileSync, realpathSync, statSync } from "node:fs";
import { createHash } from "node:crypto";
import { containsForbiddenGraphToken, filePredatesStart } from "./legacy-pi-native-provenance.js";
import { delimiter, dirname, isAbsolute, join, resolve as resolvePath } from "node:path";
import { pathToFileURL } from "node:url";

/** The two class exports the witness reads from ONE module. Fixed by the witness
 *  contract; never caller-supplied and never discovered at runtime. */
export const LEGACY_PI_REQUIRED_EXPORTS = ["AgentSessionRuntime", "AgentSession"] as const;

/** Closed vocabulary for every refusal. */
export const PI_INSTALLATION_RESOLVER_REASONS = [
  "executable_unresolved",
  "entry_module_unresolved",
  "installation_root_unresolved",
  "module_graph_unreadable",
  "module_graph_outside_installation",
  "required_export_absent",
  "required_export_ambiguous",
  "module_graph_forbidden_token",
  "module_graph_newer_than_target",
] as const;
export type PiInstallationResolverReason = (typeof PI_INSTALLATION_RESOLVER_REASONS)[number];

export interface PiInstallationFsOps {
  /** Canonical path for an existing file, or null when it does not resolve. */
  realpath(path: string): string | null;
  isFile(path: string): boolean;
  isExecutable(path: string): boolean;
  readFile(path: string): string | null;
}

const nodeFsOps: PiInstallationFsOps = {
  realpath: path => { try { return realpathSync(path); } catch { return null; } },
  isFile: path => { try { return statSync(path).isFile(); } catch { return false; } },
  isExecutable: path => { try { accessSync(path, fsConstants.X_OK); return true; } catch { return false; } },
  readFile: path => { try { return readFileSync(path, "utf-8"); } catch { return null; } },
};

export interface PiInstallationResolverInput {
  /** The executable the daemon launches as the Pi runtime: an absolute path, or a
   *  bare name resolved against the supplied PATH. Daemon-owned configuration only. */
  executable: string;
  /** PATH used for a bare executable name. Daemon-owned configuration only. */
  pathEnv?: string;
  fs?: PiInstallationFsOps;
  /** Bounded cap on graph walk breadth. The installation's own entry graph is
   *  small and finite; a cap turns a pathological tree into a refusal, not a scan. */
  maxModules?: number;
  /** Start time of the target that LOADED this installation, in epoch milliseconds.
   *  When supplied, every graph file must predate it: a package replaced after the
   *  child started would otherwise let today's bytes describe different code than the
   *  code that is running. */
  targetStartedAtMs?: number;
  /** Refuse a graph that could change or intercept the inspector itself. Defaults on. */
  scanForbiddenTokens?: boolean;
}

export interface PiInstallationResolverResult {
  moduleUrl: string;
  /** Canonical entry the daemon actually launches, for the receipt. */
  entryUrl: string;
  /** Canonical package root that owns the entry, plus its declared identity. */
  installationRoot: string;
  packageName: string | null;
  packageVersion: string | null;
  /** How many canonical modules the graph walk visited. */
  modulesVisited: number;
  /** Digest over every graph file's canonical path and content, so a receipt can be
   *  tied to the exact graph that was proved rather than to a version string. */
  graphHash: string;
}

const DEFAULT_MAX_MODULES = 512;
/** Relative specifiers only: a bare or absolute specifier leaves the trusted
 *  installation graph and is therefore not evidence of this installation. */
const RELATIVE_SPECIFIER = /^\.{1,2}\//;

/** Import edges that are literal at the source level: static import/export-from,
 *  a literal dynamic import(), and the CommonJS-shaped literal call the real bundle
 *  entry uses (`createRequire(import.meta.url)("./cli-runtime.js")`). Dynamic
 *  import() of a computed string and re-export stars are deliberately NOT followed:
 *  they are not part of the deterministic graph, and a runtime-conditional edge
 *  cannot be qualified before the target runs. */
function staticSpecifiers(source: string): string[] {
  const out: string[] = [];
  const patterns = [
    /\bfrom\s*["']([^"']+)["']/g,
    /\bimport\s*["']([^"']+)["']/g,
    /\bimport\s*\(\s*["']([^"']+)["']\s*\)/g,
    /\brequire\s*\(\s*["']([^"']+)["']\s*\)/g,
    // A require-shaped call whose base is any expression, e.g. createRequire(import.meta.url)("./x.js").
    /\)\s*\(\s*["']([^"']+)["']\s*\)/g,
  ];
  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      const specifier = match[1];
      if (specifier && RELATIVE_SPECIFIER.test(specifier)) out.push(specifier);
    }
  }
  return out;
}

/** True when `name` is genuinely EXPORTED by this module's source: either a direct
 *  `export class/function/const …` declaration or an export clause naming it. */
function exportsName(source: string, name: string): boolean {
  const direct = new RegExp(`\\bexport\\s+(?:default\\s+)?(?:abstract\\s+)?class\\s+${name}\\b`).test(source)
    || new RegExp(`\\bexport\\s+(?:async\\s+)?function\\s*\\*?\\s*${name}\\b`).test(source)
    || new RegExp(`\\bexport\\s+(?:const|let|var)\\s+${name}\\b`).test(source);
  if (direct) return true;
  for (const clause of source.matchAll(/\bexport\s*\{([^}]*)\}/g)) {
    for (const part of clause[1]!.split(",")) {
      const pieces = part.split(/\s+as\s+/).map(p => p.trim()).filter(Boolean);
      if (pieces.some(piece => piece === name)) return true;
    }
  }
  return false;
}

/** True when this source DEFINES `name`, so an export cannot be a bare re-export of
 *  a module that is not part of the graph we proved. */
function definesName(source: string, name: string): boolean {
  return new RegExp(`(?:^|[^\\w$.])${name}\\s*=\\s*class\\b`).test(source)
    || new RegExp(`\\bclass\\s+${name}\\b`).test(source);
}

function resolveExecutable(executable: string, pathEnv: string | undefined, fs: PiInstallationFsOps): string | null {
  const candidates = isAbsolute(executable)
    ? [executable]
    : (pathEnv ?? "").split(delimiter).filter(dir => dir !== "").map(dir => join(dir, executable));
  for (const candidate of candidates) {
    if (!fs.isFile(candidate)) continue;
    // A PATH shim is resolved through its own realpath, exactly as the runner
    // resolves a mise shim before it may execute one.
    const canonical = fs.realpath(candidate);
    if (!canonical) continue;
    if (!isAbsolute(canonical)) continue;
    if (!executable.startsWith("/") && !fs.isExecutable(canonical)) continue;
    return canonical;
  }
  return null;
}

/** Nearest ancestor carrying a package.json: the installation that owns the entry. */
function installationRootOf(entryPath: string, fs: PiInstallationFsOps): { root: string; name: string | null; version: string | null } | null {
  let cursor = dirname(entryPath);
  for (let hop = 0; hop < 8; hop += 1) {
    const manifest = join(cursor, "package.json");
    if (fs.isFile(manifest)) {
      const text = fs.readFile(manifest);
      if (text === null) return null;
      let name: string | null = null, version: string | null = null;
      try {
        const parsed = JSON.parse(text) as { name?: unknown; version?: unknown };
        if (typeof parsed.name === "string") name = parsed.name;
        if (typeof parsed.version === "string") version = parsed.version;
      } catch { return null; }
      return { root: cursor, name, version };
    }
    const parent = dirname(cursor);
    if (parent === cursor) break;
    cursor = parent;
  }
  return null;
}

/**
 * Resolve the ONE canonical module URL that both required classes come from,
 * through the installation's own static import graph.
 *
 * Refuses — never approximates — when the executable, the entry, the package root,
 * the graph or the qualifying module cannot be established unambiguously.
 */
export function resolvePiInstallationModule(input: PiInstallationResolverInput): { ok: true; value: PiInstallationResolverResult } | { ok: false; reason: PiInstallationResolverReason } {
  const fs = input.fs ?? nodeFsOps;
  const maxModules = input.maxModules ?? DEFAULT_MAX_MODULES;
  const entry = resolveExecutable(input.executable, input.pathEnv, fs);
  if (!entry) return { ok: false, reason: "executable_unresolved" };
  const root = installationRootOf(entry, fs);
  if (!root) return { ok: false, reason: "installation_root_unresolved" };

  const entryUrl = pathToFileURL(entry).href;
  const visited = new Map<string, string>();
  const queue: string[] = [entry];
  visited.set(entry, fs.readFile(entry) ?? "");
  let sawUnreadable = false;
  while (queue.length > 0) {
    if (visited.size > maxModules) return { ok: false, reason: "module_graph_unreadable" };
    const current = queue.shift()!;
    const source = visited.get(current);
    if (source === undefined) continue;
    for (const specifier of staticSpecifiers(source)) {
      const candidate = fs.realpath(resolvePath(dirname(current), specifier));
      // An edge that leaves the trusted installation, or names nothing on disk,
      // cannot be evidence for this installation either way.
      if (!candidate || !(candidate === root.root || candidate.startsWith(`${root.root}/`))) continue;
      if (visited.has(candidate)) continue;
      const text = fs.readFile(candidate);
      if (text === null) { sawUnreadable = true; continue; }
      visited.set(candidate, text);
      queue.push(candidate);
    }
  }
  if (visited.size <= 1 && sawUnreadable) return { ok: false, reason: "module_graph_unreadable" };

  // Every file in the proved graph must predate the target that loaded it, and none may
  // carry a token that could change or intercept the inspector. Both are properties of
  // the graph that was actually walked, not of the package as a whole.
  if (input.targetStartedAtMs !== undefined) {
    for (const path of visited.keys()) {
      if (!filePredatesStart(path, input.targetStartedAtMs)) return { ok: false, reason: "module_graph_newer_than_target" };
    }
  }
  if (input.scanForbiddenTokens !== false) {
    for (const source of visited.values()) {
      if (containsForbiddenGraphToken(source)) return { ok: false, reason: "module_graph_forbidden_token" };
    }
  }
  const graphHash = createHash("sha256");
  for (const [path, source] of [...visited.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    graphHash.update(path).update("\0").update(createHash("sha256").update(source).digest("hex")).update("\0");
  }

  const qualifying: string[] = [];
  for (const [path, source] of visited) {
    if (LEGACY_PI_REQUIRED_EXPORTS.every(name => exportsName(source, name) && definesName(source, name))) qualifying.push(path);
  }
  if (qualifying.length === 0) return { ok: false, reason: "required_export_absent" };
  // Two candidates is not a choice: the witness reads ONE module, and picking one
  // would be exactly the guess this resolver exists to avoid.
  if (qualifying.length > 1) return { ok: false, reason: "required_export_ambiguous" };
  return {
    ok: true,
    value: {
      moduleUrl: pathToFileURL(qualifying[0]!).href,
      entryUrl,
      installationRoot: root.root,
      packageName: root.name,
      packageVersion: root.version,
      modulesVisited: visited.size,
      graphHash: `sha256:${graphHash.digest("hex").slice(0, 32)}`,
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Node inspector launch qualification.
//
// SIGUSR1 opens the inspector on the port the process was CONFIGURED with, so the
// endpoint is a property of LAUNCH CONFIGURATION, not of what is listening now.
// Absence is therefore never claimed from a command-line scrape. What this
// classifier reports is derived from OBSERVABLE tokens only: the runner's own
// command before its script token, and the shebang flags of the child's resolved
// entry. There is no composed-argv seam, no daemon-composed argv, and no authored
// launch record accepted here. A target whose node options cannot be observed from
// those two sources stays unresolved; it is never classified from a composed
// record. Anything that is not "no inspector configuration at all" refuses, because
// the fixed helper speaks only the built-in private loopback endpoint.
// ─────────────────────────────────────────────────────────────────────────────

export type NodeInspectorConfiguration =
  | "none"                        // no configuring flag: built-in private loopback default
  | "private_loopback_default"     // --inspect[/brk] with the default private loopback host
  | "custom_endpoint"              // any explicit port, or a non-loopback host
  | "signal_disabled";             // --disable-sigusr1

export interface NodeInspectorConfigurationVerdict {
  configuration: NodeInspectorConfiguration;
  /** Names of the configuring flags seen. Flag NAMES only: no value text. */
  flags: string[];
}

/** Classify the OBSERVABLE node options of one Node target. The only inputs are the
 *  runner's own tokens before its script token, or the child's shebang flags. Values
 *  are examined only to tell a private loopback default from an explicit
 *  non-default endpoint; no value is returned. */
export function classifyNodeInspectorConfiguration(observableTokens: readonly string[]): NodeInspectorConfigurationVerdict {
  const flags = new Set<string>();
  let configuration: NodeInspectorConfiguration = "none";
  const escalate = (next: NodeInspectorConfiguration) => {
    // A disabled signal, an explicit port and a public host all outrank a default.
    const rank: Record<NodeInspectorConfiguration, number> = { none: 0, private_loopback_default: 1, custom_endpoint: 2, signal_disabled: 3 };
    if (rank[next] > rank[configuration]) configuration = next;
  };
  for (const token of observableTokens) {
    if (token === "--disable-sigusr1") { flags.add(token); escalate("signal_disabled"); continue; }
    const withValue = token.match(/^(--inspect-brk|--inspect|-{1,2}debug-port|--remote-debugging-port|--inspect-port)=(.*)$/);
    if (withValue) {
      const name = withValue[1]!;
      flags.add(name);
      if (name === "--inspect-port" || name === "--debug-port" || name === "--remote-debugging-port") { escalate("custom_endpoint"); continue; }
      const host = (withValue[2] ?? "").split(":")[0]!.replace(/^\[|\]$/g, "");
      escalate(host === "" || host === "127.0.0.1" || host === "localhost" || host === "::1" ? "private_loopback_default" : "custom_endpoint");
      continue;
    }
    if (token === "--inspect" || token === "--inspect-brk") { flags.add(token); escalate("private_loopback_default"); continue; }
    if (token === "--inspect-port" || token === "--debug-port" || token === "--remote-debugging-port") { flags.add(token); escalate("custom_endpoint"); continue; }
  }
  return { configuration, flags: [...flags].sort() };
}

/** Whether a node inspector configuration qualifies for the
 *  private loopback endpoint this bridge speaks.
 *
 *  There are exactly TWO kinds of input, and nothing else feeds this function:
 *    - the RUNNER's own observable tokens, taken from its command before its script token;
 *    - the SHEBANG flags of the child's resolved entry, interpreter name only.
 *  A full or composed argv is NOT an input and there is no seam for one. A target whose node
 *  options cannot be observed from those two sources stays unresolved; it is never classified
 *  from an authored record. This function classifies; it does not resolve, prove or author. */

export function qualifiesDefaultPrivateInspector(configuration: NodeInspectorConfiguration): boolean {
  return configuration === "none" || configuration === "private_loopback_default";
}
