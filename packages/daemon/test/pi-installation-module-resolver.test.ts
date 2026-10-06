// Bounded suite for the daemon-owned Pi installation resolver.
//
// The fixture reproduces the SHAPE of the real installation observed in
// ROOT-LEGACY-LIVE-BINDING-EVIDENCE.json — a package whose bin symlink resolves to
// dist/bundle/cli.js, which createRequire()s cli-runtime.js, which statically imports
// hashed chunks — but every file, hash and export is generated. Nothing here asserts a
// chunk name: the resolver must FIND the module by walking the graph and checking real
// exports, which is precisely what a hardcoded name or a guessed suffix would fail.
import { describe, it, expect, afterEach } from "vitest";
import { chmodSync, mkdtempSync, mkdirSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  classifyNodeInspectorConfiguration,
  qualifiesDefaultPrivateInspector,
  resolvePiInstallationModule,
} from "../src/domain/pi-installation-module-resolver.js";

const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });

const CLASS_BODY = (kind: string) => `var ${kind}=class{get leafId(){return null}};`;

/** An installation shaped exactly like the observed one: bin symlink → bundle entry
 *  → createRequire runtime → hashed chunks, with the two required classes in ONE chunk
 *  that the runtime imports unconditionally. `split` puts each class in its own chunk
 *  (nothing qualifies), `duplicate` puts both in two chunks (ambiguous). */
function installation(opts?: { split?: boolean; duplicate?: boolean; noExports?: boolean; entry?: "cli.js" | "rpc-entry.js" }): { root: string; chunk: string; entryPath: string } {
  const root = mkdtempSync(join(tmpdir(), "pi-install-"));
  dirs.push(root);
  const pkg = join(root, "package");
  const dist = join(pkg, "dist");
  const bundle = join(dist, "bundle");
  const chunks = join(bundle, "chunks");
  mkdirSync(chunks, { recursive: true });
  mkdirSync(join(pkg, "bin"), { recursive: true });
  writeFileSync(join(pkg, "package.json"), JSON.stringify({ name: "pi-fixture", version: "9.9.9" }));
  // A generated hash-shaped name proves the resolver is not keyed to any literal.
  const sessionChunk = join(chunks, `chunk-${"Z7KQ4M2P"}.js`);
  const runtimeChunk = join(chunks, `chunk-${"A1B2C3D4"}.js`);
  writeFileSync(runtimeChunk, `export const helper=1;${CLASS_BODY("Unrelated")}`);
  const exportsClause = opts?.noExports ? "" : "export{AgentSessionRuntime,AgentSession};";
  if (opts?.split) {
    writeFileSync(sessionChunk, `${CLASS_BODY("AgentSession")}${exportsClause.replace("AgentSessionRuntime,", "")}`);
    writeFileSync(runtimeChunk, `${CLASS_BODY("AgentSessionRuntime")}export{AgentSessionRuntime};${CLASS_BODY("Unrelated")}`);
  } else {
    writeFileSync(sessionChunk, `${CLASS_BODY("AgentSessionRuntime")}${CLASS_BODY("AgentSession")}${exportsClause}`);
  }
  if (opts?.duplicate) {
    writeFileSync(runtimeChunk, `${CLASS_BODY("AgentSessionRuntime")}${CLASS_BODY("AgentSession")}export{AgentSessionRuntime,AgentSession};`);
  }
  const imports = `import{configureHttpDispatcher,main}from"./chunks/${sessionChunk.split("/").pop()}";import"./chunks/${runtimeChunk.split("/").pop()}";`;
  writeFileSync(join(bundle, "cli-runtime.js"), `${imports}function setupCli(){process.title="pi"}setupCli();main(process.argv.slice(2));`);
  writeFileSync(join(bundle, "rpc-entry.js"), `${imports}main(["--mode","rpc",...process.argv.slice(2)]);`);
  writeFileSync(join(bundle, "cli.js"), `#!/usr/bin/env node\nimport { createRequire, enableCompileCache } from "node:module";\nenableCompileCache();\ncreateRequire(import.meta.url)("./cli-runtime.js");\n`);
  writeFileSync(join(bundle, "index.js"), "export const marker=1;\n");
  const binTarget = join(bundle, opts?.entry ?? "cli.js");
  // A real installation's bin entry is executable; the resolver refuses a PATH entry
  // the runtime could not execute, so the fixture must carry the same bit.
  chmodSync(binTarget, 0o755);
  symlinkSync(binTarget, join(pkg, "bin", "pi"));
  return { root: pkg, chunk: sessionChunk, entryPath: binTarget };
}

describe("pi installation module resolver", () => {
  it("resolves the ONE module exporting both required classes, through the installation graph", () => {
    const install = installation();
    const resolved = resolvePiInstallationModule({ executable: "pi", pathEnv: join(install.root, "bin") });
    expect(resolved.ok).toBe(true);
    if (!resolved.ok) return;
    // Canonical: macOS reports /private/var for /var, and the target's own parsed
    // script URLs are realpathed, so the resolver must return the realpath form.
    expect(resolved.value.moduleUrl).toBe(pathToFileURL(realpathSync(install.chunk)).href);
    expect(resolved.value.entryUrl).toBe(pathToFileURL(realpathSync(install.entryPath)).href);
    expect(resolved.value.packageName).toBe("pi-fixture");
    expect(resolved.value.packageVersion).toBe("9.9.9");
    // The graph walk saw the entry, its runtime and the chunks, not just one file.
    expect(resolved.value.modulesVisited).toBeGreaterThan(3);
  });

  it("resolves identically from the rpc-mode entry, so the mode is not part of the mapping", () => {
    const install = installation({ entry: "rpc-entry.js" });
    const resolved = resolvePiInstallationModule({ executable: join(install.root, "bin", "pi") });
    expect(resolved.ok).toBe(true);
    if (resolved.ok) expect(resolved.value.moduleUrl).toBe(pathToFileURL(realpathSync(install.chunk)).href);
  });

  it("refuses rather than guessing when the classes live in different modules", () => {
    const install = installation({ split: true });
    expect(resolvePiInstallationModule({ executable: "pi", pathEnv: join(install.root, "bin") })).toEqual({ ok: false, reason: "required_export_absent" });
  });

  it("refuses ambiguity: two qualifying modules is never a choice", () => {
    const install = installation({ duplicate: true });
    expect(resolvePiInstallationModule({ executable: "pi", pathEnv: join(install.root, "bin") })).toEqual({ ok: false, reason: "required_export_ambiguous" });
  });

  it("refuses when the classes exist but are never exported", () => {
    const install = installation({ noExports: true });
    expect(resolvePiInstallationModule({ executable: "pi", pathEnv: join(install.root, "bin") })).toEqual({ ok: false, reason: "required_export_absent" });
  });

  it("refuses an unresolvable executable and an installation with no package root", () => {
    const install = installation();
    expect(resolvePiInstallationModule({ executable: "definitely-not-installed-pi", pathEnv: join(install.root, "bin") })).toEqual({ ok: false, reason: "executable_unresolved" });
    const orphan = mkdtempSync(join(tmpdir(), "pi-orphan-"));
    dirs.push(orphan);
    const bundle = join(orphan, "bundle");
    mkdirSync(bundle, { recursive: true });
    writeFileSync(join(bundle, "cli.js"), `import "./other.js";`);
    expect(resolvePiInstallationModule({ executable: join(bundle, "cli.js") })).toEqual({ ok: false, reason: "installation_root_unresolved" });
  });

  it("ignores the child's argv entirely, so an overwritten process title cannot mislead it", () => {
    const install = installation();
    // The live child's census command is the bare string "pi" after it rewrites its own
    // title. The resolver takes no argv at all, so this cannot degrade its answer.
    const resolved = resolvePiInstallationModule({ executable: "pi", pathEnv: join(install.root, "bin") });
    expect(resolved.ok).toBe(true);
    if (resolved.ok) expect(resolved.value.moduleUrl).toContain("/chunks/chunk-");
  });
});

describe("node inspector launch configuration", () => {
  it("classifies a composed argv with no inspector flags as the built-in default", () => {
    const verdict = classifyNodeInspectorConfiguration(["node", "/x/pi-runner.js", "--session-name", "s", "--launch-id", "l"]);
    expect(verdict).toEqual({ configuration: "none", flags: [] });
    expect(qualifiesDefaultPrivateInspector(verdict.configuration)).toBe(true);
  });

  it("accepts an explicit private loopback default and refuses any other endpoint", () => {
    expect(classifyNodeInspectorConfiguration(["--inspect"]).configuration).toBe("private_loopback_default");
    expect(classifyNodeInspectorConfiguration(["--inspect=127.0.0.1"]).configuration).toBe("private_loopback_default");
    expect(classifyNodeInspectorConfiguration(["--inspect-brk=localhost:9229"]).configuration).toBe("private_loopback_default");
    expect(qualifiesDefaultPrivateInspector("private_loopback_default")).toBe(true);
    for (const argv of [["--inspect-port=9333"], ["--inspect-port", "9333"], ["--inspect=0.0.0.0"], ["--inspect=0.0.0.0:9229"], ["--remote-debugging-port=9333"], ["--debug-port=9333"]]) {
      const verdict = classifyNodeInspectorConfiguration(argv);
      expect(verdict.configuration, argv.join(" ")).toBe("custom_endpoint");
      expect(qualifiesDefaultPrivateInspector(verdict.configuration), argv.join(" ")).toBe(false);
    }
  });

  it("refuses a target whose SIGUSR1 inspector activation is disabled, and never echoes values", () => {
    const verdict = classifyNodeInspectorConfiguration(["--inspect-port=9333", "--disable-sigusr1"]);
    expect(verdict.configuration).toBe("signal_disabled");
    expect(qualifiesDefaultPrivateInspector(verdict.configuration)).toBe(false);
    // Only flag NAMES are ever returned, so no endpoint value can leak.
    expect(verdict.flags.every(flag => flag.startsWith("--"))).toBe(true);
  });
});
