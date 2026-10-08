import { afterEach, expect, it } from "vitest";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { SeatLaunchEnvironment } from "../src/domain/seat-launch-environment.js";
import { shellQuote } from "../src/adapters/shell-quote.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
it.each(["pi", "codex"] as const)("%s managed launch preserves its intended child toolchain after shell rc", async runtime => {
  const root = mkdtempSync(path.join(tmpdir(), "pi-path-")); roots.push(root);
  const selected = path.join(root, "selected bin's"), ambient = path.join(root, "shell-bin");
  for (const p of [selected, ambient]) mkdirSync(p);
  const executable = (p: string, value: string) => writeFileSync(p, `#!/bin/sh\necho ${value}\n`, { mode: 0o700 });
  executable(path.join(selected, "pi"), "selected-pi"); executable(path.join(selected, "node"), "selected-node");
  executable(path.join(ambient, "pi"), "old-wrapper"); executable(path.join(ambient, "node"), "old-node");
  executable(path.join(ambient, "extra-tool"), "extra-tool"); executable(path.join(selected, "rig"), "paired-rig");
  const identity: Record<string, string> = { OPENRIG_NODE_ID: "node", OPENRIG_SESSION_NAME: "seat@rig", OPENRIG_RUNTIME: runtime, OPENRIG_OCCUPANT_GENERATION: "generation" };
  const tmux = { getPaneCommand: async () => "bash", getSessionEnv: async (_session: string, key: string) => identity[key] } as unknown as TmuxAdapter;
  const daemonPath = `${selected}:/usr/bin:/bin`;
  const environment = new SeatLaunchEnvironment(tmux, { PATH: daemonPath, OPENRIG_HOME: path.join(root, "instance") }, root, path.join(selected, "rig"), undefined,
    { enabled: () => true, wrap: async input => input.harness });
  const command = await environment.structuredCommand("seat@rig", { executable: "/bin/sh", args: ["-c", "pi; node; extra-tool; rig"], cwd: root }, { nodeId: "node", generation: "generation", runtime });
  // Actual shell rc override, not an assertion mirroring command construction.
  const output = execFileSync("/bin/bash", ["--noprofile", "--norc", "-c", `export PATH=${shellQuote(`${ambient}:/usr/bin:/bin`)}; ${command}`], { encoding: "utf8", cwd: root });
  expect(output.trim().split("\n")).toEqual(runtime === "pi"
    ? ["selected-pi", "selected-node", "extra-tool", "paired-rig"]
    : ["old-wrapper", "old-node", "extra-tool", "paired-rig"]);
});
