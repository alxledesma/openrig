import { describe, it, expect, vi } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { SeatLaunchEnvironment, type NativeDutyLaunchWrapper } from "../src/domain/seat-launch-environment.js";
import { NativeDutyLaunchStore } from "../src/domain/native-duty-launch.js";
import { CodexRuntimeAdapter } from "../src/adapters/codex-runtime-adapter.js";
import { CodexResumeAdapter } from "../src/adapters/codex-resume.js";
import { PiRuntimeAdapter } from "../src/adapters/pi-runtime-adapter.js";
import { PiResumeAdapter } from "../src/adapters/pi-resume.js";
import { buildPiRunnerArgs, buildPiRunnerCommand } from "../src/adapters/pi-runner-protocol.js";
import { codexPostureArgs } from "../src/adapters/yolo-mode.js";
import { buildCodexResumeArgs } from "../src/domain/native-resume-probe.js";
import type { NodeBinding } from "../src/domain/runtime-adapter.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";

describe("managed native duty launch", () => {
  it("wraps structured fresh/resume/fork argv, preserves disabled bytes and refuses opted-in composition failures", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "duty-managed-")); fs.chmodSync(root, 0o700);
    try {
      const bin = path.join(root, "bin"); fs.mkdirSync(bin);
      const codex = path.join(bin, "codex"), node = path.join(bin, "node"), cli = path.join(root, "rig");
      fs.writeFileSync(codex, "#!/usr/bin/env node\n// fixture only\n", { mode: 0o755 });
      fs.symlinkSync(process.execPath, node); fs.writeFileSync(cli, "#!/bin/sh\n", { mode: 0o755 });
      const supervisor = path.join(root, "supervisor.js"); fs.writeFileSync(supervisor, "// immutable fixture entry\n", { mode: 0o600 });
      const store = new NativeDutyLaunchStore({ root: path.join(root, "launches"), nodeExecutable: process.execPath, supervisorEntry: supervisor });
      const typed: string[] = [], captured: Parameters<NativeDutyLaunchWrapper["wrap"]>[0][] = [];
      let shell = "zsh", missing = "", broken = false;
      const identity: Record<string, string> = { OPENRIG_NODE_ID: "node", OPENRIG_SESSION_NAME: "lead-lead@rig",
        OPENRIG_OCCUPANT_GENERATION: "old-generation", OPENRIG_RUNTIME: "codex" };
      const tmux = { getPaneCommand: async () => shell, getSessionEnv: async (_s: string, key: string) => key === missing ? undefined : identity[key],
        sendShellCommand: vi.fn(async (_s: string, text: string) => { typed.push(text); return { ok: false, message: "bounded-stop-after-compose" }; }) } as unknown as TmuxAdapter;
      const privateEnv = { PATH: bin, OPENRIG_HOME: root, OPENRIG_URL: "http://127.0.0.1:17538",
        OPENRIG_TERMINAL_BEARER_TOKEN: "private-fixture-secret", PROVIDER_API_KEY: "private-fixture-provider" };
      const wrapper: NativeDutyLaunchWrapper = { enabled: () => true, wrap: async input => {
        if (broken) throw new Error("private-fixture-secret");
        captured.push(structuredClone(input));
        const prepared = store.prepare({ ...input, scopeId: "scope", configurationDigest: "a".repeat(64), pollMs: 1000 });
        const bytes = fs.readFileSync(prepared.intent.configPath, "utf8");
        expect(bytes).not.toMatch(/private-fixture|BEARER|API_KEY/);
        return prepared.launch;
      } };
      const env = new SeatLaunchEnvironment(tmux, privateEnv, root, cli, path.join(root, "codex-home"), wrapper);
      const binding = { nodeId: "node", tmuxSession: "lead-lead@rig", launchGeneration: "reserved-generation", runtime: "codex",
        cwd: root, model: "fixture model", effort: "medium", launchPosture: "floor" } as NodeBinding;
      const codexAdapter = new CodexRuntimeAdapter({ tmux, seatLaunchEnvironment: env, fsOps: { homedir: root } as never,
        resolveGitAddDirs: async () => [path.join(root, ".git")], detectDaemonSupport: async () => ({ kind: "supported" }) as never,
        readNetworkDefault: async () => ({ apply: true, elapsedMs: 0 }) });
      for (const opts of [{ name: "lead-lead@rig" }, { name: "lead-lead@rig", resumeToken: "native token" },
        { name: "lead-lead@rig", forkSource: { kind: "native_id", value: "parent thread" } as const }]) {
        expect((await codexAdapter.launchHarness(binding, opts)).ok).toBe(false);
      }
      const queue = path.join(root, ".openrig", "shared-docs", "rigs", "rig", "state", "lead");
      const top = ["--no-daemon", "-s", "workspace-write", "-c", "sandbox_workspace_write.network_access=true"];
      const model = ["-m", "fixture model", "-c", 'model_reasoning_effort="medium"'];
      expect(captured[0]!.harness).toEqual({ executable: node, args: [codex, ...top, "-C", root,
        "--add-dir", path.join(root, ".git"), "--add-dir", queue, ...model], cwd: root });
      expect(captured[1]!.harness.args).toEqual([codex, ...top, ...model, "resume", "--add-dir", queue, "native token"]);
      expect(captured[2]!.harness.args).toEqual([codex, ...top, ...model, "fork", "--add-dir", queue, "parent thread"]);
      expect(captured.every(c => c.generation === "reserved-generation" && c.sessionName === "lead-lead@rig")).toBe(true);
      expect(typed.every(s => s.includes("--supervise") && s.includes("OPENRIG_OCCUPANT_GENERATION=reserved-generation"))).toBe(true);
      expect(typed.join("\n")).not.toMatch(/private-fixture|BEARER|API_KEY|sh -c/);

      const restore = new CodexResumeAdapter(tmux, { seatLaunchEnvironment: env, detectDaemonSupport: async () => ({ kind: "supported" }) as never });
      await restore.resume("lead-lead@rig", "codex_id", "restore-token", root, undefined, "full_bypass", "restore model", "high", "ledger-generation");
      expect(captured.at(-1)!.harness.args).toEqual([codex, "--no-daemon", "-s", "danger-full-access", "-a", "never",
        "-m", "restore model", "-c", 'model_reasoning_effort="high"', "resume", "restore-token"]);
      expect(captured.at(-1)!.generation).toBe("ledger-generation");
      const runner = path.join(root, "pi-runner.js"), stateRoot = path.join(root, "pi-state");
      const fakeFs = { readFile: () => "", writeFile: () => {}, mkdirp: () => {}, exists: () => true };
      const pi = new PiRuntimeAdapter({ tmux, fsOps: fakeFs, runnerEntryPath: runner, stateRoot,
        seatLaunchEnvironment: env, newLaunchId: () => "pi-launch" });
      for (const opts of [{ name: "lead-lead@rig" }, { name: "lead-lead@rig", resumeToken: "/native/session.jsonl" },
        { name: "lead-lead@rig", forkSource: { kind: "native_id", value: "/native/parent" } as const }]) {
        const result = await pi.launchHarness({ ...binding, runtime: "pi", model: "provider/model" }, opts);
        expect(result.ok).toBe(false);
      }
      const base = { runnerEntryPath: runner, sessionName: "lead-lead@rig", stateRoot, cwd: root,
        model: "provider/model", trust: "no-approve" as const, launchId: "pi-launch", runtime: "pi" as const };
      for (const [i, extra] of [{}, { sessionFile: "/native/session.jsonl" }, { forkRef: "/native/parent" }].entries()) {
        expect(captured[4 + i]!.harness).toEqual({ executable: node, args: buildPiRunnerArgs({ ...base, ...extra }), cwd: root });
      }
      const piRestore = new PiResumeAdapter(tmux, fakeFs, { runnerEntryPath: runner, stateRoot },
        { seatLaunchEnvironment: env, newLaunchId: () => "restore-launch" });
      await piRestore.resume("lead-lead@rig", "pi_session_file", "/native/restore.jsonl", root, "provider/model", "floor", "pi-ledger");
      expect(captured.at(-1)!.generation).toBe("pi-ledger");
      expect(captured.at(-1)!.harness.args).toEqual(buildPiRunnerArgs({ ...base, launchId: "restore-launch", sessionFile: "/native/restore.jsonl" }));

      const disabled = new SeatLaunchEnvironment(tmux, privateEnv, root, cli, undefined, { ...wrapper, enabled: () => false });
      const old = new SeatLaunchEnvironment(tmux, privateEnv, root, cli);
      expect(await disabled.command("lead-lead@rig", "codex -s workspace-write", { codexCwd: root, nodeId: "node", runtime: "codex" }))
        .toBe(await old.command("lead-lead@rig", "codex -s workspace-write", { codexCwd: root, nodeId: "node", runtime: "codex" }));
      const disabledPi = new PiRuntimeAdapter({ tmux, fsOps: fakeFs, runnerEntryPath: runner, stateRoot,
        seatLaunchEnvironment: disabled, newLaunchId: () => "pi-launch" });
      await disabledPi.launchHarness({ ...binding, runtime: "pi", model: "provider/model" }, { name: "lead-lead@rig" });
      expect(typed.at(-1)).toBe(buildPiRunnerCommand(base));
      expect(buildPiRunnerCommand(base)).toBe(`node '${runner}' --session-name 'lead-lead@rig' --state-root '${stateRoot}' --cwd '${root}' --launch-id 'pi-launch' --no-approve --model 'provider/model'`);
      expect(buildPiRunnerCommand({ ...base, runtime: "omp" })).toContain("--runtime omp --approval-mode always-ask");
      expect(codexPostureArgs("profile with spaces", {}, "full_bypass")).toEqual(["-p", "profile with spaces", "-s", "danger-full-access", "-a", "never"]);
      expect(buildCodexResumeArgs({ resumeToken: "ignored", useLast: true, postureArgs: ["-p", "profile"] })).toEqual(["-p", "profile", "resume", "--last"]);
      for (const fault of ["wrapper", "generation", "identity", "shell"] as const) {
        broken = fault === "wrapper"; missing = fault === "generation" ? "OPENRIG_OCCUPANT_GENERATION" : fault === "identity" ? "OPENRIG_NODE_ID" : "";
        shell = fault === "shell" ? "nu" : "zsh";
        const count = typed.length;
        const result = await codexAdapter.launchHarness({ ...binding, launchGeneration: undefined }, { name: "lead-lead@rig", resumeToken: "native-token" });
        expect(result.ok).toBe(false); expect(typed).toHaveLength(count);
      }
      await expect(env.command("lead-lead@rig", "codex resume 'native-token'", { nodeId: "node" })).rejects.toThrow();
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
