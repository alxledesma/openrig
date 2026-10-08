import { afterEach, expect, it, vi } from "vitest";
import { Command } from "commander";
import { seatCommand } from "../src/commands/seat.js";
import { DaemonClient } from "../src/client.js";
import { STATE_FILE, type DaemonState, type LifecycleDeps } from "../src/daemon-lifecycle.js";
import type { StatusDeps } from "../src/commands/status.js";

const originalExit = process.exitCode;
const bearer = "test-only-pi-detached-token";
const operator = "operator-agent@kernel";
const generation = "operator-generation-exact";
const attemptId = "c47de976-e4eb-4028-9b05-ae1efbd3caaa";
const beganSha256 = "a".repeat(64);
const originalRunnerEntryPath = "/private/var/tmp/openrig/runner-entry.js";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); process.exitCode = originalExit; });

function makeCli() {
  vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN", bearer);
  vi.stubEnv("OPENRIG_SESSION_NAME", operator);
  vi.stubEnv("OPENRIG_OCCUPANT_GENERATION", generation);
  const lifecycle: LifecycleDeps = {
    spawn: vi.fn(() => ({ pid: 1, unref: vi.fn() }) as never), fetch: vi.fn(async () => ({ ok: true })), kill: vi.fn(() => true),
    readFile: vi.fn((p: string) => p === STATE_FILE ? JSON.stringify({ pid: 1, port: 17538, db: "fixture.sqlite", startedAt: "2026-10-08T00:00:00Z" } as DaemonState) : null),
    writeFile: vi.fn(), removeFile: vi.fn(), exists: vi.fn((p: string) => p === STATE_FILE), mkdirp: vi.fn(), openForAppend: vi.fn(() => 3), isProcessAlive: vi.fn(() => true),
  };
  const requests: Array<{ path: string; body: unknown; options?: unknown }> = [];
  const response = { status: 200, data: { ok: true, runtime: "pi", generation, generationUnchanged: true, custodyPreserved: true, guardLeftEnabled: true } };
  const client = { post: vi.fn(async (path: string, body: unknown, options?: unknown) => { requests.push({ path, body, options }); return response; }) };
  const deps = { lifecycleDeps: lifecycle, clientFactory: vi.fn(() => client as unknown as DaemonClient) } as unknown as StatusDeps;
  const program = new Command().addCommand(seatCommand(deps));
  const override = (cmd: Command) => { cmd.exitOverride(); for (const sub of cmd.commands) override(sub); };
  override(program);
  const logs: string[] = [];
  vi.spyOn(console, "log").mockImplementation((...values) => logs.push(values.join(" ")));
  vi.spyOn(console, "error").mockImplementation((...values) => logs.push(values.join(" ")));
  process.exitCode = undefined;
  return { program, requests, logs, client };
}

it("sends strict Pi detached mode with typed original-runner recovery and bearer authentication", async () => {
  const f = makeCli();
  await f.program.parseAsync(["node", "rig", "seat", "rehost-runner", "intake-lead@app-handy-conveyor", "--reason", "resume exact Pi history", "--pi-detached-resume", "--pi-recovery-attempt", attemptId, "--pi-began-sha256", beganSha256, "--pi-original-runner", originalRunnerEntryPath, "--json"]);
  expect(f.requests).toHaveLength(1);
  expect(f.requests[0]).toMatchObject({
    path: "/api/seat/rehost-runner/intake-lead%40app-handy-conveyor",
    body: { reason: "resume exact Pi history", legacyNativeWitness: false, piDetachedResume: true, piDetachedRecovery: { attemptId, beganSha256, originalRunnerEntryPath } },
    options: { timeoutMs: 60000 },
  });
  expect((f.requests[0]!.body as Record<string, unknown>).operator).toBeUndefined();
  expect(new Headers((f.requests[0]!.options as any).headers).get("Authorization")).toBe(`Bearer ${bearer}`);
  expect(process.env.OPENRIG_SESSION_NAME).toBe(operator);
  expect(process.env.OPENRIG_OCCUPANT_GENERATION).toBe(generation);
  expect(process.exitCode).toBeUndefined();
});

it("sends Pi detached mode without an optional recovery continuation", async () => {
  const f = makeCli();
  await f.program.parseAsync(["node", "rig", "seat", "rehost-runner", "intake-lead@app-handy-conveyor", "--reason", "resume exact Pi history", "--pi-detached-resume", "--json"]);
  expect(f.requests).toHaveLength(1);
  expect(f.requests[0]!.body).toMatchObject({ piDetachedResume: true });
  expect(f.requests[0]!.body).not.toHaveProperty("piDetachedRecovery");
});

it.each([
  ["attempt without hash", ["--pi-detached-resume", "--pi-recovery-attempt", attemptId]],
  ["hash without attempt", ["--pi-detached-resume", "--pi-began-sha256", beganSha256]],
  ["attempt without mode", ["--pi-recovery-attempt", attemptId, "--pi-began-sha256", beganSha256]],
  ["invalid UUID", ["--pi-detached-resume", "--pi-recovery-attempt", "bad", "--pi-began-sha256", beganSha256]],
  ["invalid hash", ["--pi-detached-resume", "--pi-recovery-attempt", attemptId, "--pi-began-sha256", "A".repeat(64)]],
  ["original runner without recovery pair", ["--pi-detached-resume", "--pi-original-runner", originalRunnerEntryPath]],
  ["original runner without Pi mode", ["--pi-recovery-attempt", attemptId, "--pi-began-sha256", beganSha256, "--pi-original-runner", originalRunnerEntryPath]],
  ["relative original runner", ["--pi-detached-resume", "--pi-recovery-attempt", attemptId, "--pi-began-sha256", beganSha256, "--pi-original-runner", "runner.js"]],
  ["unnormalized original runner", ["--pi-detached-resume", "--pi-recovery-attempt", attemptId, "--pi-began-sha256", beganSha256, "--pi-original-runner", "/tmp/../runner.js"]],
  ["newline original runner", ["--pi-detached-resume", "--pi-recovery-attempt", attemptId, "--pi-began-sha256", beganSha256, "--pi-original-runner", "/tmp/runner\n.js"]],
  ["caller-selected actor", ["--pi-detached-resume", "--operator", operator]],
  ["Codex mixed mode", ["--pi-detached-resume", "--codex-detached-resume"]],
  ["legacy mixed mode", ["--pi-detached-resume", "--legacy-native-witness"]],
  ["stopped mixed mode", ["--pi-detached-resume", "--stopped-target-recovery", "--accept-unpersisted-turn-loss", "owner-ref"]],
] as const)("refuses %s before any request", async (_name, flags) => {
  const f = makeCli();
  await f.program.parseAsync(["node", "rig", "seat", "rehost-runner", "intake-lead@app-handy-conveyor", "--reason", "invalid mode", ...flags, "--json"]);
  expect(f.requests).toHaveLength(0);
  expect(process.exitCode).toBe(1);
});
