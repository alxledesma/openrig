import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
import { coordinatorRoutes } from "../../daemon/src/routes/coordinator.js";
import { CoordinatorFenceError } from "../../daemon/src/domain/coordinator-authority-service.js";
import { DaemonClient } from "../src/client.js";
import { coordinatorCommand } from "../src/commands/coordinator.js";

const actor = "lead@test-rig@test-host";
const generation = "fixture-generation-from-seat-env";
const token = "disposable-fixture-token";
const operations = ["enable", "transfer", "acknowledge", "renew", "admit", "dispose", "recover", "legacy-inventory", "migrate-legacy"];
let home: string;
let app: Hono;
let calls: Array<{ operation: string; actor?: string; generation?: string; body?: unknown }>;

// Exercise real CLI -> DaemonClient -> bearer/identity middleware -> coordinator route.
// Only authority repository effects are replaced; no production URL or credentials are used.
beforeEach(() => {
  home = fs.mkdtempSync(path.join(os.tmpdir(), "coordinator-cli-auth-"));
  vi.stubEnv("OPENRIG_HOME", home);
  vi.stubEnv("OPENRIG_URL", "http://coordinator-fixture.invalid");
  vi.stubEnv("OPENRIG_SESSION_NAME", actor);
  vi.stubEnv("OPENRIG_OCCUPANT_GENERATION", generation);
  vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN", "");
  fs.writeFileSync(path.join(home, "terminal-token"), token, { mode: 0o600 });
  calls = [];
  const svc: Record<string, unknown> = {
    get: (rigId: string) => ({ rigId }), obligations: () => [], reconciliationDigest: () => "fixture-digest",
    refreshRuntimeAvailability: async () => {},
    legacyInventory: (rigId: string, authorizationId: string) => {
      calls.push({ operation: "legacy-inventory", body: { rigId, authorizationId } }); return { rigId };
    },
    acknowledge: (a: string, t: { generation: string }, b: unknown) => {
      calls.push({ operation: "acknowledge", actor: a, generation: t.generation, body: b }); return { ok: true };
    },
    renew: (a: string, t: { generation: string }) => {
      calls.push({ operation: "renew", actor: a, generation: t.generation }); return { ok: true };
    },
    resumeOwned: (a: string, g: string, b: unknown) => {
      calls.push({ operation: "resume-owned", actor: a, generation: g, body: b }); return { rigId: (b as { rigId: string }).rigId, state: "active", lease_until: 130000 };
    },
  };
  for (const [method, operation] of [["enable", "enable"], ["transfer", "transfer"], ["admit", "admit"], ["dispose", "dispose"], ["recordOutage", "recover"], ["migrateLegacy", "migrate-legacy"]]) {
    svc[method!] = (a: string, g: string, b: unknown) => {
      calls.push({ operation: operation!, actor: a, generation: g, body: b }); return { ok: true };
    };
  }
  app = new Hono();
  app.use("*", async (c, next) => { c.set("queueRepo" as never, { coordinatorAuthority: svc } as never); await next(); });
  app.route("/api/coordinator", coordinatorRoutes({ bearerToken: token }));
  vi.stubGlobal("fetch", vi.fn((input: string | URL | Request, init?: RequestInit) => app.request(input, init)));
  vi.spyOn(console, "log").mockImplementation(() => {});
  process.exitCode = undefined;
});
afterEach(() => {
  process.exitCode = undefined;
  vi.restoreAllMocks(); vi.unstubAllGlobals(); vi.unstubAllEnvs();
  fs.rmSync(home, { recursive: true, force: true });
});

function contractFile(overrides: Record<string, unknown> = {}): string {
  const file = path.join(home, "contract.json");
  fs.writeFileSync(file, JSON.stringify({ rigId: "test-rig", expected: { rigId: "test-rig" }, token: { generation }, authorizationId: "fixture-authorization", ...overrides }));
  return file;
}

describe("resume-owned command contract", () => {
  it("generates a fresh exact operation id by default and prints the id actually used", async () => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    const log = vi.mocked(console.log);
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig"]);
    expect(post.mock.calls[0]?.[0]).toBe("/api/coordinator/resume-owned");
    expect(post.mock.calls[0]?.[2]).toEqual({ headers: { Authorization: `Bearer ${token}` } });
    const body = post.mock.calls[0]?.[1] as { rigId: string; leaseMs: number; operationId: string };
    expect(body.rigId).toBe("test-rig");
    expect(body.leaseMs).toBe(1200000);
    // A fresh generated id: never a reuse of the operator's earlier attempt.
    expect(body.operationId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    // The printed receipt reports the exact id used, so it can be replayed deliberately.
    const printed = JSON.parse(String((log.mock.calls.at(-1)?.[0] as string)));
    expect(printed.operationId).toBe(body.operationId);
    expect(printed.operationIdSource).toBe("generated");
    expect(printed.state).toBe("active");
    // Identity derives from the seat environment; a body claim could not win.
    expect(calls[0]?.actor).toBe(actor);
    expect(calls[0]?.generation).toBe(generation);
    expect(process.exitCode).toBeUndefined();
  });
  it("replays a controlled exact id when supplied and never auto-retries", async () => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig", "--operation-id", "controlled-replay-1", "--lease-ms", "60000"]);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.body).toEqual({ rigId: "test-rig", leaseMs: 60000, operationId: "controlled-replay-1" });
    const printed = JSON.parse(String((vi.mocked(console.log).mock.calls.at(-1)?.[0] as string)));
    expect(printed.operationId).toBe("controlled-replay-1");
    expect(printed.operationIdSource).toBe("supplied");
    // No automatic mutation retry: exactly one call for one invocation.
    expect(post).toHaveBeenCalledTimes(1);
  });
  it("sends no caller epoch or generation and refuses an out-of-range lease before any call", async () => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig", "--lease-ms", "999"]);
    expect(post).not.toHaveBeenCalled();
    expect(calls).toHaveLength(0);
    expect(process.exitCode).toBe(1);
    expect(vi.mocked(console.log).mock.calls).toHaveLength(0);
  });
});

describe("coordinator terminal authentication", () => {
  it("show supplies real token-file headers and preserves rig escaping", async () => {
    const get = vi.spyOn(DaemonClient.prototype, "get");
    await coordinatorCommand().parseAsync(["node", "rig", "show", "rig/one"]);
    expect(get).toHaveBeenCalledWith("/api/coordinator/rig%2Fone", { headers: { Authorization: `Bearer ${token}` } });
    expect(process.exitCode).toBeUndefined();
  });
  it.each(operations)("%s supplies auth options and retains native actor/generation", async operation => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    await coordinatorCommand().parseAsync(["node", "rig", operation, contractFile({ actor: "body-claim-must-not-win", generation: "body-generation-must-not-win" })]);
    expect(post.mock.calls[0]?.[0]).toBe(`/api/coordinator/${operation}`);
    expect(post.mock.calls[0]?.[2]).toEqual({ headers: { Authorization: `Bearer ${token}` } });
    expect(calls).toHaveLength(1);
    if (operation !== "legacy-inventory") {
      expect(calls[0]?.actor).toBe(actor);
      expect(calls[0]?.generation).toBe(generation);
    }
    expect(process.exitCode).toBeUndefined();
  });
  it.each(["show", ...operations])("%s remains HTTP 401 without a token and never reaches authority", async operation => {
    fs.unlinkSync(path.join(home, "terminal-token"));
    await coordinatorCommand().parseAsync(["node", "rig", operation, operation === "show" ? "test-rig" : contractFile()]);
    expect(process.exitCode).toBe(1);
    expect(console.log).toHaveBeenCalledWith(expect.stringContaining("unauthorized"));
    expect(calls).toHaveLength(0);
  });
  it("does not manufacture absent native generation", async () => {
    vi.stubEnv("OPENRIG_OCCUPANT_GENERATION", "");
    await coordinatorCommand().parseAsync(["node", "rig", "enable", contractFile()]);
    expect(process.exitCode).toBe(1);
    expect(console.log).toHaveBeenCalledWith(expect.stringContaining("coordinator_caller_required"));
    expect(calls).toHaveLength(0);
  });
  it("forwards stale generation unchanged and preserves authority refusal", async () => {
    vi.stubEnv("OPENRIG_OCCUPANT_GENERATION", "stale-native-generation");
    const authority = { enable: (a: string, g: string) => {
      expect(a).toBe(actor); expect(g).toBe("stale-native-generation");
      throw new CoordinatorFenceError("coordinator_retired", "Retired caller");
    } };
    app = new Hono();
    app.use("*", async (c, next) => { c.set("queueRepo" as never, { coordinatorAuthority: authority } as never); await next(); });
    app.route("/api/coordinator", coordinatorRoutes({ bearerToken: token }));
    await coordinatorCommand().parseAsync(["node", "rig", "enable", contractFile()]);
    expect(process.exitCode).toBe(1);
    expect(console.log).toHaveBeenCalledWith(expect.stringContaining("coordinator_retired"));
  });
});
