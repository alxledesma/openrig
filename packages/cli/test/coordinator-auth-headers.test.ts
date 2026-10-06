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
    get: (rigId: string) => ({ rigId, epoch: 4, state: "reconciling" }), obligations: () => [], reconciliationDigest: () => "fixture-digest",
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
  it("derives the expected contract from one show read and prepares it before POSTing", async () => {
    const get = vi.spyOn(DaemonClient.prototype, "get");
    const post = vi.spyOn(DaemonClient.prototype, "post");
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig"]);
    expect(get).toHaveBeenCalledTimes(1);
    expect(get.mock.calls[0]?.[0]).toBe("/api/coordinator/test-rig");
    expect(get.mock.calls[0]?.[1]).toEqual({ headers: { Authorization: `Bearer ${token}` } });
    expect(post).toHaveBeenCalledTimes(1);
    expect(post.mock.calls[0]?.[0]).toBe("/api/coordinator/resume-owned");
    expect(post.mock.calls[0]?.[2]).toEqual({ headers: { Authorization: `Bearer ${token}` } });
    const body = post.mock.calls[0]?.[1] as Record<string, unknown>;
    // Derived from the read, so the operator invents neither field.
    expect(body.expectedEpoch).toBe(4);
    expect(body.expectedObligationsDigest).toBe("fixture-digest");
    expect(body.rigId).toBe("test-rig");
    expect(body.leaseMs).toBe(1200000);
    expect(body.operationId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    expect(Object.keys(body).sort()).toEqual(["expectedEpoch", "expectedObligationsDigest", "leaseMs", "operationId", "rigId"]);
    // A timeout must never lose the request: it is persisted owner-only, with no credentials in it.
    const preparedPath = String(body.operationId && err.mock.calls.map(c => String(c[0])).join("").match(/prepared request: (\S+)/)?.[1]);
    expect(preparedPath).toMatch(/openrig-resume-owned-/);
    expect(fs.statSync(preparedPath).mode & 0o777).toBe(0o600);
    const persisted = JSON.parse(fs.readFileSync(preparedPath, "utf8"));
    expect(persisted).toEqual(body);
    expect(JSON.stringify(persisted)).not.toContain(token);
    // The exact id and path are announced before the network mutation.
    expect(err.mock.calls.map(c => String(c[0])).join("")).toContain(body.operationId as string);
    const printed = JSON.parse(String((vi.mocked(console.log).mock.calls.at(-1)?.[0] as string)));
    expect(printed.operationId).toBe(body.operationId);
    expect(printed.operationIdSource).toBe("generated");
    expect(calls[0]?.actor).toBe(actor);
    expect(calls[0]?.generation).toBe(generation);
    expect(process.exitCode).toBeUndefined();
  });
  it("replays the exact prepared contract with no read, no new id and no retry", async () => {
    const get = vi.spyOn(DaemonClient.prototype, "get");
    const post = vi.spyOn(DaemonClient.prototype, "post");
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig"]);
    const sent = post.mock.calls[0]?.[1] as Record<string, unknown>;
    const preparedPath = String(err.mock.calls.map(c => String(c[0])).join("").match(/prepared request: (\S+)/)?.[1]);
    // Simulate the operator replaying after an unknown outcome.
    get.mockClear(); post.mockClear(); calls.length = 0;
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig", "--replay-contract", preparedPath]);
    // No fresh read, no regenerated id: the exact original bytes are resubmitted.
    expect(get).not.toHaveBeenCalled();
    expect(post).toHaveBeenCalledTimes(1);
    expect(post.mock.calls[0]?.[1]).toEqual(sent);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.body).toEqual(sent);
    const printed = JSON.parse(String((vi.mocked(console.log).mock.calls.at(-1)?.[0] as string)));
    expect(printed.operationId).toBe(sent.operationId);
    expect(printed.operationIdSource).toBe("replayed");
  });
  it("refuses a replay contract that is unreadable, incomplete, or for another rig", async () => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const missing = path.join(home, "absent.json");
    const partial = path.join(home, "partial.json");
    const otherRig = path.join(home, "other.json");
    fs.writeFileSync(partial, JSON.stringify({ rigId: "test-rig", operationId: "x" }));
    fs.writeFileSync(otherRig, JSON.stringify({ rigId: "elsewhere", leaseMs: 60000, operationId: "y", expectedEpoch: 4, expectedObligationsDigest: "d" }));
    for(const file of [missing, partial, otherRig]){
      await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig", "--replay-contract", file]);
      expect(post).not.toHaveBeenCalled();
      expect(calls).toHaveLength(0);
      expect(process.exitCode).toBe(1);
    }
  });
  it("does not POST when preparation cannot persist the request", async () => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    vi.spyOn(DaemonClient.prototype, "get").mockResolvedValue({ status: 200, data: { authority: { epoch: 4 }, obligationsDigest: "fixture-digest" } } as never);
    vi.spyOn(fs, "writeFileSync").mockImplementation(() => { throw new Error("disk unavailable"); });
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig"]);
    expect(post).not.toHaveBeenCalled();
    expect(calls).toHaveLength(0);
    expect(process.exitCode).toBe(1);
  });
  it("refuses an out-of-range lease before reading, preparing or posting anything", async () => {
    const post = vi.spyOn(DaemonClient.prototype, "post");
    const get = vi.spyOn(DaemonClient.prototype, "get");
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig", "--lease-ms", "999"]);
    expect(post).not.toHaveBeenCalled();
    expect(get).not.toHaveBeenCalled();
    expect(calls).toHaveLength(0);
    expect(process.exitCode).toBe(1);
  });
  it("does not post when the supported read supplies no usable contract", async () => {
    vi.spyOn(DaemonClient.prototype, "get").mockResolvedValue({ status: 200, data: { authority: {}, obligationsDigest: "" } } as never);
    const post = vi.spyOn(DaemonClient.prototype, "post");
    await coordinatorCommand().parseAsync(["node", "rig", "resume-owned", "test-rig"]);
    expect(post).not.toHaveBeenCalled();
    expect(calls).toHaveLength(0);
    expect(process.exitCode).toBe(1);
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
