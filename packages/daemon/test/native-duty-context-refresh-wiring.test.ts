import { expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { inheritedNativeDutyTransport, nativeDutySupervisorEntry } from "../src/adapters/native-duty-supervisor.js";

it("routes only the inherited Operator helper to context refresh and retains ordinary holder enrollment", async () => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "native-duty-context-refresh-"));
  fs.chmodSync(root, 0o700);
  const journalDir = path.join(root, "journal"); fs.mkdirSync(journalDir, { mode: 0o700 }); fs.chmodSync(journalDir, 0o700);
  const launchId = "wiring-launch-20261007";
  const configPath = path.join(root, "supervisor-config.json");
  const config = { scopeId: "wiring-scope", launchId, journalDir,
    harness: { executable: "/usr/bin/true", args: [], cwd: root }, pollMs: 1000 };
  fs.writeFileSync(configPath, JSON.stringify(config) + "\n", { mode: 0o600 }); fs.chmodSync(configPath, 0o600);
  const envKeys = ["OPENRIG_SESSION_NAME", "OPENRIG_OCCUPANT_GENERATION", "OPENRIG_TERMINAL_BEARER_TOKEN",
    "OPENRIG_URL", "OPENRIG_NODE_ID", "OPENRIG_RUNTIME", "OPENRIG_HOME"] as const;
  const previous = new Map(envKeys.map(key => [key, process.env[key]]));
  const oldFetch = globalThis.fetch;
  const requests: Array<{ url: URL; method: string; headers: Headers }> = [];
  const listenersBefore = new Set(process.listeners("SIGTERM"));
  const operator = { session: "operator-agent@kernel", generation: "operator-native-g1" };
  const supervisorPid = process.ppid;
  const useActor = (session: string, generation: string) => {
    process.env.OPENRIG_SESSION_NAME = session;
    process.env.OPENRIG_OCCUPANT_GENERATION = generation;
    process.env.OPENRIG_TERMINAL_BEARER_TOKEN = "synthetic-test-token";
    process.env.OPENRIG_URL = "http://127.0.0.1:45678";
    process.env.OPENRIG_NODE_ID = "operator-node";
    process.env.OPENRIG_RUNTIME = "codex";
    delete process.env.OPENRIG_HOME;
  };
  globalThis.fetch = (async (input, init) => {
    const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url);
    requests.push({ url, method: init?.method ?? "GET", headers: new Headers(init?.headers) });
    const payload = url.pathname === "/api/context-refresh/enrollment"
      ? { state: "held", reason: "fixture-no-grant" }
      : { state: "held", reason: "fixture-no-holder-grant" };
    const response = new Response(JSON.stringify(payload), { status: 200, headers: { "Content-Type": "application/json" } });
    if (url.pathname === "/api/context-refresh/enrollment") {
      // Invoke only the helper entry's just-installed local abort callback. This
      // ends its loop without sending an OS signal or affecting other listeners.
      const stop = process.listeners("SIGTERM").find(listener => !listenersBefore.has(listener));
      if (!stop) throw new Error("helper-abort-listener-not-installed");
      (stop as () => void)();
    }
    return response;
  }) as typeof fetch;

  try {
    expect(fs.realpathSync(root)).toBe(root);
    expect(fs.realpathSync(journalDir)).toBe(journalDir);
    expect(fs.statSync(root).mode & 0o077).toBe(0);
    expect(fs.statSync(journalDir).mode & 0o077).toBe(0);
    expect(fs.statSync(configPath).mode & 0o777).toBe(0o600);

    useActor(operator.session, operator.generation);
    await expect(nativeDutySupervisorEntry(["--helper", configPath, String(supervisorPid)])).resolves.toBe(0);
    expect(requests).toHaveLength(1);
    expect(requests[0]!.url.pathname).toBe("/api/context-refresh/enrollment");
    expect(requests[0]!.url.searchParams.get("launchId")).toBe(launchId);
    expect(requests[0]!.url.searchParams.get("supervisorPid")).toBe(String(supervisorPid));
    expect(requests[0]!.method).toBe("GET");
    expect(requests[0]!.headers.get("Authorization")).toBe("Bearer synthetic-test-token");
    expect(requests[0]!.headers.get("X-OpenRig-Session")).toBe(operator.session);
    expect(requests[0]!.headers.get("X-OpenRig-Occupant-Generation")).toBe(operator.generation);

    const inherited = inheritedNativeDutyTransport();
    expect(() => inherited.contextRefreshTransport.enrollment({ session: "forged-actor", generation: operator.generation },
      { launchId, supervisorPid })).toThrow("context-refresh-inherited-actor-mismatch");
    expect(requests).toHaveLength(1); // substitution is refused before fetch

    useActor("builder@rig", "builder-native-g3");
    await expect(nativeDutySupervisorEntry(["--helper", configPath, String(supervisorPid)])).rejects.toThrow("native-duty-enrollment-held");
    expect(requests).toHaveLength(2);
    expect(requests[1]!.url.pathname).toBe("/api/native-duty/enrollment");
    expect(requests[1]!.url.searchParams.get("launchId")).toBe(launchId);
    expect(requests[1]!.url.searchParams.get("supervisorPid")).toBe(String(supervisorPid));
    expect(requests.some(request => request.url.pathname === "/api/context-refresh/enrollment" && request.headers.get("X-OpenRig-Session") === "builder@rig")).toBe(false);
    expect(requests.map(request => request.url.pathname)).not.toContain("/api/native-duty/register");
  } finally {
    globalThis.fetch = oldFetch;
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});
