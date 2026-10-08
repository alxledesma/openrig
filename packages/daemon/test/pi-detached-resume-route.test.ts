import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { createTestApp, mockTmuxAdapter } from "./helpers/test-app.js";

const transport = vi.hoisted(() => ({ address: "127.0.0.1" }));
vi.mock("@hono/node-server/conninfo", () => ({ getConnInfo: () => ({ remote: { address: transport.address } }) }));

const operator = "operator-agent@kernel";
const operatorGeneration = "operator-agent-g1";
const peer = "lead@xv";
const peerGeneration = "lead-g1";
const token = "pi-detached-route-test-token";
const recovery = { attemptId: "c47de976-e4eb-4028-9b05-ae1efbd3caaa", beganSha256: "a".repeat(64) };

describe("guarded detached Pi resume route", () => {
  let db: ReturnType<typeof createDb>;
  let guard: SeatDeliveryGuard;
  let app: ReturnType<typeof createTestApp>["app"];
  let run: ReturnType<typeof vi.fn>;

  const peerHeaders = () => ({ Authorization: `Bearer ${token}`, "X-OpenRig-Session": operator, "X-OpenRig-Occupant-Generation": operatorGeneration });
  const postPeer = (body: unknown = { reason: "continue exact detached Pi", piDetachedResume: true }, headers: Record<string,string> = peerHeaders()) =>
    app.request(`/api/seat/rehost-runner/${encodeURIComponent(peer)}`, { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(body) });

  beforeEach(async () => {
    transport.address = "127.0.0.1";
    db = createDb();
    seed(db);
    db.prepare("UPDATE nodes SET runtime='codex',cwd='/fixture',model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id=?").run(operator);
    db.prepare("UPDATE nodes SET runtime='pi',cwd='/fixture/peer',model='openrouter/nvidia/nemotron-3-ultra-550b-a55b:free' WHERE id=?").run(peer);
    db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed' WHERE node_id=?").run(operator);
    db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='pi_session_file',resume_token='/private/exact-session.jsonl' WHERE node_id=?").run(peer);
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('operator-binding',?,?, '%9')").run(operator, operator);
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('peer-binding',?,?, '%78')").run(peer, peer);
    guard = new SeatDeliveryGuard(db, name => resolveGuardTarget(db, name));
    await guard.set(operator, true, "operator", "route fixture");
    await guard.set(peer, true, "operator", "route fixture");
    run = vi.fn(async (input: unknown) => ({ ok: true, runtime: "pi", nodeId: (input as any).nodeId, sessionName: (input as any).sessionName, generation: peerGeneration, generationUnchanged: true, custodyPreserved: true, guardLeftEnabled: true, authorityRepaired: false }));
    const tmuxAdapter = Object.assign(mockTmuxAdapter(), { deliveryGuard: guard });
    ({ app } = createTestApp(db, {
      tmux: tmuxAdapter,
      appDeps: { terminalBearerToken: token, piDetachedResume: { run } as never },
    }));
  });

  afterEach(() => db?.close());

  it("authenticates current peer Operator transport and forwards only the exact typed recovery input", async () => {
    const response = await postPeer({ reason: "continue exact detached Pi", operator: "caller-forgery", piDetachedResume: true, piDetachedRecovery: recovery });
    const payload = await response.json();
    expect(response.status, JSON.stringify(payload)).toBe(200);
    expect(run).toHaveBeenCalledOnce();
    expect(run.mock.calls[0]![0]).toEqual({ nodeId: peer, sessionName: peer, reason: "continue exact detached Pi", operator, actorGeneration: operatorGeneration, recovery });

    run.mockClear();
    expect((await postPeer(undefined, { ...peerHeaders(), "X-OpenRig-Occupant-Generation": "stale" })).status).toBe(403);
    expect((await postPeer(undefined, { ...peerHeaders(), "X-OpenRig-Session": peer })).status).toBe(403);
    expect((await postPeer(undefined, { ...peerHeaders(), "X-OpenRig-Origin-Unknown": "true" })).status).toBe(403);
    expect((await postPeer(undefined, { ...peerHeaders(), Authorization: "Bearer wrong" })).status).toBe(401);
    expect(run).not.toHaveBeenCalled();
  });

  it("rejects self-target and every mixed recovery mode before the engine", async () => {
    const self = await app.request(`/api/seat/rehost-runner/${encodeURIComponent(operator)}`, {
      method: "POST", headers: { "Content-Type": "application/json", ...peerHeaders() },
      body: JSON.stringify({ reason: "self", piDetachedResume: true }),
    });
    expect(self.status).toBe(403);
    for (const body of [
      { reason: "mixed", piDetachedResume: true, codexDetachedResume: true },
      { reason: "mixed", piDetachedResume: true, legacyNativeWitness: true },
      { reason: "mixed", piDetachedResume: true, stoppedTargetRecovery: true, stoppedTargetAcceptanceReference: "owner-ref" },
      { reason: "mixed", piDetachedResume: true, legacyCodexProfile: "profile" },
      { reason: "local maintenance is not Pi", expected: { nodeId: operator, generation: operatorGeneration }, piDetachedResume: true },
    ]) {
      const path = body.expected ? "/api/seat/operator-maintenance/rehost-runner" : `/api/seat/rehost-runner/${encodeURIComponent(peer)}`;
      const response = await app.request(path, { method: "POST", headers: { "Content-Type": "application/json", ...peerHeaders() }, body: JSON.stringify(body) });
      expect(response.status).toBe(400);
    }
    expect(run).not.toHaveBeenCalled();
  });

  it("requires recovery attempt and digest to be paired, exact, and Pi-mode-only", async () => {
    for (const body of [
      { reason: "missing digest", piDetachedResume: true, piDetachedRecovery: { attemptId: recovery.attemptId } },
      { reason: "bad uuid", piDetachedResume: true, piDetachedRecovery: { attemptId: "x", beganSha256: recovery.beganSha256 } },
      { reason: "bad hash", piDetachedResume: true, piDetachedRecovery: { attemptId: recovery.attemptId, beganSha256: "A".repeat(64) } },
      { reason: "extra proof", piDetachedResume: true, piDetachedRecovery: { ...recovery, pid: 5 } },
      { reason: "without mode", piDetachedRecovery: recovery },
      { reason: "nonboolean", piDetachedResume: "true" },
    ]) expect((await postPeer(body)).status).toBe(400);
    expect(run).not.toHaveBeenCalled();
  });
});
