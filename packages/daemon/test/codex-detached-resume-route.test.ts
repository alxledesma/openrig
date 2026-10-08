import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { seatRoutes } from "../src/routes/seat.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";

const transport = vi.hoisted(() => ({ address: "127.0.0.1" }));
vi.mock("@hono/node-server/conninfo", () => ({ getConnInfo: () => ({ remote: { address: transport.address } }) }));
const operator = "operator-agent@kernel", operatorGeneration = "operator-agent-g1";
const peer = "lead@xv", peerGeneration = "lead-g1";
const token = "route-fixture-token";

describe("detached Codex resume route authority", () => {
  let db: ReturnType<typeof createDb>, guard: SeatDeliveryGuard, app: Hono;
  let resumeDetached: ReturnType<typeof vi.fn>, configuredToken: string | null;
  const peerBody = { reason: "Resume exact detached peer", codexDetachedResume: true };
  const peerHeaders = () => ({ Authorization: `Bearer ${token}`, "X-OpenRig-Session": operator, "X-OpenRig-Occupant-Generation": operatorGeneration });
  const postPeer = (body: unknown = peerBody, headers: Record<string,string> = peerHeaders()) => app.request(`/api/seat/rehost-runner/${encodeURIComponent(peer)}`, { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(body) });
  const postLocal = (body: unknown = { reason: "Repair exact local Operator", expected: { nodeId: operator, generation: operatorGeneration }, codexDetachedResume: true }, headers: Record<string,string> = { Authorization: `Bearer ${token}` }) => app.request("/api/seat/operator-maintenance/rehost-runner", { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(body) });

  beforeEach(async () => {
    transport.address = "127.0.0.1"; configuredToken = token;
    db = createDb(); seed(db);
    db.prepare("UPDATE nodes SET runtime='codex',cwd='/fixture',model='gpt-6-luna',effort='high',codex_config_profile='exact' WHERE id IN (?,?)").run(operator, peer);
    db.prepare("UPDATE sessions SET status='running',startup_status='ready',origin='claimed' WHERE node_id=?").run(operator);
    db.prepare("UPDATE sessions SET status='detached',startup_status='ready',origin='claimed',resume_type='codex_id',resume_token='peer-native' WHERE node_id=?").run(peer);
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('operator-binding',?,?, '%9')").run(operator,operator);
    db.prepare("INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES ('peer-binding',?,?, '%78')").run(peer,peer);
    guard = new SeatDeliveryGuard(db, name => resolveGuardTarget(db, name));
    await guard.set(operator, true, "operator", "route fixture"); await guard.set(peer, true, "operator", "route fixture");
    resumeDetached = vi.fn(async (input: unknown) => ({ ok: true, nodeId: (input as any).nodeId, generation: (input as any).actorGeneration, generationUnchanged: true, custodyPreserved: true }));
    const context = {
      rigRepo: new RigRepository(db), sessionRegistry: new SessionRegistry(db), eventBus: new EventBus(db),
      tmuxAdapter: { deliveryGuard: guard }, codexRehost: { resumeDetached },
    };
    app = new Hono();
    app.use("*", async (c, next) => { for (const [key,value] of Object.entries(context)) c.set(key as never,value as never); c.set("terminalBearerToken" as never,configuredToken as never); await next(); });
    app.route("/api/seat", seatRoutes);
  });
  afterEach(() => db?.close());

  it("authenticates a current Operator transport generation, rejects self-target and conflicting modes", async () => {
    const response = await postPeer(); expect(response.status).toBe(200);
    expect(resumeDetached).toHaveBeenCalledOnce();
    expect(resumeDetached.mock.calls[0]![0]).toMatchObject({ nodeId: peer, sessionName: peer, operator, actorGeneration: operatorGeneration });
    resumeDetached.mockClear();
    expect((await postPeer(peerBody, { ...peerHeaders(), "X-OpenRig-Occupant-Generation": "stale" })).status).toBe(403);
    expect((await postPeer(peerBody, { ...peerHeaders(), "X-OpenRig-Session": peer })).status).toBe(403);
    expect((await postPeer(peerBody, { ...peerHeaders(), "X-OpenRig-Origin-Unknown": "true" })).status).toBe(403);
    expect((await postPeer({ ...peerBody, legacyNativeWitness: true })).status).toBe(400);
    expect((await app.request(`/api/seat/rehost-runner/${encodeURIComponent(operator)}`, { method:"POST", headers:{"Content-Type":"application/json",...peerHeaders()}, body:JSON.stringify(peerBody) })).status).toBe(403);
    expect(resumeDetached).not.toHaveBeenCalled();
  });

  it("derives local maintenance identity server-side and enforces exact expected generation", async () => {
    const response = await postLocal(); expect(response.status).toBe(200);
    expect(resumeDetached).toHaveBeenCalledOnce();
    expect(resumeDetached.mock.calls[0]![0]).toMatchObject({ nodeId: operator, sessionName: operator, actorGeneration: operatorGeneration, maintenanceAuthority: expect.any(Object) });
    expect((resumeDetached.mock.calls[0]![0] as any).maintenanceAuthority).toMatchObject({ mode: "detached-resume" });
    resumeDetached.mockClear();
    expect((await postLocal({ reason:"wrong pin", expected:{nodeId:operator,generation:"old"}, codexDetachedResume:true })).status).toBe(409);
    expect((await postLocal({ reason:"mixed", expected:{nodeId:operator,generation:operatorGeneration}, codexDetachedResume:true, legacyCodexProfile:"exact" })).status).toBe(400);
    expect((await postLocal(undefined, { Authorization:`Bearer ${token}`, "X-OpenRig-Session":operator })).status).toBe(400);
    expect(resumeDetached).not.toHaveBeenCalled();
  });
});
