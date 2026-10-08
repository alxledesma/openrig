import { describe, it, expect, vi } from "vitest";
import { contextRefreshRoutes } from "../src/routes/context-refresh.js";
import type { ContextRefreshFacade } from "../src/domain/context-refresh-integration.js";

const headers = { Authorization: "Bearer private-test-token", "X-OpenRig-Session": "operator-agent@kernel", "X-OpenRig-Occupant-Generation": "current-generation", "Content-Type": "application/json" };
function fixture() {
  const step = vi.fn(async (_actor: unknown, input: unknown) => input);
  const status = vi.fn(async (_actor: unknown, input: unknown) => input);
  const service = { step, status, reconcile: status } as unknown as ContextRefreshFacade;
  return { step, status, app: contextRefreshRoutes({ bearerToken: "private-test-token", service }) };
}
describe("finite context refresh transport boundary", () => {
  it("returns a target draft submission without claiming an idle frozen checkpoint", async () => {
    const submitted = { draftId: "draft-1", grantId: "grant", nodeId: "target", phase: "submitted" };
    const submitCheckpoint = vi.fn(async () => submitted);
    const app = contextRefreshRoutes({ bearerToken: "private-test-token", service: { submitCheckpoint } as unknown as ContextRefreshFacade });
    const packet = { current_work: "task", decisions: [], memory: [], constraints: [], standing_duties: [], evidence: [], next_action: "continue", outstanding_effects: [] };
    const targetHeaders = { ...headers, "X-OpenRig-Session": "worker@rig" };
    const response = await app.request("/checkpoint", { method: "POST", headers: targetHeaders,
      body: JSON.stringify({ grantId: "grant", nodeId: "target", packet }) });
    expect(response.status).toBe(201); expect(await response.json()).toEqual(submitted);
    expect(submitCheckpoint).toHaveBeenCalledWith({ session: "worker@rig", generation: "current-generation" }, { grantId: "grant", nodeId: "target", packet });
    const refused = await app.request("/checkpoint", { method: "POST", headers: targetHeaders,
      body: JSON.stringify({ grantId: "grant", nodeId: "target", packet: { ...packet, outstanding_effects: ["unresolved"] } }) });
    expect(refused.status).toBe(400); expect(submitCheckpoint).toHaveBeenCalledTimes(1);
  });
  it("derives the actor from authenticated transport and preserves exact invocation identity", async () => {
    const f = fixture(), body = { grantId: "finite-grant", nodeId: "target", operationId: "exact-operation" };
    expect((await f.app.request("/step", { method: "POST", headers, body: JSON.stringify(body) })).status).toBe(200);
    expect(f.step).toHaveBeenCalledWith({ session: "operator-agent@kernel", generation: "current-generation" }, body);
    const response = await f.app.request("/status?grantId=finite-grant&nodeId=target&operationId=exact-operation", { headers });
    expect(response.status).toBe(200); expect(await response.json()).toEqual(body);
    expect((await f.app.request("/reconcile", { method: "POST", headers, body: JSON.stringify(body) })).status).toBe(200);
  });
  it("refuses missing authentication or generation before invoking effects", async () => {
    const f = fixture(), body = JSON.stringify({ grantId: "grant", nodeId: "node", operationId: "operation" });
    expect((await f.app.request("/step", { method: "POST", body })).status).toBe(401);
    expect((await f.app.request("/step", { method: "POST", headers: { Authorization: headers.Authorization, "X-OpenRig-Session": headers["X-OpenRig-Session"] }, body })).status).toBe(403);
    expect(f.step).not.toHaveBeenCalled();
  });
  it("rejects caller proof, actor overrides and malformed operation IDs", async () => {
    const f = fixture();
    for (const extra of [{ actor: { session: "other", generation: "other" } }, { nativeVerified: true }, { operationId: "" }]) {
      const response = await f.app.request("/step", { method: "POST", headers, body: JSON.stringify({ grantId: "grant", nodeId: "node", operationId: "operation", ...extra }) });
      expect(response.status).toBe(400);
    }
    expect(f.step).not.toHaveBeenCalled();
  });
});
