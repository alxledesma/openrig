import { Hono, type Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { transportSenderSession } from "./require-sender-identity.js";
import { ContextRefreshError } from "../domain/context-refresh-service.js";
import type { ContextRefreshFacade } from "../domain/context-refresh-integration.js";
import type { ContextRefreshActor } from "../domain/context-refresh-contract.js";

export function contextRefreshRoutes(opts: { bearerToken: string | null; service: ContextRefreshFacade }): Hono {
  const app = new Hono();
  app.use("*", authBearerTokenMiddleware({ expectedToken: opts.bearerToken }));
  const actor = (c: Context): ContextRefreshActor | null => {
    const session = transportSenderSession(c), generation = c.req.header("X-OpenRig-Occupant-Generation");
    return session && generation ? { session, generation } : null;
  };
  const fail = (c: Context, e: unknown) => {
    if (e instanceof SyntaxError || e instanceof TypeError) return c.json({ error: "context_refresh_invalid_request" }, 400);
    if (e instanceof ContextRefreshError) return c.json({ error: e.code, message: e.message }, 409);
    if (e && typeof e === "object" && "code" in e && typeof e.code === "string") {
      const status = "status" in e && typeof e.status === "number" && e.status >= 400 && e.status < 600 ? e.status : 409;
      return c.json({ error: e.code }, status as ContentfulStatusCode);
    }
    throw e;
  };
  app.use("*", async (c, next) => {
    if (!opts.bearerToken) return c.json({ error: "context_refresh_authenticated_control_required" }, 503);
    if (!actor(c)) return c.json({ error: "context_refresh_actor_required" }, 403);
    await next();
  });
  const exact = (body: unknown, keys: string[]): Record<string, unknown> => {
    if (!body || typeof body !== "object" || Array.isArray(body)
      || Object.keys(body).sort().join(",") !== [...keys].sort().join(",")) throw new TypeError("shape");
    return body as Record<string, unknown>;
  };
  const id = (v: unknown): string => {
    if (typeof v !== "string" || !v.trim() || v.length > 200 || /[\x00-\x1f]/.test(v)) throw new TypeError("id");
    return v;
  };
  const selection = (b: Record<string, unknown>) => ({ grantId: id(b.grantId), nodeId: id(b.nodeId) });
  const receiptSelection = (value: unknown) => {
    const hasOperation = !!value && typeof value === "object" && Object.hasOwn(value, "operationId");
    const b = exact(value, hasOperation ? ["grantId", "nodeId", "operationId"] : ["grantId", "nodeId"]);
    return { ...selection(b), ...(hasOperation ? { operationId: id(b.operationId) } : {}) };
  };
  app.get("/enrollment", async c => {
    try {
      const b = exact(c.req.query(), ["launchId", "supervisorPid"]);
      if (typeof b.supervisorPid !== "string" || !/^[1-9]\d*$/.test(b.supervisorPid) || !Number.isSafeInteger(Number(b.supervisorPid))) throw new TypeError("pid");
      return c.json(await opts.service.enrollment(actor(c)!, { launchId: id(b.launchId), supervisorPid: Number(b.supervisorPid) }));
    } catch (e) { return fail(c, e); }
  });
  for (const method of ["status", "observe", "evaluate"] as const) app.get(`/${method}`, async c => {
    try { return c.json(await opts.service[method](actor(c)!, method === "status" ? receiptSelection(c.req.query()) : selection(exact(c.req.query(), ["grantId", "nodeId"])))); }
    catch (e) { return fail(c, e); }
  });
  app.post("/grant", async c => {
    try { return c.json(await opts.service.grant(actor(c)!, await c.req.json()), 201); }
    catch (e) { return fail(c, e); }
  });
  app.post("/revoke", async c => {
    try { const b = exact(await c.req.json(), ["grantId"]); await opts.service.revoke(actor(c)!, { grantId: id(b.grantId) }); return c.json({ revoked: true }); }
    catch (e) { return fail(c, e); }
  });
  app.post("/checkpoint", async c => {
    try {
      const b = exact(await c.req.json(), ["grantId", "nodeId", "packet"]);
      const packet = exact(b.packet, ["current_work", "decisions", "memory", "constraints", "standing_duties", "evidence", "next_action", "outstanding_effects"]);
      if (!Array.isArray(packet.outstanding_effects) || packet.outstanding_effects.length) throw new TypeError("effects");
      // Submission records the genuine target's draft. The native executor
      // freezes it later, after the author has finished its tool call and turn.
      return c.json(await opts.service.submitCheckpoint(actor(c)!, { ...selection(b), packet: packet as unknown as Parameters<ContextRefreshFacade["submitCheckpoint"]>[1]["packet"] }), 201);
    } catch (e) { return fail(c, e); }
  });
  app.post("/step", async c => {
    try { const b = exact(await c.req.json(), ["grantId", "nodeId", "operationId"]); return c.json(await opts.service.step(actor(c)!, { ...selection(b), operationId: id(b.operationId) })); }
    catch (e) { return fail(c, e); }
  });
  app.post("/reconcile", async c => {
    try { return c.json(await opts.service.reconcile(actor(c)!, receiptSelection(await c.req.json()))); }
    catch (e) { return fail(c, e); }
  });
  app.post("/attest", async c => {
    try {
      const b = exact(await c.req.json(), ["attemptId", "kind", "evidenceRef"]);
      if (b.kind !== "successor_ack" && b.kind !== "independent_acceptance") throw new TypeError("kind");
      return c.json(await opts.service.attest(actor(c)!, { attemptId: id(b.attemptId), kind: b.kind, evidenceRef: id(b.evidenceRef) }));
    } catch (e) { return fail(c, e); }
  });
  return app;
}
