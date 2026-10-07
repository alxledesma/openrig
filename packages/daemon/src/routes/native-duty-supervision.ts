import { Hono, type Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { NativeDutyError, type NativeDutySupervisionService } from "../domain/native-duty-supervision-service.js";
import { transportSenderSession } from "./require-sender-identity.js";
import type { NativeDutyActor } from "../domain/native-duty-contract.js";

export interface NativeDutyRoutesOptions {
  bearerToken: string | null;
  service: NativeDutySupervisionService;
  enrollment?: (actor: NativeDutyActor, input: { scopeId: string; launchId: string; supervisorPid: number }) => Promise<{ state: "waiting" | "ready" | "held"; registrationId?: string }>;
  refreshNative?: (actor: NativeDutyActor, input: { scopeId?: string; launchId?: string; supervisorPid?: number; registrationId?: string }) => Promise<void>;
}

/** Authenticated routes for opt-in continuation supervision; actor is always transport-derived. */
export function nativeDutySupervisionRoutes(opts: NativeDutyRoutesOptions): Hono {
  const app = new Hono();
  app.use("*", authBearerTokenMiddleware({ expectedToken: opts.bearerToken }));
  const actor = (c: Context) => {
    const session = transportSenderSession(c);
    const generation = c.req.header("X-OpenRig-Occupant-Generation");
    return session && generation ? { session, generation } : null;
  };
  const failure = (c: Context, error: unknown) => {
    if (error instanceof NativeDutyError) return c.json({ error: error.code, message: error.message }, error.status as ContentfulStatusCode);
    if (error instanceof SyntaxError) return c.json({ error: "invalid_json" }, 400);
    throw error;
  };
  app.get("/enrollment", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    if (!opts.enrollment) return c.json({ error: "native_duty_enrollment_unavailable" }, 503);
    const q = c.req.query();
    if (Object.keys(q).sort().join(",") !== "launchId,scopeId,supervisorPid" || !/^\d+$/.test(q.supervisorPid ?? "")) return c.json({ error: "native_duty_enrollment_request_invalid" }, 400);
    try { return c.json(await opts.enrollment(current, { scopeId: q.scopeId!, launchId: q.launchId!, supervisorPid: Number(q.supervisorPid) })); }
    catch (error) { return failure(c, error); }
  });
  app.post("/grant", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { return c.json(opts.service.grant(current, await c.req.json()), 201); } catch (error) { return failure(c, error); }
  });
  app.post("/revoke", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try {
      const body = await c.req.json();
      if (!body || typeof body !== "object" || Array.isArray(body) || Object.keys(body).length !== 1 || typeof body.scopeId !== "string") return c.json({ error: "native_duty_revoke_request_invalid" }, 400);
      return c.json(opts.service.revoke(current, body.scopeId));
    } catch (error) { return failure(c, error); }
  });
  app.post("/register", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { const body = await c.req.json(); if (body && typeof body === "object") await opts.refreshNative?.(current, body); return c.json(opts.service.register(current, body), 201); } catch (error) { return failure(c, error); }
  });
  app.post("/heartbeat", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { const body = await c.req.json(); if (!body || typeof body.registrationId !== "string" || Object.keys(body).length !== 1) return c.json({ error: "native_duty_heartbeat_request_invalid" }, 400); await opts.refreshNative?.(current, body); return c.json(opts.service.heartbeat(current, body.registrationId)); } catch (error) { return failure(c, error); }
  });
  app.post("/prepare", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { const body = await c.req.json(); if (body && typeof body === "object") await opts.refreshNative?.(current, body); return c.json(opts.service.prepare(current, body), 201); } catch (error) { return failure(c, error); }
  });
  app.post("/in-flight", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { const body = await c.req.json(); if (body && typeof body === "object") await opts.refreshNative?.(current, body); return c.json(opts.service.markInFlight(current, body)); } catch (error) { return failure(c, error); }
  });
  app.post("/reconcile", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { return c.json(opts.service.reconcile(current, await c.req.json())); } catch (error) { return failure(c, error); }
  });
  app.post("/stop", async (c) => {
    if (!opts.bearerToken) return c.json({ error: "native_duty_authenticated_control_required" }, 503);
    const current = actor(c); if (!current) return c.json({ error: "native_duty_actor_required" }, 403);
    try { return c.json(opts.service.stop(current, await c.req.json())); } catch (error) { return failure(c, error); }
  });
  app.get("/status/:registrationId", (c) => {
    try { return c.json(opts.service.status(c.req.param("registrationId"))); } catch (error) { return failure(c, error); }
  });
  return app;
}
