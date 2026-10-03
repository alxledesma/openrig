import { Hono } from "hono";
import { getConnInfo } from "@hono/node-server/conninfo";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { transportSenderSession } from "./require-sender-identity.js";
import { isRotationLoopback } from "../domain/rotation-precondition.js";
import { rotationFactsResolver, resolveRotationNativeState } from "../domain/rotation-facts-resolver.js";
import { SeatDispatchReservationService, DispatchReservationError, type ReservationRequest } from "../domain/seat-dispatch-reservation.js";
import { censusAttemptNative, readHistoricalFailure } from "../domain/failed-precommit-proof.js";
import type { RigRepository } from "../domain/rig-repository.js";
import type { TmuxAdapter } from "../adapters/tmux.js";
import type { WhoamiService } from "../domain/whoami-service.js";
import type { SeatActivityService } from "../domain/seat-activity-service.js";

export function dispatchReservationRoutes(opts: { bearerToken: string | null }): Hono {
 const app = new Hono();
 app.use("*", authBearerTokenMiddleware({ expectedToken: opts.bearerToken }));
 app.use("*", async (c, next) => {
   if (!opts.bearerToken) return c.json({ code: "reservation_authenticated_control_required" }, 503);
   let address: string | undefined; try { address = getConnInfo(c).remote.address; } catch { /* unavailable is unknown */ }
   if (c.req.header("Origin") || !isRotationLoopback(address)) return c.json({ code: "reservation_local_only" }, 403);
   await next();
 });
 app.post("/:operation", async c => {
   const actor = transportSenderSession(c), generation = c.req.header("X-OpenRig-Occupant-Generation");
   if (!actor || !generation) return c.json({ code: "reservation_caller_required" }, 403);
   const rigRepo = c.get("rigRepo" as never) as RigRepository, tmux = c.get("tmuxAdapter" as never) as TmuxAdapter;
   const root = process.env["OPENRIG_ROTATION_ROOT"];
   if (!root || !tmux.deliveryGuard || tmux.deliveryGuard.db !== rigRepo.db) return c.json({ code: "reservation_runtime_proof_unavailable" }, 503);
   const deps = { db: rigRepo.db, tmux, whoami: c.get("whoamiService" as never) as WhoamiService, activity: c.get("seatActivityService" as never) as SeatActivityService, root };
   const service = new SeatDispatchReservationService({ db: rigRepo.db, guard: tmux.deliveryGuard,
     verifyPredecessor: rotationFactsResolver(deps),
     observeSuccessor: async seat => { const state = await resolveRotationNativeState(deps, seat); return { nativeId: state.usage.sessionId!, runtimeContract: state.runtimeContract }; },
     censusFailedAttempt: censusAttemptNative,
     historicalFailure: (reference,sha256,reservation) => readHistoricalFailure(root,reference,sha256,reservation),
   });
   try {
     const body = await c.req.json(), operation = c.req.param("operation");
     if (operation === "reserve") return c.json(await service.reserve(actor, generation, body as ReservationRequest), 201);
     if (operation === "attest") return c.json(await service.attest(actor, generation, body.reservationId, body));
     if (operation === "release") return c.json(await service.release(actor, generation, body.reservationId, body));
     if (operation === "abandon-failed-precommit") return c.json(await service.abandonFailedPrecommit(actor,generation,body.reservationId,body));
     return c.json({ code: "reservation_operation_unknown" }, 404);
   } catch (error) {
     if (error instanceof DispatchReservationError) return error.getResponse();
     if (error instanceof SyntaxError) return c.json({ code: "invalid_json" }, 400);
     if (error instanceof Error && error.message.includes("UNIQUE constraint failed: seat_dispatch_reservations")) return c.json({ code: "seat_dispatch_reserved", retryable: true }, 409);
     throw error;
   }
 });
 return app;
}
