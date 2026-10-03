import { Hono } from "hono";
import { authBearerTokenMiddleware } from "../middleware/auth-bearer-token.js";
import { transportSenderSession } from "./require-sender-identity.js";
import type { QueueRepository } from "../domain/queue-repository.js";
import { CoordinatorFenceError } from "../domain/coordinator-authority-service.js";

/** Mutating controls require the terminal bearer plus a registered current caller generation.
 * This is the existing trusted-local-seat transport boundary, not hostile caller authentication. */
export function coordinatorRoutes(opts:{bearerToken:string|null}):Hono {
 const app=new Hono();
 app.use("*",authBearerTokenMiddleware({expectedToken:opts.bearerToken}));
 app.get("/:rigId",c=>{
   const svc=(c.get("queueRepo" as never) as QueueRepository).coordinatorAuthority;
   const rigId=c.req.param("rigId"), authority=svc.get(rigId);
   return authority?c.json({authority,coordinationPlan:svc.coordinationRecovery?.plan(rigId)??null,obligations:svc.obligations(rigId),obligationsDigest:svc.reconciliationDigest(rigId)}):c.json({error:"coordinator_not_enabled"},404);
 });
 app.post("/:operation",async c=>{
   // No active token means controls are unavailable rather than silently unauthenticated.
   if(!opts.bearerToken)return c.json({error:"coordinator_authenticated_control_required"},503);
   const actor=transportSenderSession(c), generation=c.req.header("X-OpenRig-Occupant-Generation");
   if(!actor||!generation)return c.json({error:"coordinator_caller_required"},403);
   const svc=(c.get("queueRepo" as never) as QueueRepository).coordinatorAuthority;
   try {
     const operation=c.req.param("operation");
     if(operation==="outcome-configure"){if(!svc.runtimeOutcomeAssessment)throw new CoordinatorFenceError("runtime_outcome_unavailable","Service not wired");svc.runtimeOutcomeAssessment.configure(actor,generation,await c.req.json());return c.json({ok:true});}
     if(operation==="coordination-plan"){const b=await c.req.json();if(!svc.coordinationRecovery)throw new CoordinatorFenceError('coordination_unavailable','Service not wired');return c.json(svc.coordinationRecovery.configure(actor,generation,b));}
     if(operation==="coordination-reconcile"){const b=await c.req.json();if(!svc.coordinationRecovery)throw new CoordinatorFenceError('coordination_unavailable','Service not wired');const result=svc.coordinationRecovery.reconcile(actor,generation,b.rigId);await svc.coordinationRecovery.deliverCommitted();return c.json(result);}
     if(operation==="coordination-continue-custody"){if(!svc.coordinationRecovery)throw new CoordinatorFenceError("coordination_unavailable","Service not wired");const result=svc.coordinationRecovery.continueCustody(actor,generation,await c.req.json());await svc.coordinationRecovery.deliverCommitted();return c.json(result);}
     if(operation==="coordination-accept"){const b=await c.req.json();if(!svc.coordinationRecovery)throw new CoordinatorFenceError('coordination_unavailable','Service not wired');svc.coordinationRecovery.accept(actor,generation,b.rigId,b.packageKey,b.dispositionId,b.evidenceRef);return c.json({ok:true});}
     if(operation==="legacy-inventory"){const b=await c.req.json();return c.json(svc.legacyInventory(b.rigId,b.authorizationId));}
     if(operation==="migrate-legacy")return c.json(svc.migrateLegacy(actor,generation,await c.req.json()),201);
     if(operation==="enable")return c.json(svc.enable(actor,generation,await c.req.json()),201);
     if(operation==="transfer"){const b=await c.req.json();await svc.refreshRuntimeAvailability(b.expected?.rigId);return c.json(svc.transfer(actor,generation,b));}
     if(operation==="acknowledge"){
       const b=await c.req.json();if(b.token?.generation!==generation)throw new CoordinatorFenceError("coordinator_generation_mismatch","Token and immutable caller generation differ");
       return c.json(svc.acknowledge(actor,b.token,b));
     }
     if(operation==="reconciliation-recover")return c.json(svc.recoverReconciliation(actor,generation,await c.req.json()));
     if(operation==="renew"){
       const b=await c.req.json();if(b.token?.generation!==generation)throw new CoordinatorFenceError("coordinator_generation_mismatch","Token and caller generation differ");return c.json(svc.renew(actor,b.token,b.leaseMs,b.operationId));
     }
     if(operation==="admit"){
       const b=await c.req.json();svc.admit(actor,generation,b.rigId,b.packageKey,b.contract);return c.json({ok:true});
     }
     if(operation==="dispose"){
       const b=await c.req.json();svc.dispose(actor,generation,b.rigId,b.packageKey,b.dispositionId);await svc.runtimeOutcomeAssessment?.drain(b.rigId);return c.json({ok:true});
     }
     if(operation==="recover"){
       const b=await c.req.json();await svc.refreshRuntimeAvailability(b.rigId);return c.json(svc.recordOutage(actor,generation,b.rigId,b.evidenceId));
     }
     return c.json({error:"unknown_coordinator_operation"},404);
   } catch(err) {
     if(err instanceof CoordinatorFenceError)return c.json({error:err.code,message:err.message,...err.meta},409);
     if(err instanceof SyntaxError)return c.json({error:"invalid_json"},400);
     throw err;
   }
 });
 return app;
}
