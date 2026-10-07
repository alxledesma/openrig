import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { Hono } from "hono";
import { createDb } from "../src/db/connection.js";
import { EventBus } from "../src/domain/event-bus.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { InboxHandler } from "../src/domain/inbox-handler.js";
import { QueueTransitionLog } from "../src/domain/queue-transition-log.js";
import { archiveAgedTerminalTransitions } from "../src/domain/queue-retention.js";
import { queueRoutes } from "../src/routes/queue.js";
import { digest, legacyProposalDigest, type LegacyEnrollment } from "../src/domain/coordinator-authority-service.js";
import { HistoricalEffectDispositionService, historicalDigest, type HistoricalPlan } from "../src/domain/historical-effect-disposition.js";
import { seed, token } from "./helpers/coordinator-fixture.js";

let db: ReturnType<typeof createDb>, repo: QueueRepository, outbox: OutboxHandler, history: HistoricalEffectDispositionService, bus: EventBus;
const operator = "operator-agent@kernel", operatorGeneration = "operator-agent-g1";
const native = { actorGeneration: "builder-g1", identityProvenance: "transport:v1" };
const contract = { inputDigest: digest("inputs"), bodyHash: digest("valuable ongoing work"), destination: "builder@xv", resources: ["source/a"], returnContract: { destination: "lead@xv", evidenceRequired: ["tests"] } };
const service = () => repo.coordinatorAuthority;
const row = () => db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-old'").get() as Record<string, unknown>;
const contained = () => service().isAdoptedHistoryContained("xv", row());
const effects = () => db.prepare("SELECT * FROM outbox_entries ORDER BY outbox_id").all();
const receipts = () => ({ adoption: db.prepare("SELECT * FROM coordinator_held_history").all(), quarantine: db.prepare("SELECT * FROM outbox_historical_quarantines").all(), operations: db.prepare("SELECT * FROM outbox_historical_operations").all() });
const advance = () => vi.setSystemTime(Date.now() + 10);
const claim = () => { advance(); return repo.claim({ qitemId: "work", destinationSession: "builder@xv", ...native }); };
const finish = (state: "done" | "failed" | "canceled" = "done") => { advance(); return repo.update({ qitemId: "work", actorSession: "builder@xv", state, closureReason: state === "done" ? "no-follow-on" : undefined, ...native }); };

beforeEach(async () => {
  vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(new Date("2026-10-06T20:00:00.000Z"));
  db = createDb(); seed(db);
  db.prepare("INSERT INTO self_host_identity(singleton,host_id,minted_at,reconciled_at) VALUES (1,?,?,?)").run("local", new Date().toISOString(), new Date().toISOString());
  for (const session of ["lead@xv", "peer@xv", "builder@xv", "reviewer@xv", operator]) db.prepare("INSERT INTO bindings(id,node_id,tmux_session) VALUES (?,?,?)").run(session, session, session);
  bus = new EventBus(db); repo = new QueueRepository(db, bus, { resolveOccupantGeneration: session => service().generation(session) });
  outbox = new OutboxHandler(db); repo.attachOutbox(outbox); history = new HistoricalEffectDispositionService(db);
  await repo.create({ qitemId: "baton", sourceSession: operator, destinationSession: "lead@xv", body: "coordinate", nudge: false, identityProvenance: "transport:v1" });
  repo.claim({ qitemId: "baton", destinationSession: "lead@xv", actorGeneration: "lead-g1", identityProvenance: "transport:v1" });
  await repo.create({ qitemId: "work", sourceSession: "lead@xv", destinationSession: "builder@xv", body: "valuable ongoing work", nudge: false, identityProvenance: "transport:v1" });
});
afterEach(() => { db.close(); vi.useRealTimers(); });

// The same public quarantine and finite native Operator recovery contract as the retained fixture.
async function adopt(claimBefore = false, legacyClaim = false) {
  if (claimBefore) { advance(); repo.claim({ qitemId: "work", destinationSession: "builder@xv", ...(legacyClaim ? { identityProvenance: "transport:v1" } : native) }); }
  outbox.record({ outboxId: "wake-intent-old", senderSession: "lead@xv", destinationSession: "builder@xv", body: "UNKNOWN prior input", auditPointer: "work" });
  outbox.markIndeterminate("wake-intent-old");
  const plan: HistoricalPlan = { rigId: "xv", leadBatonId: "baton", leadGeneration: "lead-g1", operatorGeneration, operationId: "hold", authorizationId: "hold-auth", expiresAt: Date.now() + 600000, effects: history.inspect("xv", ["wake-intent-old"]) };
  await repo.create({ qitemId: plan.authorizationId, sourceSession: "lead@xv", destinationSession: operator, body: JSON.stringify({ kind: "outbox-historical-quarantine-authorization", requestDigest: historicalDigest({ actor: operator, generation: operatorGeneration, input: plan }) }), nudge: false, identityProvenance: "transport:v1" });
  repo.claim({ qitemId: plan.authorizationId, destinationSession: operator, actorGeneration: operatorGeneration, identityProvenance: "transport:v1" });
  history.quarantine(operator, operatorGeneration, plan);
  const inv = service().legacyInventory("xv", "authorization", true), deadline = Date.now() + 600000;
  await repo.create({ qitemId: "recovery", sourceSession: "lead@xv", destinationSession: operator, body: JSON.stringify({ kind: "coordinator-held-history-recovery.v1", rigId: "xv", operationId: "migrate", owner: operator, generation: operatorGeneration, lead: "lead@xv", leadGeneration: "lead-g1", effects: inv.heldHistory!.map(h => h.outboxId), action: "reconcile-preserved-unknown-history", deadline, returnPath: { session: "lead@xv", queueId: "recovery" } }), expiresAt: new Date(deadline).toISOString(), nudge: false, identityProvenance: "transport:v1" });
  repo.claim({ qitemId: "recovery", destinationSession: operator, actorGeneration: operatorGeneration, identityProvenance: "transport:v1" });
  const inventory = service().legacyInventory("xv", "authorization", true);
  const input: LegacyEnrollment = { rigId: "xv", batonId: "baton", owner: "lead@xv", ownerGeneration: "lead-g1", coordinators: ["lead@xv", "peer@xv"], leaseMs: 60000, operationId: "migrate", authorizationId: "authorization", inventory, heldHistoryRecovery: { queueId: "recovery", rowHash: historicalDigest(db.prepare("SELECT * FROM queue_items WHERE qitem_id='recovery'").get()) }, obligations: inventory.rows.map(q => q.queueId === "work" ? { queueId: "work", kind: "work", evidenceRef: "exact-current-work-contract", packageKey: "p1", resourceScope: "exclusive", contract } : { queueId: q.queueId, kind: "coordination", evidenceRef: "actual-control-return" }) };
  await repo.create({ qitemId: "authorization", sourceSession: "lead@xv", destinationSession: operator, body: JSON.stringify({ kind: "coordinator-legacy-enrollment", proposalDigest: legacyProposalDigest(input) }), identityProvenance: "transport:v1", nudge: false });
  repo.claim({ qitemId: "authorization", destinationSession: operator, actorGeneration: operatorGeneration, identityProvenance: "transport:v1" });
  service().migrateLegacy(operator, operatorGeneration, input);
  service().acknowledge("lead@xv", token, { operationId: "ack", obligationsDigest: service().reconciliationDigest("xv") });
  expect(contained()).toBe(true);
}
async function dispose() {
  await repo.create({ qitemId: "returned", sourceSession: "builder@xv", destinationSession: "lead@xv", body: JSON.stringify({ packageKey: "p1", inputDigest: contract.inputDigest, evidence: [{ kind: "tests", ref: "bounded/tests.txt" }] }), nudge: false, identityProvenance: "transport:v1" });
  service().dispose("builder@xv", "builder-g1", "xv", "p1", "returned");
}

it.each([false, true])("preserves adopted UNKNOWN through native claim, completion and exact dispose (preclaimed=%s)", async preclaimed => {
  await adopt(preclaimed); const unknown = effects(), historical = receipts(), authority = service().get("xv");
  if (!preclaimed) claim(); expect(contained()).toBe(true);
  advance(); repo.update({ qitemId: "work", actorSession: "builder@xv", transitionNote: "native progress note", ...native });
  finish(); expect(contained()).toBe(true);
  await dispose(); expect(contained()).toBe(true);
  expect(effects()).toEqual(unknown); expect(receipts()).toEqual(historical); expect(service().get("xv")).toEqual(authority);
  expect(outbox.getById("wake-intent-old")?.deliveryState).toBe("indeterminate"); expect(outbox.claimForDelivery("wake-intent-old")).toBe(false);
  expect(db.prepare("SELECT disposition_id FROM coordinator_assignments WHERE package_key='p1'").get()).toEqual({ disposition_id: "returned" });
  expect(db.prepare("SELECT * FROM coordinator_resources WHERE package_key='p1'").all()).toEqual([]);
  expect(db.prepare("SELECT * FROM coordinator_operations WHERE kind LIKE '%accept%'").all()).toEqual([]);
  // Historical proof survives a later occupant change; it grants that occupant no authority.
  db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
  expect(contained()).toBe(true); expect(() => service().dispose("builder@xv", "builder-g2", "xv", "p1", "replacement-return")).toThrow();
});

it.each(["failed", "canceled"] as const)("native %s is an outcome, not delivery or acceptance", async state => {
  await adopt(); claim(); const before = effects(); finish(state); expect(contained()).toBe(true); expect(effects()).toEqual(before);
});

it("preserves the same proof when the retention consumer moves transitions into the archive", async () => {
  await adopt(); claim(); finish(); await dispose(); const evidence = db.prepare("SELECT * FROM queue_native_custody_evidence").all();
  const archived = archiveAgedTerminalTransitions(db, { nowIso: new Date(Date.now() + 40 * 86400000).toISOString(), transitionsRetentionDays: 30 });
  expect(archived.archivedQitems).toBe(1); expect(db.prepare("SELECT * FROM queue_transitions WHERE qitem_id='work'").all()).toEqual([]);
  expect(db.prepare("SELECT * FROM queue_native_custody_evidence").all()).toEqual(evidence); expect(contained()).toBe(true);
  expect(new QueueTransitionLog(db).latestForQitem("work")?.state).toBe("done");
});

it.each(["non-native", "missing-generation", "other-actor", "replacement"])("holds %s custody progress", async failure => {
  await adopt(); claim(); advance();
  let actorSession = "builder@xv", actorGeneration: string | undefined = "builder-g1", identityProvenance = "transport:v1";
  if (failure === "non-native") identityProvenance = "claimed:v1";
  if (failure === "missing-generation") actorGeneration = undefined;
  if (failure === "other-actor") { actorSession = "reviewer@xv"; actorGeneration = "reviewer-g1"; }
  if (failure === "replacement") { db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run(); actorGeneration = "builder-g2"; }
  repo.update({ qitemId: "work", actorSession, actorGeneration, identityProvenance, state: "done", closureReason: "no-follow-on" });
  expect(contained()).toBe(false);
});

it("does not backfill proof for legacy claims from native session provenance", async () => {
  await adopt(true, true); expect(db.prepare("SELECT * FROM coordinator_held_history_outcome_bases").all()).toEqual([]);
  finish(); expect(contained()).toBe(false);
});

it("holds unverifiable existing adoption receipts even when later transitions are proven", async () => {
  db.exec("DROP TABLE coordinator_held_history_outcome_bases"); await adopt();
  // Reintroducing an empty proof table models an upgraded database with old adoption receipts.
  db.exec("CREATE TABLE coordinator_held_history_outcome_bases(rig_id TEXT,outbox_id TEXT,receipt TEXT,PRIMARY KEY(rig_id,outbox_id))");
  claim(); finish(); expect(contained()).toBe(false);
});

it.each(["body", "destination", "claim-generation", "resource", "assignment", "unreceipted-dispose"])("holds unexplained %s drift", async drift => {
  await adopt(); claim(); finish();
  if (drift === "body") db.prepare("UPDATE queue_items SET body='changed' WHERE qitem_id='work'").run();
  if (drift === "destination") db.prepare("UPDATE queue_items SET destination_session='reviewer@xv' WHERE qitem_id='work'").run();
  if (drift === "claim-generation") db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='builder-g2' WHERE qitem_id='work'").run();
  if (drift === "resource") db.prepare("DELETE FROM coordinator_resources WHERE package_key='p1'").run();
  if (drift === "assignment") db.prepare("UPDATE coordinator_assignments SET epoch=2 WHERE package_key='p1'").run();
  if (drift === "unreceipted-dispose") { db.prepare("UPDATE coordinator_assignments SET disposition_id='forged' WHERE package_key='p1'").run(); db.prepare("DELETE FROM coordinator_resources WHERE package_key='p1'").run(); }
  expect(contained()).toBe(false);
});

it("a native reopen cycle cannot restore forward-only containment", async () => {
  await adopt(); claim(); finish(); advance();
  repo.update({ qitemId: "work", actorSession: "builder@xv", state: "in-progress", reopen: true, transitionNote: "explicit native reopen", ...native });
  finish(); expect(contained()).toBe(false);
});

it("unexplained intermediate queue writes cannot be laundered by a later native terminal snapshot", async () => {
  await adopt(); claim(); db.prepare("UPDATE queue_items SET body='changed' WHERE qitem_id='work'").run(); finish();
  expect(contained()).toBe(false);
});

it("generation mismatch and evidence insert failure roll back the entire native mutation", async () => {
  const before = db.prepare("SELECT * FROM queue_items WHERE qitem_id='work'").get(), transitions = repo.transitionLog.listForQitem("work");
  expect(() => repo.claim({ qitemId: "work", destinationSession: "builder@xv", identityProvenance: "transport:v1", actorGeneration: "retired" })).toThrow("actual current actor generation");
  expect(db.prepare("SELECT * FROM queue_items WHERE qitem_id='work'").get()).toEqual(before);
  db.exec("CREATE TRIGGER reject_test_custody BEFORE INSERT ON queue_native_custody_evidence BEGIN SELECT RAISE(ABORT,'test-evidence-failure'); END;");
  expect(claim).toThrow("test-evidence-failure"); expect(db.prepare("SELECT * FROM queue_items WHERE qitem_id='work'").get()).toEqual(before);
  expect(repo.transitionLog.listForQitem("work")).toEqual(transitions);
  expect(db.prepare("SELECT * FROM queue_native_custody_evidence WHERE qitem_id='work'").all()).toEqual([]);
});

it("evidence and adoption generation boundaries are insert-only", async () => {
  await adopt(); claim();
  expect(() => db.exec("UPDATE queue_native_custody_evidence SET receipt='{}' WHERE qitem_id='work'")).toThrow("immutable");
  expect(() => db.exec("DELETE FROM queue_native_custody_evidence WHERE qitem_id='work'")).toThrow("immutable");
  expect(() => db.exec("UPDATE coordinator_held_history_outcome_bases SET receipt='{}'")).toThrow("immutable");
  expect(() => db.exec("DELETE FROM coordinator_held_history_outcome_bases")).toThrow("immutable");
});

it("producer uses transport identity and checked generation, never body actor/generation labels", async () => {
  const app = new Hono();
  app.use("*", async (c, next) => { c.set("queueRepo" as never, repo); c.set("eventBus" as never, bus); c.set("outboxHandler" as never, outbox); c.set("inboxHandler" as never, new InboxHandler(db, bus, repo)); await next(); });
  app.route("/api/queue", queueRoutes());
  const response = await app.request("/api/queue/work/claim", { method: "POST", headers: { "Content-Type": "application/json", "X-OpenRig-Session": "builder@xv", "X-OpenRig-Occupant-Generation": "builder-g1" }, body: JSON.stringify({ destinationSession: "reviewer@xv", actorGeneration: "reviewer-g1" }) });
  expect(response.status).toBe(200);
  const recorded = JSON.parse((db.prepare("SELECT receipt FROM queue_native_custody_evidence WHERE qitem_id='work'").get() as { receipt: string }).receipt);
  expect(recorded).toMatchObject({ actorGeneration: "builder-g1", transition: { actorSession: "builder@xv", identityProvenance: "transport:v1" } });
});


it("current-epoch adoption binds the same generation proof without resetting authority or granting acceptance", async () => {
  await adopt();
  const next = { ...contract, resources: ["source/b"] };
  service().admit(operator, operatorGeneration, "xv", "p2", next);
  await repo.create({ qitemId: "current-work", sourceSession: "lead@xv", destinationSession: "builder@xv", body: "valuable ongoing work", dispatch: { token, packageKey: "p2" }, nudge: false, identityProvenance: "transport:v1" });
  outbox.record({ outboxId: "wake-intent-current", senderSession: "lead@xv", destinationSession: "builder@xv", body: "UNKNOWN current input", auditPointer: "current-work" });
  outbox.markIndeterminate("wake-intent-current");
  const hp: HistoricalPlan = { rigId: "xv", leadBatonId: "baton", leadGeneration: "lead-g1", operatorGeneration, operationId: "current-hold", authorizationId: "current-hold-auth", expiresAt: Date.now() + 600000, effects: history.inspect("xv", ["wake-intent-current"]) };
  await repo.create({ qitemId: hp.authorizationId, sourceSession: "lead@xv", destinationSession: operator, body: JSON.stringify({ kind: "outbox-historical-quarantine-authorization", requestDigest: historicalDigest({ actor: operator, generation: operatorGeneration, input: hp }) }), nudge: false, identityProvenance: "transport:v1" });
  repo.claim({ qitemId: hp.authorizationId, destinationSession: operator, actorGeneration: operatorGeneration, identityProvenance: "transport:v1" }); history.quarantine(operator, operatorGeneration, hp);
  const effectsToAdopt = service().legacyInventory("xv", "none", true).heldHistory!.filter(h => h.outboxId === "wake-intent-current"), deadline = Date.now() + 600000;
  await repo.create({ qitemId: "current-recovery", sourceSession: "lead@xv", destinationSession: operator, body: JSON.stringify({ kind: "coordinator-held-history-recovery.v1", rigId: "xv", operationId: "adopt-current", owner: operator, generation: operatorGeneration, lead: "lead@xv", leadGeneration: "lead-g1", effects: effectsToAdopt.map(h => h.outboxId), action: "reconcile-preserved-unknown-history", deadline, returnPath: { session: "lead@xv", queueId: "current-recovery" } }), expiresAt: new Date(deadline).toISOString(), nudge: false, identityProvenance: "transport:v1" });
  repo.claim({ qitemId: "current-recovery", destinationSession: operator, actorGeneration: operatorGeneration, identityProvenance: "transport:v1" });
  const input = { rigId: "xv", operationId: "adopt-current", expected: token, effects: effectsToAdopt, recovery: { queueId: "current-recovery", rowHash: historicalDigest(db.prepare("SELECT * FROM queue_items WHERE qitem_id='current-recovery'").get()) } };
  const authority = service().get("xv"), unknown = effects();
  const adopted = service().adoptHeldHistory(operator, operatorGeneration, input);
  expect(service().adoptHeldHistory(operator, operatorGeneration, input)).toEqual(adopted);
  advance(); repo.claim({ qitemId: "current-work", destinationSession: "builder@xv", ...native });
  advance(); repo.update({ qitemId: "current-work", actorSession: "builder@xv", state: "done", closureReason: "no-follow-on", ...native });
  const current = db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='wake-intent-current'").get() as Record<string, unknown>;
  expect(service().isAdoptedHistoryContained("xv", current)).toBe(true);
  expect(service().isAdoptedHistoryContained("other", current)).toBe(false);
  expect(effects()).toEqual(unknown); expect(service().get("xv")).toEqual(authority);
  outbox.record({ outboxId: "unrelated-unknown", senderSession: "builder@xv", destinationSession: "lead@xv", body: "independent unresolved effect" });
  const unrelated = db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='unrelated-unknown'").get() as Record<string, unknown>;
  expect(service().isAdoptedHistoryContained("xv", unrelated)).toBe(false);
});

it.each(["handoff", "handoff-and-complete", "cross-host"])("records the %s source-close producer atomically with its native generation", async kind => {
  // Unmanaged rig avoids creating any extra dispatch/admission contract for this producer test.
  await repo.create({ qitemId: "handoff-source", sourceSession: "lead@xv", destinationSession: "builder@xv", body: "forwardable work", nudge: false });
  repo.claim({ qitemId: "handoff-source", destinationSession: "builder@xv", ...native });
  const input = { qitemId: "handoff-source", fromSession: "builder@xv", toSession: "reviewer@xv", nudge: false, ...native };
  if (kind === "handoff") await repo.handoff(input);
  if (kind === "handoff-and-complete") await repo.handoffAndComplete(input);
  if (kind === "cross-host") repo.closeCrossHostHandoffSource({ ...input, terminalState: "handed-off", closureTarget: "successor@remote" });
  const transition = repo.transitionLog.latestForQitem("handoff-source")!;
  const recorded = JSON.parse((db.prepare("SELECT receipt FROM queue_native_custody_evidence WHERE transition_id=?").get(transition.transitionId) as { receipt: string }).receipt);
  expect(recorded).toMatchObject({ actorGeneration: "builder-g1", beforeQueue: { state: "in-progress" }, afterQueue: { state: kind === "handoff-and-complete" ? "done" : "handed-off", claimed_by_generation_uuid: "builder-g1" } });
  expect(recorded.transition).toEqual(transition);
});


it.each(["handoff", "handoff-and-complete"])("preserves adoption through a genuine %s return and exact worker disposal", async kind => {
  await adopt(); claim(); const before = effects();
  const input = { qitemId: "work", fromSession: "builder@xv", toSession: "lead@xv", body: JSON.stringify({ packageKey: "p1", inputDigest: contract.inputDigest, evidence: [{ kind: "tests", ref: "bounded/handoff-tests.txt" }] }), nudge: false, ...native };
  advance(); const result = kind === "handoff" ? await repo.handoff(input) : await repo.handoffAndComplete(input);
  expect(contained()).toBe(true);
  service().dispose("builder@xv", "builder-g1", "xv", "p1", result.created.qitemId);
  expect(contained()).toBe(true); expect(effects()).toEqual(before);
});

it("an evidence-write failure rolls back native update and close/create handoff interactions", async () => {
  claim();
  const before = db.prepare("SELECT * FROM queue_items ORDER BY qitem_id").all(), transitions = repo.transitionLog.listForQitem("work"), events = db.prepare("SELECT * FROM events ORDER BY rowid").all();
  db.exec("CREATE TRIGGER reject_test_custody BEFORE INSERT ON queue_native_custody_evidence BEGIN SELECT RAISE(ABORT,'test-evidence-failure'); END;");
  expect(() => finish()).toThrow("test-evidence-failure");
  await expect(repo.handoff({ qitemId: "work", fromSession: "builder@xv", toSession: "lead@xv", nudge: false, ...native })).rejects.toThrow("test-evidence-failure");
  expect(db.prepare("SELECT * FROM queue_items ORDER BY qitem_id").all()).toEqual(before);
  expect(repo.transitionLog.listForQitem("work")).toEqual(transitions);
  expect(db.prepare("SELECT * FROM events ORDER BY rowid").all()).toEqual(events);
});

it("a bounded proof refuses an over-budget chain rather than accepting a truncated prefix", async () => {
  await adopt(); claim();
  // Use the shared append producer in one transaction; the limit is on proof history, not events.
  const snapshot = db.prepare("SELECT * FROM queue_items WHERE qitem_id='work'").get() as Record<string, unknown>;
  expect(service().generation("builder@xv")).toBe(native.actorGeneration);
  db.transaction(() => {
    for (let i = 0; i < 2000; i++) repo.transitionLog.appendNativeCustody({ qitemId: "work", state: "in-progress", actorSession: "builder@xv", transitionNote: "native same-state note " + i, identityProvenance: "transport:v1" }, native.actorGeneration, snapshot);
  })();
  finish(); expect(contained()).toBe(false);
});


it("changed native transition provenance invalidates the immutable outcome chain", async () => {
  await adopt(); claim(); finish();
  db.prepare("UPDATE queue_transitions SET identity_provenance=NULL WHERE qitem_id='work' AND transition_note='claimed'").run();
  expect(contained()).toBe(false);
});
