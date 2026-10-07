import { afterEach, describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { createFullTestDb } from "./helpers/test-app.js";
import { seed, token } from "./helpers/coordinator-fixture.js";
import { EventBus } from "../src/domain/event-bus.js";
import { OutboxHandler } from "../src/domain/outbox-handler.js";
import { QueueRepository } from "../src/domain/queue-repository.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { isContainedExpiredAdministrativeHistory } from "../src/domain/expired-administrative-history.js";

const hash = (value: string): string => createHash("sha256").update(value).digest("hex");

describe("expired administrative history containment", () => {
  afterEach(() => vi.useRealTimers());

  async function scenario(retireEffect = true) {
    vi.useFakeTimers({ toFake: ["Date"] });
    const db = createFullTestDb();
    seed(db);
    const eventBus = new EventBus(db);
    const sessions = new SessionRegistry(db);
    const repo = new QueueRepository(db, eventBus, {
      resolveOccupantGeneration: (session) => sessions.currentOccupantGenerationForSession(session),
    });
    const outbox = new OutboxHandler(db);
    const guard = new SeatDeliveryGuard(db, (session) => resolveGuardTarget(db, session));
    repo.attachOutbox(outbox);

    await repo.create({ qitemId: "baton", sourceSession: "operator-agent@kernel", destinationSession: "lead@xv", body: "coordinate", nudge: false });
    repo.coordinatorAuthority.enable("operator-agent@kernel", "operator-agent-g1", {
      rigId: "xv", batonId: "baton", owner: "lead@xv", ownerGeneration: "lead-g1",
      coordinators: ["lead@xv", "peer@xv"], leaseMs: 60_000, operationId: "enable-test-rig",
    });
    repo.coordinatorAuthority.acknowledge("lead@xv", token, {
      operationId: "ack-test-rig", obligationsDigest: repo.coordinatorAuthority.reconciliationDigest("xv"),
    });

    const body = "immutable unknown effect body";
    const outboxId = "effect-for-expired-admin-history";
    const effect = outbox.record({ outboxId, senderSession: "builder@xv", destinationSession: "lead@xv", body, auditPointer: "source-history" });
    const now = Date.now();
    const bodySha256 = hash(body);
    const contract = (authorizationId: string) => ({
      authorizationId,
      deadline: now + (authorizationId === "auth-expired-unclaimed" ? 1_000 : 60_000),
      contract: {
        kind: "outbox-abandon-authorization" as const,
        outboxId,
        bodySha256,
        expectedState: "pending" as const,
        operationId: authorizationId === "auth-expired-unclaimed" ? "abandon-sibling-not-executed" : "abandon-actual-retirement",
        senderGeneration: "builder-g1",
        reason: "preserve uncertainty while withdrawing obsolete effect",
        evidenceRef: "fixture:original-effect-proof",
      },
    });
    const expired = contract("auth-expired-unclaimed");
    const actual = contract("auth-actual-retirement");
    await repo.issueOutboxAbandonAuthorization("operator-agent@kernel", "operator-agent-g1", expired, guard);
    // The first fixed administrative wake is known failed, so it cannot block
    // a second independently issued authorization for the same effect.
    db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?")
      .run(`wake-intent-outbox-abandon-notification:${expired.authorizationId}`);
    await repo.issueOutboxAbandonAuthorization("operator-agent@kernel", "operator-agent-g1", actual, guard);

    if (retireEffect) {
      repo.claim({ qitemId: actual.authorizationId, destinationSession: "builder@xv" });
      await outbox.abandonUncertain({
        outboxId,
        bodySha256,
        expectedState: "pending",
        operationId: actual.contract.operationId,
        authorizationId: actual.authorizationId,
        reason: actual.contract.reason,
        evidenceRef: actual.contract.evidenceRef,
        actor: "builder@xv",
        generation: "builder-g1",
      }, guard);
    }
    vi.setSystemTime(now + 2_000);
    return { db, repo, outbox, guard, authorizationId: expired.authorizationId, rigId: "xv", now: now + 2_000, effect };
  }

  it("contains only a byte-preserved expired unclaimed authorization after another exact authorization records UNKNOWN retirement", async () => {
    const s = await scenario();
    try {
      const before = {
        queues: s.db.prepare("SELECT * FROM queue_items WHERE qitem_id IN (?,?) ORDER BY qitem_id").all(s.authorizationId, "auth-actual-retirement"),
        outbox: s.db.prepare("SELECT * FROM outbox_entries WHERE outbox_id IN (?,?,?,?) ORDER BY outbox_id").all(
          "effect-for-expired-admin-history",
          `wake-intent-outbox-abandon-notification:${s.authorizationId}`,
          "wake-intent-outbox-abandon-notification:auth-actual-retirement",
          `wake-intent-outbox-abandon-continuation:${s.authorizationId}:consume-or-return`,
        ),
        authorizations: s.db.prepare("SELECT * FROM coordinator_operations WHERE kind='outbox-abandon-authorization' ORDER BY operation_id").all(),
        event: s.db.prepare("SELECT * FROM events WHERE type='outbox.uncertain_abandoned'").all(),
        transitions: s.db.prepare("SELECT * FROM queue_transitions WHERE qitem_id IN (?,?) ORDER BY transition_id").all(s.authorizationId, "auth-actual-retirement"),
        custody: s.db.prepare("SELECT * FROM queue_native_custody_evidence ORDER BY transition_id").all(),
      };
      expect(isContainedExpiredAdministrativeHistory(s.db, s.rigId, s.authorizationId, s.now)).toBe(true);
      const after = {
        queues: s.db.prepare("SELECT * FROM queue_items WHERE qitem_id IN (?,?) ORDER BY qitem_id").all(s.authorizationId, "auth-actual-retirement"),
        outbox: s.db.prepare("SELECT * FROM outbox_entries WHERE outbox_id IN (?,?,?,?) ORDER BY outbox_id").all(
          "effect-for-expired-admin-history",
          `wake-intent-outbox-abandon-notification:${s.authorizationId}`,
          "wake-intent-outbox-abandon-notification:auth-actual-retirement",
          `wake-intent-outbox-abandon-continuation:${s.authorizationId}:consume-or-return`,
        ),
        authorizations: s.db.prepare("SELECT * FROM coordinator_operations WHERE kind='outbox-abandon-authorization' ORDER BY operation_id").all(),
        event: s.db.prepare("SELECT * FROM events WHERE type='outbox.uncertain_abandoned'").all(),
        transitions: s.db.prepare("SELECT * FROM queue_transitions WHERE qitem_id IN (?,?) ORDER BY transition_id").all(s.authorizationId, "auth-actual-retirement"),
        custody: s.db.prepare("SELECT * FROM queue_native_custody_evidence ORDER BY transition_id").all(),
      };
      expect(after).toEqual(before);
    } finally {
      s.db.close();
    }
  });

  it.each([
    "unexpired",
    "claimed-now",
    "claimed-in-history",
    "malformed-contract",
    "receipt-mismatch",
    "unknown-not-preserved",
    "effect-still-live",
    "active-reservation",
  ] as const)("keeps %s authorization actionable", async (boundary) => {
    const s = await scenario(boundary !== "effect-still-live");
    try {
      const qid = s.authorizationId;
      if (boundary === "claimed-now") {
        s.db.prepare("UPDATE queue_items SET claimed_at=?,claimed_by_generation_uuid='builder-g1' WHERE qitem_id=?").run(new Date(s.now).toISOString(), qid);
      } else if (boundary === "claimed-in-history") {
        s.db.prepare("INSERT INTO queue_transitions(qitem_id,ts,state,transition_note,actor_session) VALUES (?,?,?,?,?)")
          .run(qid, new Date(s.now).toISOString(), "in-progress", "historical claim", "build-builder@app-handy-conveyor");
      } else if (boundary === "malformed-contract") {
        s.db.prepare("UPDATE queue_items SET body='{' WHERE qitem_id=?").run(qid);
      } else if (boundary === "receipt-mismatch") {
        s.db.prepare("UPDATE coordinator_operations SET receipt='{}' WHERE operation_id=? AND kind='outbox-abandon-authorization'")
          .run(`outbox-abandon-authorization:${qid}`);
      } else if (boundary === "unknown-not-preserved") {
        const event = s.db.prepare("SELECT seq,payload FROM events WHERE type='outbox.uncertain_abandoned'").get() as { seq: number; payload: string };
        const payload = JSON.parse(event.payload);
        payload.deliveryConclusion = "delivered";
        s.db.prepare("UPDATE events SET payload=? WHERE seq=?").run(JSON.stringify(payload), event.seq);
      } else if (boundary === "active-reservation") {
        s.db.prepare(`INSERT INTO seat_dispatch_reservations
          (reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,
           actor_session,actor_generation,request_hash,expected_json,frozen_snapshot,state,created_at,updated_at)
          VALUES ('reservation-test','reservation-test','builder@xv','builder@xv','builder-g1','native-1',
           'operator-agent@kernel','operator-agent-g1',?,?,?,'reserved',?,?)`)
          .run("a".repeat(64), "{}", "{}", new Date(s.now).toISOString(), new Date(s.now).toISOString());
      }

      const checkAt = boundary === "unexpired" ? s.now - 2_000 : s.now;
      expect(isContainedExpiredAdministrativeHistory(s.db, s.rigId, qid, checkAt)).toBe(false);
    } finally {
      s.db.close();
    }
  });
});
