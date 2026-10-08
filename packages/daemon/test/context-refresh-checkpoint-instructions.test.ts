import { afterEach, expect, it } from "vitest";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { contextRefreshSchema } from "../src/db/migrations/108_context_refresh.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { ContextRefreshService, contextRefreshDigest } from "../src/domain/context-refresh-service.js";
import { DEFAULT_CONTEXT_REFRESH_POLICY, type ContextRefreshActor, type ContextRefreshGrant, type ContextRefreshObservation, type ContextRefreshTarget } from "../src/domain/context-refresh-contract.js";
import { contextRefreshCheckpointInstructions } from "../src/domain/context-refresh-checkpoint-instructions.js";

const operator: ContextRefreshActor = { session: "operator-agent@kernel", generation: "operator-g1" };
const reviewer: ContextRefreshActor = { session: "reviewer@rig", generation: "reviewer-g1" };
const target: ContextRefreshTarget = { nodeId: "peer@rig", sessionName: "peer@rig", generation: "peer-native-g1",
  runtime: "codex", nativeId: "peer-native-thread", configurationDigest: "a".repeat(64) };
let db: ReturnType<typeof createDb> | undefined;
afterEach(() => { db?.close(); db = undefined; });

it("generates actionable checkpoint IDs, submission and receipt-reconciliation instructions", async () => {
  db = createDb(); seed(db); migrate(db, [contextRefreshSchema]);
  const now = Date.UTC(2026, 9, 7, 20);
  const executorTarget: ContextRefreshTarget = { ...target, nodeId: "operator-node", sessionName: operator.session,
    generation: operator.generation, nativeId: "operator-native", configurationDigest: "b".repeat(64) };
  const observation: ContextRefreshObservation = {
    identity: target, observedAt: now, capability: "codex-reserved-fresh",
    native: { verified: true, observedAt: now, launchId: "operator-launch", fingerprint: "kernel-observed" },
    activity: { value: "idle", observedAt: now }, usage: { usedPercent: 75, observedAt: now, source: "token-count", cursor: "cursor-1" },
    compactions: { count: 0, observedAt: now, source: "native-compactions", cursor: "cursor-0" }, holds: [],
  };
  const grant: ContextRefreshGrant = { grantId: "guidance-grant", kind: "context-refresh",
    executor: { ...operator, nodeId: executorTarget.nodeId, launchId: "operator-launch", configurationDigest: executorTarget.configurationDigest },
    targets: [target], policy: { ...DEFAULT_CONTEXT_REFRESH_POLICY }, policyRevision: "policy-r1", validUntil: now + 60_000,
    validator: reviewer, recoveryOwner: operator };
  const service = new ContextRefreshService({ db, now: () => now,
    assertCurrentOperator: () => {}, assertCurrentActor: () => {}, assertExecutor: () => {},
    currentTarget: nodeId => nodeId === executorTarget.nodeId ? executorTarget : nodeId === target.nodeId ? target : null,
    observe: async () => observation, checkpoint: () => null, checkpointReceipt: () => null,
    reservationEvidence: () => null, assertCutoverReady: () => {},
  });
  service.grant(operator, grant);
  const request = await service.prepareCheckpointRequest(operator, grant.grantId, target.nodeId, "prepare-operation-1");
  const formatted = contextRefreshCheckpointInstructions({ grantId: grant.grantId, nodeId: target.nodeId, generation: target.generation });
  expect(request.qitemId).toBe(formatted.qitemId);
  expect(request.qitemId).toBe("context-refresh-" + contextRefreshDigest([grant.grantId, target.nodeId, target.generation]));
  expect(request.body).toBe(formatted.body);
  expect(request.body).toContain(request.qitemId);
  expect(request.body).toContain("grant " + grant.grantId + ", node " + target.nodeId + ", generation " + target.generation);
  expect(request.body).toContain("POST \u0024{OPENRIG_URL}/api/context-refresh/checkpoint");
  expect(request.body).toContain("\"grantId\":\"" + grant.grantId + "\",\"nodeId\":\"" + target.nodeId + "\",\"packet\":{...}");
  expect(request.body).toContain("current_work, decisions, memory, constraints, standing_duties, evidence, next_action, outstanding_effects");
  expect(request.body).toContain("only after actually reconciling all effects");
  expect(request.body).toContain("\"phase\":\"submitted\"");
  expect(request.body).toContain("do not submit again");
  expect(request.body).toContain("GET \u0024{OPENRIG_URL}/api/context-refresh/status?grantId=\u0024{encodeURIComponent(grantId)}&nodeId=\u0024{encodeURIComponent(nodeId)}");
  expect(request.body).toContain("rig queue show " + request.qitemId + " --full --json");
  expect(request.body).toContain("rig queue update " + request.qitemId + " --state done --closure-reason no-follow-on");
  expect(request.body).toContain("not rotation approval or cutover authority");
});
