import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
import { coordinatorRoutes } from "../../daemon/src/routes/coordinator.js";
import { CoordinatorFenceError } from "../../daemon/src/domain/coordinator-authority-service.js";
import { coordinatorCommand } from "../src/commands/coordinator.js";

const actor = "operator-agent@kernel";
const generation = "native-operator-generation";
const token = "qualification-cli-test-token";
const rigId = "01M4A6883B0J2NZZ22XVH7P4QB";
let home: string;
let calls: Array<{ operation: string; actor: string; generation: string; body: Record<string, unknown> }>;
let recovery: Record<string, (...args: any[]) => unknown>;
let app: Hono;

beforeEach(() => {
  home = fs.mkdtempSync(path.join(os.tmpdir(), "qualification-cli-test-"));
  vi.stubEnv("OPENRIG_HOME", home);
  vi.stubEnv("OPENRIG_URL", "http://qualification-cli.invalid");
  vi.stubEnv("OPENRIG_SESSION_NAME", actor);
  vi.stubEnv("OPENRIG_OCCUPANT_GENERATION", generation);
  vi.stubEnv("OPENRIG_TERMINAL_BEARER_TOKEN", "");
  fs.writeFileSync(path.join(home, "terminal-token"), token, { mode: 0o600 });
  calls = [];
  recovery = {
    deliverCommitted: vi.fn(async () => {}),
    stageQualificationAssessment: vi.fn((a: string, g: string, body: Record<string, unknown>) => {
      calls.push({ operation: "qualification-assessment-stage", actor: a, generation: g, body });
      return { queueId: "assessment-duty-1", contractDigest: "a".repeat(64), deadline: body.deadline };
    }),
    stageQualificationAssessmentRetirement: vi.fn((a: string, g: string, body: Record<string, unknown>) => {
      calls.push({ operation: "qualification-assessment-retirement-stage", actor: a, generation: g, body });
      return { queueId: "retirement-duty-1", targetQueueId: body.targetQueueId, deadline: body.deadline };
    }),
    recordQualificationAssessmentUncertainty: vi.fn((a: string, g: string, body: Record<string, unknown>) => {
      calls.push({ operation: "qualification-assessment-uncertainty-dispose", actor: a, generation: g, body });
      return { operationId: "uncertainty-1", outcome: "unknown-preserved", wakeReplayed: false, custodyTransferred: false };
    }),
    recordQualificationAssessmentReturn: vi.fn((a: string, g: string, body: Record<string, unknown>) => {
      calls.push({ operation: "qualification-assessment-return", actor: a, generation: g, body });
      return undefined;
    }),
    assessQualificationDuty: vi.fn((a: string, g: string, body: Record<string, unknown>) => {
      calls.push({ operation: "qualification-assessment-review", actor: a, generation: g, body });
      return { reviewId: "review-1", grantsAuthority: false };
    }),
  };
  app = new Hono();
  app.use("*", async (c, next) => {
    c.set("queueRepo" as never, { coordinatorAuthority: { coordinationRecovery: recovery } } as never);
    await next();
  });
  app.route("/api/coordinator", coordinatorRoutes({ bearerToken: token }));
  vi.stubGlobal("fetch", vi.fn((input: string | URL | Request, init?: RequestInit) => app.request(input, init)));
  vi.spyOn(console, "log").mockImplementation(() => {});
  process.exitCode = undefined;
});

afterEach(() => {
  process.exitCode = undefined;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  fs.rmSync(home, { recursive: true, force: true });
});

function contractFile(body: Record<string, unknown>): string {
  const file = path.join(home, "contract.json");
  fs.writeFileSync(file, JSON.stringify(body));
  return file;
}

function validFor(operation: string): Record<string, unknown> {
  const deadline = Date.now() + 60_000;
  if (operation === "qualification-assessment-stage") return {
    rigId,
    worker: "queue-worker@kernel",
    workerGeneration: "worker-generation",
    configurationDigest: "c".repeat(64),
    deadline,
    contract: {
      schema: "qualification-assessment-contract.v1",
      artifactRef: "proof-results/qualification-worker.json",
      artifactSha256: `sha256:${"a".repeat(64)}`,
      taskDigest: `sha256:${"b".repeat(64)}`,
      scope: "qualification-only",
      productAuthority: false,
    },
  };
  if (operation === "qualification-assessment-retirement-stage") return {
    rigId,
    targetQueueId: "assessment-old",
    targetBodyHash: "a".repeat(64),
    sweepFindingQueueId: "sweep-old",
    sweepFindingBodyHash: "b".repeat(64),
    deadline,
  };
  if (operation === "qualification-assessment-uncertainty-dispose") return {
    rigId,
    rows: [{ targetQueueId: "assessment-old", targetBodyHash: "a".repeat(64), sweepFindingQueueId: "sweep-old", sweepFindingBodyHash: "b".repeat(64) }],
    deadline,
  };
  if (operation === "qualification-assessment-return") return {
    rigId,
    dutyQueueId: "assessment-duty-1",
    returnQueueId: "worker-return-1",
  };
  if (operation === "qualification-assessment-review") return {
    rigId,
    dutyQueueId: "assessment-duty-1",
    finding: "inconclusive",
    evidenceRef: "proof-results/qualification-review.json",
  };
  throw new Error(`unsupported test operation ${operation}`);
}

describe("qualification assessment coordinator CLI verbs", () => {
  const operations = [
    "qualification-assessment-stage",
    "qualification-assessment-retirement-stage",
    "qualification-assessment-uncertainty-dispose",
    "qualification-assessment-return",
    "qualification-assessment-review",
  ];

  it.each(operations)("%s uses the existing authenticated native transport", async operation => {
    const body = validFor(operation);
    const file = contractFile(body);
    await coordinatorCommand().parseAsync(["node", "rig", operation, file]);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.actor).toMatch(/^operator-agent@kernel@.+/);
    expect(calls[0]).toMatchObject({ operation, generation, body });
    expect(process.exitCode).toBeUndefined();
    expect(recovery.deliverCommitted).toHaveBeenCalledTimes(operation.endsWith("stage") ? 1 : 0);
  });

  it.each([
    ["qualification-assessment-stage", { ...validFor("qualification-assessment-stage"), contract: { schema: "qualification-assessment-contract.v1", scope: "product", productAuthority: true } }],
    ["qualification-assessment-stage", { ...validFor("qualification-assessment-stage"), deadline: Date.now() + 1_300_000 }],
    ["qualification-assessment-stage", { ...validFor("qualification-assessment-stage"), configurationDigest: ["c".repeat(64)] }],
    ["qualification-assessment-retirement-stage", { ...validFor("qualification-assessment-retirement-stage"), targetBodyHash: "sending" }],
    ["qualification-assessment-uncertainty-dispose", { ...validFor("qualification-assessment-uncertainty-dispose"), rows: [{ targetQueueId: "x", targetBodyHash: "indeterminate", sweepFindingQueueId: "y", sweepFindingBodyHash: "b".repeat(64) }] }],
    ["qualification-assessment-uncertainty-dispose", { ...validFor("qualification-assessment-uncertainty-dispose"), rows: [] }],
    ["qualification-assessment-return", { ...validFor("qualification-assessment-return"), generation: "caller-supplied" }],
    ["qualification-assessment-review", { ...validFor("qualification-assessment-review"), finding: "pass" }],
    ["qualification-assessment-review", { ...validFor("qualification-assessment-review"), finding: ["inconclusive"] }],
  ])("rejects malformed %s contract before any request", async (operation, body) => {
    const file = contractFile(body as Record<string, unknown>);
    await coordinatorCommand().parseAsync(["node", "rig", operation as string, file]);
    expect(calls).toHaveLength(0);
    expect(recovery.deliverCommitted).not.toHaveBeenCalled();
    expect(process.exitCode).toBe(1);
  });

  it("preserves an exact backend UNKNOWN refusal and sends only once", async () => {
    recovery.stageQualificationAssessmentRetirement = vi.fn(() => {
      throw new CoordinatorFenceError("qualification_retirement_effect_unknown", "Sending or unknown wake effect is protected");
    });
    const file = contractFile(validFor("qualification-assessment-retirement-stage"));
    await coordinatorCommand().parseAsync(["node", "rig", "qualification-assessment-retirement-stage", file]);
    expect(recovery.stageQualificationAssessmentRetirement).toHaveBeenCalledTimes(1);
    expect(process.exitCode).toBe(1);
    expect(console.log).toHaveBeenCalledWith(expect.stringContaining("qualification_retirement_effect_unknown"));
  });
});
