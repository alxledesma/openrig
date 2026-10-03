import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SeatLifecycleService } from "../src/domain/seat-lifecycle-service.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";

const PROFILE = `model = "gpt-6.1-sol"
model_provider = "openai"
model_reasoning_effort = "low"
approval_policy = "never"
sandbox_mode = "danger-full-access"
`;

describe("set-codex-profile", () => {
  let db: Database.Database;
  let repo: RigRepository;
  let sessions: SessionRegistry;
  let bus: EventBus;
  let home: string;
  let probe: ReturnType<typeof vi.fn>;
  let service: SeatLifecycleService;
  let guard: SeatDeliveryGuard;

  beforeEach(() => {
    db = createFullTestDb();
    db.exec("CREATE TABLE seat_delivery_guards (node_id TEXT PRIMARY KEY, desired INTEGER NOT NULL, effective INTEGER NOT NULL)");
    repo = new RigRepository(db);
    sessions = new SessionRegistry(db);
    bus = new EventBus(db);
    home = mkdtempSync(join(tmpdir(), "openrig-codex-profile-"));
    writeFileSync(join(home, "config.toml"), 'sandbox_mode = "danger-full-access"\n');
    probe = vi.fn(async () => undefined);
    guard = new SeatDeliveryGuard(db, name => resolveGuardTarget(db, name));
    service = new SeatLifecycleService({
      db, rigRepo: repo, sessionRegistry: sessions, eventBus: bus,
      tmuxAdapter: { deliveryGuard: guard } as TmuxAdapter, codexProfileHome: home, codexProfileProbe: probe,
    });
  });

  afterEach(() => { db.close(); rmSync(home, { recursive: true, force: true }); });

  function seat(runtime = "codex") {
    if (runtime === "codex") writeFileSync(join(home, "old-profile.config.toml"), PROFILE.replace("gpt-6.1-sol", "gpt-6-luna").replace('model_provider = "openai"\n', "").replace('sandbox_mode = "danger-full-access"\n', ""));
    const rig = repo.createRig("test-rig");
    const node = repo.addNode(rig.id, "dev.qa", {
      runtime, model: "gpt-6.1-sol", codexConfigProfile: "old-profile", cwd: "/project",
    });
    const session = sessions.registerSession(node.id, "dev-qa@test-rig");
    sessions.updateStatus(session.id, "running");
    sessions.updateResumeToken(session.id, "codex_id", "native-uuid", "scrape");
    sessions.updateBinding(node.id, { attachmentType: "tmux", tmuxSession: "dev-qa@test-rig", tmuxPane: "%7" });
    return { rig, node, session };
  }

  const row = (db: Database.Database, table: string) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();

  it("atomically persists only the existing Codex node pin and a nonsecret audit event", async () => {
    const { node } = seat();
    writeFileSync(join(home, "xv-sol61-low-continuity.config.toml"), PROFILE);
    db.prepare("INSERT INTO queue_items (qitem_id, ts_created, ts_updated, source_session, destination_session, state, body) VALUES (?, ?, ?, ?, ?, ?, ?)")
      .run("q-preserve", "2026-10-03T00:00:00Z", "2026-10-03T00:00:00Z", "lead@test-rig", "dev-qa@test-rig", "pending", "existing review assignment");
    const before = {
      nodes: row(db, "nodes"), sessions: row(db, "sessions"), bindings: row(db, "bindings"),
      queue: row(db, "queue_items"), tenures: row(db, "occupant_tenures"),
    };

    const result = await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "xv-sol61-low-continuity", reason: "align successor with observed native tuple", actor: "operator@kernel" });

    expect(result).toMatchObject({ ok: true, from: "old-profile", to: "xv-sol61-low-continuity", changed: true,
      effective: { model: "gpt-6.1-sol", provider: "openai", effort: "low", approval: "never", sandbox: "danger-full-access" },
    });
    expect(probe).toHaveBeenCalledWith("xv-sol61-low-continuity");
    expect(row(db, "nodes")).toEqual(before.nodes.map((n) => ({ ...n as object, codex_config_profile: "xv-sol61-low-continuity" })));
    expect(row(db, "sessions")).toEqual(before.sessions);
    expect(row(db, "bindings")).toEqual(before.bindings);
    expect(row(db, "queue_items")).toEqual(before.queue);
    expect(row(db, "occupant_tenures")).toEqual(before.tenures);
    const events = db.prepare("SELECT payload FROM events WHERE type = 'node.codex_profile_changed'").all() as Array<{ payload: string }>;
    expect(events).toHaveLength(1);
    const audit = JSON.parse(events[0]!.payload);
    expect(audit).toMatchObject({ nodeId: node.id, from: "old-profile", to: "xv-sol61-low-continuity", operator: "operator@kernel" });
    expect(audit.profileSha256).toMatch(/^[0-9a-f]{64}$/);
    expect(JSON.stringify(audit)).not.toContain("native-uuid");
    const repeated = await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "xv-sol61-low-continuity", reason: "idempotent check", actor: "operator@kernel" });
    expect(repeated).toMatchObject({ ok: true, changed: false });
    expect(db.prepare("SELECT count(*) AS n FROM events WHERE type = 'node.codex_profile_changed'").get()).toEqual({ n: 1 });
  });

  it("refuses missing, invalid, unloadable, or model-mismatched profiles without any mutation", async () => {
    seat();
    const before = row(db, "nodes");
    for (const profile of ["missing", "../escape", "bad.name"]) {
      const result = await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile, reason: "test", actor: "operator@kernel" });
      expect(result.ok).toBe(false);
    }
    writeFileSync(join(home, "bad-toml.config.toml"), "model = [");
    expect((await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "bad-toml", reason: "test", actor: "operator@kernel" })).ok).toBe(false);
    writeFileSync(join(home, "wrong-model.config.toml"), PROFILE.replace("gpt-6.1-sol", "gpt-6-luna"));
    expect((await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "wrong-model", reason: "test", actor: "operator@kernel" })).ok).toBe(false);
    writeFileSync(join(home, "unloadable.config.toml"), PROFILE);
    probe.mockRejectedValueOnce(new Error("sensitive loader stderr"));
    const unloadable = await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "unloadable", reason: "test", actor: "operator@kernel" });
    expect(unloadable).toMatchObject({ ok: false, code: "profile_load_failed" });
    expect(JSON.stringify(unloadable)).not.toContain("sensitive loader stderr");
    expect(row(db, "nodes")).toEqual(before);
    expect(db.prepare("SELECT count(*) AS n FROM events WHERE type = 'node.codex_profile_changed'").get()).toEqual({ n: 0 });
  });

  it("refuses non-Codex seats and a missing audit reason", async () => {
    seat("claude-code");
    writeFileSync(join(home, "valid.config.toml"), PROFILE);
    expect(await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "valid", reason: "test", actor: "operator@kernel" })).toMatchObject({ ok: false, code: "runtime_mismatch" });
    expect(await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "valid", reason: "", actor: "operator@kernel" })).toMatchObject({ ok: false, code: "missing_reason" });
    expect(await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "valid", reason: "test", actor: "" })).toMatchObject({ ok: false, code: "missing_actor" });
    expect(probe).not.toHaveBeenCalled();
  });

  it("refuses a profile that changes provider or future launch posture", async () => {
    seat();
    writeFileSync(join(home, "different-posture.config.toml"), PROFILE.replace("approval_policy = \"never\"", "approval_policy = \"on-request\""));
    expect(await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "different-posture", reason: "test", actor: "operator@kernel" }))
      .toMatchObject({ ok: false, code: "profile_posture_mismatch" });
    expect(probe).not.toHaveBeenCalled();
    expect(db.prepare("SELECT codex_config_profile FROM nodes").get()).toEqual({ codex_config_profile: "old-profile" });
  });

  it("refuses a concurrent seat pin change after profile probing", async () => {
    seat();
    writeFileSync(join(home, "next-profile.config.toml"), PROFILE);
    probe.mockImplementationOnce(async () => {
      db.prepare("UPDATE nodes SET codex_config_profile = ? WHERE logical_id = ?").run("someone-else", "dev.qa");
    });
    const result = await service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "next-profile", reason: "test", actor: "operator@kernel" });
    expect(result).toMatchObject({ ok: false, code: "profile_selection_conflict" });
    expect(db.prepare("SELECT codex_config_profile FROM nodes").get()).toEqual({ codex_config_profile: "someone-else" });
    expect(db.prepare("SELECT count(*) AS n FROM events WHERE type = 'node.codex_profile_changed'").get()).toEqual({ n: 0 });
  });

  it("rolls back the pin when its audit event cannot be persisted", async () => {
    seat();
    writeFileSync(join(home, "next-profile.config.toml"), PROFILE);
    vi.spyOn(bus, "persistWithinTransaction").mockImplementationOnce(() => { throw new Error("audit store unavailable"); });
    await expect(service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "next-profile", reason: "test", actor: "operator@kernel" }))
      .rejects.toThrow("audit store unavailable");
    expect(db.prepare("SELECT codex_config_profile FROM nodes").get()).toEqual({ codex_config_profile: "old-profile" });
    expect(db.prepare("SELECT count(*) AS n FROM events WHERE type = 'node.codex_profile_changed'").get()).toEqual({ n: 0 });
  });

  it("waits for an in-flight lifecycle operation before validating or changing the profile", async () => {
    const { node } = seat();
    writeFileSync(join(home, "next-profile.config.toml"), PROFILE);
    let release!: () => void;
    let entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const held = guard.lifecycle([node.id], async () => {
      entered();
      await new Promise<void>(resolve => { release = resolve; });
    });
    await ready;
    let settled = false;
    const selection = service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "next-profile", reason: "safe boundary", actor: "operator@kernel" })
      .finally(() => { settled = true; });
    try {
      await new Promise(resolve => setTimeout(resolve, 15));
      expect(settled).toBe(false);
      expect(probe).not.toHaveBeenCalled();
      expect(db.prepare("SELECT codex_config_profile FROM nodes WHERE id=?").get(node.id)).toEqual({ codex_config_profile: "old-profile" });
    } finally {
      release();
      await held;
    }
    expect(await selection).toMatchObject({ ok: true, changed: true });
    expect(db.prepare("SELECT codex_config_profile FROM nodes WHERE id=?").get(node.id)).toEqual({ codex_config_profile: "next-profile" });
  });

  it("joins an already-owned lifecycle lease without reacquiring it", async () => {
    const { node } = seat();
    writeFileSync(join(home, "next-profile.config.toml"), PROFILE);
    const result = await guard.lifecycle([node.id], () =>
      service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "next-profile", reason: "nested operation", actor: "operator@kernel" }));
    expect(result).toMatchObject({ ok: true, changed: true });
  });

  it("re-reads the seat after waiting and refuses a changed runtime", async () => {
    const { node } = seat();
    writeFileSync(join(home, "next-profile.config.toml"), PROFILE);
    let release!: () => void;
    let entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const held = guard.lifecycle([node.id], async () => {
      entered();
      await new Promise<void>(resolve => { release = resolve; });
    });
    await ready;
    const selection = service.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "next-profile", reason: "safe boundary", actor: "operator@kernel" });
    db.prepare("UPDATE nodes SET runtime = ? WHERE id = ?").run("claude-code", node.id);
    release();
    await held;
    expect(await selection).toMatchObject({ ok: false, code: "runtime_mismatch" });
    expect(probe).not.toHaveBeenCalled();
    expect(db.prepare("SELECT codex_config_profile FROM nodes WHERE id=?").get(node.id)).toEqual({ codex_config_profile: "old-profile" });
  });

  it("fails closed if the daemon has no lifecycle guard", async () => {
    const { node } = seat();
    writeFileSync(join(home, "next-profile.config.toml"), PROFILE);
    const unguarded = new SeatLifecycleService({ db, rigRepo: repo, sessionRegistry: sessions, eventBus: bus,
      tmuxAdapter: {} as TmuxAdapter, codexProfileHome: home, codexProfileProbe: probe });
    expect(await unguarded.setCodexProfile({ seatRef: "dev-qa@test-rig", profile: "next-profile", reason: "test", actor: "operator@kernel" }))
      .toMatchObject({ ok: false, code: "profile_guard_unavailable" });
    expect(db.prepare("SELECT codex_config_profile FROM nodes WHERE id=?").get(node.id)).toEqual({ codex_config_profile: "old-profile" });
  });
});
