// S-A/S-B regressions (ROOT-CODEX-GENERATION-ARCHITECTURE + R3 review F1/F2):
// provenance is STRUCTURED and ledger-rechecked (node/session/tenure/type/token),
// never an echoed request token; composer binding is proven by parsing LEADING
// environment assignments of the ACTUAL SeatLaunchEnvironment output, so neither a
// best-effort fallback nor argument-position text can fake generation identity.
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mkdtempSync, rmSync, chmodSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { createDb } from "../src/db/connection.js";
import { seed } from "./helpers/coordinator-fixture.js";
import { continuationLedgerGeneration, type ContinuationNativeProvenance } from "../src/domain/restore-orchestrator.js";
import { CodexResumeAdapter, leadingEnvAssignments } from "../src/adapters/codex-resume.js";
import { ClaudeResumeAdapter } from "../src/adapters/claude-resume.js";
import { SeatLaunchEnvironment } from "../src/domain/seat-launch-environment.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";
let dir: string, db: Database.Database;

beforeEach(() => { dir = mkdtempSync(join(tmpdir(), "cg-")); db = createDb(join(dir, "db")); seed(db); });
afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

const tokenOf = (t: string | null, type = "claude_id") => { db.prepare("UPDATE sessions SET resume_token=?, resume_type=? WHERE id='lead@xv'").run(t, type); };
const prov = (over: Partial<ContinuationNativeProvenance> = {}): ContinuationNativeProvenance => ({ nodeId: "lead@xv", sessionId: "lead@xv", tenureGeneration: "lead-g1", nativeType: "claude_id", nativeToken: "NATIVE-2", ...over });

describe("structured continuation provenance", () => {
  it("current recorded token inherits the ledger generation (G2 over stale session env G1)", () => {
    tokenOf("NATIVE-2");
    expect(continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id")).toEqual({ generation: "G2" });
  });
  it("unknown continuity refuses strictly: absent/empty tokens never fall back to G1", () => {
    tokenOf(null);
    const d = continuationLedgerGeneration(db, "lead@xv", "G2", null, "claude_id");
    expect("refusal" in d && d.refusal.message).toContain("continuation_identity_unproven");
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "ANY", "claude_id")).toBe(true);
  });
  it("an echoed request token is NOT provenance: blank row + unbacked claim refuses", () => {
    tokenOf(null); // pre-launch row itself recorded nothing
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,created_at) VALUES ('zz-new-blank','lead@xv','lead@xv','running',NULL,'2999-01-01T00:00:00Z')").run();
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id", prov())).toBe(true);
  });
  it("an arbitrary or unregistered tenure generation never qualifies", () => {
    tokenOf(null);
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,resume_type,created_at) VALUES ('a-old-row','lead@xv','lead@xv','superseded','NATIVE-A','claude_id','2000-01-01T00:00:00Z')").run();
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,created_at) VALUES ('z-blank-new','lead@xv','lead@xv','running',NULL,'2999-01-01T00:00:00Z')").run();
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "CURRENT-G", "NATIVE-A", "claude_id", prov({ tenureGeneration: "GHOST-G", sessionId: "a-old-row", nativeToken: "NATIVE-A" }))).toBe(true);
  });
  it("a NON-immediate older conversation cannot ride the exception", () => {
    tokenOf(null);
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,resume_type,created_at) VALUES ('a-ancient','lead@xv','lead@xv','superseded','NATIVE-A','claude_id','2000-01-01T00:00:00Z')").run();
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,resume_type,created_at) VALUES ('m-recent','lead@xv','lead@xv','superseded','NATIVE-M','claude_id','2997-01-01T00:00:00Z')").run();
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,created_at) VALUES ('z-new','lead@xv','lead@xv','running',NULL,'2999-01-01T00:00:00Z')").run();
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-A", "claude_id", prov({ sessionId: "a-ancient", nativeToken: "NATIVE-A" }))).toBe(true);
  });
  it("foreign node, tampered session, or wrong native type all refuse", () => {
    tokenOf(null);
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,resume_type,created_at) VALUES ('p-row','lead@xv','lead@xv','superseded','NATIVE-2','claude_id','2000-01-01T00:00:00Z')").run();
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,created_at) VALUES ('z-new','lead@xv','lead@xv','running',NULL,'2999-01-01T00:00:00Z')").run();
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id", prov({ nodeId: "peer@xv" }))).toBe(true);
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id", prov({ sessionId: "ghost" }))).toBe(true);
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id", prov({ nativeType: "codex_id" }))).toBe(true);
  });
  it("valid exact same-current pre-launch provenance composes the intended generation", () => {
    tokenOf(null);
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,resume_type,created_at) VALUES ('p-real','lead@xv','lead@xv','superseded','NATIVE-2','claude_id','2998-01-01T00:00:00Z')").run();
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,created_at) VALUES ('z-new','lead@xv','lead@xv','running',NULL,'2999-01-01T00:00:00Z')").run();
    expect(continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id", prov({ sessionId: "p-real" }))).toEqual({ generation: "G2" });
  });
  it("missing ledger tenure keeps today's posture even with provenance present", () => {
    expect(continuationLedgerGeneration(db, "lead@xv", undefined, "X", "claude_id", prov())).toEqual({});
  });
  it("the guard mutates nothing (tenures, sessions, events byte-stable)", () => {
    tokenOf("NATIVE-2");
    const snap = () => JSON.stringify([db.prepare("SELECT * FROM occupant_tenures ORDER BY id").all(), db.prepare("SELECT * FROM sessions ORDER BY id").all(), db.prepare("SELECT * FROM events ORDER BY rowid").all()]);
    const before = snap();
    continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_id");
    continuationLedgerGeneration(db, "lead@xv", "G2", "WRONG", "claude_id");
    expect(snap()).toBe(before);
  });
});

// ---- actual composer behavior (R3-F2): real SeatLaunchEnvironment, not stubs ----
function composerFixture(generationEnv: string | (() => Promise<string>), native: "codex" | "claude", homeOverride?: string) {
  const bin = join(dir, "bin"); mkdirSync(bin, { recursive: true });
  const cli = join(bin, "rig"); writeFileSync(cli, "#!/bin/sh\nexit 0\n"); chmodSync(cli, 0o755);
  const fakebin = join(dir, "fakebin"); mkdirSync(fakebin, { recursive: true });
  for (const t of ["codex", "claude"]) { const p = join(fakebin, t); writeFileSync(p, "#!/bin/sh\nexit 0\n"); chmodSync(p, 0o755); }
  let paneCalls = 0;
  const tmux = {
    sendText: vi.fn(async () => ({ ok: true as const })), sendKeys: vi.fn(async () => ({ ok: true as const })), sendShellCommand: vi.fn(async () => ({ ok: true as const })),
    // First read is the composer's POSIX-shell check; later reads verify the resume.
    getPaneCommand: vi.fn(async () => (++paneCalls === 1 ? "bash" : native)),
    capturePaneContent: vi.fn(async () => native === "codex" ? "OpenAI Codex (v0.0.0)\n› Ask Codex to do anything" : ""),
    createSession: async () => ({ ok: true as const }), killSession: async () => ({ ok: true as const }), listSessions: async () => [], listWindows: async () => [], listPanes: async () => [], hasSession: async () => false,
    getSessionEnv: vi.fn(async (_s: string, key: string) => key === "OPENRIG_OCCUPANT_GENERATION" ? (typeof generationEnv === "string" ? generationEnv : await generationEnv())
      : key === "OPENRIG_NODE_ID" ? "N1" : key === "OPENRIG_SESSION_NAME" ? "lead@xv" : undefined),
    setenv: vi.fn(async () => ({ ok: true as const })),
  } as unknown as TmuxAdapter;
  const sle = new SeatLaunchEnvironment(tmux, { PATH: `${fakebin}:/usr/bin:/bin`, OPENRIG_HOME: homeOverride ?? "home" }, dir, cli, undefined);
  return { tmux, sle };
}

describe("actual composer binding", () => {
  it("codex real composition proves G2 binding and no G1 inheritance", async () => {
    const { tmux, sle } = composerFixture("G1", "codex");
    const adapter = new CodexResumeAdapter(tmux, { seatLaunchEnvironment: sle, launchPath: "/usr/bin:/bin" } as never);
    const r = await adapter.resume("lead@xv", "codex_id", "NATIVE-2", dir, null, undefined, null, null, "G2");
    expect(r.ok).toBe(true);
    const sent = String((tmux.sendShellCommand as ReturnType<typeof vi.fn>).mock.calls[0]![1]);
    expect(sent).toContain("OPENRIG_OCCUPANT_GENERATION=G2");
    expect(sent).not.toContain("G1");
  });
  it("composer catch-fallback (no leading binding) refuses with ZERO input", async () => {
    const boom = async () => { throw new Error("transport noise"); };
    const { tmux, sle } = composerFixture(boom, "codex");
    const adapter = new CodexResumeAdapter(tmux, { seatLaunchEnvironment: sle, launchPath: "/usr/bin:/bin" } as never);
    const r = await adapter.resume("lead@xv", "codex_id", "NATIVE-2", dir, null, undefined, null, null, "G2");
    expect(r.ok).toBe(false);
    expect(!r.ok && r.message).toContain("continuation_generation_composition_unverified");
    expect(tmux.sendShellCommand).not.toHaveBeenCalled();
    expect(tmux.sendText).not.toHaveBeenCalled();
  });
  it("generation text only in ARGUMENT position (fallback binding) refuses", async () => {
    const boom = async () => { throw new Error("transport noise"); };
    const { tmux, sle } = composerFixture(boom, "claude");
    const adapter = new ClaudeResumeAdapter(tmux, { seatLaunchEnvironment: sle } as never);
    const r = await adapter.resume("lead@xv", "claude_id", "OPENRIG_OCCUPANT_GENERATION=G2", dir, undefined, null, undefined, "N1", null, "G2");
    expect(r.ok).toBe(false);
    expect(!r.ok && r.message).toContain("continuation_generation_composition_unverified");
    expect(tmux.sendShellCommand).not.toHaveBeenCalled();
  });
  it("claude real composition proves G2 over the stale session G1", async () => {
    const { tmux, sle } = composerFixture("G1", "claude");
    const adapter = new ClaudeResumeAdapter(tmux, { seatLaunchEnvironment: sle } as never);
    const r = await adapter.resume("lead@xv", "claude_id", "NATIVE-2", dir, undefined, null, undefined, "N1", null, "G2");
    expect(r.ok).toBe(true);
    const sent = String((tmux.sendShellCommand as ReturnType<typeof vi.fn>).mock.calls[0]![1]);
    expect(sent).toMatch(/OPENRIG_OCCUPANT_GENERATION='?G2'?/);
    expect(sent).not.toMatch(/OPENRIG_OCCUPANT_GENERATION='?G1'?/);
  });
  it("real emitter grammar: quoted HOME value with SPACES keeps the later G2 binding valid", async () => {
    const { tmux, sle } = composerFixture("G1", "claude", join(dir, "instance home with space"));
    const adapter = new ClaudeResumeAdapter(tmux, { seatLaunchEnvironment: sle } as never);
    const r = await adapter.resume("lead@xv", "claude_id", "NATIVE-2", dir, undefined, null, undefined, "N1", null, "G2");
    expect(r.ok).toBe(true);
    const sent = String((tmux.sendShellCommand as ReturnType<typeof vi.fn>).mock.calls[0]![1]);
    expect(leadingEnvAssignments(sent).OPENRIG_OCCUPANT_GENERATION).toBe("G2");
    expect(leadingEnvAssignments(sent).OPENRIG_HOME).toBe(join(dir, "instance home with space"));
  });
  it("stray generation TEXT inside a quoted HOME value cannot masquerade as the binding", async () => {
    const { sle } = composerFixture("G1", "claude", "home OPENRIG_OCCUPANT_GENERATION=G2 ");
    const composed = await sle.command("lead@xv", "claude --resume 't'", { runtime: "claude-code", nodeId: "N1" });
    // Real assignment says G1; the G2 text lives inside HOME's value only.
    expect(leadingEnvAssignments(composed).OPENRIG_OCCUPANT_GENERATION).toBe("G1");
  });
});

describe("requested native type binding", () => {
  it("correct stored token but INCOMPATIBLE requested runtime refuses before inheritance", () => {
    tokenOf("NATIVE-2", "claude_id");
    const d = continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "codex_id");
    expect("refusal" in d && d.refusal.message).toContain("continuation_native_mismatch");
  });
  it("legitimate claude_name versus stored claude_id stays compatible", () => {
    tokenOf("NATIVE-2", "claude_id");
    expect(continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_name")).toEqual({ generation: "G2" });
  });
  it("blank-row lineage also binds the requested runtime family", () => {
    tokenOf(null);
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,resume_type,created_at) VALUES ('p-c','lead@xv','lead@xv','superseded','NATIVE-2','claude_id','2998-01-01T00:00:00Z')").run();
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status,resume_token,created_at) VALUES ('z-n','lead@xv','lead@xv','running',NULL,'2999-01-01T00:00:00Z')").run();
    expect("refusal" in continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "codex_id", prov({ sessionId: "p-c" }))).toBe(true);
    expect(continuationLedgerGeneration(db, "lead@xv", "G2", "NATIVE-2", "claude_name", prov({ sessionId: "p-c" }))).toEqual({ generation: "G2" });
  });
});
