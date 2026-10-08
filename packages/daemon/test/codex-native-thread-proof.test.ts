import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, writeFileSync, rmSync, realpathSync, unlinkSync, appendFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { proveCodexNativeThread } from "../src/domain/codex-native-thread-proof.js";
const parentId = "019a1000-0000-7000-8000-000000000001";
const childId = "019a1000-0000-7000-8000-000000000002";
const child2Id = "019a1000-0000-7000-8000-000000000003";
const childPath = (dir: string, id = childId) => join(dir, `rollout-2026-10-08-${id}.jsonl`);
const childBornAt = "2026-10-08T01:00:00.000Z";
const meta = (id: string, extra: Record<string, unknown> = {}) => ({ type: "session_meta", payload: { id, ...extra } });
const event = (type: string, turnId: string, timestamp?: string) => ({ type: "event_msg", timestamp, payload: { type, turn_id: turnId } });
function directChildRows(id: string, parent: string, mode: string = "complete") {
  const childTimestamp = mode === "missing-metadata-timestamp" ? undefined : childBornAt;
  const first = meta(id, { ...(childTimestamp ? { timestamp: childTimestamp } : {}), parent_thread_id: parent, thread_source: "subagent",
    source: { subagent: { thread_spawn: { parent_thread_id: parent } } } });
  const rows: unknown[] = [first, event("task_started", "child-turn-1", "2026-10-08T01:00:01.000Z")];
  if (mode === "complete") rows.push(event("task_complete", "child-turn-1", "2026-10-08T01:00:02.000Z"));
  if (mode === "mismatched-terminal") rows.push(event("task_complete", "different-turn", "2026-10-08T01:00:02.000Z"));
  if (mode === "later-task") rows.push(event("task_started", "child-turn-2", "2026-10-08T01:00:03.000Z"));
  if (mode === "missing-event-timestamp") rows[2] = event("task_complete", "child-turn-1");
  if (mode === "inherited-only") {
    rows.length = 1;
    rows.push(event("task_started", "parent-turn", "2026-10-08T00:59:58.000Z"));
    rows.push(event("task_complete", "parent-turn", "2026-10-08T00:59:59.000Z"));
  }
  if (mode === "inherited-plus-child") {
    rows.length = 1;
    rows.push(event("task_started", "parent-turn", "2026-10-08T00:59:58.000Z"));
    rows.push(event("task_complete", "parent-turn", "2026-10-08T00:59:59.000Z"));
    rows.push(event("task_started", "child-turn-1", "2026-10-08T01:00:01.000Z"));
    rows.push(event("task_complete", "child-turn-1", "2026-10-08T01:00:02.000Z"));
  }
  // Codex child rollouts may append inherited parent metadata after the child header.
  rows.push(meta(parent, { thread_source: "main" }));
  return rows.map(row => JSON.stringify(row)).join("\n") + "\n";
}

describe("positive kernel live Codex thread proof", () => {
  let dir: string, file: string, files: string, start: string, argv: string;
  let read: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    dir = realpathSync(mkdtempSync(join(tmpdir(), "codex-thread-proof-")));
    file = join(dir, `rollout-2026-10-08-${parentId}.jsonl`);
    writeFileSync(file, JSON.stringify(meta(parentId, { timestamp: "2026-10-08T00:00:00.000Z" })) + "\n");
    files = `p20\nn${file}\n`; start = "Wed Oct 7 16:01:40 2026"; argv = "codex -p exact";
    read = vi.fn(async (command, args) => ({ stdout: command.endsWith("lsof") ? files : args.includes("lstart=") ? start : argv, stderr: "" }));
  });
  afterEach(() => rmSync(dir, { recursive: true, force: true }));
  it("fresh launch binds exact open rollout across both observations", async () => {
    const recheck = await proveCodexNativeThread(20, file, argv, read); await recheck();
    expect(read.mock.calls.filter(([command]) => command.endsWith("lsof"))).toHaveLength(2);
  });
  it("resume argv alone is insufficient after a TUI thread switch", async () => {
    argv += " resume saved-thread"; files = `p20\nn${join(dir, "rollout-new-thread.jsonl")}\n`;
    await expect(proveCodexNativeThread(20, file, argv, read)).rejects.toThrow("different or competing");
  });
  it.each(["missing", "ambiguous", "wrong-pid", "deleted"])("%s open rollout refuses", async kind => {
    if (kind === "missing") files = "p20\nn/dev/tty\n";
    if (kind === "ambiguous") files += `n${join(dir, "rollout-other.jsonl")}\n`;
    if (kind === "wrong-pid") files = files.replace("p20", "p21");
    if (kind === "deleted") files = `p20\nn${file} (deleted)\n`;
    await expect(proveCodexNativeThread(20, file, argv, read)).rejects.toThrow("codex_native_thread_unbound");
  });
  it.each(["start", "argv", "file", "unavailable"])("post-observation %s drift refuses", async kind => {
    const recheck = await proveCodexNativeThread(20, file, argv, read);
    if (kind === "start") start = "new process start";
    if (kind === "argv") argv += " resume different";
    if (kind === "file") files = `p20\nn${join(dir, "rollout-other.jsonl")}\n`;
    if (kind === "unavailable") read.mockRejectedValue(new Error("kernel inaccessible"));
    await expect(recheck()).rejects.toThrow("codex_native_thread_unbound");
  });
  it("duplicate descriptors of the same exact rollout are not competing threads", async () => {
    files += `n${file}\n`; await expect(proveCodexNativeThread(20, file, argv, read)).resolves.toBeTypeOf("function");
  });
  it("accepts completed direct child rollouts with inherited parent metadata", async () => {
    const first = childPath(dir), second = childPath(dir, child2Id);
    writeFileSync(first, directChildRows(childId, parentId));
    writeFileSync(second, directChildRows(child2Id, parentId));
    files = `p20\nn${file}\nn${first}\nn${second}\n`;
    const recheck = await proveCodexNativeThread(20, file, argv, read);
    await expect(recheck()).resolves.toBeUndefined();
  });
  it("does not mistake inherited parent completion before child birth for child completion", async () => {
    const p = childPath(dir); writeFileSync(p, directChildRows(childId, parentId, "inherited-only"));
    files = `p20\nn${file}\nn${p}\n`;
    await expect(proveCodexNativeThread(20, file, argv, read)).rejects.toThrow("codex_native_thread_unbound");
    writeFileSync(p, directChildRows(childId, parentId, "inherited-plus-child"));
    const recheck = await proveCodexNativeThread(20, file, argv, read);
    await expect(recheck()).resolves.toBeUndefined();
  });
  it.each([
    ["unrelated parent", "wrong-parent"],
    ["active child without task_complete", "active"],
    ["mismatched terminal turn", "mismatched-terminal"],
    ["later unfinished task lifecycle", "later-task"],
    ["missing child metadata timestamp", "missing-metadata-timestamp"],
    ["missing task lifecycle timestamp", "missing-event-timestamp"],
  ])("rejects child rollout with %s", async (_label, mode) => {
    const p = childPath(dir);
    const relationship = mode === "wrong-parent" ? "019a1000-0000-7000-8000-000000000099" : parentId;
    writeFileSync(p, directChildRows(childId, relationship, mode));
    files = `p20\nn${file}\nn${p}\n`;
    await expect(proveCodexNativeThread(20, file, argv, read)).rejects.toThrow("codex_native_thread_unbound");
  });
  it("rejects malformed child JSONL", async () => {
    const p = childPath(dir); writeFileSync(p, '{"type":"session_meta"\nnot-json\n');
    files = `p20\nn${file}\nn${p}\n`;
    await expect(proveCodexNativeThread(20, file, argv, read)).rejects.toThrow("codex_native_thread_unbound");
  });
  it("rejects child file changes when revalidating an accepted proof", async () => {
    const p = childPath(dir); writeFileSync(p, directChildRows(childId, parentId));
    files = `p20\nn${file}\nn${p}\n`;
    const recheck = await proveCodexNativeThread(20, file, argv, read);
    appendFileSync(p, JSON.stringify({ type: "response_item", payload: { changed: true } }) + "\n");
    await expect(recheck()).rejects.toThrow("codex_native_thread_unbound");
  });
  it("requires the saved parent rollout to exist and anchor child metadata", async () => {
    const p = childPath(dir); writeFileSync(p, directChildRows(childId, parentId));
    files = `p20\nn${file}\nn${p}\n`;
    unlinkSync(file);
    await expect(proveCodexNativeThread(20, file, argv, read)).rejects.toThrow("saved rollout unavailable");
  });
});
