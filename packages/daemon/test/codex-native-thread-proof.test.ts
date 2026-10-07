import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, writeFileSync, rmSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { proveCodexNativeThread } from "../src/domain/codex-native-thread-proof.js";
describe("positive kernel live Codex thread proof", () => {
  let dir: string, file: string, files: string, start: string, argv: string;
  let read: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    dir = realpathSync(mkdtempSync(join(tmpdir(), "codex-thread-proof-"))); file = join(dir, "rollout-saved.jsonl"); writeFileSync(file, "{}\n");
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
});
