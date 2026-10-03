import { describe, it, expect } from "vitest";
import { readOpenCodexRollout, type RolloutProbe } from "../src/domain/codex-open-rollout.js";
const id = "00000000-0000-7000-8000-000000000001";
const file = `/sessions/rollout-2026-${id}.jsonl`;
function fixture(opts: { files?: string[]; cwd?: string; changed?: boolean; malformed?: boolean; command?: string } = {}): RolloutProbe {
  let calls = 0;
  return {
    async run(command, args) {
      if (command === "ps") {
        if (args.includes("lstart=")) return ++calls > 1 && opts.changed ? "different" : "Fri Oct 2 20:39:00 2026";
        return opts.command ?? "/usr/local/bin/codex";
      }
      if (args.includes("cwd")) return "p123\nfcwd\nn/work\n";
      return (opts.files ?? [file]).map(f => `n${f}`).join("\n");
    },
    async canonical(f) { return f; },
    async prefix(f) { expect(f).toBe(file); return opts.malformed ? "broken" : JSON.stringify({ type: "session_meta", payload: { id, cwd: opts.cwd ?? "/work" } }) + "\n"; },
  };
}
describe("native open rollout identity", () => {
  it("binds one exact open native rollout", async () => expect(await readOpenCodexRollout(123, fixture())).toBe(id));
  it("deduplicates multiple descriptors for the same file", async () => expect(await readOpenCodexRollout(123, fixture({ files: [file, file] }))).toBe(id));
  it.each([{ files: [] }, { files: [file, "/sessions/rollout-other.jsonl"] }, { cwd: "/other" }, { changed: true }, { malformed: true }, { command: "/usr/bin/bash" }])("refuses unproven provenance %j", async opts => expect(await readOpenCodexRollout(123, fixture(opts))).toBeUndefined());
  it("refuses a rollout that closes during observation", async () => {
    const io = fixture(); const run = io.run; let openReads = 0;
    io.run = async (command, args) => command === "lsof" && args.includes("0-999") && ++openReads > 1 ? "" : run(command, args);
    expect(await readOpenCodexRollout(123, io)).toBeUndefined();
  });
  it("refuses an incarnation different from the authoritative census", async () => {
    expect(await readOpenCodexRollout(123, fixture(), "Fri Oct 2 19:00:00 2026")).toBeUndefined();
    expect(await readOpenCodexRollout(123, fixture(), "Fri Oct 2 20:39:00 2026")).toBe(id);
  });
  it("never enumerates unrelated newer files", async () => {
    const io = fixture(); const canonical = io.canonical; const observed: string[] = [];
    io.canonical = async f => { observed.push(f); return canonical(f); };
    expect(await readOpenCodexRollout(123, io)).toBe(id);
    expect(observed).toEqual(["/work", file, "/work", file]);
  });
});
