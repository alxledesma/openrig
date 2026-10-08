import { mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test, vi } from "vitest";
import { CodexSameGenerationRehost, type CodexRehostOptions } from "../src/domain/codex-rehost.js";

const nativeId = "native-thread-exact";
const sessionMeta = JSON.stringify({ type: "session_meta", payload: { id: nativeId } });
const turnContext = JSON.stringify({ type: "turn_context", payload: { model: "gpt-6-luna" } });

function withHistory(contents: Buffer, run: (file: string) => void): void {
  const dir = realpathSync(mkdtempSync(path.join(tmpdir(), "codex-rehost-history-")));
  const file = path.join(dir, "native.jsonl");
  try {
    writeFileSync(file, contents, { mode: 0o600 });
    run(file);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function readHistory(file: string): Buffer {
  const service = new CodexSameGenerationRehost({} as CodexRehostOptions);
  return (service as unknown as { history(path: string, id: string): Buffer }).history(file, nativeId);
}

test("rehost verifies a large exact Buffer snapshot without decoding the full history as one string", () => {
  const repeatedLine = JSON.stringify({ type: "event", payload: { value: "x".repeat(48 * 1024) } }) + "\n";
  const contents = Buffer.from(`${sessionMeta}\n${repeatedLine.repeat(420)}${turnContext}\n`);
  expect(contents.byteLength).toBeGreaterThan(16 * 1024 * 1024);

  const NativeTextDecoder = globalThis.TextDecoder;
  class GuardedTextDecoder extends NativeTextDecoder {
    override decode(input?: AllowSharedBufferSource, options?: TextDecodeOptions): string {
      if (input instanceof Uint8Array && input.byteLength > 1024 * 1024) {
        throw new Error("whole-history decode attempted");
      }
      return super.decode(input, options);
    }
  }

  vi.stubGlobal("TextDecoder", GuardedTextDecoder);
  try {
    withHistory(contents, (file) => {
      expect(readHistory(file).equals(contents)).toBe(true);
    });
  } finally {
    vi.unstubAllGlobals();
  }
});

test("rehost history keeps fatal UTF-8, strict JSON, and exactly-one matching metadata checks", () => {
  withHistory(Buffer.from(`${sessionMeta}\n${turnContext}\n`), (file) => {
    expect(readHistory(file).toString("utf8")).toBe(`${sessionMeta}\n${turnContext}\n`);
  });

  withHistory(Buffer.from(`\uFEFF${sessionMeta}\n${turnContext}\n`), (file) => {
    expect(readHistory(file).toString("utf8")).toBe(`\uFEFF${sessionMeta}\n${turnContext}\n`);
  });

  withHistory(Buffer.from(`${sessionMeta}\n{malformed}\n${turnContext}\n`), (file) => {
    expect(() => readHistory(file)).toThrow(SyntaxError);
  });

  withHistory(Buffer.from(`${sessionMeta}\n\uFEFF${turnContext}\n`), (file) => {
    expect(() => readHistory(file)).toThrow(SyntaxError);
  });

  withHistory(Buffer.concat([Buffer.from(`${sessionMeta}\n`), Buffer.from([0xff]), Buffer.from(`\n${turnContext}\n`)]), (file) => {
    expect(() => readHistory(file)).toThrow(TypeError);
  });

  withHistory(Buffer.from(`${JSON.stringify({ type: "session_meta", payload: { id: "different-native-id" } })}\n${turnContext}\n`), (file) => {
    expect(() => readHistory(file)).toThrow(expect.objectContaining({ code: "codex_rehost_history_mismatch" }));
  });

  withHistory(Buffer.from(`${sessionMeta}\n${sessionMeta}\n${turnContext}\n`), (file) => {
    expect(() => readHistory(file)).toThrow(expect.objectContaining({ code: "codex_rehost_history_mismatch" }));
  });

  withHistory(Buffer.from(`${sessionMeta}\n${turnContext}`), (file) => {
    expect(() => readHistory(file)).toThrow(expect.objectContaining({ code: "codex_rehost_history_unstable" }));
  });
});
