import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Buffer } from "node:buffer";
import * as vm from "node:vm";
import * as ts from "typescript";
import { beforeEach, expect, it, vi } from "vitest";

const serializedConversation = "x".repeat(2048);
const extensionPath = resolve(dirname(fileURLToPath(import.meta.url)), "../assets/pi-bounded-compaction-extension.ts");
const extensionSource = readFileSync(extensionPath, "utf8");
const nodeRequire = createRequire(import.meta.url);

function loadExtension(consoleError: ReturnType<typeof vi.fn>) {
  const transpiled = ts.transpileModule(extensionSource, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const module = { exports: {} as Record<string, unknown> };
  const injectedRequire = (specifier: string): unknown => {
    if (specifier === "node:crypto") return nodeRequire(specifier);
    if (specifier === "@earendil-works/pi-coding-agent") {
      return {
        convertToLlm: (messages: unknown[]) => messages,
        serializeConversation: () => serializedConversation,
      };
    }
    if (specifier === "./pi-bounded-summary.js") {
      return {
        boundedPiSummary: async (
          text: string,
          previous: string,
          budget: number,
          summarize: (segment: string, previous: string, signal: AbortSignal) => Promise<string>,
          signal: AbortSignal,
        ) => {
          signal.throwIfAborted();
          const summary = await summarize(text.slice(0, budget), previous, signal);
          signal.throwIfAborted();
          return summary;
        },
      };
    }
    throw new Error(`Unexpected extension dependency: ${specifier}`);
  };
  vm.runInNewContext(transpiled.outputText, {
    exports: module.exports,
    module,
    require: injectedRequire,
    Buffer,
    console: { error: consoleError },
  }, { filename: extensionPath });
  return module.exports.default as (pi: any) => void;
}

type AssistantMessage = {
  stopReason: string;
  errorMessage?: string;
  content: Array<{ type: string; text?: string }>;
};

function setup(response: AssistantMessage, reasoning: boolean, maxTokens = 32768) {
  let beforeCompact: ((event: any, context: any) => Promise<any>) | undefined;
  const consoleError = vi.fn();
  loadExtension(consoleError)({
    on: (event: string, handler: (event: any, context: any) => Promise<any>) => {
      if (event === "session_before_compact") beforeCompact = handler;
    },
  });

  const signal = new AbortController().signal;
  const messagesToSummarize = [{ role: "user", content: "historical input" }];
  const turnPrefixMessages = [{ role: "assistant", content: "current turn prefix" }];
  const preparation = {
    messagesToSummarize,
    turnPrefixMessages,
    previousSummary: "",
    firstKeptEntryId: "retained-entry-17",
    tokensBefore: 4321,
    fileOps: { read: ["src/read.ts"], edited: ["src/edited.ts"], written: [] },
  };
  const streamSimple = vi.fn((_model: unknown, _context: unknown, _options: unknown) => ({
    result: async () => response,
  }));
  const event = {
    reason: "threshold",
    preparation,
    signal,
    customInstructions: "Keep the project constraints.",
  };
  const notify = vi.fn();
  const context = {
    model: {
      provider: "fixture",
      id: "reasoning-fixture",
      contextWindow: 2048,
      maxTokens,
      reasoning,
    },
    modelRegistry: { streamSimple },
    ui: { notify },
  };
  return {
    event,
    preparation,
    beforeCompact: () => beforeCompact!(event, context),
    model: context.model,
    streamSimple,
    notify,
    context,
    consoleError,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

it("uses provider-neutral low reasoning with a bounded total output budget and preserves the retained boundary", async () => {
  const response: AssistantMessage = { stopReason: "stop", content: [{ type: "text", text: "concise summary" }] };
  const test = setup(response, true);

  const result = await test.beforeCompact();

  expect(test.streamSimple).toHaveBeenCalledTimes(1);
  expect(test.streamSimple.mock.calls[0]?.[2]).toEqual({
    maxTokens: 16384,
    reasoning: "low",
    signal: test.event.signal,
    cacheRetention: "none",
  });
  expect(result.compaction).toMatchObject({
    firstKeptEntryId: "retained-entry-17",
    tokensBefore: 4321,
    summary: expect.stringContaining("concise summary"),
  });
  expect(result.compaction.details).toMatchObject({
    boundedHistorySummary: true,
    readFiles: ["src/read.ts"],
    modifiedFiles: ["src/edited.ts"],
  });
});

it("caps reasoning output at the model's advertised maximum when it is below 16384", async () => {
  const test = setup({ stopReason: "stop", content: [{ type: "text", text: "summary" }] }, true, 8192);

  await test.beforeCompact();

  expect(test.streamSimple.mock.calls[0]?.[2]).toEqual({
    maxTokens: 8192,
    reasoning: "low",
    signal: test.event.signal,
    cacheRetention: "none",
  });
});

it("keeps the existing 4096 cap for non-reasoning models", async () => {
  const test = setup({ stopReason: "stop", content: [{ type: "text", text: "summary" }] }, false);

  await test.beforeCompact();

  expect(test.streamSimple.mock.calls[0]?.[2]).toMatchObject({ maxTokens: 4096 });
  expect(test.streamSimple.mock.calls[0]?.[2]).not.toHaveProperty("reasoning");
});

it("rejects truncated or aborted output without changing history, exposing partial text, or retrying", async () => {
  const providerPayload = "PRIVATE_PROVIDER_PAYLOAD_SHOULD_NOT_BE_LOGGED";
  const partialText = "PARTIAL_SUMMARY_MUST_NOT_BE_USED";
  const test = setup({
    stopReason: "length",
    errorMessage: providerPayload,
    content: [{ type: "text", text: partialText }],
  }, true);
  const before = structuredClone(test.preparation);
  const first = await test.beforeCompact();
  const second = await test.beforeCompact();

  expect(first).toEqual({ cancel: true });
  expect(second).toEqual({ cancel: true });
  expect(test.streamSimple).toHaveBeenCalledTimes(1);
  expect(test.preparation).toEqual(before);
  expect(JSON.stringify(test.consoleError.mock.calls)).not.toContain(providerPayload);
  expect(JSON.stringify(test.consoleError.mock.calls)).not.toContain(partialText);
  expect(test.notify).toHaveBeenCalledWith(expect.stringContaining("provider_output_truncated"), "error");

  const aborted = setup({ stopReason: "stop", content: [{ type: "text", text: partialText }] }, true);
  const abortController = new AbortController();
  abortController.abort();
  aborted.event.signal = abortController.signal;
  const abortedBefore = structuredClone(aborted.preparation);
  const cancelled = await aborted.beforeCompact();
  expect(cancelled).toEqual({ cancel: true });
  expect(aborted.streamSimple).not.toHaveBeenCalled();
  expect(aborted.preparation).toEqual(abortedBefore);
});
