import { expect, it } from "vitest";
import { boundedPiSummary } from "../src/adapters/pi-bounded-summary.js";
it("covers every UTF-8 byte and carries prior summaries within budget", async () => {
  const text = "a😀漢字".repeat(900), parts: string[] = [], prior: string[] = [];
  const result = await boundedPiSummary(text, "original", 1024, async (part, old) => {
    expect(Buffer.byteLength(part + old)).toBeLessThanOrEqual(1024);
    parts.push(part); prior.push(old); return `summary-${parts.length}`;
  }, new AbortController().signal);
  expect(parts.join("")).toBe(text); expect(prior[0]).toBe("original");
  expect(prior[1]).toBe("summary-1"); expect(result).toBe(`summary-${parts.length}`);
});
it("returns no partial result when a provider fails or cancellation occurs", async () => {
  let count = 0;
  await expect(boundedPiSummary("x".repeat(3000), "", 1024, async () => {
    if (++count === 2) throw new Error("provider failed"); return "partial";
  }, new AbortController().signal)).rejects.toThrow("provider failed");
  const c = new AbortController();
  await expect(boundedPiSummary("x".repeat(3000), "", 1024, async () => {
    c.abort(); return "partial";
  }, c.signal)).rejects.toThrow();
});
it("rejects empty or oversized summaries", async () => {
  for (const value of ["", "x".repeat(1024)])
    await expect(boundedPiSummary("source", "", 1024, async () => value, new AbortController().signal)).rejects.toThrow();
});

it("caps generated summary size and total provider calls", async () => {
  let calls = 0;
  await expect(boundedPiSummary("x".repeat(3000), "", 1024, async () => {
    calls++; return "x".repeat(1010);
  }, new AbortController().signal)).rejects.toThrow("Invalid generated summary");
  expect(calls).toBe(1);
  calls = 0;
  await expect(boundedPiSummary("x".repeat(100000), "", 1024, async () => {
    calls++; return "brief";
  }, new AbortController().signal)).rejects.toThrow("call limit");
  expect(calls).toBe(64);
});
