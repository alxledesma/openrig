/** Keep partial summaries local; return only after all original bytes are covered. */
export async function boundedPiSummary(text: string, previous: string, budget: number,
  summarize: (segment: string, previous: string, signal: AbortSignal) => Promise<string>, signal: AbortSignal): Promise<string> {
  if (!Number.isSafeInteger(budget) || budget < 1024) throw new Error("Invalid input budget");
  const bytes = Buffer.from(text); let offset = 0, summary = previous, calls = 0;
  const summaryLimit = Math.min(12000, Math.floor(budget / 4));
  while (offset < bytes.length) {
    signal.throwIfAborted();
    const available = budget - Buffer.byteLength(summary);
    if (available < 4) throw new Error("Carried summary exceeds budget");
    let end = Math.min(bytes.length, offset + available);
    while (end < bytes.length && (bytes[end]! & 0xc0) === 0x80) end--;
    if (end <= offset) throw new Error("Cannot fit segment");
    if (++calls > 64) throw new Error("Compaction call limit reached");
    const next = await summarize(bytes.subarray(offset, end).toString("utf8"), summary, signal);
    signal.throwIfAborted();
    if (!next.trim() || Buffer.byteLength(next) > summaryLimit) throw new Error("Invalid generated summary");
    summary = next; offset = end;
  }
  signal.throwIfAborted();
  if (!summary.trim()) throw new Error("Nothing to summarize");
  return summary;
}
