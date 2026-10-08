import { closeSync, openSync, readSync } from "node:fs";

const HISTORY_CHUNK_BYTES = 64 * 1024;
const MAX_HISTORY_LINE_BYTES = 16 * 1024 * 1024;

/** Visit complete JSONL records without decoding or retaining the whole history.
 * A string source is a file path; a Buffer source is an already-owned snapshot. */
export function forEachJsonlLine(source: string | Buffer, visit: (line: Buffer) => void): void {
  let line = Buffer.allocUnsafe(HISTORY_CHUNK_BYTES);
  let lineBytes = 0;
  const chunk = Buffer.allocUnsafe(HISTORY_CHUNK_BYTES);

  const append = (source: Buffer, start: number, end: number): void => {
    const count = end - start;
    const required = lineBytes + count;
    if (required > MAX_HISTORY_LINE_BYTES) {
      throw new Error("Native history line exceeds streaming limit");
    }
    if (required > line.length) {
      const capacity = Math.min(MAX_HISTORY_LINE_BYTES, Math.max(required, line.length * 2));
      const expanded = Buffer.allocUnsafe(capacity);
      line.copy(expanded, 0, 0, lineBytes);
      line = expanded;
    }
    source.copy(line, lineBytes, start, end);
    lineBytes = required;
  };

  const consumeLine = (): void => {
    visit(line.subarray(0, lineBytes));
    lineBytes = 0;
  };

  if (Buffer.isBuffer(source)) {
    let start = 0;
    while (start < source.length) {
      const newline = source.indexOf(0x0a, start);
      if (newline < 0) break;
      const length = newline - start;
      if (length > MAX_HISTORY_LINE_BYTES) throw new Error("Native history line exceeds streaming limit");
      visit(source.subarray(start, newline));
      start = newline + 1;
    }
    if (source.length - start > MAX_HISTORY_LINE_BYTES) throw new Error("Native history line exceeds streaming limit");
    if (start < source.length) visit(source.subarray(start));
    return;
  }

  const fd = openSync(source, "r");
  try {
    let bytesRead: number;
    while ((bytesRead = readSync(fd, chunk, 0, chunk.length, null)) > 0) {
      let start = 0;
      while (start < bytesRead) {
        const newline = chunk.indexOf(0x0a, start);
        if (newline < 0 || newline >= bytesRead) {
          append(chunk, start, bytesRead);
          break;
        }
        append(chunk, start, newline);
        consumeLine();
        start = newline + 1;
      }
    }
    if (lineBytes > 0) consumeLine();
  } finally {
    closeSync(fd);
  }
}

function latestNativeContext(path: string): { meta: Record<string, unknown> | null; turn: Record<string, unknown> | null } {
  let meta: Record<string, unknown> | null = null;
  let turn: Record<string, unknown> | null = null;
  forEachJsonlLine(path, bytes => {
    const line = bytes.toString("utf8");
    if (!line.trim()) return;
    let row: { type?: unknown; payload?: unknown };
    try {
      row = JSON.parse(line);
    } catch {
      return;
    }
    if (row.type === "session_meta") meta = row.payload as Record<string, unknown>;
    if (row.type === "turn_context") turn = row.payload as Record<string, unknown>;
  });
  return { meta, turn };
}

export function codexRotationContract(path:string, generation:string, launchArgv:string[], pinnedModel:string|null, pinnedProfile:string|null):Record<string,unknown> {
  const { meta, turn } = latestNativeContext(path);
  if(!meta || !turn || (meta.id??meta.session_id)!==generation) throw new Error("Native generation evidence missing or mismatched");
  const provider=turn.model_provider??meta.model_provider;
  const model=turn.model;
  if(typeof provider!=="string" || typeof model!=="string" || !turn.sandbox_policy || typeof turn.approval_policy!=="string") throw new Error("Native provider/model/posture evidence unavailable");
  const index=launchArgv.findIndex(a=>a==="-p" || a==="--profile");
  const profile=index>=0?launchArgv[index+1]:null;
  if(!profile || profile!==pinnedProfile || model!==pinnedModel) throw new Error("Live launch profile or model differs from persistent successor pin");
  return {runtime:"codex",model,provider,profile,permissions:{sandbox:turn.sandbox_policy,approval:turn.approval_policy},effort:turn.effort??null};
}
