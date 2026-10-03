#!/usr/bin/env node
// OpenRig Claude Status Line Context Collector
// Reads Claude status line JSON from stdin, extracts context window data,
// and writes atomically to a sidecar file.
//
// Usage: node claude-statusline-context.js <context-output-path-or-dir> [provider-usage-dir]

const fs = require("fs");
const path = require("path");

const outputTarget = process.argv[2];
const providerUsageTarget = process.argv[3];
if (!outputTarget) {
  process.exit(0); // No output path — silently exit
}

function logFailure(message) {
  // Never include exception text: parse and filesystem errors can contain input or private paths.
  console.error(`[openrig][collector] ${message}`);
}

let input = "";
process.stdin.setEncoding("utf-8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  try {
    const raw = JSON.parse(input);
    const contextWindow = raw.context_window;
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("invalid payload");
    const sampledAt = new Date().toISOString();
    const generation = process.env.OPENRIG_OCCUPANT_GENERATION || process.env.RIGGED_OCCUPANT_GENERATION;
    const sample = {
      ...(contextWindow ? { context_window: {
        context_window_size: contextWindow.context_window_size ?? null,
        used_percentage: contextWindow.used_percentage ?? null,
        remaining_percentage: contextWindow.remaining_percentage ?? null,
        total_input_tokens: contextWindow.total_input_tokens ?? null,
        total_output_tokens: contextWindow.total_output_tokens ?? null,
        current_usage: contextWindow.current_usage ?? null,
      } } : {}),
      runtime_metadata: runtimeMetadata(raw, generation, sampledAt),
      session_id: raw.session_id ?? null,
      session_name: raw.session_name ?? null,
      occupant_generation: process.env.OPENRIG_OCCUPANT_GENERATION || process.env.RIGGED_OCCUPANT_GENERATION || null,
      transcript_path: raw.transcript_path ?? null,
      sampled_at: sampledAt,
    };

    const outputPath = resolveOutputPath(outputTarget, raw);
    if (!outputPath) {
      logFailure("could not resolve output path from Claude status line payload");
      process.exit(0);
    }

    writeJsonAtomic(outputPath, sample);

    if (providerUsageTarget) {
      const providerUsagePath = resolveOutputPath(providerUsageTarget, raw);
      if (!providerUsagePath) {
        logFailure("could not resolve provider_usage output path from Claude status line payload");
        process.exit(0);
      }
      const rateLimits = normalizeRateLimits(raw.rate_limits);
      const providerUsage = {
        seatSession: raw.session_name || raw.session_id,
        asOf: new Date().toISOString(),
        ...(rateLimits ? { accountKind: "subscription", rateLimits } : {}),
      };
      writeJsonAtomic(providerUsagePath, providerUsage);
    }
  } catch (error) {
    logFailure("failed to collect Claude status line");
    process.exit(0);
  }
});

// Versioned receipt fields are closed; legacy sidecar fields are not public receipts.
function validated(value, present, validate) {
  if (!present) return { value: null, availability: "unavailable", reason: "missing" };
  if (value === null) return { value: null, availability: "unavailable", reason: "null" };
  if (typeof value !== "string" || value.length === 0 || value.length > 256 || /[\x00-\x1f\x7f]/.test(value)) return { value: null, availability: "unavailable", reason: "malformed" };
  if (!validate(value)) return { value: null, availability: "unavailable", reason: "unsupported" };
  return { value, availability: "available", reason: "observed" };
}
function childField(raw, parent, key, validate) {
  if (!Object.hasOwn(raw, parent)) return validated(undefined, false, validate);
  if (raw[parent] === null) return validated(null, true, validate);
  if (typeof raw[parent] !== "object" || Array.isArray(raw[parent])) return validated(0, true, validate);
  return validated(raw[parent][key], Object.hasOwn(raw[parent], key), validate);
}
function runtimeMetadata(raw, generation, sampledAt) {
  const uuid = value => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
  const receipt = {
    schema_version: 1,
    source: "claude_statusline_json",
    sampled_at: sampledAt,
    session_id: validated(raw.session_id, Object.hasOwn(raw, "session_id"), uuid),
    model_id: childField(raw, "model", "id", value => value.length <= 128 && /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/.test(value)),
    effort_level: childField(raw, "effort", "level", value => ["low", "medium", "high", "xhigh", "max"].includes(value)),
    occupant_generation: { ...validated(generation, generation !== undefined, uuid), provenance: "inherited_environment" },
  };
  if (Object.hasOwn(raw, "session_name")) receipt.session_name = validated(raw.session_name, true, value => /^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+$/.test(value));
  return receipt;
}

function resolveOutputPath(target, raw) {
  if (target.endsWith(".json")) {
    return target;
  }

  const sessionKey = raw.session_name || raw.session_id;
  if (!sessionKey) {
    return null;
  }

  const safe = String(sessionKey).replace(/[^a-zA-Z0-9@._-]/g, "_");
  return path.join(target, safe + ".json");
}

function normalizeRateLimits(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const result = {};
  for (const key of ["five_hour", "seven_day"]) {
    const window = value[key];
    if (!window || typeof window !== "object" || Array.isArray(window)) continue;
    const resetsAt = typeof window.resets_at === "number"
      ? unixSecondsToIso(window.resets_at)
      : window.resets_at;
    if (typeof window.used_percentage !== "number" || !Number.isFinite(window.used_percentage)
      || typeof resetsAt !== "string") continue;
    result[key] = { usedPercent: window.used_percentage, resetsAt };
  }
  return result.five_hour || result.seven_day ? result : null;
}

function unixSecondsToIso(value) {
  if (!Number.isFinite(value)) return null;
  const date = new Date(value * 1000);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function writeJsonAtomic(outputPath, value) {
  const tmpPath = outputPath + ".tmp";
  const dir = path.dirname(outputPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  fs.writeFileSync(tmpPath, JSON.stringify(value), "utf-8");
  fs.renameSync(tmpPath, outputPath);
}
