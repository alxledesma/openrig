// Bounded live-projection recovery verb: correct a FALSELY DETACHED projection
// of the SAME still-live native occupant without reconcile-session's tenure
// minting/adoption. Identity comes from the stamped seat env (DaemonClient
// chokepoint); the daemon re-asserts current Kernel Operator authority, probes
// genuine native identity per runtime (codex/claude token-bound process
// lineage; pi runner typed sidecar bound to the stored session-file token),
// fences against reservations/guarded delivery, and writes only the original
// row's status plus one durable receipt/event. Never sends input, launches,
// kills, adopts, or mints generations.

import { randomUUID } from "node:crypto";
import { Command } from "commander";
import { DaemonClient, terminalAuthHeaders } from "../client.js";
import { getDaemonStatus, getDaemonUrl, daemonStatusGuard } from "../daemon-lifecycle.js";
import { realDeps } from "./daemon.js";
import type { StatusDeps } from "./status.js";

interface RecoveryResponse { ok?: boolean; code?: string; message?: string; receipt?: Record<string, unknown>; error?: string }

export function recoverLiveProjectionCommand(depsOverride?: StatusDeps): Command {
  const cmd = new Command("recover-live-projection")
    .description("Recover a falsely detached live native seat projection WITHOUT minting a generation (no launch, no input, no adoption)");
  const getDeps = () => depsOverride ?? { lifecycleDeps: realDeps(), clientFactory: (url: string) => new DaemonClient(url) };

  cmd
    .argument("<session>", "Canonical session name of the falsely detached seat")
    .requiredOption("--session-id <id>", "Original sessions row id (must be the node's latest row)")
    .requiredOption("--node <nodeId>", "Persisted node id owning that row")
    .requiredOption("--generation <uuid>", "Expected CURRENT occupant generation uuid")
    .option("--operation <id>", "Exact idempotent operation id (generated when omitted)")
    .option("--json", "JSON output for agents")
    .addHelpText("after", `
Examples:
  rig recover-live-projection lead-seat@my-rig --session-id 01ABC --node 01DEF --generation <uuid> --json

Use ONLY when markDetached fired while the same native occupant is in fact
live. Unlike reconcile-session this never appends an occupant generation and
never adopts: it proves same-native identity through the runtime's own fresh
observation (process lineage tokens / pi runner typed state) and refuses on any
missing, stale, mismatched, fenced, or drifting custody. Replay of the exact
same --operation returns the stored receipt; a different payload under one
operation id is refused. Absent/unknown proof outcomes are reported honestly —
nothing is retried blindly.`)
    .action(async (session: string, opts: { sessionId: string; node: string; generation: string; operation?: string; json?: boolean }) => {
      const deps = getDeps();
      const status = await getDaemonStatus(deps.lifecycleDeps);
      if (!daemonStatusGuard(status)) return;
      const client = deps.clientFactory(getDaemonUrl(status));
      const res = await client.post<RecoveryResponse>(
        `/api/sessions/${encodeURIComponent(session)}/recover-live-projection`,
        { operationId: opts.operation ?? randomUUID(), sessionId: opts.sessionId, nodeId: opts.node, sessionName: session, expectedGeneration: opts.generation },
        { headers: terminalAuthHeaders() },
      );
      const body = res.data ?? { ok: false, code: "transport_error", message: `request failed (HTTP ${res.status})` };
      if (opts.json) console.log(JSON.stringify(body));
      else if (body.ok) console.log(`Recovered ${session}: receipt persisted; no generation minted, no input sent.`);
      else console.error(`Refused: ${body.code ?? "unknown"} — ${body.message ?? `HTTP ${res.status}`}`);
      if (!body.ok || res.status >= 400) process.exitCode = 1;
    });
  return cmd;
}
