import { setTimeout as sleep } from "node:timers/promises";
import type { TmuxAdapter } from "./tmux.js";
import type { SeatLaunchEnvironment } from "../domain/seat-launch-environment.js";
import type { ResumeResult } from "./claude-resume.js";
import { assessNativeResumeProbe, buildCodexResumeCore, buildCodexResumeArgs } from "../domain/native-resume-probe.js";
import { runSyncSite } from "../domain/sync-site-wrap.js";
import { shellQuote } from "./shell-quote.js";
import { codexPostureArg, codexPostureArgs } from "./yolo-mode.js";
import { observeCodexSandbox } from "../domain/permission-drift.js";
import { unknownDaemonSupportMessage, type CodexDaemonSupportDetector } from "../domain/codex-daemon-support.js";
import { codexNetworkDefaultArgs, type CodexNetworkDefaultReader } from "../domain/codex-network-default.js";

const CODEX_TYPES = new Set(["codex_id", "codex_last"]);
const SHELL_COMMANDS = new Set(["bash", "fish", "nu", "sh", "tmux", "zsh"]);

export { type ResumeResult };
/** Parse ONLY the leading KEY=VALUE assignment block of a composed launch command
 * using the actual supported emitter grammar (shellQuote single-quote wrapping with
 * '"'"' escapes, mixed quoted/unquoted segments, whole-field 'K=V' form). Quote-aware:
 * whitespace INSIDE quotes never splits a field; the block ends at the first complete
 * non-assignment token. Text embedded in another variable's value can therefore never
 * masquerade as a binding, and paths containing spaces parse correctly. */
function takeAssignment(s: string): { key: string; value: string; rest: string } | null {
  let i = s.match(/^\s+/)?.[0].length ?? 0;
  let key = "", value = "", inValue = false;
  while (i < s.length) {
    const c = s[i]!;
    if (c === "'" || c === '"') {
      const q = c; i++; let seg = "";
      while (i < s.length && s[i] !== q) { seg += s[i]!; i++; }
      if (i >= s.length) return null; // unterminated quote: refuse the block here
      i++;
      if (!inValue && seg.includes("=")) { const [k, ...r] = seg.split("="); key += k!; value += r.join("="); inValue = true; }
      else if (inValue) value += seg; else key += seg;
      continue;
    }
    if (/\s/.test(c)) break;
    if (c === "=" && !inValue) { inValue = true; i++; continue; }
    if (inValue) value += c; else key += c;
    i++;
  }
  if (!inValue || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) return null;
  return { key, value, rest: s.slice(i).replace(/^\s+/, "") };
}
export function leadingEnvAssignments(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  let rest = text.startsWith("/usr/bin/env ") ? text.slice(13) : text.startsWith("env ") ? text.slice(4) : text;
  for (;;) {
    const a = takeAssignment(rest);
    if (!a) break;
    out[a.key] = a.value;
    if (!a.rest) break;
    rest = a.rest;
  }
  return out;
}

interface CodexResumeOptions {
  seatLaunchEnvironment?: SeatLaunchEnvironment;
  launchPath?: string;
  codexHome?: string;
  pollMs?: number;
  maxWaitMs?: number;
  sleep?: (ms: number) => Promise<void>;
  exec?: (cmd: string) => Promise<string>;
  /** #69: whether the installed Codex supports --no-daemon; absent keeps the existing invocation. */
  detectDaemonSupport?: CodexDaemonSupportDetector;
  /** #275: Codex's own answer on the plain floor's network default; absent keeps the existing invocation. */
  readNetworkDefault?: CodexNetworkDefaultReader;
}

export class CodexResumeAdapter {
  constructor(
    private tmux: TmuxAdapter,
    private options: CodexResumeOptions = {}
  ) {}

  canResume(resumeType: string | null, resumeToken: string | null): boolean {
    if (!resumeType || !CODEX_TYPES.has(resumeType)) return false;
    // codex_last does not need a token
    if (resumeType === "codex_last") return true;
    // codex_id needs a token
    if (!resumeToken) return false;
    return true;
  }

  async resume(
    tmuxSessionName: string,
    resumeType: string | null,
    resumeToken: string | null,
    cwd: string,
    codexConfigProfile?: string | null,
    // OPR.0.4.8.3 Seam B: persisted resolved posture threaded from restore.
    resolvedPosture?: "floor" | "full_bypass",
    // 0.5.2-07: the seat's SPEC-pinned model. TRAILING param so existing positional callers that pass
    // resolvedPosture as the 6th arg stay correct; threaded so the legacy (non-pod-aware) restore boots
    // the resumed seat on its spec model, not the runtime default; absent → command byte-identical.
    model?: string | null,
    // #75: optional reasoning effort for the seat.
    effort?: string | null,
    // S-A (ROOT-CODEX-GENERATION-ARCHITECTURE): ledger-owned generation for a
    // continuation relaunch. When set it overrides the session-environment copy that
    // handover deliberately leaves stale; absent = byte-identical prior behavior.
    ledgerGeneration?: string,
  ): Promise<ResumeResult> {
    if (!this.canResume(resumeType, resumeToken)) {
      return { ok: false, code: "no_resume", message: "Codex resume not available" };
    }

    if (codexConfigProfile?.trim()) {
      const { verifyCodexProfileLoads } = await import("../domain/codex-profile-preflight.js");
      const execFn = this.options.exec ?? (async (cmd: string) => {
        const { execSync } = await import("node:child_process");
        return runSyncSite("codex.resume.profile_preflight", () =>
          execSync(cmd, { encoding: "utf-8", stdio: ["pipe", "pipe", "pipe"], timeout: 10_000,
            ...(this.options.codexHome ? { cwd, env: { ...process.env, CODEX_HOME: this.options.codexHome, ...(this.options.launchPath ? { PATH: this.options.launchPath } : {}) } } : {}),
          })
        );
      });
      const probeResult = await verifyCodexProfileLoads(codexConfigProfile, execFn, undefined, this.options.codexHome);
      if (!probeResult.ok) {
        return {
          ok: false,
          code: "resume_failed",
          message: `Profile preflight failed: ${probeResult.error}${probeResult.migrationHint ? `\n  Fix: ${probeResult.migrationHint}` : ""}`,
        };
      }
    }

    // #69: detect for the Codex the restored pane runs (its cwd, the launch PATH).
    const daemonSupport = this.options.detectDaemonSupport ? await this.options.detectDaemonSupport(cwd) : undefined;
    if (daemonSupport?.kind === "unknown") {
      return { ok: false, code: "resume_failed", message: unknownDaemonSupportMessage(daemonSupport.detail) };
    }

    const profileArg = codexConfigProfile ? ` -p ${shellQuote(codexConfigProfile)}` : "";
    const postureArg = codexPostureArg(profileArg, process.env, resolvedPosture);
    const postureArgs = codexPostureArgs(codexConfigProfile, process.env, resolvedPosture);
    const appliedLaunch = observeCodexSandbox(postureArg);
    const networkArgs = await codexNetworkDefaultArgs(this.options.readNetworkDefault, appliedLaunch, cwd, tmuxSessionName);
    const networkArg = networkArgs.length ? ` -c ${shellQuote(networkArgs[1]!)}` : "";
    const cmd = buildCodexResumeCore(
      resumeToken ?? "",
      codexConfigProfile,
      resumeType === "codex_last",
      undefined,
      resolvedPosture,
      model,
      `${postureArg}${networkArg}`,
      daemonSupport?.kind === "supported",
      effort,
    );

    const launchEnv = [this.options.launchPath ? `PATH=${shellQuote(this.options.launchPath)}` : "", this.options.codexHome ? `CODEX_HOME=${shellQuote(this.options.codexHome)}` : ""].filter(Boolean);
    let textTarget: string;
    try {
      const env = this.options.seatLaunchEnvironment;
      textTarget = env && await env.usesNativeDuty?.(tmuxSessionName)
        ? await env.structuredCommand(tmuxSessionName, { executable: "codex", cwd,
          args: buildCodexResumeArgs({ resumeToken: resumeToken ?? "", useLast: resumeType === "codex_last",
            postureArgs, networkArgs, model, effort, daemonOptOut: daemonSupport?.kind === "supported" }) },
          { runtime: "codex", generation: ledgerGeneration })
        : env ? await env.command(tmuxSessionName, cmd, { codexCwd: cwd, runtime: "codex", ...(ledgerGeneration !== undefined ? { generation: ledgerGeneration } : {}) })
          : launchEnv.length ? `env ${launchEnv.join(" ")} ${cmd}` : cmd;
    } catch { return { ok: false, code: "resume_failed", message: "Native duty launch composition refused" }; }
    // Structured binding proof (R3-F2): only LEADING environment assignments count —
    // a generation string inside command arguments proves nothing about the child env,
    // and the composer's best-effort fallback carries none at all.
    if (ledgerGeneration !== undefined && leadingEnvAssignments(textTarget).OPENRIG_OCCUPANT_GENERATION !== ledgerGeneration)
      return { ok: false, code: "resume_failed", message: "continuation_generation_composition_unverified: refusing to launch without the ledger generation bound in the composed environment" };
    const textResult = await this.tmux.sendShellCommand(tmuxSessionName, textTarget);
    if (!textResult.ok) {
      return { ok: false, code: "resume_failed", message: textResult.message };
    }

    const result = await this.verifyResume(tmuxSessionName);
    return result.ok ? { ...result, appliedLaunch } : result;
  }

  // Mirrors ClaudeResumeAdapter.verifyResume: poll the pane, run the native
  // probe, return resumed / retry_fresh / attention_required / resume_failed
  // based on observable runtime state. The `attention_required` outcome
  // (Codex auth refusal — stored OAuth token can no longer be refreshed)
  // closes the deferral recorded by the lifecycle scenario matrix slice.
  private async verifyResume(tmuxSessionName: string): Promise<ResumeResult> {
    const pollMs = this.options.pollMs ?? 200;
    const maxWaitMs = this.options.maxWaitMs ?? 5_000;
    const sleepFn = this.options.sleep ?? sleep;
    const attempts = Math.max(1, Math.floor(maxWaitMs / Math.max(pollMs, 1)) + 1);

    for (let attempt = 0; attempt < attempts; attempt++) {
      const paneCommand = await this.tmux.getPaneCommand(tmuxSessionName);
      const paneContent = (await this.tmux.capturePaneContent(tmuxSessionName, 40)) ?? "";
      const probe = assessNativeResumeProbe({
        runtime: "codex",
        paneCommand,
        paneContent,
      });

      if (probe.code === "no_saved_session") {
        return {
          ok: false,
          code: "retry_fresh",
          message: "Codex resume failed: no saved session found for the requested token",
        };
      }

      // Codex auth-refusal is alive-but-recoverable: the stored access token
      // can no longer be refreshed. Surface evidence (last 12 pane lines) so
      // the operator/UI can decide whether to `codex login` and continue, or
      // mark the seat permanently rebuilt. Mirror Claude's evidence shape.
      if (probe.status === "attention_required") {
        return {
          ok: false,
          code: "attention_required",
          message: probe.detail,
          evidence: paneContent.split("\n").slice(-12).join("\n"),
        };
      }

      if (probe.status === "resumed") {
        return { ok: true };
      }

      if (attempt < attempts - 1) {
        await sleepFn(pollMs);
      }
    }

    const finalCommand = await this.tmux.getPaneCommand(tmuxSessionName);
    const finalContent = (await this.tmux.capturePaneContent(tmuxSessionName, 40)) ?? "";
    const finalProbe = assessNativeResumeProbe({
      runtime: "codex",
      paneCommand: finalCommand,
      paneContent: finalContent,
    });

    if (finalProbe.status === "resumed") {
      return { ok: true };
    }

    if (finalCommand && SHELL_COMMANDS.has(finalCommand)) {
      return {
        ok: false,
        code: "retry_fresh",
        message: "Codex resume failed: pane returned to shell instead of entering Codex",
      };
    }

    return {
      ok: false,
      code: "resume_failed",
      message: "Codex resume failed: timed out waiting for Codex to become active",
    };
  }
}
