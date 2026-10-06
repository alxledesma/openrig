import {describe,it,expect,beforeEach} from "vitest";
import {makeCoordinatorRuntimeObserver, makePiNativeProver, type PiNativeProof} from "../src/domain/coordinator-runtime-availability.js";
const binding={id:"node",runtime:"codex",tmux_pane:"%4",tmux_session:"lead@pilot",generation_uuid:"generation-1"};
const stamp="Fri Oct 2 20:00:00 2026";
function probe(rows:string,options:{pane?:string;drift?:boolean;fail?:boolean}={}){
 let reads=0;
 const db={prepare:()=>({all:()=>[++reads>1&&options.drift?{...binding,generation_uuid:"generation-2"}:binding]})} as any;
 return makeCoordinatorRuntimeObserver(db,async()=>{if(options.fail)throw Error("server unavailable");return options.pane??"%4|100|0";},async()=>rows);
}
describe("strict runtime availability",()=>{
 it("registered live shell without native descendant proves absence",async()=>expect((await probe(`100 1 ${stamp} /bin/zsh`)("lead@pilot"))?.state).toBe("absent"));
 it.each(["-zsh","-bash","-sh","-fish"])("recognizes exact login shell %s in actual ps census",async shell=>{
  expect((await probe(`94536 94535 ${stamp} ${shell}`,{pane:"%17|94536|0"})("lead@pilot"))?.state).toBe("absent");
 });
 it.each(["--zsh","-codex","-python","-zsh-other"])("unknown executable remains unknown %s",async shell=>{
  expect(await probe(`94536 94535 ${stamp} ${shell}`,{pane:"%17|94536|0"})("lead@pilot")).toBeNull();
 });
 it("retained dead pane plus complete census proves absence",async()=>expect((await probe(`2 1 ${stamp} /bin/zsh`,{pane:"%4|100|1"})("lead@pilot"))?.state).toBe("absent"));
 it("native process is present regardless of idle/provider capacity",async()=>expect((await probe(`100 1 ${stamp} /bin/zsh\n101 100 ${stamp} /bin/codex`)("lead@pilot"))?.state).toBe("present"));
 it.each([{rows:""},{rows:"malformed"},{rows:`100 1 ${stamp} /bin/zsh\n101 100 ${stamp} /bin/node`},{rows:`2 1 ${stamp} /bin/zsh`},{rows:`100 1 ${stamp} /bin/python`},{rows:`100 1 ${stamp} /bin/zsh`,fail:true},{rows:`100 1 ${stamp} /bin/zsh`,drift:true}])("unknown evidence never excludes %j",async({rows,...options})=>expect(await probe(rows,options)("lead@pilot")).toBeNull());
 it("changing native census is not stable absence",async()=>{
 let calls=0;const db={prepare:()=>({all:()=>[binding]})} as any;
 const observe=makeCoordinatorRuntimeObserver(db,async()=>"%4|100|0",async()=>++calls===1?`100 1 ${stamp} /bin/zsh`:`100 1 ${stamp} /bin/zsh\n101 100 ${stamp} /bin/codex`);
 expect(await observe("lead@pilot")).toBeNull();
 });
});

describe("shared Pi native prover", () => {
  const row = { nodeId: "n", runtime: "pi", tmux_pane: "%4", tmux_session: null, generation_uuid: "lead-g1", resume_token: "/state/pi/lead@xv/sessions/s.json" };
  const runnerLine = "101 100 node /d/adapters/pi-runner.js --session-name lead@xv --launch-id L-77 --state-root /state/pi";
  const childLine = "102 101 /opt/homebrew/bin/node /x/@earendil-works/pi-coding-agent/dist/bundle/cli.js --mode rpc --session-dir /state/pi/lead@xv/sessions --name lead@xv --approve";
  const census = (child = childLine) => `100 1 /bin/zsh -l\n${runnerLine}\n${child}`;
  const env = (gen = "lead-g1") => `101 node pi-runner SECRET=never-logged OPENRIG_OCCUPANT_GENERATION=${gen}\n102 node cli.js OTHER=x OPENRIG_OCCUPANT_GENERATION=${gen}`;
  const sidecar = (over: Record<string, unknown> = {}) => JSON.stringify({ ready: true, launchId: "L-77", sessionFile: row.resume_token, updatedAt: "2026-01-01T00:00:00.000Z", ...over });
  function prover(opts: { rows?: unknown[]; exec?: () => Promise<string>; argvCensus?: () => Promise<string>; envProbe?: (p: number[]) => Promise<string>; sidecar?: string; procArgs?: (p: number[]) => Promise<Map<number, string | null>> } = {}) {
    const rows = opts.rows ?? [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    return makePiNativeProver(db, opts.exec ?? (async () => "%4|100|0"), {
      fs: { readFile: () => opts.sidecar ?? sidecar() }, piStateRoot: "/state/pi",
      // Hermetic default: behave like a non-Darwin host (kernel evidence
      // unavailable) so injected envProbe text decides, unless a case overrides.
      procArgs: opts.procArgs ?? (async () => new Map()),
      argvCensus: opts.argvCensus ?? (async () => census()), envProbe: opts.envProbe ?? (async () => env()), now: () => Date.parse("2026-10-05T12:00:00Z"),
    });
  }
  it("healthy IDLE runner with an old sidecar heartbeat proves present via launch-id instance binding", async () => {
    expect(await prover()("lead@xv")).toMatchObject({ state: "present", generation: "lead-g1", launchId: "L-77" });
  });
  it("fingerprint and receipts carry identifiers only, never environment text", async () => {
    const proof = await prover()("lead@xv");
    expect(JSON.stringify(proof)).not.toContain("never-logged");
  });
  it.each([
    ["sidecar launch-id differs from argv (replacement process)", { sidecar: sidecar({ launchId: "L-78" }) }],
    ["sidecar session token differs (changed history)", { sidecar: sidecar({ sessionFile: "/state/pi/lead@xv/sessions/other.json" }) }],
    ["Pi child runs outside the matched runner subtree", { argvCensus: async () => census("102 100 /opt/homebrew/bin/node /x/@earendil-works/pi-coding-agent/dist/bundle/cli.js --mode rpc") }],
    ["generation evidence missing on one selected process", { envProbe: async () => `101 node pi-runner OPENRIG_OCCUPANT_GENERATION=lead-g1\n102 node cli.js no-token-here` }],
    ["generation contradicts latest tenure", { envProbe: async () => env("lead-GHOST") }],
    ["census changes between double observations", { argvCensus: (() => { let n = 0; return async () => (n++ === 0 ? census() : census("103 102 /bin/sh")); })() }],
  ] as const)("%s stays unknown", async (_name, opts) => expect(await prover(opts)("lead@xv")).toBeNull());
  it("live runner and child under an exited sidecar marker stay UNKNOWN, never false absence", async () => {
    expect(await prover({ sidecar: sidecar({ exited: { code: 1, at: "2026-10-05T11:00:00.000Z" } }) })("lead@xv")).toBeNull();
  });
  it("live bare shell with no runner descendant positively proves absence", async () => {
    expect(await prover({ argvCensus: async () => "100 1 /bin/zsh -l" })("lead@xv")).toMatchObject({ state: "absent" });
  });
  it("a dead retained pane positively proves absence", async () => {
    expect(await prover({ exec: async () => "%4|100|1", argvCensus: async () => "100 1 /bin/cat something" })("lead@xv")).toMatchObject({ state: "absent" });
  });
  it("kernel process-args evidence proves seats whose ps env view was erased by title rewriting", async () => {
    const gens = new Map<number, string | null>([[101, "lead-g1"], [102, "lead-g1"]]);
    expect(await prover({ procArgs: async () => gens, envProbe: async () => { throw new Error("ps eww renders no env after setproctitle"); } })("lead@xv")).toMatchObject({ state: "present" });
  });
  it("kernel-confirmed absence is never contradicted by a stale ps token", async () => {
    const gens = new Map<number, string | null>([[101, "lead-g1"], [102, null]]);
    expect(await prover({ procArgs: async () => gens, envProbe: async () => env() })("lead@xv")).toBeNull();
  });
  it("unavailable kernel probe falls back to the legacy ps evidence", async () => {
    expect(await prover({ procArgs: async () => new Map() })("lead@xv")).toMatchObject({ state: "present" });
  });
  it("observer delegates Pi seats to the shared prover and stays unknown without one", async () => {
    const db = { prepare: () => ({ all: () => [{ id: "n", runtime: "pi", tmux_pane: "%4", tmux_session: null, generation_uuid: "lead-g1" }] }) } as never;
    const proof: PiNativeProof = { state: "present", generation: "lead-g1", launchId: "L-77", fingerprint: "{}" };
    expect(await makeCoordinatorRuntimeObserver(db, async () => "%4|100|0")( "lead@xv")).toBeNull();
    expect(await makeCoordinatorRuntimeObserver(db, async () => "%4|100|0", undefined, async () => proof)("lead@xv")).toMatchObject({ state: "present", generation: "lead-g1" });
    expect(await makeCoordinatorRuntimeObserver(db, async () => "%4|100|0", undefined, async () => null)("lead@xv")).toBeNull();
  });
});

// F1: every null exit of the Pi prover reports exactly one reduced reason code,
// and no reason ever carries argv, environment, path or pid text.
describe("pi prover typed UNKNOWN reasons (F1)", () => {
  const row = { nodeId: "n", runtime: "pi", tmux_pane: "%4", tmux_session: null, generation_uuid: "lead-g1", resume_token: "/state/pi/lead@xv/sessions/s.json" };
  const runnerLine = "101 100 node /d/adapters/pi-runner.js --session-name lead@xv --launch-id L-77 --state-root /state/pi";
  const childLine = "102 101 /opt/homebrew/bin/node /x/@earendil-works/pi-coding-agent/dist/bundle/cli.js --mode rpc --session-dir /state/pi/lead@xv/sessions --name lead@xv --approve";
  const census = (child = childLine) => `100 1 /bin/zsh -l\n${runnerLine}\n${child}`;
  const env = (gen = "lead-g1") => `101 node pi-runner SECRET=never-logged OPENRIG_OCCUPANT_GENERATION=${gen}\n102 node cli.js OTHER=x OPENRIG_OCCUPANT_GENERATION=${gen}`;
  const sidecar = (over: Record<string, unknown> = {}) => JSON.stringify({ ready: true, launchId: "L-77", sessionFile: row.resume_token, updatedAt: "2026-01-01T00:00:00.000Z", ...over });
  const seen: Array<unknown> = [];
  const calls: Array<unknown[]> = [];
  function prover(opts: { rows?: unknown[]; exec?: () => Promise<string>; argvCensus?: () => Promise<string>; envProbe?: (p: number[]) => Promise<string>; sidecar?: string; procArgs?: (p: number[]) => Promise<Map<number, string | null>> } = {}) {
    const rows = opts.rows ?? [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    return makePiNativeProver(db, opts.exec ?? (async () => "%4|100|0"), {
      fs: { readFile: () => opts.sidecar ?? sidecar() }, piStateRoot: "/state/pi",
      procArgs: opts.procArgs ?? (async () => new Map()),
      argvCensus: opts.argvCensus ?? (async () => census()), envProbe: opts.envProbe ?? (async () => env()),
      now: () => Date.parse("2026-10-05T12:00:00Z"),
      // Each diagnose CALL must carry exactly one reason; the double sample means
      // several calls per proof. Record one entry per call.
      diagnose: r => { calls.push(r); seen.push(...r); },
    });
  }
  beforeEach(() => { seen.length = 0; calls.length = 0; });

  const CODES = new Set(["binding_rows","no_pane_target","tmux_probe_failed","census_empty","census_invalid","duplicate_pids","runner_count","pi_child_count","sidecar_unreadable","sidecar_exited","sidecar_not_ready","sidecar_launch_mismatch","sidecar_session_file_mismatch","generation_unverified_runner","generation_unverified_child","unstable_between_samples","binding_changed","exception"]);
  const cases: Array<[string, Parameters<typeof prover>[0]]> = [
    ["no binding row", { rows: [] as unknown[] }],
    ["ambiguous binding rows", { rows: [row, row] }],
    ["no pane target", { rows: [{ ...row, tmux_pane: null, tmux_session: null }] }],
    ["tmux probe unparseable", { exec: async () => "not-a-pane" }],
    ["census empty", { argvCensus: async () => "" }],
    ["census invalid", { argvCensus: async () => "malformed line" }],
    ["duplicate pids", { argvCensus: async () => `${runnerLine}\n${runnerLine}` }],
    ["two runners", { argvCensus: async () => `${runnerLine}\n102 100 node /d/adapters/pi-runner.js --session-name lead@xv --launch-id L-77\n${childLine}` }],
    ["wrong pi child count", { argvCensus: async () => `${runnerLine}\n102 101 node pi\n103 101 node pi` }],
    ["sidecar unreadable", { sidecar: "{not json" }],
    ["sidecar exited", { sidecar: sidecar({ exited: { code: 1, at: "2026-10-05T11:00:00.000Z" } }) }],
    ["sidecar not ready", { sidecar: sidecar({ ready: false }) }],
    ["sidecar launch mismatch", { sidecar: sidecar({ launchId: "L-78" }) }],
    ["sidecar session file mismatch", { sidecar: sidecar({ sessionFile: "/state/pi/lead@xv/sessions/other.json" }) }],
    ["generation absent on runner", { envProbe: async () => `101 node pi-runner no-token\n102 node cli.js OPENRIG_OCCUPANT_GENERATION=lead-g1` }],
    ["generation absent on child", { envProbe: async () => `101 node pi-runner OPENRIG_OCCUPANT_GENERATION=lead-g1\n102 node cli.js no-token` }],
    ["generation contradicts tenure", { envProbe: async () => env("lead-GHOST") }],
    ["census changes between samples", { argvCensus: (() => { let n = 0; return async () => (n++ === 0 ? census() : census("103 102 /bin/sh")); })() }],
    ["exec throws", { exec: async () => { throw new Error("tmux exploded /state/pi/lead@xv SECRET=never-logged"); } }],
  ];
  it.each(cases)("%s yields null with exactly one reason per diagnose call", async (_name, opts) => {
    expect(await prover(opts)("lead@xv")).toBeNull();
    // The prover double-samples, so one proof may produce several diagnose calls.
    // EVERY call carries exactly ONE reason drawn from the closed code set.
    expect(calls.length).toBeGreaterThan(0);
    for (const call of calls) expect(call).toHaveLength(1);
    for (const r of seen) expect(CODES.has((r as { code: string }).code)).toBe(true);
  });

  it("no reason ever carries argv, environment, path or pid text", async () => {
    const cases: Array<Parameters<typeof prover>[0]> = [
      { rows: [] }, { exec: async () => "not-a-pane" }, { argvCensus: async () => "malformed line" },
      { sidecar: sidecar({ launchId: "L-78" }) }, { envProbe: async () => `101 node pi-runner SECRET=never-logged no-token\n102 node cli.js OPENRIG_OCCUPANT_GENERATION=lead-g1` },
      { exec: async () => { throw new Error("boom SECRET=never-logged /state/pi/lead@xv pid 4242"); } },
    ];
    for (const opts of cases) {
      seen.length = 0;
      await prover(opts)("lead@xv");
      const text = JSON.stringify(seen);
      expect(seen.length).toBeGreaterThan(0);
      expect(text).not.toContain("SECRET");
      expect(text).not.toContain("never-logged");
      expect(text).not.toContain("/state/pi");
      expect(text).not.toContain("pi-runner.js");
      expect(text).not.toContain("4242");
      expect(text).not.toContain("101");
      expect(text).not.toContain("boom");
    }
  });

  it("the exception reason carries only an error class token, never the message", async () => {
    await prover({ exec: async () => { throw new Error("SENSITIVE detail here"); } })("lead@xv");
    expect(seen.length).toBeGreaterThan(0);
    for (const r of seen) expect(r).toEqual({ code: "exception", detail: "error" });
    expect(JSON.stringify(seen)).not.toContain("SENSITIVE");
  });

  it("a healthy proof emits no reason at all", async () => {
    expect(await prover()("lead@xv")).toMatchObject({ state: "present" });
    expect(seen).toHaveLength(0);
  });

  it("a positive absence also emits no reason", async () => {
    expect(await prover({ argvCensus: async () => "100 1 /bin/zsh -l" })("lead@xv")).toMatchObject({ state: "absent" });
    expect(seen).toHaveLength(0);
  });
});

// ── Native quiescence credit (evidence only) ────────────────────────────────
// The prover keeps proving IDENTITY exactly as before; only the idle claim is
// withheld. Missing, old or malformed metadata must degrade to UNKNOWN and
// never become idle credit.

describe("pi prover native quiescence credit", () => {
  const row = { nodeId: "n", runtime: "pi", tmux_pane: "%4", tmux_session: null, generation_uuid: "lead-g1", resume_token: "/state/pi/lead@xv/sessions/s.json" };
  const runnerLine = "101 100 node /d/adapters/pi-runner.js --session-name lead@xv --launch-id L-77 --state-root /state/pi";
  const childLine = "102 101 /opt/homebrew/bin/node /x/@earendil-works/pi-coding-agent/dist/bundle/cli.js --mode rpc --session-dir /state/pi/lead@xv/sessions --name lead@xv --approve";
  const census = () => `100 1 /bin/zsh -l\n${runnerLine}\n${childLine}`;
  const env = (gen = "lead-g1") => `101 node pi-runner SECRET=never-logged OPENRIG_OCCUPANT_GENERATION=${gen}\n102 node cli.js OTHER=x OPENRIG_OCCUPANT_GENERATION=${gen}`;
  const observedAt = "2026-10-05T11:59:59.000Z";
  /** A sidecar carrying a well-formed, fully bound quiescence record. */
  const settledSidecar = (over: Record<string, unknown> = {}) => JSON.stringify({
    ready: true, launchId: "L-77", sessionFile: row.resume_token, lastEntryId: "e-9", updatedAt: "2026-01-01T00:00:00.000Z",
    quiescence: { launchId: "L-77", generation: "lead-g1", sessionFile: row.resume_token, lastEntryId: "e-9", settled: true, observedAt },
    ...over,
  });
  const oldSidecar = () => JSON.stringify({ ready: true, launchId: "L-77", sessionFile: row.resume_token, updatedAt: "2026-01-01T00:00:00.000Z" });
  function prover(opts: { sidecar?: string; rows?: unknown[]; envProbe?: (p: number[]) => Promise<string>; argvCensus?: () => Promise<string> } = {}) {
    const rows = opts.rows ?? [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    return makePiNativeProver(db, async () => "%4|100|0", {
      fs: { readFile: () => opts.sidecar ?? settledSidecar() }, piStateRoot: "/state/pi",
      procArgs: async () => new Map(),
      argvCensus: opts.argvCensus ?? (async () => census()), envProbe: opts.envProbe ?? (async () => env()),
      now: () => Date.parse("2026-10-05T12:00:00Z"),
    });
  }

  it("a valid proof with exact current bindings retains settled true", async () => {
    expect(await prover()("lead@xv")).toMatchObject({ state: "present", generation: "lead-g1", launchId: "L-77", quiescence: { settled: true, observedAt } });
  });

  it("an OLD sidecar written before this field existed is identity-proven but UNKNOWN", async () => {
    const proof = await prover({ sidecar: oldSidecar() })("lead@xv");
    expect(proof).toMatchObject({ state: "present", launchId: "L-77", quiescence: { settled: null, observedAt: null } });
    expect(proof!.quiescence!.settled).not.toBe(true);
  });

  it.each([
    ["metadata absent", { quiescence: undefined }],
    ["settled missing", { quiescence: { launchId: "L-77", sessionFile: "/s", lastEntryId: "e-9", observedAt } }],
    ["settled not a boolean", { quiescence: { launchId: "L-77", sessionFile: "/s", lastEntryId: "e-9", settled: "yes", observedAt } }],
    ["observedAt missing", { quiescence: { launchId: "L-77", sessionFile: "/s", lastEntryId: "e-9", settled: true } }],
    ["quiescence not an object", { quiescence: "idle" }],
    ["quiescence is null", { quiescence: null }],
  ])("malformed metadata (%s) yields UNKNOWN, never idle", async (_label, over) => {
    const proof = await prover({ sidecar: settledSidecar(over) })("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null, observedAt: null } });
  });

  it("a launch-id drift refuses idle credit while still proving identity", async () => {
    const proof = await prover({ sidecar: settledSidecar({ quiescence: { launchId: "L-OLD", generation: "lead-g1", sessionFile: row.resume_token, lastEntryId: "e-9", settled: true, observedAt } }) })("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null } });
  });

  it("a generation drift refuses idle credit", async () => {
    const proof = await prover({ sidecar: settledSidecar({ quiescence: { launchId: "L-77", generation: "lead-g0", sessionFile: row.resume_token, lastEntryId: "e-9", settled: true, observedAt } }) })("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null } });
  });

  it("a native session-file drift refuses idle credit", async () => {
    const proof = await prover({ sidecar: settledSidecar({ quiescence: { launchId: "L-77", generation: "lead-g1", sessionFile: "/state/pi/lead@xv/sessions/other.json", lastEntryId: "e-9", settled: true, observedAt } }) })("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null } });
  });

  it("a cursor that moved past the recorded one refuses idle credit", async () => {
    const proof = await prover({ sidecar: settledSidecar({ quiescence: { launchId: "L-77", generation: "lead-g1", sessionFile: row.resume_token, lastEntryId: "e-4", settled: true, observedAt } }) })("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null } });
  });

  it("a genuine busy claim is carried honestly as settled false, not as unknown", async () => {
    const proof = await prover({ sidecar: settledSidecar({ quiescence: { launchId: "L-77", generation: "lead-g1", sessionFile: row.resume_token, lastEntryId: "e-9", settled: false, observedAt } }) })("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: false, observedAt } });
  });

  it("absence and every UNKNOWN exit carry no idle credit at all", async () => {
    expect((await prover({ argvCensus: async () => "100 1 /bin/zsh -l" })("lead@xv"))!.quiescence).toBeUndefined();
    const retired = await prover({ sidecar: settledSidecar({ exited: { code: 1, at: observedAt } }) })("lead@xv");
    expect(retired).toBeNull();
    const drifted = await prover({ envProbe: async () => env("lead-g0") })("lead@xv");
    expect(drifted).toBeNull();
  });

  it("quiescence credit never weakens the fingerprint double-sampling or reason vocabulary", async () => {
    const reasons: Array<unknown> = [];
    const rows = [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    const p = makePiNativeProver(db, async () => "%4|100|0", {
      fs: { readFile: () => settledSidecar() }, piStateRoot: "/state/pi", procArgs: async () => new Map(),
      argvCensus: async () => census(), envProbe: async () => env(), now: () => Date.parse("2026-10-05T12:00:00Z"),
      diagnose: r => reasons.push(...r),
    });
    expect(await p("lead@xv")).toMatchObject({ state: "present", quiescence: { settled: true } });
    expect(reasons).toHaveLength(0);
    expect(JSON.stringify(await p("lead@xv"))).not.toMatch(/pi-coding-agent|OPENRIG_OCCUPANT_GENERATION/);
  });
});

// ── R2 review findings: required bindings and a valid timestamp ─────────────

describe("pi prover requires complete bindings and a valid instant", () => {
  const row = { nodeId: "n", runtime: "pi", tmux_pane: "%4", tmux_session: null, generation_uuid: "lead-g1", resume_token: "/state/pi/lead@xv/sessions/s.json" };
  const runnerLine = "101 100 node /d/adapters/pi-runner.js --session-name lead@xv --launch-id L-77 --state-root /state/pi";
  const childLine = "102 101 /opt/homebrew/bin/node /x/@earendil-works/pi-coding-agent/dist/bundle/cli.js --mode rpc --session-dir /state/pi/lead@xv/sessions --name lead@xv --approve";
  const census = () => `100 1 /bin/zsh -l\n${runnerLine}\n${childLine}`;
  const env = () => `101 node pi-runner SECRET=never-logged OPENRIG_OCCUPANT_GENERATION=lead-g1\n102 node cli.js OTHER=x OPENRIG_OCCUPANT_GENERATION=lead-g1`;
  const observedAt = "2026-10-05T11:59:59.000Z";
  const bound = (over: Record<string, unknown> = {}) => JSON.stringify({
    ready: true, launchId: "L-77", sessionFile: row.resume_token, lastEntryId: "e-9", updatedAt: "2026-01-01T00:00:00.000Z",
    quiescence: { launchId: "L-77", generation: "lead-g1", sessionFile: row.resume_token, lastEntryId: "e-9", settled: true, observedAt, ...over },
  });
  function prover(sidecar: string) {
    const rows = [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    return makePiNativeProver(db, async () => "%4|100|0", {
      fs: { readFile: () => sidecar }, piStateRoot: "/state/pi", procArgs: async () => new Map(),
      argvCensus: async () => census(), envProbe: async () => env(), now: () => Date.parse("2026-10-05T12:00:00Z"),
    });
  }

  it.each([
    ["generation absent", { generation: undefined }],
    ["generation empty", { generation: "" }],
    ["generation wrong", { generation: "lead-g2" }],
    ["launchId absent", { launchId: undefined }],
    ["sessionFile absent", { sessionFile: undefined }],
  ])("a %s record earns no idle credit even with settled true", async (_label, over) => {
    const proof = await prover(bound(over))("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null, observedAt: null } });
  });

  it.each([
    ["not a timestamp", "yesterday"],
    ["date only", "2026-10-05"],
    ["empty", ""],
    ["non-string", 1757000000000],
    ["month 13", "2026-13-05T11:59:59.000Z"],
    ["impossible day", "2026-02-30T11:59:59.000Z"],
  ])("an invalid observedAt (%s) yields UNKNOWN, never idle", async (_label, bad) => {
    const proof = await prover(bound({ observedAt: bad }))("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: null, observedAt: null } });
  });

  it.each([
    ["UTC Z", "2026-10-05T11:59:59.000Z"],
    ["no fractional seconds", "2026-10-05T11:59:59Z"],
    ["explicit offset", "2026-10-05T21:59:59+10:00"],
  ])("a valid instant (%s) keeps its idle credit", async (_label, good) => {
    const proof = await prover(bound({ observedAt: good }))("lead@xv");
    expect(proof).toMatchObject({ state: "present", quiescence: { settled: true, observedAt: good } });
  });

  it("a fully bound current record still earns idle credit after the tightening", async () => {
    expect(await prover(bound())("lead@xv")).toMatchObject({ state: "present", generation: "lead-g1", quiescence: { settled: true } });
  });

  it("tightened bindings never turn a proof into an UNKNOWN exit or a reason", async () => {
    const reasons: Array<unknown> = [];
    const rows = [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    const p = makePiNativeProver(db, async () => "%4|100|0", {
      fs: { readFile: () => bound({ generation: "lead-g2" }) }, piStateRoot: "/state/pi", procArgs: async () => new Map(),
      argvCensus: async () => census(), envProbe: async () => env(), now: () => Date.parse("2026-10-05T12:00:00Z"),
      diagnose: r => reasons.push(...r),
    });
    const proof = await p("lead@xv");
    expect(proof).toMatchObject({ state: "present", launchId: "L-77" });
    expect(proof!.quiescence!.settled).toBeNull();
    expect(reasons).toHaveLength(0);
  });
});
