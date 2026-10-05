import {describe,it,expect} from "vitest";
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
  function prover(opts: { rows?: unknown[]; exec?: () => Promise<string>; argvCensus?: () => Promise<string>; envProbe?: (p: number[]) => Promise<string>; sidecar?: string } = {}) {
    const rows = opts.rows ?? [row];
    const db = { prepare: () => ({ all: () => rows }) } as never;
    return makePiNativeProver(db, opts.exec ?? (async () => "%4|100|0"), {
      fs: { readFile: () => opts.sidecar ?? sidecar() }, piStateRoot: "/state/pi",
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
  it("observer delegates Pi seats to the shared prover and stays unknown without one", async () => {
    const db = { prepare: () => ({ all: () => [{ id: "n", runtime: "pi", tmux_pane: "%4", tmux_session: null, generation_uuid: "lead-g1" }] }) } as never;
    const proof: PiNativeProof = { state: "present", generation: "lead-g1", launchId: "L-77", fingerprint: "{}" };
    expect(await makeCoordinatorRuntimeObserver(db, async () => "%4|100|0")( "lead@xv")).toBeNull();
    expect(await makeCoordinatorRuntimeObserver(db, async () => "%4|100|0", undefined, async () => proof)("lead@xv")).toMatchObject({ state: "present", generation: "lead-g1" });
    expect(await makeCoordinatorRuntimeObserver(db, async () => "%4|100|0", undefined, async () => null)("lead@xv")).toBeNull();
  });
});
