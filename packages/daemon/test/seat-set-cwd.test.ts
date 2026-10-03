import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync, writeFileSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type Database from "better-sqlite3";
import { createFullTestDb } from "./helpers/test-app.js";
import { RigRepository } from "../src/domain/rig-repository.js";
import { SessionRegistry } from "../src/domain/session-registry.js";
import { EventBus } from "../src/domain/event-bus.js";
import { SeatLifecycleService } from "../src/domain/seat-lifecycle-service.js";
import { SeatDeliveryGuard, resolveGuardTarget } from "../src/domain/seat-delivery-guard.js";
import type { TmuxAdapter } from "../src/adapters/tmux.js";

const PROFILE = `model = "gpt-6.1-sol"
model_provider = "openai"
model_reasoning_effort = "low"
approval_policy = "never"
sandbox_mode = "danger-full-access"
`;

describe("set-cwd", () => {
  let db: Database.Database;
  let repo: RigRepository;
  let sessions: SessionRegistry;
  let bus: EventBus;
  let home: string;
  let probe: ReturnType<typeof vi.fn>;
  let service: SeatLifecycleService;
  let guard: SeatDeliveryGuard;

  beforeEach(() => {
    db = createFullTestDb();
    db.exec("CREATE TABLE seat_delivery_guards (node_id TEXT PRIMARY KEY, desired INTEGER NOT NULL, effective INTEGER NOT NULL)");
    repo = new RigRepository(db);
    sessions = new SessionRegistry(db);
    bus = new EventBus(db);
    home = realpathSync(mkdtempSync(join(tmpdir(), "openrig-codex-profile-")));
    writeFileSync(join(home, "config.toml"), 'sandbox_mode = "danger-full-access"\n');
    probe = vi.fn(async () => undefined);
    guard = new SeatDeliveryGuard(db, name => resolveGuardTarget(db, name));
    service = new SeatLifecycleService({
      db, rigRepo: repo, sessionRegistry: sessions, eventBus: bus,
      tmuxAdapter: { deliveryGuard: guard } as TmuxAdapter, codexProfileHome: home, codexProfileProbe: probe,
    });
  });

  afterEach(() => { db.close(); rmSync(home, { recursive: true, force: true }); });

  function seat(runtime = "codex") {
    if (runtime === "codex") writeFileSync(join(home, "old-profile.config.toml"), PROFILE.replace("gpt-6.1-sol", "gpt-6-luna").replace('model_provider = "openai"\n', "").replace('sandbox_mode = "danger-full-access"\n', ""));
    const rig = repo.createRig("test-rig");
    const node = repo.addNode(rig.id, "dev.qa", {
      runtime, model: "gpt-6.1-sol", codexConfigProfile: "old-profile", cwd: "/project",
    });
    const session = sessions.registerSession(node.id, "dev-qa@test-rig");
    sessions.updateStatus(session.id, "running");
    sessions.updateResumeToken(session.id, "codex_id", "native-uuid", "scrape");
    sessions.updateBinding(node.id, { attachmentType: "tmux", tmuxSession: "dev-qa@test-rig", tmuxPane: "%7" });
    return { rig, node, session };
  }

  const row = (db: Database.Database, table: string) => db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();

  it("changes only node cwd and audit; all runtime, custody and identity fields survive", async()=>{
    const {node}=seat("pi");const before={nodes:row(db,"nodes"),sessions:row(db,"sessions"),bindings:row(db,"bindings"),queue:row(db,"queue_items"),tenures:row(db,"occupant_tenures")};
    const result=await service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"owner project move",actor:"operator-agent@kernel"});
    expect(result).toMatchObject({ok:true,from:"/project",to:home,changed:true});
    expect(row(db,"nodes")).toEqual(before.nodes.map(n=>({...n as object,cwd:home})));for(const [k,t] of [["sessions","sessions"],["bindings","bindings"],["queue","queue_items"],["tenures","occupant_tenures"]])expect(row(db,t!)).toEqual(before[k as keyof typeof before]);
    const audit=JSON.parse((db.prepare("SELECT payload FROM events WHERE type='node.cwd_changed'").get() as {payload:string}).payload);expect(audit).toMatchObject({operator:"operator-agent@kernel",reason:"owner project move",nodeId:node.id,effect:"future_launches_only"});expect(probe).not.toHaveBeenCalled();
    expect(await service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"repeat",actor:"operator-agent@kernel"})).toMatchObject({ok:true,changed:false});expect(db.prepare("SELECT count(*) n FROM events WHERE type='node.cwd_changed'").get()).toEqual({n:1});
  });
  it("refuses missing/relative/file paths, missing actor and reason without mutations",async()=>{
    seat();const before=row(db,"nodes");writeFileSync(join(home,"file"),"data");for(const cwd of ["","relative",join(home,"missing"),join(home,"file")])expect(await service.setCwd({seatRef:"dev-qa@test-rig",cwd,reason:"test",actor:"operator"})).toMatchObject({ok:false,code:"invalid_cwd"});
    expect(await service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"test",actor:""})).toMatchObject({ok:false,code:"missing_actor"});expect(await service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"",actor:"operator"})).toMatchObject({ok:false,code:"missing_reason"});expect(row(db,"nodes")).toEqual(before);
  });
  it("waits for lifecycle then refuses concurrent configuration drift",async()=>{
    const {node}=seat();let unlock!:()=>void;let entered!:()=>void;const ready=new Promise<void>(r=>entered=r);const held=guard.lifecycle([node.id],async()=>{entered();await new Promise<void>(r=>unlock=r);});await ready;
    let settled=false;const selection=service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"move",actor:"operator"}).finally(()=>settled=true);await new Promise(r=>setTimeout(r,10));expect(settled).toBe(false);expect(row(db,"nodes")[0]).toMatchObject({cwd:"/project"});db.prepare("UPDATE nodes SET cwd='/other' WHERE id=?").run(node.id);unlock();await held;expect(await selection).toMatchObject({ok:false,code:"cwd_selection_conflict"});expect(row(db,"nodes")[0]).toMatchObject({cwd:"/other"});
  });
  it("joins an owned lifecycle lease and refuses absent/wrong database guard",async()=>{
    const {node}=seat();expect(await guard.lifecycle([node.id],()=>service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"move",actor:"operator"}))).toMatchObject({ok:true});
    const unguarded=new SeatLifecycleService({db,rigRepo:repo,sessionRegistry:sessions,eventBus:bus,tmuxAdapter:{} as TmuxAdapter});expect(await unguarded.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"move",actor:"operator"})).toMatchObject({ok:false,code:"cwd_guard_unavailable"});
  });
  it("rolls back cwd if audit persistence fails",async()=>{seat();vi.spyOn(bus,"persistWithinTransaction").mockImplementationOnce(()=>{throw new Error("audit unavailable")});await expect(service.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"move",actor:"operator"})).rejects.toThrow("audit unavailable");expect(row(db,"nodes")[0]).toMatchObject({cwd:"/project"});});
  it("refuses unknown seat identity and a guard attached to another database",async()=>{
    seat();expect(await service.setCwd({seatRef:"unknown@test-rig",cwd:home,reason:"move",actor:"operator"})).toMatchObject({ok:false,code:"seat_not_found"});
    const other=createFullTestDb();try{const wrong=new SeatLifecycleService({db,rigRepo:repo,sessionRegistry:sessions,eventBus:bus,tmuxAdapter:{deliveryGuard:new SeatDeliveryGuard(other,()=>null)} as TmuxAdapter});expect(await wrong.setCwd({seatRef:"dev-qa@test-rig",cwd:home,reason:"move",actor:"operator"})).toMatchObject({ok:false,code:"cwd_guard_unavailable"});}finally{other.close();}
    expect(row(db,"nodes")[0]).toMatchObject({cwd:"/project"});
  });
});
