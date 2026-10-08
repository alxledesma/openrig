import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createDb } from "../src/db/connection.js";
import { migrate } from "../src/db/migrate.js";
import { nativeDutySupervisionSchema } from "../src/db/migrations/107_native_duty_supervision.js";
import { NativeDutyError, NativeDutySupervisionService, type NativeDutyOperationReceipt } from "../src/domain/native-duty-supervision-service.js";
import type { NativeDutyActor, NativeDutyProof, NativeDutyResumeRequest, NativeDutyScope } from "../src/domain/native-duty-contract.js";

function canonical(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    return "{" + Object.keys(object).sort().map((key) => JSON.stringify(key) + ":" + canonical(object[key])).join(",") + "}";
  }
  return JSON.stringify(value);
}
const sha = (value: string) => createHash("sha256").update(value).digest("hex");

describe("native duty supervision", () => {
  it("binds a current native grant, confirms only the exact receipt, and preserves unresolved intent", () => {
    const db = createDb(); migrate(db, [nativeDutySupervisionSchema]);
    let now = 10_000;
    const operator: NativeDutyActor = { session: "operator-agent@kernel", generation: "operator-g7" };
    const native: NativeDutyActor = { session: "lead@xv", generation: "lead-g12" };
    const scope: NativeDutyScope = { scopeId: "scope-xv-12", nodeId: "node-xv", sessionName: native.session,
      generation: native.generation, runtime: "codex", rigId: "xv", configurationDigest: "cfg-sha256-12",
      validUntil: 90_000, maxLeaseMs: 20_000, kind: "holder-continuation" };
    let config = scope.configurationDigest, proofUnavailable = false;
    let receipt: NativeDutyOperationReceipt | null = null;
    const proof = (launchId: string, supervisorPid: number): NativeDutyProof => ({ nodeId: scope.nodeId,
      sessionName: native.session, generation: native.generation, runtime: "codex", launchId, supervisorPid,
      configurationDigest: config, fingerprint: "fingerprint-" + launchId, observedAt: now,
      nativePresent: true, supervisorIsNativeAncestor: true, lifecycleReserved: false });
    const service = new NativeDutySupervisionService({ db, now: () => now,
      approvedScope: (id) => id === scope.scopeId ? scope : null,
      assertCurrentOperator: (actor) => { if (actor.session !== operator.session || actor.generation !== operator.generation) throw new Error("operator generation is not current"); },
      observeNative: (_scope, launchId, pid) => proofUnavailable ? null : proof(launchId, pid),
      assertResumeAuthority: (_scope, actor, request) => { expect(actor).toEqual(native); expect(request.rigId).toBe(scope.rigId); },
      operationReceipt: () => receipt,
    });

    const grant = service.grant(operator, scope);
    expect(service.grant(operator, scope)).toEqual(grant);
    expect(grant.grantedBy).toEqual(operator);
    expect(() => service.register({ ...native, generation: "lead-old" }, { scopeId: scope.scopeId, launchId: "launch-a", supervisorPid: 710 })).toThrowError(NativeDutyError);
    const registered = service.register(native, { scopeId: scope.scopeId, launchId: "launch-a", supervisorPid: 710 });
    expect(service.register(native, { scopeId: scope.scopeId, launchId: "launch-a", supervisorPid: 710 }).registrationId).toBe(registered.registrationId);

    const watchingBefore=db.prepare("SELECT * FROM native_duty_registrations WHERE registration_id=?").get(registered.registrationId);
    proofUnavailable=true;
    expect(()=>service.heartbeat(native,registered.registrationId)).toThrowError(NativeDutyError);
    expect(service.status(registered.registrationId).phase).toBe("watching");
    expect(db.prepare("SELECT * FROM native_duty_registrations WHERE registration_id=?").get(registered.registrationId)).toEqual(watchingBefore);
    proofUnavailable=false;now++;
    expect(service.heartbeat(native,registered.registrationId)).toMatchObject({registrationId:registered.registrationId,phase:"watching",lastHeartbeatAt:now});
    expect(db.prepare("SELECT count(*) n FROM native_duty_registrations WHERE scope_id=?").get(scope.scopeId)).toEqual({n:1});

    const mismatched = service.register(native, { scopeId: scope.scopeId, launchId: "launch-c", supervisorPid: 712 });
    config = "different-config";
    expect(() => service.heartbeat(native, mismatched.registrationId)).toThrowError(/does not match grant/);
    expect(service.status(mismatched.registrationId).phase).toBe("held");
    config = scope.configurationDigest;
    expect(() => service.heartbeat(native, mismatched.registrationId)).toThrowError(/watching registration/);
    const request: NativeDutyResumeRequest = { rigId: "xv", operationId: "resume-xv-001", leaseMs: 8_000,
      expectedEpoch: 12, expectedObligationsDigest: "a".repeat(64) };
    expect(() => service.prepare(native, { registrationId: registered.registrationId, request: { ...request, leaseMs: 999 } })).toThrowError(/bounded resume-owned body/);
    expect(() => service.prepare(native, { registrationId: registered.registrationId, request: { ...request, operationId: "x".repeat(161) } })).toThrowError(/bounded resume-owned body/);
    expect(() => service.prepare(native, { registrationId: registered.registrationId, request: { ...request, expectedEpoch: 0 } })).toThrowError(/bounded resume-owned body/);
    expect(() => service.prepare(native, { registrationId: registered.registrationId, request: { ...request, expectedObligationsDigest: "bad" } })).toThrowError(/bounded resume-owned body/);
    const prepared = service.prepare(native, { registrationId: registered.registrationId, request });
    const original = db.prepare("SELECT request_json FROM native_duty_intents WHERE registration_id=? AND operation_id=?").get(registered.registrationId, request.operationId) as { request_json: string };
    expect(prepared.phase).toBe("prepared");
    expect(() => service.prepare({ ...native, generation: "retired-generation" }, { registrationId: registered.registrationId, request })).toThrowError(/retained registration generation/);
    expect(() => service.prepare(native, { registrationId: registered.registrationId, request: { ...request, operationId: "resume-xv-002" } })).toThrowError(/unresolved/);
    expect(service.markInFlight(native, { registrationId: registered.registrationId, operationId: request.operationId }).maySendEffect).toBe(true);
    expect(service.markInFlight(native, { registrationId: registered.registrationId, operationId: request.operationId }).maySendEffect).toBe(false);

    const authority = { rig_id: "xv", owner_session: native.session, owner_generation: native.generation, epoch: 12, state: "active", lease_until: now - 1 };
    receipt = { rigId: "xv", operationId: request.operationId, kind: "resume-owned",
      requestHash: sha(canonical({ actor: native.session, callerGeneration: native.generation, input: request })),
      receiptDigest: sha(canonical(authority)), receipt: authority };
    expect(service.reconcile(native, { registrationId: registered.registrationId, operationId: request.operationId }).phase).toBe("receipt-confirmed");
    expect((db.prepare("SELECT request_json FROM native_duty_intents WHERE registration_id=? AND operation_id=?").get(registered.registrationId, request.operationId) as { request_json: string }).request_json).toBe(original.request_json);

    const second = service.register(native, { scopeId: scope.scopeId, launchId: "launch-b", supervisorPid: 711 });
    const uncertainRequest = { ...request, operationId: "resume-xv-003" };
    service.prepare(native, { registrationId: second.registrationId, request: uncertainRequest });
    service.markInFlight(native, { registrationId: second.registrationId, operationId: uncertainRequest.operationId });
    receipt = null;
    expect(service.reconcile(native, { registrationId: second.registrationId, operationId: uncertainRequest.operationId }).phase).toBe("uncertainty-held");
    const retained = db.prepare("SELECT request_json,phase FROM native_duty_intents WHERE registration_id=? AND operation_id=?").get(second.registrationId, uncertainRequest.operationId) as { request_json: string; phase: string };
    expect(retained).toEqual({ request_json: canonical(uncertainRequest), phase: "uncertainty-held" });
    expect(() => service.prepare(native, { registrationId: second.registrationId, request: { ...uncertainRequest, operationId: "resume-xv-004" } })).toThrowError(/unresolved/);
    service.revoke(operator, scope.scopeId);
    const lateAuthority = { rig_id: "xv", owner_session: native.session, owner_generation: native.generation, epoch: 12, state: "active", lease_until: now - 1 };
    receipt = { rigId: "xv", operationId: uncertainRequest.operationId, kind: "resume-owned",
      requestHash: sha(canonical({ actor: native.session, callerGeneration: native.generation, input: uncertainRequest })),
      receiptDigest: sha(canonical(lateAuthority)), receipt: lateAuthority };
    expect(service.reconcile(native, { registrationId: second.registrationId, operationId: uncertainRequest.operationId }).phase).toBe("receipt-confirmed");
    expect(() => service.markInFlight(native, { registrationId: second.registrationId, operationId: uncertainRequest.operationId })).toThrowError(/watching registration/);
    expect(service.status(registered.registrationId).intent?.phase).toBe("receipt-confirmed");
    expect(service.status(second.registrationId).intent?.phase).toBe("receipt-confirmed");
    expect(() => service.heartbeat(native, registered.registrationId)).toThrowError(/watching registration/);
    db.close();
  });
});

function nodeDebtFixture() {
  const db=createDb();migrate(db,[nativeDutySupervisionSchema]);
  const operator={session:"operator@kernel",generation:"operator-1"};
  const native={session:"peer@rig",generation:"generation-1"};
  const scope:NativeDutyScope={scopeId:"scope-1",nodeId:"logical-node",sessionName:native.session,generation:native.generation,
    runtime:"codex",rigId:"rig",configurationDigest:"a".repeat(64),validUntil:90000,maxLeaseMs:20000,kind:"holder-continuation"};
  const scopes=new Map([[scope.scopeId,scope]]);
  let receipt:NativeDutyOperationReceipt|null=null;
  const service=new NativeDutySupervisionService({db,now:()=>10000,approvedScope:id=>scopes.get(id)??null,
    assertCurrentOperator:actor=>{expect(actor).toEqual(operator);},assertResumeAuthority:()=>{},operationReceipt:()=>receipt,
    observeNative:(current,launchId,supervisorPid)=>({...current,launchId,supervisorPid,fingerprint:"native-proof",observedAt:10000,
      nativePresent:true,supervisorIsNativeAncestor:true,lifecycleReserved:false}),
  });
  const request:NativeDutyResumeRequest={rigId:"rig",operationId:"old-operation",leaseMs:5000,expectedEpoch:1,expectedObligationsDigest:"b".repeat(64)};
  service.grant(operator,scope);
  const first=service.register(native,{scopeId:scope.scopeId,launchId:"launch-old",supervisorPid:100});
  const second=service.register(native,{scopeId:scope.scopeId,launchId:"launch-preexisting",supervisorPid:101});
  const successor={...scope,scopeId:"scope-pregranted",generation:"generation-2"};scopes.set(successor.scopeId,successor);service.grant(operator,successor);
  const ungranted={...successor,scopeId:"scope-ungranted"};scopes.set(ungranted.scopeId,ungranted);
  const operation={registrationId:first.registrationId,operationId:request.operationId};
  return {db,service,operator,native,scope,scopes,request,first,second,successor,ungranted,operation,
    confirm:()=>{const authority={rig_id:"rig",owner_session:native.session,owner_generation:native.generation,epoch:1,state:"active",lease_until:15000};
      receipt={rigId:"rig",operationId:request.operationId,kind:"resume-owned",requestHash:sha(canonical({actor:native.session,callerGeneration:native.generation,input:request})),receiptDigest:sha(canonical(authority)),receipt:authority};
    }};
}

describe("node-wide unresolved native duty debt",()=>{
  it.each(["prepared","effect-in-flight","uncertainty-held"] as const)("retains %s debt across registrations, grants, generations and revocation until exact receipt",phase=>{
    const f=nodeDebtFixture();const {db,service,operator,native,scope,scopes,request,first,second,successor,ungranted,operation}=f;
    try {
      service.prepare(native,{registrationId:first.registrationId,request});
      if(phase!=="prepared")service.markInFlight(native,operation);
      if(phase==="uncertainty-held")expect(service.reconcile(native,operation).phase).toBe(phase);
      const original=db.prepare("SELECT * FROM native_duty_intents").all();
      // An exact immutable replay is readable; it does not prepare another operation or erase debt.
      expect(service.prepare(native,{registrationId:first.registrationId,request}).phase).toBe(phase);
      expect(()=>service.register(native,{scopeId:scope.scopeId,launchId:"fresh-launch",supervisorPid:102})).toThrowError(/node remains unresolved/);
      expect(()=>service.prepare(native,{registrationId:second.registrationId,request:{...request,operationId:"another-operation"}})).toThrowError(/node remains unresolved/);
      expect(()=>service.grant(operator,ungranted)).toThrowError(/node remains unresolved/);
      expect(db.prepare("SELECT * FROM native_duty_grants WHERE scope_id=?").get(ungranted.scopeId)).toBeUndefined();
      service.revoke(operator,scope.scopeId);
      const successorActor={session:successor.sessionName,generation:successor.generation};
      expect(()=>service.register(successorActor,{scopeId:successor.scopeId,launchId:"next-generation",supervisorPid:103})).toThrowError(/node remains unresolved/);
      expect(()=>service.grant(operator,ungranted)).toThrowError(/node remains unresolved/);
      expect(db.prepare("SELECT * FROM native_duty_intents").all()).toEqual(original);
      // The exclusion is node-local; separate work remains eligible while this node is held.
      const other={...scope,scopeId:"unrelated-scope",nodeId:"other-node",sessionName:"other@rig"};scopes.set(other.scopeId,other);
      service.grant(operator,other);const otherActor={session:other.sessionName,generation:other.generation};
      const otherReg=service.register(otherActor,{scopeId:other.scopeId,launchId:"unrelated-launch",supervisorPid:104});
      service.prepare(otherActor,{registrationId:otherReg.registrationId,request:{...request,operationId:"unrelated-operation"}});
      // A genuine late receipt remains reconcilable after revocation, without reactivating the predecessor.
      if(phase==="prepared")expect(service.reconcile(native,operation).phase).toBe("uncertainty-held");
      f.confirm();expect(service.reconcile(native,operation).phase).toBe("receipt-confirmed");
      expect(service.status(first.registrationId).phase).toBe("stopped");
      expect((db.prepare("SELECT request_json FROM native_duty_intents WHERE registration_id=?").get(first.registrationId) as {request_json:string}).request_json).toBe(canonical(request));
      service.grant(operator,ungranted);
      const next=service.register(successorActor,{scopeId:successor.scopeId,launchId:"next-generation",supervisorPid:103});
      expect(service.prepare(successorActor,{registrationId:next.registrationId,request:{...request,operationId:"after-receipt"}}).phase).toBe("prepared");
    } finally {db.close();}
  });

  it("permits only the sole prepared intent to cross the effect boundary once",()=>{
    const {db,service,native,request,first,operation}=nodeDebtFixture();
    try {
      service.prepare(native,{registrationId:first.registrationId,request});
      expect(service.markInFlight(native,operation).maySendEffect).toBe(true);
      expect(service.markInFlight(native,operation).maySendEffect).toBe(false);
      expect(service.prepare(native,{registrationId:first.registrationId,request}).phase).toBe("effect-in-flight");
    } finally {db.close();}
  });

  it("refuses legacy competing prepared intents at the effect boundary without erasing either immutable body",()=>{
    const {db,service,native,request,first,second,operation}=nodeDebtFixture();
    try {
      service.prepare(native,{registrationId:first.registrationId,request});
      // Fixture represents debt retained from the older per-registration exclusion rule.
      db.prepare("INSERT INTO native_duty_intents(registration_id,operation_id,request_json,request_hash,body_digest,prepared_at,phase) SELECT ?,?,request_json,request_hash,body_digest,prepared_at,'prepared' FROM native_duty_intents WHERE registration_id=?").run(second.registrationId,"legacy-operation",first.registrationId);
      const before=db.prepare("SELECT * FROM native_duty_intents ORDER BY registration_id").all();
      expect(()=>service.markInFlight(native,operation)).toThrowError(/node remains unresolved/);
      expect(()=>service.markInFlight(native,{registrationId:second.registrationId,operationId:"legacy-operation"})).toThrowError(/node remains unresolved/);
      expect(db.prepare("SELECT * FROM native_duty_intents ORDER BY registration_id").all()).toEqual(before);
    } finally {db.close();}
  });
});
