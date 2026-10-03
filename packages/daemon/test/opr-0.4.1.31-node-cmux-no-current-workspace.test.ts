// OPR.0.4.1.31 part A — node open-in-cmux must NOT fail just because no cmux
// workspace is current. Before the fix, createAndBindSurface anchored only on
// currentWorkspace(), so every unbound row failed when nothing was current.
// resolveWorkspaceAnchor now uses ONLY transport-allowlisted methods: the
// current workspace if present, else create one (cmux opens a new workspace as
// the active/visible one). It deliberately does NOT call workspace.select (the
// cmux CLI exposes no select command + our transport does not allow that RPC —
// dev1-guard B1). A genuinely-unavailable cmux propagates honestly.

import { describe, it, expect, vi } from "vitest";
import { NodeCmuxService } from "../src/domain/node-cmux-service.js";

type Res<T> = { ok: true; data: T } | { ok: false; code: string; message: string };

function makeAdapter(overrides: Record<string, unknown>) {
  const base = {
    currentWorkspace: async (): Promise<Res<string>> => ({ ok: false, code: "request_failed", message: "cmux current-workspace returned no workspace handle" }),
    createWorkspace: vi.fn(async (): Promise<Res<string>> => ({ ok: true, data: "ws-new" })),
    createTerminalSurface: async (): Promise<Res<string>> => ({ ok: true, data: "surface-1" }),
    sendText: async (): Promise<Res<void>> => ({ ok: true, data: undefined }),
    focusSurface: async (): Promise<Res<void>> => ({ ok: true, data: undefined }),
  };
  return { ...base, ...overrides };
}

function makeService(adapter: Record<string, unknown>) {
  const rigRepo = { getRig: () => ({ nodes: [{ id: "n1", logicalId: "dev.impl", binding: null }] }) };
  const sessionRegistry = { updateBinding: vi.fn() };
  return new NodeCmuxService(rigRepo as never, sessionRegistry as never, adapter as never);
}

describe("OPR.0.4.1.31 part A — NodeCmuxService no-current-workspace handling (allowed methods only)", () => {
  it("no current workspace -> creates a workspace (becomes visible) and opens", async () => {
    const adapter = makeAdapter({});
    const svc = makeService(adapter);
    const result = await svc.openOrFocusNodeSurface("rig-1", "dev.impl");
    expect(result.ok).toBe(true);
    expect((adapter.createWorkspace as ReturnType<typeof vi.fn>)).toHaveBeenCalled();
  });

  it("cmux unavailable -> propagates the honest error, does NOT create a workspace", async () => {
    const adapter = makeAdapter({
      currentWorkspace: async () => ({ ok: false, code: "unavailable", message: "cmux is not connected" }),
    });
    const svc = makeService(adapter);
    const result = await svc.openOrFocusNodeSurface("rig-1", "dev.impl");
    expect(result.ok).toBe(false);
    expect((result as { code?: string }).code).toBe("unavailable");
    expect((adapter.createWorkspace as ReturnType<typeof vi.fn>)).not.toHaveBeenCalled();
  });

  it("a current workspace exists -> uses it, no create fallback", async () => {
    const adapter = makeAdapter({
      currentWorkspace: async () => ({ ok: true, data: "ws-current" }),
    });
    const svc = makeService(adapter);
    const result = await svc.openOrFocusNodeSurface("rig-1", "dev.impl");
    expect(result.ok).toBe(true);
    expect((adapter.createWorkspace as ReturnType<typeof vi.fn>)).not.toHaveBeenCalled();
  });

  it("no current + create fails -> honest error (operator can fall back to rig-level Launch)", async () => {
    const adapter = makeAdapter({
      createWorkspace: vi.fn(async () => ({ ok: false, code: "request_failed", message: "cmux workspace.create failed" })),
    });
    const svc = makeService(adapter);
    const result = await svc.openOrFocusNodeSurface("rig-1", "dev.impl");
    expect(result.ok).toBe(false);
    expect((result as { error?: string }).error).toContain("cmux workspace.create failed");
  });
  it("closed bound surface falls through to recreation before custom-title rename",async()=>{
    const order:string[]=[];
    const adapter=makeAdapter({focusSurface:async(surface:string)=>{order.push(`focus:${surface}`);return surface==="closed"?{ok:false,code:"not_found",message:"surface closed"}:{ok:true,data:undefined};},renameSurface:vi.fn(async(surface:string)=>{order.push(`rename:${surface}`);return {ok:true,data:undefined};})});
    const rigRepo={getRig:()=>({nodes:[{id:"n1",logicalId:"dev.impl",binding:{attachmentType:"tmux",tmuxSession:"dev-impl@rig",cmuxSurface:"closed",cmuxWorkspace:"old"}}]})};
    const registry={updateBinding:vi.fn()};
    const svc=new NodeCmuxService(rigRepo as never,registry as never,adapter as never,{hasSession:async()=>true} as never);
    const result=await svc.openOrFocusNodeSurface("rig-1","dev.impl");
    expect(result).toEqual({ok:true,action:"created_new"});
    expect(order[0]).toBe("focus:closed");expect(order).not.toContain("rename:closed");expect(order).toContain("rename:surface-1");expect(registry.updateBinding).toHaveBeenCalled();
  });

});
