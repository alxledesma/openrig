import {describe,it,expect} from "vitest";
import {execFileSync} from "node:child_process";
import {randomUUID} from "node:crypto";
import {managedCmuxCommand,managedSeatTitle} from "../src/domain/managed-cmux-window.js";
import {TmuxAdapter} from "../src/adapters/tmux.js";
import {shellQuote} from "../src/adapters/shell-quote.js";

describe("managed cmux native per-window sizing",()=>{
 it("repairs manual sizing persistently while preserving explicit user exceptions and sibling windows",async()=>{
  const socket=`openrig-display-test-${randomUUID()}`;
  const run=(...args:string[])=>execFileSync("tmux",["-L",socket,...args],{encoding:"utf8"});
  try{
   run("-f","/dev/null","new-session","-d","-s","managed","-x","90","-y","27");
   run("new-session","-d","-s","unrelated","-x","80","-y","24");
   run("set-option","-w","-t","managed","window-size","manual");run("set-option","-w","-t","unrelated","window-size","manual");
   const adapter=new TmuxAdapter(async command=>execFileSync("/bin/bash",["-c",`function tmux(){ command tmux -L ${shellQuote(socket)} "$@"; }; ${command}`],{encoding:"utf8"}));
   expect(await adapter.configureManagedCmuxWindow("managed")).toEqual({ok:true});
   expect(run("show-options","-wqv","-t","managed","window-size").trim()).toBe("latest");
   expect(run("show-options","-wqv","-t","managed","fill-character").replace(/\n$/,"")).toBe(" ");
   expect(await adapter.shouldPreserveWindowSizing("managed")).toBe(true);
   await adapter.resizeWindow("managed",100,30);
   expect(run("show-options","-wqv","-t","managed","window-size").trim()).toBe("latest");
   expect(run("show-options","-wqv","-t","unrelated","window-size").trim()).toBe("manual");
   // A new managed opening repairs accidental old manual mode again.
   run("resize-window","-t","managed","-x","91","-y","28");
   await adapter.configureManagedCmuxWindow("managed");expect(run("show-options","-wqv","-t","managed","window-size").trim()).toBe("latest");
   run("set-option","-w","-t","managed","@openrig-preserve-window-size","1");run("set-option","-w","-t","managed","window-size","manual");run("set-option","-w","-t","managed","fill-character",".");
   await adapter.configureManagedCmuxWindow("managed");expect(run("show-options","-wqv","-t","managed","window-size").trim()).toBe("manual");expect(run("show-options","-wqv","-t","managed","fill-character").trim()).toBe(".");
  }finally{try{run("kill-server");}catch{}}
 });
 it("local attach wrapper preserves read-only command and quotes target",()=>{
  const command=managedCmuxCommand("seat@rig","tmux attach -r -t 'seat@rig'");
  expect(command).toContain("window-size latest");expect(command).toContain("fill-character ' '");expect(command).toContain("&& tmux attach -r -t 'seat@rig'");
 });
 it("managed titles identify role instead of attachment command",()=>{
  for(const [seat,title] of [["orch1-lead@xv","Lead"],["orch1-peer@xv","Peer"],["arch1-architect@xv","Architect"],["arch1-advisor@xv","Advisor"],["dev1-impl@xv","Builder Sol"],["dev1-impl2@xv","Builder Luna"],["dev1-design@xv","Designer"],["dev1-qa@xv","QA"],["rev1-r1@xv","Reviewer GLM"],["rev1-r2@xv","Reviewer Sonnet"],["review-space-bunny@xv","Reviewer Space Bunny"],["review-dgx@xv","Reviewer DGX"],["operator-agent@kernel","Operator"],["operator-human@kernel","Dashboard"]])expect(managedSeatTitle(seat!)).toBe(title);
 });

 it("publishes managed marker before geometry and finishes latest after an interleaved broker resize",async()=>{
  let marker=false,mode="manual";const order:string[]=[];
  const adapter=new TmuxAdapter(async cmd=>{
    if(cmd.startsWith("tmux show-options")&&cmd.includes("@openrig-preserve-window-size"))return "";
    if(cmd.startsWith("tmux show-options")&&cmd.includes("@openrig-cmux-auto-size"))return marker?"1":"";
    if(cmd.startsWith("tmux resize-window")){mode="manual";order.push("broker-resize");return "";}
    if(cmd.startsWith("tmux set-option")&&cmd.endsWith("window-size latest")){mode="latest";order.push("latest");return "";}
    const setup=managedCmuxCommand("managed","true");expect(cmd).toBe(setup);
    expect(setup.indexOf("@openrig-cmux-auto-size 1")).toBeLessThan(setup.indexOf("fill-character ' '"));expect(setup.indexOf("fill-character ' '")).toBeLessThan(setup.indexOf("window-size latest"));
    marker=true;order.push("marker");await adapter.resizeWindow("managed",90,27);mode="latest";order.push("helper-final-latest");return "";
  });
  await adapter.configureManagedCmuxWindow("managed");expect(order).toEqual(["marker","broker-resize","latest","helper-final-latest"]);expect(mode).toBe("latest");
 });

});
