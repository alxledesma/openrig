import {describe,it,expect} from "vitest";
import {makeCoordinatorRuntimeObserver} from "../src/domain/coordinator-runtime-availability.js";
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
