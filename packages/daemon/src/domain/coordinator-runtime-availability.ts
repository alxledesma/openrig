import type Database from "better-sqlite3";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
const run = promisify(execFile);
export interface RuntimeAvailability { session: string; generation: string; state: "present" | "absent" | "unknown"; observedAt: number; fingerprint: string }
export function makeCoordinatorRuntimeObserver(db: Database.Database, exec: (command:string)=>Promise<string>, census = async () => (await run("ps",["-axo","pid=,ppid=,lstart=,comm="],{timeout:2000,maxBuffer:8*1024*1024})).stdout) {
 return async (session:string):Promise<RuntimeAvailability|null> => {
  const read=()=>{
   const rows=db.prepare(`SELECT DISTINCT n.id,n.runtime,b.tmux_pane,b.tmux_session,t.generation_uuid FROM sessions s JOIN nodes n ON n.id=s.node_id JOIN bindings b ON b.node_id=n.id JOIN occupant_tenures t ON t.node_id=n.id WHERE s.session_name=? AND t.generation_ordinal=(SELECT MAX(x.generation_ordinal) FROM occupant_tenures x WHERE x.node_id=n.id)`).all(session) as Array<{id:string;runtime:string;tmux_pane:string|null;tmux_session:string|null;generation_uuid:string}>;
   return rows.length===1?rows[0]:undefined;
  };
  const binding=read();if(!binding||!['codex','claude-code'].includes(binding.runtime))return null;
  const target=binding.tmux_pane??binding.tmux_session;if(!target)return null;
  const quote=(s:string)=>"'"+s.replace(/'/g,"'\\''")+"'";
  try {
   const sample=async()=>{
    // Successful explicit pane metadata is required. Missing server/pane is unknown.
    const pane=(await exec(`tmux display-message -p -t ${quote(target)} '#{pane_id}|#{pane_pid}|#{pane_dead}'`)).trim();
    const match=pane.match(/^(%\d+)\|(\d+)\|([01])$/);if(!match)return null;
    const text=await census();const rows=text.trim().split('\n').map(line=>line.trim().match(/^(\d+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s+(.+)$/));
    if(!rows.length||rows.some(row=>!row))return null;
    const parsed=rows.map(row=>({pid:Number(row![1]),ppid:Number(row![2]),started:row![3]!,comm:row![4]!}));
    if(new Set(parsed.map(r=>r.pid)).size!==parsed.length)return null;
    const descendants=new Set<number>([Number(match[2])]);let changed=true;while(changed){changed=false;for(const r of parsed)if(descendants.has(r.ppid)&&!descendants.has(r.pid)){descendants.add(r.pid);changed=true;}}
    const runtime=binding.runtime==='codex'?'codex':'claude';const native=parsed.filter(r=>descendants.has(r.pid)&&path.basename(r.comm)===runtime);
    // A live shell with no native descendant, or retained dead pane with no live
    // pane PID, is positive absence. Unknown root mismatch is not absence.
    const root=parsed.find(r=>r.pid===Number(match[2]));
    if(native.length===0&&parsed.some(r=>descendants.has(r.pid)&&r.pid!==Number(match[2])))return null;
    if(!root&&match[3]!=='1')return null;
    if(root&&native.length===0&&!['zsh','bash','sh','fish'].includes(path.basename(root.comm).replace(/^-(zsh|bash|sh|fish)$/, '$1')))return null;
    return {state:native.length?'present' as const:'absent' as const,fingerprint:JSON.stringify({pane,native: native.map(r=>[r.pid,r.ppid,r.started,r.comm]),root:root?[root.pid,root.ppid,root.started,root.comm]:null})};
   };
   const first=await sample(),second=await sample();if(!first||!second||first.fingerprint!==second.fingerprint||JSON.stringify(read())!==JSON.stringify(binding))return null;
   return {session,generation:binding.generation_uuid,state:second.state,observedAt:Date.now(),fingerprint:second.fingerprint};
  } catch {return null;}
 };
}
