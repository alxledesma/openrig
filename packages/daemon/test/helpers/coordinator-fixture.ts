import type Database from "better-sqlite3";
import { migrate } from "../../src/db/migrate.js";
import { ALL_MIGRATIONS } from "../../src/db/all-migrations.js";
export function seed(db:Database.Database):void {
 migrate(db,ALL_MIGRATIONS);
 for(const name of ["xv","kernel","other"])db.prepare("INSERT INTO rigs(id,name) VALUES (?,?)").run(name,name);
 for(const [name,rig] of [["lead","xv"],["peer","xv"],["builder","xv"],["reviewer","xv"],["architect","xv"],["operator-agent","kernel"],["worker","other"]]){
   const id=`${name}@${rig}`;
   db.prepare("INSERT INTO nodes(id,rig_id,logical_id) VALUES (?,?,?)").run(id,rig,name);
   db.prepare("INSERT INTO sessions(id,node_id,session_name) VALUES (?,?,?)").run(id,id,id);
   db.prepare("INSERT INTO occupant_tenures(id,node_id,generation_ordinal,generation_uuid,kind) VALUES (?,?,1,?,'fresh')").run(id,id,`${name}-g1`);
 }
}
export const token={rigId:"xv",epoch:1,generation:"lead-g1"};
