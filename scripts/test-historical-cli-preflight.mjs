import assert from "node:assert/strict";
import {mkdtempSync,existsSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {spawnSync} from "node:child_process";
const moduleUrl=new URL("../packages/cli/dist/commands/daemon.js",import.meta.url);
assert(existsSync(moduleUrl),"Build the CLI before running this actual-command regression");
const root=mkdtempSync(join(tmpdir(),"openrig-cli-preflight-"));
const cases=[["--wake-recovery-mode","invalid"],["--wake-recovery-mode","observe"],["--wake-recovery-mode","observe","--wake-recovery-manifest","relative.json"],["--wake-recovery-mode","deliver","--wake-recovery-manifest","/synthetic/exact.json"]];
try {
 for(const [index,flags] of cases.entries()) {
  const home=join(root,String(index),"home");
  const env={...process.env,OPENRIG_HOME:home,OPENRIG_DB_PATH:join(home,"openrig.sqlite"),OPENRIG_TRANSCRIPTS_PATH:join(home,"transcripts"),OPENRIG_URL:"http://127.0.0.1:19587"};
  delete env.OPENRIG_SESSION_NAME;delete env.OPENRIG_OCCUPANT_GENERATION;
  const code=`import {daemonCommand} from ${JSON.stringify(moduleUrl.href)}; await daemonCommand().parseAsync(${JSON.stringify(["start",...flags,"--no-kernel","--port","19587"])},{from:"user"});`;
  const result=spawnSync(process.execPath,["--input-type=module","-e",code],{env,encoding:"utf8",timeout:15000});
  assert.equal(result.status,1,`Case ${index} must reject before startup: ${result.stderr}`);
  assert.equal(existsSync(home),false,`Case ${index} created runtime home before validation`);
  assert.match(result.stderr,/absolute exact cohort manifest/);
 }
 console.log("PASS: four actual CLI rejection cases created no runtime homes");
} finally {rmSync(root,{recursive:true,force:true});}
