import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, cpSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const expectedSha='1'.repeat(40);
function fixture(){
 const root=mkdtempSync(join(tmpdir(),'builtin-guard-tamper-'));
 writeFileSync(join(root,'package.json'),'{"type":"module"}');
 for(const dir of ['daemon/dist','dist']){mkdirSync(join(root,dir),{recursive:true});writeFileSync(join(root,dir,'build-info.js'),`export const BUILD_INFO={commit:"${expectedSha}",dirty:false};`);}
 cpSync(resolve('packages/daemon/policies'),join(root,'daemon/policies'),{recursive:true});
 writeFileSync(join(root,'daemon/dist/startup.js'),'throw new Error("unexpected startup sentinel");');
 return root;
}
test('compiled package guard still refuses altered YOLO bytes before native startup',()=>{
 const root=fixture();try{
 const file=join(root,'daemon/policies/builtin/yolo.policy.md');writeFileSync(file,readFileSync(file,'utf8')+'\nunauthorized policy change\n');
 const result=spawnSync(process.execPath,['scripts/verify-packaged-builtins.mjs',root,expectedSha],{encoding:'utf8'});
 assert.equal(result.status,1);assert.match(result.stderr,/assembled yolo\.policy\.md diverged from authority/);assert.doesNotMatch(result.stderr,/unexpected startup sentinel/);
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('canonical policy bytes pass all unchanged digest checks before controlled startup sentinel',()=>{
 const root=fixture();try{
 const result=spawnSync(process.execPath,['scripts/verify-packaged-builtins.mjs',root,expectedSha],{encoding:'utf8'});
 assert.equal(result.status,1);assert.match(result.stdout,/all authority hashes match/);assert.match(result.stderr,/unexpected startup sentinel/);
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('repair does not weaken build identity or other policy hash pins',()=>{
 const root=fixture();try{
 writeFileSync(join(root,'daemon/policies/builtin/standard.policy.md'),'altered');
 const result=spawnSync(process.execPath,['scripts/verify-packaged-builtins.mjs',root,expectedSha],{encoding:'utf8'});
 assert.equal(result.status,1);assert.match(result.stderr,/standard\.policy\.md diverged from authority/);
 const wrong=spawnSync(process.execPath,['scripts/verify-packaged-builtins.mjs',root,'2'.repeat(40)],{encoding:'utf8'});
 assert.equal(wrong.status,1);assert.match(wrong.stderr,/commit .* !== candidate/);
 }finally{rmSync(root,{recursive:true,force:true});}
});
