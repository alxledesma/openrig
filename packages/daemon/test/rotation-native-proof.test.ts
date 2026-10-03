import {test} from 'vitest';import assert from 'node:assert/strict';import {mkdtempSync,writeFileSync,rmSync}from'node:fs';import {tmpdir}from'node:os';import{join}from'node:path';import{codexRotationContract}from'../src/domain/rotation-native-proof.js';
const dir=mkdtempSync(join(tmpdir(),'rotation-proof-'));const file=join(dir,'native.jsonl');
writeFileSync(file,JSON.stringify({type:'session_meta',payload:{id:'old',model_provider:'openai'}})+'\n'+JSON.stringify({type:'turn_context',payload:{model:'gpt-6-astra',sandbox_policy:{type:'danger-full-access'},approval_policy:'never',effort:'medium'}}));
test('native posture establishes unknown launcher effect without treating unknown as proof',()=>assert.equal(codexRotationContract(file,'old',['codex','-p','selected'],'gpt-6-astra','selected').provider,'openai'));
test('wrong native generation refuses',()=>assert.throws(()=>codexRotationContract(file,'other',['-p','selected'],'gpt-6-astra','selected')));
test('profile absent refuses',()=>assert.throws(()=>codexRotationContract(file,'old',[],'gpt-6-astra','selected')));
test('persistent model drift refuses',()=>assert.throws(()=>codexRotationContract(file,'old',['-p','selected'],'gpt-6-sol','selected')));
process.on('exit',()=>rmSync(dir,{recursive:true,force:true}));

import {parseProcessInventory,assertSuccessorProfileContinuity} from "../src/domain/rotation-facts-resolver.js";
test("separate process inventory preserves final comm path without combined args truncation",()=>{
 const p=parseProcessInventory(" 91701 123 /opt/homebrew/bin/codex\n 123 1 /bin/zsh\n");assert.equal(p[0]![3],"/opt/homebrew/bin/codex");assert.equal(p[0]![1],"91701");
});
test("native named profile continuity works without global legacy profiles table",()=>{
 const contract={provider:"openai",effort:"medium",permissions:{approval:"never"}};
 assertSuccessorProfileContinuity({}, {model_reasoning_effort:"medium",approval_policy:"never"},contract,["codex","-p","xv-sol61-medium"]);
 assertSuccessorProfileContinuity({approval_policy:"on-request"},{model_reasoning_effort:"medium",approval_policy:"on-request"},contract,["codex","-p","xv-sol61-medium","-a","never"]);
 assert.throws(()=>assertSuccessorProfileContinuity({}, {model_reasoning_effort:"high",approval_policy:"never"},contract,["codex","-p","xv-sol61-medium"]));
 assert.throws(()=>assertSuccessorProfileContinuity({}, {},contract,["codex","-p","xv-sol61-medium"]));
});
