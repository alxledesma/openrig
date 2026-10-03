import { test } from "vitest";
import assert from "node:assert/strict";
import { assertRotationPrecondition, assertManagedUnattended, RotationPreconditionRefusal, type RotationFacts } from "../src/domain/rotation-precondition.js";
const expected={protocol:"generation-queue-runtime-idle-v1", generation:"native-old",queue:["work"],runtimeContract:{model:"model",permissions:"explicit"},checkpointHash:"frozen"};
const facts:RotationFacts={generation:"native-old",queue:["work"],runtimeContract:{model:"model",permissions:"explicit"},activity:"idle-at-prompt",observedAt:1000,checkpointHash:"frozen"};
test("matching current immutable boundary passes",()=>assertRotationPrecondition(expected,facts,1000));
for(const [name,changed] of Object.entries({busy:{activity:"working"},unknown:{activity:null},generation:{generation:"successor"},custody:{queue:[]},permission:{runtimeContract:null},checkpoint:{checkpointHash:"changed"},stale:{observedAt:0}})) {
 test(`refuses ${name} before mutation`,()=>assert.throws(()=>assertRotationPrecondition(expected,{...facts,...changed},6000)));
}

const policy={automatic_cutover_enabled:true,managed_unattended_seats:["seat"]};
const receipt={quiescent:true,unattended_eligible:true,snapshot:{who:{identity:{sessionName:"seat"}}},packet:Object.fromEntries(["current_work","decisions","memory","constraints","standing_duties","evidence","next_action","outstanding_effects"].map(field=>[field,[]]))};
test("explicit unattended opt-in plus native-caller bound quiescence is accepted",()=>assertManagedUnattended(policy,"seat",receipt));
test("missing protection row or default false never supplies unattended eligibility",()=>assert.throws(()=>assertManagedUnattended({automatic_cutover_enabled:true},"seat",receipt)));
test("different seat opt-in does not authorize pilot",()=>assert.throws(()=>assertManagedUnattended({...policy,managed_unattended_seats:["other"]},"seat",receipt)));
test("unattended opt-in without quiescent checkpoint refuses",()=>assert.throws(()=>assertManagedUnattended(policy,"seat",{...receipt,quiescent:false})));
test("checkpoint from another caller refuses",()=>assert.throws(()=>assertManagedUnattended(policy,"seat",{...receipt,snapshot:{who:{identity:{sessionName:"other"}}}})));

test("typed precondition refusal proves no process replacement without claiming no evidence writes",async()=>{const response=new RotationPreconditionRefusal("busy").getResponse();assert.equal(response.status,409);assert.deepEqual(await response.json(),{ok:false,code:"rotation_precondition_failed",refusedBeforeReplacement:true,message:"busy"});});
