import type {Policy} from './types.js';
import type {ResilienceRolloutService} from '../resilience-rollout-service.js';
/** Native host auditor creates only bounded recovery custody, never admission. */
export function makeResilienceRolloutPolicy(service:ResilienceRolloutService):Policy {
 return {name:'resilience-rollout',async evaluate(job){
  if(job.registeredBySession!=='daemon@kernel'||job.target.session!=='operator-agent@kernel'||job.context.policyRef!=='builtin:standard')return {action:'skip',reason:'rollout-audit-not-authorized'};
  const receipts=service.reconcile();await service.deliver();
  return {action:'skip',reason:receipts.every(r=>r.state==='covered')?'all-projects-covered':'accountable-rollout-recovery',notes:{receipts}};
 }};
}
