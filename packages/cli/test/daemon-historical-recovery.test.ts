import { it, expect, vi } from 'vitest';
import { buildDaemonEnv, startDaemon, validateWakeRecoveryOptions, type LifecycleDeps } from '../src/daemon-lifecycle.js';

it('normal startup scrubs inherited adoption mode and historical manifest',()=>{
 const env=buildDaemonEnv({OPENRIG_WAKE_RECOVERY_MODE:'observe',OPENRIG_WAKE_RECOVERY_MANIFEST:'/private/stale.json'},{port:7433,db:'isolated.sqlite'});
 expect(env.OPENRIG_WAKE_RECOVERY_MODE).toBe('deliver');expect(env.OPENRIG_WAKE_RECOVERY_MANIFEST).toBeUndefined();
});
it('explicit observe carries only the enumerated manifest; it has no per-project delivery stop switch',()=>{
 const env=buildDaemonEnv({OPENRIG_WAKE_RECOVERY_MANIFEST:'/stale.json'},{port:17535,db:'isolated.sqlite',wakeRecoveryMode:'observe',wakeRecoveryManifest:'/synthetic/exact.json'});
 expect(env.OPENRIG_WAKE_RECOVERY_MODE).toBe('observe');expect(env.OPENRIG_WAKE_RECOVERY_MANIFEST).toBe('/synthetic/exact.json');expect(env.OPENRIG_PORT).toBe('17535');
});
it.each([
 {wakeRecoveryMode:'invalid'},
 {wakeRecoveryMode:'observe'},
 {wakeRecoveryMode:'observe',wakeRecoveryManifest:'relative.json'},
 {wakeRecoveryMode:'deliver',wakeRecoveryManifest:'/synthetic/exact.json'},
])('rejects invalid startup contract before acquiring launch lock or creating files: %j',async opts=>{
 const acquireStartLock=vi.fn(),spawn=vi.fn(),mkdirp=vi.fn(),writeFile=vi.fn();
 await expect(startDaemon(opts as never,{acquireStartLock,spawn,mkdirp,writeFile} as unknown as LifecycleDeps)).rejects.toThrow('absolute exact cohort');
 expect(acquireStartLock).not.toHaveBeenCalled();expect(spawn).not.toHaveBeenCalled();expect(mkdirp).not.toHaveBeenCalled();expect(writeFile).not.toHaveBeenCalled();
});
it('supports explicit normal delivery after disposition rather than inheriting a permanent stop',()=>{
 expect(validateWakeRecoveryOptions({})).toBe('deliver');expect(validateWakeRecoveryOptions({wakeRecoveryMode:'deliver'})).toBe('deliver');
});
