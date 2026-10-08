import { Command } from 'commander';
import { afterEach, expect, it, vi } from 'vitest';
import { seatCommand } from '../src/commands/seat.js';
import { DaemonClient } from '../src/client.js';
import type { StatusDeps } from '../src/commands/status.js';

const generation = '12345678-1234-1234-1234-123456789abc';
const attempt = 'abcdef12-1234-1234-1234-123456789abc';
const hash = 'a'.repeat(64);
const token = 'test-only-maintenance-private-bearer';
const args = ['seat', 'operator-maintenance', '--reason', 'repair exact retained runner', '--expected-node', 'operator-node', '--expected-generation', generation, '--json'];
const originalExit = process.exitCode;
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs(); process.exitCode = originalExit; });

function fixture(url = 'http://127.0.0.1:17538', result = { ok: true, attemptId: attempt, receiptPath: '/private/retained/receipt.json' }, status = 200) {
  vi.stubEnv('OPENRIG_URL', url);
  vi.stubEnv('OPENRIG_TERMINAL_BEARER_TOKEN', token);
  vi.stubEnv('OPENRIG_SESSION_NAME', 'inherited-production@xv');
  vi.stubEnv('OPENRIG_OCCUPANT_GENERATION', 'inherited-generation');
  const fetchImpl = vi.fn(async (_url: string | URL | Request, _init?: RequestInit) => new Response(JSON.stringify(result), { status }));
  const client = new DaemonClient(url, { fetchImpl: fetchImpl as typeof fetch });
  const deps = { clientFactory: vi.fn(() => client), lifecycleDeps: {} } as unknown as StatusDeps;
  const program = new Command().addCommand(seatCommand(deps));
  function override(cmd: Command) { cmd.exitOverride(); cmd.configureOutput({writeErr:()=>{}}); for (const sub of cmd.commands) override(sub); }
  override(program);
  const logs: string[] = [];
  vi.spyOn(console, 'log').mockImplementation((...parts) => { logs.push(parts.join(' ')); });
  vi.spyOn(console, 'error').mockImplementation((...parts) => { logs.push(parts.join(' ')); });
  process.exitCode = undefined;
  const run = (extra: string[] = [], actualArgs = args) => program.parseAsync([...actualArgs, ...extra], { from: 'user' });
  return { run, fetchImpl, logs, deps };
}

it.each([false, true])('registered maintenance command sends one exact authenticated body, with recovery=%s and no agent headers or identity probe', async recovery => {
  const f = fixture(); await f.run(recovery ? ['--attempt-id', attempt, '--began-sha256', hash] : []);
  expect(f.fetchImpl).toHaveBeenCalledOnce();
  const [url, init] = f.fetchImpl.mock.calls[0]!;
  expect(url).toBe('http://127.0.0.1:17538/api/seat/operator-maintenance/rehost-runner');
  expect(init?.method).toBe('POST'); expect(init?.redirect).toBe('error');
  const headers = new Headers(init?.headers);
  expect(headers.get('Authorization')).toBe(`Bearer ${token}`);
  for (const name of ['X-OpenRig-Session', 'X-OpenRig-Occupant-Generation', 'X-OpenRig-Origin-Unknown', 'Origin']) expect(headers.has(name)).toBe(false);
  expect(JSON.parse(String(init?.body))).toEqual({ reason: 'repair exact retained runner', expected: { nodeId: 'operator-node', generation }, ...(recovery ? { codexStoppedRecovery: { attemptId: attempt, beganSha256: hash } } : {}) });
  expect(JSON.parse(f.logs[0]!)).toEqual({ok:true,attemptId:attempt,receiptPath:'/private/retained/receipt.json'});
  expect(f.logs.join(' ')).not.toContain(token); expect(process.exitCode).toBeUndefined();
});

it.each([
 ['--attempt-id', attempt], ['--began-sha256', hash], ['--attempt-id', 'not-uuid', '--began-sha256', hash],
 ['--attempt-id', attempt, '--began-sha256', 'A'.repeat(64)], ['--reason', ' '], ['--expected-node', ' '], ['--expected-generation', ' '],
 ['--legacy-codex-profile', '../profile'], ['--legacy-codex-profile', ''],
 ['--legacy-codex-profile', 'kernel-luna-high', '--attempt-id', attempt, '--began-sha256', hash],
].map(extra => [extra]))('malformed flags have no request: %j', async extra => {
  const f = fixture(); await f.run(extra); expect(f.fetchImpl).not.toHaveBeenCalled(); expect(process.exitCode).toBe(1); expect(f.logs.join(' ')).not.toContain(token);
});
it('sends an explicit legacy profile and guard request through terminal maintenance with no agent impersonation',async()=>{
 const f=fixture();await f.run(['--legacy-codex-profile','kernel-luna-high','--enable-guard']);
 expect(f.fetchImpl).toHaveBeenCalledOnce();const init=f.fetchImpl.mock.calls[0]![1];
 expect(JSON.parse(String(init?.body))).toEqual({reason:'repair exact retained runner',expected:{nodeId:'operator-node',generation},legacyCodexProfile:'kernel-luna-high',enableGuard:true});
 const headers=new Headers(init?.headers);expect(headers.get('Authorization')).toBe(`Bearer ${token}`);
 expect(headers.has('X-OpenRig-Session')).toBe(false);expect(headers.has('X-OpenRig-Occupant-Generation')).toBe(false);
});
it.each(['--operator', '--actor'])('does not expose caller actor flag %s', async flag => {
  const f = fixture(); await expect(f.run([flag,'operator-agent@kernel'])).rejects.toThrow(); expect(f.fetchImpl).not.toHaveBeenCalled();
});
it('requires the expected target pins', async () => {
  const f=fixture();await expect(f.run([],['seat','operator-maintenance','--reason','repair'])).rejects.toThrow();expect(f.fetchImpl).not.toHaveBeenCalled();
});
it.each(['https://remote.example', 'http://localhost:17538', 'http://127.0.0.1:17538/private', 'http://user:password@127.0.0.1:17538'])('rejects unsafe endpoint without sending credentials: %s',async url=>{
  const f=fixture(url);await f.run();expect(f.fetchImpl).not.toHaveBeenCalled();expect(process.exitCode).toBe(1);expect(f.logs.join(' ')).not.toContain(token);expect(f.logs.join(' ')).not.toContain('password');
});
it('requires configured bearer without any request',async()=>{
  const f=fixture();vi.stubEnv('OPENRIG_TERMINAL_BEARER_TOKEN','');vi.stubEnv('OPENRIG_HOME','/nonexistent/operator-maintenance-fixture-home');await f.run();expect(f.fetchImpl).not.toHaveBeenCalled();expect(process.exitCode).toBe(1);
});
it.each([200,403,503])('server refusal stays nonzero and retains exact server receipt at HTTP %s',async status=>{
  const f=fixture('http://127.0.0.1:17538',{ok:false,attemptId:attempt,receiptPath:'/private/retained/refusal.json'} as any,status);await f.run();expect(f.fetchImpl).toHaveBeenCalledOnce();expect(process.exitCode).toBe(status>=500?2:1);expect(JSON.parse(f.logs[0]!)).toMatchObject({ok:false,attemptId:attempt});
});
it('60-second timeout retains UNKNOWN attempt with one POST, no retry or another stop and no error credential echo',async()=>{
  vi.useFakeTimers();const f=fixture();f.fetchImpl.mockImplementation((_url,_init)=>new Promise((_resolve,reject)=>{_init?.signal?.addEventListener('abort',()=>reject(new Error(token)));}));
  const pending=f.run(['--attempt-id',attempt,'--began-sha256',hash]);await vi.advanceTimersByTimeAsync(59999);expect(f.logs).toHaveLength(0);await vi.advanceTimersByTimeAsync(1);await pending;
  expect(f.fetchImpl).toHaveBeenCalledOnce();expect(JSON.parse(f.logs[0]!)).toMatchObject({ok:false,status:'unknown',codexStoppedRecovery:{attemptId:attempt,beganSha256:hash}});expect(process.exitCode).toBe(1);expect(f.logs.join(' ')).not.toContain(token);
});
it('literal IPv6 loopback uses same isolated maintenance contract',async()=>{
 const f=fixture('http://[::1]:17538');await f.run();expect(f.fetchImpl).toHaveBeenCalledOnce();expect(new Headers(f.fetchImpl.mock.calls[0]![1]?.headers).has('X-OpenRig-Session')).toBe(false);
});
