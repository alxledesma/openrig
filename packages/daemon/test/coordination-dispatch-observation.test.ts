import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type Database from 'better-sqlite3';
import { Hono } from 'hono';
import { createDb } from '../src/db/connection.js';
import { EventBus } from '../src/domain/event-bus.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import { SeatDeliveryGuard, resolveGuardTarget } from '../src/domain/seat-delivery-guard.js';
import { CoordinationRecoveryService, type CoordinationActivity, type CoordinationTask, type CoordinationPlan } from '../src/domain/coordination-recovery-service.js';
import { CoordinatorFenceError, digest } from '../src/domain/coordinator-authority-service.js';
import { makeCoordinatorContinuityPolicy } from '../src/domain/policies/coordinator-continuity.js';
import { coordinatorRoutes } from '../src/routes/coordinator.js';
import { seed, token } from './helpers/coordinator-fixture.js';

// Real SQLite/authority/queue/HTTP/policy paths; only the two native producers and
// transport are deterministic seams. A wake is never used as proof of pickup.
describe('shared prepared dispatch observations', () => {
  let dir: string, db: Database.Database, repo: QueueRepository, svc: CoordinationRecoveryService, clock: number;
  let samples: Map<string, CoordinationActivity>, identities: string[], activities: string[], sent: string[];
  let identityHook: ((owner: string) => Promise<void>) | undefined;
  let activityHook: ((owner: string) => Promise<void>) | undefined;
  const sessions = ['lead@xv', 'peer@xv', 'builder@xv', 'reviewer@xv', 'architect@xv', 'operator-agent@kernel'];
  const queueId = (key: string) => 'qitem-coordination-' + digest('xv:' + key).slice(0, 24);
  function sample(owner: string, at: number): CoordinationActivity {
    return { generation: repo.coordinatorAuthority.generation(owner)!, identityVerified: true, identityObservedAt: new Date(at).toISOString(),
      state: { seatNodeId: owner, activity: 'idle-at-prompt', needsInput: { count: 0, reason: null }, decidedBy: 'window-sampling', seq: 1, changedAt: new Date(at).toISOString(), rungs: [], lastSwap: null },
      witness: { seatNodeId: owner, sessionName: owner, rung: 'window-sampling', sourceId: 'tmux', seq: 1, observedAt: new Date(at).toISOString(), activity: 'idle-at-prompt' } };
  }
  function task(key: string, owner = 'builder@xv', more: Partial<CoordinationTask> = {}): CoordinationTask {
    return { key, packageKey: key, owner, action: 'Perform exact ' + key, deadline: clock + 200_000, body: key, predecessors: [],
      admission: { generation: repo.coordinatorAuthority.generation(owner)!, configurationDigest: svc.configurationDigest(owner)!, qualificationRef: 'approved/' + key, capacityRef: 'capacity/' + key, effortRef: 'effort/' + key, validUntil: clock + 300_000 }, ...more };
  }
  const primary = () => [task('A'), task('A-repair', 'architect@xv', { recoveryFor: 'A' })];
  function admit(tasks: CoordinationTask[]) {
    for (const t of tasks) repo.coordinatorAuthority.admit('operator-agent@kernel', 'operator-agent-g1', 'xv', t.packageKey,
      { inputDigest: digest(t.key), destination: t.owner, bodyHash: digest(t.body), resources: [], returnContract: { destination: 'lead@xv', evidenceRequired: ['report'] } });
  }
  function configure(tasks = primary(), more: Partial<CoordinationPlan> = {}) {
    admit(tasks);
    return svc.configure('operator-agent@kernel', 'operator-agent-g1', { rigId: 'xv', revision: 'r1', operatorGeneration: 'operator-agent-g1', stallMs: 10000, allowIdlePeerTransfer: false, refreshDispatchIdentity: true, ...more, tasks });
  }
  function prebound() {
    const tasks = [...primary(), task('B', 'reviewer@xv'), task('B-repair', 'architect@xv', { recoveryFor: 'B' })];
    admit(tasks);
    const contract = db.prepare("SELECT contract_hash FROM coordinator_packages WHERE rig_id='xv' AND package_key='A'").get() as { contract_hash: string };
    tasks[2]!.predecessors = [{ packageKey: 'A', contractHash: contract.contract_hash, queueId: queueId('A') }];
    return configure(tasks);
  }
  function job() {
    db.prepare("INSERT INTO watchdog_jobs(job_id,target_session,policy,interval_seconds,spec_yaml,state,registered_by_session,registered_at,registered_by_generation_uuid) VALUES('j','operator-agent@kernel','coordinator-continuity',1,'context: {}','active','operator-agent@kernel',?,'operator-agent-g1')").run(new Date(clock).toISOString());
  }
  async function post(operation: string, body: Record<string, unknown>) {
    const app = new Hono();
    app.use('*', async (c, next) => { c.set('queueRepo' as never, repo as never); await next(); });
    app.route('/coordinator', coordinatorRoutes({ bearerToken: 'fixture-only-token' }));
    const response = await app.request('/coordinator/' + operation, { method: 'POST', headers: { Authorization: 'Bearer fixture-only-token', 'Content-Type': 'application/json', 'X-OpenRig-Session': 'lead@xv', 'X-OpenRig-Occupant-Generation': 'lead-g1' }, body: JSON.stringify(body) });
    expect(response.status).toBe(200);
    return response.json();
  }
  async function returnedA() {
    await post('coordination-reconcile', { rigId: 'xv' });
    expect(repo.getById(queueId('A'))?.state).toBe('pending');
    repo.claim({ qitemId: queueId('A'), destinationSession: 'builder@xv', actorGeneration: 'builder-g1', identityProvenance: 'transport:v1' });
    repo.update({ qitemId: queueId('A'), actorSession: 'builder@xv', actorGeneration: 'builder-g1', identityProvenance: 'transport:v1', state: 'done', closureReason: 'no-follow-on' });
    await repo.create({ qitemId: 'A-return', sourceSession: 'builder@xv', destinationSession: 'lead@xv', body: JSON.stringify({ packageKey: 'A', inputDigest: digest('A'), evidence: [{ kind: 'report', ref: 'actual/A.md' }] }), nudge: false });
    repo.coordinatorAuthority.dispose('builder@xv', 'builder-g1', 'xv', 'A', 'A-return');
    await svc.deliverCommitted();
    clock += 4000; vi.setSystemTime(clock); identities.length = 0; activities.length = 0;
  }
  const assignments = () => db.prepare('SELECT * FROM coordinator_assignments').all();
  const acceptance = () => db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id='coordination-accept:A'").get();
  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['Date'] }); clock = Date.now();
    dir = mkdtempSync(join(tmpdir(), 'dispatch-observation-')); db = createDb(join(dir, 'db')); seed(db);
    db.prepare("INSERT INTO self_host_identity VALUES(1,'fixture-host',?,?)").run(new Date(clock).toISOString(), new Date(clock).toISOString());
    repo = new QueueRepository(db, new EventBus(db), { resolveOccupantGeneration: owner => repo.coordinatorAuthority.generation(owner) });
    repo.attachOutbox(new OutboxHandler(db)); identities = []; activities = []; sent = []; identityHook = undefined; activityHook = undefined;
    for (const owner of sessions) db.prepare('INSERT INTO bindings(id,node_id,tmux_session,tmux_pane) VALUES(?,?,?,?)').run('binding-' + owner, owner, owner, '%1');
    repo.attachTransport({ send: async (owner, _text, opts) => { expect(db.inTransaction).toBe(false); repo.coordinatorAuthority.assertManagedSend(opts?.actorSession, owner, opts?.queueAssignmentId); sent.push(opts!.queueAssignmentId!); return { ok: true, verified: true }; } });
    await repo.create({ qitemId: 'baton', sourceSession: 'operator-agent@kernel', destinationSession: 'lead@xv', body: 'coordinate', nudge: false });
    repo.coordinatorAuthority.enable('operator-agent@kernel', 'operator-agent-g1', { rigId: 'xv', batonId: 'baton', owner: 'lead@xv', ownerGeneration: 'lead-g1', coordinators: ['lead@xv', 'peer@xv'], leaseMs: 60000, operationId: 'enable' });
    repo.coordinatorAuthority.acknowledge('lead@xv', token, { operationId: 'ack', obligationsDigest: repo.coordinatorAuthority.reconciliationDigest('xv') });
    samples = new Map(sessions.map(owner => [owner, sample(owner, clock - 5000)]));
    svc = new CoordinationRecoveryService(repo, owner => samples.get(owner) ?? null, () => clock, async owners => {
      expect(db.inTransaction).toBe(false);
      for (const owner of owners) { identities.push(owner); samples.set(owner, { ...samples.get(owner)!, identityObservedAt: new Date(clock).toISOString() }); await identityHook?.(owner); }
    }, async owner => {
      expect(db.inTransaction).toBe(false); activities.push(owner);
      const current = samples.get(owner)!; const fresh = sample(owner, clock);
      samples.set(owner, { ...current, state: fresh.state, witness: fresh.witness }); await activityHook?.(owner);
    });
    repo.coordinatorAuthority.coordinationRecovery = svc;
  });
  afterEach(() => { db.close(); rmSync(dir, { recursive: true, force: true }); vi.useRealTimers(); });

  it('direct HTTP reconcile observes the ready primary plus separate native duties without a manual probe', async () => {
    configure();
    const result = await post('coordination-reconcile', { rigId: 'xv', prepared: { identityObservedAt: new Date(clock).toISOString() } });
    expect(result).toEqual(expect.arrayContaining([expect.objectContaining({ key: 'A', state: 'pending-pickup', queueId: queueId('A') })]));
    expect(identities.sort()).toEqual(['builder@xv', 'lead@xv', 'operator-agent@kernel']); expect(activities.sort()).toEqual(['builder@xv', 'lead@xv', 'operator-agent@kernel']);
    expect(assignments()).toHaveLength(1); expect(sent).toContain(queueId('A'));
    expect(repo.getById(queueId('A'))?.claimedAt).toBeNull();
  });
  it('HTTP acceptance commits first, refreshes exact successor, and creates one distinct B', async () => {
    prebound(); await returnedA();
    identityHook = async () => { expect(acceptance()).toBeTruthy(); };
    const result = await post('coordination-accept', { rigId: 'xv', packageKey: 'A', dispositionId: 'A-return', evidenceRef: 'actual/accept-A.md' });
    expect(result).toMatchObject({ ok: true, accepted: true });
    expect(identities.sort()).toEqual(['lead@xv', 'operator-agent@kernel', 'reviewer@xv']); expect(activities.sort()).toEqual(['lead@xv', 'operator-agent@kernel', 'reviewer@xv']);
    expect(repo.getById(queueId('A'))?.state).toBe('done'); expect(repo.getById(queueId('B'))?.state).toBe('pending');
    expect(sent).toContain(queueId('B')); expect(assignments()).toHaveLength(2);
    const resolution = db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-predecessor-resolution'").get() as { receipt: string };
    expect(JSON.parse(resolution.receipt).predecessors[0]).toMatchObject({ packageKey: 'A', queueId: queueId('A'), dispositionId: 'A-return' });
    await post('coordination-reconcile', { rigId: 'xv' }); expect(assignments()).toHaveLength(2);
    // Pickup is a separate genuine fixture claim, never an inferred wake effect.
    repo.claim({ qitemId: queueId('B'), destinationSession: 'reviewer@xv', actorGeneration: 'reviewer-g1', identityProvenance: 'transport:v1' });
    expect(repo.getById(queueId('B'))?.state).toBe('in-progress');
  });
  it('registered automatic policy refreshes stale accepted predecessor frontier and dispatches exactly one B', async () => {
    prebound(); await returnedA(); svc.accept('lead@xv', 'lead-g1', 'xv', 'A', 'A-return', 'actual/accept-A.md');
    expect(repo.getById(queueId('B'))).toBeNull(); job();
    await makeCoordinatorContinuityPolicy(repo.coordinatorAuthority, async () => {}).evaluate({ jobId: 'j', registeredBySession: 'operator-agent@kernel', target: { session: 'operator-agent@kernel' }, context: { rigId: 'xv' } } as any);
    expect(identities).toContain('reviewer@xv'); expect(activities).toContain('reviewer@xv');
    expect(identities).not.toContain('builder@xv'); expect(identities).not.toContain('architect@xv');
    expect(repo.getById(queueId('B'))?.state).toBe('pending'); expect(assignments()).toHaveLength(2); expect(sent).toContain(queueId('B'));
  });
  it('a slow coordinator duty observer cannot delay a ready owner assignment or wake', async () => {
    configure(primary(), { allowIdlePeerTransfer: true }); job();
    let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
    identityHook = async owner => { if (owner === 'peer@xv') await blocked; };
    const running = svc.supervisePrepared('xv', 'j');
    for (let i = 0; i < 60 && !sent.includes(queueId('A')); i++) await Promise.resolve();
    expect(repo.getById(queueId('A'))?.state).toBe('pending'); expect(sent).toContain(queueId('A'));
    clock += 5000; vi.setSystemTime(clock); release(); await running;
    expect(assignments()).toHaveLength(1);
  });
  it('a slow eligible owner does not age or block another owner and cannot dispatch on stale completed evidence', async () => {
    configure([...primary(), task('slow', 'reviewer@xv'), task('slow-repair', 'architect@xv', { recoveryFor: 'slow' })]);
    let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
    identityHook = async owner => { if (owner === 'reviewer@xv') await blocked; };
    const running = svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    for (let i = 0; i < 60 && !sent.includes(queueId('A')); i++) await Promise.resolve();
    expect(sent).toContain(queueId('A')); expect(repo.getById(queueId('slow'))).toBeNull();
    clock += 4000; vi.setSystemTime(clock); release(); await running;
    expect(repo.getById(queueId('slow'))).toBeNull(); expect(assignments()).toHaveLength(1);
  });
  it('one failed owner observer leaves that owner held while another progresses', async () => {
    configure([...primary(), task('unavailable', 'reviewer@xv'), task('unavailable-repair', 'architect@xv', { recoveryFor: 'unavailable' })]);
    identityHook = async owner => { if (owner === 'reviewer@xv') throw new Error('fixture unavailable'); };
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect(repo.getById(queueId('A'))?.state).toBe('pending'); expect(repo.getById(queueId('unavailable'))).toBeNull();
  });
  it('unrelated blocked, expired and dormant owners never enter product preparation', async () => {
    const tasks = [...primary(), task('blocked', 'reviewer@xv', { boundary: 'owner-material' }), task('expired', 'reviewer@xv'), task('expired-repair', 'architect@xv', { recoveryFor: 'expired', boundary: 'owner-material' })];
    configure(tasks); clock += 1000; vi.setSystemTime(clock);
    const retained = svc.plan('xv')!;
    db.prepare("UPDATE coordinator_operations SET receipt=? WHERE operation_id='coordination-plan:r1'").run(JSON.stringify({ ...retained, tasks: retained.tasks.map(t => t.key === 'expired' ? { ...t, deadline: clock - 1 } : t) }));
    identityHook = async owner => { expect(['builder@xv', 'lead@xv', 'operator-agent@kernel']).toContain(owner); };
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect(identities.sort()).toEqual(['builder@xv', 'lead@xv', 'operator-agent@kernel']); expect(assignments()).toHaveLength(1);
  });
  it('refreshes a recovery target working under lapsed admission and suppresses its backup without starving independent work', async () => {
    const original = task('A'); original.admission.validUntil = clock + 1;
    configure([original, task('A-repair', 'architect@xv', { recoveryFor: 'A' }), task('independent', 'reviewer@xv'), task('independent-repair', 'architect@xv', { recoveryFor: 'independent' })]);
    clock += 4000; vi.setSystemTime(clock);
    const target = samples.get('builder@xv')!; target.state.activity = 'working'; target.witness!.activity = 'working';
    let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
    identityHook = async owner => { if (owner === 'builder@xv') await blocked; };
    activityHook = async owner => { if (owner === 'builder@xv') { const busy = samples.get(owner)!; busy.state.activity = 'working'; busy.witness!.activity = 'working'; } };
    const running = svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    for (let i = 0; i < 60 && !sent.includes(queueId('independent')); i++) await Promise.resolve();
    expect(sent).toContain(queueId('independent')); expect(repo.getById(queueId('A-repair'))).toBeNull();
    release(); const result = await running;
    expect(identities).toContain('builder@xv'); expect(activities).toContain('builder@xv');
    expect(result).toEqual(expect.arrayContaining([expect.objectContaining({ key: 'A-repair', state: 'held', reason: 'recovery-not-needed' })]));
    expect(repo.getById(queueId('A'))).toBeNull(); expect(repo.getById(queueId('A-repair'))).toBeNull();
    expect(repo.getById(queueId('independent'))?.state).toBe('pending');
  });
  it.each(['failed', 'blocked', 'overdue'] as const)('D1 dispatches recovery for an unobservable %s assigned primary', async kind => {
    const original = task('A'); original.deadline = clock + 1000;
    configure([original, task('A-repair', 'architect@xv', { recoveryFor: 'A' })]);
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    repo.claim({ qitemId: queueId('A'), destinationSession: 'builder@xv', actorGeneration: 'builder-g1', identityProvenance: 'transport:v1' });
    if (kind !== 'overdue') repo.update({ qitemId: queueId('A'), actorSession: 'builder@xv', actorGeneration: 'builder-g1', identityProvenance: 'transport:v1', state: kind });
    clock += 4000; vi.setSystemTime(clock);
    const retained = repo.getById(queueId('A'));
    identityHook = async owner => { if (owner === 'builder@xv') throw new Error('fixture unreadable primary'); };
    activityHook = async owner => { if (owner === 'builder@xv') throw new Error('fixture unreadable primary'); };
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect(repo.getById(queueId('A-repair'))?.state).toBe('pending'); expect(sent).toContain(queueId('A-repair'));
    expect(repo.getById(queueId('A'))).toEqual(retained);
    expect(identities).toContain('builder@xv'); expect(activities).toContain('builder@xv');
    expect(assignments()).toHaveLength(2);
  });
  it('D1 dispatches recovery for an undispatched primary with no native generation', async () => {
    configure(); db.prepare("DELETE FROM occupant_tenures WHERE node_id='builder@xv'").run();
    expect(repo.coordinatorAuthority.generation('builder@xv')).toBeNull();
    identityHook = async owner => { if (owner === 'builder@xv') throw new Error('fixture retired primary'); };
    activityHook = async owner => { if (owner === 'builder@xv') throw new Error('fixture retired primary'); };
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect(repo.getById(queueId('A'))).toBeNull(); expect(repo.getById(queueId('A-repair'))?.state).toBe('pending');
    expect(sent).toContain(queueId('A-repair')); expect(assignments()).toHaveLength(1);
  });
  it.each(['unavailable-target', 'changed-target-generation', 'stale-target'] as const)('D1 keeps existing recovery eligibility for %s under lapsed admission', async kind => {
    const original = task('A'); original.admission.validUntil = clock + 1;
    configure([original, task('A-repair', 'architect@xv', { recoveryFor: 'A' })]); clock += 4000; vi.setSystemTime(clock);
    identityHook = async owner => {
      if (owner !== 'builder@xv') return;
      if (kind === 'unavailable-target') throw new Error('fixture unavailable');
      if (kind === 'changed-target-generation') db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
      if (kind === 'stale-target') samples.get(owner)!.identityObservedAt = new Date(clock - 3001).toISOString();
    };
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect(repo.getById(queueId('A-repair'))?.state).toBe('pending'); expect(sent).toContain(queueId('A-repair'));
    expect(repo.getById(queueId('A'))).toBeNull(); expect(assignments()).toHaveLength(1);
  });
  it('settles a scoped typed failure, waits for independent work and still runs final administrative supervision', async () => {
    configure([...primary(), task('independent', 'reviewer@xv'), task('independent-repair', 'architect@xv', { recoveryFor: 'independent' })]); job();
    let release!: () => void; const blocked = new Promise<void>(resolve => { release = resolve; });
    identityHook = async owner => { if (owner === 'reviewer@xv') await blocked; };
    const original = (svc as any).superviseScoped.bind(svc); let finalPasses = 0, failed = false;
    vi.spyOn(svc as any, 'superviseScoped').mockImplementation((rigId: unknown, jobId: unknown, scope: unknown) => {
      const prepared = scope as ReadonlyMap<string, unknown>;
      if (prepared.size === 0) finalPasses++;
      if (prepared.has('builder@xv')) { failed = true; throw new CoordinatorFenceError('coordinator_retired', 'Injected scoped fence failure'); }
      return original(rigId, jobId, scope);
    });
    let finished = false; const running = svc.supervisePrepared('xv', 'j').then(result => { finished = true; return result; });
    for (let i = 0; i < 60 && !failed; i++) await Promise.resolve();
    expect(failed).toBe(true); expect(finished).toBe(false); expect(finalPasses).toBe(0);
    release(); const result = await running;
    expect(repo.getById(queueId('independent'))?.state).toBe('pending'); expect(sent).toContain(queueId('independent'));
    expect(finalPasses).toBe(1); expect(repo.getById(queueId('A'))).toBeNull();
    expect(result).toEqual(expect.arrayContaining([expect.objectContaining({ key: 'dispatch:builder@xv', state: 'held', reason: 'coordinator_retired' })]));
    const failure = db.prepare("SELECT receipt FROM coordinator_operations WHERE kind='coordination-dispatch-hold'").get() as { receipt: string };
    expect(JSON.parse(failure.receipt)).toMatchObject({ state: 'held', reason: 'coordinator_retired', activityEvidence: { stage: 'scoped-reconcile', owner: 'builder@xv' } });
  });
  it.each(['plan', 'generation', 'configuration', 'epoch', 'stale', 'busy', 'wrong-seat', 'needs-input', 'unknown', 'custody'] as const)('revalidates %s at the actual assignment boundary', async kind => {
    configure();
    activityHook = async owner => {
      if (owner !== 'builder@xv') return;
      if (kind === 'plan') svc.configure('operator-agent@kernel', 'operator-agent-g1', { ...svc.plan('xv')!, revision: 'drift-r2' });
      if (kind === 'generation') db.prepare("UPDATE occupant_tenures SET generation_uuid='builder-g2' WHERE node_id='builder@xv'").run();
      if (kind === 'configuration') db.prepare("UPDATE nodes SET model='changed' WHERE id='builder@xv'").run();
      if (kind === 'epoch') db.prepare("UPDATE coordinator_authority SET epoch=epoch+1 WHERE rig_id='xv'").run();
      if (kind === 'stale') { clock += 3001; vi.setSystemTime(clock); }
      const current = samples.get(owner)!;
      if (kind === 'busy') { current.state.activity = 'working'; current.witness!.activity = 'working'; }
      if (kind === 'wrong-seat') current.witness!.sessionName = 'other@rig';
      if (kind === 'needs-input') current.state.needsInput.count = 1;
      if (kind === 'unknown') db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('unknown-original','watchdog@system','builder@xv','preserved-unknown-bytes',?,'indeterminate')").run(new Date(clock).toISOString());
      if (kind === 'custody') {
        repo.coordinatorAuthority.admit('operator-agent@kernel', 'operator-agent-g1', 'xv', 'intervening', { inputDigest: digest('intervening'), destination: owner, bodyHash: digest('existing duty'), resources: [], returnContract: { destination: 'lead@xv', evidenceRequired: ['report'] } });
        await repo.create({ qitemId: 'other-custody', sourceSession: 'lead@xv', destinationSession: owner, body: 'existing duty', dispatch: { token, packageKey: 'intervening' }, nudge: false });
      }
    };
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect((assignments() as Array<{package_key:string}>).filter(row => row.package_key === 'A')).toHaveLength(0); expect(repo.getById(queueId('A'))).toBeNull();
    if (kind === 'unknown') expect(db.prepare("SELECT body,delivery_state FROM outbox_entries WHERE outbox_id='unknown-original'").get()).toEqual({ body: 'preserved-unknown-bytes', delivery_state: 'indeterminate' });
    if (kind === 'custody') expect(repo.getById('other-custody')?.state).toBe('pending');
  });
  it.each(['pending', 'sending', 'indeterminate'] as const)('preserves existing %s effect debt while freshly observing its eligible recovery pair', async state => {
    configure(); db.prepare("INSERT INTO outbox_entries(outbox_id,sender_session,destination_session,body,ts_dispatched,delivery_state) VALUES('retained-effect','watchdog@system','builder@xv','original',?,?)").run(new Date(clock).toISOString(), state);
    const original = db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='retained-effect'").get();
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv');
    expect(identities).toContain('builder@xv'); expect(identities).toContain('architect@xv'); expect(repo.getById(queueId('A'))).toBeNull();
    // Distinct admitted recovery remains eligible; it never disposes or retries this effect.
    expect((assignments() as Array<{package_key:string}>).every(row => row.package_key === 'A-repair')).toBe(true);
    expect(db.prepare("SELECT * FROM outbox_entries WHERE outbox_id='retained-effect'").get()).toEqual(original);
  });
  it('observer failure after HTTP acceptance reports acceptance as committed and leaves B held', async () => {
    prebound(); await returnedA(); identityHook = async () => { expect(acceptance()).toBeTruthy(); throw new Error('native observation unavailable'); };
    const accepted = await post('coordination-accept', { rigId: 'xv', packageKey: 'A', dispositionId: 'A-return', evidenceRef: 'actual/accept-A.md' });
    expect(accepted).toMatchObject({ ok: true, accepted: true }); expect(acceptance()).toBeTruthy(); expect(repo.getById(queueId('B'))).toBeNull();
    const before = acceptance(); identityHook = undefined;
    await svc.reconcilePrepared('lead@xv', 'lead-g1', 'xv'); expect(acceptance()).toEqual(before); expect(repo.getById(queueId('B'))?.state).toBe('pending');
  });
  it('holder retirement after acceptance cannot roll back or report rejected acceptance', async () => {
    prebound(); await returnedA();
    identityHook = async () => { db.prepare("UPDATE coordinator_authority SET owner_session='peer@xv',owner_generation='peer-g1' WHERE rig_id='xv'").run(); };
    const result = await post('coordination-accept', { rigId: 'xv', packageKey: 'A', dispositionId: 'A-return', evidenceRef: 'actual/accept-A.md' });
    expect(result).toMatchObject({ ok: true, accepted: true, dispatchError: { code: 'coordinator_retired' } });
    expect(acceptance()).toBeTruthy(); expect(repo.getById(queueId('B'))).toBeNull();
  });
  it('expired administrative history caller excludes only exact contained history and preserves live custody and UNKNOWN', async () => {
    const outbox = new OutboxHandler(db); repo.attachOutbox(outbox);
    const guard = new SeatDeliveryGuard(db, owner => resolveGuardTarget(db, owner));
    const outboxId = 'caller-contained-direct-effect', body = 'immutable UNKNOWN effect';
    outbox.record({ outboxId, senderSession: 'builder@xv', destinationSession: 'lead@xv', body, auditPointer: 'caller-original-history' });
    const expired = { authorizationId: 'caller-expired-sibling', deadline: clock + 1000, contract: { kind: 'outbox-abandon-authorization' as const, outboxId, bodySha256: digest(body), expectedState: 'pending' as const, operationId: 'caller-proposed-never-executed', senderGeneration: 'builder-g1', reason: 'withdraw obsolete effect preserving UNKNOWN', evidenceRef: 'fixture/caller-original-proof' } };
    const actual = { authorizationId: 'caller-actual-retirement', deadline: clock + 60000, contract: { ...expired.contract, operationId: 'caller-genuine-retirement' } };
    await repo.issueOutboxAbandonAuthorization('operator-agent@kernel', 'operator-agent-g1', expired, guard);
    db.prepare("UPDATE outbox_entries SET delivery_state='failed' WHERE outbox_id=?").run('wake-intent-outbox-abandon-notification:' + expired.authorizationId);
    await repo.issueOutboxAbandonAuthorization('operator-agent@kernel', 'operator-agent-g1', actual, guard);
    repo.claim({ qitemId: actual.authorizationId, destinationSession: 'builder@xv', actorGeneration: 'builder-g1', identityProvenance: 'transport:v1' });
    await outbox.abandonUncertain({ ...actual.contract, authorizationId: actual.authorizationId, actor: 'builder@xv', generation: 'builder-g1' }, guard);
    clock += 2000; vi.setSystemTime(clock);
    const snapshot = () => ({ queue: db.prepare('SELECT * FROM queue_items ORDER BY qitem_id').all(), outbox: db.prepare('SELECT * FROM outbox_entries ORDER BY outbox_id').all(), operations: db.prepare('SELECT * FROM coordinator_operations ORDER BY operation_id').all(), transitions: db.prepare('SELECT * FROM queue_transitions ORDER BY transition_id').all(), custody: db.prepare('SELECT * FROM queue_native_custody_evidence ORDER BY transition_id').all(), events: db.prepare('SELECT * FROM events ORDER BY seq').all() });
    const before = snapshot();
    expect(svc.genericWatchActionable(expired.authorizationId, 'xv')).toBe(false);
    expect(svc.genericWatchActionable(expired.authorizationId)).toBe(true);
    expect(svc.genericWatchActionable(expired.authorizationId, 'other')).toBe(true);
    expect(svc.genericWatchActionable(actual.authorizationId, 'xv')).toBe(true);
    expect(snapshot()).toEqual(before);
    expect(repo.getById(expired.authorizationId)?.state).toBe('pending');
    expect(repo.getById(actual.authorizationId)?.state).toBe('in-progress');
    const event = db.prepare("SELECT payload FROM events WHERE type='outbox.uncertain_abandoned'").get() as { payload: string };
    expect(JSON.parse(event.payload)).toMatchObject({ operationId: actual.contract.operationId, deliveryConclusion: 'unknown', authorizationId: actual.authorizationId });
  });
  it('unauthorized registered observation performs no native preparation or assignment', async () => {
    configure(); await expect(svc.supervisePrepared('xv', 'not-registered')).rejects.toMatchObject({ code: 'coordination_observer_not_authorized' });
    expect(identities).toEqual([]); expect(activities).toEqual([]); expect(assignments()).toHaveLength(0);
  });
});
