import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type Database from 'better-sqlite3';
import { createDb } from '../src/db/connection.js';
import { EventBus } from '../src/domain/event-bus.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { OutboxHandler } from '../src/domain/outbox-handler.js';
import {
  CoordinationRecoveryService,
  type CoordinationActivity,
  type CoordinationTask,
  type CoordinationPlan,
} from '../src/domain/coordination-recovery-service.js';
import { digest } from '../src/domain/coordinator-authority-service.js';
import { seed, token } from './helpers/coordinator-fixture.js';

describe('acceptance terminalizes its exact claimed lifecycle obligation', () => {
  let dir: string;
  let db: Database.Database;
  let repo: QueueRepository;
  let service: CoordinationRecoveryService;
  let bus: EventBus;
  let clock: number;
  let samples: Map<string, CoordinationActivity>;

  function sample(session: string): CoordinationActivity {
    const generation = repo.coordinatorAuthority.generation(session)!;
    const observedAt = new Date(clock).toISOString();
    return {
      generation,
      identityVerified: true,
      identityObservedAt: observedAt,
      state: {
        seatNodeId: session,
        activity: 'idle-at-prompt',
        needsInput: { count: 0, reason: null },
        decidedBy: 'window-sampling',
        seq: 1,
        changedAt: observedAt,
        rungs: [],
        lastSwap: { generation, at: observedAt },
      },
      witness: {
        seatNodeId: session,
        sessionName: session,
        rung: 'window-sampling',
        sourceId: 'tmux',
        seq: 1,
        observedAt,
        activity: 'idle-at-prompt',
      },
    };
  }

  const task = (
    key: string,
    owner: string,
    predecessors: Array<{ queueId: string; dispositionId: string }> = [],
  ): CoordinationTask => ({
    key,
    packageKey: key,
    owner,
    action: `Perform ${key} and return bounded evidence`,
    deadline: clock + 20_000,
    body: `${key}-body`,
    predecessors,
    admission: {
      generation: repo.coordinatorAuthority.generation(owner)!,
      configurationDigest: service.configurationDigest(owner)!,
      qualificationRef: `approved/non-subject/${key}`,
      capacityRef: `current/provider/${key}`,
      effortRef: `current/effort/${key}`,
      validUntil: clock + 60_000,
    },
  });

  const plan = (tasks: CoordinationTask[]): CoordinationPlan => ({
    rigId: 'xv',
    revision: 'acceptance-closure-r1',
    operatorGeneration: 'operator-agent-g1',
    stallMs: 10_000,
    allowIdlePeerTransfer: true,
    tasks,
  });

  function configure(tasks: CoordinationTask[]): void {
    for (const item of tasks) {
      repo.coordinatorAuthority.admit(
        'operator-agent@kernel',
        'operator-agent-g1',
        'xv',
        item.packageKey,
        {
          inputDigest: digest(item.key),
          destination: item.owner,
          bodyHash: digest(item.body),
          resources: [],
          returnContract: { destination: 'lead@xv', evidenceRequired: ['report'] },
        },
      );
    }
    service.configure('operator-agent@kernel', 'operator-agent-g1', plan(tasks));
  }

  async function createAcceptedProductReturn(returnClaim: 'current' | 'unclaimed' = 'current'): Promise<void> {
    const originalBody = 'exact-outside-product-body';
    repo.coordinatorAuthority.admit('operator-agent@kernel', 'operator-agent-g1', 'xv', 'outside', {
      inputDigest: digest('outside'),
      destination: 'builder@xv',
      bodyHash: digest(originalBody),
      resources: ['scope/outside.ts'],
      returnContract: { destination: 'lead@xv', evidenceRequired: ['report'] },
    });
    await repo.create({
      qitemId: 'outside-product',
      sourceSession: 'lead@xv',
      destinationSession: 'builder@xv',
      body: originalBody,
      dispatch: { token, packageKey: 'outside' },
      nudge: false,
    });
    repo.claim({
      qitemId: 'outside-product',
      destinationSession: 'builder@xv',
      actorGeneration: 'builder-g1',
      identityProvenance: 'transport:v1',
    });
    repo.update({
      qitemId: 'outside-product',
      actorSession: 'builder@xv',
      actorGeneration: 'builder-g1',
      identityProvenance: 'transport:v1',
      state: 'done',
      closureReason: 'no-follow-on',
    });
    await repo.create({
      qitemId: 'outside-return',
      sourceSession: 'builder@xv',
      destinationSession: 'lead@xv',
      body: JSON.stringify({
        packageKey: 'outside',
        inputDigest: digest('outside'),
        evidence: [{ kind: 'report', ref: 'actual/outside-report.md' }],
      }),
      nudge: false,
    });
    if (returnClaim === 'current') {
      await repo.create({ qitemId: 'return-park-blocker', sourceSession: 'builder@xv', destinationSession: 'lead@xv', body: 'live return blocker', nudge: false });
      repo.claim({ qitemId: 'outside-return', destinationSession: 'lead@xv', actorGeneration: 'lead-g1', identityProvenance: 'transport:v1' });
      repo.update({
        qitemId: 'outside-return',
        actorSession: 'lead@xv',
        actorGeneration: 'lead-g1',
        identityProvenance: 'transport:v1',
        state: 'blocked',
        blockedOn: 'return-park-blocker',
        wakeAfterSeconds: 3_600,
      });
    }
    repo.coordinatorAuthority.dispose('builder@xv', 'builder-g1', 'xv', 'outside', 'outside-return');
    db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id='wake-intent-outside-product'").run();
  }

  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    clock = Date.now();
    dir = mkdtempSync(join(tmpdir(), 'coordination-acceptance-closure-'));
    db = createDb(join(dir, 'db'));
    seed(db);
    db.prepare('INSERT INTO self_host_identity VALUES(1,?,?,?)').run('fixture-host', new Date(clock).toISOString(), new Date(clock).toISOString());
    bus = new EventBus(db);
    repo = new QueueRepository(db, bus, {
      resolveOccupantGeneration: (session) => repo.coordinatorAuthority.generation(session),
    });
    repo.attachOutbox(new OutboxHandler(db));
    await repo.create({ qitemId: 'baton', sourceSession: 'operator-agent@kernel', destinationSession: 'lead@xv', body: 'coordinate', nudge: false });
    repo.coordinatorAuthority.enable('operator-agent@kernel', 'operator-agent-g1', {
      rigId: 'xv', batonId: 'baton', owner: 'lead@xv', ownerGeneration: 'lead-g1',
      coordinators: ['lead@xv', 'peer@xv'], leaseMs: 60_000, operationId: 'enable',
    });
    repo.coordinatorAuthority.acknowledge('lead@xv', token, {
      operationId: 'ack', obligationsDigest: repo.coordinatorAuthority.reconciliationDigest('xv'),
    });
    samples = new Map();
    for (const session of ['lead@xv', 'peer@xv', 'builder@xv', 'reviewer@xv', 'architect@xv']) samples.set(session, sample(session));
    service = new CoordinationRecoveryService(repo, (session) => samples.get(session) ?? null, () => clock);
    repo.coordinatorAuthority.coordinationRecovery = service;
  });

  afterEach(() => {
    db.close();
    rmSync(dir, { recursive: true, force: true });
    vi.useRealTimers();
  });

  it('accepts the exact typed return, closes the Lead claim once, retires its park timer, and releases the declared successor', async () => {
    const next = task('next', 'reviewer@xv', [{ queueId: 'outside-product', dispositionId: 'outside-return' }]);
    configure([next, { ...task('next-repair', 'architect@xv'), recoveryFor: 'next' }]);
    await createAcceptedProductReturn();

    const rows = service.reconcile('lead@xv', 'lead-g1', 'xv');
    const acceptance = rows.find((row) => row.key === 'acceptance:outside')!;
    expect(acceptance.state).toBe('pending-native-acceptance');
    expect(rows.find((row) => row.key === 'next')?.reason).toBe('predecessor-disposition');
    const dutyId = acceptance.queueId!;
    const dutyBeforeClaim = repo.getById(dutyId)!;
    const control = service.lifecycleControlReceipt(dutyId)!;
    expect(control).toMatchObject({ kind: 'acceptance', originalQueueId: 'outside-product', dispositionId: 'outside-return', recipient: 'lead@xv', recipientGeneration: 'lead-g1' });

    repo.claim({ qitemId: dutyId, destinationSession: 'lead@xv', actorGeneration: 'lead-g1', identityProvenance: 'transport:v1' });
    db.prepare("UPDATE outbox_entries SET delivery_state='delivered' WHERE outbox_id=?").run(`wake-intent-${dutyId}`);
    await repo.create({ qitemId: 'live-park-blocker', sourceSession: 'builder@xv', destinationSession: 'lead@xv', body: 'live blocker', nudge: false });
    repo.update({
      qitemId: dutyId,
      actorSession: 'lead@xv',
      actorGeneration: 'lead-g1',
      identityProvenance: 'transport:v1',
      state: 'blocked',
      blockedOn: 'live-park-blocker',
      wakeAfterSeconds: 3_600,
    });
    const parkedDuty = repo.getById(dutyId)!;
    const parkWake = repo.getParkWakeStatus(dutyId)!;
    expect(parkedDuty.state).toBe('blocked');
    expect(parkWake?.kind).toBe('timer');
    expect(parkWake?.live).toBe(true);
    const returnWake = repo.getParkWakeStatus('outside-return')!;
    expect(repo.getById('outside-return')?.state).toBe('blocked');
    expect(returnWake?.kind).toBe('timer');
    expect(returnWake?.live).toBe(true);

    const beforeFailedAcceptance = {
      acceptCount: (db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-accept'").get() as { n: number }).n,
      wake: repo.getParkWakeStatus(dutyId),
    };
    expect(() => service.accept('peer@xv', 'peer-g1', 'xv', 'outside', 'outside-return', 'actual/outside-acceptance.md')).toThrow('Current holder');
    expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-accept'").get() as { n: number }).n).toBe(beforeFailedAcceptance.acceptCount);
    expect(repo.getById(dutyId)?.state).toBe('blocked');
    expect(repo.getParkWakeStatus(dutyId)).toEqual(beforeFailedAcceptance.wake);
    expect(() => service.accept('lead@xv', 'lead-g1', 'xv', 'outside', 'wrong-return', 'actual/outside-acceptance.md')).toThrow('Exact successful');
    expect(repo.getById(dutyId)?.state).toBe('blocked');
    expect(repo.getParkWakeStatus(dutyId)).toEqual(beforeFailedAcceptance.wake);

    const productBefore = repo.getById('outside-product')!;
    const returnBefore = repo.getById('outside-return')!;
    const returnCustodyBefore = db.prepare('SELECT claimed_at,claimed_by_generation_uuid,minting_generation_uuid FROM queue_items WHERE qitem_id=?').get('outside-return');
    expect(returnCustodyBefore).toEqual({ claimed_at: expect.any(String), claimed_by_generation_uuid: 'lead-g1', minting_generation_uuid: 'builder-g1' });
    const productCustodyBefore = db.prepare('SELECT claimed_at,claimed_by_generation_uuid,minting_generation_uuid FROM queue_items WHERE qitem_id=?').get('outside-product');
    const assignmentBefore = db.prepare('SELECT * FROM coordinator_assignments WHERE queue_id=?').get('outside-product');
    const atomicNotifications: Array<{ queueId: string; bothClosed: boolean; accepted: boolean }> = [];
    const unsubscribe = bus.subscribe((event) => {
      if (event.type !== 'queue.updated' || !['outside-return', dutyId].includes(event.qitemId)) return;
      atomicNotifications.push({
        queueId: event.qitemId,
        bothClosed: repo.getById('outside-return')?.state === 'done' && repo.getById(dutyId)?.state === 'done',
        accepted: !!db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id='coordination-accept:outside' AND kind='coordination-accept'").get(),
      });
    });
    service.accept('lead@xv', 'lead-g1', 'xv', 'outside', 'outside-return', 'actual/outside-acceptance.md');
    unsubscribe();

    const accepted = db.prepare("SELECT receipt FROM coordinator_operations WHERE operation_id='coordination-accept:outside' AND kind='coordination-accept'").get() as { receipt: string };
    expect(JSON.parse(accepted.receipt)).toMatchObject({ queueId: 'outside-product', dispositionId: 'outside-return', actor: 'lead@xv', generation: 'lead-g1', evidenceRef: 'actual/outside-acceptance.md' });
    const closedDuty = repo.getById(dutyId)!;
    expect(closedDuty.state).toBe('done');
    expect(closedDuty.body).toBe(dutyBeforeClaim.body);
    expect(closedDuty.sourceSession).toBe(dutyBeforeClaim.sourceSession);
    expect(closedDuty.destinationSession).toBe('lead@xv');
    expect((db.prepare('SELECT claimed_at,claimed_by_generation_uuid,minting_generation_uuid FROM queue_items WHERE qitem_id=?').get(dutyId) as { claimed_by_generation_uuid: string }).claimed_by_generation_uuid).toBe('lead-g1');
    expect(repo.getParkWakeStatus(dutyId)).toMatchObject({ kind: 'timer', live: false });
    expect(db.prepare('SELECT state FROM watchdog_jobs WHERE job_id=?').get(parkWake.ref)).not.toEqual({ state: 'active' });
    expect(db.prepare("SELECT actor_session,identity_provenance FROM queue_transitions WHERE qitem_id=? AND state='done' ORDER BY transition_id DESC LIMIT 1").get(dutyId)).toEqual({ actor_session: 'lead@xv', identity_provenance: 'transport:v1' });
    expect(repo.getById('outside-product')?.body).toBe(productBefore.body);
    expect(db.prepare('SELECT claimed_at,claimed_by_generation_uuid,minting_generation_uuid FROM queue_items WHERE qitem_id=?').get('outside-product')).toEqual(productCustodyBefore);
    const closedReturn = repo.getById('outside-return')!;
    expect(closedReturn.state).toBe('done');
    expect(closedReturn.body).toBe(returnBefore.body);
    expect(closedReturn.sourceSession).toBe('builder@xv');
    expect(closedReturn.destinationSession).toBe('lead@xv');
    expect(db.prepare('SELECT claimed_at,claimed_by_generation_uuid,minting_generation_uuid FROM queue_items WHERE qitem_id=?').get('outside-return')).toEqual(returnCustodyBefore);
    expect(repo.getParkWakeStatus('outside-return')).toMatchObject({ kind: 'timer', live: false });
    expect(db.prepare('SELECT state FROM watchdog_jobs WHERE job_id=?').get(returnWake.ref)).not.toEqual({ state: 'active' });
    expect(db.prepare('SELECT * FROM coordinator_assignments WHERE queue_id=?').get('outside-product')).toEqual(assignmentBefore);
    expect(atomicNotifications.length).toBe(2);
    expect(atomicNotifications.every((event) => event.bothClosed && event.accepted)).toBe(true);
    expect((db.prepare("SELECT count(*) n FROM coordinator_operations WHERE kind='coordination-accept' AND json_extract(receipt,'$.queueId')='outside-product'").get() as { n: number }).n).toBe(1);

    const assignmentCount = (db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='next'").get() as { n: number }).n;
    const wakeCount = (db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get() as { n: number }).n;
    service.accept('lead@xv', 'lead-g1', 'xv', 'outside', 'outside-return', 'actual/outside-acceptance.md');
    expect((db.prepare("SELECT count(*) n FROM coordinator_assignments WHERE package_key='next'").get() as { n: number }).n).toBe(assignmentCount);
    expect((db.prepare("SELECT count(*) n FROM outbox_entries WHERE outbox_id LIKE 'wake-intent-%'").get() as { n: number }).n).toBe(wakeCount);
    const nextAssignment = db.prepare("SELECT queue_id FROM coordinator_assignments WHERE package_key='next'").get() as { queue_id: string };
    repo.claim({ qitemId: nextAssignment.queue_id, destinationSession: 'reviewer@xv', identityProvenance: 'transport:v1' });
    expect(repo.getById(nextAssignment.queue_id)?.state).toBe('in-progress');
  });

  it('acceptance never acquires or closes an unclaimed typed return', async () => {
    configure([
      task('next', 'reviewer@xv', [{ queueId: 'outside-product', dispositionId: 'outside-return' }]),
      { ...task('next-repair', 'architect@xv'), recoveryFor: 'next' },
    ]);
    await createAcceptedProductReturn('unclaimed');
    const duty = service.reconcile('lead@xv', 'lead-g1', 'xv').find((row) => row.key === 'acceptance:outside')!;
    repo.claim({ qitemId: duty.queueId!, destinationSession: 'lead@xv', actorGeneration: 'lead-g1', identityProvenance: 'transport:v1' });

    service.accept('lead@xv', 'lead-g1', 'xv', 'outside', 'outside-return', 'actual/outside-acceptance.md');

    expect(repo.getById('outside-return')?.state).toBe('pending');
    expect(repo.getById('outside-return')?.claimedAt).toBeNull();
    expect((db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get('outside-return') as { claimed_by_generation_uuid: string | null }).claimed_by_generation_uuid).toBeNull();
    expect(repo.getById(duty.queueId!)?.state).toBe('done');
    expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id='coordination-accept:outside'").get()).toBeTruthy();
  });

  it('preserves a return claimed by a different generation while recording the current holder acceptance', async () => {
    configure([
      task('next', 'reviewer@xv', [{ queueId: 'outside-product', dispositionId: 'outside-return' }]),
      { ...task('next-repair', 'architect@xv'), recoveryFor: 'next' },
    ]);
    await createAcceptedProductReturn();
    const duty = service.reconcile('lead@xv', 'lead-g1', 'xv').find((row) => row.key === 'acceptance:outside')!;
    repo.claim({ qitemId: duty.queueId!, destinationSession: 'lead@xv', actorGeneration: 'lead-g1', identityProvenance: 'transport:v1' });
    db.prepare("UPDATE queue_items SET claimed_by_generation_uuid='lead-old-g0' WHERE qitem_id='outside-return'").run();
    const wakeBefore = repo.getParkWakeStatus('outside-return');

    service.accept('lead@xv', 'lead-g1', 'xv', 'outside', 'outside-return', 'actual/outside-acceptance.md');

    expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id='coordination-accept:outside'").get()).toBeTruthy();
    expect(repo.getById('outside-return')?.state).toBe('blocked');
    expect((db.prepare('SELECT claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?').get('outside-return') as { claimed_by_generation_uuid: string }).claimed_by_generation_uuid).toBe('lead-old-g0');
    expect(repo.getParkWakeStatus('outside-return')).toEqual(wakeBefore);
    expect(repo.getById(duty.queueId!)?.state).toBe('done');
  });

  it('rolls back acceptance and both park closures if the second queue closure fails', async () => {
    configure([
      task('next', 'reviewer@xv', [{ queueId: 'outside-product', dispositionId: 'outside-return' }]),
      { ...task('next-repair', 'architect@xv'), recoveryFor: 'next' },
    ]);
    await createAcceptedProductReturn();
    const duty = service.reconcile('lead@xv', 'lead-g1', 'xv').find((row) => row.key === 'acceptance:outside')!;
    repo.claim({ qitemId: duty.queueId!, destinationSession: 'lead@xv', actorGeneration: 'lead-g1', identityProvenance: 'transport:v1' });
    await repo.create({ qitemId: 'duty-park-blocker', sourceSession: 'builder@xv', destinationSession: 'lead@xv', body: 'live duty blocker', nudge: false });
    repo.update({ qitemId: duty.queueId!, actorSession: 'lead@xv', actorGeneration: 'lead-g1', identityProvenance: 'transport:v1', state: 'blocked', blockedOn: 'duty-park-blocker', wakeAfterSeconds: 3_600 });
    const returnWake = repo.getParkWakeStatus('outside-return');
    const dutyWake = repo.getParkWakeStatus(duty.queueId!);
    const events: string[] = [];
    const unsubscribe = bus.subscribe((event) => { if (event.type === 'queue.updated') events.push(event.qitemId); });
    const original = repo.updateWithinTransaction.bind(repo);
    let updateCount = 0;
    const spy = vi.spyOn(repo, 'updateWithinTransaction').mockImplementation((input) => {
      updateCount++;
      if (updateCount === 2) throw new Error('injected second close rejection');
      return original(input);
    });

    expect(() => service.accept('lead@xv', 'lead-g1', 'xv', 'outside', 'outside-return', 'actual/outside-acceptance.md')).toThrow('injected second close rejection');
    spy.mockRestore();
    unsubscribe();

    expect(updateCount).toBe(2);
    expect(db.prepare("SELECT 1 FROM coordinator_operations WHERE operation_id='coordination-accept:outside'").get()).toBeUndefined();
    expect(repo.getById('outside-return')?.state).toBe('blocked');
    expect(repo.getById(duty.queueId!)?.state).toBe('blocked');
    expect(repo.getParkWakeStatus('outside-return')).toEqual(returnWake);
    expect(repo.getParkWakeStatus(duty.queueId!)).toEqual(dutyWake);
    expect(events).toEqual([]);
  });
});
