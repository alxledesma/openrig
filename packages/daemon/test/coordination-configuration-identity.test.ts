import { afterEach, describe, expect, it } from 'vitest';
import type Database from 'better-sqlite3';
import { createDb } from '../src/db/connection.js';
import { EventBus } from '../src/domain/event-bus.js';
import { QueueRepository } from '../src/domain/queue-repository.js';
import { CoordinationRecoveryService } from '../src/domain/coordination-recovery-service.js';
import { digest } from '../src/domain/coordinator-authority-service.js';
import { seed } from './helpers/coordinator-fixture.js';

describe('coordination configuration digest identity', () => {
  let db: Database.Database;
  let recovery: CoordinationRecoveryService;
  const sessionName = 'shared-operator-name@fixture';
  const targetNode = 'builder@xv';
  const archivedNode = 'worker@other';

  function setup(): void {
    db = createDb();
    seed(db);
    db.prepare("UPDATE nodes SET runtime='pi',model='old-model',profile='fixture',codex_config_profile=NULL,cwd='/target' WHERE id=?").run(targetNode);
    db.prepare("UPDATE sessions SET session_name=? WHERE id=?").run(sessionName, targetNode);
    db.prepare('INSERT INTO bindings(node_id,tmux_session,tmux_pane) VALUES (?,?,?)').run(targetNode, sessionName, '%target');

    // This archived rig has a newer session row with the same display address.
    // Name-only ORDER BY session.id would choose it instead of the live target.
    db.prepare("UPDATE rigs SET archived_at='2026-10-08T00:00:00.000Z' WHERE id='other'").run();
    db.prepare('INSERT INTO bindings(node_id,tmux_session,tmux_pane) VALUES (?,?,?)').run(archivedNode, sessionName, '%archived');
    db.prepare("INSERT INTO sessions(id,node_id,session_name,status) VALUES ('zz-archived-session',?,?, 'detached')").run(archivedNode, sessionName);

    const repo = new QueueRepository(db, new EventBus(db));
    recovery = new CoordinationRecoveryService(repo, () => null);
  }

  afterEach(() => db?.close());

  it('keeps the live node digest when a newer archived rig reuses its session name', () => {
    setup();
    const newestByName = db.prepare('SELECT node_id FROM sessions WHERE session_name=? ORDER BY id DESC LIMIT 1').get(sessionName) as { node_id: string };
    expect(newestByName.node_id).toBe(archivedNode);

    const targetRow = db.prepare(`SELECT n.id,n.runtime,n.model,n.profile,n.codex_config_profile,n.cwd
      FROM nodes n JOIN sessions s ON s.node_id=n.id
      WHERE n.id=? AND s.session_name=? ORDER BY s.id DESC LIMIT 1`).get(targetNode, sessionName) as Record<string, unknown>;
    const expected = digest(JSON.stringify(targetRow));
    expect(recovery.configurationDigest(sessionName)).toBe(expected);

    // A caller-supplied node cannot redirect the identity selected by the live binding.
    expect(recovery.configurationDigest(sessionName, { nodeId: archivedNode, generation: 'worker-g1', runtime: 'codex' })).toBeNull();

    // Preserve staged runtime-migration projection, but only for the exact target node.
    const launch = { nodeId: targetNode, generation: 'builder-g2', runtime: 'codex' };
    const packet = {
      protocol: 'runtime-migration-v1',
      expected: { nodeId: targetNode, sessionName },
      target: { runtime: 'codex', model: 'gpt-6-luna', codexConfigProfile: 'fixture-profile' },
    };
    db.prepare(`INSERT INTO seat_dispatch_reservations
      (reservation_id,operation_id,node_id,session_name,predecessor_generation,predecessor_native_id,actor_session,actor_generation,
       request_hash,expected_json,frozen_snapshot,state,performer_session,performer_generation,successor_generation,created_at,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,'started',?,?,?,?,?)`).run(
      'identity-projection-reservation', 'identity-projection-operation', targetNode, sessionName,
      'builder-g1', 'retained-native-id', 'operator-agent@kernel', 'operator-g1',
      digest(JSON.stringify(packet)), JSON.stringify(packet), 'retained-custody',
      'operator-agent@kernel', 'operator-g1', launch.generation, '2026-10-08T00:00:00.000Z', '2026-10-08T00:00:00.000Z',
    );
    const projectedRow = { ...targetRow, runtime: 'codex', model: 'gpt-6-luna', codex_config_profile: 'fixture-profile' };
    const projected = digest(JSON.stringify(projectedRow));
    expect(recovery.configurationDigest(sessionName, launch)).toBe(projected);
    expect(recovery.configurationDigest(sessionName, { ...launch, generation: 'wrong-successor' })).toBeNull();
    expect(recovery.configurationDigest(sessionName, { ...launch, runtime: 'pi' })).toBeNull();
  });

  it('refuses to guess when two unarchived nodes share a session name', () => {
    setup();
    db.prepare('UPDATE rigs SET archived_at=NULL WHERE id=?').run('other');
    expect(recovery.configurationDigest(sessionName)).toBeNull();
    expect(recovery.configurationDigest(sessionName, { nodeId: targetNode, generation: 'builder-g1', runtime: 'codex' })).toBeNull();
  });
});
