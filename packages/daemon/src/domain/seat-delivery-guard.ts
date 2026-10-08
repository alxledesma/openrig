import { AsyncLocalStorage } from "node:async_hooks";
import type Database from "better-sqlite3";

/** A binding is captured before waiting. Never rebind an old operation to a new occupant. */
export interface GuardTarget {
  nodeId: string;
  session: string;
  occupant: string | null;
  pane: string | null;
}

interface Lease {
  target: GuardTarget;
  active: boolean;
  origin: "automatic" | "human";
  lifecycle?: boolean;
  /** Set only by the dedicated same-generation runner-rehost lease. Distinguishes rehost
   * authority from ordinary input/lifecycle/human authority, which never confer it. */
  rehost?: boolean;
  reservationId?: string;
}

export class DeliveryGuardError extends Error {
  constructor(readonly code: string, message: string) { super(message); }

  // Hono's error protocol also preserves this typed refusal on lifecycle routes.
  getResponse(): Response {
    return Response.json({ ok: false, code: this.code, error: this.message }, { status: 409 });
  }
}

export interface GuardPreference {
  nodeId: string;
  desired: boolean;
  effective: boolean;
  pending: boolean;
}

/** One serialization domain for preference activation, delivery and writing lifecycle.
 * No timer drains held messages. Async context carries a lease through nested adapters;
 * active=false prevents a detached task from retaining permission after its operation ends.
 */
export class SeatDeliveryGuard {
  private readonly tails = new Map<string, Promise<void>>();
  private readonly scope = new AsyncLocalStorage<Map<string, Lease>>();
  private readonly humanLeases = new Set<Lease>();
  /** Tracked per node, independent of the tail, so a human lease cannot START after the
   * rehost already holds the tail and still interleave with the stop/resume window. */
  private readonly rehostLeases = new Set<Lease>();
  private readonly operationLeases = new Set<Lease>();

  constructor(
    readonly db: Database.Database,
    private readonly resolve: (target: string) => GuardTarget | null,
  ) {}

  /** Startup-only, before exposing routes or starting writers. A stopped operation cannot
   * retain an in-memory lease. Persisted desired protection applies at the new boundary. */
  recoverActivation(): void {
    this.db.transaction(() => {
      this.db.prepare("UPDATE seat_delivery_guards SET effective = desired WHERE effective != desired").run();
      this.db.prepare("UPDATE seat_delivery_guard_changes SET effective_at = ? WHERE effective_at IS NULL")
        .run(new Date().toISOString());
    })();
  }

  preference(nodeId: string): GuardPreference {
    const row = this.db.prepare("SELECT desired, effective FROM seat_delivery_guards WHERE node_id = ?")
      .get(nodeId) as { desired: number; effective: number } | undefined;
    return { nodeId, desired: !!row?.desired, effective: !!row?.effective, pending: row !== undefined && row.desired !== row.effective };
  }

  maybeTarget(name: string): GuardTarget | null { return this.resolve(name); }

  target(name: string): GuardTarget {
    const target = this.resolve(name);
    if (!target) throw new DeliveryGuardError("guard_target_unknown", `Cannot establish managed input target ${name}; no input written.`);
    return target;
  }

  private same(a: GuardTarget, b: GuardTarget): boolean {
    return a.nodeId === b.nodeId && a.session === b.session && a.occupant === b.occupant && a.pane === b.pane;
  }

  private async serial<T>(nodeId: string, fn: () => Promise<T>): Promise<T> {
    const before = this.tails.get(nodeId) ?? Promise.resolve();
    let release!: () => void;
    const done = new Promise<void>(resolve => { release = resolve; });
    const tail = before.then(() => done);
    this.tails.set(nodeId, tail);
    await before;
    try { return await fn(); }
    finally { release(); if (this.tails.get(nodeId) === tail) this.tails.delete(nodeId); }
  }

  async set(nodeId: string, enabled: boolean, actor: string, reason: string, timeoutMs = 2000): Promise<GuardPreference> {
    if (!actor.trim() || !reason.trim()) throw new DeliveryGuardError("guard_reason_required", "Actor and reason are required.");
    const at = new Date().toISOString();
    const change = this.db.transaction(() => {
      const old = this.preference(nodeId);
      this.db.prepare(`INSERT INTO seat_delivery_guards(node_id, desired, effective, actor, reason, changed_at)
        VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(node_id) DO UPDATE SET
        desired=excluded.desired, actor=excluded.actor, reason=excluded.reason, changed_at=excluded.changed_at`)
        .run(nodeId, Number(enabled), Number(old.effective), actor, reason, at);
      return this.db.prepare(`INSERT INTO seat_delivery_guard_changes(node_id, desired, previous_desired, previous_effective, actor, reason, requested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`).run(nodeId, Number(enabled), Number(old.desired), Number(old.effective), actor, reason, at).lastInsertRowid;
    })();
    const activation = this.serial(nodeId, async () => {
      this.db.transaction(() => {
        // Later requests are serialized too. Apply each accepted transition, in order.
        this.db.prepare("UPDATE seat_delivery_guards SET effective = ? WHERE node_id = ?").run(Number(enabled), nodeId);
        this.db.prepare("UPDATE seat_delivery_guard_changes SET effective_at = ? WHERE id = ?")
          .run(new Date().toISOString(), change);
      })();
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([activation, new Promise<void>(resolve => { timer = setTimeout(resolve, timeoutMs); })]);
      return this.preference(nodeId);
    } finally { if (timer) clearTimeout(timer); }
  }

  /** fn must include lifecycle preflight, effects and last write. Retention callbacks
   * execute under this same lease, before any pane capture/paste/submit branch. */
  async operation<T>(name: string, fn: () => Promise<T>, held?: (target: GuardTarget) => Promise<T>, reservationId?: string): Promise<T> {
    const bound = this.target(name);
    const inherited = this.scope.getStore()?.get(bound.nodeId);
    if (inherited?.active && inherited.target.nodeId === bound.nodeId) {
      // G1: an ordinary operation or lifecycle call must never inherit a REHOST lease.
      // Before this, lifecycle() reached an in-scope rehost lease through this generic
      // branch, then set lease.lifecycle = true on it and silently upgraded rehost
      // authority into general lifecycle authority. The rehost lease is a narrow,
      // guard-ON-scoped permission for exactly one operation; it is not a licence for
      // lifecycle preflight, rebinding or any other ordinary writing path.
      // input() is untouched and keeps its own active-lease fast path, so the required
      // resume typing still works under the rehost lease.
      if (inherited.rehost === true) throw new DeliveryGuardError("rehost_lease_not_upgradable", "A same-generation runner rehost lease cannot be upgraded to ordinary operation or lifecycle authority; this writing path did not inherit it.");
      this.assertCurrent(name, inherited);
      return fn();
    }
    return this.serial(bound.nodeId, async () => {
      const current = this.target(name);
      if (!this.same(bound, current)) throw new DeliveryGuardError("guard_target_changed", "Input target changed while waiting; no input written.");
      const reservation = this.activeReservation(bound.nodeId);
      if (reservation && reservation.reservation_id !== reservationId) {
        if (held) return held(bound);
        throw new DeliveryGuardError("seat_dispatch_reserved", "Seat has a durable cutover reservation; input/lifecycle retained until exact disposition.");
      }
      const pref = this.preference(bound.nodeId);
      if (pref.effective || pref.desired) {
        if (held) return held(bound);
        throw new DeliveryGuardError("typing_guard_enabled", "Automatic input is paused for this seat. Disable its typing guard explicitly before this writing operation.");
      }
      const lease: Lease = { target: bound, active: true, origin: "automatic", reservationId };
      this.operationLeases.add(lease);
      try { return await this.scope.run(new Map([...(this.scope.getStore() ?? []), [lease.target.nodeId, lease]]), fn); }
      finally { lease.active = false; this.operationLeases.delete(lease); }
    });
  }

  /** Dedicated SAME-GENERATION runner-rehost lease.
   *
   * This is the one writing path that requires typing protection to be ON, and it requires
   * it strictly: desired AND effective, with no pending activation. The operator's
   * quiescence guarantee is exactly what makes replacing a runner on its own session file
   * safe, and the replacement must be typed while that quiescence still holds.
   *
   * It deliberately keeps every other protection:
   *   - it serializes on the SAME per-node tail as input and lifecycle, so it waits for and
   *     blocks any other input/lifecycle tail rather than racing them;
   *   - it captures the binding before waiting and re-proves node/session/occupant/pane
   *     unchanged, so a new occupant or a recycled pane can never be adopted;
   *   - a durable dispatch reservation always excludes it, and it takes no reservation of
   *     its own and creates no human lease;
   *   - it grants no preference, disables nothing and bypasses nothing.
   *
   * There is no global permissive flag: `operation`, `input`, `reconcileBinding` and every
   * other path keep refusing a writing operation while the guard is on, exactly as before. */
  async runnerRehost<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const bound = this.target(name);
    const inherited = this.scope.getStore()?.get(bound.nodeId);
    // Only a REHOST lease may be inherited, and only after the guard-ON requirement is
    // proven again here. An ordinary input, lifecycle or human lease carries no rehost
    // authority at all: it is never inherited and never launders this operation through
    // a scope whose protections were proven for a different purpose.
    if (inherited?.active && inherited.target.nodeId === bound.nodeId) {
      if (inherited.rehost !== true) {
        // Refuse here, BEFORE serial(). An ordinary operation holds this node's own tail
        // while it runs, so re-entering serial() from inside it would wait on a tail that
        // only this call can release. That self-wait must never be allowed to hang; it is a
        // composition error, not a transient contention to retry.
        throw new DeliveryGuardError("rehost_not_nestable", "A same-generation runner rehost cannot run inside an ordinary input, lifecycle or human lease for this seat. Run it as its own top-level operation.");
      }
      this.assertCurrent(name, inherited);
      this.assertRehostGuard(bound.nodeId);
      return fn();
    }
    return this.serial(bound.nodeId, async () => {
      const current = this.target(name);
      if (!this.same(bound, current)) throw new DeliveryGuardError("guard_target_changed", "Input target changed while waiting; no input written.");
      // A human lease is an intentional binding path that deliberately does not take this
      // tail, so the tail alone cannot exclude it. Rehost never interleaves with it.
      if ([...this.humanLeases].some(lease => lease.active && lease.target.nodeId === bound.nodeId)) throw new DeliveryGuardError("guard_operation_in_progress", "Human input is in progress for this seat; a same-generation runner rehost did not interleave with it. Retry after it finishes.");
      if (this.activeReservation(bound.nodeId)) throw new DeliveryGuardError("seat_dispatch_reserved", "Seat has a durable cutover reservation; a same-generation runner rehost is excluded from it.");
      this.assertRehostGuard(bound.nodeId);
      // Deliberately NOT marked lifecycle: a rehost lease must never satisfy ownsLifecycle
      // or rebindLifecycle, so it cannot be reused as general lifecycle authority.
      const lease: Lease = { target: bound, active: true, origin: "automatic", rehost: true };
      this.rehostLeases.add(lease);
      try { return await this.scope.run(new Map([...(this.scope.getStore() ?? []), [lease.target.nodeId, lease]]), fn); }
      finally { lease.active = false; this.rehostLeases.delete(lease); }
    });
  }

  /** Narrow requirement, one direction only: typing protection must be ON. Never a waiver. */
  private assertRehostGuard(nodeId: string): void {
    const pref = this.preference(nodeId);
    if (pref.desired !== true || pref.effective !== true || pref.pending) throw new DeliveryGuardError("typing_guard_required_for_rehost", "A same-generation runner rehost requires this seat's typing guard desired AND effective ON. Rehost never disables, relaxes or bypasses typing protection.");
  }

  /** Read-only protection inspection under the exact delivery serialization domain.
   * Callback cannot grant delivery/lifecycle permission. No preference or input write. */
  async inspectProtection<T>(name: string, fn: (target: GuardTarget, protection: {code: "seat_dispatch_reserved" | "typing_guard_enabled"; fingerprint: string} | null) => T): Promise<T> {
    const bound = this.target(name);
    return this.serial(bound.nodeId, async () => {
      const current = this.target(name);
      if (!this.same(bound, current)) throw new DeliveryGuardError("guard_target_changed", "Inspection target changed while waiting");
      const protection = this.protectionFacts(current.nodeId);
      return fn(current, protection);
    });
  }

  /** Pure current protection facts; reads grant no input/lifecycle authority. */
  protectionFacts(nodeId: string): {code: "seat_dispatch_reserved" | "typing_guard_enabled"; fingerprint: string} | null {
    const reservation=this.activeReservation(nodeId),preference=this.preference(nodeId);
    return reservation ? {code:"seat_dispatch_reserved",fingerprint:JSON.stringify(this.db.prepare("SELECT * FROM seat_dispatch_reservations WHERE reservation_id=?").get(reservation.reservation_id))}
      : preference.desired||preference.effective ? {code:"typing_guard_enabled",fingerprint:JSON.stringify(preference)} : null;
  }

  ownsLifecycle(nodeId: string): boolean {
    const lease = this.scope.getStore()?.get(nodeId);
    if (!lease?.active || !lease.lifecycle) return false;
    this.assertCurrent(nodeId, lease);
    return true;
  }

  /** Only the current async rehost operation may inspect its replacement while
   * its own guard is held. This grants neither input nor lifecycle authority. */
  ownsRunnerRehost(nodeId: string): boolean {
    const lease = this.scope.getStore()?.get(nodeId);
    if (!lease?.active || lease.rehost !== true) return false;
    this.assertCurrent(nodeId, lease);
    this.assertRehostGuard(nodeId);
    return true;
  }

  /** Read-only cross-request observation; unlike ownsLifecycle this confers no
   * authority and does not depend on the observer's async execution context. */
  lifecycleActive(nodeId: string): boolean {
    return [...this.operationLeases, ...this.rehostLeases].some(lease =>
      lease.active && lease.target.nodeId === nodeId && (lease.lifecycle === true || lease.rehost === true));
  }

  /** Multi-seat restore takes leases in stable order before any rig mutation.
   * Nested per-seat launch joins these leases; it must not reacquire them. */
  async lifecycle<T>(nodeIds: string[], fn: () => Promise<T>, reservationId?: string): Promise<T> {
    const ids = [...new Set(nodeIds)].sort();
    const acquire = async (index: number): Promise<T> => {
      const id = ids[index]; if (!id) return fn();
      if (this.ownsLifecycle(id)) return acquire(index + 1);
      return this.operation(id, async () => {
        const lease = this.scope.getStore()!.get(id)!;
        lease.lifecycle = true;
        return acquire(index + 1);
      }, undefined, reservationId);
    };
    return acquire(0);
  }

  /** Called only after an intentional lifecycle binding change, under its lease.
   * Ordinary sends cannot adopt a replacement occupant or recycled pane. */
  rebindLifecycle(nodeId: string): void {
    const lease = this.scope.getStore()?.get(nodeId);
    if (!lease?.active || !lease.lifecycle) throw new DeliveryGuardError("guard_lease_required", "Binding changes require the complete lifecycle lease.");
    lease.target = this.target(nodeId);
  }

  /** Adopt only a physical terminal replacement inside an owned guarded rehost.
   * The occupant and canonical address cannot change through this capability. */
  rebindRunnerRehost(nodeId: string): void {
    const lease = this.scope.getStore()?.get(nodeId);
    const target = this.target(nodeId);
    if (!lease?.active || lease.rehost !== true || lease.target.nodeId !== target.nodeId
      || lease.target.session !== target.session || lease.target.occupant !== target.occupant
      || this.activeReservation(nodeId)) {
      throw new DeliveryGuardError("guard_lease_required", "Physical resume binding requires the same occupant under its rehost lease.");
    }
    this.assertRehostGuard(nodeId);
    lease.target = target;
  }

  private activeReservation(nodeId: string): { reservation_id: string } | null {
    if (!this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='seat_dispatch_reservations'").get()) return null;
    return this.db.prepare("SELECT reservation_id FROM seat_dispatch_reservations WHERE node_id=? AND state!='released'").get(nodeId) as {reservation_id:string} | undefined ?? null;
  }

  private assertCurrent(name: string, lease: Lease): void {
    const reservation = this.activeReservation(lease.target.nodeId);
    if (reservation && reservation.reservation_id !== lease.reservationId) throw new DeliveryGuardError("seat_dispatch_reserved", "Durable reservation excludes this operation; no input written.");
    if (!lease.active || !this.same(lease.target, this.target(name))) {
      throw new DeliveryGuardError("guard_target_changed", "Input target/occupant changed; no input written.");
    }
  }

  /** Synchronous final-effect check: no await between this and issuing the write. */
  checkInput(name: string): void {
    const target = this.target(name);
    const lease = this.scope.getStore()?.get(target.nodeId);
    if (!lease) throw new DeliveryGuardError("guard_lease_required", "Input requires an active operation lease.");
    this.assertCurrent(name, lease);
  }

  /** Reconciliation is a synchronous DB transaction, not a nested input operation.
   * Refuse rather than waiting on a sender that could itself be awaiting this call.
   * Pending activation and explicit human input protect the same occupant boundary. */
  reconcileBinding<T>(expected: GuardTarget, commit: () => T): T {
    if (!this.same(expected, this.target(expected.nodeId))) {
      throw new DeliveryGuardError("guard_target_changed", "Reconciliation target changed during observation; retry with current identity.");
    }
    if (this.tails.has(expected.nodeId) || [...this.humanLeases].some(l => l.active && l.target.nodeId === expected.nodeId)) {
      throw new DeliveryGuardError("guard_operation_in_progress", "Seat operation in progress; reconciliation did not change custody. Retry after it finishes.");
    }
    const pref = this.preference(expected.nodeId);
    if (pref.desired || pref.effective) {
      throw new DeliveryGuardError("typing_guard_enabled", "Reconciliation cannot replace the occupant while typing protection is enabled.");
    }
    return commit();
  }

  async input<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const target = this.target(name);
    const lease = this.scope.getStore()?.get(target.nodeId);
    if (lease?.active) { this.assertCurrent(name, lease); return fn(); }
    return this.operation(name, fn);
  }

  /** Internal broker path only; never an option accepted by the send HTTP route. */
  async humanInput<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const target = this.target(name);
    // Symmetric fence with the human-lease check inside runnerRehost. That one stops a
    // rehost from starting while human input runs; this one stops human input from starting
    // once a rehost already holds the window. The rehost set is checked by node, so ordinary
    // human input on every other seat, and with no rehost active, is unchanged.
    if ([...this.rehostLeases].some(lease => lease.active && lease.target.nodeId === target.nodeId)) {
      throw new DeliveryGuardError("rehost_in_progress", "A same-generation runner rehost is in progress for this seat; human input did not interleave with its stop/resume window. Retry after it finishes.");
    }
    const lease: Lease = { target, active: true, origin: "human" };
    this.humanLeases.add(lease);
    try { return await this.scope.run(new Map([...(this.scope.getStore() ?? []), [lease.target.nodeId, lease]]), fn); }
    finally { lease.active = false; this.humanLeases.delete(lease); }
  }
}

/** Current binding, never a latest historical session-name guess. Unbound seats
 * resolve by node/canonical address for preferences and lifecycle preflight.
 * #174: an archived rig can keep a binding to the same session name as a live
 * seat, so unarchived matches win; archived ones count only when nothing else
 * matches. Exactly one match is still required. */
export function resolveGuardTarget(db: Database.Database, name: string): GuardTarget | null {
  const rows = db.prepare(`SELECT n.id AS nodeId,
      coalesce(b.tmux_session, replace(n.logical_id,'.','-') || '@' || r.name) AS session,
      b.tmux_pane AS pane,
      (SELECT generation_uuid FROM occupant_tenures t WHERE t.node_id=n.id ORDER BY generation_ordinal DESC LIMIT 1) AS occupant,
      r.archived_at AS archivedAt
    FROM nodes n JOIN rigs r ON r.id=n.rig_id LEFT JOIN bindings b ON b.node_id=n.id
    WHERE n.id=? OR b.tmux_session=? OR b.tmux_pane=? OR n.logical_id=?
      OR (b.tmux_session IS NULL AND replace(n.logical_id,'.','-') || '@' || r.name=?)`)
    .all(name, name, name, name, name) as Array<GuardTarget & { archivedAt: string | null }>;
  const unarchived = rows.filter((row) => row.archivedAt === null);
  const pool = unarchived.length > 0 ? unarchived : rows;
  if (pool.length !== 1) return null;
  const { archivedAt: _archivedAt, ...target } = pool[0]!;
  return target;
}
