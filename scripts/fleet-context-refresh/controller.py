#!/usr/bin/env python3
"""Production-only, actor-authored context rotation over durable daemon reservations.

The external poller can only request a checkpoint or stage a frozen attempt. It
cannot mint a managed actor. Reserve/handover/release run in the current Kernel
Operator shell; acknowledgments run in the actual successor/validator shells.
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import pathlib
import re
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import evidence as ev

ROOT = pathlib.Path(__file__).resolve().parent
HOME = ev.RIG_HOME
RIG = ev.RIG
REQUIRED = ("current_work", "decisions", "memory", "constraints", "standing_duties", "evidence", "next_action", "outstanding_effects")
PROTOCOL = "generation-queue-runtime-idle-v1"
OPERATOR = "operator-agent@kernel"


def read(path, default=None):
    return ev.read(path, default)


def write(path, value):
    ev.write(path, value)


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def policy():
    p = read(ROOT / "policy.json")
    if not isinstance(p, dict) or p.get("production_only") is not True:
        raise RuntimeError("production-only policy absent")
    if p.get("daemon_url") != "http://127.0.0.1:17433" or p.get("atomic_cutover_protocol") != PROTOCOL:
        raise RuntimeError("production daemon/protocol mismatch")
    rigs, seats, mappings = p.get("rigs"), p.get("managed_unattended_seats"), p.get("seat_policies")
    canonical = r"[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+"
    if (not isinstance(rigs, list) or not rigs or any(not isinstance(r, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", r) for r in rigs)
            or len(set(rigs)) != len(rigs) or not isinstance(seats, list) or not seats
            or any(not isinstance(x, str) or not re.fullmatch(canonical, x) or x.split("@")[1] not in rigs or x == OPERATOR for x in seats)
            or len(set(seats)) != len(seats) or not isinstance(mappings, dict) or set(mappings) != set(seats)):
        raise RuntimeError("malformed explicit fleet seat policy")
    for flag in ("controller_enabled", "automatic_cutover_enabled", "automatic_trigger_enabled", "operator_auto_execute_enabled", "allow_manual_canary"):
        if type(p.get(flag)) is not bool:
            raise RuntimeError("malformed fleet policy gate: " + flag)
    for seat, config in mappings.items():
        if not isinstance(config, dict) or set(config) - {"validator_seat", "prepare_percent", "rotate_percent", "compactions", "poll_seconds", "coordination"}:
            raise RuntimeError("malformed per-seat policy")
        validator = config.get("validator_seat")
        if not isinstance(validator, str) or not re.fullmatch(canonical, validator) or validator in (seat, OPERATOR):
            raise RuntimeError("distinct pinned validator seat required")
        coordination = config.get("coordination")
        if coordination is not None:
            if not isinstance(coordination, dict) or set(coordination) != {"admissions", "recovery_owner", "deadline_seconds"} or not isinstance(coordination["admissions"], dict) or not re.fullmatch(canonical, str(coordination["recovery_owner"])) or type(coordination["deadline_seconds"]) is not int or not 10 <= coordination["deadline_seconds"] <= 3600:
                raise RuntimeError("malformed supplied coordination policy")
            for owner, admission in coordination["admissions"].items():
                if not re.fullmatch(canonical, owner) or not isinstance(admission, dict) or set(admission) != {"generation", "configurationDigest", "qualificationRef", "capacityRef", "effortRef", "validUntil"} or any(not isinstance(admission[k], str) or not admission[k] for k in ("generation", "configurationDigest", "qualificationRef", "capacityRef", "effortRef")) or type(admission["validUntil"]) not in (int, float) or not 0 < admission["validUntil"] < float('inf'):
                    raise RuntimeError("malformed supplied admission evidence")
        settings = dict(p, **config)
        if any(type(settings.get(k)) not in (int, float) or not 0 < settings[k] <= 100 for k in ("prepare_percent", "rotate_percent")) or settings["prepare_percent"] >= settings["rotate_percent"]:
            raise RuntimeError("malformed context thresholds")
        if any(type(settings.get(k)) is not int or settings[k] <= 0 for k in ("compactions", "poll_seconds")) or settings["poll_seconds"] > 600:
            raise RuntimeError("malformed compaction/poll policy")
    return p


def seat_policy(seat):
    p = policy()
    if seat not in p["managed_unattended_seats"]:
        raise RuntimeError("seat outside explicit fleet allowlist")
    return dict(p, **p["seat_policies"][seat])


def attempt_seat(request):
    seat = request.get("seat")
    if not isinstance(seat, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+", seat):
        raise RuntimeError("frozen attempt seat unavailable")
    return seat


@contextlib.contextmanager
def database():
    db = sqlite3.connect(f"file:{HOME / 'openrig.sqlite'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def seat_row(db, seat):
    row = db.execute("""SELECT n.id AS node_id,n.codex_config_profile,b.tmux_session,
       (SELECT generation_uuid FROM occupant_tenures t WHERE t.node_id=n.id
        ORDER BY generation_ordinal DESC LIMIT 1) AS occupant_generation
       FROM nodes n JOIN rigs r ON r.id=n.rig_id
       LEFT JOIN bindings b ON b.node_id=n.id WHERE b.tmux_session=? AND r.name=?
       AND r.archived_at IS NULL""", (seat, seat.split("@")[1])).fetchall()
    if len(row) != 1 or not row[0]["occupant_generation"]:
        raise RuntimeError("exact current managed target unavailable")
    return dict(row[0])


def reservation(db, reservation_id):
    row = db.execute("SELECT * FROM seat_dispatch_reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
    return dict(row) if row else None


def reservation_audit(db, reservation_id):
    return [dict(x) for x in db.execute("SELECT action,actor_session,actor_generation,evidence_json,created_at FROM seat_dispatch_reservation_audit WHERE reservation_id=? ORDER BY id", (reservation_id,))]


def actor(expected=None):
    # Environment alone is not proof: require whoami, a current DB occupant, and a ready session.
    claimed = os.environ.get("OPENRIG_SESSION_NAME", "").strip()
    generation = os.environ.get("OPENRIG_OCCUPANT_GENERATION", "").strip()
    if not claimed or not generation:
        raise RuntimeError("current managed actor environment required")
    who = ev.call(["whoami", "--full", "--json"])
    actual = who.get("identity", {}).get("sessionName")
    if actual != claimed or (expected and actual != expected):
        raise RuntimeError("managed actor identity mismatch")
    with database() as db:
        nodes = db.execute("""SELECT n.id,(SELECT generation_uuid FROM occupant_tenures t WHERE t.node_id=n.id
          ORDER BY generation_ordinal DESC LIMIT 1) AS generation
          FROM nodes n JOIN rigs r ON r.id=n.rig_id JOIN bindings b ON b.node_id=n.id
          WHERE b.tmux_session=? AND r.archived_at IS NULL""", (actual,)).fetchall()
        rows = db.execute("SELECT status,startup_status FROM sessions WHERE node_id=? ORDER BY id DESC LIMIT 1", (nodes[0]["id"],)).fetchall() if len(nodes) == 1 else []
    if len(nodes) != 1 or len(rows) != 1 or nodes[0]["generation"] != generation or rows[0]["status"] != "running" or rows[0]["startup_status"] != "ready":
        raise RuntimeError("managed actor is not the current ready generation")
    return actual, generation


def profile_sha(profile):
    if not profile or not re.fullmatch(r"[A-Za-z0-9_-]+", profile):
        raise RuntimeError("named Codex profile unavailable")
    path = pathlib.Path(os.environ.get("CODEX_HOME", str(pathlib.Path.home() / ".codex"))) / f"{profile}.config.toml"
    return sha(path)


def active_reservation(db, node_id):
    row = db.execute("SELECT * FROM seat_dispatch_reservations WHERE node_id=? AND state!='released'", (node_id,)).fetchone()
    return dict(row) if row else None


def receipt_ok(receipt, snap, rows, now):
    return bool(isinstance(receipt, dict)
                and snap.get("who", {}).get("identity", {}).get("sessionName")
                and receipt.get("snapshot", {}).get("who", {}).get("identity", {}).get("sessionName") == snap["who"]["identity"]["sessionName"]
                and receipt.get("generation") == snap["generation"]
                and isinstance(receipt.get("at"), (int, float)) and 0 <= now - receipt["at"] <= 600
                and receipt.get("queue_hash") == ev.digest(rows)
                and receipt.get("quiescent") is True and receipt.get("unattended_eligible") is True
                and isinstance(receipt.get("packet"), dict)
                and all(key in receipt["packet"] for key in REQUIRED)
                and receipt["packet"]["outstanding_effects"] == [])


def checkpoint(seat, packet_path):
    if seat not in policy()["managed_unattended_seats"]:
        raise RuntimeError("seat outside explicit fleet allowlist")
    actor(seat)
    packet = read(packet_path)
    if not isinstance(packet, dict) or any(key not in packet for key in REQUIRED) or packet["outstanding_effects"] != []:
        raise RuntimeError("quiescent continuity packet and reconciled effects required")
    snap = ev.snapshot(seat)
    if snap["generation"] != ev.call(["whoami", "--full", "--json"])["contextUsage"]["sessionId"]:
        raise RuntimeError("predecessor generation changed")
    rows = ev.queue(seat)
    contract = ev.runtime_contract(snap)
    receipt = {"generation": snap["generation"], "at": time.time(), "queue_hash": ev.digest(rows),
               "quiescent": True, "unattended_eligible": True, "packet": packet,
               "snapshot": snap, "runtime_contract": contract}
    dest = ROOT / "receipts" / f"{seat}.json"
    write(dest, receipt)
    return {"checkpoint": str(dest), "nativeGeneration": snap["generation"], "queueItems": len(rows)}


def metrics(seat, snap):
    usage = snap["who"].get("contextUsage", {})
    if usage.get("fresh") is not True or not isinstance(usage.get("usedPercentage"), (int, float)):
        raise RuntimeError("fresh context usage unavailable")
    count, model = ev.native(usage["transcriptPath"])
    return usage["usedPercentage"], count, model


def positive_idle(seat, snap):
    nodes = ev.call(["ps", "--nodes", "--rig", seat.split("@")[1], "--full", "--json"])
    matches = [n for n in nodes if n.get("canonicalSessionName") == seat]
    if len(matches) != 1 or not ev.idle_ready(matches[0], snap):
        raise RuntimeError("positive current idle evidence unavailable")
    return matches[0]


def observe(seat):
    p = seat_policy(seat)
    if seat not in p["managed_unattended_seats"]:
        raise RuntimeError("seat outside explicit fleet allowlist")
    snap = ev.snapshot(seat)
    if snap.get("who", {}).get("identity", {}).get("sessionName") != seat:
        raise RuntimeError("snapshot belongs to different seat")
    used, count, model = metrics(seat, snap)
    node = positive_idle(seat, snap)
    receipt_path = ROOT / "receipts" / f"{seat}.json"
    receipt = read(receipt_path)
    rows = ev.queue(seat)
    if not receipt_ok(receipt, snap, rows, time.time()):
        raise RuntimeError("fresh quiescent checkpoint/custody unavailable")
    if model and model != node.get("model"):
        raise RuntimeError("native model differs from persistent pin")
    state = read(ROOT / "state.json", {})
    entry = state.setdefault(seat, {})
    with database() as db:
        target = seat_row(db, seat)
        if active_reservation(db, target["node_id"]):
            raise RuntimeError("durable dispatch reservation already active")
    key = [snap["generation"], sha(receipt_path), target["occupant_generation"]]
    previous = entry.get("idle")
    at = time.time()
    if not previous or previous.get("key") != key or at - previous.get("at", 0) > 600:
        entry["idle"] = {"key": key, "at": at, "observations": 1}
    elif at - previous["at"] >= p["poll_seconds"]:
        entry["idle"] = {"key": key, "at": at, "observations": min(2, previous["observations"] + 1)}
    entry["usage"] = {"usedPercentage": used, "observedCompactions": count, "at": at}
    if "baselineCompactions" not in entry or entry.get("baselineGeneration") != snap["generation"]:
        entry["baselineCompactions"] = count
        entry["baselineGeneration"] = snap["generation"]
    state[seat] = entry
    write(ROOT / "state.json", state)
    return entry["idle"]


def stage(seat, manual=False):
    p = seat_policy(seat)
    if seat not in p["managed_unattended_seats"] or p.get("controller_enabled") is not True or p["automatic_cutover_enabled"] is not True:
        raise RuntimeError("fleet allowlist/daemon automatic rotation gate disabled")
    if manual and p.get("allow_manual_canary") is not True:
        raise RuntimeError("manual canary disabled")
    if not manual and p.get("automatic_trigger_enabled") is not True:
        raise RuntimeError("automatic trigger disabled")
    validator = p.get("validator_seat")
    if not isinstance(validator, str) or validator in (seat, OPERATOR) or not re.fullmatch(r"[A-Za-z0-9_.-]+@[A-Za-z0-9_.-]+", validator):
        raise RuntimeError("distinct pinned validator seat required before staging")
    with database() as db:
        validator_rows = db.execute("""SELECT n.id FROM nodes n JOIN rigs r ON r.id=n.rig_id
          JOIN bindings b ON b.node_id=n.id WHERE b.tmux_session=? AND r.archived_at IS NULL""", (validator,)).fetchall()
    if len(validator_rows) != 1:
        raise RuntimeError("pinned validator seat is not a unique active member")
    state = read(ROOT / "state.json", {})
    entry = state.get(seat, {})
    if entry.get("attempt"):
        raise RuntimeError("pending attempt must be reconciled, never silently replaced")
    snap = ev.snapshot(seat)
    if snap.get("who", {}).get("identity", {}).get("sessionName") != seat:
        raise RuntimeError("snapshot belongs to different seat")
    used, count, model = metrics(seat, snap)
    if not manual and used < p["rotate_percent"] and count - entry.get("baselineCompactions", count) < p["compactions"]:
        raise RuntimeError("rotation threshold not reached")
    node = positive_idle(seat, snap)
    receipt_path = ROOT / "receipts" / f"{seat}.json"
    receipt = read(receipt_path)
    rows = ev.queue(seat)
    now = time.time()
    if not receipt_ok(receipt, snap, rows, now):
        raise RuntimeError("current quiescent checkpoint/custody unavailable")
    with database() as db:
        target = seat_row(db, seat)
        if active_reservation(db, target["node_id"]):
            raise RuntimeError("durable dispatch reservation already active")
    idle = entry.get("idle", {})
    if idle.get("key") != [snap["generation"], sha(receipt_path), target["occupant_generation"]] or idle.get("observations") != 2 or now - idle.get("at", 0) > 5:
        raise RuntimeError("two positive idle polls and fresh final witness required")
    if model and model != node.get("model"):
        raise RuntimeError("native model differs from persistent pin")
    contract = ev.runtime_contract(snap)
    if receipt.get("runtime_contract") != contract:
        raise RuntimeError("native runtime changed since checkpoint")
    reservation_id, operation_id = f"refresh-{uuid.uuid4().hex}", f"cutover-{uuid.uuid4().hex}"
    frozen = pathlib.Path("frozen") / f"{reservation_id}.json"
    write(ROOT / frozen, receipt)
    os.chmod(ROOT / frozen, 0o400)
    expected = {"reservationId": reservation_id, "operationId": operation_id,
                "protocol": PROTOCOL, "generation": snap["generation"], "queue": rows,
                "runtimeContract": contract, "checkpointPath": str(frozen),
                "checkpointHash": sha(ROOT / frozen)}
    request = {"seat": seat, "reservationId": reservation_id, "operationId": operation_id,
               "nodeId": target["node_id"], "generation": target["occupant_generation"],
               "validatorSession": validator,
               "reason": ("Owner-authorized bounded fleet context rotation. Successor: read frozen checkpoint "
                          + str((ROOT / frozen).resolve()) + "; verify the actual current native/runtime/queue tuple, "
                          + "author your own acknowledgment artifact, then run python3 "
                          + str((ROOT / "controller.py").resolve()) + " successor-ack --reservation "
                          + reservation_id + " --evidence <your-artifact>. Do not self-review or release the fence."),
               "profileSha256": profile_sha(target["codex_config_profile"]), "expected": expected}
    contract_path = ROOT / "contracts" / f"{reservation_id}.json"
    write(contract_path, request)
    entry["request_sha256"] = sha(contract_path)
    entry["policy_sha256"] = sha(ROOT / "policy.json")
    entry["attempt_mode"] = "manual" if manual else "automatic"
    entry["attempt"] = reservation_id
    entry["phase"] = "reservation_pending"
    entry["staged_at"] = now
    state[seat] = entry
    write(ROOT / "state.json", state)
    return {"attempt": reservation_id, "contract": str(contract_path), "nextActor": OPERATOR}


def load_attempt(reservation_id):
    path = ROOT / "contracts" / f"{reservation_id}.json"
    request = read(path)
    if not isinstance(request, dict) or request.get("reservationId") != reservation_id:
        raise RuntimeError("frozen attempt contract unavailable")
    seat = attempt_seat(request)
    entry = read(ROOT / "state.json", {}).get(seat, {})
    owners = [name for name, value in read(ROOT / "state.json", {}).items() if value.get("attempt") == reservation_id or reservation_id in value.get("completed", [])]
    if owners != [seat]:
        raise RuntimeError("attempt state is not exclusively bound to frozen seat")
    if not entry.get("request_sha256") or entry["request_sha256"] != sha(path):
        raise RuntimeError("frozen attempt contract changed after staging")
    with database() as db:
        target = seat_row(db, seat)
    if request["nodeId"] != target["node_id"]:
        raise RuntimeError("attempt targets different frozen seat/node")
    return request


def admit_mutation(reservation_id, request):
    """Recheck live policy and exact staged bytes at each pre-cutover write.

    This does not guard acknowledgment or release: disabling a controller after
    replacement must not strand an already committed reservation.
    """
    p = policy()
    if p.get("controller_enabled") is not True or p.get("automatic_cutover_enabled") is not True:
        raise RuntimeError("controller or automatic cutover disabled before mutation")
    seat = attempt_seat(request)
    if seat not in p["managed_unattended_seats"]:
        raise RuntimeError("fleet seat no longer allowlisted")
    state = read(ROOT / "state.json", {})
    entry = state.get(seat, {})
    if entry.get("attempt") != reservation_id or entry.get("policy_sha256") != sha(ROOT / "policy.json"):
        raise RuntimeError("staged controller policy changed before mutation")
    if entry.get("request_sha256") != sha(ROOT / "contracts" / f"{reservation_id}.json"):
        raise RuntimeError("frozen request changed before mutation")
    mode = entry.get("attempt_mode")
    if mode == "manual":
        if p.get("allow_manual_canary") is not True:
            raise RuntimeError("manual canary disabled before mutation")
    elif mode == "automatic":
        if p.get("automatic_trigger_enabled") is not True:
            raise RuntimeError("automatic trigger disabled before mutation")
    else:
        raise RuntimeError("staged attempt mode unavailable")
    if request.get("reservationId") != reservation_id or request.get("expected", {}).get("reservationId") != reservation_id:
        raise RuntimeError("frozen request identity changed")
    return p


def run_cli(operation, body, attempt):
    path = ROOT / "contracts" / f"{attempt}-{operation}.json"
    write(path, body)
    result = subprocess.run([RIG, "seat", "dispatch-reservation", operation, str(path)],
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"{operation} refused: {result.stderr.strip() or result.stdout.strip()}")
    return json.loads(result.stdout)


def task_id(reservation_id, kind):
    return f"rotation-{reservation_id}-{kind}"


def task_path(reservation_id, kind):
    return ROOT / "tasks" / f"{task_id(reservation_id, kind)}.txt"


def task_row(db, qitem_id):
    row = db.execute("""SELECT q.qitem_id,q.source_session,q.destination_session,q.body,q.state,q.minting_generation_uuid,
       q.claimed_by_generation_uuid,q.claimed_at,
       (SELECT identity_provenance FROM queue_transitions t WHERE t.qitem_id=q.qitem_id
        AND t.transition_note='created' ORDER BY t.transition_id LIMIT 1) AS provenance
       FROM queue_items q WHERE q.qitem_id=?""", (qitem_id,)).fetchone()
    return dict(row) if row else None


def coordinator_authority(destination):
    """Read native enrollment; absent tables mean legacy unenrolled daemon."""
    with database() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_authority'").fetchone():
            return None
        rows = db.execute("SELECT a.* FROM coordinator_authority a JOIN rigs r ON r.id=a.rig_id WHERE r.name=? AND r.archived_at IS NULL", (destination.split("@")[1],)).fetchall()
    if len(rows) > 1:
        raise RuntimeError("ambiguous enrolled coordinator authority")
    return dict(rows[0]) if rows else None


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def coordination_plan(rig_id):
    with database() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_operations'").fetchone():
            return None
        row = db.execute("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND kind='coordination-plan' ORDER BY rowid DESC LIMIT 1", (rig_id,)).fetchone()
    return json.loads(row["receipt"]) if row else None


def authenticated_post(operation, body):
    # All operations here are existing native Operator control routes.
    actor()
    token = os.environ.get("OPENRIG_TERMINAL_BEARER_TOKEN", "").strip()
    if not token:
        token_file = HOME / "terminal-token"
        if token_file.stat().st_mode & 0o077:
            raise RuntimeError("terminal bearer file permissions are too broad")
        token = token_file.read_text().strip()
    if not token:
        raise RuntimeError("authenticated local control unavailable")
    request = urllib.request.Request(policy()["daemon_url"] + "/api/coordinator/" + operation,
                                     data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + token,
                                              "X-OpenRig-Session": os.environ["OPENRIG_SESSION_NAME"],
                                              "X-OpenRig-Occupant-Generation": os.environ["OPENRIG_OCCUPANT_GENERATION"]})
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"native {operation} refused HTTP {error.code}; inspect exact durable operation before disposition") from None


def append_coordination_plan(route, route_path, settings):
    admission = route["admission"]
    rig_id, qid = admission["rigId"], admission["packageKey"]
    prior = coordination_plan(rig_id)
    if not prior:
        raise RuntimeError("existing admitted coordination plan required before enrolled refresh")
    if not settings:
        raise RuntimeError("explicit current qualification/capacity/effort admissions and recovery owner required")
    _operator, generation = actor(OPERATOR)
    ordinary_owner = admission["contract"]["destination"]
    recovery_owner = settings["recovery_owner"]
    owners = {ordinary_owner, recovery_owner}
    if any(owner not in settings["admissions"] for owner in owners):
        raise RuntimeError("explicit ordinary/recovery admission evidence required")
    if any(settings["admissions"][owner]["validUntil"] <= time.time()*1000 for owner in owners):
        raise RuntimeError("supplied current admission expired")
    # Configuration and generation are verified by native configure against current
    # rows. Existing stable body/package/predecessor fields are never rewritten.
    plan = json.loads(json.dumps(prior))
    for task in plan["tasks"]:
        if task["owner"] in settings["admissions"]:
            task["admission"] = settings["admissions"][task["owner"]]
    ordinary_body = pathlib.Path(route["bodyPath"]).read_text()
    recovery_key = qid + "-recovery"
    recovery_body = (f"Inspect only exact refresh task {qid} and route {route_path}; reconcile durable queue, reservation and effect markers. "
                     "Report the concrete blocker and preserved custody in your own artifact. Never retry a started or uncertain refresh, "
                     "reset an attempt, replace a coordination plan, manufacture acknowledgment, or release a rotation fence. "
                     "Return through the admitted package contract; expiration authorizes this inspection only.")
    recovery_contract = {"inputDigest": admission["contract"]["inputDigest"], "destination": recovery_owner,
                         "bodyHash": hashlib.sha256(recovery_body.encode()).hexdigest(), "resources": ["context-refresh-recovery:" + qid],
                         "returnContract": {"destination": OPERATOR, "evidenceRequired": ["authored-context-refresh-recovery"]}}
    recovery_admission = {"rigId": rig_id, "packageKey": recovery_key, "contract": recovery_contract}
    recovery_path = ROOT / "routes" / f"{recovery_key}-admission.json"
    write(recovery_path, recovery_admission)
    marker = ROOT / "effects" / f"{recovery_key}-admit.json"
    with database() as db:
        persisted = db.execute("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?", (rig_id, recovery_key)).fetchone()
    if not persisted:
        if read(marker):
            raise RuntimeError("prior recovery admission uncertain; no automatic retry")
        write(marker, {"phase": "admission_uncertain", "admission": recovery_admission})
        result = subprocess.run([RIG, "coordinator", "admit", str(recovery_path)], capture_output=True, text=True, timeout=120)
        with database() as db:
            persisted = db.execute("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?", (rig_id, recovery_key)).fetchone()
        if not persisted:
            raise RuntimeError("recovery admission refused or uncertain: " + (result.stderr.strip() or result.stdout.strip()))
    if json.loads(persisted["contract"]) != recovery_contract:
        raise RuntimeError("exact recovery package changed")
    deadline = int(time.time()*1000) + settings["deadline_seconds"]*1000
    new_tasks = [{"key":qid,"packageKey":qid,"owner":ordinary_owner,"action":"Prepare exact context refresh evidence at safe boundary", "deadline":deadline,
                  "body":ordinary_body,"predecessors":[],"admission":settings["admissions"][ordinary_owner]},
                 {"key":recovery_key,"packageKey":recovery_key,"owner":recovery_owner,"action":"Inspect blocked refresh and preserve uncertain attempt custody", "deadline":deadline,
                  "body":recovery_body,"recoveryFor":qid,"predecessors":[],"admission":settings["admissions"][recovery_owner]}]
    if any(task["key"] in (qid,recovery_key) for task in prior["tasks"]):
        if not all(any(t["key"]==new["key"] and t["body"]==new["body"] and t["packageKey"]==new["packageKey"] for t in prior["tasks"]) for new in new_tasks):
            raise RuntimeError("existing refresh plan task differs")
        return prior
    plan["tasks"].extend(new_tasks)
    plan["operatorGeneration"] = generation
    plan["revision"] = "refresh-" + canonical_digest({"prior":prior,"package":qid,"settings":settings,"operator":generation})[:32]
    plan_path = ROOT / "routes" / f"{qid}-plan.json"
    marker = ROOT / "effects" / f"{qid}-plan.json"
    attempted = read(marker)
    if attempted:
        with database() as db:
            existing = db.execute("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?", (rig_id,"coordination-plan:"+attempted["revision"])).fetchone()
        if existing and json.loads(existing["receipt"]) == read(plan_path):
            return json.loads(existing["receipt"])
        raise RuntimeError("prior plan configure uncertain; no automatic retry")
    write(plan_path, plan)
    write(marker, {"phase":"configure_uncertain","revision":plan["revision"]})
    authenticated_post("coordination-plan", plan)
    with database() as db:
        configured = db.execute("SELECT receipt FROM coordinator_operations WHERE rig_id=? AND operation_id=?", (rig_id,"coordination-plan:"+plan["revision"])).fetchone()
    if not configured or json.loads(configured["receipt"]) != plan:
        raise RuntimeError("plan configure response lacks exact durable revision")
    return plan


def route_admitted_task(reservation_id, kind, destination, body, summary, authority):
    actor(OPERATOR)
    if authority.get("state") != "active" or authority.get("lease_until", 0) <= time.time() * 1000:
        raise RuntimeError("current active coordinator required; recovery/expiry does not authorize dispatch")
    qid = task_id(reservation_id, kind)
    route_path = ROOT / "routes" / f"{qid}.json"
    body_path = task_path(reservation_id, kind)
    body += (f"\nEnrolled package return: after closing this exact worker task, from the same actual generation run "
             f"python3 {ROOT / 'controller.py'} worker-disposition --route {route_path} --evidence <your-authored-artifact>. "
             "This records the required durable package return and releases its resource through native coordinator dispose. "
             "It does not accept rotation. For checkpoint preparation, complete this disposition before the final checkpoint.")
    contract = {"inputDigest": ev.digest({"reservation": reservation_id, "kind": kind, "seat": destination}),
                "destination": destination, "bodyHash": hashlib.sha256(body.encode()).hexdigest(),
                "resources": ["context-refresh:" + destination],
                "returnContract": {"destination": OPERATOR, "evidenceRequired": ["authored-context-refresh-evidence"]}}
    admission = {"rigId": authority["rig_id"], "packageKey": qid, "contract": contract}
    prior = read(route_path)
    if prior and prior.get("admission") != admission:
        raise RuntimeError("frozen admitted route changed")
    write_text(body_path, body)
    admission_path = ROOT / "routes" / f"{qid}-admission.json"
    write(admission_path, admission)
    with database() as db:
        package = db.execute("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?", (authority["rig_id"], qid)).fetchone()
    if package:
        if json.loads(package["contract"]) != contract:
            raise RuntimeError("persisted admitted package differs")
    else:
        if prior:
            raise RuntimeError("prior admission outcome uncertain; no automatic retry")
        write(route_path, {"admission": admission, "bodyPath": str(body_path), "summary": summary, "phase": "admission_uncertain"})
        result = subprocess.run([RIG, "coordinator", "admit", str(admission_path)], capture_output=True, text=True, timeout=120)
        with database() as db:
            package = db.execute("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?", (authority["rig_id"], qid)).fetchone()
        if not package or json.loads(package["contract"]) != contract:
            raise RuntimeError("admission refused or uncertain; preserve route: " + (result.stderr.strip() or result.stdout.strip()))
    route = {"admission": admission, "bodyPath": str(body_path), "summary": summary, "phase": "admitted"}
    write(route_path, route)
    settings = next((config.get("coordination") for target, config in policy()["seat_policies"].items() if destination == target or destination == config["validator_seat"]), None)
    # Existing active plans require a frozen successor revision before routing.
    if coordination_plan(authority["rig_id"]):
        appended = append_coordination_plan(route, route_path, settings)
        route["planRevision"] = appended["revision"]
        route["workerQitemId"] = "qitem-coordination-" + hashlib.sha256((authority["rig_id"] + ":" + qid).encode()).hexdigest()[:24]
        write(route_path, route)
    owner = authority["owner_session"]
    # Coordination traffic to registered coordinator is explicitly supported by
    # scope(); it does not assign worker work or impersonate the baton holder.
    routed = publish_task(reservation_id, kind + "-coordination", owner,
                          f"Exact admitted refresh package {qid}. Inspect {route_path}, retain current baton/custody and runtime plan. "
                          f"From your actual current coordinator shell run python3 {ROOT / 'controller.py'} coordinator-dispatch --route {route_path}. "
                          "Do not replace a coordination plan, recover authority, or retry an uncertain dispatch. Return a concrete hold if no eligible slot exists.",
                          "Dispatch admitted context refresh through current coordinator", _coordination=True)
    return {**routed, "workerQitemId": route.get("workerQitemId", qid), "route": str(route_path), "phase": "awaiting_coordinator_dispatch"}


def coordinator_dispatch(route_path):
    path = pathlib.Path(route_path).resolve()
    if path.parent != (ROOT / "routes").resolve():
        raise RuntimeError("route must be a local frozen route")
    route = read(path)
    if not isinstance(route, dict) or route.get("phase") != "admitted":
        raise RuntimeError("admitted frozen route required")
    admission = route["admission"]
    qid, contract = admission["packageKey"], admission["contract"]
    source, generation = actor()
    authority = coordinator_authority(contract["destination"])
    if not authority or authority["rig_id"] != admission["rigId"] or authority["owner_session"] != source or authority["owner_generation"] != generation or authority["state"] != "active" or authority["lease_until"] <= time.time() * 1000:
        raise RuntimeError("actual current coordinator owner/token required")
    with database() as db:
        baton = task_row(db, authority.get("baton_id", ""))
    if not baton or baton["destination_session"] != source or baton["state"] != "in-progress" or baton["claimed_by_generation_uuid"] != generation or not baton["claimed_at"]:
        return {"phase":"held_current_baton_required","recoveryOwner":OPERATOR,
                "action":"Reconcile exact current holder, canonical baton and obligations; perform only authorized native self-transfer and acknowledgment before dispatch",
                "wake":"Run coordinator-dispatch again only after native current baton acknowledgment",
                "noAutomaticRetry":True}
    body = pathlib.Path(route["bodyPath"]).read_text()
    if hashlib.sha256(body.encode()).hexdigest() != contract["bodyHash"]:
        raise RuntimeError("frozen admitted task body changed")
    if route.get("planRevision"):
        marker = ROOT / "effects" / f"{qid}-reconcile.json"
        def assignment():
            with database() as db:
                return db.execute("SELECT queue_id FROM coordinator_assignments WHERE rig_id=? AND package_key=?", (authority["rig_id"], qid)).fetchone()
        existing = assignment()
        if existing:
            if existing["queue_id"] != route["workerQitemId"]:
                raise RuntimeError("planned refresh has different native queue identity")
            return {"qitemId": existing["queue_id"], "persisted": True}
        if read(marker, {}).get("phase") == "reconcile_uncertain":
            raise RuntimeError("prior coordination reconcile uncertain; no automatic retry")
        write(marker, {"phase":"reconcile_uncertain","packageKey":qid})
        result = authenticated_post("coordination-reconcile", {"rigId":authority["rig_id"]})
        existing = assignment()
        write(marker, {"phase":"observed","packageKey":qid})
        if not existing:
            return {"phase":"held_by_native_coordination","packageKey":qid,"results":result}
        if existing["queue_id"] != route["workerQitemId"]:
            raise RuntimeError("planned refresh has different native queue identity")
        return {"qitemId":existing["queue_id"],"persisted":True}
    envelope = {"token": {"rigId": authority["rig_id"], "epoch": authority["epoch"], "generation": generation}, "packageKey": qid}
    envelope_path = ROOT / "routes" / f"{qid}-dispatch.json"
    write(envelope_path, envelope)
    # qid is already the full native task identity, so use the exact queue seam.
    return create_task(qid, contract["destination"], body, route["summary"], source, generation, envelope_path)


def worker_disposition(route_path, evidence_path):
    path = pathlib.Path(route_path).resolve()
    if path.parent != (ROOT / "routes").resolve():
        raise RuntimeError("route must be a local frozen route")
    route = read(path)
    if not isinstance(route, dict) or route.get("phase") != "admitted":
        raise RuntimeError("admitted frozen route required")
    admission = route["admission"]
    qid, contract = admission["packageKey"], admission["contract"]
    worker_qid = route.get("workerQitemId", qid)
    source, generation = actor(contract["destination"])
    artifact = pathlib.Path(evidence_path).resolve()
    if not artifact.is_file():
        raise RuntimeError("authored return artifact required")
    with database() as db:
        package = db.execute("SELECT contract FROM coordinator_packages WHERE rig_id=? AND package_key=?", (admission["rigId"], qid)).fetchone()
        task = task_row(db, worker_qid)
    if not package or json.loads(package["contract"]) != contract or not task or task["destination_session"] != source or task["claimed_by_generation_uuid"] != generation or task["state"] not in ("done", "failed", "denied", "canceled", "handed-off") or task["provenance"] not in (("transport:v1", "system:operator-authorized-coordination") if route.get("planRevision") else ("transport:v1",)) or hashlib.sha256(task["body"].encode()).hexdigest() != contract["bodyHash"]:
        raise RuntimeError("exact terminal admitted worker task and current generation required")
    returned = qid + "-disposition-return"
    body = json.dumps({"packageKey": qid, "inputDigest": contract["inputDigest"],
                       "evidence": [{"kind": "authored-context-refresh-evidence", "ref": str(artifact)}]}, sort_keys=True)
    receipt = create_task(returned, contract["returnContract"]["destination"], body, "Attributed context refresh package disposition", source, generation)
    disposition_path = ROOT / "routes" / f"{qid}-disposition.json"
    write(disposition_path, {"rigId": admission["rigId"], "packageKey": qid, "dispositionId": returned})
    result = subprocess.run([RIG, "coordinator", "dispose", str(disposition_path)], capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError("native package disposition refused; retain resources: " + (result.stderr.strip() or result.stdout.strip()))
    with database() as db:
        disposed = db.execute("SELECT disposition_id FROM coordinator_assignments WHERE rig_id=? AND package_key=?", (admission["rigId"], qid)).fetchone()
    if not disposed or disposed["disposition_id"] != returned:
        raise RuntimeError("package disposition response lacks exact durable return")
    return {"phase": "package_disposed", "qitemId": receipt["qitemId"], "rotationAcceptance": False}


def create_task(qid, destination, body, summary, sender, generation, dispatch_path=None):
    effect_path = ROOT / "effects" / f"{qid}.json"
    with database() as db:
        existing = task_row(db, qid)
    if existing:
        if (existing["source_session"], existing["destination_session"], existing["body"], existing["provenance"]) != (sender, destination, body, "transport:v1") or existing["minting_generation_uuid"] != generation:
            raise RuntimeError("existing actor task differs from exact transport-authored packet")
        return {"qitemId": qid, "persisted": True, "state": existing["state"]}
    if read(effect_path):
        raise RuntimeError("prior queue create outcome uncertain; no automatic retry")
    body_path = ROOT / "tasks" / f"{qid}.txt"
    write_text(body_path, body)
    command = [RIG, "queue", "create", "--destination", destination, "--id", qid,
               "--body-file", str(body_path), "--summary", summary, "--json"]
    if dispatch_path:
        command.extend(["--dispatch-file", str(dispatch_path)])
    write(effect_path, {"phase": "create_uncertain", "bodyHash": hashlib.sha256(body.encode()).hexdigest(), "sender": sender, "generation": generation})
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    with database() as db:
        persisted = task_row(db, qid)
    if not persisted or (persisted["source_session"], persisted["destination_session"], persisted["body"], persisted["provenance"], persisted["minting_generation_uuid"]) != (sender, destination, body, "transport:v1", generation):
        raise RuntimeError("actor task create refused/uncertain; inspect exact queue row before disposition: " + (result.stderr.strip() if result.returncode else "no persisted task"))
    write(effect_path, {"phase": "persisted", "qitemId": qid})
    return {"qitemId": qid, "persisted": True, "state": persisted["state"]}


def publish_task(reservation_id, kind, destination, body, summary, _coordination=False):
    """Native queue only; enrolled workers are assigned through their coordinator."""
    sender, generation = actor()
    qid = task_id(reservation_id, kind)
    write_text(task_path(reservation_id, kind), body)
    authority = coordinator_authority(destination)
    if authority and destination not in json.loads(authority["coordinators"]) and not destination.endswith("@kernel"):
        if _coordination:
            raise RuntimeError("coordinator roster changed before coordination routing")
        return route_admitted_task(reservation_id, kind, destination, body, summary, authority)
    return create_task(qid, destination, body, summary, sender, generation)


def write_text(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(value)
    os.chmod(temp, 0o600)
    temp.replace(path)


def require_claim(reservation_id, kind, expected_recipient, generation):
    package_id = task_id(reservation_id, kind)
    route = read(ROOT / "routes" / f"{package_id}.json", {})
    qid = route.get("workerQitemId", package_id)
    with database() as db:
        row = task_row(db, qid)
        assigned = db.execute("SELECT queue_id FROM coordinator_assignments WHERE rig_id=? AND package_key=?", (route["admission"]["rigId"],package_id)).fetchone() if route.get("planRevision") else None
    frozen = task_path(reservation_id, kind)
    provenance = ("transport:v1", "system:operator-authorized-coordination") if route.get("planRevision") and assigned and assigned["queue_id"] == qid else ("transport:v1",)
    if not row or row["destination_session"] != expected_recipient or row["state"] != "in-progress" or row["claimed_by_generation_uuid"] != generation or not row["claimed_at"] or row["provenance"] not in provenance or not row["minting_generation_uuid"] or not frozen.exists() or row["body"] != frozen.read_text():
        raise RuntimeError("actual current recipient has not claimed exact durable actor task")
    return row


def operator_reserve(reservation_id):
    actor(OPERATOR)
    req = load_attempt(reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
    if r:
        if r["operation_id"] != req["operationId"]:
            raise RuntimeError("durable reservation mismatch")
        return {"reservation": reservation_id, "state": r["state"]}
    admit_mutation(reservation_id, req)
    state = read(ROOT / "state.json", {})
    entry = state[attempt_seat(req)]
    if entry.get("phase") == "reserve_uncertain":
        raise RuntimeError("prior reserve outcome uncertain; no automatic retry")
    entry["phase"] = "reserve_uncertain"
    write(ROOT / "state.json", state)
    result = run_cli("reserve", req, reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
    if not r or r["state"] != "reserved":
        raise RuntimeError("reserve response lacks durable reservation")
    entry["phase"] = "reserved"
    write(ROOT / "state.json", state)
    return {"reservation": reservation_id, "state": r["state"], "api": result.get("reservation_id")}


def handover_request(req, p):
    # This command is only callable from the verified current managed Operator shell.
    token = os.environ.get("OPENRIG_TERMINAL_BEARER_TOKEN", "").strip()
    if not token:
        token_file = HOME / "terminal-token"
        if token_file.stat().st_mode & 0o077:
            raise RuntimeError("terminal bearer file permissions are too broad")
        token = token_file.read_text().strip()
    if not token:
        raise RuntimeError("authenticated local control unavailable")
    body = {"source": "fresh", "reason": req["reason"], "operator": "current-kernel-operator",
            "rotationExpected": req["expected"]}
    url = p["daemon_url"] + "/api/seat/handover/" + urllib.parse.quote(attempt_seat(req), safe="")
    request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + token,
                                              "X-OpenRig-Session": os.environ["OPENRIG_SESSION_NAME"],
                                              "X-OpenRig-Occupant-Generation": os.environ["OPENRIG_OCCUPANT_GENERATION"]})
    try:
        with urllib.request.urlopen(request, timeout=240) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"guarded handover refused HTTP {error.code}; inspect durable reservation before any retry") from None


def operator_handover(reservation_id):
    actor(OPERATOR)
    req = load_attempt(reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
    if not r or r["state"] != "reserved" or r["operation_id"] != req["operationId"]:
        raise RuntimeError("exact durable reserved state required; no retry of started/committed cutover")
    # A crash after this write is intentionally ambiguous; reconcile durable daemon state.
    state = read(ROOT / "state.json", {})
    if state.get(attempt_seat(req), {}).get("phase") == "handover_uncertain":
        raise RuntimeError("prior handover outcome uncertain; no automatic retry")
    current_policy = admit_mutation(reservation_id, req)
    state[attempt_seat(req)]["phase"] = "handover_uncertain"
    write(ROOT / "state.json", state)
    result = handover_request(req, current_policy)
    with database() as db:
        after = reservation(db, reservation_id)
    if not after or after["state"] != "committed":
        raise RuntimeError("handover response lacks committed successor; fence retained")
    state[attempt_seat(req)]["phase"] = "awaiting_successor_ack"
    write(ROOT / "state.json", state)
    routed = operator_route_validator(reservation_id)
    return {"reservation": reservation_id, "state": after["state"], "successorGeneration": after["successor_generation"], "result": result.get("ok"), "validatorTask": routed["qitemId"]}


def operator_route_validator(reservation_id):
    actor(OPERATOR)
    req = load_attempt(reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
    if not r or r["state"] != "committed" or r["operation_id"] != req["operationId"] or not r["successor_generation"]:
        raise RuntimeError("committed successor required before validator routing")
    validator = req.get("validatorSession")
    if not isinstance(validator, str) or validator in (attempt_seat(req), OPERATOR):
        raise RuntimeError("exact staged independent validator unavailable")
    body = (f"Independent fleet rotation validation task {reservation_id}. Frozen contract: "
            f"{ROOT / 'contracts' / (reservation_id + '.json')} (SHA-256 "
            f"{sha(ROOT / 'contracts' / (reservation_id + '.json'))}). Verify current successor generation "
            f"{r['successor_generation']}, authored checkpoint consumption, native/runtime/queue continuity "
            f"and your independence from the author, reserver, performer and successor. Claim this queue item "
            f"in your current managed session. Author your own pass/fail artifact. Only if PASS, run "
            f"python3 {ROOT / 'controller.py'} validator-attest --reservation {reservation_id} "
            f"--evidence <your-artifact>. Once the Operator return task is durably persisted, close this "
            f"review task as handed_off_to that returned qitem. Do not copy the Operator's verdict or release the fence.")
    return publish_task(reservation_id, "validator", validator, body, "Independently validate exact fleet successor and checkpoint")


def successor_ack(reservation_id, evidence_path):
    req = load_attempt(reservation_id)
    seat, generation = actor(attempt_seat(req))
    with database() as db:
        r = reservation(db, reservation_id)
    if not r or r["state"] != "committed" or r["successor_generation"] != generation:
        raise RuntimeError("actual committed successor generation required")
    snap = ev.snapshot(seat)
    frozen = read(ROOT / req["expected"]["checkpointPath"])
    if snap["generation"] == frozen["generation"] or ev.runtime_contract(snap) != req["expected"]["runtimeContract"]:
        raise RuntimeError("native successor identity/runtime continuity failed")
    if not ev.queue_continuity(req["expected"]["queue"], ev.queue(seat), r["predecessor_generation"], generation):
        raise RuntimeError("queue continuity failed")
    report = read(evidence_path)
    if not isinstance(report, dict) or report.get("reservationId") != reservation_id or report.get("verdict") != "acknowledged" or report.get("checkpointHash") != req["expected"]["checkpointHash"]:
        raise RuntimeError("actual successor must author exact checkpoint acknowledgment")
    return run_cli("attest", {"reservationId": reservation_id, "operationId": req["operationId"],
                              "checkpointHash": req["expected"]["checkpointHash"], "kind": "successor_ack",
                              "evidenceRef": str(pathlib.Path(evidence_path).resolve())}, reservation_id)


def validator_attest(reservation_id, evidence_path):
    seat, generation = actor()
    req = load_attempt(reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
    if not r or r["state"] != "committed" or seat != req.get("validatorSession") or seat in (attempt_seat(req), r["actor_session"], r.get("performer_session")):
        raise RuntimeError("distinct current validator required")
    require_claim(reservation_id, "validator", seat, generation)
    with database() as db:
        audit = reservation_audit(db, reservation_id)
    if not any(x["action"] == "successor_ack" and x["actor_session"] == attempt_seat(req) and x["actor_generation"] == r["successor_generation"] for x in audit):
        raise RuntimeError("actual successor acknowledgment required before validation")
    report = read(evidence_path)
    if not isinstance(report, dict) or report.get("verdict") != "pass" or report.get("reservationId") != reservation_id or report.get("checkpointHash") != req["expected"]["checkpointHash"] or report.get("validatorSession") != seat or report.get("validatorGeneration") != generation:
        raise RuntimeError("actual validator-authored exact pass evidence required")
    attested = run_cli("attest", {"reservationId": reservation_id, "operationId": req["operationId"],
                              "checkpointHash": req["expected"]["checkpointHash"], "kind": "independent_acceptance",
                              "evidenceRef": str(pathlib.Path(evidence_path).resolve())}, reservation_id)
    returned = validator_return(reservation_id)
    return {"attestation": attested, "operatorTask": returned["qitemId"]}


def validator_return(reservation_id):
    seat, generation = actor()
    req = load_attempt(reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
        audit = reservation_audit(db, reservation_id) if r else []
    if not r or r["state"] != "committed" or seat != req.get("validatorSession"):
        raise RuntimeError("exact current validator return required")
    require_claim(reservation_id, "validator", seat, generation)
    if not any(x["action"] == "independent_acceptance" and x["actor_session"] == seat and x["actor_generation"] == generation for x in audit):
        raise RuntimeError("durable independent validator attestation required before return")
    body = (f"Fleet rotation {reservation_id} has actual successor and independent validator audit receipts. "
            f"Claim this return task in your current Kernel Operator session, inspect exact contract "
            f"{ROOT / 'contracts' / (reservation_id + '.json')} and daemon reservation audit, then run "
            f"python3 {ROOT / 'controller.py'} operator-release --reservation {reservation_id} "
            f"only if current receipts and successor still pass. After durable release, close this task "
            f"with no-follow-on. A failed receipt keeps the fence.")
    return publish_task(reservation_id, "operator-return", OPERATOR, body, "Release Fleet rotation only after two current actor receipts")


def operator_release(reservation_id):
    _seat, generation = actor(OPERATOR)
    req = load_attempt(reservation_id)
    require_claim(reservation_id, "operator-return", OPERATOR, generation)
    with database() as db:
        r = reservation(db, reservation_id)
        audit = reservation_audit(db, reservation_id)
    if not r or r["state"] != "committed" or not {"successor_ack", "independent_acceptance"}.issubset({x["action"] for x in audit}):
        raise RuntimeError("committed successor and two durable actor receipts required")
    result = run_cli("release", {"reservationId": reservation_id, "operationId": req["operationId"],
                                 "reason": "actual-successor-and-independent-validator-accepted",
                                 "mode": "accepted_successor"}, reservation_id)
    with database() as db:
        after = reservation(db, reservation_id)
    if not after or after["state"] != "released":
        raise RuntimeError("release not durable")
    state = read(ROOT / "state.json", {})
    state[attempt_seat(req)]["phase"] = "released"
    state[attempt_seat(req)].setdefault("completed", []).append(reservation_id)
    state[attempt_seat(req)].pop("attempt", None)
    state[attempt_seat(req)].pop("idle", None)
    write(ROOT / "state.json", state)
    return {"reservation": reservation_id, "state": after["state"], "api": result.get("state")}


def reconcile(reservation_id):
    req = load_attempt(reservation_id)
    with database() as db:
        r = reservation(db, reservation_id)
        audit = reservation_audit(db, reservation_id) if r else []
    if not r:
        phase = "reservation_pending"
    elif r["state"] == "reserved":
        phase = "reserved_or_handover_uncertain"
    elif r["state"] == "started":
        phase = "replacement_started_uncertain"
    elif r["state"] == "committed":
        phase = "awaiting_actor_receipts" if not {"successor_ack", "independent_acceptance"}.issubset({x["action"] for x in audit}) else "awaiting_operator_release"
    else:
        phase = "released"
    return {"reservation": reservation_id, "operation": req["operationId"], "durableState": r["state"] if r else None,
            "phase": phase, "auditActions": [x["action"] for x in audit], "noAutomaticRetry": True}


def tick_seat(seat):
    """One seat only; actors execute authenticated mutations."""
    p = seat_policy(seat)
    state = read(ROOT / "state.json", {})
    entry = state.get(seat, {})
    if entry.get("attempt"):
        status = reconcile(entry["attempt"])
        if status["durableState"] == "released":
            entry.setdefault("completed", []).append(entry.pop("attempt"))
            entry.pop("idle", None)
            entry["phase"] = "released"
            state[seat] = entry
            write(ROOT / "state.json", state)
        return status
    if p.get("controller_enabled") is not True:
        return {"phase": "disabled", "mutated": False}
    snap = ev.snapshot(seat)
    used, count, _model = metrics(seat, snap)
    if entry.get("baselineGeneration") != snap["generation"]:
        entry = {"baselineGeneration": snap["generation"], "baselineCompactions": count, "phase": "monitoring"}
    delta = count - entry["baselineCompactions"]
    if used >= p["prepare_percent"] or delta >= p["compactions"]:
        receipt = read(ROOT / "receipts" / f"{seat}.json")
        if not receipt_ok(receipt, snap, ev.queue(seat), time.time()) and entry.get("phase") == "monitoring":
            if p.get("operator_auto_execute_enabled") is not True:
                return {"phase": "checkpoint_requires_current_operator", "seat": seat, "mutated": False}
            actor(OPERATOR)
            task = publish_task(
                "prep-" + hashlib.sha256(seat.encode()).hexdigest()[:16] + "-" + snap["generation"], "checkpoint", seat,
                "At a safe task boundary prepare a continuity packet with current_work, decisions, memory, "
                "constraints, standing_duties, evidence, next_action and reconciled empty outstanding_effects. "
                "Close this preparation task in your current queue before authoring the final checkpoint, "
                f"then run python3 {ROOT / 'controller.py'} checkpoint --seat {seat} --packet <your-packet> from your current managed seat "
                "and stay quiescent pending Operator disposition. This task alone does not authorize rotation.",
                "Prepare fleet continuity checkpoint at a safe idle boundary")
            entry["phase"] = "checkpoint_requested"
            entry["requested_at"] = time.time()
            state[seat] = entry
            write(ROOT / "state.json", state)
            return {"phase": "checkpoint_requested", "seat": seat, "qitemId": task["qitemId"]}
        if receipt_ok(receipt, snap, ev.queue(seat), time.time()):
            try:
                observed = observe(seat)
            except RuntimeError as error:
                return {"phase": "waiting_positive_idle", "reason": str(error)}
            if p.get("automatic_trigger_enabled") and p.get("automatic_cutover_enabled") and (used >= p["rotate_percent"] or delta >= p["compactions"]):
                if observed["observations"] == 2:
                    if p.get("operator_auto_execute_enabled") is True:
                        # A scheduled Operator-owned tick may execute the full guarded
                        # mutation; a host poller has no authority to do so.
                        actor(OPERATOR)
                    staged = stage(seat)
                    if p.get("operator_auto_execute_enabled") is True:
                        operator_reserve(staged["attempt"])
                        return operator_handover(staged["attempt"])
                    return staged
            return {"phase": "checkpointed", "idleObservations": observed["observations"], "automaticTriggerEnabled": p.get("automatic_trigger_enabled") is True}
    state[seat] = entry
    write(ROOT / "state.json", state)
    return {"phase": entry.get("phase", "monitoring"), "usedPercentage": used, "compactionsSinceBaseline": delta}


def tick():
    p = policy()
    if p["controller_enabled"] is not True:
        # Preserve reconciliation visibility even while activation is disabled.
        state = read(ROOT / "state.json", {})
        if not any(state.get(seat, {}).get("attempt") for seat in p["managed_unattended_seats"]):
            return {"phase": "disabled", "mutated": False}
    results = {}
    for seat in p["managed_unattended_seats"]:
        try:
            results[seat] = tick_seat(seat)
        except (RuntimeError, TimeoutError, subprocess.TimeoutExpired) as error:
            results[seat] = {"phase": "hold", "reason": str(error), "noAutomaticRetry": True}
    return {"seats": results}


def schedule(until):
    """Finite deterministic duty launched by the actual current native Operator.

    Inherits the real actor environment. No daemonization, token copying, actor
    synthesis or ambiguous-effect retry is performed by this entry point.
    """
    if type(until) not in (int, float) or not time.time() < until < float("inf"):
        raise RuntimeError("explicit finite future --until Unix timestamp required")
    _seat, operator_generation = actor(OPERATOR)
    started = time.time()
    summary = {"phase":"running","startedAt":started,"until":until,"iterations":0,"phaseCounts":{}}
    def finish(phase):
        summary["phase"] = phase
        summary["finishedAt"] = time.time()
        write(ROOT / "scheduler-summary.json", summary)
        return summary
    with open(ROOT / "scheduler.lock", "w") as scheduler_lock:
        try:
            fcntl.flock(scheduler_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another finite scheduler already owns this controller") from None
        while time.time() < until:
            # Each tick owns the same lock as all standalone actor commands;
            # sleep never retains that lock or blocks checkpoint/acknowledgment.
            with open(ROOT / "controller.lock", "w") as controller_lock:
                try:
                    fcntl.flock(controller_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    # A concurrent native checkpoint/ack is normal. No tick or
                    # mutation started, so skip this observation without retiring
                    # the scheduler or retrying an uncertain operation.
                    summary["contentionSkips"] = summary.get("contentionSkips", 0) + 1
                    write(ROOT / "scheduler-summary.json", summary)
                    time.sleep(min(1, max(0, until - time.time())))
                    continue
                if time.time() >= until:
                    return finish("end_time_reached")
                try:
                    _seat, current_generation = actor(OPERATOR)
                    if current_generation != operator_generation:
                        return finish("operator_generation_retired")
                    p = policy()
                    if any(p.get(flag) is not True for flag in ("controller_enabled", "automatic_trigger_enabled", "automatic_cutover_enabled", "operator_auto_execute_enabled")):
                        return finish("policy_refused")
                    interval = min(seat_policy(seat)["poll_seconds"] for seat in p["managed_unattended_seats"])
                    result = tick()
                except (RuntimeError, TimeoutError, subprocess.TimeoutExpired, OSError, ValueError):
                    # Deliberately exclude arbitrary exception text/native output
                    # from this bounded nonsecret operational summary.
                    return finish("actor_or_policy_or_tick_refused")
                summary["iterations"] += 1
                phases = [item.get("phase", "observed") for item in result.get("seats", {}).values()] or [result.get("phase", "observed")]
                for phase in phases:
                    # Fixed aggregate counters prevent arbitrary upstream strings
                    # from becoming a log or unbounded operational history.
                    label = phase if phase in ("hold", "monitoring", "checkpoint_requested", "checkpointed", "disabled", "released") else "other"
                    summary["phaseCounts"][label] = summary["phaseCounts"].get(label, 0) + 1
                write(ROOT / "scheduler-summary.json", summary)
            wake_at = min(time.time() + interval, until)
            while True:
                remaining = wake_at - time.time()
                if remaining <= 0:
                    break
                time.sleep(min(60, remaining))
        return finish("end_time_reached")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["tick", "observe", "stage", "manual-stage", "checkpoint", "operator-reserve", "operator-handover", "operator-route-validator", "successor-ack", "validator-attest", "validator-return", "operator-release", "reconcile", "coordinator-dispatch", "worker-disposition", "schedule"])
    parser.add_argument("--seat")
    parser.add_argument("--packet")
    parser.add_argument("--route")
    parser.add_argument("--reservation")
    parser.add_argument("--evidence")
    parser.add_argument("--until", type=float, help="Finite scheduler end time as Unix seconds")
    args = parser.parse_args(argv)
    action = args.action
    if action == "schedule":
        if args.until is None:
            parser.error("--until required for finite scheduler")
        print(json.dumps(schedule(args.until), indent=2))
        return
    if action in ("checkpoint", "observe", "stage", "manual-stage") and not args.seat:
        parser.error("--seat required for explicit seat actions")
    with open(ROOT / "controller.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if action == "worker-disposition": result = worker_disposition(args.route, args.evidence)
        elif action == "coordinator-dispatch": result = coordinator_dispatch(args.route)
        elif action == "checkpoint": result = checkpoint(args.seat, args.packet)
        elif action == "successor-ack": result = successor_ack(args.reservation, args.evidence)
        elif action == "validator-attest": result = validator_attest(args.reservation, args.evidence)
        elif action == "validator-return": result = validator_return(args.reservation)
        elif action == "tick": result = tick()
        elif action == "observe": result = observe(args.seat)
        elif action == "stage": result = stage(args.seat)
        elif action == "manual-stage": result = stage(args.seat, manual=True)
        elif action == "operator-reserve": result = operator_reserve(args.reservation)
        elif action == "operator-handover": result = operator_handover(args.reservation)
        elif action == "operator-route-validator": result = operator_route_validator(args.reservation)
        elif action == "operator-release": result = operator_release(args.reservation)
        else: result = reconcile(args.reservation)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
