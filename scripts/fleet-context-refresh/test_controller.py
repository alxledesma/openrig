import hashlib
import contextlib
import json
import pathlib
import sqlite3
import tempfile
import time
import unittest
import urllib.request
from unittest.mock import patch

import controller as c


class ControllerContract(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.dbpath = self.home / "openrig.sqlite"
        db = sqlite3.connect(self.dbpath)
        db.executescript("""
        CREATE TABLE rigs(id TEXT PRIMARY KEY,name TEXT,archived_at TEXT);
        CREATE TABLE nodes(id TEXT PRIMARY KEY,rig_id TEXT,codex_config_profile TEXT);
        CREATE TABLE bindings(node_id TEXT,tmux_session TEXT);
        CREATE TABLE occupant_tenures(node_id TEXT,generation_ordinal INT,generation_uuid TEXT);
        CREATE TABLE sessions(id TEXT,node_id TEXT,status TEXT,startup_status TEXT);
        CREATE TABLE seat_dispatch_reservations(reservation_id TEXT,operation_id TEXT,node_id TEXT,state TEXT,
            predecessor_generation TEXT,successor_generation TEXT,actor_session TEXT,successor_native_id TEXT,performer_session TEXT);
        CREATE TABLE seat_dispatch_reservation_audit(id INTEGER PRIMARY KEY, reservation_id TEXT,
            action TEXT,actor_session TEXT,actor_generation TEXT,evidence_json TEXT,created_at TEXT);
        CREATE TABLE self_host_identity(singleton INT PRIMARY KEY,host_id TEXT);
        CREATE TABLE queue_items(qitem_id TEXT,destination_session TEXT,state TEXT,claimed_at TEXT,
            ts_updated TEXT,claimed_by_generation_uuid TEXT,body TEXT,source_session TEXT,minting_generation_uuid TEXT);
        CREATE TABLE queue_transitions(transition_id INTEGER PRIMARY KEY,qitem_id TEXT,transition_note TEXT,identity_provenance TEXT);
        """)
        db.execute("INSERT INTO rigs VALUES('forge','forge',NULL)")
        db.execute("INSERT INTO rigs VALUES('kernel','kernel',NULL)")
        db.execute("INSERT INTO nodes VALUES('qa','forge','forge-sol61-low-continuity')")
        db.execute("INSERT INTO bindings VALUES('qa','builder@forge')")
        db.execute("INSERT INTO occupant_tenures VALUES('qa',1,'occupant-old')")
        db.execute("INSERT INTO nodes VALUES('validator','forge','review-profile')")
        db.execute("INSERT INTO bindings VALUES('validator','review@forge')")
        db.execute("INSERT INTO occupant_tenures VALUES('validator',1,'check-gen')")
        db.execute("INSERT INTO self_host_identity VALUES(1,'fixture-host')")
        db.commit(); db.close()
        self.patchers = [patch.object(c, "ROOT", self.root), patch.object(c, "HOME", self.home),
                         patch.object(c.ev, "RIG_HOME", self.home)]
        for p in self.patchers: p.start()
        self.policy = {"production_only": True, "daemon_url": "http://127.0.0.1:17433", "rigs": ["forge"],
                       "managed_unattended_seats": ["builder@forge"], "prepare_percent": 75,
                       "rotate_percent": 85, "compactions": 2, "poll_seconds": 120,
                       "controller_enabled": False, "automatic_cutover_enabled": False,
                       "automatic_trigger_enabled": False, "allow_manual_canary": False,
                       "atomic_cutover_protocol": c.PROTOCOL, "operator_auto_execute_enabled": False,
                       "seat_policies": {"builder@forge": {"validator_seat": "review@forge"}}}
        c.write(self.root / "policy.json", self.policy)

    def tearDown(self):
        for p in reversed(self.patchers): p.stop()
        self.tmp.cleanup()

    def update_policy(self, **changes):
        self.policy.update(changes)
        c.write(self.root / "policy.json", self.policy)

    @contextlib.contextmanager
    def db(self):
        db = sqlite3.connect(self.dbpath)
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def insert_reservation(self, state="reserved"):
        db = sqlite3.connect(self.dbpath)
        db.execute("INSERT INTO seat_dispatch_reservations VALUES(?,?,?,?,?,?,?,?,?)",
                   ("qa-test", "cutover-test", "qa", state, "occupant-old", "occupant-new" if state == "committed" else None, c.OPERATOR, "new-native" if state == "committed" else None, c.OPERATOR if state == "committed" else None))
        db.commit(); db.close()

    def request(self):
        req = {"seat": "builder@forge", "reservationId": "qa-test", "operationId": "cutover-test", "nodeId": "qa",
               "generation": "occupant-old", "reason": "test", "profileSha256": "a" * 64, "validatorSession": "review@forge",
               "expected": {"reservationId": "qa-test", "operationId": "cutover-test",
                            "protocol": c.PROTOCOL, "checkpointHash": "b" * 64}}
        c.write(self.root / "contracts/qa-test.json", req)
        state = c.read(self.root / "state.json", {})
        entry = state.setdefault("builder@forge", {})
        entry.update(attempt="qa-test", request_sha256=c.sha(self.root / "contracts/qa-test.json"))
        c.write(self.root / "state.json", state)
        return req

    def staged(self, phase="reservation_pending"):
        self.update_policy(controller_enabled=True, automatic_cutover_enabled=True, allow_manual_canary=True)
        req = self.request()
        c.write(self.root / "state.json", {"builder@forge": {"attempt": "qa-test", "phase": phase,
          "attempt_mode": "manual", "request_sha256": c.sha(self.root / "contracts/qa-test.json"),
          "policy_sha256": c.sha(self.root / "policy.json")}})
        return req

    def task(self, kind, destination, source, generation, claim_generation=None, body="task"):
        qid = c.task_id("qa-test", kind)
        c.write_text(c.task_path("qa-test", kind), body)
        db = sqlite3.connect(self.dbpath)
        db.execute("INSERT INTO queue_items VALUES(?,?,?,?,?,?,?,?,?)",
                   (qid, destination, "in-progress" if claim_generation else "pending", "now" if claim_generation else None,
                    "now", claim_generation, body, source, generation))
        db.execute("INSERT INTO queue_transitions(qitem_id,transition_note,identity_provenance) VALUES(?, 'created','transport:v1')", (qid,))
        db.commit(); db.close()
        return qid

    def test_disabled_policy_blocks_poll_effects(self):
        with patch.object(c.ev, "snapshot", side_effect=AssertionError("should not sample")):
            self.assertEqual(c.tick_seat("builder@forge"), {"phase": "disabled", "mutated": False})

    def test_operator_auto_execution_requires_current_actor_before_staging(self):
        self.update_policy(controller_enabled=True, automatic_cutover_enabled=True,
                           automatic_trigger_enabled=True, operator_auto_execute_enabled=True)
        c.write(self.root / "state.json", {"builder@forge": {"baselineGeneration": "native-old",
                                                    "baselineCompactions": 0, "phase": "checkpoint_requested"}})
        snap = {"generation": "native-old", "who": {"identity": {"sessionName": "builder@forge"}, "contextUsage": {"fresh": True, "usedPercentage": 86}}}
        with patch.object(c.ev, "snapshot", return_value=snap), patch.object(c, "metrics", return_value=(86, 0, None)), \
             patch.object(c, "receipt_ok", return_value=True), patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c, "observe", return_value={"observations": 2}), \
             patch.object(c, "actor", side_effect=RuntimeError("not current Operator")), \
             patch.object(c, "stage", side_effect=AssertionError("must not stage without actor")):
            with self.assertRaisesRegex(RuntimeError, "not current Operator"):
                c.tick_seat("builder@forge")

    def test_checkpoint_request_requires_real_operator_and_durable_task(self):
        self.update_policy(controller_enabled=True, operator_auto_execute_enabled=True)
        snap = {"generation": "native-old", "who": {"identity": {"sessionName": "builder@forge"}, "contextUsage": {"fresh": True, "usedPercentage": 76}}}
        c.write(self.root / "state.json", {"builder@forge": {"baselineGeneration": "native-old",
          "baselineCompactions": 0, "phase": "monitoring"}})
        with patch.object(c.ev, "snapshot", return_value=snap), \
             patch.object(c, "metrics", return_value=(76, 0, None)), \
             patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c, "receipt_ok", return_value=False), \
             patch.object(c, "actor", side_effect=RuntimeError("no managed Operator")), \
             patch.object(c, "publish_task", side_effect=AssertionError("no unauthenticated task")):
            with self.assertRaisesRegex(RuntimeError, "no managed Operator"):
                c.tick_seat("builder@forge")
        self.assertEqual(c.read(self.root / "state.json")["builder@forge"]["phase"], "monitoring")
        with patch.object(c.ev, "snapshot", return_value=snap), \
             patch.object(c, "metrics", return_value=(76, 0, None)), \
             patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c, "receipt_ok", return_value=False), \
             patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "publish_task", return_value={"qitemId": "durable-prep"}) as publish:
            self.assertEqual(c.tick_seat("builder@forge")["qitemId"], "durable-prep")
        self.assertEqual(publish.call_args.args[2], "builder@forge")
        self.assertEqual(c.read(self.root / "state.json")["builder@forge"]["phase"], "checkpoint_requested")

    def test_operator_auto_path_orders_reserve_before_guarded_handover(self):
        self.update_policy(controller_enabled=True, automatic_cutover_enabled=True,
                           automatic_trigger_enabled=True, operator_auto_execute_enabled=True)
        c.write(self.root / "state.json", {"builder@forge": {"baselineGeneration": "native-old",
                                                    "baselineCompactions": 0, "phase": "checkpoint_requested"}})
        snap = {"generation": "native-old", "who": {"identity": {"sessionName": "builder@forge"}, "contextUsage": {"fresh": True, "usedPercentage": 86}}}
        ordered = []
        with patch.object(c.ev, "snapshot", return_value=snap), patch.object(c, "metrics", return_value=(86, 0, None)), \
             patch.object(c, "receipt_ok", return_value=True), patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c, "observe", return_value={"observations": 2}), \
             patch.object(c, "actor", side_effect=lambda *_: ordered.append("actor")), \
             patch.object(c, "stage", side_effect=lambda *_: ordered.append("stage") or {"attempt": "qa-test"}), \
             patch.object(c, "operator_reserve", side_effect=lambda *_: ordered.append("reserve")), \
             patch.object(c, "operator_handover", side_effect=lambda *_: ordered.append("handover") or {"state": "committed"}):
            self.assertEqual(c.tick_seat("builder@forge"), {"state": "committed"})
        self.assertEqual(ordered, ["actor", "stage", "reserve", "handover"])

    def test_wrong_control_plane_and_scope_refuse(self):
        self.update_policy(daemon_url="http://127.0.0.1:17533")
        with self.assertRaisesRegex(RuntimeError, "production daemon"):
            c.policy()
        self.update_policy(daemon_url="http://127.0.0.1:17433", managed_unattended_seats=["other@forge"])
        with self.assertRaisesRegex(RuntimeError, "malformed explicit fleet"):
            c.policy()

    def test_expected_queue_binds_local_alias_and_excludes_foreign_address(self):
        db = sqlite3.connect(self.dbpath)
        for qid, dest in (("canonical", "builder@forge"), ("local", "builder@forge@fixture-host"),
                          ("foreign", "builder@forge@elsewhere")):
            db.execute("INSERT INTO queue_items(qitem_id,destination_session,state,claimed_at,ts_updated,claimed_by_generation_uuid,body) VALUES(?,?,?,?,?,?,?)", (qid, dest, "pending", None, "now", None, "valuable work"))
        db.commit(); db.close()
        rows = c.ev.queue("builder@forge")
        self.assertEqual([x["id"] for x in rows], ["canonical", "local"])
        self.assertEqual(rows[1]["destinationSession"], "builder@forge@fixture-host")
        self.assertEqual(rows[1]["bodyHash"], hashlib.sha256(b"valuable work").hexdigest())
        self.assertEqual(list(rows[0]), ["id", "destinationSession", "state", "claimedAt", "updated", "claimGeneration", "bodyHash"])
        changed = [dict(rows[0], destinationSession="builder@forge@fixture-host"), rows[1]]
        self.assertFalse(c.ev.queue_continuity(rows, changed, "old", "new"))
        db = sqlite3.connect(self.dbpath)
        db.execute("DELETE FROM self_host_identity")
        db.commit(); db.close()
        with self.assertRaisesRegex(RuntimeError, "local host identity"):
            c.ev.queue("builder@forge")

    def test_manual_trigger_is_disabled_by_default(self):
        with self.assertRaisesRegex(RuntimeError, "gate disabled"):
            c.stage("builder@forge", manual=True)
        self.update_policy(controller_enabled=True, automatic_cutover_enabled=True)
        with self.assertRaisesRegex(RuntimeError, "manual canary disabled"):
            c.stage("builder@forge", manual=True)

    def test_actor_requires_current_managed_generation(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "environment required"):
                c.actor(c.OPERATOR)
        db = sqlite3.connect(self.dbpath)
        db.execute("INSERT INTO nodes VALUES('operator','kernel','other')")
        db.execute("INSERT INTO bindings VALUES('operator',?)", (c.OPERATOR,))
        db.execute("INSERT INTO occupant_tenures VALUES('operator',1,'actual-gen')")
        db.execute("INSERT INTO sessions VALUES('current','operator','running','ready')")
        db.commit(); db.close()
        who = {"identity": {"sessionName": c.OPERATOR}}
        with patch.dict("os.environ", {"OPENRIG_SESSION_NAME": c.OPERATOR, "OPENRIG_OCCUPANT_GENERATION": "stale-gen"}), patch.object(c.ev, "call", return_value=who):
            with self.assertRaisesRegex(RuntimeError, "current ready generation"):
                c.actor(c.OPERATOR)
        with patch.dict("os.environ", {"OPENRIG_SESSION_NAME": c.OPERATOR, "OPENRIG_OCCUPANT_GENERATION": "actual-gen"}), patch.object(c.ev, "call", return_value=who):
            self.assertEqual(c.actor(c.OPERATOR), (c.OPERATOR, "actual-gen"))

    def test_stage_freezes_exact_api_shape_without_sending_mutation(self):
        self.update_policy(controller_enabled=True, automatic_cutover_enabled=True, allow_manual_canary=True)
        snap = {"generation": "native-old", "who": {"identity": {"sessionName": "builder@forge"}, "contextUsage": {"fresh": True, "usedPercentage": 50, "transcriptPath": "native"}}, "status": {}}
        receipt = {"generation": "native-old", "at": 1000, "queue_hash": c.ev.digest([]),
                   "quiescent": True, "unattended_eligible": True, "snapshot": {"who": {"identity": {"sessionName": "builder@forge"}}},
                   "packet": {key: [] for key in c.REQUIRED}, "runtime_contract": {"runtime": "codex"}}
        c.write(self.root / "receipts/builder@forge.json", receipt)
        c.write(self.root / "state.json", {"builder@forge": {"idle": {"key": ["native-old", c.sha(self.root / "receipts/builder@forge.json"), "occupant-old"], "at": 1000, "observations": 2}}})
        with patch.object(c.ev, "snapshot", return_value=snap), patch.object(c, "metrics", return_value=(50, 0, "model")), \
             patch.object(c, "positive_idle", return_value={"model": "model"}), patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c.ev, "runtime_contract", return_value={"runtime": "codex"}), \
             patch.object(c, "profile_sha", return_value="a" * 64), patch.object(c.time, "time", return_value=1002), \
             patch.object(c, "run_cli", side_effect=AssertionError("stage must not mutate daemon")):
            result = c.stage("builder@forge", manual=True)
        req = c.read(result["contract"])
        self.assertEqual(req["nodeId"], "qa")
        self.assertEqual(req["generation"], "occupant-old")
        self.assertEqual(req["expected"]["protocol"], c.PROTOCOL)
        self.assertEqual(req["expected"]["checkpointHash"], c.sha(self.root / req["expected"]["checkpointPath"]))
        self.assertEqual(result["nextActor"], c.OPERATOR)
        with self.assertRaisesRegex(RuntimeError, "pending attempt"):
            c.stage("builder@forge", manual=True)

    def test_stage_rejects_stale_idle_and_does_not_fabricate_witness(self):
        self.update_policy(controller_enabled=True, automatic_cutover_enabled=True, allow_manual_canary=True)
        snap = {"generation": "native-old", "who": {"identity": {"sessionName": "builder@forge"}, "contextUsage": {"fresh": True, "usedPercentage": 90}}}
        receipt = {"generation": "native-old", "at": 1000, "queue_hash": c.ev.digest([]),
                   "quiescent": True, "unattended_eligible": True, "snapshot": {"who": {"identity": {"sessionName": "builder@forge"}}}, "packet": {key: [] for key in c.REQUIRED}}
        c.write(self.root / "receipts/builder@forge.json", receipt)
        with patch.object(c.ev, "snapshot", return_value=snap), patch.object(c, "metrics", return_value=(90, 0, None)), \
             patch.object(c, "positive_idle", return_value={}), patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c.time, "time", return_value=1002):
            with self.assertRaisesRegex(RuntimeError, "two positive idle polls"):
                c.stage("builder@forge", manual=True)

    def test_handover_uncertain_crash_never_retries_even_if_db_still_reserved(self):
        self.request(); self.insert_reservation()
        c.write(self.root / "state.json", {"builder@forge": {"phase": "handover_uncertain", "attempt": "qa-test", "request_sha256": c.sha(self.root / "contracts/qa-test.json")}})
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "handover_request", side_effect=AssertionError("no retry")):
            with self.assertRaisesRegex(RuntimeError, "prior handover outcome uncertain"):
                c.operator_handover("qa-test")
        self.assertEqual(c.reconcile("qa-test")["durableState"], "reserved")

    def test_handover_failure_retains_uncertain_state_and_fence(self):
        self.staged(); self.insert_reservation()
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "handover_request", side_effect=TimeoutError("network timeout")):
            with self.assertRaises(TimeoutError): c.operator_handover("qa-test")
        self.assertEqual(c.read(self.root / "state.json")["builder@forge"]["phase"], "handover_uncertain")
        self.assertEqual(c.reconcile("qa-test")["durableState"], "reserved")

    def test_guarded_handover_uses_exact_api_and_actual_managed_headers(self):
        req = self.request()
        calls = []
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self, *_): return b'{"ok":true}'
        def open_request(request, timeout):
            calls.append((request, timeout))
            return Response()
        with patch.dict("os.environ", {"OPENRIG_TERMINAL_BEARER_TOKEN": "private-test-token",
                                       "OPENRIG_SESSION_NAME": c.OPERATOR,
                                       "OPENRIG_OCCUPANT_GENERATION": "operator-current"}), \
             patch.object(c.urllib.request, "urlopen", side_effect=open_request):
            self.assertEqual(c.handover_request(req, c.policy()), {"ok": True})
        request, timeout = calls[0]
        self.assertEqual(timeout, 240)
        self.assertEqual(request.full_url, "http://127.0.0.1:17433/api/seat/handover/builder%40forge")
        self.assertEqual(json.loads(request.data)["rotationExpected"], req["expected"])
        self.assertEqual(request.get_header("X-openrig-session"), c.OPERATOR)
        self.assertEqual(request.get_header("X-openrig-occupant-generation"), "operator-current")
        self.assertNotIn("private-test-token", json.dumps(req))

    def test_started_crash_does_not_release_or_retry(self):
        self.request(); self.insert_reservation("started")
        self.assertEqual(c.reconcile("qa-test")["phase"], "replacement_started_uncertain")
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")):
            with self.assertRaisesRegex(RuntimeError, "exact durable reserved"):
                c.operator_handover("qa-test")

    def test_no_successor_self_validation_or_auto_release(self):
        self.request(); self.insert_reservation("committed")
        with patch.object(c, "actor", return_value=("builder@forge", "occupant-new")):
            with self.assertRaisesRegex(RuntimeError, "distinct current validator"):
                c.validator_attest("qa-test", "unused")
        self.task("operator-return", c.OPERATOR, "review@forge", "check-gen", "operator-gen")
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "run_cli", side_effect=AssertionError("must not release")):
            with self.assertRaisesRegex(RuntimeError, "two durable actor receipts"):
                c.operator_release("qa-test")

    def test_actual_successor_ack_uses_exact_checkpoint_and_native_proof(self):
        req = self.request(); self.insert_reservation("committed")
        req["expected"].update({"checkpointPath": "frozen/qa-test.json", "runtimeContract": {"model": "sol"}, "queue": []})
        c.write(self.root / "contracts/qa-test.json", req)
        state = c.read(self.root / "state.json")
        state["builder@forge"]["request_sha256"] = c.sha(self.root / "contracts/qa-test.json")
        c.write(self.root / "state.json", state)
        c.write(self.root / "frozen/qa-test.json", {"generation": "native-old"})
        evidence = self.root / "successor.json"
        c.write(evidence, {"reservationId": "qa-test", "checkpointHash": "b" * 64, "verdict": "acknowledged"})
        snap = {"generation": "native-new"}
        with patch.object(c, "actor", return_value=("builder@forge", "occupant-new")), \
             patch.object(c.ev, "snapshot", return_value=snap), \
             patch.object(c.ev, "runtime_contract", return_value={"model": "sol"}), \
             patch.object(c.ev, "queue", return_value=[]), \
             patch.object(c.ev, "queue_continuity", return_value=True), \
             patch.object(c, "run_cli", return_value={"state": "committed"}) as cli:
            self.assertEqual(c.successor_ack("qa-test", evidence), {"state": "committed"})
        self.assertEqual(cli.call_args.args[0], "attest")
        self.assertEqual(cli.call_args.args[1]["kind"], "successor_ack")
        self.assertEqual(cli.call_args.args[1]["checkpointHash"], "b" * 64)

    def test_actual_distinct_validator_must_author_matching_pass(self):
        self.request(); self.insert_reservation("committed")
        self.task("validator", "review@forge", c.OPERATOR, "operator-gen", "check-gen")
        db = sqlite3.connect(self.dbpath)
        db.execute("INSERT INTO seat_dispatch_reservation_audit VALUES(?,?,?,?,?,?,?)",
                   (1, "qa-test", "successor_ack", "builder@forge", "occupant-new", "{}", "now"))
        db.commit(); db.close()
        evidence = self.root / "validator.json"
        c.write(evidence, {"reservationId": "qa-test", "checkpointHash": "b" * 64,
                           "verdict": "pass", "validatorSession": "review@forge", "validatorGeneration": "check-gen"})
        with patch.object(c, "actor", return_value=("review@forge", "check-gen")), \
             patch.object(c, "run_cli", return_value={"state": "committed"}) as cli, \
             patch.object(c, "validator_return", return_value={"qitemId": "return"}):
            self.assertEqual(c.validator_attest("qa-test", evidence)["operatorTask"], "return")
        self.assertEqual(cli.call_args.args[1]["kind"], "independent_acceptance")
        c.write(evidence, {"reservationId": "qa-test", "checkpointHash": "b" * 64,
                           "verdict": "pass", "validatorSession": "review@forge", "validatorGeneration": "stale"})
        with patch.object(c, "actor", return_value=("review@forge", "check-gen")):
            with self.assertRaisesRegex(RuntimeError, "validator-authored"):
                c.validator_attest("qa-test", evidence)

    def test_real_receipt_actions_then_operator_release_not_controller_autoapproval(self):
        self.request(); self.insert_reservation("committed")
        self.task("operator-return", c.OPERATOR, "review@forge", "check-gen", "operator-current")
        db = sqlite3.connect(self.dbpath)
        for index, (kind, actor, generation) in enumerate((("successor_ack", "builder@forge", "occupant-new"),
                                                            ("independent_acceptance", "validator@forge", "validator-current")), 1):
            db.execute("INSERT INTO seat_dispatch_reservation_audit VALUES(?,?,?,?,?,?,?)",
                       (index, "qa-test", kind, actor, generation, "{}", "now"))
        db.commit(); db.close()
        self.assertEqual(c.reconcile("qa-test")["phase"], "awaiting_operator_release")
        def release(_verb, body, _attempt):
            self.assertEqual(body["mode"], "accepted_successor")
            db = sqlite3.connect(self.dbpath)
            db.execute("UPDATE seat_dispatch_reservations SET state='released' WHERE reservation_id='qa-test'")
            db.commit(); db.close()
            return {"state": "released"}
        c.write(self.root / "state.json", {"builder@forge": {"attempt": "qa-test", "request_sha256": c.sha(self.root / "contracts/qa-test.json"), "phase": "awaiting_actor_receipts"}})
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-current")), patch.object(c, "run_cli", side_effect=release):
            result = c.operator_release("qa-test")
        self.assertEqual(result["state"], "released")
        state = c.read(self.root / "state.json")["builder@forge"]
        self.assertNotIn("attempt", state)
        self.assertEqual(state["completed"], ["qa-test"])

    def test_disable_after_stage_refuses_reserve_without_daemon_write(self):
        self.staged()
        self.update_policy(automatic_cutover_enabled=False)
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "run_cli", side_effect=AssertionError("no reserve")):
            with self.assertRaisesRegex(RuntimeError, "disabled"):
                c.operator_reserve("qa-test")
        with self.db() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM seat_dispatch_reservations").fetchone()[0], 0)

    def test_disable_after_reserve_refuses_handover_and_keeps_recoverable_fence(self):
        self.staged(); self.insert_reservation()
        self.update_policy(controller_enabled=False)
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "handover_request", side_effect=AssertionError("no handover")):
            with self.assertRaisesRegex(RuntimeError, "disabled"):
                c.operator_handover("qa-test")
        self.assertEqual(c.read(self.root / "state.json")["builder@forge"]["phase"], "reservation_pending")
        self.assertEqual(c.reconcile("qa-test")["durableState"], "reserved")

    def test_handover_commit_routes_validator_task_without_repeating_cutover(self):
        self.staged(); self.insert_reservation()
        def committed(_request, _policy):
            with self.db() as db:
                db.execute("UPDATE seat_dispatch_reservations SET state='committed',successor_generation='occupant-new' WHERE reservation_id='qa-test'")
            return {"ok": True}
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "handover_request", side_effect=committed) as handover, \
             patch.object(c, "operator_route_validator", return_value={"qitemId": "durable-validator"}) as route:
            result = c.operator_handover("qa-test")
        self.assertEqual(result["validatorTask"], "durable-validator")
        self.assertEqual(handover.call_count, 1)
        route.assert_called_once_with("qa-test")
        self.assertEqual(c.read(self.root / "state.json")["builder@forge"]["phase"], "awaiting_successor_ack")
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "handover_request", side_effect=AssertionError("no second handover")):
            with self.assertRaisesRegex(RuntimeError, "exact durable reserved"):
                c.operator_handover("qa-test")

    def test_allowlist_or_config_or_frozen_request_change_refuses_mutation(self):
        self.staged()
        self.update_policy(managed_unattended_seats=["other@forge"])
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "run_cli", side_effect=AssertionError("no reserve")):
            with self.assertRaisesRegex(RuntimeError, "malformed explicit fleet"):
                c.operator_reserve("qa-test")
        self.update_policy(managed_unattended_seats=["builder@forge"], rotate_percent=90)
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "run_cli", side_effect=AssertionError("no reserve")):
            with self.assertRaisesRegex(RuntimeError, "policy changed"):
                c.operator_reserve("qa-test")
        self.staged()
        request = c.read(self.root / "contracts/qa-test.json"); request["reason"] = "changed after stage"
        c.write(self.root / "contracts/qa-test.json", request)
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c, "run_cli", side_effect=AssertionError("no reserve")):
            with self.assertRaisesRegex(RuntimeError, "frozen attempt contract changed"):
                c.operator_reserve("qa-test")

    def test_postcommit_release_remains_possible_after_controller_disabled(self):
        self.request(); self.insert_reservation("committed")
        self.task("operator-return", c.OPERATOR, "review@forge", "check-gen", "operator-current")
        db = sqlite3.connect(self.dbpath)
        for index, (kind, seat, generation) in enumerate((("successor_ack", "builder@forge", "occupant-new"),
                                                          ("independent_acceptance", "review@forge", "check-gen")), 1):
            db.execute("INSERT INTO seat_dispatch_reservation_audit VALUES(?,?,?,?,?,?,?)",
                       (index, "qa-test", kind, seat, generation, "{}", "now"))
        db.commit(); db.close()
        self.update_policy(controller_enabled=False, automatic_cutover_enabled=False)
        c.write(self.root / "state.json", {"builder@forge": {"attempt": "qa-test", "request_sha256": c.sha(self.root / "contracts/qa-test.json")}})
        def release(_verb, _body, _attempt):
            with self.db() as db:
                db.execute("UPDATE seat_dispatch_reservations SET state='released' WHERE reservation_id='qa-test'")
            return {"state": "released"}
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-current")), \
             patch.object(c, "run_cli", side_effect=release):
            self.assertEqual(c.operator_release("qa-test")["state"], "released")

    def test_actual_validator_claim_required_even_with_pass_artifact(self):
        self.request(); self.insert_reservation("committed")
        self.task("validator", "review@forge", c.OPERATOR, "operator-gen", None)
        with patch.object(c, "actor", return_value=("review@forge", "check-gen")), \
             patch.object(c, "run_cli", side_effect=AssertionError("no attestation")):
            with self.assertRaisesRegex(RuntimeError, "has not claimed"):
                c.validator_attest("qa-test", self.root / "unused")

    def test_validator_task_create_is_idempotent_and_transport_attributed(self):
        self.request(); self.insert_reservation("committed")
        created = []
        def cli(argv, **_kwargs):
            created.append(argv)
            qid = argv[argv.index("--id")+1]
            body = pathlib.Path(argv[argv.index("--body-file")+1]).read_text()
            with self.db() as db:
                db.execute("INSERT INTO queue_items VALUES(?,?,?,?,?,?,?,?,?)",
                           (qid, "review@forge", "pending", None, "now", None, body, c.OPERATOR, "operator-gen"))
                db.execute("INSERT INTO queue_transitions(qitem_id,transition_note,identity_provenance) VALUES(?, 'created','transport:v1')", (qid,))
            return type("Result", (), {"returncode": 0, "stdout": "{}", "stderr": ""})()
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), \
             patch.object(c.subprocess, "run", side_effect=cli):
            first = c.operator_route_validator("qa-test")
            second = c.operator_route_validator("qa-test")
        self.assertEqual(first["qitemId"], second["qitemId"])
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0][1:3], ["queue", "create"])
        self.assertIn("--destination", created[0])
        self.assertEqual(created[0][created[0].index("--destination")+1], "review@forge")

    def test_spoofed_task_provenance_cannot_be_treated_as_actor_pickup(self):
        self.request(); self.insert_reservation("committed")
        qid = self.task("validator", "review@forge", c.OPERATOR, "operator-gen", "check-gen")
        with self.db() as db:
            db.execute("UPDATE queue_transitions SET identity_provenance='claimed:v1' WHERE qitem_id=?", (qid,))
        with self.assertRaisesRegex(RuntimeError, "has not claimed"):
            c.require_claim("qa-test", "validator", "review@forge", "check-gen")

    def test_changed_task_body_or_stale_claim_generation_refuses_pickup(self):
        self.task("validator", "review@forge", c.OPERATOR, "operator-gen", "check-gen")
        with self.db() as db:
            db.execute("UPDATE queue_items SET body='different' WHERE qitem_id=?", (c.task_id("qa-test", "validator"),))
        with self.assertRaisesRegex(RuntimeError, "has not claimed"):
            c.require_claim("qa-test", "validator", "review@forge", "check-gen")
        with self.db() as db:
            db.execute("UPDATE queue_items SET body='task' WHERE qitem_id=?", (c.task_id("qa-test", "validator"),))
        with self.assertRaisesRegex(RuntimeError, "has not claimed"):
            c.require_claim("qa-test", "validator", "review@forge", "new-generation")

    def add_second_seat(self):
        with self.db() as db:
            db.execute("INSERT INTO rigs VALUES('health','health',NULL)")
            db.execute("INSERT INTO nodes VALUES('second','health','health-native-profile')")
            db.execute("INSERT INTO bindings VALUES('second','writer@health')")
            db.execute("INSERT INTO occupant_tenures VALUES('second',1,'second-generation')")
        self.update_policy(rigs=["forge", "health"], managed_unattended_seats=["builder@forge", "writer@health"],
                           seat_policies={"builder@forge": {"validator_seat": "review@forge"}, "writer@health": {"validator_seat": "independent@health", "rotate_percent": 90}})

    def test_two_seats_have_separate_policy_and_tick_state(self):
        self.add_second_seat()
        self.assertEqual(c.seat_policy("writer@health")["rotate_percent"], 90)
        self.assertEqual(c.seat_policy("builder@forge")["rotate_percent"], 85)
        self.update_policy(controller_enabled=True)
        def snapshot(seat):
            return {"generation": seat + "-native", "who": {"identity": {"sessionName": seat}}}
        with patch.object(c.ev, "snapshot", side_effect=snapshot), patch.object(c, "metrics", return_value=(25, 3, None)):
            result = c.tick()
        self.assertEqual(set(result["seats"]), {"builder@forge", "writer@health"})
        state = c.read(self.root / "state.json")
        self.assertNotEqual(state["builder@forge"]["baselineGeneration"], state["writer@health"]["baselineGeneration"])

    def test_attempt_cannot_move_or_share_seat_state(self):
        self.add_second_seat()
        self.request()
        state = c.read(self.root / "state.json")
        state["writer@health"] = dict(state["builder@forge"])
        c.write(self.root / "state.json", state)
        with self.assertRaisesRegex(RuntimeError, "exclusively bound"):
            c.load_attempt("qa-test")
        del state["builder@forge"]
        c.write(self.root / "state.json", state)
        with self.assertRaisesRegex(RuntimeError, "exclusively bound"):
            c.load_attempt("qa-test")

    def test_checkpoint_from_other_seat_refuses_even_with_same_generation(self):
        snap = {"generation": "shared", "who": {"identity": {"sessionName": "builder@forge"}}}
        receipt = {"generation": "shared", "at": 1000, "queue_hash": c.ev.digest([]), "quiescent": True,
                   "unattended_eligible": True, "packet": {key: [] for key in c.REQUIRED},
                   "snapshot": {"who": {"identity": {"sessionName": "writer@health"}}}}
        self.assertFalse(c.receipt_ok(receipt, snap, [], 1001))

    def test_malformed_policy_refuses_before_sampling_or_mutation(self):
        for changes in ({"poll_seconds": 0}, {"compactions": True}, {"rotate_percent": float('nan')},
                        {"controller_enabled": 1}, {"managed_unattended_seats": ["builder@forge", "builder@forge"]},
                        {"seat_policies": {"builder@forge": {"validator_seat": "builder@forge"}}},
                        {"seat_policies": {}}, {"rigs": ["other"]}):
            original = dict(self.policy)
            self.update_policy(**changes)
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                c.policy()
            self.policy = original
            c.write(self.root / "policy.json", original)

    def test_reserve_timeout_is_not_retried_without_durable_row(self):
        self.staged()
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), patch.object(c, "run_cli", side_effect=TimeoutError("uncertain reserve")) as call:
            with self.assertRaises(TimeoutError): c.operator_reserve("qa-test")
            with self.assertRaisesRegex(RuntimeError, "prior reserve outcome uncertain"): c.operator_reserve("qa-test")
        self.assertEqual(call.call_count, 1)

    def test_queue_timeout_is_not_retried_without_durable_row(self):
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), patch.object(c.subprocess, "run", side_effect=TimeoutError("uncertain queue")) as call:
            with self.assertRaises(TimeoutError): c.publish_task("test", "return", c.OPERATOR, "body", "summary")
            with self.assertRaisesRegex(RuntimeError, "prior queue create outcome uncertain"): c.publish_task("test", "return", c.OPERATOR, "body", "summary")
        self.assertEqual(call.call_count, 1)

    def test_one_seat_hold_does_not_block_other_ready_observation(self):
        self.add_second_seat()
        self.update_policy(controller_enabled=True)
        def step(seat):
            if seat == "builder@forge": raise RuntimeError("uncertain reserved attempt")
            return {"phase": "monitoring"}
        with patch.object(c, "tick_seat", side_effect=step):
            result = c.tick()["seats"]
        self.assertEqual(result["builder@forge"]["phase"], "hold")
        self.assertEqual(result["writer@health"]["phase"], "monitoring")

    def enroll(self):
        with self.db() as db:
            db.executescript("CREATE TABLE coordinator_authority(rig_id TEXT,owner_session TEXT,owner_generation TEXT,epoch INT,state TEXT,lease_until INT,coordinators TEXT,baton_id TEXT); CREATE TABLE coordinator_packages(rig_id TEXT,package_key TEXT,contract TEXT);")
            db.execute("INSERT INTO coordinator_authority VALUES(?,?,?,?,?,?,?,?)", ("forge", "lead@forge", "lead-current", 7, "active", int(time.time()*1000)+60000, json.dumps(["lead@forge", "peer@forge"]), "canonical-baton"))
            db.execute("INSERT INTO queue_items VALUES(?,?,?,?,?,?,?,?,?)", ("canonical-baton","lead@forge","in-progress","now","now","lead-current","canonical long-lived baton",c.OPERATOR,"operator-gen"))

    def test_enrolled_operator_admits_then_routes_and_current_owner_dispatches(self):
        self.enroll()
        calls = []
        def cli(argv, **kwargs):
            calls.append(argv)
            if argv[1:3] == ["coordinator", "admit"]:
                admission = c.read(argv[3])
                with self.db() as db:
                    db.execute("INSERT INTO coordinator_packages VALUES(?,?,?)", (admission["rigId"], admission["packageKey"], json.dumps(admission["contract"])))
            else:
                qid = argv[argv.index("--id")+1]
                dest = argv[argv.index("--destination")+1]
                body = pathlib.Path(argv[argv.index("--body-file")+1]).read_text()
                sender, gen = (c.OPERATOR, "operator-gen") if dest == "lead@forge" else ("lead@forge", "lead-current")
                with self.db() as db:
                    db.execute("INSERT INTO queue_items VALUES(?,?,?,?,?,?,?,?,?)", (qid,dest,"pending",None,"now",None,body,sender,gen))
                    db.execute("INSERT INTO queue_transitions(qitem_id,transition_note,identity_provenance) VALUES(?,'created','transport:v1')", (qid,))
            return type("Result", (), {"returncode":0,"stdout":"{}","stderr":""})()
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), patch.object(c.subprocess, "run", side_effect=cli):
            routed = c.publish_task("attempt", "checkpoint", "builder@forge", "exact checkpoint task", "summary")
        self.assertEqual(calls[0][1:3], ["coordinator", "admit"])
        self.assertEqual(calls[1][calls[1].index("--destination")+1], "lead@forge")
        self.assertNotIn("--dispatch-file", calls[1])
        with patch.object(c, "actor", return_value=("lead@forge", "lead-current")), patch.object(c.subprocess, "run", side_effect=cli):
            dispatched = c.coordinator_dispatch(routed["route"])
        self.assertEqual(dispatched["qitemId"], routed["workerQitemId"])
        self.assertIn("--dispatch-file", calls[2])
        envelope = c.read(calls[2][calls[2].index("--dispatch-file")+1])
        self.assertEqual(envelope["token"], {"rigId":"forge","epoch":7,"generation":"lead-current"})
        with patch.object(c, "actor", return_value=("peer@forge", "peer-current")), patch.object(c.subprocess, "run", side_effect=AssertionError("no stale owner dispatch")):
            with self.assertRaisesRegex(RuntimeError, "actual current coordinator"): c.coordinator_dispatch(routed["route"])

    def test_enrolled_recovery_holds_before_admission(self):
        self.enroll()
        with self.db() as db: db.execute("UPDATE coordinator_authority SET state='recovery'")
        with patch.object(c, "actor", return_value=(c.OPERATOR, "operator-gen")), patch.object(c.subprocess, "run", side_effect=AssertionError("no admission in recovery")):
            with self.assertRaisesRegex(RuntimeError, "current active coordinator"):
                c.publish_task("attempt", "checkpoint", "builder@forge", "body", "summary")

    def test_worker_disposition_requires_terminal_claim_and_native_durable_release(self):
        self.enroll()
        qid = c.task_id("attempt", "checkpoint")
        body = "terminal checkpoint work"
        contract = {"destination":"builder@forge", "bodyHash":hashlib.sha256(body.encode()).hexdigest(),
                    "inputDigest":"input-digest", "resources":["context-refresh:builder@forge"],
                    "returnContract":{"destination":c.OPERATOR,"evidenceRequired":["authored-context-refresh-evidence"]}}
        admission = {"rigId":"forge","packageKey":qid,"contract":contract}
        route = self.root / "routes" / (qid + ".json")
        c.write(route, {"phase":"admitted","admission":admission})
        artifact = self.root / "authored.json"
        c.write(artifact,{"checkpoint":"authored"})
        old_qid = self.task("checkpoint", "builder@forge", "lead@forge", "lead-current", "occupant-old", body)
        with self.db() as db:
            db.execute("UPDATE queue_items SET qitem_id=? WHERE qitem_id=?", (qid,old_qid))
            db.execute("UPDATE queue_transitions SET qitem_id=? WHERE qitem_id=?", (qid,old_qid))
        with self.db() as db:
            db.executescript("CREATE TABLE coordinator_assignments(rig_id TEXT,package_key TEXT,disposition_id TEXT);")
            db.execute("INSERT INTO coordinator_packages VALUES(?,?,?)", ("forge",qid,json.dumps(contract)))
            db.execute("INSERT INTO coordinator_assignments VALUES(?,?,NULL)", ("forge",qid))
        with patch.object(c, "actor", return_value=("builder@forge","occupant-old")), patch.object(c.subprocess,"run",side_effect=AssertionError("no disposition while active")):
            with self.assertRaisesRegex(RuntimeError,"exact terminal admitted"):
                c.worker_disposition(route,artifact)
        with self.db() as db: db.execute("UPDATE queue_items SET state='done' WHERE qitem_id=?", (qid,))
        def dispose(argv, **kwargs):
            self.assertEqual(argv[1:3],["coordinator","dispose"])
            packet = c.read(argv[3])
            with self.db() as db: db.execute("UPDATE coordinator_assignments SET disposition_id=?", (packet["dispositionId"],))
            return type("Result",(),{"returncode":0,"stdout":"{}","stderr":""})()
        with patch.object(c,"actor",return_value=("builder@forge","occupant-old")), patch.object(c,"create_task",return_value={"qitemId":qid+"-disposition-return"}) as returned, patch.object(c.subprocess,"run",side_effect=dispose):
            result=c.worker_disposition(route,artifact)
        self.assertFalse(result["rotationAcceptance"])
        payload=json.loads(returned.call_args.args[2])
        self.assertEqual(payload["packageKey"],qid)
        self.assertEqual(payload["inputDigest"],"input-digest")

    def plan_fixture(self):
        self.enroll()
        now = int(time.time()*1000)
        admission = {"generation":"occupant-old","configurationDigest":"exact-owner-supplied-config",
                     "qualificationRef":"actual-qualified-evidence","capacityRef":"actual-capacity-evidence",
                     "effortRef":"actual-effort-evidence","validUntil":now+60000}
        prior = {"rigId":"forge","revision":"original","operatorGeneration":"operator-gen","stallMs":30000,"allowIdlePeerTransfer":False,
                 "tasks":[{"key":"existing-task","packageKey":"existing-package","owner":"builder@forge","action":"Retain existing action",
                           "deadline":now+30000,"body":"Existing immutable body","predecessors":[],"boundary":"owner-material","admission":admission}]}
        qid = c.task_id("attempt","checkpoint")
        body_path = self.root / "tasks" / (qid+".txt")
        c.write_text(body_path,"Exact refresh packet")
        contract = {"destination":"builder@forge","inputDigest":"frozen-input","bodyHash":hashlib.sha256(body_path.read_bytes()).hexdigest(),
                    "resources":["context-refresh:builder@forge"],"returnContract":{"destination":c.OPERATOR,"evidenceRequired":["authored-context-refresh-evidence"]}}
        route = {"admission":{"rigId":"forge","packageKey":qid,"contract":contract},"bodyPath":str(body_path)}
        route_path = self.root / "routes" / (qid+".json")
        c.write(route_path,route)
        with self.db() as db:
            db.executescript("CREATE TABLE coordinator_operations(rig_id TEXT,operation_id TEXT,kind TEXT,receipt TEXT,request_hash TEXT);")
            db.execute("INSERT INTO coordinator_operations VALUES(?,?,?,?,?)", ("forge","coordination-plan:original","coordination-plan",json.dumps(prior),"hash"))
            db.execute("INSERT INTO coordinator_packages VALUES(?,?,?)", ("forge",qid,json.dumps(contract)))
        settings = {"recovery_owner":"review@forge","deadline_seconds":120,
                    "admissions":{"builder@forge":admission,"review@forge":dict(admission,generation="check-gen")}}
        return prior,route,route_path,settings

    def test_plan_append_preserves_prior_contract_and_distinct_recovery(self):
        prior,route,path,settings=self.plan_fixture()
        posts=[]
        def cli(argv, **kwargs):
            self.assertEqual(argv[1:3],["coordinator","admit"])
            admission=c.read(argv[3])
            with self.db() as db: db.execute("INSERT INTO coordinator_packages VALUES(?,?,?)", (admission["rigId"],admission["packageKey"],json.dumps(admission["contract"])))
            return type("Result",(),{"returncode":0,"stdout":"{}","stderr":""})()
        def post(operation,plan):
            posts.append((operation,plan))
            with self.db() as db: db.execute("INSERT INTO coordinator_operations VALUES(?,?,?,?,?)", ("forge","coordination-plan:"+plan["revision"],"coordination-plan",json.dumps(plan),"hash"))
            return plan
        with patch.object(c,"actor",return_value=(c.OPERATOR,"operator-gen")), patch.object(c.subprocess,"run",side_effect=cli), patch.object(c,"authenticated_post",side_effect=post):
            result=c.append_coordination_plan(route,path,settings)
        self.assertEqual(posts[0][0],"coordination-plan")
        self.assertEqual(result["tasks"][0],prior["tasks"][0])
        self.assertEqual(len(result["tasks"]),3)
        ordinary,recovery=result["tasks"][1:]
        self.assertNotEqual(ordinary["packageKey"],recovery["packageKey"])
        self.assertEqual(recovery["recoveryFor"],ordinary["key"])
        self.assertEqual(ordinary["admission"],settings["admissions"]["builder@forge"])
        self.assertIn("Never retry a started or uncertain refresh",recovery["body"])

    def test_uncertain_plan_configuration_never_retries_post(self):
        prior,route,path,settings=self.plan_fixture()
        def cli(argv,**kwargs):
            admission=c.read(argv[3])
            with self.db() as db: db.execute("INSERT INTO coordinator_packages VALUES(?,?,?)", (admission["rigId"],admission["packageKey"],json.dumps(admission["contract"])))
            return type("Result",(),{"returncode":0,"stdout":"{}","stderr":""})()
        with patch.object(c,"actor",return_value=(c.OPERATOR,"operator-gen")), patch.object(c.subprocess,"run",side_effect=cli), patch.object(c,"authenticated_post",side_effect=TimeoutError("uncertain configure")) as post:
            with self.assertRaises(TimeoutError): c.append_coordination_plan(route,path,settings)
            with self.assertRaisesRegex(RuntimeError,"prior plan configure uncertain"): c.append_coordination_plan(route,path,settings)
        self.assertEqual(post.call_count,1)

    def test_plan_append_refuses_missing_or_expired_supplied_evidence(self):
        prior,route,path,settings=self.plan_fixture()
        with patch.object(c,"actor",return_value=(c.OPERATOR,"operator-gen")), patch.object(c.subprocess,"run",side_effect=AssertionError("no missing admission mutation")), patch.object(c,"authenticated_post",side_effect=AssertionError("no config mutation")):
            with self.assertRaisesRegex(RuntimeError,"explicit current qualification"): c.append_coordination_plan(route,path,None)
            settings["admissions"]["builder@forge"]["validUntil"]=1
            with self.assertRaisesRegex(RuntimeError,"supplied current admission expired"): c.append_coordination_plan(route,path,settings)

    def test_planned_dispatch_uses_native_reconcile_queue_identity_and_pickup(self):
        prior,route,path,settings=self.plan_fixture()
        package=route["admission"]["packageKey"]
        qid="qitem-coordination-"+hashlib.sha256(("forge:"+package).encode()).hexdigest()[:24]
        route.update(phase="admitted",planRevision="appended",workerQitemId=qid)
        c.write(path,route)
        with self.db() as db: db.executescript("CREATE TABLE coordinator_assignments(rig_id TEXT,package_key TEXT,queue_id TEXT);")
        body=pathlib.Path(route["bodyPath"]).read_text()
        def reconcile(operation, packet):
            self.assertEqual(operation,"coordination-reconcile")
            self.assertEqual(packet,{"rigId":"forge"})
            with self.db() as db:
                db.execute("INSERT INTO coordinator_assignments VALUES(?,?,?)",("forge",package,qid))
                db.execute("INSERT INTO queue_items VALUES(?,?,?,?,?,?,?,?,?)",(qid,"builder@forge","in-progress","now","now","occupant-old",body,"lead@forge","lead-current"))
                db.execute("INSERT INTO queue_transitions(qitem_id,transition_note,identity_provenance) VALUES(?,'created','system:operator-authorized-coordination')",(qid,))
            return [{"state":"pending-pickup","queueId":qid}]
        with patch.object(c,"actor",return_value=("lead@forge","lead-current")), patch.object(c,"authenticated_post",side_effect=reconcile) as native, patch.object(c.subprocess,"run",side_effect=AssertionError("no direct dispatch for planned work")):
            self.assertEqual(c.coordinator_dispatch(path)["qitemId"],qid)
            self.assertEqual(c.coordinator_dispatch(path)["qitemId"],qid)
        self.assertEqual(native.call_count,1)
        self.assertEqual(c.require_claim("attempt","checkpoint","builder@forge","occupant-old")["qitem_id"],qid)

    def test_active_enum_with_closed_baton_holds_dispatch(self):
        prior,route,path,settings=self.plan_fixture()
        route.update(phase="admitted",summary="checkpoint")
        c.write(path,route)
        with self.db() as db: db.execute("UPDATE queue_items SET state='done' WHERE qitem_id='canonical-baton'")
        with patch.object(c,"actor",return_value=("lead@forge","lead-current")), patch.object(c.subprocess,"run",side_effect=AssertionError("no closed baton dispatch")):
            result=c.coordinator_dispatch(path)
        self.assertEqual(result["phase"],"held_current_baton_required")
        self.assertEqual(result["recoveryOwner"],c.OPERATOR)
        self.assertTrue(result["noAutomaticRetry"])

    def scheduler_policy(self):
        self.update_policy(controller_enabled=True,automatic_trigger_enabled=True,
                           automatic_cutover_enabled=True,operator_auto_execute_enabled=True,poll_seconds=10)

    def test_scheduler_stops_at_finite_end_without_extra_tick(self):
        self.scheduler_policy()
        clock=[1000.0]
        def sleep(seconds): clock[0]+=seconds
        with patch.object(c.time,"time",side_effect=lambda:clock[0]), patch.object(c.time,"sleep",side_effect=sleep), patch.object(c,"actor",return_value=(c.OPERATOR,"native-current")), patch.object(c,"tick",return_value={"seats":{"builder@forge":{"phase":"monitoring"}}}) as tick:
            result=c.schedule(1015)
        self.assertEqual(result["phase"],"end_time_reached")
        self.assertEqual(tick.call_count,2)
        self.assertEqual(result["iterations"],2)
        self.assertEqual(clock[0],1015)
        self.assertEqual(c.read(self.root/"scheduler-summary.json")["phaseCounts"],{"monitoring":2})

    def test_scheduler_skips_normal_controller_contention_then_continues(self):
        self.scheduler_policy()
        clock=[1000.0]
        locks=[None, BlockingIOError(), None]
        def flock(*args):
            if locks:
                result=locks.pop(0)
                if isinstance(result, Exception): raise result
        def sleep(seconds): clock[0]+=seconds
        with patch.object(c.time,"time",side_effect=lambda:clock[0]), patch.object(c.time,"sleep",side_effect=sleep), patch.object(c.fcntl,"flock",side_effect=flock), patch.object(c,"actor",return_value=(c.OPERATOR,"native-current")), patch.object(c,"tick",return_value={"phase":"monitoring"}) as tick:
            result=c.schedule(1005)
        self.assertEqual(result["phase"],"end_time_reached")
        self.assertEqual(result["contentionSkips"],1)
        self.assertEqual(tick.call_count,1)
        self.assertEqual(clock[0],1005)

    def test_scheduler_stops_when_current_native_operator_generation_retires(self):
        self.scheduler_policy()
        clock=[1000.0]
        def sleep(seconds): clock[0]+=seconds
        with patch.object(c.time,"time",side_effect=lambda:clock[0]), patch.object(c.time,"sleep",side_effect=sleep), patch.object(c,"actor",side_effect=[(c.OPERATOR,"native-old"),(c.OPERATOR,"native-old"),(c.OPERATOR,"native-new")]) as actor, patch.object(c,"tick",return_value={"phase":"monitoring"}) as tick:
            result=c.schedule(1100)
        self.assertEqual(result["phase"],"operator_generation_retired")
        self.assertEqual(tick.call_count,1)
        self.assertTrue(all(call.args==(c.OPERATOR,) for call in actor.call_args_list))

    def test_scheduler_requires_real_actor_finite_end_and_enabled_policy(self):
        for until in (None,float('inf'),float('nan'),0):
            with self.subTest(until=until), patch.object(c,"actor",side_effect=AssertionError("invalid end must not invoke actor")), self.assertRaisesRegex(RuntimeError,"finite future"):
                c.schedule(until)
        with patch.object(c,"actor",side_effect=RuntimeError("not a managed actor")), patch.object(c,"tick",side_effect=AssertionError("no unauthenticated scheduler")):
            with self.assertRaisesRegex(RuntimeError,"not a managed actor"): c.schedule(time.time()+60)
        with patch.object(c,"actor",return_value=(c.OPERATOR,"native-current")), patch.object(c,"tick",side_effect=AssertionError("disabled scheduler cannot tick")):
            result=c.schedule(time.time()+60)
        self.assertEqual(result["phase"],"policy_refused")
        self.assertEqual(result["iterations"],0)

    def test_scheduler_stops_on_refusal_without_retry_or_private_output(self):
        self.scheduler_policy()
        with patch.object(c,"actor",return_value=(c.OPERATOR,"native-current")), patch.object(c,"tick",side_effect=RuntimeError("private-native-token-content")) as tick, patch.object(c.time,"sleep",side_effect=AssertionError("no retry after refusal")):
            result=c.schedule(time.time()+60)
        self.assertEqual(result["phase"],"actor_or_policy_or_tick_refused")
        self.assertEqual(tick.call_count,1)
        self.assertNotIn("private-native-token-content",(self.root/"scheduler-summary.json").read_text())

    def test_existing_same_body_task_from_retired_producer_is_not_reused(self):
        self.task("return",c.OPERATOR,"review@forge","producer-old",body="same body")
        with patch.object(c.subprocess,"run",side_effect=AssertionError("cannot recreate an existing mismatched receipt")), self.assertRaisesRegex(RuntimeError,"existing actor task differs"):
            c.create_task(c.task_id("qa-test","return"),c.OPERATOR,"same body","summary","review@forge","producer-new")


if __name__ == "__main__":
    unittest.main()
