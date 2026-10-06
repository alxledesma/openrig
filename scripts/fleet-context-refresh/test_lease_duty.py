import unittest, tempfile, pathlib, json, time, subprocess, os, sqlite3, contextlib
import lease_duty
from lease_duty import guard, unresolved_renewal, classify_renewal, canonical, digest

class LeaseGuardTests(unittest.TestCase):
    def setUp(self):
        self.row={'owner_session':'lead@rig','owner_generation':'g','state':'active','lease_until':2000};self.baton={'state':'in-progress','claimed_by_generation_uuid':'g'}
    def check(self,actor='lead@rig',generation='g',current='g',now=1000):return guard(self.row,self.baton,actor,generation,current,now)
    def test_current_owner_allowed(self):self.assertTrue(self.check())
    def test_expired_never_recovered(self):self.assertFalse(self.check(now=2000))
    def test_other_actor_refused(self):self.assertFalse(self.check(actor='operator@kernel'))
    def test_retired_native_generation_refused(self):self.assertFalse(self.check(current='new'))
    def test_unclaimed_baton_refused(self):self.baton['claimed_by_generation_uuid']=None;self.assertFalse(self.check())
    def test_reconciliation_hold_refused(self):self.row['state']='reconciling';self.assertFalse(self.check())

def expect(rig='r',op='x',gen='g',epoch=3,lease_ms=3600000):
    token={'rigId':rig,'epoch':epoch,'generation':gen}
    return {'requestHash':digest(canonical({'token':token,'leaseMs':lease_ms})),'rigId':rig,'operationId':op,'generation':gen,'epoch':epoch}

class ClassificationTests(unittest.TestCase):
    def test_committed_requires_exact_durable_row(self):
        e=expect()
        body={'kind':'renew','requestHash':e['requestHash'],'rigId':'r','operationId':'x','receipt':{'rig_id':'r','operation_id':'x','owner_generation':'g','epoch':3,'lease_until':999}}
        k,d=classify_renewal(completed=False,stdout=None,returncode=-1,readback_status=200,readback_body=body,expect=e)
        self.assertEqual(k,'committed');self.assertEqual(d['leaseUntil'],999)
    def test_timeout_plus_404_is_unknown_never_rejection(self):
        e=expect()
        k,_=classify_renewal(completed=False,stdout=None,returncode=-1,readback_status=404,readback_body={'error':'operation_not_recorded'},expect=e)
        self.assertEqual(k,'unknown')
    def test_unreachable_readback_is_unknown(self):
        e=expect()
        k,_=classify_renewal(completed=False,stdout=None,returncode=-1,readback_status=None,readback_body=None,expect=e)
        self.assertEqual(k,'unknown')
    def test_typed_4xx_fence_plus_absent_row_is_rejected(self):
        # D2: rejected REQUIRES the durable row to be absent (404 operation_not_recorded).
        e=expect()
        k,d=classify_renewal(completed=True,stdout=json.dumps({'error':'coordinator_epoch_mismatch','httpStatus':409}),returncode=1,readback_status=404,readback_body={'error':'operation_not_recorded'},expect=e)
        self.assertEqual(k,'rejected');self.assertEqual(d['fenceCode'],'coordinator_epoch_mismatch')
    def test_typed_4xx_with_matching_200_row_is_committed_never_rejected(self):
        e=expect()
        body={'kind':'renew','requestHash':e['requestHash'],'rigId':'r','operationId':'x','receipt':{'rig_id':'r','operation_id':'x','owner_generation':'g','epoch':3,'lease_until':777}}
        k,_=classify_renewal(completed=True,stdout=json.dumps({'error':'coordinator_late_error','httpStatus':409}),returncode=1,readback_status=200,readback_body=body,expect=e)
        self.assertEqual(k,'committed')
    def test_typed_5xx_is_unknown_even_with_absent_row(self):
        e=expect()
        k,_=classify_renewal(completed=True,stdout=json.dumps({'error':'coordinator_internal','httpStatus':500}),returncode=1,readback_status=404,readback_body={'error':'operation_not_recorded'},expect=e)
        self.assertEqual(k,'unknown')
    def test_typed_4xx_with_unreachable_readback_is_unknown(self):
        e=expect()
        k,_=classify_renewal(completed=True,stdout=json.dumps({'error':'coordinator_epoch_mismatch','httpStatus':409}),returncode=1,readback_status=None,readback_body=None,expect=e)
        self.assertEqual(k,'unknown')
    def test_local_cli_error_without_http_provenance_stays_unknown(self):
        # A bare coordinator_* error printed by a CLI that never proved an HTTP response
        # must NOT be classified as proved-not-started.
        e=expect()
        k,_=classify_renewal(completed=True,stdout=json.dumps({'error':'coordinator_unreachable_locally'}),returncode=1,readback_status=404,readback_body={'error':'operation_not_recorded'},expect=e)
        self.assertEqual(k,'unknown')
    def test_request_hash_mismatch_is_unknown(self):
        e=expect()
        body={'kind':'renew','requestHash':'f'*64,'rigId':'r','operationId':'x','receipt':{'rig_id':'r','operation_id':'x','owner_generation':'g','epoch':3}}
        k,_=classify_renewal(completed=False,stdout=None,returncode=-1,readback_status=200,readback_body=body,expect=e)
        self.assertEqual(k,'unknown')
    def test_foreign_receipt_identity_is_unknown(self):
        e=expect()
        body={'kind':'renew','requestHash':e['requestHash'],'rigId':'r','operationId':'x','receipt':{'rig_id':'r','operation_id':'x','owner_generation':'OTHER','epoch':3}}
        k,_=classify_renewal(completed=False,stdout=None,returncode=-1,readback_status=200,readback_body=body,expect=e)
        self.assertEqual(k,'unknown')

class RestartFenceTests(unittest.TestCase):
    def _marker(self,root,e):
        (root/'native-lease-x.started.json').write_text(json.dumps({'rigId':'r','actor':'lead@r','generation':'g','operationId':'x','requestHash':e['requestHash']}))
    def test_unresolved_started_operation_blocks_restart(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);(root/'native-lease-x.started.json').write_text(json.dumps({'rigId':'r','actor':'lead@r','generation':'g','operationId':'x'}))
            self.assertTrue(unresolved_renewal(root,'r'))
    def test_exact_verified_receipt_releases_restart_fence(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=expect();self._marker(root,e)
            receipt=root/'native-lease-x.receipt.json'
            receipt.write_text(json.dumps({'verifiedFromAuthority':True,'source':'durable-readback','actualOwner':'lead@r','generation':'foreign','operationId':'x','requestHash':e['requestHash']}));self.assertTrue(unresolved_renewal(root,'r'))
            receipt.write_text(json.dumps({'verifiedFromAuthority':True,'source':'stale-format','actualOwner':'lead@r','generation':'g','operationId':'x','requestHash':e['requestHash']}));self.assertTrue(unresolved_renewal(root,'r'))
            receipt.write_text(json.dumps({'verifiedFromAuthority':True,'source':'durable-readback','terminalClass':'committed','actualOwner':'lead@r','generation':'g','operationId':'x','requestHash':'0'*64}));self.assertTrue(unresolved_renewal(root,'r'))
            receipt.write_text(json.dumps({'verifiedFromAuthority':True,'source':'durable-readback','terminalClass':'committed','actualOwner':'lead@r','generation':'g','operationId':'x','requestHash':e['requestHash']}));self.assertFalse(unresolved_renewal(root,'r'))
    def test_exact_proved_rejected_receipt_releases_but_foreign_does_not(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=expect();self._marker(root,e)
            receipt=root/'native-lease-x.receipt.json'
            receipt.write_text(json.dumps({'terminalClass':'rejected','fenceCode':'coordinator_epoch_mismatch','actualOwner':'someone@else','generation':'g','operationId':'x','requestHash':e['requestHash']}));self.assertTrue(unresolved_renewal(root,'r'))
            receipt.write_text(json.dumps({'terminalClass':'rejected','fenceCode':'coordinator_epoch_mismatch','actualOwner':'lead@r','generation':'g','operationId':'x','requestHash':e['requestHash']}));self.assertFalse(unresolved_renewal(root,'r'))

class RunLoopTests(unittest.TestCase):
    """End-to-end over run() with ONLY external seams mocked (CLI subprocess and
    http_readback) against a real read-only snapshot DB; proves exactly one submission
    per attempt and preserves the finite-deadline loop."""
    def _run(self,d,*,renew_side_effect,readback,row_lease_delta=1000):
        root=pathlib.Path(d);calls={'submit':0,'readback':0}
        def fake_run(argv,**kw):
            calls['submit']+=1
            if isinstance(renew_side_effect,Exception):raise renew_side_effect
            return renew_side_effect
        def fake_readback(rig,op):
            calls['readback']+=1
            return readback(op) if callable(readback) else readback
        old_sub,old_rb,old_env=subprocess.run,lease_duty.http_readback,dict(os.environ)
        try:
            lease_duty.subprocess.run=fake_run;lease_duty.http_readback=fake_readback
            os.environ.update({'OPENRIG_SESSION_NAME':'lead@r','OPENRIG_OCCUPANT_GENERATION':'g','OPENRIG_HOME':str(root)})
            con=sqlite3.connect(str(root/'openrig.sqlite'))
            con.execute('CREATE TABLE coordinator_authority(rig_id TEXT PRIMARY KEY,baton_id TEXT,owner_session TEXT,owner_generation TEXT,epoch INTEGER,lease_until INTEGER,state TEXT,operation_id TEXT,coordinators TEXT,recovery_queue_id TEXT)')
            con.execute("CREATE TABLE queue_items(qitem_id TEXT PRIMARY KEY,state TEXT,claimed_by_generation_uuid TEXT)")
            con.execute("CREATE TABLE occupant_tenures(id TEXT,node_id TEXT,generation_ordinal INTEGER,generation_uuid TEXT,kind TEXT)")
            con.execute("CREATE TABLE bindings(node_id TEXT,tmux_session TEXT)")
            con.execute("INSERT INTO coordinator_authority VALUES('r','b','lead@r','g',7,?,'active','x','[]',NULL)",(int(time.time()*1000)+row_lease_delta,))
            con.execute("INSERT INTO queue_items VALUES('b','in-progress','g')")
            con.execute("INSERT INTO bindings VALUES('n1','lead@r')")
            con.execute("INSERT INTO occupant_tenures VALUES('t1','n1',1,'g','fresh')")
            con.commit();con.close()
            result=lease_duty.run('r',time.time()+0.2,root)
            return result,calls,root
        finally:
            lease_duty.subprocess.run=old_sub;lease_duty.http_readback=old_rb
            os.environ.clear();os.environ.update(old_env)

    def test_a_timeout_plus_committed_row_writes_readback_receipt_and_one_submit(self):
        def rb(op):
            tok={'rigId':'r','epoch':7,'generation':'g'}
            return 200,{'kind':'renew','requestHash':digest(canonical({'token':tok,'leaseMs':3600000})),'rigId':'r','operationId':op,'receipt':{'rig_id':'r','operation_id':op,'owner_generation':'g','epoch':7,'lease_until':int(time.time()*1000)+3600000}}
        with tempfile.TemporaryDirectory() as d:
            result,calls,root=self._run(d,renew_side_effect=subprocess.TimeoutExpired(cmd='rig',timeout=45),readback=rb)
            self.assertEqual(calls['submit'],1)
            self.assertGreaterEqual(result['renewals'],1)
            receipts=list(root.glob('native-lease-*.receipt.json'))
            self.assertTrue(receipts and all(json.loads(p.read_text()).get('source')=='durable-readback' for p in receipts))
            # Audit pair preserved: the marker stays on disk yet RESOLVES via the exact receipt.
            self.assertTrue(list(root.glob('native-lease-*.started.json')));self.assertFalse(unresolved_renewal(root,'r'))
    def test_b_timeout_plus_no_row_is_unknown_single_submit_marker_kept(self):
        with tempfile.TemporaryDirectory() as d:
            result,calls,root=self._run(d,renew_side_effect=subprocess.TimeoutExpired(cmd='rig',timeout=45),readback=(404,{'error':'operation_not_recorded'}))
            self.assertEqual(calls['submit'],1);self.assertEqual(result['phase'],'renew_unknown')
            self.assertTrue(result['noAutomaticRetry']);self.assertEqual(result['recoveryOwner'],'operator-agent@kernel')
            self.assertTrue(list(root.glob('native-lease-*.started.json')));self.assertFalse(list(root.glob('native-lease-*.receipt.json')))
    def test_c_typed_http_rejection_records_terminal_rejected_receipt_no_resend(self):
        done=subprocess.CompletedProcess(args=['rig'],returncode=1,stdout=json.dumps({'error':'coordinator_epoch_mismatch','httpStatus':409}))
        with tempfile.TemporaryDirectory() as d:
            result,calls,root=self._run(d,renew_side_effect=done,readback=lambda op:(404,{'error':'operation_not_recorded'}))
            self.assertEqual(calls['submit'],1)  # terminal for this run: never sleep-resubmit
            self.assertEqual(result['phase'],'renew_refused');self.assertTrue(result['noAutomaticRetry'])
            receipts=[json.loads(p.read_text()) for p in root.glob('native-lease-*.receipt.json')]
            self.assertTrue(receipts and all(r.get('terminalClass')=='rejected' for r in receipts))
            self.assertTrue(list(root.glob('native-lease-*.started.json')))  # audit pair kept
            self.assertFalse(unresolved_renewal(root,'r'))                    # proved not started releases
    def test_f_expired_lease_never_renews(self):
        with tempfile.TemporaryDirectory() as d:
            result,calls,_=self._run(d,renew_side_effect=AssertionError('must not submit'),readback=(None,None),row_lease_delta=-1)
            self.assertEqual(calls['submit'],0);self.assertEqual(result['phase'],'authority_hold')

class LegacyReconciliationTests(unittest.TestCase):
    """D1: pre-P3b verified pairs must self-heal through the DURABLE READBACK — never by
    hand-written receipts — while unprovable evidence stays fenced."""
    def _pair(self,root,*,legacy=True,packet=True,marker_hash=None):
        e=expect()
        marker={'rigId':'r','actor':'lead@r','generation':'g','operationId':'x'}
        if not legacy:marker['requestHash']=marker_hash if marker_hash is not None else e['requestHash']
        (root/'native-lease-x.started.json').write_text(json.dumps(marker))
        if legacy:(root/'native-lease-x.receipt.json').write_text(json.dumps({'operationId':'x','exitCode':0,'leaseUntil':999,'actualOwner':'lead@r','generation':'g','verifiedFromAuthority':True}))
        if packet:(root/'x.json').write_text(json.dumps({'token':{'rigId':'r','epoch':3,'generation':'g'},'leaseMs':3600000,'operationId':'x'}))
        return e
    def _committed_row(self,e,op='x'):
        return 200,{'kind':'renew','requestHash':e['requestHash'],'rigId':'r','operationId':op,'receipt':{'rig_id':'r','operation_id':op,'owner_generation':'g','epoch':3,'lease_until':999}}
    @contextlib.contextmanager
    def _with_readback(self,fn,body):
        old=lease_duty.http_readback;lease_duty.http_readback=fn
        try:yield body
        finally:lease_duty.http_readback=old
    def test_legacy_pair_with_matching_committed_row_releases_without_touching_history(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=self._pair(root)
            old_bytes=(root/'native-lease-x.receipt.json').read_bytes()
            gen=self._with_readback(lambda rig,op:self._committed_row(e),None)
            with gen:released=not unresolved_renewal(root,'r')
            self.assertTrue(released)
            self.assertEqual((root/'native-lease-x.receipt.json').read_bytes(),old_bytes)  # history immutable
            rec=json.loads((root/'x.readback.json').read_text())
            self.assertEqual(rec['source'],'durable-readback-reconciliation');self.assertEqual(rec['requestHash'],e['requestHash'])
    def test_legacy_pair_with_404_stays_fenced(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);self._pair(root)
            gen=self._with_readback(lambda rig,op:(404,{'error':'operation_not_recorded'}),None)
            with gen:self.assertTrue(unresolved_renewal(root,'r'))
    def test_legacy_pair_with_unreachable_readback_stays_fenced(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);self._pair(root)
            gen=self._with_readback(lambda rig,op:(None,None),None)
            with gen:self.assertTrue(unresolved_renewal(root,'r'))
    def test_missing_packet_stays_fenced(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=self._pair(root,packet=False)
            gen=self._with_readback(lambda rig,op:self._committed_row(e),None)
            with gen:self.assertTrue(unresolved_renewal(root,'r'))
    def test_packet_generation_mismatch_stays_fenced(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=self._pair(root)
            (root/'x.json').write_text(json.dumps({'token':{'rigId':'r','epoch':3,'generation':'OTHER'},'leaseMs':3600000,'operationId':'x'}))
            gen=self._with_readback(lambda rig,op:self._committed_row(e),None)
            with gen:self.assertTrue(unresolved_renewal(root,'r'))
    def test_row_with_different_request_hash_stays_fenced(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=self._pair(root)
            status,body=self._committed_row(e);body=dict(body,requestHash='0'*64)
            gen=self._with_readback(lambda rig,op:(status,body),None)
            with gen:self.assertTrue(unresolved_renewal(root,'r'))
    def test_new_unknown_marker_reconciles_on_next_launch_with_zero_submits(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);e=self._pair(root,legacy=False)
            submits=[]
            old_sub=lease_duty.subprocess.run;lease_duty.subprocess.run=lambda *a,**k:submits.append(a)
            gen=self._with_readback(lambda rig,op:self._committed_row(e),None)
            try:
                with gen:released=not unresolved_renewal(root,'r')
            finally:lease_duty.subprocess.run=old_sub
            self.assertTrue(released);self.assertEqual(submits,[])  # reconciliation NEVER resubmits the renew

if __name__=='__main__':unittest.main()
