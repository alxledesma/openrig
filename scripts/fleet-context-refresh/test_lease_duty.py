import unittest, tempfile, pathlib, json
from lease_duty import guard, unresolved_renewal
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
class RestartFenceTests(unittest.TestCase):
 def test_unresolved_started_operation_blocks_restart(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);(root/'native-lease-x.started.json').write_text(json.dumps({'rigId':'r','actor':'lead@r','generation':'g','operationId':'x'}))
   self.assertTrue(unresolved_renewal(root,'r'))
 def test_exact_verified_receipt_releases_restart_fence(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);(root/'native-lease-x.started.json').write_text(json.dumps({'rigId':'r','actor':'lead@r','generation':'g','operationId':'x'}));receipt=root/'native-lease-x.receipt.json';receipt.write_text(json.dumps({'verifiedFromAuthority':True,'actualOwner':'lead@r','generation':'foreign','operationId':'x'}));self.assertTrue(unresolved_renewal(root,'r'));receipt.write_text(json.dumps({'verifiedFromAuthority':True,'actualOwner':'lead@r','generation':'g','operationId':'x'}));self.assertFalse(unresolved_renewal(root,'r'))
if __name__=='__main__':unittest.main()
