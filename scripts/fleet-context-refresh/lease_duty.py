#!/usr/bin/env python3
"""Finite native-owner lease duty; never obtains or recovers authority."""
import argparse, fcntl, json, os, pathlib, sqlite3, subprocess, time, uuid

def guard(row, baton, actor, generation, current, now):
    return bool(row and baton and row['owner_session']==actor and row['owner_generation']==generation==current
                and row['state']=='active' and row['lease_until']>now and baton['state']=='in-progress'
                and baton['claimed_by_generation_uuid']==generation)

def write(path, data):
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(data,indent=2)+'\n');tmp.chmod(0o600);tmp.replace(path)

def unresolved_renewal(root, rig_id):
    for started in root.glob('native-lease-*.started.json'):
        try:
            marker=json.loads(started.read_text())
            if marker.get('rigId') not in (None,rig_id):continue
            receipt=json.loads(started.with_name(started.name.replace('.started.json','.receipt.json')).read_text())
            if not (receipt.get('verifiedFromAuthority') is True and receipt.get('operationId')==marker.get('operationId')
                    and receipt.get('actualOwner')==marker.get('actor') and receipt.get('generation')==marker.get('generation')):return True
        except (OSError,ValueError):return True
    return False

def run(rig_id, until, root):
    actor=os.environ.get('OPENRIG_SESSION_NAME');generation=os.environ.get('OPENRIG_OCCUPANT_GENERATION')
    if not actor or not generation or not time.time()<until<float('inf'):raise RuntimeError('Actual native identity and finite future deadline required')
    root.mkdir(parents=True,exist_ok=True);summary={'actor':actor,'generation':generation,'rigId':rig_id,'until':until,'renewals':0}
    dbpath=pathlib.Path(os.environ.get('OPENRIG_HOME',str(pathlib.Path.home()/'.openrig')))/'openrig.sqlite'
    def snapshot():
        with sqlite3.connect('file:'+str(dbpath)+'?mode=ro',uri=True) as db:
            db.row_factory=sqlite3.Row
            row=db.execute('SELECT * FROM coordinator_authority WHERE rig_id=?',(rig_id,)).fetchone()
            baton=db.execute('SELECT state,claimed_by_generation_uuid FROM queue_items WHERE qitem_id=?',(row['baton_id'],)).fetchone() if row else None
            current=db.execute('SELECT t.generation_uuid FROM occupant_tenures t JOIN bindings b ON b.node_id=t.node_id WHERE b.tmux_session=? ORDER BY t.generation_ordinal DESC LIMIT 1',(actor,)).fetchone()
            return dict(row) if row else None,dict(baton) if baton else None,current[0] if current else None
    with open(root/(rig_id+'.lock'),'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if unresolved_renewal(root,rig_id):
            summary.update(phase='renew_uncertainty_hold',noAutomaticRetry=True,recoveryOwner='operator-agent@kernel',nextAction='Reconcile exact started operation against durable coordinator operation receipt before new duty')
            write(root/(rig_id+'.summary.json'),summary);return summary
        expected_epoch=None
        while time.time()<until:
            row,baton,current=snapshot();now=int(time.time()*1000)
            if not guard(row,baton,actor,generation,current,now) or expected_epoch is not None and row['epoch']!=expected_epoch:
                summary.update(phase='authority_hold',recoveryOwner='operator-agent@kernel',nextAction='Reconcile current authority; use supported explicit transfer and acknowledgment if expired',wake='New valid native authority disposition');break
            expected_epoch=row['epoch']
            if row['lease_until']-now<600000:
                op='native-lease-'+uuid.uuid4().hex;body={'token':{'rigId':rig_id,'epoch':row['epoch'],'generation':generation},'leaseMs':3600000,'operationId':op};packet=root/(op+'.json');write(packet,body)
                write(root/(op+'.started.json'),{'rigId':rig_id,'actor':actor,'generation':generation,'operationId':op})
                try:result=subprocess.run([os.environ.get('OPENRIG_ROTATION_CLI',str(pathlib.Path.home()/'.local/bin/rig')),'coordinator','renew',str(packet)],capture_output=True,text=True,timeout=45)
                except (OSError,subprocess.TimeoutExpired):summary.update(phase='renew_uncertain',operationId=op,noAutomaticRetry=True);break
                after,after_baton,current=snapshot()
                if not guard(after,after_baton,actor,generation,current,int(time.time()*1000)) or after['epoch']!=expected_epoch or after['operation_id']!=op:
                    summary.update(phase='renew_refused_or_uncertain',operationId=op,noAutomaticRetry=True);break
                write(root/(op+'.receipt.json'),{'operationId':op,'exitCode':result.returncode,'leaseUntil':after['lease_until'],'actualOwner':actor,'generation':generation,'verifiedFromAuthority':True});summary['renewals']+=1
            summary['phase']='running';write(root/(rig_id+'.summary.json'),summary)
            time.sleep(min(60,max(0,until-time.time())))
        else:summary['phase']='end_time_reached'
        write(root/(rig_id+'.summary.json'),summary);return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--rig-id',required=True);p.add_argument('--until',required=True,type=float);p.add_argument('--state-dir',required=True,type=pathlib.Path);a=p.parse_args();print(json.dumps(run(a.rig_id,a.until,a.state_dir)))
