#!/usr/bin/env python3
"""Finite native-owner lease duty; never obtains or recovers authority.

P3b: every renew attempt is classified into exactly ONE terminal class using the
supported authenticated durable-operation readback (GET /api/coordinator/:rigId/
operations/:operationId, added by P3a). A local receipt may resolve a started
marker ONLY when its exact operationId, actor, generation and requestHash all
match the marker; a foreign or unprovable outcome stays an owned UNKNOWN hold.
An uncertain renew is NEVER resent and a receipt is NEVER forged."""
import argparse, fcntl, hashlib, json, os, pathlib, sqlite3, subprocess, time, urllib.error, urllib.parse, urllib.request, uuid

def guard(row, baton, actor, generation, current, now):
    return bool(row and baton and row['owner_session']==actor and row['owner_generation']==generation==current
                and row['state']=='active' and row['lease_until']>now and baton['state']=='in-progress'
                and baton['claimed_by_generation_uuid']==generation)

def write(path, data):
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(data,indent=2)+'\n');tmp.chmod(0o600);tmp.replace(path)

def canonical(value):
    if isinstance(value,list):return '['+','.join(canonical(x) for x in value)+']'
    if isinstance(value,dict):return '{'+','.join(f'{json.dumps(k)}:{canonical(value[k])}' for k in sorted(value))+'}'
    return json.dumps(value,separators=(',',':'))
def digest(text):return hashlib.sha256(text.encode()).hexdigest()

def _daemon_base_url():
    """Existing supported discovery, mirrored: OPENRIG_URL wins, else daemon.json
    {host,port} under OPENRIG_HOME. Never a guessed production address."""
    url=os.environ.get('OPENRIG_URL')
    if url:return url.rstrip('/')
    home=os.environ.get('OPENRIG_HOME',str(pathlib.Path.home()/'.openrig'))
    try:
        state=json.loads((pathlib.Path(home)/'daemon.json').read_text())
        host=state.get('host') or '127.0.0.1';port=state.get('port')
        if isinstance(port,int) and 1<=port<=65535:return f'http://{host}:{port}'
    except (OSError,ValueError):pass
    return None

def _terminal_bearer_token():
    """Same resolution order as the shipped CLI client.ts resolveTerminalToken:
    env OPENRIG_TERMINAL_BEARER_TOKEN, else $OPENRIG_HOME/terminal-token. Value is
    NEVER printed or logged."""
    env=os.environ.get('OPENRIG_TERMINAL_BEARER_TOKEN')
    if env and env.strip():return env.strip()
    home=os.environ.get('OPENRIG_HOME',str(pathlib.Path.home()/'.openrig'))
    try:
        token=(pathlib.Path(home)/'terminal-token').read_text().strip()
        return token or None
    except OSError:return None

def http_readback(rig_id,operation_id):
    """Authenticated durable operation readback. Returns (status,body|None) or
    (None,None) on ANY transport/auth gap: an unreachable or unauthenticated
    readback can never prove commitment, and absence is never rejection."""
    url=_daemon_base_url();token=_terminal_bearer_token()
    if not url or not token:return None,None
    target=url+'/api/coordinator/'+urllib.parse.quote(rig_id,safe='')+'/operations/'+urllib.parse.quote(operation_id,safe='')
    try:
        req=urllib.request.Request(target,headers={'Authorization':'Bearer '+token})
        with urllib.request.urlopen(req,timeout=10) as r:
            try:return r.status,json.loads(r.read())
            except ValueError:return r.status,None
    except urllib.error.HTTPError as e:
        try:return e.code,json.loads(e.read())
        except Exception:return e.code,None
    except Exception:return None,None

def classify_renewal(*,completed,stdout,returncode,readback_status,readback_body,expect):
    """Exactly one terminal class per attempt. The durable READBACK is always consulted;
    no class is asserted from local evidence alone.
    'committed' REQUIRES the durable row to match rigId, operationId, kind='renew',
    requestHash and the receipt's own rig/operation/generation/epoch.
    'rejected' additionally REQUIRES (packet contract): a completed call whose JSON body
    carries a typed coordinator_* code AND a 4xx httpStatus (5xx is never proof of
    not-started), AND the durable row ABSENT — readback 404 operation_not_recorded.
    Anything else — timeout, transport loss, non-JSON, mismatch, 5xx, unreachable or
    missing readback — is 'unknown': owned, fenced, never resent."""
    s,p=readback_status,readback_body
    if s==200 and isinstance(p,dict) and p.get('kind')=='renew' and p.get('requestHash')==expect['requestHash'] and p.get('rigId')==expect['rigId'] and p.get('operationId')==expect['operationId']:
        r=p.get('receipt')
        if isinstance(r,dict) and r.get('rig_id')==expect['rigId'] and r.get('operation_id')==expect['operationId'] and r.get('owner_generation')==expect['generation'] and r.get('epoch')==expect['epoch']:
            return ('committed',{'leaseUntil':r.get('lease_until')})
    fence=None
    if completed:
        try:body=json.loads(stdout)
        except (TypeError,ValueError):body=None
        if (isinstance(body,dict) and str(body.get('error','')).startswith('coordinator_')
            and isinstance(body.get('httpStatus'),int) and 400<=body['httpStatus']<500):
            fence=(body['error'],body['httpStatus'])
    if fence is not None and s==404 and isinstance(p,dict) and p.get('error')=='operation_not_recorded':
        return ('rejected',{'fenceCode':fence[0],'httpStatus':fence[1]})
    return ('unknown',{'readbackStatus':s})

def _marker_expect(packet,rig_id):
    tok=packet['token']
    return {'requestHash':digest(canonical({'token':tok,'leaseMs':packet['leaseMs']})),'rigId':tok['rigId'],'operationId':packet['operationId'],'generation':tok['generation'],'epoch':tok['epoch']}

def unresolved_renewal(root, rig_id):
    """A started marker releases ONLY through proven outcome, never hand-assertion:
    (a) an exact P3b receipt (same operationId, actor, generation AND requestHash) that
    is verified-readback-committed or proved-rejected; OR
    (b) RECONCILIATION via the durable readback itself: the preserved <operation>.json
    packet must bind (operationId, rigId, token.generation == marker.generation) and its
    recomputed requestHash must equal any hash on the marker; then a matching committed
    row releases and a NEW readback-sourced receipt is written WITHOUT overwriting the
    historical receipt. Legacy pre-P3b pairs therefore self-heal through the supported
    API instead of fencing relaunch forever. 404, unreachable, absent/mismatched packet,
    or hash mismatch stay fenced (owned UNKNOWN; never replayed)."""
    for started in root.glob('native-lease-*.started.json'):
        try:
            marker=json.loads(started.read_text())
            if marker.get('rigId') not in (None,rig_id):continue
            op=marker.get('operationId')
            try:
                receipt=json.loads(started.with_name(started.name.replace('.started.json','.receipt.json')).read_text())
                identity_ok=(receipt.get('operationId')==op and receipt.get('actualOwner')==marker.get('actor')
                             and receipt.get('generation')==marker.get('generation') and receipt.get('requestHash')==marker.get('requestHash'))
                if identity_ok and (receipt.get('terminalClass')=='rejected'
                                    or (receipt.get('verifiedFromAuthority') is True and receipt.get('source')=='durable-readback')):continue
            except (OSError,ValueError):pass
            # No releasing local receipt: reconcile strictly through the durable readback.
            try:
                packet=json.loads((root/(op+'.json')).read_text())
                if packet.get('operationId')!=op:raise ValueError('packet-op-mismatch')
                tok=packet['token']
                if tok.get('rigId')!=rig_id or tok.get('generation')!=marker.get('generation'):raise ValueError('packet-binding-mismatch')
                expect=_marker_expect(packet,rig_id)
                if marker.get('requestHash') is not None and marker['requestHash']!=expect['requestHash']:return True
                status,body=http_readback(rig_id,op)
                kind,_detail=classify_renewal(completed=False,stdout=None,returncode=-1,readback_status=status,readback_body=body,expect=expect)
                if kind=='committed':
                    alt=root/(op+'.readback.json')
                    if not alt.exists():write(alt,{'operationId':op,'requestHash':expect['requestHash'],'terminalClass':'committed','source':'durable-readback-reconciliation','actualOwner':marker.get('actor'),'generation':marker.get('generation'),'verifiedFromAuthority':True})
                    continue
            except (OSError,ValueError,KeyError,TypeError):return True
            return True
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
                expect={'requestHash':digest(canonical({'token':body['token'],'leaseMs':body['leaseMs']})),'rigId':rig_id,'operationId':op,'generation':generation,'epoch':row['epoch']}
                write(root/(op+'.started.json'),{'rigId':rig_id,'actor':actor,'generation':generation,'operationId':op,'requestHash':expect['requestHash']})
                completed=False;stdout='';returncode=-1
                try:result=subprocess.run([os.environ.get('OPENRIG_ROTATION_CLI',str(pathlib.Path.home()/'.local/bin/rig')),'coordinator','renew',str(packet)],capture_output=True,text=True,timeout=45)
                except (OSError,subprocess.TimeoutExpired):pass
                else:completed,stdout,returncode=True,result.stdout,result.returncode
                # D2: the durable readback is ALWAYS consulted before any class decision.
                rb_status,rb_body=http_readback(rig_id,op)
                kind,detail=classify_renewal(completed=completed,stdout=stdout,returncode=returncode,readback_status=rb_status,readback_body=rb_body,expect=expect)
                if kind=='rejected':
                    # PROVED NOT STARTED with explicit HTTP provenance: TERMINAL for this
                    # finite run. The marker+receipt audit pair is preserved; never
                    # sleep-then-resubmit the same refusal, never an automatic restart.
                    write(root/(op+'.receipt.json'),{'operationId':op,'requestHash':expect['requestHash'],'terminalClass':'rejected','fenceCode':detail['fenceCode'],'httpStatus':detail['httpStatus'],'verifiedFromAuthority':False,'actualOwner':actor,'generation':generation})
                    summary.update(phase='renew_refused',operationId=op,noAutomaticRetry=True,recoveryOwner='operator-agent@kernel',nextAction='A later genuine bounded duty under a fresh authority guard is a separate supported owner action; this proved refusal is terminal for this run')
                    break
                if kind=='committed':
                    write(root/(op+'.receipt.json'),{'operationId':op,'requestHash':expect['requestHash'],'terminalClass':'committed','source':'durable-readback','leaseUntil':detail['leaseUntil'],'actualOwner':actor,'generation':generation,'verifiedFromAuthority':True});summary['renewals']+=1
                else:
                    # UNKNOWN: timeout, transport loss, non-JSON, 404/absent row, hash or
                    # identity mismatch, or auth-discovery gaps. Owned hold; marker kept
                    # UNRESOLVED; NEVER replayed; a receipt is NEVER forged.
                    summary.update(phase='renew_unknown',operationId=op,noAutomaticRetry=True,recoveryOwner='operator-agent@kernel',nextAction='Reconcile this exact operationId later through the supported authenticated operations readback; never resend the renew. If endpoint/bearer discovery is unavailable, that access gap is itself the integration hold.')
                    break
            expected_epoch=row['epoch']
            summary['phase']='running';write(root/(rig_id+'.summary.json'),summary)
            time.sleep(min(60,max(0,until-time.time())))
        else:summary['phase']='end_time_reached'
        write(root/(rig_id+'.summary.json'),summary);return summary

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--rig-id',required=True);p.add_argument('--until',required=True,type=float);p.add_argument('--state-dir',required=True,type=pathlib.Path);a=p.parse_args();print(json.dumps(run(a.rig_id,a.until,a.state_dir)))
