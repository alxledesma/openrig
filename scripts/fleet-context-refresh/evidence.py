#!/usr/bin/env python3
"""Cooperative, generation-fenced OpenRig context rotation. No transcript edits."""
import argparse, datetime, fcntl, hashlib, json, os, pathlib, subprocess, time, urllib.request, urllib.parse, urllib.error, shlex, sqlite3
ROOT = pathlib.Path(__file__).resolve().parent
RIG = os.environ.get('OPENRIG_ROTATION_CLI', '/Users/alex/.local/bin/rig')
RIG_HOME = pathlib.Path(os.environ.get('OPENRIG_HOME', str(pathlib.Path.home()/'.openrig')))
COMMAND = 'env OPENRIG_HOME='+shlex.quote(str(RIG_HOME))+' OPENRIG_ROTATION_CLI='+shlex.quote(RIG)+' python3 '+shlex.quote(str(ROOT/'controller.py'))
REQUIRED = ('current_work','decisions','memory','constraints','standing_duties','evidence','next_action','outstanding_effects')

def read(p, default=None):
    try: return json.loads(pathlib.Path(p).read_text())
    except FileNotFoundError: return default

def write(p, obj):
    p=pathlib.Path(p); p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_suffix(p.suffix+'.tmp'); tmp.write_text(json.dumps(obj,indent=2)+'\n'); os.chmod(tmp,0o600); tmp.replace(p)

def call(args, timeout=45):
    r=subprocess.run([RIG]+args,text=True,capture_output=True,timeout=timeout)
    if r.returncode: raise RuntimeError(r.stderr.strip() or r.stdout.strip())
    return json.loads(r.stdout)

def digest(obj): return hashlib.sha256(json.dumps(obj,sort_keys=True).encode()).hexdigest()

def native(path):
    count=0; model=None
    with open(path) as f:
        for line in f:
            try: row=json.loads(line)
            except ValueError: continue
            if row.get('type')=='compacted': count+=1
            if row.get('type')=='turn_context': model=row.get('payload',{}).get('model',model)
    return count,model

def snapshot(seat):
    who=call(['whoami','--session',seat,'--full','--json'])
    status=call(['seat','status',seat,'--json'])
    usage=who.get('contextUsage',{})
    generation=usage.get('sessionId')
    if not generation: raise RuntimeError('native generation unavailable')
    return {'who':who,'status':status,'generation':generation}

def queue(seat):
    # Match rotationActiveQueueRows in the daemon, including the current local
    # host-qualified alias. Foreign host suffixes remain outside local custody.
    db=sqlite3.connect('file:'+str(RIG_HOME/'openrig.sqlite')+'?mode=ro',uri=True)
    try:
        host=db.execute("SELECT host_id FROM self_host_identity WHERE singleton=1").fetchone()
        if not host or not host[0] or len(seat.split('@'))!=2:
            raise RuntimeError('current local host identity or canonical seat unavailable for rotation custody')
        rows=db.execute("SELECT qitem_id,destination_session,state,claimed_at,ts_updated,claimed_by_generation_uuid,body FROM queue_items WHERE destination_session IN (?,?) AND state NOT IN ('done','cancelled') ORDER BY qitem_id",(seat,seat+'@'+host[0])).fetchall()
        return [{'id':qid,'destinationSession':dest,'state':state,'claimedAt':claimed,'updated':updated,'claimGeneration':gen,'bodyHash':hashlib.sha256(body.encode()).hexdigest()} for qid,dest,state,claimed,updated,gen,body in rows]
    finally: db.close()

def queue_continuity(before,after,old_generation,new_generation):
    if len(before)!=len(after): return False
    for old,new in zip(before,after):
        if old['id']!=new['id'] or old['bodyHash']!=new['bodyHash'] or old.get('destinationSession')!=new.get('destinationSession'): return False
        if old==new: continue
        if old['state']!='in-progress' or not old_generation or old.get('claimGeneration')!=old_generation: return False
        if new['state']=='pending' and new.get('claimGeneration') is None and new.get('claimedAt') is None: continue
        if new['state']=='in-progress' and new_generation and new.get('claimGeneration')==new_generation: continue
        return False
    return True

def runtime_contract(snap):
    identity=snap['who'].get('identity',{}); usage=snap['who'].get('contextUsage',{})
    if identity.get('runtime')!='codex': raise RuntimeError('native rotation proof currently supports Codex only')
    meta=turn=None
    with open(usage['transcriptPath']) as stream:
        for line in stream:
            try: row=json.loads(line)
            except ValueError: continue
            if row.get('type')=='session_meta': meta=row.get('payload')
            if row.get('type')=='turn_context': turn=row.get('payload')
    if not meta or not turn or (meta.get('id') or meta.get('session_id'))!=snap['generation']: raise RuntimeError('native generation evidence unavailable')
    provider=turn.get('model_provider') or meta.get('model_provider')
    if not provider or not turn.get('model') or not turn.get('sandbox_policy') or not turn.get('approval_policy'): raise RuntimeError('native provider/model/permissions unavailable')
    profile=launch_profile(identity['sessionName'])
    return {'runtime':'codex','model':turn['model'],'provider':provider,'profile':profile,'permissions':{'sandbox':turn['sandbox_policy'],'approval':turn['approval_policy']},'effort':turn.get('effort')}

def launch_profile(seat):
    pid=int(subprocess.check_output(['tmux','display-message','-p','-t',seat,'#{pane_pid}'],text=True).strip())
    lines=subprocess.check_output(['/bin/ps','-axo','pid=,ppid=,comm='],text=True).splitlines()
    import re
    rows=[re.match(r'^\s*(\d+)\s+(\d+)\s+(.*)$',line) for line in lines]; rows=[row for row in rows if row]
    descendants={pid}
    for _ in rows:
        previous=len(descendants)
        descendants.update(int(row[1]) for row in rows if int(row[2]) in descendants)
        if len(descendants)==previous: break
    native_rows=[row for row in rows if int(row[1]) in descendants and pathlib.Path(row[3]).name=='codex']
    if len(native_rows)!=1: raise RuntimeError('exactly one native Codex process required')
    args=shlex.split(subprocess.check_output(['/bin/ps','-p',native_rows[0][1],'-o','args='],text=True))
    for flag in ('-p','--profile'):
        if flag in args and args.index(flag)+1<len(args): return args[args.index(flag)+1]
    raise RuntimeError('launch profile unavailable')

def idle_ready(node,snap):
    return (node.get('activityState') or {}).get('activity')=='idle-at-prompt' and (node.get('activityState') or {}).get('decidedBy') is not None and not snap['status'].get('typingGuard',{}).get('desired') and snap['status'].get('session_status')=='running' and snap['status'].get('startup_status')=='ready'
