import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { constants, closeSync, existsSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import path from 'node:path';

/** Kernel instance proof shared by the authenticated producer and absence census.
 * No argv or environment bytes leave Python. Artifact role is an explicit trusted
 * peer judgment; compiled-image detection alone never grants a disposition. */
export const SERVICE_PROCESS_PY = String.raw`import ctypes,json,os,struct,hashlib,sys
IDENTITY_KEYS=['OPENRIG_NODE_ID','OPENRIG_SESSION_NAME','OPENRIG_HOME','OPENRIG_OCCUPANT_GENERATION','OPENRIG_RUNTIME']
LOADER_KEYS=['LD_PRELOAD','LD_LIBRARY_PATH','LD_AUDIT','LD_DEBUG']
def proof_hash(v): return hashlib.sha256(json.dumps(v,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def executable_path(kernel,native):
 if not kernel or not os.path.isabs(kernel) or any(ord(c)<32 for c in kernel): raise ValueError()
 if native is not None:
  if not os.path.isabs(native) or any(ord(c)<32 for c in native): raise ValueError()
  if 'codex' in [os.path.basename(kernel),os.path.basename(native)] and native!=kernel and not os.path.samefile(native,kernel): raise ValueError()
 if os.path.basename(kernel)=='codex': return kernel
 return native if native is not None else kernel
def argv_env(pid):
 if sys.platform=='darwin':
  libc=ctypes.CDLL(None); mib=(ctypes.c_int*3)(1,49,pid); n=ctypes.c_size_t(0)
  if libc.sysctl(mib,3,None,ctypes.byref(n),None,0)!=0 or not 0<n.value<=8388608: raise ValueError()
  b=ctypes.create_string_buffer(n.value)
  if libc.sysctl(mib,3,b,ctypes.byref(n),None,0)!=0: raise ValueError()
  raw=b.raw[:n.value]; argc=struct.unpack_from('i',raw)[0]
  if not 0<argc<=65536: raise ValueError()
  end=raw.index(b'\0',4); kernel=raw[4:end].decode('utf8'); at=end+1
  while at<len(raw) and raw[at]==0: at+=1
  argv=[]
  for _ in range(argc):
   end=raw.index(b'\0',at); argv.append(raw[at:end].decode('utf8')); at=end+1
  env=raw[at:].split(b'\0'); lib=ctypes.CDLL('/usr/lib/libproc.dylib'); buf=ctypes.create_string_buffer(4096)
  if lib.proc_pidpath(pid,buf,len(buf))<=0: raise ValueError()
  executable=executable_path(kernel,buf.value.decode('utf8'))
 elif sys.platform.startswith('linux'):
  argv=open('/proc/'+str(pid)+'/cmdline','rb').read().rstrip(b'\0').decode('utf8').split('\0')
  env=open('/proc/'+str(pid)+'/environ','rb').read().split(b'\0'); executable=os.readlink('/proc/'+str(pid)+'/exe')
 else: raise ValueError()
 return argv,env,executable
# Preserve the strict image reader when composed with the legacy census helper.
service_argv_env=argv_env
def kernel_instance(pid):
 if sys.platform=='darwin':
  lib=ctypes.CDLL('/usr/lib/libproc.dylib'); b=ctypes.create_string_buffer(136)
  if lib.proc_pidinfo(pid,3,0,b,136)!=136: raise ValueError()
  raw=b.raw; observed_pid,ppid,uid=struct.unpack_from('III',raw,12); sec,usec=struct.unpack_from('QQ',raw,120)
  if observed_pid!=pid or usec>=1000000: raise ValueError()
  libc=ctypes.CDLL(None)
  def sysctl(name,size):
   out=ctypes.create_string_buffer(size); n=ctypes.c_size_t(size)
   if libc.sysctlbyname(name,out,ctypes.byref(n),None,0)!=0: raise ValueError()
   return out.raw[:n.value]
  boot=proof_hash([sysctl(b'kern.boottime',16).hex(),sysctl(b'kern.uuid',256).rstrip(b'\0').decode()])
  start=str(sec*1000000+usec)
 elif sys.platform.startswith('linux'):
  raw=open('/proc/'+str(pid)+'/stat').read(); fields=raw[raw.rindex(')')+2:].split()
  ppid=int(fields[1]); uid=os.stat('/proc/'+str(pid)).st_uid; start=fields[19]
  boot=proof_hash(open('/proc/sys/kernel/random/boot_id').read().strip())
 else: raise ValueError()
 return {'pid':pid,'ppid':ppid,'uid':uid,'boot':boot,'start':start}
def image_stamp(executable):
 canonical=os.path.realpath(executable); before=os.stat(canonical)
 if not os.path.isabs(executable) or not os.path.isfile(canonical) or before.st_mode&0o022: raise ValueError()
 with open(canonical,'rb') as f:
  magic=f.read(4); f.seek(0); digest=hashlib.sha256(f.read()).hexdigest()
 after=os.stat(canonical)
 stamp=lambda s:[s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_mode]
 if stamp(before)!=stamp(after): raise ValueError()
 compiled=magic in [b'\x7fELF',b'\xcf\xfa\xed\xfe',b'\xce\xfa\xed\xfe',b'\xfe\xed\xfa\xcf',b'\xfe\xed\xfa\xce',b'\xca\xfe\xba\xbe',b'\xca\xfe\xba\xbf']
 return {'path':canonical,'device':str(before.st_dev),'inode':str(before.st_ino),'size':before.st_size,'mtime':str(before.st_mtime_ns),'ctime':str(before.st_ctime_ns),'sha256':digest,'compiled':compiled}
def inherited_identity(env):
 result={}
 for key in IDENTITY_KEYS:
  prefix=(key+'=').encode(); values=[x[len(prefix):] for x in env if x.startswith(prefix)]
  if len(values)>1: raise ValueError()
  result[key]=values[0].decode('utf8') if values else None
 loader=any(x.split(b'=',1)[0].startswith(b'DYLD_') or x.split(b'=',1)[0].decode('utf8') in LOADER_KEYS for x in env if x)
 return result,loader
def service_hazard(argv,executable):
 name=os.path.basename(executable).lower()
 # A trusted artifact judgment cannot enroll a known native agent, shell,
 # interpreter or script wrapper as a service. Unknown compiled artifacts
 # still require the explicit peer judgment bound to their content hash.
 names=['pi','codex','claude','node','nodejs','bun','deno','python','ruby','perl','php','java','sh','bash','zsh','fish','dash','ksh','csh','tcsh','env']
 if name in names or name.startswith('python') or name.startswith('node-'): return True
 return any(os.path.basename(a) in ['pi-runner.js','native-duty-supervisor.js','cli.js'] or a.endswith(('.py','.sh','.js','.mjs','.cjs')) for a in argv)
def sample_process(pid):
 before=kernel_instance(pid); argv,env,executable=service_argv_env(pid); image=image_stamp(executable); identity,loader=inherited_identity(env); after=kernel_instance(pid)
 argv2,env2,executable2=service_argv_env(pid); image2=image_stamp(executable2); identity2,loader2=inherited_identity(env2)
 if before!=after or image!=image2 or argv!=argv2 or identity!=identity2 or loader!=loader2 or kernel_instance(pid)!=before: raise ValueError()
 return {'pid':pid,'ppid':before['ppid'],'uid':before['uid'],'boot':before['boot'],'start':before['start'],'image':image,'argvHash':proof_hash(argv),'identityHash':proof_hash(identity),'loader':loader,'hazard':service_hazard(argv,executable),'identity':identity}
def eligible_service(sample): return sample['image']['compiled'] and not sample['loader'] and not sample['hazard']
`;

export interface ServiceProcessSample {
  pid:number; ppid:number; uid:number; boot:string; start:string;
  image:{path:string;device:string;inode:string;size:number;mtime:string;ctime:string;sha256:string;compiled:boolean};
  argvHash:string; identityHash:string; loader:boolean; hazard:boolean;
  identity:Record<string,string|null>;
}
export interface PeerServiceDisposition {
  schema:'peer-service-disposition-v1'; trustBoundary:'trusted-local-artifact-role; in-process extensions are not attested'; nodeId:string; sessionName:string; home:string;
  actor:string; actorGeneration:string; purpose:string; evidencePath:string; evidenceSha256:string;
  approvedArtifactSha256:string; recordedAt:string; process:ServiceProcessSample;
}
const hash=(v:Buffer|string)=>createHash('sha256').update(v).digest('hex');
const exact=(v:unknown,keys:string[]):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v)&&JSON.stringify(Object.keys(v).sort())===JSON.stringify([...keys].sort());
const text=(v:unknown):v is string=>typeof v==='string'&&v.length>0&&v.length<=4096&&!/[\x00-\x1f]/.test(v);
const digest=(v:unknown):v is string=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const absolute=(v:unknown):v is string=>text(v)&&path.isAbsolute(v)&&path.normalize(v)===v;
const integer=(v:unknown)=>Number.isSafeInteger(v)&&(v as number)>=0;
export function validServiceSample(v:unknown):v is ServiceProcessSample {
  if(!exact(v,['pid','ppid','uid','boot','start','image','argvHash','identityHash','loader','hazard','identity']))return false;
  const i=v.image;
  return integer(v.pid)&&(v.pid as number)>1&&integer(v.ppid)&&integer(v.uid)&&v.uid===process.getuid?.()&&digest(v.boot)&&typeof v.start==='string'&&/^\d+$/.test(v.start)
    &&digest(v.argvHash)&&digest(v.identityHash)&&v.loader===false&&v.hazard===false
    &&exact(i,['path','device','inode','size','mtime','ctime','sha256','compiled'])&&absolute(i.path)&&digest(i.sha256)&&i.compiled===true&&integer(i.size)
    &&['device','inode','mtime','ctime'].every(k=>typeof i[k]==='string'&&/^\d+$/.test(i[k] as string))
    &&exact(v.identity,['OPENRIG_NODE_ID','OPENRIG_SESSION_NAME','OPENRIG_HOME','OPENRIG_OCCUPANT_GENERATION','OPENRIG_RUNTIME'])&&Object.values(v.identity).every(x=>x===null||text(x));
}
function validate(v:unknown):asserts v is PeerServiceDisposition {
  if(!exact(v,['schema','trustBoundary','nodeId','sessionName','home','actor','actorGeneration','purpose','evidencePath','evidenceSha256','approvedArtifactSha256','recordedAt','process'])
    ||v.schema!=='peer-service-disposition-v1'||v.trustBoundary!=='trusted-local-artifact-role; in-process extensions are not attested'||!text(v.nodeId)||!text(v.sessionName)||!absolute(v.home)||v.actor!=='operator-agent@kernel'||!text(v.actorGeneration)||!text(v.purpose)
    ||!absolute(v.evidencePath)||!digest(v.evidenceSha256)||!digest(v.approvedArtifactSha256)||!text(v.recordedAt)||!Number.isFinite(Date.parse(v.recordedAt))||!validServiceSample(v.process)
    ||v.process.image.sha256!==v.approvedArtifactSha256||v.process.identity.OPENRIG_NODE_ID!==v.nodeId||v.process.identity.OPENRIG_SESSION_NAME!==v.sessionName||v.process.identity.OPENRIG_HOME!==v.home)throw new Error('peer-service-disposition-invalid');
}
function privateDirectory(dir:string,create=false):void {
  if(create)mkdirSync(dir,{recursive:true,mode:0o700});
  const s=lstatSync(dir);if(!s.isDirectory()||s.isSymbolicLink()||realpathSync(dir)!==dir||s.uid!==process.getuid?.()||(s.mode&0o077))throw new Error('peer-service-private-directory-required');
}
function evidenceHash(file:string):string {
  const s=lstatSync(file);if(!s.isFile()||s.isSymbolicLink()||realpathSync(file)!==file||s.size>1_048_576)throw new Error('peer-service-evidence-invalid');
  const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW);try{return hash(readFileSync(fd));}finally{closeSync(fd);}
}
export async function sampleServiceProcess(pid:number):Promise<ServiceProcessSample>{
  if(!Number.isSafeInteger(pid)||pid<=1)throw new Error('peer-service-pid-invalid');
  const {stdout}=await promisify(execFile)('python3',['-c',SERVICE_PROCESS_PY+'\nprint(json.dumps(sample_process(int(sys.argv[1]))))',String(pid)],{timeout:5000,maxBuffer:16384,encoding:'utf8'});
  const v:unknown=JSON.parse(stdout);if(!validServiceSample(v))throw new Error('peer-service-image-not-eligible');return v;
}
export class PeerServiceDispositionStore {
  readonly root:string;
  constructor(readonly home:string){if(!absolute(home)||realpathSync(home)!==home)throw new Error('peer-service-home-invalid');this.root=path.join(home,'state','peer-service-dispositions');}
  async record(input:Omit<PeerServiceDisposition,'schema'|'trustBoundary'|'recordedAt'|'process'|'home'> & {pid:number},authorize:()=>void,sample=sampleServiceProcess):Promise<PeerServiceDisposition>{
    authorize();const first=await sample(input.pid);
    const {pid:_pid,...fields}=input;const record:PeerServiceDisposition={...fields,schema:'peer-service-disposition-v1',trustBoundary:'trusted-local-artifact-role; in-process extensions are not attested',home:this.home,recordedAt:new Date().toISOString(),process:first};validate(record);
    if(evidenceHash(record.evidencePath)!==record.evidenceSha256)throw new Error('peer-service-evidence-changed');
    const second=await sample(input.pid);if(JSON.stringify(first)!==JSON.stringify(second))throw new Error('peer-service-process-changed');authorize();
    const prior=this.read(record.nodeId,record.sessionName).filter(row=>row.process.pid===first.pid&&row.process.boot===first.boot&&row.process.start===first.start);
    if(prior.length){
      if(prior.length!==1)throw new Error('peer-service-conflicting-records');
      const {recordedAt:_at,...old}=prior[0]!,{recordedAt:_now,...fresh}=record;
      if(JSON.stringify(old)!==JSON.stringify(fresh))throw new Error('peer-service-conflicting-records');
      return prior[0]!;
    }
    privateDirectory(this.root,true);const bytes=Buffer.from(JSON.stringify(record)+'\n');const file=path.join(this.root,hash(bytes)+'.json');const fd=openSync(file,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
    try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}const dir=openSync(this.root,constants.O_RDONLY);try{fsyncSync(dir);}finally{closeSync(dir);}return record;
  }
  read(nodeId:string,sessionName:string):PeerServiceDisposition[]{
    if(!existsSync(this.root))return [];privateDirectory(this.root);const names=readdirSync(this.root);if(names.length>64)throw new Error('peer-service-record-limit');
    const rows:PeerServiceDisposition[]=[];for(const name of names){if(!/^[a-f0-9]{64}\.json$/.test(name))throw new Error('peer-service-record-name-invalid');const file=path.join(this.root,name),s=lstatSync(file);if(!s.isFile()||s.isSymbolicLink()||s.uid!==process.getuid?.()||(s.mode&0o077)||s.size>65536)throw new Error('peer-service-record-private-required');const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW);let b:Buffer;try{b=readFileSync(fd);}finally{closeSync(fd);}if(hash(b)!==name.slice(0,-5))throw new Error('peer-service-record-changed');const row:unknown=JSON.parse(b.toString('utf8'));validate(row);if(row.home!==this.home)throw new Error('peer-service-home-changed');if(row.nodeId===nodeId&&row.sessionName===sessionName){rows.push(row);}}
    return rows;
  }
}
