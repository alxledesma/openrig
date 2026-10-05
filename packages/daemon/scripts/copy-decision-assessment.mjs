import {mkdirSync,copyFileSync} from 'node:fs';
import {join} from 'node:path';
const source=join(process.cwd(),'src/domain/decision-assessment'),target=join(process.cwd(),'dist/domain/decision-assessment');
mkdirSync(target,{recursive:true});
for(const name of ['assess','config','contract','http-provider','normalize','secrets'])copyFileSync(join(source,name+'.mjs'),join(target,name+'.mjs'));

mkdirSync(join(process.cwd(),"dist/adapters"),{recursive:true});
copyFileSync(join(process.cwd(),"assets/pi-bounded-compaction-extension.ts"),join(process.cwd(),"dist/adapters/pi-bounded-compaction-extension.ts"));
