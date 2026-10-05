import {readFileSync,realpathSync,statSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';
/** Read bytes only. Operator evidence cannot introduce a different executable,
 * arbitrary shell text, PATH lookup, or a provider/tool invocation. */
export function administrativeCliEvidence(cliPath?:string):{cliPath:string;cliSha256:string} {
 const candidates=[new URL('../../../dist/bin-wrapper.js',import.meta.url),new URL('../../dist/bin-wrapper.js',import.meta.url),new URL('../../../cli/dist/bin-wrapper.js',import.meta.url)];
 for(const candidate of candidates){try{const path=realpathSync(fileURLToPath(candidate)),consumer=join(dirname(path),'commands','queue.js');if(!statSync(path).isFile()||!(statSync(path).mode&0o111))continue;const bytes=readFileSync(consumer);if(!bytes.includes(Buffer.from('outbox-abandon-uncertain')))continue;const cliSha256=createHash('sha256').update(readFileSync(path)).update('\0').update(bytes).digest('hex');if(cliPath&&realpathSync(cliPath)!==path)continue;return {cliPath:path,cliSha256};}catch{}}
 throw new Error('Only this daemon deployment current executable CLI bin-wrapper.js and consumer module are supported; arbitrary wrappers/commands are refused');
}
