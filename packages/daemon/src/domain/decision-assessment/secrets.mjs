import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fail } from './contract.mjs';

const execFileAsync = promisify(execFile);

export async function resolveSecret(ref, { secrets = process.env, signal, keychainReader } = {}) {
  let value;
  if (ref.startsWith('env:')) value = secrets[ref.slice(4)];
  else if (/^keychain:[A-Za-z0-9._-]+\/[A-Za-z0-9._@-]+$/.test(ref)) {
    const [service, account] = ref.slice(9).split('/');
    try {
      if (keychainReader) value = await keychainReader({ service, account, signal });
      else {
        const { stdout } = await execFileAsync('/usr/bin/security', ['find-generic-password', '-s', service, '-a', account, '-w'], {
          signal, timeout: 2000, maxBuffer: 65536,
        });
        value = stdout.replace(/\r?\n$/, '');
      }
    } catch { fail('secret_unavailable'); }
  } else fail('invalid_secret_reference');
  if (typeof value !== 'string' || !value || /[\r\n]/.test(value)) fail('secret_unavailable');
  return value;
}
