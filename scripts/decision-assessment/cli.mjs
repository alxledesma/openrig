import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { assess } from './assess.mjs';

export async function main(argv = process.argv.slice(2)) {
  // No implicit run, network or provider fallback. Inputs are saved files, never secrets as args.
  if (!argv.includes('--run') || !argv.includes('--config') || !argv.includes('--input')) {
    process.stderr.write('Usage: node cli.mjs --run --config <file> --input <file> [--allow-paid]\n');
    return 2;
  }
  const allowed = new Set(['--run', '--config', '--input', '--allow-paid']);
  const values = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!allowed.has(arg) || Object.hasOwn(values, arg)) return 2;
    if (arg === '--config' || arg === '--input') {
      if (!argv[i + 1] || argv[i + 1].startsWith('--')) return 2;
      values[arg] = argv[++i];
    } else values[arg] = true;
  }
  try {
    const [config, request] = await Promise.all([values['--config'], values['--input']].map(async p => JSON.parse(await readFile(p, 'utf8'))));
    const receipt = await assess(request, config, { allowPaid: values['--allow-paid'] === true });
    process.stdout.write(`${JSON.stringify(receipt)}\n`);
    return receipt.status === 'invalid' ? 2 : receipt.status === 'unavailable' ? 3 : 0;
  } catch { process.stderr.write('Unable to read assessment inputs.\n'); return 2; }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) process.exitCode = await main();
