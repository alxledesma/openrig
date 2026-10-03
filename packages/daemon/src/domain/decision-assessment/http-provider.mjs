import { AssessmentError, fail } from './contract.mjs';
import { hostingPolicy } from './config.mjs';
import { resolveSecret } from './secrets.mjs';

async function readBounded(response, maximum) {
  const length = response.headers.get('content-length');
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > maximum)) fail('response_too_large');
  if (!response.body) fail('empty_response');
  const reader = response.body.getReader();
  const parts = [];
  let count = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      count += value.byteLength;
      if (count > maximum) fail('response_too_large');
      parts.push(Buffer.from(value));
    }
  } finally { await reader.cancel().catch(() => {}); }
  try { return JSON.parse(Buffer.concat(parts).toString('utf8')); } catch { fail('invalid_json_response'); }
}

export async function fetchDecision(provider, request, { fetchImpl = fetch, secrets = process.env, allowPaid = false, keychainReader } = {}) {
  if (typeof allowPaid !== 'boolean') fail('invalid_boolean_flag');
  const { hosted } = hostingPolicy(provider);
  if (hosted && allowPaid !== true) fail('paid_inference_not_enabled');
  if (hosted && request.dataClass === 'private' && provider.allowPrivateHosted !== true) fail('private_hosted_not_authorized');
  const headers = { 'content-type': 'application/json' };
  const controller = new AbortController();
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => { controller.abort(); reject(new AssessmentError('provider_timeout')); }, provider.timeoutMs);
  });
  const operation = (async () => {
    if (provider.secretRef) headers.authorization = `Bearer ${await resolveSecret(provider.secretRef, { secrets, signal: controller.signal, keychainReader })}`;
    const response = await fetchImpl(provider.endpoint, {
      method: 'POST', redirect: 'error', signal: controller.signal, headers,
      body: JSON.stringify({ model: provider.model, state: request.state, questions: request.questions }),
    });
    if (!response.ok) fail(response.status === 401 || response.status === 403 ? 'provider_auth' : response.status === 402 ? 'provider_credits' : response.status === 429 || response.status === 503 || response.status === 529 ? 'provider_busy' : 'provider_error');
    return readBounded(response, provider.maxResponseBytes);
  })();
  try { return await Promise.race([operation, timeout]); }
  catch (error) {
    if (error instanceof AssessmentError) throw error;
    fail(controller.signal.aborted ? 'provider_timeout' : 'provider_transport');
  } finally { clearTimeout(timer); controller.abort(); }
}
