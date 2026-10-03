import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { assess } from './assess.mjs';
import { normalizeResponse } from './normalize.mjs';
import { validateConfig } from './config.mjs';

const clone = value => structuredClone(value);
const questions = {
  cause: { type: 'choice', instructions: 'Which cause is supported?', criteria: { dependency: 'Ordinary recoverable dependency', boundary: 'Explicit protected boundary', unknown: 'Evidence insufficient' } },
  urgent: { type: 'noul', instructions: 'Is the recorded incident urgent?' },
  severity: { type: 'score', instructions: 'Rate impact', criteria: ['Low', 'High'] },
};
const request = {
  schema: 'assessment.v1', rubricId: 'openrig-outcome-v1', dataClass: 'public', state: 'A browser test tool is missing. The Lead can install it under the approved policy.', questions,
  context: { qitemId: 'qitem-fixture', operationId: 'op-fixture', generation: 'generation-a', currentGeneration: 'generation-a', stateRevision: 'revision-a', currentStateRevision: 'revision-a', authorized: true, deterministicEvidenceSufficient: true },
};
const raw = {
  model: 'laya-rl-agent',
  answers: {
    cause: { type: 'choice', choice: 'dependency', probabilities: { dependency: 0.8, boundary: 0.1, unknown: 0.1 }, confidence: 0.4, answer_confidence: 0.8, action: { act_probability: 1 } },
    urgent: { type: 'noul', noul: 0.8 },
    severity: { type: 'score', score: 0.8, legend: { 0: 'Low', 1: 'High' }, probabilities: { 0: 0.2, 1: 0.8 }, confidence: 0.6 },
  }, usage: { input_tokens: 150, output_tokens: 0, truncated: false },
};
const provider = {
  adapter: 'laya', endpoint: 'http://127.0.0.1:18091/v1/systemone', model: 'typed-decisions', acceptedModels: ['laya-rl-agent'], timeoutMs: 100,
  allowedData: ['public', 'private'], maxResponseBytes: 16384,
  capabilities: { schema: 'systemone.v1', primitives: ['choice', 'score', 'noul'], maxStateChars: 1000, maxQuestions: 10, maxOptions: 10, inputCoverage: 'reported' },
};
const config = { schemaVersion: 1, mode: 'observe', enabled: true, primary: 'local', providers: { local: provider } };
const reply = body => new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });

test('normalizes each primitive while separating vendor confidence and excluding act head', () => {
  const n = normalizeResponse(raw, questions, provider.acceptedModels);
  assert.equal(n.answers.cause.topProbability, 0.8);
  assert.ok(Math.abs(n.answers.cause.margin - 0.7) < 1e-12);
  assert.equal(n.answers.cause.providerConfidence, 0.4);
  assert.equal(n.answers.cause.providerAnswerConfidence, 0.8);
  assert.equal(n.answers.urgent.topProbability, 0.8);
  assert.equal(n.answers.severity.score, 0.8);
  assert.equal('action' in n.answers.cause, false);
});

test('all transports serialize same questions, map model names, and keep receipts advisory', async () => {
  for (const [adapter, endpoint, model] of [
    ['laya', provider.endpoint, 'typed-decisions'],
    ['typesafe', 'https://api.typesafe.ai/v1/systemone', 'jev-1.13.0'],
    ['openrouter', 'https://openrouter.ai/api/alpha/decisions', 'typesafe/jev-1.13'],
  ]) {
    const c = clone(config); const p = c.providers.local;
    Object.assign(p, { adapter, endpoint, model, acceptedModels: [model], allowedData: ['public'], secretRef: 'env:TEST_ASSESSMENT_TOKEN' });
    let calls = 0;
    const result = await assess(request, c, { allowPaid: true, secrets: { TEST_ASSESSMENT_TOKEN: 'fixture-secret' }, fetchImpl: async (url, opts) => {
      calls++; assert.equal(url, endpoint); assert.equal(opts.redirect, 'error');
      assert.equal(opts.headers.authorization, 'Bearer fixture-secret');
      assert.deepEqual(JSON.parse(opts.body), { model, state: request.state, questions });
      return reply({ ...raw, model, id: 'gen-example', provider: 'ignored', usage: { ...raw.usage, cost: 0.00001 } });
    } });
    assert.equal(calls, 1); assert.equal(result.status, 'abstained'); assert.equal(result.reason, 'uncalibrated');
    assert.equal(result.baseline.disposition, 'continue'); assert.equal(result.modelAdvisory.grantsAuthority, false);
    assert.deepEqual(result.effects, []); assert.equal(JSON.stringify(result).includes('fixture-secret'), false);
  }
});

test('unauthorized and stale contexts prevent any inference', async () => {
  for (const [patch, reason] of [[{ authorized: false }, 'unauthorized'], [{ generation: 'retired' }, 'stale_generation'], [{ stateRevision: 'old' }, 'stale_state']]) {
    const r = clone(request); Object.assign(r.context, patch);
    const result = await assess(r, config, { fetchImpl: () => assert.fail('network must not run') });
    assert.equal(result.status, 'abstained'); assert.equal(result.reason, reason); assert.equal(result.baseline.disposition, 'deny');
  }
});

test('model confidence, outage and abstention never change deterministic baseline', async () => {
  for (const sufficient of [true, false]) {
    const r = clone(request); r.context.deterministicEvidenceSufficient = sufficient;
    for (const fetchImpl of [async () => reply(raw), async () => { throw new Error('Authorization: secret-value'); }]) {
      const result = await assess(r, config, { fetchImpl });
      assert.equal(result.baseline.disposition, sufficient ? 'continue' : 'review');
      assert.deepEqual(result.effects, []); assert.equal(JSON.stringify(result).includes('secret-value'), false);
    }
  }
});

test('disabled, private hosted, missing credentials and explicit paid opt-in fail before network', async () => {
  const noNetwork = () => assert.fail('network must not run');
  const disabled = clone(config); disabled.enabled = false;
  assert.equal((await assess(request, disabled, { fetchImpl: noNetwork })).reason, 'disabled');
  const hosted = clone(config); Object.assign(hosted.providers.local, { adapter: 'openrouter', endpoint: 'https://openrouter.ai/api/alpha/decisions', model: 'typesafe/jev-1.13', acceptedModels: ['typesafe/jev-1.13'], secretRef: 'env:TEST_TOKEN', allowedData: ['public'] });
  const privateReq = clone(request); privateReq.dataClass = 'private';
  assert.equal((await assess(privateReq, hosted, { fetchImpl: noNetwork })).reason, 'privacy_policy');
  assert.equal((await assess(request, hosted, { fetchImpl: noNetwork })).reason, 'paid_inference_not_enabled');
  assert.equal((await assess(request, hosted, { fetchImpl: noNetwork, allowPaid: true, secrets: {} })).reason, 'secret_unavailable');
});

test('invalid configs, auth literals, new schema majors and unsafe destinations are rejected', () => {
  for (const patch of [{ schemaVersion: 2 }, { mode: 'enforce' }, { enabled: undefined }]) {
    assert.throws(() => validateConfig({ ...clone(config), ...patch }));
  }
  for (const patch of [{ apiKey: 'secret' }, { secretRef: 'literal:secret' }, { endpoint: 'https://secret@example.com/v1/systemone' }, { endpoint: 'http://example.com/v1/systemone' }, { timeoutMs: NaN }, { capabilities: { schema: 'systemone.v2' } }]) {
    const c = clone(config); Object.assign(c.providers.local, patch); assert.throws(() => validateConfig(c));
  }
});

test('Keychain references use fixed service/account and never put keys in receipts', async () => {
  const c = clone(config); c.providers.local.secretRef = 'keychain:openrig.typesafe-jev/example-user';
  let reads = 0;
  const result = await assess(request, c, {
    keychainReader: async ({ service, account }) => { reads++; assert.equal(service, 'openrig.typesafe-jev'); assert.equal(account, 'example-user'); return 'synthetic-keychain-secret'; },
    fetchImpl: async (_, opts) => { assert.equal(opts.headers.authorization, 'Bearer synthetic-keychain-secret'); return reply(raw); },
  });
  assert.equal(reads, 1); assert.equal(result.reason, 'uncalibrated'); assert.equal(JSON.stringify(result).includes('synthetic-keychain-secret'), false);
  const unavailable = await assess(request, c, { keychainReader: async () => { throw new Error('synthetic-keychain-secret'); }, fetchImpl: () => assert.fail('must not fetch') });
  assert.equal(unavailable.reason, 'secret_unavailable'); assert.equal(JSON.stringify(unavailable).includes('synthetic-keychain-secret'), false);
});

test('missing, malformed, inconsistent, nonfinite and unexpected-model responses are rejected', async () => {
  const mutations = [
    r => { delete r.answers.urgent; }, r => { r.answers.cause.choice = 'invented'; },
    r => { r.answers.cause.probabilities.boundary = 0.8; }, r => { r.answers.urgent.noul = NaN; },
    r => { r.answers.severity.score = 0.2; }, r => { r.answers.cause.type = 'noul'; },
    r => { r.model = 'Bearer leaked-secret'; }, r => { r.usage.input_tokens = -1; },
  ];
  for (const mutate of mutations) {
    const r = clone(raw); mutate(r);
    const result = await assess(request, config, { fetchImpl: async () => reply(r) });
    assert.equal(result.status, 'invalid'); assert.equal(JSON.stringify(result).includes('leaked-secret'), false);
  }
});

test('truncation or absent coverage abstains even with configured calibration', async () => {
  const c = clone(config); c.providers.local.threshold = { minTopProbability: 0.7, calibrationId: 'fixture-only' };
  for (const truncated of [true, undefined]) {
    const r = clone(raw); r.usage.truncated = truncated;
    const result = await assess(request, c, { fetchImpl: async () => reply(r) });
    assert.equal(result.status, 'abstained'); assert.equal(result.reason, truncated ? 'evidence_truncated' : 'coverage_unverified');
  }
  const result = await assess(request, c, { fetchImpl: async () => reply(raw) });
  assert.equal(result.status, 'assessed'); assert.equal(result.baseline.disposition, 'continue'); assert.equal(result.modelAdvisory.grantsAuthority, false);
});

test('busy, credits, auth and transport errors are sanitized with no retry or fallback', async () => {
  for (const [status, reason] of [[401, 'provider_auth'], [402, 'provider_credits'], [429, 'provider_busy'], [503, 'provider_busy'], [529, 'provider_busy'], [500, 'provider_error']]) {
    let calls = 0;
    const result = await assess(request, config, { fetchImpl: async () => { calls++; return new Response('secret-error-body', { status }); } });
    assert.equal(result.reason, reason); assert.equal(calls, 1); assert.equal(JSON.stringify(result).includes('secret-error-body'), false);
  }
});

test('real loopback transport reads bounded bodies and timeout includes response body', async () => {
  const sockets = new Set();
  const server = createServer((req, res) => {
    req.resume();
    if (req.url.startsWith('/slow/')) { res.writeHead(200); res.write('{'); return; }
    if (req.url.startsWith('/large/')) { res.writeHead(200); res.end('x'.repeat(2000)); return; }
    res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify(raw));
  });
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  try {
    const c = clone(config); const base = `http://127.0.0.1:${server.address().port}`;
    c.providers.local.endpoint = `${base}/v1/systemone`; c.providers.local.timeoutMs = 1000;
    assert.equal((await assess(request, c)).reason, 'uncalibrated');
    c.providers.local.endpoint = `${base}/large/v1/systemone`; c.providers.local.maxResponseBytes = 1024;
    assert.equal((await assess(request, c)).reason, 'response_too_large');
    c.providers.local.endpoint = `${base}/slow/v1/systemone`; c.providers.local.timeoutMs = 30;
    assert.equal((await assess(request, c)).reason, 'provider_timeout');
  } finally { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); }
});

test('strict authorization flags reject truthy strings before transport or secrets', async () => {
  for (const value of ['false', 'true', 1, 0, null, {}, []]) {
    let calls = 0;
    const result = await assess(request, config, { allowPaid: value, fetchImpl: async () => { calls++; return reply(raw); } });
    assert.equal(result.reason, 'invalid_boolean_flag'); assert.equal(calls, 0);
    for (const flag of ['allowPrivateHosted', 'allowInsecurePrivateHttp']) {
      const c = clone(config); c.providers.local[flag] = value;
      assert.throws(() => validateConfig(c), /invalid_boolean_flag/);
    }
  }
});

test('hosting trust cannot be changed by protocol relabeling; DGX binding remains supported', async () => {
  let calls = 0;
  const fetchImpl = async () => { calls++; return reply(raw); };
  for (const endpoint of ['https://api.typesafe.ai/v1/systemone', 'https://openrouter.ai/v1/systemone']) {
    const c = clone(config); c.providers.local.endpoint = endpoint;
    c.providers.local.deployment = { hosting: 'self-hosted', approvedOrigins: [new URL(endpoint).origin] };
    assert.equal((await assess({ ...request, dataClass: 'private' }, c, { fetchImpl })).reason, 'hosted_endpoint_mismatch');
  }
  const c = clone(config); const p = c.providers.local;
  p.endpoint = 'http://198.51.100.10:18091/v1/systemone'; p.allowInsecurePrivateHttp = true;
  assert.equal((await assess(request, c, { fetchImpl })).reason, 'missing_deployment_binding');
  p.deployment = { hosting: 'self-hosted', approvedOrigins: ['http://198.51.100.10:18091'] };
  assert.equal((await assess(request, c, { fetchImpl })).reason, 'uncalibrated'); assert.equal(calls, 1);
  p.endpoint = 'http://198.51.100.11:18091/v1/systemone';
  assert.equal((await assess(request, c, { fetchImpl })).reason, 'invalid_deployment_binding'); assert.equal(calls, 1);
  p.endpoint = 'https://decisions.example.net/v1/systemone'; p.deployment = { hosting: 'hosted', approvedOrigins: ['https://decisions.example.net'] }; p.secretRef = 'env:TEST_TOKEN'; p.allowPrivateHosted = true;
  assert.equal((await assess(request, c, { fetchImpl })).reason, 'paid_inference_not_enabled'); assert.equal(calls, 1);
});

test('all trusted request and enabled flags require booleans', async () => {
  for (const value of ['false', 'true', null, 1, {}]) {
    const c = clone(config); c.enabled = value;
    assert.equal((await assess(request, c)).reason, 'invalid_config');
    for (const flag of ['authorized', 'deterministicEvidenceSufficient']) {
      const r = clone(request); r.context[flag] = value;
      assert.equal((await assess(r, config)).reason, 'invalid_context');
    }
  }
});

test('known hosted case, root-dot and port forms cannot bypass paid/privacy gates', async () => {
  let calls = 0, secretReads = 0;
  const dependencies = { allowPaid: false, keychainReader: async () => { secretReads++; return 'synthetic-key'; }, fetchImpl: async () => { calls++; return reply(raw); } };
  for (const hostname of ['api.typesafe.ai', 'openrouter.ai']) {
    for (const host of [hostname.toUpperCase(), `${hostname}.`, `${hostname}%2e`, `${hostname}.:443`, `${hostname}.:8443`, `${hostname}:8443`]) {
      const c = clone(config), p = c.providers.local;
      p.endpoint = `https://${host}/v1/systemone`; p.secretRef = 'keychain:fixture/account';
      p.deployment = { hosting: 'self-hosted', approvedOrigins: [new URL(p.endpoint).origin] };
      const result = await assess({ ...request, dataClass: 'private' }, c, dependencies);
      assert.ok(['invalid_endpoint', 'hosted_endpoint_mismatch'].includes(result.reason), `${host}: ${result.reason}`);
    }
    const c = clone(config), p = c.providers.local;
    p.adapter = hostname.startsWith('api.') ? 'typesafe' : 'openrouter';
    p.endpoint = `https://${hostname.toUpperCase()}:8443${p.adapter === 'openrouter' ? '/api/alpha/decisions' : '/v1/systemone'}`;
    p.allowedData = ['public']; p.secretRef = 'keychain:fixture/account';
    assert.equal((await assess(request, c, dependencies)).reason, 'paid_inference_not_enabled');
  }
  assert.equal(calls, 0); assert.equal(secretReads, 0);
});
