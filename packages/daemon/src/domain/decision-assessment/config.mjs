import { fail, object } from './contract.mjs';

// Hosting is a trusted deployment contract, independent of wire protocol labels.
export function hostingPolicy(p) {
  const url = new URL(p.endpoint);
  // URL normalizes case/escaped dots, but preserves the DNS root-dot form.
  // Refuse it before origin binding, classification, credentials or transport.
  if (url.hostname.endsWith('.')) fail('invalid_endpoint');
  const known = ['api.typesafe.ai', 'openrouter.ai'];
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  for (const key of ['allowPrivateHosted', 'allowInsecurePrivateHttp']) {
    if (p[key] !== undefined && typeof p[key] !== 'boolean') fail('invalid_boolean_flag');
  }
  const d = p.deployment;
  if (d !== undefined && (!object(d) || !['hosted', 'self-hosted'].includes(d.hosting) ||
      Object.keys(d).some(k => !['hosting', 'approvedOrigins'].includes(k)) ||
      !Array.isArray(d.approvedOrigins) || !d.approvedOrigins.length ||
      d.approvedOrigins.some(o => { try { return typeof o !== 'string' || new URL(o).origin !== o; } catch { return true; } }) ||
      !d.approvedOrigins.includes(url.origin))) fail('invalid_deployment_binding');
  if (p.adapter !== 'laya' || known.includes(url.hostname)) {
    if (d?.hosting === 'self-hosted' || p.adapter === 'laya' && known.includes(url.hostname)) fail('hosted_endpoint_mismatch');
    return { hosted: true };
  }
  if (!loopback && !d) fail('missing_deployment_binding');
  return { hosted: d?.hosting === 'hosted' };
}

export function validateConfig(config) {
  if (!object(config) || config.schemaVersion !== 1 || config.mode !== 'observe' || typeof config.enabled !== 'boolean' || !object(config.providers)) fail('invalid_config');
  if (typeof config.primary !== 'string' || !Object.hasOwn(config.providers, config.primary)) fail('invalid_primary');
  for (const [id, p] of Object.entries(config.providers)) {
    if (!id || !object(p) || !['typesafe', 'openrouter', 'laya'].includes(p.adapter)) fail('unsupported_provider');
    if (typeof p.endpoint !== 'string' || typeof p.model !== 'string' || !p.model.trim() || p.model.length > 256) fail('invalid_provider');
    if (!Array.isArray(p.acceptedModels) || !p.acceptedModels.length || p.acceptedModels.some(x => typeof x !== 'string' || !x || x.length > 256)) fail('invalid_model_identity');
    let url;
    try { url = new URL(p.endpoint); } catch { fail('invalid_endpoint'); }
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) fail('invalid_endpoint');
    if (url.protocol === 'http:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname) && p.allowInsecurePrivateHttp !== true) fail('insecure_endpoint');
    const expectedPath = p.adapter === 'openrouter' ? '/api/alpha/decisions' : '/v1/systemone';
    if (!url.pathname.endsWith(expectedPath)) fail('invalid_protocol_path');
    const { hosted } = hostingPolicy(p);
    if (hosted && url.protocol !== 'https:') fail('insecure_hosted_endpoint');
    if (p.adapter !== 'laya' && (p.adapter === 'typesafe' && url.hostname !== 'api.typesafe.ai' || p.adapter === 'openrouter' && url.hostname !== 'openrouter.ai')) fail('hosted_endpoint_mismatch');
    if (hosted && typeof p.secretRef !== 'string') fail('missing_secret_reference');
    if (p.secretRef !== undefined && (typeof p.secretRef !== 'string' || !/^(env:[A-Z][A-Z0-9_]{0,127}|keychain:[A-Za-z0-9._-]{1,128}\/[A-Za-z0-9._@-]{1,128})$/.test(p.secretRef))) fail('invalid_secret_reference');
    if (!Number.isInteger(p.timeoutMs) || p.timeoutMs < 1 || p.timeoutMs > 60000) fail('invalid_timeout');
    if (!Array.isArray(p.allowedData) || !p.allowedData.length || p.allowedData.some(x => !['public', 'private'].includes(x))) fail('invalid_data_policy');
    if (hosted && p.allowedData.includes('private') && p.allowPrivateHosted !== true) fail('private_hosted_not_authorized');
    const c = p.capabilities;
    if (!object(c) || c.schema !== 'systemone.v1' || !Array.isArray(c.primitives) || !c.primitives.length || c.primitives.some(x => !['choice', 'score', 'noul'].includes(x))) fail('invalid_capabilities');
    for (const [key, max] of [['maxStateChars', 131072], ['maxQuestions', 32], ['maxOptions', 255]]) {
      if (!Number.isInteger(c[key]) || c[key] < 1 || c[key] > max) fail('invalid_capabilities');
    }
    if (!['reported', 'unverified'].includes(c.inputCoverage)) fail('invalid_capabilities');
    if (!Number.isInteger(p.maxResponseBytes) || p.maxResponseBytes < 1024 || p.maxResponseBytes > 1048576) fail('invalid_response_limit');
    if (p.threshold !== undefined && (!object(p.threshold) || !Number.isFinite(p.threshold.minTopProbability) || p.threshold.minTopProbability < 0 || p.threshold.minTopProbability > 1 || typeof p.threshold.calibrationId !== 'string' || !p.threshold.calibrationId.trim())) fail('invalid_calibration');
    // Raw keys and other opaque fields cannot silently become credentials/configuration.
    const allowed = new Set(['adapter', 'endpoint', 'model', 'acceptedModels', 'secretRef', 'timeoutMs', 'allowedData', 'capabilities', 'maxResponseBytes', 'threshold', 'allowPrivateHosted', 'allowInsecurePrivateHttp', 'deployment']);
    if (Object.keys(p).some(key => !allowed.has(key))) fail('unknown_provider_field');
  }
  if (Object.keys(config).some(key => !['schemaVersion', 'mode', 'enabled', 'primary', 'providers'].includes(key))) fail('unknown_config_field');
  return config;
}

export function checkCapabilities(provider, request) {
  const c = provider.capabilities;
  if (!provider.allowedData.includes(request.dataClass)) fail('privacy_policy');
  if (request.state.length > c.maxStateChars || Object.keys(request.questions).length > c.maxQuestions) fail('capability_limit');
  for (const q of Object.values(request.questions)) {
    if (!c.primitives.includes(q.type)) fail('unsupported_primitive');
    if (q.type !== 'noul' && Object.keys(q.criteria).length > c.maxOptions) fail('capability_limit');
  }
}
