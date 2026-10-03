import { AssessmentError, SCHEMA, digest, deterministicBaseline, validateRequest } from './contract.mjs';
import { checkCapabilities, validateConfig } from './config.mjs';
import { fetchDecision } from './http-provider.mjs';
import { normalizeResponse } from './normalize.mjs';

/** Context is supplied by trusted runtime code; this library does not authenticate it. */
export async function assess(request, config, dependencies = {}) {
  const started = Date.now();
  let receipt = { schema: SCHEMA, mode: 'observe', status: 'invalid', reason: 'invalid_request', modelAdvisory: null, effects: [] };
  try {
    validateRequest(request);
    validateConfig(config);
    receipt = { ...receipt, operationId: request.context.operationId, qitemId: request.context.qitemId,
      inputDigest: digest({ state: request.state, context: request.context, dataClass: request.dataClass }),
      rubricId: request.rubricId, rubricDigest: digest(request.questions), baseline: deterministicBaseline(request.context),
      providerId: config.primary, requestedModel: config.providers[config.primary].model };
    if (receipt.baseline.disposition === 'deny') return { ...receipt, status: 'abstained', reason: receipt.baseline.reason, latencyMs: Date.now() - started };
    if (!config.enabled) return { ...receipt, status: 'unavailable', reason: 'disabled', latencyMs: Date.now() - started };
    const provider = config.providers[config.primary];
    checkCapabilities(provider, request);
    const raw = await fetchDecision(provider, request, dependencies);
    const normalized = normalizeResponse(raw, request.questions, provider.acceptedModels);
    const hasThreshold = !!provider.threshold;
    const belowThreshold = hasThreshold && Object.values(normalized.answers).some(a => a.topProbability < provider.threshold.minTopProbability);
    // Limits are declared, not tokenizer-proven. Unverified coverage cannot yield qualified advice.
    const coverage = normalized.inputCoverage;
    const covered = provider.capabilities.inputCoverage === 'reported' && coverage === 'reported_complete';
    const abstain = belowThreshold || !hasThreshold || !covered;
    return { ...receipt, status: abstain ? 'abstained' : 'assessed', reason: coverage === 'truncated' ? 'evidence_truncated' : belowThreshold ? 'low_probability' : !hasThreshold ? 'uncalibrated' : !covered ? 'coverage_unverified' : 'qualified_observation',
      model: normalized.model, answers: normalized.answers, usage: normalized.usage, inputCoverage: coverage,
      calibrationId: provider.threshold?.calibrationId ?? null,
      modelAdvisory: { kind: 'semantic_observation', qualified: !abstain, grantsAuthority: false }, latencyMs: Date.now() - started };
  } catch (error) {
    const reason = error instanceof AssessmentError ? error.code : 'assessment_error';
    const invalid = reason.startsWith('invalid_') || reason.startsWith('unknown_') || reason === 'answer_type_mismatch' || reason === 'response_too_large' || reason === 'request_too_large';
    return { ...receipt, status: invalid ? 'invalid' : 'unavailable', reason, latencyMs: Date.now() - started };
  }
}
