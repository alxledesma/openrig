import { createHash } from 'node:crypto';

export const SCHEMA = 'assessment.v1';
export class AssessmentError extends Error {
  constructor(code) { super(code); this.code = code; }
}
export const fail = (code) => { throw new AssessmentError(code); };
export const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
export const digest = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
export const probability = (value) => Number.isFinite(value) && value >= 0 && value <= 1;

export function validateRequest(request) {
  if (!object(request) || request.schema !== SCHEMA || !object(request.context) || !object(request.questions)) fail('invalid_request');
  const c = request.context;
  for (const field of ['qitemId', 'operationId', 'generation', 'currentGeneration', 'stateRevision', 'currentStateRevision']) {
    if (typeof c[field] !== 'string' || !c[field].trim() || c[field].length > 256) fail('invalid_context');
  }
  if (typeof c.authorized !== 'boolean' || typeof c.deterministicEvidenceSufficient !== 'boolean') fail('invalid_context');
  if (!['public', 'private'].includes(request.dataClass) || typeof request.state !== 'string' || !request.state.trim()) fail('invalid_state');
  if (Buffer.byteLength(request.state) > 131072) fail('request_too_large');
  if (typeof request.rubricId !== 'string' || !request.rubricId.trim() || request.rubricId.length > 256) fail('invalid_rubric');
  const entries = Object.entries(request.questions);
  if (entries.length < 1 || entries.length > 32) fail('invalid_questions');
  for (const [id, q] of entries) {
    if (!id || id.length > 100 || !object(q) || typeof q.instructions !== 'string' || !q.instructions.trim() || q.instructions.length > 4096) fail('invalid_questions');
    if (q.type === 'choice') {
      if (!object(q.criteria) || Object.keys(q.criteria).length < 2 || Object.keys(q.criteria).length > 255) fail('invalid_choice');
      for (const [key, description] of Object.entries(q.criteria)) {
        if (!key || key.length > 200 || !(description === null || typeof description === 'string' && description.length <= 2048)) fail('invalid_choice');
      }
    } else if (q.type === 'score') {
      if (!Array.isArray(q.criteria) || q.criteria.length < 2 || q.criteria.length > 10 || q.criteria.some(x => typeof x !== 'string' || !x.trim() || x.length > 2048)) fail('invalid_score');
    } else if (q.type === 'noul') {
      if (q.criteria !== undefined && (!object(q.criteria) || Object.keys(q.criteria).length !== 2 || typeof q.criteria.true !== 'string' || typeof q.criteria.false !== 'string' || q.criteria.true.length > 2048 || q.criteria.false.length > 2048)) fail('invalid_noul');
    } else fail('unsupported_question');
  }
  return request;
}

export function deterministicBaseline(context) {
  if (!context.authorized) return { disposition: 'deny', reason: 'unauthorized' };
  if (context.generation !== context.currentGeneration) return { disposition: 'deny', reason: 'stale_generation' };
  if (context.stateRevision !== context.currentStateRevision) return { disposition: 'deny', reason: 'stale_state' };
  return context.deterministicEvidenceSufficient
    ? { disposition: 'continue', reason: 'deterministic_evidence' }
    : { disposition: 'review', reason: 'evidence_required' };
}
