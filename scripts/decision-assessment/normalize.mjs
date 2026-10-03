import { fail, object, probability } from './contract.mjs';

function distribution(raw, keys) {
  if (!object(raw) || Object.keys(raw).length !== keys.length || keys.some(k => !Object.hasOwn(raw, k) || !probability(raw[k]))) fail('invalid_distribution');
  const sum = keys.reduce((n, k) => n + raw[k], 0);
  if (Math.abs(sum - 1) > 0.0001) fail('invalid_distribution');
  return Object.fromEntries(keys.map(k => [k, raw[k] / sum]));
}

export function normalizeResponse(raw, questions, acceptedModels) {
  if (!object(raw) || typeof raw.model !== 'string' || !raw.model.trim() || raw.model.length > 256 || !object(raw.answers) || Object.keys(raw.answers).length !== Object.keys(questions).length || !object(raw.usage)) fail('invalid_response');
  if (!acceptedModels.includes(raw.model)) fail('invalid_model_identity');
  const usage = {};
  for (const name of ['input_tokens', 'output_tokens']) {
    if (!Number.isSafeInteger(raw.usage[name]) || raw.usage[name] < 0) fail('invalid_usage');
    usage[name] = raw.usage[name];
  }
  if (raw.usage.cost !== undefined) {
    if (!Number.isFinite(raw.usage.cost) || raw.usage.cost < 0) fail('invalid_usage');
    usage.cost = raw.usage.cost;
  }
  const answers = Object.create(null);
  for (const [id, q] of Object.entries(questions)) {
    const a = raw.answers[id];
    if (!object(a) || a.type !== q.type) fail('answer_type_mismatch');
    let probabilities;
    if (q.type === 'noul') {
      if (!probability(a.noul)) fail('invalid_noul_answer');
      probabilities = { false: 1 - a.noul, true: a.noul };
    } else if (q.type === 'choice') {
      const keys = Object.keys(q.criteria);
      probabilities = distribution(a.probabilities, keys);
      if (!keys.includes(a.choice) || probabilities[a.choice] < Math.max(...Object.values(probabilities)) - 1e-8) fail('invalid_choice_answer');
    } else {
      const keys = q.criteria.map((_, i) => String(i));
      probabilities = distribution(a.probabilities, keys);
      if (!object(a.legend) || Object.keys(a.legend).length !== keys.length || keys.some((k, i) => a.legend[k] !== q.criteria[i])) fail('invalid_legend');
      const expected = keys.reduce((sum, k) => sum + Number(k) * probabilities[k], 0);
      if (!Number.isFinite(a.score) || Math.abs(a.score - expected) > 0.0001) fail('invalid_score_answer');
    }
    const ranked = Object.values(probabilities).sort((a, b) => b - a);
    const answer = { type: q.type, probabilities, topProbability: ranked[0], margin: ranked[0] - ranked[1] };
    if (q.type === 'choice') answer.choice = a.choice;
    if (q.type === 'score') answer.score = a.score;
    if (q.type === 'noul') answer.noul = a.noul;
    for (const key of ['confidence', 'answer_confidence']) {
      if (a[key] !== undefined) {
        if (!probability(a[key])) fail('invalid_confidence');
        answer[key === 'confidence' ? 'providerConfidence' : 'providerAnswerConfidence'] = a[key];
      }
    }
    // action.act_probability is deliberately excluded: it supplies no authority.
    answers[id] = answer;
  }
  let inputCoverage = 'unverified';
  if (raw.usage.truncated !== undefined) {
    if (typeof raw.usage.truncated !== 'boolean') fail('invalid_coverage');
    inputCoverage = raw.usage.truncated ? 'truncated' : 'reported_complete';
  }
  return { model: raw.model, answers, usage, inputCoverage };
}
