# Durable outcome assessment evidence

Finished runtime assessment receipts retain a fixed-schema, sanitized `assessmentEvidence` record. It includes the normalized outcome choice and probabilities, bounded confidence metrics, validated usage counters/cost, latency, coverage label, rubric/input/provider-configuration digests and model/provider provenance. It never stores raw HTTP payloads, errors, prompts, credentials or work-body text in this evidence record. Original queue bodies remain under their existing custody/privacy controls.

`jobBinding` preserves the original operation, worker/generation, worker configuration digest, policy revision, input digest, created/started times and observed state/holder binding. The adapter's assessment input digest is separate from the terminal-return input digest.

`applied` means the current, unchanged response classified an incomplete outcome. It does not grant authority or acceptance. Stale responses retain diagnostic evidence with `suppressed:true`, `applied:false`; unavailable responses have no fabricated outcome or usage. Coverage remains exactly reported, unverified or truncated. This record does not establish calibration, qualification, completeness or admission.

The allowlist caps strings at 256 characters, permits only finite bounded probabilities, exact three-option distributions and safe nonnegative counters, and drops unknown fields. Enforcement transitions are unchanged. This is source-only auditability; runtime adoption and live model evidence remain separate release checks.
