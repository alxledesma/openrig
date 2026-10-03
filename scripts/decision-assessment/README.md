# OpenRig decision assessment MVP

Standalone observation library and CLI. It does not register daemon middleware, write queues, dispatch agents, accept work, install models or change permissions. Node 22 built-ins only; no dependency installation required. The accepted OpenRig baseline used for interface research is `22a2f4c9d5386d06f9d7ab21f01434a04601be38`.

## Run and test

```sh
node --test scripts/decision-assessment/assessment.test.mjs
node scripts/decision-assessment/cli.mjs --run --config scripts/decision-assessment/config.example.json --input scripts/decision-assessment/request.example.json
```

The example config is disabled, so the second command returns a `disabled` receipt without network activity and exits 3. No network operation occurs just by importing modules or invoking the CLI without `--run`. Hosted inference additionally requires `--allow-paid` (or `allowPaid: true` in the library call). No automatic retry/fallback occurs. The example local port is illustrative; it does not establish an installed service.

Credentials are **references**, such as `env:OPENROUTER_API_KEY` or `keychain:openrig.typesafe-jev/example-user`, resolved only at request time. Keychain references invoke the fixed macOS `security find-generic-password` command privately; store credentials separately under the installation owner’s authorization. The example account is a placeholder. Do not put a secret value in config, command arguments, request state, tests or receipts. An authorized caller can inject a narrowly scoped `secrets` mapping into `assess`; the library does not require importing the full environment. Errors return fixed codes, never provider error bodies or thrown network messages. HTTPS is required for hosted endpoints. HTTP to non-loopback LAYA needs explicit `allowInsecurePrivateHttp: true` and a separately secured private transport. Redirects are rejected.

## Public interface

```js
import { assess } from './assess.mjs';
const receipt = await assess(request, config, {
  allowPaid: false,
  secrets: {},
  // fetchImpl: injected fetch compatible transport for contract tests
});
```

Request `schema` is `assessment.v1`; config `schemaVersion` is 1. Config supports only `mode: observe`. `context` is a **trusted caller supplied snapshot**, not authenticated by this library: qitem/operation ID, carried and current generation, expected and current state revision, authorized flag, and deterministic evidence sufficiency. Runtime integration must establish those facts through actual custody/authority checks. Supplying `authorized: true` in a user file establishes no OpenRig authority. No receipt is a commit authorization; future consumers must recheck state, expiry, assignment/evidence digests and caller provenance inside their transaction.

The deterministic baseline reports `deny`, `continue` or `review`, independently of the model. Unauthorized/stale snapshots return without inference. A real credential/resource/Owner boundary belongs in the trusted context or deterministic policy; models cannot create or remove it. `review` names an evidence requirement, not an automatic Owner gate.

All transports send `{model,state,questions}`. `laya` and `typesafe` use `/v1/systemone`; `openrouter` uses `/api/alpha/decisions`. Models and exact `acceptedModels` are configured per provider: the returned identity must match the allowlist before it appears in a receipt. Official OpenRouter documentation shows a resolved dated model ID; this MVP does not discover or automatically trust an unexpected model. Change the allowlist only after verifying the identity. Prefix proxies may precede the LAYA protocol path. Current author LAYA returns generic `laya-rl-agent`; this model field alone does not attest the typed checkpoint. A future integration must bind authenticated service health/routing and pinned checkpoint digests to the receipt. The example generic allowlist proves protocol identity only.

Supported primitives are `choice`, `score`, `noul`. This MVP supports a deliberately narrower portable request subset: string state/instructions/descriptions, 2–255 choice options, 2–10 score levels, up to 32 questions, and optional string true/false Noul criteria. Provider capability limits can be stricter. Object/array state and structured descriptions from provider APIs are not implemented here. Response validation checks exact question coverage/type, exact distribution keys and unit sum, chosen label probability, score expectation and legend, finite probabilities/usage and pinned model identity. Additive provider metadata is ignored.

Receipts report `assessed`, `abstained`, `unavailable` or `invalid`, `effects: []`, and model advice explicitly marked `grantsAuthority: false`. They include input/context and rubric hashes, provider/model identities, usage/cost when supplied and deterministic baseline; they omit input text and secret values. Semantic option labels can appear as the caller-defined answer keys: do not put credentials in labels. `status: assessed` means a configured calibration/coverage rule passed; it is not independent acceptance or permission.

`providerConfidence` and `providerAnswerConfidence` preserve differing vendor semantics; `topProbability` and `margin` come from validated distributions. LAYA's `action.act_probability` is excluded. A configured threshold has a calibration ID; this is a reference to independently established evaluation evidence, not proof that calibration happened. Example files intentionally contain no threshold. Every successful uncalibrated request abstains while retaining answers for evaluation.

Coverage is conservative: current LAYA can return boolean `usage.truncated`; true abstains, absent remains unverified. A provider declaring `inputCoverage: reported` cannot qualify a receipt unless the response actually says untruncated. Hosted Jev responses without that field retain unverified coverage even below published context limits; this initial MVP abstains. No tokenizer or evidence compressor is bundled. Character caps are transport bounds, not token-count proof. A later compatible adapter can add measured tokenizer/capability evidence; never fabricate a coverage field for qualification.

## Failure and privacy behavior

Total per-request timeout includes connect/headers and the full bounded body. Responses are capped incrementally, with content-length checked early. Auth/credits/busy/transport failures return unavailable and preserve the baseline. CLI exit codes: 0 for assessed/abstained, 2 for invalid input/response, 3 for unavailable. These codes describe the assessment service, not a work-completion gate.

`allowedData` is enforced before transport. Hosted private state additionally needs `allowPrivateHosted: true` in trusted config. Data class is supplied by the runtime; the library does not determine whether text contains private material or redact it. Redaction, approved endpoints, retention agreements, authentication and dedicated credential handling remain integration responsibilities. No hosted service is a mandatory fallback.

## Verification and later integration

Contract tests use saved synthetic responses and a real loopback HTTP server, no external inference. They prove all three payload transports, model identity rejection, confidence separation, stale/unauthorized denial, privacy/cost opt-in, secret redaction, malformed response rejection, coverage/calibration abstention, finite timeout through stalled body, and response size rejection. Tests do not establish provider quality, hosted access, DGX support, semantic calibration or seamless OpenRig upgrades.

Dependency graph: contract/transport library -> explicitly authorized bounded live public OpenRouter test; contract/transport library -> event observer; hardware/access/capacity reconciliation -> LAYA qualification; accepted observer/provider/corpus plus independent review -> later common runtime seam. The installation owner controls integration. The exclusive package is this directory and its tests/docs; daemon, coordinator, handover and reservation code are outside it. Application rollout is outside this MVP.

The proposed common seam is documented in [the fork architecture note](../../docs/reference/fork-coordination-and-decisions.md). The coordination controller confirmed it remains independent of model availability; native activity is availability evidence, and unavailable/abstained receipts must not become a global hold or fake acceptance. This MVP does not yet subscribe to runtime SSE or consume actual outcome records.

## Deployment trust boundary (paid/privacy repair)

Wire adapter (`laya`, `typesafe`, `openrouter`) describes the protocol, not who
operates the endpoint. Known TypeSafe/OpenRouter authorities cannot be labeled
self-hosted or disguised with a LAYA adapter. Hosted transports require boolean
`allowPaid: true`; private hosted data additionally requires boolean
`allowPrivateHosted: true`. Strings, numbers, null and objects are invalid flags.
`enabled` and trusted request context flags also require actual booleans.

Loopback LAYA retains its local default. Every non-loopback LAYA destination must
have a trusted deployment declaration bound to its exact origin, for example:

```json
"deployment": {
  "hosting": "self-hosted",
  "approvedOrigins": ["http://198.51.100.10:18091"]
},
"allowInsecurePrivateHttp": true
```

This supports an owner-approved DGX endpoint without treating arbitrary LAYA
labels as local. For a third-party compatible service use `hosting: "hosted"`,
HTTPS, a secret reference and the paid/private opt-ins. Approved origins include
scheme and port, contain no path/credentials/query, and must exactly match the
configured endpoint origin. Transport redirects remain prohibited. Declarations
are trusted operator configuration: this adapter cannot attest ownership, DNS,
loaded artifacts or retention policy. Provisioning must verify those facts before
approving a self-hosted origin; changing the destination requires updating and
reviewing the binding. No automatic endpoint discovery or provider fallback.

Terminal DNS root-dot endpoints (including escaped dots normalized by URL parsing)
are rejected rather than interpreted as a distinct deployment. Hostname case is
normalized by URL parsing; alternate ports cannot convert a known hosted
hostname into a self-hosted one. Exact origin approval still binds scheme/port.
