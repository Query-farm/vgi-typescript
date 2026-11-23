# Orchard hosted mode

`VGI_ORCHARD_HOSTED=1` runs the worker as an Orchard *Field Worker*. The hosted
base image sets it; you do not set it per deployment. Callers present a *Pass*:
a short-lived [RFC 9068](https://www.rfc-editor.org/rfc/rfc9068) JWT access
token issued by the Orchard Front Desk for one canonical Desk Address. The
worker verifies every Pass locally and accepts no other credential. The full
design is in the extension repository's `docs/location-delegation.md`; the
reference implementation is vgi-python's `vgi/hosted.py`.

Hosted mode is HTTP only and works on every runtime the HTTP entry points do:
Bun (`serveVgiWorker`), Node (`createVgiWorkerFetch` behind your own server) and
Cloudflare Workers (`createVgiFetch`). Passes are verified with
[`jose`](https://github.com/panva/jose) on Web Crypto (ES256 is ECDSA P-256);
the hosted path imports nothing from `node:*`.

## Turning it on

With `serveVgiWorker` (or `createVgiWorkerFetch`), the environment is all it
takes. The worker code does not change:

```bash
VGI_ORCHARD_HOSTED=1 \
VGI_JWT_ISSUER=https://auth.queryfarm.cloud \
VGI_JWT_AUDIENCE=https://desk.queryfarm.cloud/d/dep_123 \
VGI_JWT_JWKS_URI=https://auth.queryfarm.cloud/.well-known/jwks.json \
VGI_SIGNING_KEY=$(openssl rand -hex 32) \
bun run serve.ts
```

On Cloudflare Workers, where the environment arrives with each request, pass
it in yourself:

```ts
import { createVgiFetch, orchardHostedFromEnv } from "@query-farm/vgi/worker-cf";

let handler: ((req: Request) => Promise<Response>) | undefined;
export default {
  fetch(req: Request, env: Env) {
    handler ??= createVgiFetch({
      protocol: { registry, catalogInterface },
      signingKey,                              // a stable 32-byte secret
      prefix: "",
      landingInfo: { name: "demo", doc: "...", version: "1.0.0" },
      orchardHosted: orchardHostedFromEnv(env), // undefined unless VGI_ORCHARD_HOSTED is on
    });
    return handler(req);
  },
};
```

## Startup fails closed

The worker refuses to start (`serveVgiWorker` and `Worker.run` print
`Error: VGI_ORCHARD_HOSTED=1 (Orchard hosted mode): ...` and exit 1;
`createVgiFetch` / `createVgiWorkerFetch` throw `OrchardHostedConfigError`)
unless all three of `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` and
`VGI_JWT_JWKS_URI` are set. Unset and empty are both errors. Each takes exactly
one value: a value containing a comma is refused. It also refuses when:

- `VGI_SIGNING_KEY` is not set (or `VGI_SIGNING_KEY_MINTED` says a parent
  process generated it), or is shorter than 32 bytes. Every machine of a
  deployment must share one stable key, or each restart breaks the sessions
  DuckDB holds;
- `VGI_BEARER_TOKENS`, `VGI_RPC_GRANT_KEYS`, `--grant-key` or a `grantKeys`
  option configures another credential;
- the worker supplies `resolveToken` or `mintGrant`;
- `VGI_OAUTH_RESOURCE` is set (a Field Worker never advertises
  `resource_metadata`);
- `VGI_PROXY_PROOF_MODE` is anything but `off`, or peer identity is configured
  (`irohBridge`, `peerIdentityProviders`, `peerAuthenticationPolicy`);
- a custom `authenticate` callback is passed;
- the worker is served over a transport that carries no Pass: `Worker.run()`
  (stdio, `--unix`, `--tcp`, `--iroh-raw-upstream`) or `Worker.serveStream()`;
- `VGI_JWT_ALGORITHMS` names `none` or an `HS*` algorithm;
- `VGI_ORCHARD_HOSTED` has a value other than
  `1`/`0`/`true`/`false`/`yes`/`no`/`on`/`off`.

## Verification

- algorithm `ES256` only (`VGI_JWT_ALGORITHMS` overrides; `none` and `HS*` are
  always refused);
- JOSE header `typ` must be `at+jwt` (case-insensitive, `application/` prefix
  optional);
- `iss` must match exactly, and `aud` must be exactly one value (a string or a
  one-element list) matching exactly;
- `exp` is required, and `exp` / `nbf` are checked with 60 s of clock skew;
- the JWKS is cached for at most 10 minutes, or less if its
  `Cache-Control: max-age` is shorter. When that time is up the keys are
  discarded. If the re-fetch fails, requests get a 503 with `Retry-After` until
  it succeeds; stale keys are never used;
- a Pass with an unknown `kid` triggers a JWKS re-fetch at most once every 60
  seconds. Between re-fetches it is simply rejected.

## Every request needs a Pass

The only exceptions are `HEAD /health` (the Front Desk's liveness probe) and
CORS preflights (`OPTIONS` with `Access-Control-Request-Method`), which
browsers send without credentials. `GET /health`, a plain `OPTIONS`, the
landing page, `/vgi-client.js`, the describe page and unknown paths all need a
Pass. A request without a valid one gets:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer error="invalid_token"
VGI-Auth-Reason: missing_credential | invalid_credential | expired_credential
VGI-Supported-Encodings: ...        (and the other VGI capability headers)
```

There is no `resource_metadata` in the challenge. If the JWKS cannot be
fetched the answer is 503 with `Retry-After` instead: the worker cannot tell
whether the Pass is valid, so it does not claim that it isn't. Authentication
runs before the request body is read, size-checked or decompressed. CORS is
open (`Access-Control-Allow-Origin: *`, whatever `corsOrigins` says), because
the only credential is the bearer Pass. The `Authorization` header is never
logged; with `VGI_DEBUG=1` a rejection logs only its reason code.

## The blob vendor and the request cap

A hosted worker reads the blob-vendor environment like any HTTP worker
(`VGI_BLOB_VENDOR_URL`, `VGI_BLOB_VENDOR_TOKEN`,
`VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`, ...): results over the threshold become
vendor pointers, `__upload_url__` grants come from the vendor, and
`VGI-Arguments-Ref-Bytes` is advertised. Every request has passed the Pass
gate, so a grant's `ref` is sealed for the Pass principal (domain `orchard`,
principal `sub`) and opens for no one else.

A hosted worker always has a request cap: `VGI_RPC_MAX_REQUEST_BYTES` (or
`maxRequestBytes`), else 32 MiB (vgi-python's
`HOSTED_DEFAULT_MAX_REQUEST_BYTES`), advertised as `VGI-Max-Request-Bytes`.
With the vendor, a larger request goes up through an upload URL.

## Claims in function code

Function code sees the Pass through the usual `info.auth` (`AuthContext`), with
the claims in their JSON types (`email_verified` is a `boolean`, `exp` a
`number`). `auth.principal` is `sub` and `auth.domain` is `"orchard"`.

| Claim | Meaning |
|-------|---------|
| `sub` | Users: the WorkOS user id. Workloads: an opaque Registry id |
| `kind` | `user` or `workload` |
| `email`, `email_verified` | Users only |
| `name` | The user's name, or the name an admin gave the Workload |
| `org_id`, `org_name` | The workspace that owns the deployment |
| `federated_iss`, `federated_sub` | Workloads signed in through a Trust Rule only |
| `client_id`, `iat`, `nbf`, `exp`, `jti` | Standard token claims |

A valid Pass means "a member, or a Workload, of the workspace that owns this
deployment". Anything finer is the worker's own decision. Code that authorizes
by `sub` or `name` must also check `kind`, because a Workload's id and name
could look like a user's.

```ts
const whoami = defineScalarFunction({
  name: "whoami",
  description: "The calling user's email",
  params: { x: int64() },
  returns: utf8(),
  compute: (batch, _consts, info) => {
    const { kind, email } = info.auth.claims;
    const who = kind === "user" ? String(email) : `workload:${info.auth.principal}`;
    return Array.from({ length: batch.numRows }, () => who);
  },
});
```

## Fixture worker

`bin/vgi-example-orchard-hosted-worker` (`examples/orchard-hosted-worker.ts`)
is the hosted-mode fixture: run it with the variables above and it serves the
catalog `orchard` with `pass_claims(x)`, which returns
`{"claims": {...}, "domain": "orchard", "principal": "<sub>"}` as JSON (keys
sorted). It prints `PORT:<n>` once listening (`PORT` and `HOST` choose the
bind; default `127.0.0.1:0`). `src/__tests__/orchard-hosted.test.ts` drives it,
and `scripts/hosted-node-smoke.mjs` runs the gate under plain Node.

`ci/run-orchard-hosted-conformance.sh` runs vgi's SDK-neutral harness
(`test/hosted/run_hosted_conformance.py`, vgi >= `fce5798`) against it, with
every non-HTTP transport as a `--refuse-cmd`:

```sh
VGI_SRC=../vgi ci/run-orchard-hosted-conformance.sh
```

All rules pass, including the opt-in `--streaming-body` ones. A request
declaring a body over the cap gets its answer before any of the body is sent:
401 from the gate, or, for a declared length above `Bun.serve`'s
`maxRequestBodySize` (the 64 GiB rule), an immediate 413 from Bun itself.
