// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Test-only OPTIONAL bearer authentication for the example HTTP workers.
//
// The integration suite points every HTTP test at ONE long-lived example
// worker, and almost all of those tests are anonymous. A handful are not:
// `cache/identity_isolation.test` attaches the same worker twice, as `alice`
// and as `bob`, and asserts the result cache never cross-serves between them.
// That needs the worker to resolve a bearer token to a principal — but it must
// never 401, or every anonymous test on the shared server breaks (a failure
// whose error text contains "HTTP", which sqllogictest turns into a silent
// skip).
//
// So: no header / blank / unknown token → anonymous, exactly as before; a known
// token → its principal. Ports the same fixture the other SDKs ship —
// vgi-python's `_test_fixtures/http_server.py`, vgi-go's
// `cmd/vgi-example-worker/auth.go`, vgi-rust's `VGI_OPTIONAL_BEARER_TOKENS`.
//
// This lives in `examples/`, not `src/`: it is fixture wiring, not framework.
// The published package exposes only the generic `authenticate` option this
// hands to `serveVgiWorker`.

import { AuthContext } from "@query-farm/vgi-rpc";

/** The tokens the shared integration suite sends, and the principals the
 *  assertions expect. Same pair in every SDK's example worker. */
const DEFAULT_TEST_TOKENS: ReadonlyArray<readonly [string, string]> = [
  ["vgi-test-alice", "alice"],
  ["vgi-test-bob", "bob"],
];

/**
 * Parse `VGI_OPTIONAL_BEARER_TOKENS` (`token=principal,token=principal`),
 * matching vgi-rust's format.
 *
 * Set-but-unparseable throws rather than silently degrading to the defaults:
 * an operator who configured a token map and got the fixture's alice/bob
 * instead would have no way to notice.
 */
function parseTokenMap(raw: string | undefined): Map<string, string> | null {
  if (raw === undefined || raw.trim() === "") return null;
  const tokens = new Map<string, string>();
  for (const pair of raw.split(",")) {
    const eq = pair.indexOf("=");
    if (eq < 0) continue;
    const token = pair.slice(0, eq).trim();
    const principal = pair.slice(eq + 1).trim();
    if (token !== "" && principal !== "") tokens.set(token, principal);
  }
  if (tokens.size === 0) {
    throw new Error(
      `VGI_OPTIONAL_BEARER_TOKENS is set but contains no \`token=principal\` pair ` +
        `(got ${JSON.stringify(raw)})`,
    );
  }
  return tokens;
}

/**
 * Build the optional-bearer `authenticate` callback the example HTTP workers
 * pass to `serveVgiWorker`. Never throws at request time and never 401s.
 */
export function optionalTestBearerAuthenticate(
  env: { VGI_OPTIONAL_BEARER_TOKENS?: string } = process.env as any,
): (request: Request) => AuthContext {
  const tokens = parseTokenMap(env.VGI_OPTIONAL_BEARER_TOKENS) ?? new Map(DEFAULT_TEST_TOKENS);
  return (request: Request): AuthContext => {
    const header = request.headers.get("authorization") ?? "";
    // A present-but-blank credential is still "offered but invalid"; on this
    // optional path both it and an unknown token fall through to anonymous.
    if (!/^Bearer\s/i.test(header)) return AuthContext.anonymous();
    const principal = tokens.get(header.slice(header.indexOf(" ") + 1).trim());
    return principal ? new AuthContext("bearer", true, principal) : AuthContext.anonymous();
  };
}
