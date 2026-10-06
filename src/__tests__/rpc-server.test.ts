// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The single server builder: what is hosted on which transport, and the guards.

import { describe, expect, test } from "bun:test";
import { Protocol, type TokenIdentity } from "@query-farm/vgi-rpc";
import { buildSecondaryProtocol } from "@query-farm/vgi-rpc/conformance";
import { buildRpcServer, grantKeysFromArgv, type HostingOptions, type ServerTransport } from "../rpc-server.js";

const primary = () => new Protocol("vgi.v2", { protocolVersion: "2.0.0" });
const resolveToken = (_token: string): TokenIdentity | null => ({ principal: "p" });
const ALL: ServerTransport[] = ["pipe", "unix", "tcp", "iroh", "stream", "http"];

function hosted(hosting: HostingOptions, transport: ServerTransport, env: Record<string, string> = {}): string[] {
  return [...buildRpcServer(primary(), hosting, { transport, env }).bindings().keys()];
}

describe("buildRpcServer", () => {
  test("hosted protocols ride every transport, after vgi.v2, in order", () => {
    const extra = new Protocol("acme.Extra.v1");
    for (const transport of ALL) {
      expect(hosted({ hostedProtocols: () => [buildSecondaryProtocol(), extra] }, transport)).toEqual([
        "vgi.v2",
        "conformance.Secondary.v1",
        "acme.Extra.v1",
        "vgi_rpc.Reflection.v1",
      ]);
    }
  });

  test("the hook is called exactly once per server build", () => {
    let calls = 0;
    const hostedProtocols = () => {
      calls++;
      return [buildSecondaryProtocol()];
    };
    buildRpcServer(primary(), { hostedProtocols }, { transport: "pipe" });
    expect(calls).toBe(1);
  });

  test("identity is hosted on HTTP only, and only when opted into", () => {
    const opted: HostingOptions = { resolveToken, introspectPrincipals: ["proxy"] };
    expect(hosted(opted, "http")).toContain("vgi_rpc.Identity.v1");
    for (const transport of ALL.filter((t) => t !== "http")) {
      expect(hosted(opted, transport)).not.toContain("vgi_rpc.Identity.v1");
    }
    expect(hosted({}, "http")).not.toContain("vgi_rpc.Identity.v1");
  });

  test("introspection without an allowlist refuses to start", () => {
    expect(() => hosted({ resolveToken }, "http")).toThrow(/VGI_INTROSPECT_PRINCIPALS/);
    expect(() => hosted({ resolveToken, introspectPrincipals: [" ", ""] }, "http")).toThrow(/allowlist/);
    // The environment is the fallback.
    expect(hosted({ resolveToken }, "http", { VGI_INTROSPECT_PRINCIPALS: "proxy, other" })).toContain(
      "vgi_rpc.Identity.v1",
    );
  });

  test("minting alone needs no allowlist", () => {
    const mintGrant = () => ({ token: "t", expiresAt: 0 });
    expect(hosted({ mintGrant }, "http")).toContain("vgi_rpc.Identity.v1");
  });

  test("hook violations are startup errors naming the hook", () => {
    expect(() => hosted({ hostedProtocols: () => [new Protocol("vgi_rpc.Sneaky.v1")] }, "pipe")).toThrow(
      /hostedProtocols\(\) entry 0 .*reserved/,
    );
    expect(() => hosted({ hostedProtocols: () => [new Protocol("vgi.v2")] }, "pipe")).toThrow(
      /hostedProtocols\(\).*worker's own protocol/,
    );
    expect(() =>
      hosted({ hostedProtocols: () => [buildSecondaryProtocol(), buildSecondaryProtocol()] }, "pipe"),
    ).toThrow(/hostedProtocols\(\) lists protocol name 'conformance.Secondary.v1' twice/);
    expect(() => hosted({ hostedProtocols: () => [{} as Protocol] }, "pipe")).toThrow(
      /hostedProtocols\(\) entry 0 must be a vgi-rpc Protocol/,
    );
    expect(() => hosted({ hostedProtocols: () => null as never }, "pipe")).toThrow(/hostedProtocols\(\)/);
    expect(() => hosted({ hostedProtocols: () => [new Protocol("bad name")] }, "pipe")).toThrow(
      /hostedProtocols\(\) entry 0/,
    );
  });
});

describe("sealed grants", () => {
  const KEY = btoa(String.fromCharCode(...new Uint8Array(32).fill(3)));

  test("grant keys from the environment host issue_grant on HTTP only, minted by the framework", () => {
    const env = { VGI_RPC_GRANT_KEYS: KEY };
    const http = buildRpcServer(primary(), {}, { transport: "http", env });
    expect([...http.bindings().keys()]).toContain("vgi_rpc.Identity.v1");
    expect([...(http.identity?.offeredMethods() ?? [])]).toEqual(["issue_grant"]);
    expect(http.identity?.grantKeys).toBeDefined();
    for (const transport of ALL.filter((t) => t !== "http")) {
      expect(hosted({}, transport, env)).not.toContain("vgi_rpc.Identity.v1");
    }
  });

  test("no key, no change; null turns grants off; a malformed key refuses to start", () => {
    expect(hosted({}, "http", {})).not.toContain("vgi_rpc.Identity.v1");
    expect(hosted({ grantKeys: null }, "http", { VGI_RPC_GRANT_KEYS: KEY })).not.toContain("vgi_rpc.Identity.v1");
    expect(() => hosted({}, "http", { VGI_RPC_GRANT_KEYS: "bad*" })).toThrow(/base64/);
  });

  test("a worker's own mintGrant wins; resolveToken still needs its allowlist", () => {
    const mintGrant = () => ({ token: "mine", expiresAt: 0 });
    const server = buildRpcServer(primary(), { mintGrant }, { transport: "http", env: { VGI_RPC_GRANT_KEYS: KEY } });
    expect(server.identity?.grantKeys).toBeDefined();
    expect(() => hosted({ resolveToken }, "http", { VGI_RPC_GRANT_KEYS: KEY })).toThrow(/VGI_INTROSPECT_PRINCIPALS/);
  });

  test("--grant-key is read from argv, repeatable, first mints", () => {
    expect(grantKeysFromArgv([], {})).toBeUndefined();
    const other = btoa(String.fromCharCode(...new Uint8Array(32).fill(4)));
    const keys = grantKeysFromArgv(["--grant-key", KEY, `--grant-key=${other}`], { VGI_RPC_GRANT_AUDIENCE: "a" });
    expect(keys?.keys.length).toBe(2);
    expect(keys?.keys[0][0]).toBe(3);
    expect(keys?.audience).toBe("a");
    expect(() => grantKeysFromArgv(["--grant-key"], {})).toThrow();
    expect(() => grantKeysFromArgv(["--grant-key", "short"], {})).toThrow();
  });
});
