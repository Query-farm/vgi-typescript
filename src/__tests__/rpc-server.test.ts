// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The single server builder: what is hosted on which transport, and the guards.

import { describe, expect, test } from "bun:test";
import { Protocol, type TokenIdentity } from "@query-farm/vgi-rpc";
import { buildSecondaryProtocol } from "@query-farm/vgi-rpc/conformance";
import { buildRpcServer, type HostingOptions, type ServerTransport } from "../rpc-server.js";

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
