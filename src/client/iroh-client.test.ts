import { describe, expect, test } from "bun:test";
import type { RpcClient } from "@query-farm/vgi-rpc";
import { VgiClient } from "./client.js";

describe("VgiClient.fromIroh", () => {
  test("selects an injected connector and forwards options", async () => {
    const endpoint = `httpi://${"01".repeat(32)}/vgi`;
    const calls: unknown[] = [];
    const rpc = { close() {} } as unknown as RpcClient;
    const client = await VgiClient.fromIroh(
      endpoint,
      { relayUrls: ["https://relay.example"] },
      undefined,
      async (actualEndpoint, options) => {
        calls.push(actualEndpoint, options);
        return rpc;
      },
    );

    expect(client).toBeInstanceOf(VgiClient);
    expect(calls).toEqual([endpoint, { relayUrls: ["https://relay.example"] }]);
  });

  test("rejects unrelated URI schemes before loading a binding", async () => {
    await expect(VgiClient.fromIroh("https://worker.example")).rejects.toThrow("iroh:// or httpi://");
  });
});
