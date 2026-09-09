# Iroh

`VgiClient.fromIroh(endpoint, options)` selects native Node/Bun raw Iroh or
HTTP-over-Iroh from the URI. Browser callers inject the connector from
`@query-farm/vgi-rpc-iroh-browser`.

```ts
const raw = await VgiClient.fromIroh(`iroh://${endpointId}`);
const http = await VgiClient.fromIroh(`httpi://${endpointId}/vgi`);
```

Workers can expose a loopback raw bridge upstream without writing transport
code:

```console
bun worker.ts --iroh-raw-upstream 127.0.0.1:9400 --iroh-issuer production
```

For HTTP, pass `irohBridge` to `serveVgiWorker`; it binds loopback by default
and uses Bun's physical `requestIP`. A portable `createVgiWorkerFetch` caller
must additionally provide `peerResolutionContext`, because Fetch alone has no
physical socket peer. The provider rejects untrusted peers, merged duplicates,
and malformed EndpointIds before the authentication policy runs.
