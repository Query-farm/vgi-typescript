// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Minimal CF-Workers-shaped module. Builds with `bun build --target=browser`
// and is inspected for Node-only imports. Exercises the same flechette APIs
// the production code would touch (decode IPC, encode IPC, per-row read).

import {
  tableFromIPC,
  tableToIPC,
  columnFromArray,
  tableFromColumns,
  utf8,
  int32,
} from "@uwdata/flechette";

// fetch handler shape mirrors a CF Workers entry. We don't actually deploy;
// the goal is to confirm the bundle has no node:* imports.
export default {
  async fetch(request: Request): Promise<Response> {
    const body = new Uint8Array(await request.arrayBuffer());
    const t = tableFromIPC(body, { useBigInt: true });

    const out = tableFromColumns({
      n: columnFromArray(["echo"], utf8()),
      v: columnFromArray([t.numRows], int32()),
    });
    const bytes = tableToIPC(out, { format: "stream" }) as Uint8Array;
    return new Response(bytes, {
      headers: { "content-type": "application/vnd.apache.arrow.stream" },
    });
  },
};
