// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Several worker processes opening one brand-new storage file at the same
// moment must all succeed. The first open switches the file to WAL, which
// takes an exclusive lock; busy_timeout has to be set before that PRAGMA so
// the other opens wait instead of failing with SQLITE_BUSY.

import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const dir = mkdtempSync(join(tmpdir(), "vgi-storage-concurrent-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

const storageModule = join(import.meta.dir, "..", "storage.ts");

test("concurrent FunctionStorageSqlite opens of one fresh db all succeed", async () => {
  const N = 8;
  for (let round = 0; round < 3; round++) {
    const dbPath = join(dir, `fresh-${round}.db`);
    const startAt = Date.now() + 750;
    // Each child waits for a shared start instant, then opens the db, writes
    // a row and exits 0; any SQLITE_BUSY surfaces as a non-zero exit.
    const child = `
      const { FunctionStorageSqlite } = await import(${JSON.stringify(storageModule)});
      while (Date.now() < ${startAt}) {}
      const s = new FunctionStorageSqlite(${JSON.stringify(dbPath)});
      await s.workerPut(new TextEncoder().encode("exec"), Number(process.env.VGI_TEST_WORKER_ID), new Uint8Array([1]));
      s.close();
    `;
    const procs = Array.from({ length: N }, (_, i) =>
      Bun.spawn([process.execPath, "-e", child], {
        env: { ...process.env, VGI_TEST_WORKER_ID: String(i) },
        stdout: "pipe",
        stderr: "pipe",
      }),
    );
    const results = await Promise.all(
      procs.map(async (p) => ({
        code: await p.exited,
        err: await new Response(p.stderr).text(),
      })),
    );
    for (const r of results) {
      // Compare as one object so a failure prints the child's stderr.
      expect({ code: r.code, err: r.code === 0 ? "" : r.err }).toEqual({ code: 0, err: "" });
    }
    const { FunctionStorageSqlite } = await import("../storage.js");
    const s = new FunctionStorageSqlite(dbPath);
    expect((await s.workerScan(new TextEncoder().encode("exec"))).length).toBe(N);
    s.close();
  }
}, 60_000);
