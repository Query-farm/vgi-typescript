// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The host-local root key for secret ATTACH options on OS-owned transports
// (stdio, AF_UNIX, launcher pools), where there is no signing key. Shared by
// every VGI worker of this user on this host, whatever its SDK, so a launcher
// pool's processes — and workers of different SDKs — agree on it.
//
// Identical across SDKs (SPEC-secret-options.md, requirement 4):
//   path    $VGI_ATTACH_SECRET_KEY_FILE if set and non-empty; else
//           $XDG_RUNTIME_DIR/vgi/attach-secret-options.key if XDG_RUNTIME_DIR is
//           a directory; else <os.tmpdir()>/vgi-<username>/attach-secret-options.key
//   content exactly 32 raw CSPRNG bytes
//   perms   directory 0700, file 0600, both owned by this user and not
//           group/other-accessible (POSIX) — refused otherwise
//   create  temp file (0600) in the same directory, hard-linked into place;
//           an existing key is never overwritten, concurrent first starts agree
//
// Node / Bun only (it needs a filesystem), and read only when a secret option
// actually has a value.

type NodeFs = typeof import("node:fs");
type NodeOs = typeof import("node:os");
type NodePath = typeof import("node:path");

/**
 * Node built-ins via `process.getBuiltinModule` (Node >= 22.3, Bun) rather than
 * static imports, so the browser / workerd bundles that reach this module
 * through the catalog code carry no `node:` imports. Throws where there is no
 * filesystem; the caller turns that into "no key available".
 */
function builtins(): { fs: NodeFs; os: NodeOs; path: NodePath } {
  const proc = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  const get = proc?.getBuiltinModule?.bind(proc);
  const fs = get?.("node:fs") as NodeFs | undefined;
  const os = get?.("node:os") as NodeOs | undefined;
  const path = get?.("node:path") as NodePath | undefined;
  if (!fs || !os || !path) throw new Error("this runtime has no filesystem for the host-local attach secret key file.");
  return { fs, os, path };
}

export const ATTACH_SECRET_KEY_FILE_ENV = "VGI_ATTACH_SECRET_KEY_FILE";
const KEY_LEN = 32;
const FILE_NAME = "attach-secret-options.key";
const REMEDY = "Set VGI_SIGNING_KEY (or configure attachSecretKey), or point VGI_ATTACH_SECRET_KEY_FILE at a valid key file.";

function username(): string {
  try {
    const name = builtins().os.userInfo().username;
    if (name) return name;
  } catch {
    // no passwd entry (containers): fall through
  }
  return process.env.USER || process.env.USERNAME || String(process.getuid?.() ?? "user");
}

function isDirectory(path: string): boolean {
  try {
    return builtins().fs.statSync(path).isDirectory();
  } catch {
    return false;
  }
}

/** The key file path this process uses, per the cross-SDK rules above. */
export function hostLocalAttachSecretKeyPath(env: Record<string, string | undefined> = (globalThis as { process?: { env: Record<string, string | undefined> } }).process?.env ?? {}): string {
  const { os, path: { join } } = builtins();
  const explicit = env[ATTACH_SECRET_KEY_FILE_ENV];
  if (explicit) return explicit;
  const xdg = env.XDG_RUNTIME_DIR;
  if (xdg && isDirectory(xdg)) return join(xdg, "vgi", FILE_NAME);
  return join(os.tmpdir(), `vgi-${username()}`, FILE_NAME);
}

const POSIX = typeof process === "undefined" || process.platform !== "win32";

function checkOwnedPrivate(path: string, mode: number, uid: number, what: string): void {
  if (!POSIX) return;
  const me = process.getuid?.();
  if (me !== undefined && uid !== me) {
    throw new Error(`attach secret key ${what} ${path} is not owned by the current user. ${REMEDY}`);
  }
  if ((mode & 0o077) !== 0) {
    throw new Error(
      `attach secret key ${what} ${path} is accessible by group/other (mode ${(mode & 0o777).toString(8)}); ` +
        `it must be ${what === "directory" ? "0700" : "0600"}. ${REMEDY}`,
    );
  }
}

function ensureDir(dir: string): void {
  const { mkdirSync, lstatSync } = builtins().fs;
  try {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "EEXIST") throw e;
  }
  const st = lstatSync(dir);
  if (!st.isDirectory()) throw new Error(`attach secret key directory ${dir} is not a directory. ${REMEDY}`);
  checkOwnedPrivate(dir, st.mode, st.uid, "directory");
}

function readKey(path: string): Uint8Array | null {
  const { openSync, fstatSync, readSync, closeSync, constants } = builtins().fs;
  let fd: number;
  try {
    fd = openSync(path, constants.O_RDONLY | (POSIX ? constants.O_NOFOLLOW : 0));
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw new Error(`attach secret key file ${path} cannot be opened (${(e as NodeJS.ErrnoException).code}). ${REMEDY}`);
  }
  try {
    const st = fstatSync(fd);
    if (!st.isFile()) throw new Error(`attach secret key file ${path} is not a regular file. ${REMEDY}`);
    checkOwnedPrivate(path, st.mode, st.uid, "file");
    const buf = new Uint8Array(KEY_LEN + 1);
    let n = 0;
    for (;;) {
      const got = readSync(fd, buf, n, buf.length - n, null);
      if (got <= 0) break;
      n += got;
      if (n === buf.length) break;
    }
    if (n !== KEY_LEN || st.size !== KEY_LEN) {
      throw new Error(
        `attach secret key file ${path} must contain exactly ${KEY_LEN} raw bytes (found ${st.size}). ${REMEDY}`,
      );
    }
    return buf.subarray(0, KEY_LEN);
  } finally {
    closeSync(fd);
  }
}

/** Create the key atomically unless it exists; never overwrite one. */
function createKey(path: string): void {
  const { openSync, writeSync, fsyncSync, closeSync, linkSync, unlinkSync, constants } = builtins().fs;
  const { dirname, join } = builtins().path;
  const tmp = join(dirname(path), `.${FILE_NAME}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`);
  const fd = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o600);
  try {
    const key = crypto.getRandomValues(new Uint8Array(KEY_LEN));
    let off = 0;
    while (off < KEY_LEN) off += writeSync(fd, key, off, KEY_LEN - off);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  try {
    linkSync(tmp, path);
  } catch (e) {
    // Another process won the race: use its key.
    if ((e as NodeJS.ErrnoException).code !== "EEXIST") {
      unlinkSync(tmp);
      throw e;
    }
  }
  unlinkSync(tmp);
}

/**
 * Read (creating on first use) the host-local key file at `path`
 * (default {@link hostLocalAttachSecretKeyPath}). Synchronous and uncached;
 * see {@link hostLocalAttachSecretKey} for the cached form the framework uses.
 */
export function loadHostLocalAttachSecretKey(path: string = hostLocalAttachSecretKeyPath()): Uint8Array {
  ensureDir(builtins().path.dirname(path));
  const existing = readKey(path);
  if (existing) return existing;
  createKey(path);
  const created = readKey(path);
  if (!created) throw new Error(`attach secret key file ${path} vanished after creation. ${REMEDY}`);
  return created;
}

let cached: { path: string; key: Uint8Array } | undefined;

/** The host-local root key, read once per process (per resolved path). */
export async function hostLocalAttachSecretKey(): Promise<Uint8Array> {
  const path = hostLocalAttachSecretKeyPath();
  if (cached?.path === path) return cached.key;
  const key = loadHostLocalAttachSecretKey(path);
  cached = { path, key };
  return key;
}
