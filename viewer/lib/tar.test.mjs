import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readTar } from "./tar.js";

const dec = new TextDecoder();

function withTree(fn) {
  const dir = mkdtempSync(join(tmpdir(), "viewer-tar-"));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("readTar reads a file, a nested dir, a symlink and long paths", () => {
  // 150 chars with slashes (ustar prefix or PAX) and one 120-char component (PAX or GNU L).
  const deep = ["d".repeat(60), "e".repeat(60), "f".repeat(28)].join("/");
  const longName = "n".repeat(120) + ".txt";
  assert.equal(deep.length, 150);
  const bytes = withTree((dir) => {
    const src = join(dir, "src");
    mkdirSync(join(src, "nested", "inner"), { recursive: true });
    mkdirSync(join(src, ...deep.split("/").slice(0, -1)), { recursive: true });
    writeFileSync(join(src, "hello.txt"), "hello\n");
    writeFileSync(join(src, "nested", "inner", "data.bin"), Buffer.from([0, 1, 2, 255]));
    writeFileSync(join(src, deep), "deep");
    writeFileSync(join(src, longName), "long");
    symlinkSync("hello.txt", join(src, "link"));
    const out = join(dir, "t.tar");
    execFileSync("tar", ["-cf", out, "-C", src, "hello.txt", "nested", "link", deep, longName], {
      env: { ...process.env, COPYFILE_DISABLE: "1" },
    });
    return new Uint8Array(readFileSync(out));
  });

  const entries = readTar(bytes);
  const by = Object.fromEntries(entries.map((e) => [e.path, e]));

  assert.equal(by["hello.txt"].type, "file");
  assert.equal(dec.decode(by["hello.txt"].data), "hello\n");
  assert.equal(by["nested"].type, "dir");
  assert.equal(by["nested/inner"].type, "dir");
  assert.equal(by["nested/inner/data.bin"].type, "file");
  assert.deepEqual([...by["nested/inner/data.bin"].data], [0, 1, 2, 255]);
  assert.equal(by["link"].type, "symlink");
  assert.equal(by["link"].linkname, "hello.txt");
  assert.equal(by["link"].data, undefined);
  assert.equal(dec.decode(by[deep].data), "deep");
  assert.equal(dec.decode(by[longName].data), "long");
});

// Ubuntu runners pack with GNU tar, whose default format stores long names in
// ././@LongLink entries of type L (path) and K (link target).
function header(name, type, size, linkname = "") {
  const h = new Uint8Array(512);
  const put = (s, off, len) => h.set(new TextEncoder().encode(s).subarray(0, len), off);
  const oct = (n, len) => n.toString(8).padStart(len - 1, "0") + "\0";
  put(name, 0, 100);
  put(oct(0o644, 8), 100, 8);
  put(oct(0, 8), 108, 8);
  put(oct(0, 8), 116, 8);
  put(oct(size, 12), 124, 12);
  put(oct(0, 12), 136, 12);
  put(type, 156, 1);
  put(linkname, 157, 100);
  put("ustar  \0", 257, 8);
  h.fill(0x20, 148, 156);
  const sum = h.reduce((a, b) => a + b, 0);
  put(sum.toString(8).padStart(6, "0") + "\0 ", 148, 8);
  return h;
}

function body(text) {
  const b = new TextEncoder().encode(text);
  const out = new Uint8Array(Math.ceil(b.length / 512) * 512);
  out.set(b);
  return out;
}

function concat(parts) {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

test("readTar reads GNU long names and long link targets", () => {
  const longPath = "a/" + "x".repeat(140) + ".log";
  const longTarget = "t/" + "y".repeat(130);
  const tar = concat([
    header("././@LongLink", "L", longPath.length + 1),
    body(longPath + "\0"),
    header(longPath.slice(0, 100), "0", 3),
    body("abc"),
    header("././@LongLink", "K", longTarget.length + 1),
    body(longTarget + "\0"),
    header("short-link", "2", 0, longTarget.slice(0, 100)),
    new Uint8Array(1024),
  ]);
  const entries = readTar(tar);
  assert.equal(entries.length, 2);
  assert.equal(entries[0].path, longPath);
  assert.equal(dec.decode(entries[0].data), "abc");
  assert.deepEqual({ path: entries[1].path, type: entries[1].type, linkname: entries[1].linkname }, {
    path: "short-link",
    type: "symlink",
    linkname: longTarget,
  });
});

test("readTar rejects bytes that are not a tar archive", () => {
  const junk = new Uint8Array(1024).fill(7);
  assert.throws(() => readTar(junk), /not a tar archive/);
});
