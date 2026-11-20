import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as age from "../vendor/age-encryption.js";
import { zipSync, strToU8, gzipSync } from "../vendor/fflate.js";
import { openSealedLog, openSealedArtifact, checkRepoKey, gunzipCapped, recipientOf, checkKeyForRepo, MAX_UNSEALED_BYTES } from "./sealed.js";

const REPO_PUB = new URL("../../scripts/seal/repo-key.pub", import.meta.url);
const RECIPIENT_RE = /^age1[02-9ac-hj-np-z]{58}$/;

function hasAgeKeygen() {
  try {
    execFileSync("age-keygen", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

test("recipientOf gives the repo-key.pub line age-keygen writes for a test identity", async (t) => {
  // scripts/seal/repo-key.pub is one recipient line, the format compared against.
  assert.match(readFileSync(REPO_PUB, "utf8").trim(), RECIPIENT_RE);
  if (!hasAgeKeygen()) {
    t.skip("age-keygen is not installed");
    return;
  }
  const dir = mkdtempSync(join(tmpdir(), "viewer-recipient-"));
  try {
    const keyFile = join(dir, "key.txt");
    execFileSync("age-keygen", ["-o", keyFile], { stdio: "ignore" });
    const identity = readFileSync(keyFile, "utf8").split("\n").find((l) => l.startsWith("AGE-SECRET-KEY-1"));
    // Same command as "Rotate the repo key" in docs/sealed-output.md.
    const pub = execFileSync("age-keygen", ["-y", keyFile], { encoding: "utf8" });
    assert.equal(await recipientOf(`  ${identity}\n`), pub.trim());
    assert.match(await recipientOf(identity), RECIPIENT_RE);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("recipientOf is the recipient that identity decrypts for", async () => {
  const identity = await age.generateIdentity();
  const recipient = await recipientOf(identity);
  assert.match(recipient, RECIPIENT_RE);
  const d = new age.Decrypter();
  d.addIdentity(identity);
  assert.equal(new TextDecoder().decode(await d.decrypt(await seal(recipient, "hello"))), "hello");
  assert.notEqual(await recipientOf(await age.generateIdentity()), recipient);
});

test("recipientOf refuses a malformed key", async () => {
  await assert.rejects(recipientOf(""), (err) => err.code === "no-key");
  await assert.rejects(recipientOf("1QQQ"), (err) => err.code === "bad-key");
  await assert.rejects(recipientOf("AGE-SECRET-KEY-1NOTBECH32"), (err) => err.code === "bad-key" && /not a valid age key/.test(err.message));
});

test("checkKeyForRepo accepts the repo's key and returns it trimmed", async () => {
  const { identity, recipient } = await keypair();
  assert.equal(await checkKeyForRepo(` ${identity}\n`, `${recipient}\n`, "connector"), identity);
});

test("a key for another repository is rejected", async () => {
  const { identity } = await keypair();
  const other = await keypair();
  await assert.rejects(checkKeyForRepo(identity, other.recipient, "connector"), (err) => {
    assert.equal(err.code, "wrong-repo");
    assert.match(err.message, /^This key is for a different repository/);
    assert.match(err.message, /connector/);
    return true;
  });
});

const ART = "sealed-log-build-1-0a1b2c3d";

async function keypair() {
  const identity = await age.generateIdentity();
  return { identity, recipient: await age.identityToRecipient(identity) };
}

async function seal(recipient, data) {
  const e = new age.Encrypter();
  e.addRecipient(recipient);
  return e.encrypt(data);
}

async function sealedLogZip(recipient) {
  return zipSync({
    "meta.json.age": await seal(recipient, '{"job":"build","attempt":"1"}'),
    "1.log.age": await seal(recipient, "first\r\nstep\n"),
    "2.log.age": await seal(recipient, "line1\nline2"),
    "2.summary.md.age": await seal(recipient, "# Summary\n<b>x</b>\n"),
    "3.log.age": await seal(recipient, ""),
  });
}

test("openSealedLog decrypts meta, segments and summaries", async () => {
  const { identity, recipient } = await keypair();
  const zip = await sealedLogZip(recipient);
  const out = await openSealedLog(zip, identity, ART);
  assert.equal(out.meta.job, "build");
  assert.deepEqual(out.segments.get(`${ART}#2`), ["line1", "line2"]);
  assert.deepEqual(out.segments.get(`${ART}#1`), ["first", "step"]);
  assert.deepEqual(out.segments.get(`${ART}#3`), []);
  assert.equal(out.summaries.get(`${ART}#2`), "# Summary\n<b>x</b>\n");
  assert.equal(out.summaries.has(`${ART}#1`), false);
});

test("openSealedLog rejects a wrong identity naming the repo key", async () => {
  const { recipient } = await keypair();
  const other = await age.generateIdentity();
  const zip = await sealedLogZip(recipient);
  await assert.rejects(openSealedLog(zip, other, ART), (err) => {
    assert.match(err.message, /repo key/);
    assert.equal(err.code, "bad-key");
    return true;
  });
});

test("checkRepoKey refuses a key without the AGE-SECRET-KEY-1 prefix", () => {
  assert.throws(() => checkRepoKey("1QQQ"), /AGE-SECRET-KEY-1/);
  assert.throws(() => checkRepoKey(""), /repo key/);
  assert.throws(() => checkRepoKey("AGE-SECRET-KEY-PQ-1ABC"), /AGE-SECRET-KEY-1/);
  assert.equal(checkRepoKey("  AGE-SECRET-KEY-1ABC \n"), "AGE-SECRET-KEY-1ABC");
});

test("openSealedLog refuses a malformed key before decrypting", async () => {
  const { recipient } = await keypair();
  const zip = await sealedLogZip(recipient);
  await assert.rejects(openSealedLog(zip, "not-a-key", ART), /AGE-SECRET-KEY-1/);
});

test("openSealedArtifact decrypts the tgz and lists its entries", async () => {
  const { identity, recipient } = await keypair();
  const dir = mkdtempSync(join(tmpdir(), "viewer-sealed-"));
  let tgz;
  try {
    mkdirSync(join(dir, "src", "reports"), { recursive: true });
    writeFileSync(join(dir, "src", "reports", "a.txt"), "report\n");
    writeFileSync(join(dir, "src", "b.json"), "{}");
    // Same pipeline as seal-pack: tar, then gzip -n outside tar.
    execFileSync("sh", ["-c", `tar -cf - -C "$1" reports/a.txt b.json | gzip -n > "$2"`, "sh", join(dir, "src"), join(dir, "x.tgz")], {
      env: { ...process.env, COPYFILE_DISABLE: "1" },
    });
    tgz = new Uint8Array(readFileSync(join(dir, "x.tgz")));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  const zip = zipSync({ "catchup-report.tgz.age": await seal(recipient, tgz) });
  const out = await openSealedArtifact(zip, identity);
  assert.equal(out.name, "catchup-report");
  assert.deepEqual(out.entries.map((e) => e.path).sort(), ["b.json", "reports/a.txt"]);
  const a = out.entries.find((e) => e.path === "reports/a.txt");
  assert.equal(new TextDecoder().decode(a.data), "report\n");
  assert.deepEqual(out.gzip, tgz);
});

test("openSealedArtifact refuses an artifact that was not sealed", async () => {
  const { identity } = await keypair();
  const zip = zipSync({ "report.txt": strToU8("plain") });
  await assert.rejects(openSealedArtifact(zip, identity), /not a sealed artifact/);
});

const MiB = 1024 * 1024;

test("the unsealed size cap defaults to 1 GiB", () => {
  assert.equal(MAX_UNSEALED_BYTES, 1024 * MiB);
});

test("gunzipCapped stops a gzip bomb at the cap", () => {
  const bomb = gzipSync(new Uint8Array(64 * MiB), { level: 9 });
  assert.ok(bomb.length < MiB);
  assert.throws(() => gunzipCapped(bomb, 8 * MiB), /8 MiB/);
  assert.equal(gunzipCapped(gzipSync(strToU8("hello")), 8 * MiB).length, 5);
});

test("openSealedArtifact rejects a gzip bomb with a message naming the cap", async () => {
  const { identity, recipient } = await keypair();
  const bomb = gzipSync(new Uint8Array(32 * MiB), { level: 9 });
  const zip = zipSync({ "x.tgz.age": await seal(recipient, bomb) });
  await assert.rejects(openSealedArtifact(zip, identity, { maxBytes: 4 * MiB }), /4 MiB/);
});

test("openSealedLog rejects a zip whose members are larger than the cap", async () => {
  const { identity, recipient } = await keypair();
  const zip = zipSync({ "meta.json.age": await seal(recipient, "{}"), "1.log.age": await seal(recipient, new Uint8Array(3 * MiB)) });
  await assert.rejects(openSealedLog(zip, identity, ART, { maxBytes: 2 * MiB }), /2 MiB/);
});
