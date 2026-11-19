// Opens sealed-log and sealed artifact zips (as downloaded from the Actions API)
// with the repo key. Only fflate's synchronous APIs are used: its async ones start
// blob: workers, which the page's CSP forbids.
import { Decrypter } from "../vendor/age-encryption.js";
import { unzipSync, gunzipSync } from "../vendor/fflate.js";
import { readTar } from "./tar.js";
import { splitLines } from "./joblog.js";

const KEY_PREFIX = "AGE-SECRET-KEY-1";

export class SealError extends Error {
  constructor(message, code) {
    super(message);
    this.name = "SealError";
    this.code = code;
  }
}

/** Trims a pasted repo key and checks its format; returns the key or throws. */
export function checkRepoKey(key) {
  const k = String(key ?? "").trim();
  if (!k) throw new SealError("No repo key set: paste REPO_AGE_KEY from Infisical in Settings.", "no-key");
  if (!k.startsWith(KEY_PREFIX)) {
    throw new SealError(`The repo key must start with ${KEY_PREFIX}: paste the whole REPO_AGE_KEY value from Infisical.`, "bad-key");
  }
  return k;
}

function decrypter(identity) {
  const d = new Decrypter();
  try {
    d.addIdentity(checkRepoKey(identity));
  } catch (err) {
    if (err instanceof SealError) throw err;
    throw new SealError("The repo key is not a valid age key: paste REPO_AGE_KEY from Infisical in Settings.", "bad-key");
  }
  return d;
}

async function decrypt(d, bytes, name) {
  try {
    return await d.decrypt(bytes);
  } catch (err) {
    if (/no identity matched/.test(String(err?.message))) {
      throw new SealError("This repo key cannot open the sealed output: paste the current REPO_AGE_KEY from Infisical in Settings.", "bad-key");
    }
    throw new SealError(`Could not decrypt ${name}: ${err?.message ?? err}`, "corrupt");
  }
}

function unzip(zipBytes) {
  try {
    return unzipSync(zipBytes);
  } catch (err) {
    throw new SealError(`The downloaded artifact is not a valid zip: ${err?.message ?? err}`, "corrupt");
  }
}

const base = (p) => p.slice(p.lastIndexOf("/") + 1);
const utf8 = new TextDecoder();

/**
 * Decrypts a sealed-log artifact. `artifact` is its name, which keys the result
 * as "<artifact>#<seq>" to match the marker lines.
 * @returns {Promise<{meta: object, segments: Map<string, string[]>, summaries: Map<string, string>}>}
 */
export async function openSealedLog(zipBytes, identity, artifact) {
  const d = decrypter(identity);
  const files = unzip(zipBytes);
  const segments = new Map();
  const summaries = new Map();
  let meta = {};
  // meta first, so a wrong key fails before the bulk of the work.
  const names = Object.keys(files).sort((a, b) => (base(a) === "meta.json.age" ? -1 : base(b) === "meta.json.age" ? 1 : 0));
  for (const path of names) {
    const name = base(path);
    let m;
    if (name === "meta.json.age") {
      const text = utf8.decode(await decrypt(d, files[path], name));
      try {
        meta = JSON.parse(text);
      } catch {
        throw new SealError("The sealed log's meta.json is not valid JSON.", "corrupt");
      }
    } else if ((m = /^(\d+)\.log\.age$/.exec(name))) {
      segments.set(`${artifact}#${Number(m[1])}`, splitLines(utf8.decode(await decrypt(d, files[path], name))));
    } else if ((m = /^(\d+)\.summary\.md\.age$/.exec(name))) {
      summaries.set(`${artifact}#${Number(m[1])}`, utf8.decode(await decrypt(d, files[path], name)));
    }
  }
  return { meta, segments, summaries };
}

/**
 * Decrypts a sealed artifact (one "<name>.tgz.age" member).
 * @returns {Promise<{name: string, entries: import("./tar.js").TarEntry[], gzip: Uint8Array}>}
 */
export async function openSealedArtifact(zipBytes, identity) {
  const d = decrypter(identity);
  const files = unzip(zipBytes);
  const path = Object.keys(files).find((p) => base(p).endsWith(".tgz.age"));
  if (!path) throw new SealError("This is not a sealed artifact (no .tgz.age inside); download it from GitHub instead.", "not-sealed");
  const name = base(path).slice(0, -".tgz.age".length);
  const gzip = await decrypt(d, files[path], base(path));
  let tar;
  try {
    tar = gunzipSync(gzip);
  } catch (err) {
    throw new SealError(`The sealed artifact is not a valid gzip: ${err?.message ?? err}`, "corrupt");
  }
  let entries;
  try {
    entries = readTar(tar);
  } catch (err) {
    throw new SealError(`The sealed artifact is not a valid tar: ${err?.message ?? err}`, "corrupt");
  }
  return { name, entries, gzip };
}
