// Minimal tar reader: ustar (with the prefix field), PAX path/linkpath/size
// records, and GNU ././@LongLink (L/K) entries, which GNU tar writes by default.

const BLOCK = 512;
const utf8 = new TextDecoder();

function str(bytes, off, len) {
  const b = bytes.subarray(off, off + len);
  const nul = b.indexOf(0);
  return utf8.decode(nul === -1 ? b : b.subarray(0, nul));
}

function num(bytes, off, len) {
  // GNU base-256 for values that do not fit in octal.
  if (bytes[off] & 0x80) {
    let n = bytes[off] & 0x7f;
    for (let i = 1; i < len; i++) n = n * 256 + bytes[off + i];
    return n;
  }
  const s = str(bytes, off, len).trim();
  return s === "" ? 0 : parseInt(s, 8);
}

function checksumOk(h) {
  let sum = 0;
  for (let i = 0; i < BLOCK; i++) sum += i >= 148 && i < 156 ? 0x20 : h[i];
  return sum === num(h, 148, 8);
}

// PAX records: "<len> <key>=<value>\n", where len counts the whole record in bytes.
function parsePax(data) {
  const out = {};
  let i = 0;
  while (i < data.length) {
    const sp = data.indexOf(0x20, i);
    if (sp === -1) break;
    const len = parseInt(utf8.decode(data.subarray(i, sp)), 10);
    if (!(len > 0)) break;
    const rec = utf8.decode(data.subarray(sp + 1, i + len - 1));
    const eq = rec.indexOf("=");
    if (eq > 0) out[rec.slice(0, eq)] = rec.slice(eq + 1);
    i += len;
  }
  return out;
}

function clean(path) {
  let p = path;
  while (p.startsWith("./")) p = p.slice(2);
  return p.replace(/\/+$/, "");
}

function entrySize(pax, type, h) {
  if (pax.size !== undefined && !"xgLK".includes(type)) {
    if (!/^\d+$/.test(pax.size)) throw new Error("not a tar archive (bad PAX size)");
    return Number(pax.size);
  }
  return num(h, 124, 12);
}

const MAX_ENTRIES = 100_000;
const MAX_BYTES = 1024 * 1024 * 1024;

/**
 * @param {Uint8Array} bytes
 * @param {{maxEntries?: number, maxBytes?: number}} [limits] members (headers included) to read,
 *   and logical file bytes to return (hard links count their target's size again)
 * @returns {{path: string, type: "file"|"dir"|"symlink", data?: Uint8Array, linkname?: string}[]}
 */
export function readTar(bytes, { maxEntries = MAX_ENTRIES, maxBytes = MAX_BYTES } = {}) {
  const entries = [];
  const files = new Map();
  let pax = {};
  let longName = null;
  let longLink = null;
  let off = 0;
  let headers = 0;
  let logical = 0;
  const count = (n) => {
    logical += n;
    if (logical > maxBytes) {
      throw new Error(`not a tar archive this viewer can open (too much data, over ${Math.round(maxBytes / (1024 * 1024))} MiB)`);
    }
  };
  while (off + BLOCK <= bytes.length) {
    const h = bytes.subarray(off, off + BLOCK);
    if (h.every((b) => b === 0)) break;
    if (!checksumOk(h)) throw new Error("not a tar archive (bad header checksum)");
    if (++headers > maxEntries) throw new Error(`not a tar archive this viewer can open (too many entries, over ${maxEntries})`);
    const type = String.fromCharCode(h[156] || 0x30);
    const size = entrySize(pax, type, h);
    if (!Number.isSafeInteger(size) || size < 0) throw new Error("not a tar archive (bad member size)");
    const start = off + BLOCK;
    // Data never extends past the input, so a member can't claim more bytes than exist.
    if (size > bytes.length - start) throw new Error("not a tar archive (truncated)");
    const data = bytes.subarray(start, start + size);
    const next = start + Math.ceil(size / BLOCK) * BLOCK;
    if (!(next > off)) throw new Error("not a tar archive (offset did not advance)");
    off = next;

    if (type === "x") {
      pax = { ...pax, ...parsePax(data) };
      continue;
    }
    if (type === "g") continue;
    if (type === "L") {
      longName = str(data, 0, data.length);
      continue;
    }
    if (type === "K") {
      longLink = str(data, 0, data.length);
      continue;
    }

    let name = str(h, 0, 100);
    if (str(h, 257, 5) === "ustar" && h[263] === 0x30) {
      // POSIX ustar ("ustar\0" + "00"): name may continue in the prefix field.
      const prefix = str(h, 345, 155);
      if (prefix) name = `${prefix}/${name}`;
    }
    const path = clean(pax.path ?? longName ?? name);
    const linkname = pax.linkpath ?? longLink ?? str(h, 157, 100);
    pax = {};
    longName = null;
    longLink = null;

    if (type === "0" || type === "7") {
      count(data.length);
      const entry = { path, type: "file", data };
      files.set(path, entry);
      entries.push(entry);
    } else if (type === "5") {
      entries.push({ path, type: "dir" });
    } else if (type === "2") {
      entries.push({ path, type: "symlink", linkname });
    } else if (type === "1") {
      // A hard link shares the data of an earlier member.
      const target = files.get(clean(linkname));
      if (target) {
        count(target.data.length);
        entries.push({ path, type: "file", data: target.data });
      }
    }
    // Devices, FIFOs and unknown types are skipped.
  }
  return entries;
}
