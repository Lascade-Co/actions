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

/** @returns {{path: string, type: "file"|"dir"|"symlink", data?: Uint8Array, linkname?: string}[]} */
export function readTar(bytes) {
  const entries = [];
  const files = new Map();
  let pax = {};
  let longName = null;
  let longLink = null;
  let off = 0;
  while (off + BLOCK <= bytes.length) {
    const h = bytes.subarray(off, off + BLOCK);
    if (h.every((b) => b === 0)) break;
    if (!checksumOk(h)) throw new Error("not a tar archive (bad header checksum)");
    const type = String.fromCharCode(h[156] || 0x30);
    const size = pax.size !== undefined && !"xgLK".includes(type) ? Number(pax.size) : num(h, 124, 12);
    const start = off + BLOCK;
    const data = bytes.subarray(start, start + size);
    if (data.length < size) throw new Error("not a tar archive (truncated)");
    off = start + Math.ceil(size / BLOCK) * BLOCK;

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
      if (target) entries.push({ path, type: "file", data: target.data });
    }
    // Devices, FIFOs and unknown types are skipped.
  }
  return entries;
}
