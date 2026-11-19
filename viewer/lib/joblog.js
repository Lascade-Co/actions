// Parses a GitHub Actions job log into step sections and puts sealed segments
// back in place of their marker lines.

/** The binding marker format printed by seal-bash (one per sealed step). */
export const MARKER_RE = /🔒 sealed (sealed-log-[A-Za-z0-9_.-]+)#(\d+)/;

// A marker counts only when it is the whole line (after the timestamp), exactly
// as seal-bash prints it, so output that merely contains one is not trusted.
const LINE_MARKER_RE = /^🔒 sealed (sealed-log-[A-Za-z0-9_.-]+)#(\d+) https:\/\/ci\.lascade\.com\/\?run=(\d+)&attempt=(\d+)$/;

const TS_RE = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z) ?/;
const RUN_HEADER = "##[group]Run ";

// Lines the runner prints at the start of post steps and of the final step.
const BOUNDARIES = new Map([
  ["Post job cleanup.", "Post job cleanup"],
  ["Cleaning up orphan processes", "Complete job"],
]);

/**
 * Splits raw output into lines: CRLF and LF end a line, and a bare CR keeps only
 * what a terminal would show (the text after the last nonempty overwrite).
 * A single trailing newline does not add an empty line.
 */
export function splitLines(text) {
  if (text === "") return [];
  const lines = text.split(/\r?\n/);
  if (lines[lines.length - 1] === "") lines.pop();
  return lines.map((l) => {
    if (!l.includes("\r")) return l;
    const parts = l.split("\r").filter((p) => p !== "");
    return parts.length ? parts[parts.length - 1] : "";
  });
}

function parseLine(raw, expected) {
  const m = TS_RE.exec(raw);
  const line = m ? { ts: m[1], text: raw.slice(m[0].length) } : { ts: null, text: raw };
  const mk = LINE_MARKER_RE.exec(line.text);
  // A marker naming another run or attempt is left as a plain line.
  if (mk && (!expected || (mk[3] === String(expected.run) && mk[4] === String(expected.attempt)))) {
    line.marker = { artifact: mk[1], seq: Number(mk[2]), run: mk[3], attempt: mk[4] };
  }
  return line;
}

/**
 * Splits a job log into sections. With `expected = {run, attempt}`, only markers
 * whose URL names that run and attempt are parsed as markers.
 * @returns {{title: string, lines: {ts: string|null, text: string, marker?: {artifact: string, seq: number, run: string, attempt: string}}[]}[]}
 */
export function parseJobLog(text, expected = null) {
  const sections = [];
  let current = { title: "Set up job", lines: [] };
  const start = (title) => {
    // Every section but an empty preamble holds at least its header line.
    if (current.lines.length) sections.push(current);
    current = { title, lines: [] };
  };
  for (const raw of splitLines(text.replace(/^\uFEFF/, ""))) {
    const line = parseLine(raw, expected);
    if (line.text.startsWith(RUN_HEADER)) start(line.text.slice("##[group]".length).trim());
    else if (BOUNDARIES.has(line.text)) start(BOUNDARIES.get(line.text));
    current.lines.push(line);
  }
  if (current.lines.length || !sections.length) sections.push(current);
  return sections;
}

/** The "artifact#seq" keys of the markers in a section, in order. */
export function markerKeys(section) {
  return section.lines.filter((l) => l.marker).map((l) => `${l.marker.artifact}#${l.marker.seq}`);
}

/**
 * Replaces each marker line with its segment's lines (tagged sealed: true).
 * A marker with no segment becomes one "unavailable" line; `reasonFor(artifact)`
 * names why. Each "artifact#seq" is used at most once: a repeat becomes an
 * "unavailable (duplicate marker)" line. Each returned section lists the keys it
 * used in `sealedKeys`. Returns new sections; the input is not changed.
 */
export function mergeSealed(sections, segments, reasonFor = () => "sealed log not loaded") {
  const used = new Set();
  const unavailable = (l, reason) => ({ ts: l.ts, text: `🔒 sealed output unavailable (${reason})`, sealed: true, unavailable: true });
  return sections.map((s) => {
    const sealedKeys = [];
    const lines = s.lines.flatMap((l) => {
      if (!l.marker) return [l];
      const key = `${l.marker.artifact}#${l.marker.seq}`;
      if (used.has(key)) return [unavailable(l, "duplicate marker")];
      used.add(key);
      sealedKeys.push(key);
      const seg = segments.get(key);
      if (seg) return seg.map((text) => ({ ts: null, text, sealed: true }));
      return [unavailable(l, reasonFor(l.marker.artifact))];
    });
    return { ...s, lines, sealedKeys };
  });
}
