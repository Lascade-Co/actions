// Parses a GitHub Actions job log into step sections and puts sealed segments
// back in place of their marker lines.

/** The binding marker format printed by seal-bash (one per sealed step). */
export const MARKER_RE = /🔒 sealed (sealed-log-[A-Za-z0-9_.-]+)#(\d+)/;

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

function parseLine(raw) {
  const m = TS_RE.exec(raw);
  const line = m ? { ts: m[1], text: raw.slice(m[0].length) } : { ts: null, text: raw };
  const mk = MARKER_RE.exec(line.text);
  if (mk) line.marker = { artifact: mk[1], seq: Number(mk[2]) };
  return line;
}

/**
 * @returns {{title: string, lines: {ts: string|null, text: string, marker?: {artifact: string, seq: number}}[]}[]}
 */
export function parseJobLog(text) {
  const sections = [];
  let current = { title: "Set up job", lines: [] };
  const start = (title) => {
    // Every section but an empty preamble holds at least its header line.
    if (current.lines.length) sections.push(current);
    current = { title, lines: [] };
  };
  for (const raw of splitLines(text.replace(/^\uFEFF/, ""))) {
    const line = parseLine(raw);
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
 * names why. Returns new sections; the input is not changed.
 */
export function mergeSealed(sections, segments, reasonFor = () => "sealed log not loaded") {
  return sections.map((s) => ({
    ...s,
    lines: s.lines.flatMap((l) => {
      if (!l.marker) return [l];
      const seg = segments.get(`${l.marker.artifact}#${l.marker.seq}`);
      if (seg) return seg.map((text) => ({ ts: null, text, sealed: true }));
      return [{ ts: l.ts, text: `🔒 sealed output unavailable (${reasonFor(l.marker.artifact)})`, sealed: true, unavailable: true }];
    }),
  }));
}
