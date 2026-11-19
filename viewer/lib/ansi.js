// ANSI to HTML for log lines. ansi_up runs in class mode, but it still writes
// style="" for bold/faint/italic/underline and for 256-colour/24-bit colours.
// The CSP forbids inline styles, so those become classes here: text attributes
// map to ansi-bold etc. and colours to the nearest of the 16 palette classes.
import { AnsiUp } from "../vendor/ansi_up.js";

const PALETTE = [
  ["black", [0, 0, 0]], ["red", [187, 0, 0]], ["green", [0, 187, 0]], ["yellow", [187, 187, 0]],
  ["blue", [0, 0, 187]], ["magenta", [187, 0, 187]], ["cyan", [0, 187, 187]], ["white", [255, 255, 255]],
  ["bright-black", [85, 85, 85]], ["bright-red", [255, 85, 85]], ["bright-green", [0, 255, 0]],
  ["bright-yellow", [255, 255, 85]], ["bright-blue", [85, 85, 255]], ["bright-magenta", [255, 85, 255]],
  ["bright-cyan", [85, 255, 255]], ["bright-white", [255, 255, 255]],
];

function nearest(r, g, b) {
  let best = PALETTE[0][0];
  let dist = Infinity;
  for (const [name, [pr, pg, pb]] of PALETTE) {
    const d = (r - pr) ** 2 + (g - pg) ** 2 + (b - pb) ** 2;
    if (d < dist) {
      dist = d;
      best = name;
    }
  }
  return `ansi-${best}`;
}

// ansi_up joins these into style="..."; they become the matching classes.
const ATTRS = { "x-bold": "ansi-bold", "x-faint": "ansi-faint", "x-italic": "ansi-italic", "x-underline": "ansi-underline" };

function styleToClasses(style) {
  const out = [];
  for (const decl of style.split(";")) {
    if (ATTRS[decl]) {
      out.push(ATTRS[decl]);
      continue;
    }
    const m = /^(color|background-color):rgb\((\d+),(\d+),(\d+)\)$/.exec(decl);
    if (m) out.push(`${nearest(+m[2], +m[3], +m[4])}-${m[1] === "color" ? "fg" : "bg"}`);
  }
  return out;
}

// Complete escape sequences: CSI, OSC ended by BEL or ST, and two-byte ESC
// sequences other than the string introducers (P X [ ] ^ _), which need more bytes.
const COMPLETE_ESC = /^\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)|[@-OQ-WYZ\\])/;

/**
 * Drops each ESC that does not start a complete sequence (keeping the text after
 * it), so ansi_up never buffers a partial sequence into the next line.
 */
export function dropIncompleteEscapes(text) {
  if (!text.includes("\x1b")) return text;
  let out = "";
  let i = 0;
  while (i < text.length) {
    const esc = text.indexOf("\x1b", i);
    if (esc === -1) {
      out += text.slice(i);
      break;
    }
    out += text.slice(i, esc);
    const m = COMPLETE_ESC.exec(text.slice(esc));
    if (m) {
      out += m[0];
      i = esc + m[0].length;
    } else {
      i = esc + 1;
    }
  }
  return out;
}

// Every complete sequence (CSI; OSC, DCS, SOS, PM and APC strings ended by BEL
// or ST; two-byte ESC sequences such as ESC 7 or ESC M) or, failing that, the
// lone ESC.
const ANY_ESC = /\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)|[PX^_][^\x07\x1b]*(?:\x07|\x1b\\)|[0-?@-OQ-WYZ\\])?/g;

/**
 * Plain text of a line: escape sequences are removed, OSC 8 links keep their
 * text, and a stray ESC is dropped while the text after it stays.
 */
export function stripAnsi(text) {
  return text.includes("\x1b") ? text.replace(ANY_ESC, "") : text;
}

// A whole OSC 8 hyperlink, in exactly the form ansi_up renders in one packet:
// opener, printable ASCII text, closer. Any other OSC is dropped (see below).
const OSC8_LINK = /\x1b\]8;[ -:<-~]*;[!-~]{0,512}(?:\x1b\\|\x07)[ -~]+\x1b\]8;;(?:\x1b\\|\x07)/g;
const OSC_ANY = /\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/g;

/**
 * Makes one line self-contained: incomplete escapes are dropped, complete OSC 8
 * links are kept, and every other OSC sequence (an opener with no closer on this
 * line, a stray closer, window titles) is removed. ansi_up buffers an unclosed
 * hyperlink until it sees the closer, which would hold back every later line.
 */
export function selfContained(text) {
  const t = dropIncompleteEscapes(text);
  if (!t.includes("\x1b]")) return t;
  let out = "";
  let last = 0;
  for (const m of t.matchAll(OSC8_LINK)) {
    out += t.slice(last, m.index).replace(OSC_ANY, "") + m[0];
    last = m.index + m[0].length;
  }
  return out + t.slice(last).replace(OSC_ANY, "");
}

/**
 * Returns a function that renders one log line to safe HTML. Each line is
 * rendered on its own: no colour, attribute or buffered input carries over.
 */
export function ansiRenderer() {
  const au = new AnsiUp();
  au.use_classes = true;
  au.escape_html = true;
  au.boldStyle = "x-bold";
  au.faintStyle = "x-faint";
  au.italicStyle = "x-italic";
  au.underlineStyle = "x-underline";
  // ansi_up 6.0.6 keeps its parser state in these fields; reset them per line.
  const reset = () => {
    au._buffer = "";
    au.fg = au.bg = null;
    au.bold = au.faint = au.italic = au.underline = false;
  };
  return (text) => {
    reset();
    const html = au.ansi_to_html(selfContained(text));
    reset();
    return html
      // Text is already escaped, so a literal "<span" or "<a " here is markup.
      .replace(/<span style="([^"]*)"(?: class="([^"]*)")?>/g, (_, style, cls) => {
        const all = [...(cls ? cls.split(" ") : []), ...styleToClasses(style)];
        const rank = (c) => (c.endsWith("-fg") ? 0 : c.endsWith("-bg") ? 1 : 2);
        all.sort((a, b) => rank(a) - rank(b));
        return all.length ? `<span class="${all.join(" ")}">` : "<span>";
      })
      .replace(/<a href="([^"]*)">/g, '<a href="$1" target="_blank" rel="noopener noreferrer">');
  };
}
