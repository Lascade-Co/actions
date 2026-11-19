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

/** Returns a stateful function that renders one log line to safe HTML. */
export function ansiRenderer() {
  const au = new AnsiUp();
  au.use_classes = true;
  au.escape_html = true;
  au.boldStyle = "x-bold";
  au.faintStyle = "x-faint";
  au.italicStyle = "x-italic";
  au.underlineStyle = "x-underline";
  return (text) =>
    au
      .ansi_to_html(text)
      // Text is already escaped, so a literal "<span" or "<a " here is markup.
      .replace(/<span style="([^"]*)"(?: class="([^"]*)")?>/g, (_, style, cls) => {
        const all = [...(cls ? cls.split(" ") : []), ...styleToClasses(style)];
        const rank = (c) => (c.endsWith("-fg") ? 0 : c.endsWith("-bg") ? 1 : 2);
        all.sort((a, b) => rank(a) - rank(b));
        return all.length ? `<span class="${all.join(" ")}">` : "<span>";
      })
      .replace(/<a href="([^"]*)">/g, '<a href="$1" target="_blank" rel="noopener noreferrer">');
}
