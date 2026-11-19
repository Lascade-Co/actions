import { test } from "node:test";
import assert from "node:assert/strict";
import { ansiRenderer } from "./ansi.js";

const ESC = "\x1b";

test("ansiRenderer escapes HTML", () => {
  const r = ansiRenderer();
  assert.equal(r("<script>&\"x\"</script>"), "&lt;script&gt;&amp;&quot;x&quot;&lt;/script&gt;");
});

test("ansiRenderer uses classes, never inline styles", () => {
  const r = ansiRenderer();
  const html = [
    r(`${ESC}[31mred${ESC}[0m`),
    r(`${ESC}[1;4;3;2mbold${ESC}[0m`),
    r(`${ESC}[38;5;208morange${ESC}[0m`),
    r(`${ESC}[38;2;10;200;10mtrue${ESC}[48;2;250;250;250mbg${ESC}[0m`),
  ].join("\n");
  assert.doesNotMatch(html, /style=/);
  assert.match(html, /<span class="ansi-red-fg">red<\/span>/);
  assert.match(html, /class="[^"]*ansi-bold[^"]*"/);
  assert.match(html, /class="[^"]*ansi-underline[^"]*"/);
  assert.match(html, /class="[^"]*ansi-italic[^"]*"/);
  assert.match(html, /class="[^"]*ansi-faint[^"]*"/);
  // 256-colour and 24-bit colours map to the nearest of the 16 palette classes.
  assert.match(html, /<span class="ansi-[a-z-]+-fg">orange<\/span>/);
  assert.match(html, /<span class="ansi-green-fg">true<\/span>/);
  assert.match(html, /<span class="ansi-[a-z-]+-fg ansi-[a-z-]+-bg">bg<\/span>/);
});

test("ansiRenderer renders every line on its own (no colour carried over)", () => {
  const r = ansiRenderer();
  assert.equal(r(`${ESC}[32mstart`), '<span class="ansi-green-fg">start</span>');
  assert.equal(r("next"), "next");
  r(`${ESC}[1m`);
  assert.equal(r("plain"), "plain");
});

for (const [label, st] of [["ESC \\", `${ESC}\\`], ["BEL", "\x07"]]) {
  test(`an OSC 8 opener without a closer (${label}) does not hold later lines`, () => {
    const r = ansiRenderer();
    const lines = [`ok ${ESC}]8;;https://x.com${st}`, "l2 secret step output", "l3", "l4", "l5", `${ESC}]8;;${st}`, "l7"];
    assert.deepEqual(lines.map(r), ["ok ", "l2 secret step output", "l3", "l4", "l5", "", "l7"]);
  });
}

test("a second OSC 8 opener on a line keeps only the complete link", () => {
  const r = ansiRenderer();
  const html = r(`${ESC}]8;;https://a.com${ESC}\\x ${ESC}]8;;https://b.com${ESC}\\y${ESC}]8;;${ESC}\\ z`);
  assert.equal(html, 'x <a href="https://b.com" target="_blank" rel="noopener noreferrer">y</a> z');
  assert.equal(r("after"), "after");
});

test("the vendored ansi_up is the version whose state fields the renderer resets", async () => {
  const { AnsiUp } = await import("../vendor/ansi_up.js");
  const au = new AnsiUp();
  assert.equal(au.VERSION, "6.0.6");
  for (const k of ["_buffer", "fg", "bg", "bold", "faint", "italic", "underline"]) assert.ok(k in au, k);
});

test("a stray OSC 8 closer is dropped", () => {
  const r = ansiRenderer();
  assert.equal(r(`a${ESC}]8;;${ESC}\\b`), "ab");
  assert.equal(r("c"), "c");
});

test("ansiRenderer opens links in a new tab without a referrer", () => {
  const r = ansiRenderer();
  const html = r(`${ESC}]8;;https://example.com/a?b=1&c=2${ESC}\\link${ESC}]8;;${ESC}\\`);
  assert.match(html, /^<a href="https:\/\/example.com\/a\?b=1&amp;c=2" target="_blank" rel="noopener noreferrer">link<\/a>$/);
});

test("an unterminated OSC at the end of a line does not swallow later lines", () => {
  const r = ansiRenderer();
  r(`before ${ESC}]8;;`);
  assert.equal(r("next line"), "next line");
  assert.equal(r("third"), "third");
});

test("an unterminated CSI at the end of a line does not eat the next line", () => {
  const r = ansiRenderer();
  r(`text${ESC}[`);
  assert.equal(r("abc"), "abc");
  const r2 = ansiRenderer();
  r2(`text${ESC}`);
  assert.equal(r2("abc"), "abc");
});

test("a complete sequence still renders after a stray escape", () => {
  const r = ansiRenderer();
  assert.equal(r(`a${ESC}]8;;b ${ESC}[31mred${ESC}[0m`), 'a]8;;b <span class="ansi-red-fg">red</span>');
});

test("stripAnsi removes CSI sequences", async () => {
  const { stripAnsi } = await import("./ansi.js");
  assert.equal(stripAnsi(`${ESC}[1;31mred${ESC}[0m plain ${ESC}[38;2;1;2;3mx${ESC}[K${ESC}[?25l`), "red plain x");
  assert.equal(stripAnsi("no escapes <b>&"), "no escapes <b>&");
});

for (const [label, st] of [["ST", `${ESC}\\`], ["BEL", "\x07"]]) {
  test(`stripAnsi keeps OSC 8 link text and drops the link (${label})`, async () => {
    const { stripAnsi } = await import("./ansi.js");
    assert.equal(stripAnsi(`see ${ESC}]8;;https://x.com/a${st}the docs${ESC}]8;;${st} now`), "see the docs now");
    assert.equal(stripAnsi(`${ESC}]0;window title${st}after`), "after");
  });
}

test("stripAnsi drops a stray ESC but keeps the text after it", async () => {
  const { stripAnsi } = await import("./ansi.js");
  assert.equal(stripAnsi(`a${ESC}b`), "ab");
  assert.equal(stripAnsi(`text${ESC}[`), "text[");
  assert.equal(stripAnsi(`text${ESC}`), "text");
  assert.equal(stripAnsi(`a${ESC}]8;;b ${ESC}[31mred`), "a]8;;b red");
  assert.equal(stripAnsi(`x${ESC}7y${ESC}8`), "xy");
});
