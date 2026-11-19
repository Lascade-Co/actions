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

test("ansiRenderer carries colour state across lines", () => {
  const r = ansiRenderer();
  r(`${ESC}[32mstart`);
  assert.equal(r("next"), '<span class="ansi-green-fg">next</span>');
});

test("ansiRenderer opens links in a new tab without a referrer", () => {
  const r = ansiRenderer();
  const html = r(`${ESC}]8;;https://example.com/a?b=1&c=2${ESC}\\link${ESC}]8;;${ESC}\\`);
  assert.match(html, /^<a href="https:\/\/example.com\/a\?b=1&amp;c=2" target="_blank" rel="noopener noreferrer">link<\/a>$/);
});
