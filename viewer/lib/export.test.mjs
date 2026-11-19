import { test } from "node:test";
import assert from "node:assert/strict";
import { exportText, exportFilename, plainLine } from "./export.js";
import { parseJobLog, mergeSealed } from "./joblog.js";

const ESC = "\x1b";
const ART = "sealed-log-build-1-0a1b2c3d";
const LOG = [
  "2026-11-20T09:59:50.0000000Z Current runner version: '2.330.0'",
  "2026-11-20T09:59:54.0000000Z ##[group]Run ./build.sh",
  "2026-11-20T09:59:54.1000000Z shell: seal-bash-e {0}",
  "2026-11-20T09:59:54.2000000Z ##[endgroup]",
  `2026-11-20T10:00:00.0000000Z 🔒 sealed ${ART}#1 https://ci.lascade.com/?run=1&attempt=1`,
  `2026-11-20T10:00:01.0000000Z ${ESC}[32mpublic after${ESC}[0m`,
  "2026-11-20T10:00:02.0000000Z ##[group]Run ./deploy.sh",
  `2026-11-20T10:00:03.0000000Z 🔒 sealed ${ART}#2 https://ci.lascade.com/?run=1&attempt=1`,
].join("\n");

function job(segments, summaries = new Map()) {
  const merged = mergeSealed(parseJobLog(LOG, { run: "1" }), segments, () => "sealed log expired");
  for (const sec of merged) sec.summaries = sec.sealedKeys.filter((k) => summaries.has(k)).map((k) => summaries.get(k));
  return { name: "build", conclusion: "success", sections: merged };
}

test("exportText merges sealed lines in place, strips ANSI and keeps public timestamps", () => {
  const segments = new Map([[`${ART}#1`, [`${ESC}[1msecret one${ESC}[0m`, `see ${ESC}]8;;https://x${ESC}\\link${ESC}]8;;${ESC}\\`]]]);
  const text = exportText([job(segments)]);
  assert.equal(text, [
    "==== build (success) ====",
    "---- Set up job ----",
    "2026-11-20T09:59:50.0000000Z Current runner version: '2.330.0'",
    "---- Run ./build.sh ----",
    "2026-11-20T09:59:54.0000000Z ##[group]Run ./build.sh",
    "2026-11-20T09:59:54.1000000Z shell: seal-bash-e {0}",
    "2026-11-20T09:59:54.2000000Z ##[endgroup]",
    "secret one",
    "see link",
    "2026-11-20T10:00:01.0000000Z public after",
    "---- Run ./deploy.sh ----",
    "2026-11-20T10:00:02.0000000Z ##[group]Run ./deploy.sh",
    "🔒 sealed output unavailable (sealed log expired)",
    "",
  ].join("\n"));
  assert.doesNotMatch(text, /\x1b/);
});

test("exportText puts a step's summaries after its lines", () => {
  const segments = new Map([[`${ART}#1`, ["s1"]], [`${ART}#2`, ["s2"]]]);
  const text = exportText([job(segments, new Map([[`${ART}#1`, "# Build\nok"]]))]);
  const lines = text.split("\n");
  const at = lines.indexOf("---- summary ----");
  assert.ok(at > lines.indexOf("2026-11-20T10:00:01.0000000Z public after"));
  assert.ok(at < lines.indexOf("---- Run ./deploy.sh ----"));
  assert.deepEqual(lines.slice(at + 1, at + 3), ["# Build", "ok"]);
});

test("exportText writes jobs in order and notes jobs whose log is unavailable", () => {
  const text = exportText([
    { name: "a", conclusion: "success", sections: [{ title: "Set up job", lines: [{ ts: null, text: "x" }] }] },
    { name: "b", conclusion: "skipped", unavailable: "job was skipped" },
    { name: "c", conclusion: null, unavailable: "log expired" },
  ]);
  assert.equal(text, [
    "==== a (success) ====",
    "---- Set up job ----",
    "x",
    "",
    "==== b (skipped) ====",
    "log unavailable (job was skipped)",
    "",
    "==== c (unknown) ====",
    "log unavailable (log expired)",
    "",
  ].join("\n"));
});

test("exportFilename keeps only [A-Za-z0-9._-]", () => {
  assert.equal(exportFilename({ workflow: "Vinext deploy", runNumber: 42, attempt: 2 }), "Vinext-deploy-run42-attempt2.log");
  assert.equal(
    exportFilename({ workflow: "CI / build (ios)", runNumber: 7, attempt: 1, job: "build: arm64 → sim" }),
    "CI-build-ios-run7-attempt1-build-arm64-sim.log",
  );
  assert.equal(exportFilename({ workflow: "../../etc", runNumber: 1, attempt: 1, job: ".." }), "etc-run1-attempt1.log");
  assert.equal(exportFilename({ workflow: "", runNumber: 1, attempt: 1 }), "run-run1-attempt1.log");
  assert.match(exportFilename({ workflow: "ж".repeat(10), runNumber: 1, attempt: 1, job: "x".repeat(300) }), /^[A-Za-z0-9._-]{1,200}\.log$/);
});

test("plainLine removes 8-bit CSI and OSC sequences with their parameters", () => {
  assert.equal(plainLine("\u009b31m\u009d52;c;aGk=\u0007"), "");
  assert.equal(plainLine("a\u009b1;2Hb\u009d0;title\u009cc\u009d8;;u\x1b\\d"), "abcd");
  assert.equal(plainLine("p\u0090qdcs\u009cq"), "pq");
});

test("plainLine removes C0 and C1 controls but keeps tabs", () => {
  assert.equal(plainLine("bell\x07 back\bspace\x0e\x0f\x7f\u0085end"), "bell backspaceend");
  assert.equal(plainLine("a\tb"), "a\tb");
  // A lone CR (an overwrite the line splitter did not resolve) is dropped.
  assert.equal(plainLine("50%\r100%"), "50%100%");
  assert.equal(plainLine("x\u009b"), "x");
});

test("exportText writes no control characters other than newlines and tabs", () => {
  const text = exportText([{
    name: "j\u009b31m",
    conclusion: "success",
    sections: [{
      title: "Run \x1b(Bx",
      lines: [{ ts: null, text: "\u009b31m\u009d52;c;aGk=\u0007copied\rover\bx\ty", sealed: true }],
      summaries: ["# S\r\nline\x07\r\n"],
    }],
  }]);
  assert.doesNotMatch(text, /[\x00-\x08\x0b-\x1f\x7f-\x9f]/);
  assert.equal(text, "==== j (success) ====\n---- Run x ----\ncopiedoverx\ty\n---- summary ----\n# S\nline\n");
});
