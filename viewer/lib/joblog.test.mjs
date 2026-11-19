import { test } from "node:test";
import assert from "node:assert/strict";
import { parseJobLog, mergeSealed, MARKER_RE } from "./joblog.js";

const ART = "sealed-log-build-1-0a1b2c3d";
const FIXTURE = [
  "﻿2026-11-20T09:59:50.0000000Z Current runner version: '2.330.0'",
  "2026-11-20T09:59:51.0000000Z Runner name: 'GitHub Actions 1'",
  "2026-11-20T09:59:52.0000000Z ##[group]Run actions/checkout@v7",
  "2026-11-20T09:59:52.1000000Z with:",
  "2026-11-20T09:59:52.2000000Z ##[endgroup]",
  "2026-11-20T09:59:53.0000000Z Syncing repository",
  "2026-11-20T09:59:54.0000000Z ##[group]Run ./gradlew assemble",
  "2026-11-20T09:59:54.1000000Z shell: seal-bash-e {0}",
  "2026-11-20T09:59:54.2000000Z ##[endgroup]",
  `2026-11-20T10:00:00.0000000Z 🔒 sealed ${ART}#2 https://ci.lascade.com/?run=1&attempt=1`,
  "2026-11-20T10:00:01.0000000Z ##[group]Run echo done",
  "2026-11-20T10:00:01.1000000Z ##[endgroup]",
  "2026-11-20T10:00:02.0000000Z 🔒 seal failed for sealed-log-build-1-0a1b2c3d#3",
  "",
].join("\n");

test("parseJobLog splits at Run group headers with a Set up job preamble", () => {
  const sections = parseJobLog(FIXTURE);
  assert.equal(sections.length, 4);
  assert.deepEqual(
    sections.map((s) => s.title),
    ["Set up job", "Run actions/checkout@v7", "Run ./gradlew assemble", "Run echo done"],
  );
  assert.equal(sections[0].lines.length, 2);
  assert.equal(sections[0].lines[0].ts, "2026-11-20T09:59:50.0000000Z");
  assert.equal(sections[0].lines[0].text, "Current runner version: '2.330.0'");
  // The header line itself opens the section.
  assert.equal(sections[1].lines[0].text, "##[group]Run actions/checkout@v7");
});

test("parseJobLog parses the marker line", () => {
  const sections = parseJobLog(FIXTURE);
  const markers = sections.flatMap((s) => s.lines.filter((l) => l.marker));
  assert.equal(markers.length, 1);
  assert.deepEqual(markers[0].marker, { artifact: ART, seq: 2, run: "1", attempt: "1" });
  assert.equal(markers[0].ts, "2026-11-20T10:00:00.0000000Z");
  assert.ok(MARKER_RE.test(markers[0].text));
});

test("parseJobLog keeps lines without a timestamp", () => {
  const [s] = parseJobLog("plain line\n2026-11-20T10:00:00.0000000Z x");
  assert.deepEqual(s.lines[0], { ts: null, text: "plain line" });
  assert.equal(s.lines[1].text, "x");
});

test("mergeSealed puts segment lines in place of the marker", () => {
  const sections = parseJobLog(FIXTURE);
  const merged = mergeSealed(sections, new Map([[`${ART}#2`, ["one", "two", "three"]]]));
  const step = merged[2];
  const texts = step.lines.map((l) => l.text);
  assert.deepEqual(texts, ["##[group]Run ./gradlew assemble", "shell: seal-bash-e {0}", "##[endgroup]", "one", "two", "three"]);
  assert.deepEqual(step.lines.slice(3).map((l) => l.sealed), [true, true, true]);
  assert.ok(step.lines.slice(0, 3).every((l) => !l.sealed));
  assert.ok(merged.every((s) => s.lines.every((l) => !l.marker)));
  // The input is not mutated.
  assert.equal(sections[2].lines[3].marker.seq, 2);
});

test("mergeSealed marks a missing segment unavailable", () => {
  const merged = mergeSealed(parseJobLog(FIXTURE), new Map(), () => "artifact expired");
  const line = merged[2].lines[3];
  assert.equal(line.text, "🔒 sealed output unavailable (artifact expired)");
  assert.equal(line.sealed, true);
  assert.equal(line.unavailable, true);
  const fallback = mergeSealed(parseJobLog(FIXTURE), new Map())[2].lines[3];
  assert.match(fallback.text, /^🔒 sealed output unavailable \(.+\)$/);
});

test("renders public log when no sealed artifacts", () => {
  const sections = parseJobLog(FIXTURE);
  const merged = mergeSealed(sections, new Map());
  const plain = (secs) => secs.flatMap((s) => s.lines.filter((l) => !l.marker && !l.unavailable).map((l) => l.text));
  assert.deepEqual(plain(merged), plain(sections));
  assert.equal(merged.length, sections.length);
  // The failed-seal line is not a marker and stays as it is.
  assert.ok(merged[3].lines.some((l) => l.text.startsWith("🔒 seal failed for")));
});

test("parseJobLog starts post-job and complete-job sections", () => {
  const log = [
    "2026-11-20T10:00:00.0000000Z ##[group]Run make",
    "2026-11-20T10:00:01.0000000Z Post job cleanup.",
    "2026-11-20T10:00:02.0000000Z Cleaning up orphan processes",
  ].join("\n");
  assert.deepEqual(parseJobLog(log).map((s) => s.title), ["Run make", "Post job cleanup", "Complete job"]);
});

const URL1 = "https://ci.lascade.com/?run=1&attempt=1";
const markerLine = (rest) => `2026-11-20T10:00:00.0000000Z ${rest}`;

test("parseJobLog parses run and attempt from the marker URL", () => {
  const [s] = parseJobLog(markerLine(`🔒 sealed ${ART}#2 ${URL1}`));
  assert.deepEqual(s.lines[0].marker, { artifact: ART, seq: 2, run: "1", attempt: "1" });
});

test("only a marker that is the whole line counts", () => {
  const log = [
    markerLine(`echo 🔒 sealed ${ART}#2 ${URL1}`),
    markerLine(`🔒 sealed ${ART}#2 ${URL1} trailing`),
    markerLine(`🔒 sealed ${ART}#2`),
  ].join("\n");
  assert.ok(parseJobLog(log)[0].lines.every((l) => !l.marker));
});

test("a marker binds for any attempt of the same run, not for another run", () => {
  const log = [
    markerLine(`🔒 sealed ${ART}#1 https://ci.lascade.com/?run=2&attempt=1`),
    markerLine(`🔒 sealed ${ART}#2 https://ci.lascade.com/?run=1&attempt=1`),
    markerLine(`🔒 sealed ${ART}#3 https://ci.lascade.com/?run=1&attempt=2`),
  ].join("\n");
  const lines = parseJobLog(log, { run: 1 })[0].lines;
  assert.deepEqual(lines.map((l) => l.marker?.seq ?? null), [null, 2, 3]);
  // Without an expected run, every whole-line marker parses.
  assert.equal(parseJobLog(log)[0].lines.filter((l) => l.marker).length, 3);
});

test("mergeSealed consumes each artifact#seq at most once", () => {
  const log = [markerLine(`🔒 sealed ${ART}#2 ${URL1}`), markerLine(`🔒 sealed ${ART}#2 ${URL1}`)].join("\n");
  const merged = mergeSealed(parseJobLog(log), new Map([[`${ART}#2`, ["secret"]]]));
  const texts = merged[0].lines.map((l) => l.text);
  assert.deepEqual(texts, ["secret", "🔒 sealed output unavailable (duplicate marker)"]);
  assert.equal(merged[0].lines[1].unavailable, true);
});

test("parseJobLog drops an empty preamble", () => {
  const sections = parseJobLog("2026-11-20T10:00:00.0000000Z ##[group]Run make\n");
  assert.deepEqual(sections.map((s) => s.title), ["Run make"]);
});
