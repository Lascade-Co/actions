import { test } from "node:test";
import assert from "node:assert/strict";
import { keyName, migrateKeys, loadKeys, saveKey, forgetKey, forgetAll, LEGACY_KEY, TOKEN_KEY } from "./keys.js";

// A Storage stand-in (localStorage's interface).
class MemoryStorage {
  constructor(entries = {}) {
    this.map = new Map(Object.entries(entries));
  }
  get length() {
    return this.map.size;
  }
  key(i) {
    return [...this.map.keys()][i] ?? null;
  }
  getItem(k) {
    return this.map.has(k) ? this.map.get(k) : null;
  }
  setItem(k, v) {
    this.map.set(k, String(v));
  }
  removeItem(k) {
    this.map.delete(k);
  }
}

test("each repo's key has its own storage name", () => {
  assert.equal(LEGACY_KEY, "ci.repoKey");
  assert.equal(TOKEN_KEY, "ci.githubToken");
  assert.equal(keyName("actions"), "ci.repoKey.actions");
  assert.equal(keyName("connector"), "ci.repoKey.connector");
  assert.throws(() => keyName(".."), /repository/i);
  assert.throws(() => keyName("a/b"), /repository/i);
});

test("migrateKeys moves the legacy key to the actions repo", () => {
  const s = new MemoryStorage({ "ci.repoKey": "AGE-SECRET-KEY-1OLD", "ci.githubToken": "tok", "ci.repoKey.connector": "C" });
  assert.equal(migrateKeys(s), true);
  assert.equal(s.getItem("ci.repoKey.connector"), "C");
  assert.equal(s.getItem("ci.repoKey.actions"), "AGE-SECRET-KEY-1OLD");
  assert.equal(s.getItem("ci.repoKey"), null);
  assert.equal(s.getItem("ci.githubToken"), "tok");
  // Runs once: nothing left to migrate.
  assert.equal(migrateKeys(s), false);
  assert.equal(s.getItem("ci.repoKey.actions"), "AGE-SECRET-KEY-1OLD");
});

test("migrateKeys keeps an existing actions key and drops the legacy one", () => {
  const s = new MemoryStorage({ "ci.repoKey": "AGE-SECRET-KEY-1OLD", "ci.repoKey.actions": "AGE-SECRET-KEY-1NEW" });
  migrateKeys(s);
  assert.equal(s.getItem("ci.repoKey.actions"), "AGE-SECRET-KEY-1NEW");
  assert.equal(s.getItem("ci.repoKey"), null);
});

test("migrateKeys keeps the legacy key when the new one cannot be written", () => {
  const s = new MemoryStorage({ "ci.repoKey": "AGE-SECRET-KEY-1OLD" });
  s.setItem = () => {
    throw new Error("QuotaExceededError");
  };
  assert.throws(() => migrateKeys(s));
  assert.equal(s.getItem("ci.repoKey"), "AGE-SECRET-KEY-1OLD");
});

test("loadKeys returns every valid repo's key", () => {
  const s = new MemoryStorage({
    "ci.repoKey.actions": "A",
    "ci.repoKey.connector": "B",
    "ci.repoKey..": "bad name",
    "ci.repoKey.a/b": "bad name",
    "ci.repoKey.empty": "",
    "ci.githubToken": "tok",
    other: "x",
  });
  assert.deepEqual([...loadKeys(s)].sort(), [["actions", "A"], ["connector", "B"]]);
});

test("saveKey and forgetKey change only that repo's key", () => {
  const s = new MemoryStorage({ "ci.repoKey.actions": "A", "ci.githubToken": "tok" });
  saveKey(s, "connector", "B");
  assert.equal(s.getItem("ci.repoKey.connector"), "B");
  saveKey(s, "connector", "C");
  assert.equal(s.getItem("ci.repoKey.connector"), "C");
  forgetKey(s, "connector");
  assert.equal(s.getItem("ci.repoKey.connector"), null);
  assert.equal(s.getItem("ci.repoKey.actions"), "A");
  assert.equal(s.getItem("ci.githubToken"), "tok");
  assert.throws(() => saveKey(s, "..", "X"), /repository/i);
});

test("repo keys are stored under the lowercase repo name", () => {
  assert.equal(keyName("Connector"), "ci.repoKey.connector");
  const s = new MemoryStorage();
  saveKey(s, "Connector", "B");
  assert.equal(s.getItem("ci.repoKey.connector"), "B");
  assert.deepEqual([...loadKeys(s)], [["connector", "B"]]);
  saveKey(s, "CONNECTOR", "C");
  assert.deepEqual([...loadKeys(s)], [["connector", "C"]]);
  forgetKey(s, "Connector");
  assert.equal(loadKeys(s).size, 0);
});

test("migrateKeys lowercases mixed-case repo keys once, keeping an existing lowercase one", () => {
  const s = new MemoryStorage({
    "ci.repoKey.Connector": "OLD-MIXED",
    "ci.repoKey.connector": "LOWER",
    "ci.repoKey.Website": "W",
    "ci.repoKey.a/B": "bad name",
  });
  assert.equal(migrateKeys(s), true);
  assert.equal(s.getItem("ci.repoKey.connector"), "LOWER");
  assert.equal(s.getItem("ci.repoKey.website"), "W");
  assert.equal(s.getItem("ci.repoKey.Connector"), null);
  assert.equal(s.getItem("ci.repoKey.Website"), null);
  assert.deepEqual([...loadKeys(s)].sort(), [["connector", "LOWER"], ["website", "W"]]);
  assert.equal(migrateKeys(s), false);
});

test("loadKeys lists each repo once even before migration", () => {
  const s = new MemoryStorage({ "ci.repoKey.Connector": "MIXED", "ci.repoKey.connector": "LOWER", "ci.repoKey.Other": "O" });
  assert.deepEqual([...loadKeys(s)].sort(), [["connector", "LOWER"], ["other", "O"]]);
  forgetKey(s, "connector");
  assert.deepEqual([...loadKeys(s)], [["other", "O"]]);
});

test("forgetAll removes the token, the legacy key and every repo key, even unlisted ones", () => {
  const s = new MemoryStorage({
    "ci.repoKey": "L",
    "ci.repoKey.actions": "A",
    "ci.repoKey.connector": "B",
    "ci.repoKey..": "bad name",
    "ci.githubToken": "tok",
    unrelated: "keep",
  });
  forgetAll(s);
  assert.deepEqual([...s.map.keys()], ["unrelated"]);
});
