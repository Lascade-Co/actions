// Where the viewer keeps its settings in localStorage: one shared GitHub token
// and one repo key per repository. `storage` is a Storage (localStorage); its
// methods may throw (blocked site data), which callers handle.
//
// Keys are stored under the lowercase repo name: GitHub matches repo names
// regardless of case, so "Connector" and "connector" are one repo with one key.
import { checkRepo } from "./github.js";
import { isRepoName, repoId, DEFAULT_REPO } from "./runs.js";

export const TOKEN_KEY = "ci.githubToken";
/** The single repo key of the viewer before it opened other repos: the actions repo's. */
export const LEGACY_KEY = "ci.repoKey";
const PREFIX = `${LEGACY_KEY}.`;

/** The storage name of `repo`'s key: `ci.repoKey.<lowercase repo>`. */
export function keyName(repo) {
  return PREFIX + repoId(checkRepo(repo));
}

function names(storage) {
  const out = [];
  for (let i = 0; i < storage.length; i++) {
    const k = storage.key(i);
    if (k !== null) out.push(k);
  }
  return out;
}

// Moves `from` to `to` unless `to` is already set, then removes `from` (only
// after the write succeeded).
function move(storage, from, to) {
  const value = storage.getItem(from);
  if (value && !storage.getItem(to)) storage.setItem(to, value);
  storage.removeItem(from);
}

/**
 * Brings older storage up to date, once: the legacy `ci.repoKey` moves to
 * `ci.repoKey.actions`, and mixed-case `ci.repoKey.<Repo>` entries move to their
 * lowercase names. An existing target is never overwritten. Returns whether
 * anything was moved.
 */
export function migrateKeys(storage) {
  let moved = false;
  if (storage.getItem(LEGACY_KEY) !== null) {
    move(storage, LEGACY_KEY, keyName(DEFAULT_REPO));
    moved = true;
  }
  for (const k of names(storage)) {
    if (!k.startsWith(PREFIX)) continue;
    const repo = k.slice(PREFIX.length);
    if (isRepoName(repo) && repo !== repoId(repo)) {
      move(storage, k, keyName(repo));
      moved = true;
    }
  }
  return moved;
}

/**
 * Every stored repo key with a valid repo name: Map lowercase repo -> key. A
 * lowercase entry wins over a mixed-case one not yet migrated.
 */
export function loadKeys(storage) {
  const keys = new Map();
  for (const k of names(storage)) {
    if (!k.startsWith(PREFIX)) continue;
    const repo = k.slice(PREFIX.length);
    const value = storage.getItem(k);
    if (!isRepoName(repo) || !value) continue;
    const id = repoId(repo);
    if (repo === id || !keys.has(id)) keys.set(id, value);
  }
  return keys;
}

export function saveKey(storage, repo, key) {
  storage.setItem(keyName(repo), key);
}

/** Removes `repo`'s key, under any case (an entry the migration could not move included). */
export function forgetKey(storage, repo) {
  const target = keyName(repo);
  for (const k of names(storage)) {
    if (k.startsWith(PREFIX) && PREFIX + repoId(k.slice(PREFIX.length)) === target) storage.removeItem(k);
  }
}

/** Removes the token, the legacy key and every `ci.repoKey.*` entry, valid name or not. */
export function forgetAll(storage) {
  for (const k of names(storage)) {
    if (k === TOKEN_KEY || k === LEGACY_KEY || k.startsWith(PREFIX)) storage.removeItem(k);
  }
}
