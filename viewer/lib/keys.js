// Where the viewer keeps its settings in localStorage: one shared GitHub token
// and one repo key per repository. `storage` is a Storage (localStorage); its
// methods may throw (blocked site data), which callers handle.
import { checkRepo } from "./github.js";
import { isRepoName, DEFAULT_REPO } from "./runs.js";

export const TOKEN_KEY = "ci.githubToken";
/** The single repo key of the viewer before it opened other repos: the actions repo's. */
export const LEGACY_KEY = "ci.repoKey";
const PREFIX = `${LEGACY_KEY}.`;

/** The storage name of `repo`'s key: `ci.repoKey.<repo>`. */
export function keyName(repo) {
  return PREFIX + checkRepo(repo);
}

/**
 * Moves the legacy `ci.repoKey` to `ci.repoKey.actions` unless that is already
 * set, then removes the legacy key (only after the write succeeded). Returns
 * whether there was a legacy key.
 */
export function migrateKeys(storage) {
  const legacy = storage.getItem(LEGACY_KEY);
  if (legacy === null) return false;
  const target = keyName(DEFAULT_REPO);
  if (legacy && !storage.getItem(target)) storage.setItem(target, legacy);
  storage.removeItem(LEGACY_KEY);
  return true;
}

function names(storage) {
  const out = [];
  for (let i = 0; i < storage.length; i++) {
    const k = storage.key(i);
    if (k !== null) out.push(k);
  }
  return out;
}

/** Every stored repo key with a valid repo name: Map repo -> key. */
export function loadKeys(storage) {
  const keys = new Map();
  for (const k of names(storage)) {
    if (!k.startsWith(PREFIX)) continue;
    const repo = k.slice(PREFIX.length);
    const value = storage.getItem(k);
    if (isRepoName(repo) && value) keys.set(repo, value);
  }
  return keys;
}

export function saveKey(storage, repo, key) {
  storage.setItem(keyName(repo), key);
}

export function forgetKey(storage, repo) {
  storage.removeItem(keyName(repo));
}

/** Removes the token, the legacy key and every `ci.repoKey.*` entry, valid name or not. */
export function forgetAll(storage) {
  for (const k of names(storage)) {
    if (k === TOKEN_KEY || k === LEGACY_KEY || k.startsWith(PREFIX)) storage.removeItem(k);
  }
}
