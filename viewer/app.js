// CI Logs: opens sealed Actions logs and artifacts of the public Lascade-Co repos.
import { getRun, listJobs, jobLog, listArtifacts, artifactZip, listRuns, listWorkflows, listSealedRepos, listPublicRepos, repoPublicKey } from "./lib/github.js";
import { parseJobLog, mergeSealed } from "./lib/joblog.js";
import { openSealedLog, openSealedArtifact, checkRepoKey, checkKeyForRepo, SealError } from "./lib/sealed.js";
import { ansiRenderer, stripAnsi } from "./lib/ansi.js";
import { runRow, repoRow, runState, pageInfo, maxPage, readListParams, readRoute, parsePick, isRepoName, statusOptions, duration, OWNER, DEFAULT_REPO, PER_PAGE, SEARCH_LIMIT } from "./lib/runs.js";
import { exportText, exportFilename } from "./lib/export.js";
import { migrateKeys, loadKeys, saveKey, forgetKey, forgetAll, TOKEN_KEY, LEGACY_KEY } from "./lib/keys.js";

const PREVIEW_MAX = 1024 * 1024; // per file
const PREVIEW_BUDGET = 5 * 1024 * 1024; // per unsealed artifact
const ENTRY_ROWS_MAX = 1000;
const EXPORT_WORKERS = 3; // jobs fetched at once by "All jobs" export
const downloadUrls = new Set();

const $ = (id) => document.getElementById(id);
const el = (tag, props = {}, ...children) => {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") e.className = v;
    else if (k === "dataset") Object.assign(e.dataset, v);
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null && v !== false) e.setAttribute(k, v === true ? "" : v);
  }
  e.append(...children.filter((c) => c !== null && c !== undefined && c !== false));
  return e;
};

// --- storage (may be unavailable: private windows, blocked site data) -------

function load(k) {
  try {
    return localStorage.getItem(k) ?? "";
  } catch {
    return "";
  }
}
function store(k, v) {
  try {
    if (v) localStorage.setItem(k, v);
    else localStorage.removeItem(k);
    return true;
  } catch {
    return false;
  }
}

// Runs `fn(localStorage)`; false when storage is unavailable or refuses the change.
function withStorage(fn) {
  try {
    fn(localStorage);
    return true;
  } catch {
    return false;
  }
}

// One shared token and one key per repo (ci.repoKey.<repo>). The single key of
// older versions (ci.repoKey) was the actions repo's and moves there once.
function loadSettings() {
  withStorage(migrateKeys);
  let keys = new Map();
  withStorage((s) => (keys = loadKeys(s)));
  // Storage refused the migration: keep using the legacy key for this tab.
  const legacy = load(LEGACY_KEY);
  if (legacy && !keys.has(DEFAULT_REPO)) keys.set(DEFAULT_REPO, legacy);
  return { token: load(TOKEN_KEY), keys };
}

const settings = loadSettings();

// Session caches of each repo's public key (repo -> Promise<age1…>) and of the
// check that the saved key matches it (repo -> {key, promise}). Forget clears both.
const publicKeys = new Map();
const verifiedKeys = new Map();

function publicKeyFor(repo, token = settings.token) {
  if (!publicKeys.has(repo)) {
    const p = repoPublicKey(token, repo);
    p.catch(() => publicKeys.get(repo) === p && publicKeys.delete(repo));
    publicKeys.set(repo, p);
  }
  return publicKeys.get(repo);
}

/**
 * The saved key of `repo` once it is checked against the repo's repo-key.pub,
 * or null when none is saved. Rejects ("wrong-repo") when the key belongs to
 * another repo and ("unverified") when the public key cannot be read, so a key
 * is never tried on output it was not checked for.
 */
function verifiedKey(repo) {
  const key = settings.keys.get(repo);
  if (!key) return Promise.resolve(null);
  const cached = verifiedKeys.get(repo);
  if (cached?.key === key) return cached.promise;
  const promise = publicKeyFor(repo).then(
    (pub) => checkKeyForRepo(key, pub, repo),
    (err) => {
      throw new SealError(`Could not check the repo key for ${repo} against its public key: ${err?.message ?? err}`, "unverified");
    },
  );
  promise.catch(() => verifiedKeys.get(repo)?.promise === promise && verifiedKeys.delete(repo));
  verifiedKeys.set(repo, { key, promise });
  return promise;
}

/** The repo being browsed (picker: null). */
function currentRepo() {
  return readRoute(params()).repo;
}

function hasKey(repo) {
  return Boolean(repo && settings.keys.get(repo));
}

// --- banner and notice ------------------------------------------------------

// A sticky banner (the storage warning) survives the run list's banner reset.
function banner(message, { settingsAction = false, sticky = false } = {}) {
  $("banner-text").textContent = message;
  $("banner-action").hidden = !settingsAction;
  $("banner").dataset.sticky = sticky ? "1" : "";
  $("banner").hidden = false;
}
function clearBanner() {
  $("banner").hidden = true;
}
function notice(message) {
  $("notice").textContent = message ?? "";
  $("notice").hidden = !message;
}

function report(err) {
  const status = err?.status;
  const code = err?.code;
  const keyAction = ["bad-key", "no-key", "wrong-repo"].includes(code);
  banner(err?.message ?? String(err), { settingsAction: status === 401 || keyAction });
}

// --- settings dialog --------------------------------------------------------

function fieldError(id, message) {
  $(id).textContent = message ?? "";
  $(id).hidden = !message;
}

// Bumped whenever the dialog opens, closes or forgets, so a key check still in
// flight never saves into a dialog the user has left.
let dialogGen = 0;

function cancelPendingSave() {
  dialogGen++;
  $("save-settings").disabled = false;
  keyStatus("");
}

function keyStatus(message) {
  $("key-status").textContent = message ?? "";
  $("key-status").hidden = !message;
}

function renderKeyList() {
  const repos = [...settings.keys.keys()].sort((a, b) => a.localeCompare(b));
  $("key-list").replaceChildren(...repos.map((repo) => el("li", {},
    el("code", {}, repo),
    el("button", { type: "button", class: "btn btn-small", "aria-label": `Replace the key for ${repo}`, onclick: () => replaceKey(repo) }, "Replace"),
    el("button", { type: "button", class: "btn btn-small btn-danger", "aria-label": `Forget the key for ${repo}`, onclick: () => forgetOneKey(repo) }, "Forget"))));
  $("key-list-empty").hidden = repos.length > 0;
}

function openSettings() {
  cancelPendingSave();
  const repo = currentRepo() ?? "";
  $("key-repo").value = repo;
  $("repo-key").value = "";
  $("repo-key").placeholder = hasKey(repo) ? "Saved: paste a new key to replace it" : "AGE-SECRET-KEY-1…";
  $("gh-token").value = settings.token;
  fieldError("key-error");
  fieldError("token-error");
  renderKeyList();
  if (!$("settings").open) $("settings").showModal();
}

function replaceKey(repo) {
  $("key-repo").value = repo;
  $("repo-key").value = "";
  $("repo-key").placeholder = "Paste the new key";
  fieldError("key-error");
  $("repo-key").focus();
}

// Drops whatever was decrypted with `repo`'s key: it is only ever the repo shown.
function dropDecrypted(repo) {
  verifiedKeys.delete(repo);
  if (state.repo === repo) {
    clearDecrypted();
    route();
  }
}

function forgetOneKey(repo) {
  cancelPendingSave();
  withStorage((s) => forgetKey(s, repo));
  settings.keys.delete(repo);
  if (repo === DEFAULT_REPO) store(LEGACY_KEY, "");
  renderKeyList();
  dropDecrypted(repo);
}

async function saveSettings(ev) {
  ev.preventDefault();
  const gen = ++dialogGen;
  const repo = $("key-repo").value.trim();
  const rawKey = $("repo-key").value.trim();
  const token = $("gh-token").value.trim();
  let key = "";
  let ok = true;
  fieldError("key-error");
  keyStatus("");
  if (rawKey) {
    try {
      if (!isRepoName(repo)) throw new Error(repo ? `“${repo}” is not a repository name.` : "Enter the repository this key is for.");
      key = checkRepoKey(rawKey);
    } catch (err) {
      fieldError("key-error", err.message);
      ok = false;
    }
  }
  if (!token) {
    fieldError("token-error", "A GitHub token is required to read the run's logs.");
    ok = false;
  } else if (/\s/.test(token)) {
    fieldError("token-error", "The token contains spaces; paste it again.");
    ok = false;
  } else fieldError("token-error");
  if (!ok) return;
  if (key) {
    // Accept the key only when it opens this repo's output: its recipient must be
    // the repo's committed repo-key.pub.
    $("save-settings").disabled = true;
    keyStatus(`Checking the key against the public key of ${OWNER}/${repo}…`);
    try {
      key = await checkKeyForRepo(key, await publicKeyFor(repo, token), repo);
    } catch (err) {
      if (gen !== dialogGen) return;
      fieldError("key-error", err?.code === "wrong-repo" || err?.code === "bad-key" ? err.message : `Could not check the key: ${err?.message ?? err}`);
      return;
    } finally {
      if (gen === dialogGen) {
        $("save-settings").disabled = false;
        keyStatus("");
      }
    }
    if (gen !== dialogGen || !$("settings").open) return;
  }
  const tokenChanged = token !== settings.token;
  const keyChanged = Boolean(key) && key !== settings.keys.get(repo);
  settings.token = token;
  let saved = store(TOKEN_KEY, token);
  if (key) {
    settings.keys.set(repo, key);
    saved = withStorage((s) => saveKey(s, repo, key)) && saved;
  }
  $("settings").close();
  if (!saved) banner("This browser blocks local storage, so the settings last only until you close the tab.", { sticky: true });
  if (tokenChanged) {
    if (keyChanged) verifiedKeys.delete(repo);
    clearDecrypted();
    route();
  } else if (keyChanged) dropDecrypted(repo);
}

// Forget all: the token, every repo's key (stored ones included, listed or not),
// the public key caches and everything decrypted.
function forgetSettings() {
  cancelPendingSave();
  withStorage(forgetAll);
  store(TOKEN_KEY, "");
  store(LEGACY_KEY, "");
  settings.token = "";
  settings.keys.clear();
  publicKeys.clear();
  verifiedKeys.clear();
  state.sealedRepos = null;
  $("repo-key").value = "";
  $("gh-token").value = "";
  $("key-repo").value = "";
  renderKeyList();
  clearDecrypted();
  $("settings").close();
  route();
}

// --- state ------------------------------------------------------------------

const state = {
  repo: null, // the repo whose runs are loaded below
  sealedRepos: null, // Promise<repos with the sealed-ci topic> for the picker
  runId: null,
  run: null,
  attempt: null,
  jobs: [],
  artifacts: [],
  sealedLogs: new Map(), // artifact id -> Promise<openSealedLog result>
  sealedArtifacts: new Map(), // artifact id -> Promise<openSealedArtifact result>
  view: null, // { sections, rendered: Set, query }
  job: null, // the job shown in the log view
  jobLogs: new Map(), // job id -> Promise<public log text>
  workflows: null, // Promise<workflow list> for the runs filter
  exporting: false,
  seq: 0,
  gen: 0, // bumped when the run or settings change: cancels exports
};

// Drops everything loaded or decrypted: cached sealed data, rendered log lines,
// artifact previews and view state. In-flight loads are abandoned (seq bump).
function clearDecrypted() {
  state.seq++;
  state.gen++;
  Object.assign(state, { runId: null, run: null, attempt: null, jobs: [], artifacts: [], view: null, job: null, workflows: null, exporting: false });
  state.sealedLogs.clear();
  state.sealedArtifacts.clear();
  state.jobLogs.clear();
  for (const url of [...downloadUrls]) revokeDownload(url);
  for (const id of ["log", "artifacts", "jobs", "run-meta", "job-title"]) $(id).replaceChildren();
  $("search").value = "";
  $("artifacts-count").textContent = "";
  $("run").hidden = true;
  $("runs").replaceChildren();
  $("pager").replaceChildren();
  $("repos").replaceChildren();
  $("repos-pager").replaceChildren();
  setExportStatus("");
  updateExportControls();
  notice("");
}

function params() {
  return new URLSearchParams(location.search);
}

// Changes the URL query (all of it with `reset`) and routes to it.
function navigate(changes, { push = true, reset = false } = {}) {
  const p = reset ? new URLSearchParams() : params();
  for (const [k, v] of Object.entries(changes)) {
    if (v === null || v === undefined || v === "") p.delete(k);
    else p.set(k, String(v));
  }
  const qs = p.toString();
  const url = qs ? `${location.pathname}?${qs}` : location.pathname;
  if (push) history.pushState(null, "", url);
  else history.replaceState(null, "", url + location.hash);
  route();
}

// --- formatting -------------------------------------------------------------

function bytes(n) {
  if (n < 1024) return `${n} B`;
  const u = ["KB", "MB", "GB"];
  let i = -1;
  do {
    n /= 1024;
    i++;
  } while (n >= 1024 && i < u.length - 1);
  return `${n.toFixed(n < 10 ? 1 : 0)} ${u[i]}`;
}

const jobState = runState;

// A plain left click on a link we route ourselves; other clicks (new tab,
// middle click) are left to the browser.
function plainClick(ev) {
  return ev.button === 0 && !ev.metaKey && !ev.ctrlKey && !ev.shiftKey && !ev.altKey;
}

const STATE_LABEL = {
  success: "Succeeded",
  failure: "Failed",
  cancelled: "Cancelled",
  skipped: "Skipped",
  timed_out: "Timed out",
  action_required: "Action required",
  neutral: "Neutral",
  running: "In progress",
  queued: "Queued",
};

// --- routing ----------------------------------------------------------------

async function route() {
  const p = params();
  const r = readRoute(p);
  // Another repo: drop the last one's run, caches and workflow list.
  if (r.repo && r.repo !== state.repo) {
    clearDecrypted();
    state.repo = r.repo;
  }
  if (r.view === "repos") {
    showRepos(p, r.error);
    return;
  }
  if (r.view === "runs") {
    showRuns(r.error ? new URLSearchParams({ repo: r.repo }) : p, r.repo, r.error);
    return;
  }
  const { repo, run: runParam } = r;
  $("home").hidden = true;
  if (!settings.token) {
    $("run").hidden = true;
    banner("Add a GitHub token in Settings to open this run.", { settingsAction: true });
    if (!$("settings").open) openSettings();
    return;
  }
  const attemptParam = p.get("attempt");
  if (attemptParam && !/^\d+$/.test(attemptParam)) {
    banner(`“${attemptParam}” is not an attempt number; showing the latest attempt.`);
  }
  const wanted = attemptParam && /^\d+$/.test(attemptParam) ? Number(attemptParam) : null;
  const seq = ++state.seq;
  try {
    if (!state.run || state.runId !== runParam || state.repo !== repo || (wanted && wanted !== state.attempt) || (!wanted && state.attempt !== state.run.run_attempt)) {
      clearBanner();
      cancelExport();
      $("run").hidden = false;
      $("job-title").textContent = "Loading run…";
      $("log").replaceChildren();
      notice("");
      const run = await getRun(settings.token, repo, runParam);
      const attempt = wanted ?? run.run_attempt ?? 1;
      const [jobs, artifacts] = await Promise.all([
        listJobs(settings.token, repo, runParam, attempt),
        listArtifacts(settings.token, repo, runParam),
      ]);
      if (seq !== state.seq) return;
      state.gen++;
      Object.assign(state, { runId: runParam, run, attempt, jobs, artifacts, job: null, exporting: false });
      state.sealedLogs.clear();
      state.sealedArtifacts.clear();
      state.jobLogs.clear();
      setExportStatus("");
      renderRunMeta();
      renderJobs();
    }
    if (seq !== state.seq) return;
    $("run").hidden = false;
    if (p.get("view") === "artifacts") {
      showArtifacts();
      return;
    }
    const jobParam = p.get("job");
    const job =
      state.jobs.find((j) => String(j.id) === jobParam) ??
      state.jobs.find((j) => ["failure", "timed_out", "cancelled"].includes(j.conclusion)) ??
      state.jobs[0];
    if (!job) {
      showEmpty("This attempt has no jobs.");
      return;
    }
    await showJob(job, seq);
  } catch (err) {
    if (seq !== state.seq) return;
    $("log").replaceChildren();
    if (!state.run) {
      $("job-title").textContent = "";
      $("run").hidden = true;
    }
    report(err);
  }
}

// --- recent runs (home) ----------------------------------------------------

// Shows the home section with the repo picker (`repo` null) or a repo's runs.
function showHome(repo, error) {
  $("run").hidden = true;
  $("run-meta").replaceChildren();
  $("home").hidden = false;
  $("repos-view").hidden = Boolean(repo);
  $("runs-view").hidden = !repo;
  $("home-back").hidden = !repo;
  if (repo) $("home-h").replaceChildren("Recent runs ", el("span", { class: "muted home-repo" }, "of ", el("code", {}, `${OWNER}/${repo}`)));
  else $("home-h").replaceChildren("Repositories");
  document.title = repo ? `${repo} · CI Logs` : "CI Logs";
  fieldError("pick-error", error);
  let hint = "";
  if (!settings.token) {
    hint = repo
      ? `Without a token GitHub allows 60 requests an hour. Add a GitHub token and the repo key for ${repo} in Settings to raise the limit and open runs.`
      : "Without a token GitHub allows 60 requests an hour. Add a GitHub token in Settings to raise the limit and open runs.";
  } else if (repo && !hasKey(repo)) hint = `Add the repo key for ${repo} in Settings to see its sealed output.`;
  $("home-hint-text").textContent = hint;
  $("home-hint").hidden = !hint;
  if (!$("banner").dataset.sticky) clearBanner();
  cancelExport();
}

function showRuns(p, repo, error) {
  showHome(repo, error);
  const q = readListParams(p);
  $("filter-status").replaceChildren(...statusOptions(q.status).map(([value, label]) => el("option", { value }, label)));
  $("filter-status").value = q.status;
  fillWorkflows(repo, q.workflow);
  loadRuns(q, repo);
}

async function loadRuns(q, repo) {
  // GitHub pages through at most 1000 runs of a status search (422 beyond).
  const limit = q.status ? SEARCH_LIMIT : Infinity;
  if (q.page > maxPage(PER_PAGE, limit)) {
    navigate({ page: maxPage(PER_PAGE, limit) }, { push: false });
    return;
  }
  const seq = ++state.seq;
  const list = $("runs");
  list.setAttribute("aria-busy", "true");
  // Keep the old rows (dimmed) while the next page loads, so nothing jumps.
  list.classList.add("stale");
  if (!list.children.length) runsMessage("Loading runs…");
  try {
    const { total, runs } = await listRuns(settings.token, repo, { page: q.page, perPage: PER_PAGE, workflow: q.workflow, status: q.status });
    if (seq !== state.seq) return;
    const info = pageInfo(total, q.page, PER_PAGE, limit);
    if (q.page > info.pages) {
      // Past the last page: show the last page instead.
      navigate({ page: info.pages === 1 ? null : info.pages }, { push: false });
      return;
    }
    const now = Date.now();
    const rows = runs.map((r) => runRow(r, now, repo)).filter(Boolean);
    list.replaceChildren(...rows.map(runItem));
    runsMessage(rows.length ? "" : "No runs.");
    $("runs-total").textContent = `${total.toLocaleString("en")} run${total === 1 ? "" : "s"}`;
    renderPager(info);
  } catch (err) {
    if (seq !== state.seq) return;
    list.replaceChildren();
    $("pager").replaceChildren();
    $("runs-total").textContent = "";
    runsMessage("Could not load runs.");
    banner(err?.message ?? String(err), { settingsAction: [401, 403, 429].includes(err?.status) });
  } finally {
    if (seq === state.seq) {
      list.setAttribute("aria-busy", "false");
      list.classList.remove("stale");
    }
  }
}

function runsMessage(message) {
  $("runs-msg").textContent = message ?? "";
  $("runs-msg").hidden = !message;
}

// Loads the repo's workflow list once (it costs a request) and selects `selected`.
async function fillWorkflows(repo, selected) {
  const select = $("filter-workflow");
  if (!state.workflows) {
    const p = listWorkflows(settings.token, repo);
    p.catch(() => state.workflows === p && (state.workflows = null));
    state.workflows = p;
  }
  const pending = state.workflows;
  let workflows = [];
  try {
    workflows = await pending;
  } catch {
    // The list still works without the filter's names.
  }
  if (state.repo !== repo || (pending !== state.workflows && state.workflows !== null)) return;
  const options = [el("option", { value: "" }, "All workflows")];
  const sorted = [...workflows].filter((w) => /^\d+$/.test(String(w.id))).sort((a, b) => String(a.name).localeCompare(String(b.name)));
  for (const w of sorted) options.push(el("option", { value: String(w.id) }, w.name || w.path || `Workflow ${w.id}`));
  // Keep the URL's choice selectable even when the list could not load.
  const current = readListParams(params()).workflow || selected;
  if (current && !sorted.some((w) => String(w.id) === current)) options.push(el("option", { value: current }, `Workflow ${current}`));
  select.replaceChildren(...options);
  select.value = current;
}

function listHref(changes) {
  const p = params();
  for (const [k, v] of Object.entries(changes)) {
    if (v === null || v === undefined || v === "" || (k === "page" && v === 1)) p.delete(k);
    else p.set(k, String(v));
  }
  const qs = p.toString();
  return qs ? `?${qs}` : location.pathname;
}

function runItem(row) {
  const label = STATE_LABEL[row.state] ?? row.state;
  const sub = [`${row.workflow} #${row.number}`, row.event].filter(Boolean).join(" · ");
  const time = [row.when, row.duration].filter(Boolean).join(" · ");
  return el("li", {},
    el("a", {
      class: "run-row",
      href: row.href,
      onclick: (ev) => {
        if (!plainClick(ev)) return;
        ev.preventDefault();
        navigate({ repo: state.repo, run: row.id, attempt: row.attempt > 1 ? row.attempt : null }, { reset: true });
        window.scrollTo(0, 0);
      },
    },
    el("span", { class: `icon icon-${row.state}`, "aria-label": label, role: "img", title: label }),
    el("span", { class: "run-main" },
      el("span", { class: "run-title" }, row.title),
      el("span", { class: "run-sub" }, sub)),
    el("span", { class: "run-side" },
      row.branch ? el("span", { class: "chip run-branch", title: row.branch }, row.branch) : null,
      el("span", { class: "run-time" },
        row.startedAt ? el("time", { datetime: row.startedAt, title: new Date(row.startedAt).toLocaleString() }, time) : time,
        row.attempt > 1 ? el("span", { class: "run-attempt" }, `Attempt ${row.attempt}`) : null))),
  );
}

function renderPager(info) {
  const link = (page, text, enabled, rel) => enabled
    ? el("a", {
      class: "btn btn-small",
      href: listHref({ page }),
      rel,
      onclick: (ev) => {
        if (!plainClick(ev)) return;
        ev.preventDefault();
        navigate({ page: page === 1 ? null : page });
        window.scrollTo(0, 0);
      },
    }, text)
    : el("span", { class: "btn btn-small", "aria-disabled": "true" }, text);
  $("pager").replaceChildren(
    link(info.page - 1, "Previous", info.hasPrev, "prev"),
    el("span", { class: "pager-pos" }, `Page ${info.page} of ${info.pages}`),
    link(info.page + 1, "Next", info.hasNext, "next"),
  );
}

function changeFilter(key, value) {
  navigate({ [key]: value || null, page: null });
}

function submitPick(ev) {
  ev.preventDefault();
  const pick = parsePick($("pick-input").value, currentRepo());
  if (pick.error) {
    fieldError("pick-error", pick.error);
    return;
  }
  $("pick-input").value = "";
  navigate({ repo: pick.repo, run: pick.run, attempt: pick.attempt, job: pick.job }, { reset: true });
}

// --- repositories (home) ---------------------------------------------------

function showRepos(p, error) {
  showHome(null, error);
  const all = p.get("all") === "1";
  const pageParam = p.get("page") ?? "";
  const page = all && /^\d+$/.test(pageParam) && Number(pageParam) >= 1 ? Number(pageParam) : 1;
  $("show-all-repos").checked = all;
  loadRepos(all, page);
}

// The sealed-ci search, once per page load (search allows 10 requests a minute unauthenticated).
function sealedRepos() {
  if (!state.sealedRepos) {
    const p = listSealedRepos(settings.token);
    p.catch(() => state.sealedRepos === p && (state.sealedRepos = null));
    state.sealedRepos = p;
  }
  return state.sealedRepos;
}

function reposMessage(message) {
  $("repos-msg").textContent = message ?? "";
  $("repos-msg").hidden = !message;
}

async function loadRepos(all, page) {
  const seq = ++state.seq;
  const list = $("repos");
  list.setAttribute("aria-busy", "true");
  list.classList.add("stale");
  if (!list.children.length) reposMessage("Loading repositories…");
  try {
    let rows;
    let hasNext = false;
    const now = Date.now();
    if (all) {
      // Marking sealed repos uses the search too, but the list does not need it.
      const [listed, sealed] = await Promise.all([listPublicRepos(settings.token, page), sealedRepos().catch(() => [])]);
      hasNext = listed.hasNext;
      const names = new Set(sealed.map((r) => r?.name));
      rows = listed.repos.map((r) => repoRow(r, now, names)).filter(Boolean).sort((a, b) => a.name.localeCompare(b.name));
    } else {
      const sealed = await sealedRepos();
      rows = sealed.map((r) => repoRow({ ...r, topics: ["sealed-ci"] }, now)).filter(Boolean)
        .sort((a, b) => String(b.pushedAt ?? "").localeCompare(String(a.pushedAt ?? "")));
    }
    if (seq !== state.seq) return;
    list.replaceChildren(...rows.map(repoItem));
    if (rows.length) reposMessage("");
    else reposMessage(all ? "No public repositories." : "No repositories have the sealed-ci topic yet. Show all public repos to browse the rest.");
    $("repos-total").textContent = all ? "" : `${rows.length} sealed repositor${rows.length === 1 ? "y" : "ies"}`;
    renderReposPager(all, page, hasNext);
  } catch (err) {
    if (seq !== state.seq) return;
    list.replaceChildren();
    $("repos-pager").replaceChildren();
    $("repos-total").textContent = "";
    reposMessage("Could not load repositories.");
    banner(err?.message ?? String(err), { settingsAction: [401, 403, 429].includes(err?.status) });
  } finally {
    if (seq === state.seq) {
      list.setAttribute("aria-busy", "false");
      list.classList.remove("stale");
    }
  }
}

function repoItem(row) {
  return el("li", {},
    el("a", {
      class: "run-row repo-row",
      href: row.href,
      onclick: (ev) => {
        if (!plainClick(ev)) return;
        ev.preventDefault();
        navigate({ repo: row.name }, { reset: true });
        window.scrollTo(0, 0);
      },
    },
    el("span", { class: "run-main" },
      el("span", { class: "run-title" }, row.name),
      row.description ? el("span", { class: "run-sub" }, row.description) : null),
    el("span", { class: "run-side" },
      row.sealed ? null : el("span", { class: "chip chip-muted", title: "Its logs have no sealed output" }, "not sealed"),
      row.pushedAt
        ? el("span", { class: "run-time" }, el("time", { datetime: row.pushedAt, title: new Date(row.pushedAt).toLocaleString() }, `pushed ${row.when}`))
        : null)),
  );
}

function renderReposPager(all, page, hasNext) {
  if (!all || (page === 1 && !hasNext)) {
    $("repos-pager").replaceChildren();
    return;
  }
  const link = (to, text, enabled, rel) => enabled
    ? el("a", {
      class: "btn btn-small",
      href: `?all=1${to > 1 ? `&page=${to}` : ""}`,
      rel,
      onclick: (ev) => {
        if (!plainClick(ev)) return;
        ev.preventDefault();
        navigate({ all: 1, page: to === 1 ? null : to }, { reset: true });
        window.scrollTo(0, 0);
      },
    }, text)
    : el("span", { class: "btn btn-small", "aria-disabled": "true" }, text);
  $("repos-pager").replaceChildren(
    link(page - 1, "Previous", page > 1, "prev"),
    el("span", { class: "pager-pos" }, `Page ${page}`),
    link(page + 1, "Next", hasNext, "next"),
  );
}

// --- run header and jobs ----------------------------------------------------

function renderRunMeta() {
  const { run, attempt, repo } = state;
  const parts = [
    el("a", {
      class: "chip repo-link",
      href: `?repo=${repo}`,
      title: `Recent runs of ${OWNER}/${repo}`,
      onclick: (ev) => {
        if (!plainClick(ev)) return;
        ev.preventDefault();
        navigate({ repo }, { reset: true });
      },
    }, repo),
    el("a", { class: "run-link", href: `${run.html_url}${attempt ? `/attempts/${attempt}` : ""}`, target: "_blank", rel: "noopener noreferrer" },
      run.display_title || run.name || `Run ${run.id}`),
    el("span", { class: "chip" }, run.name || "workflow"),
    run.head_branch ? el("span", { class: "chip" }, run.head_branch) : null,
  ];
  const total = run.run_attempt ?? 1;
  if (total > 1) {
    const attempts = el("span", { class: "attempts" }, "Attempt ");
    for (let a = 1; a <= total; a++) {
      attempts.append(
        el("a", {
          href: `?repo=${repo}&run=${run.id}&attempt=${a}`,
          class: a === attempt ? "attempt current" : "attempt",
          "aria-current": a === attempt ? "page" : null,
          onclick: (ev) => {
            ev.preventDefault();
            navigate({ attempt: a, job: null, view: null });
          },
        }, String(a)),
      );
    }
    parts.push(attempts);
  }
  $("run-meta").replaceChildren(...parts);
  document.title = `${run.display_title || run.name} · CI Logs`;
}

function renderJobs() {
  const items = state.jobs.map((job) => {
    const s = jobState(job);
    return el("li", {},
      el("a", {
        class: "job",
        href: `?repo=${state.repo}&run=${state.runId}&attempt=${state.attempt}&job=${job.id}`,
        dataset: { job: String(job.id) },
        title: `${job.name}: ${STATE_LABEL[s] ?? s}`,
        onclick: (ev) => {
          ev.preventDefault();
          navigate({ job: job.id, view: null });
        },
      },
      el("span", { class: `icon icon-${s}`, "aria-label": STATE_LABEL[s] ?? s, role: "img" }),
      el("span", { class: "job-name" }, job.name),
      el("span", { class: "job-dur" }, duration(job.started_at, job.completed_at))),
    );
  });
  $("jobs").replaceChildren(...items);
  const n = visibleArtifacts().length;
  $("artifacts-count").textContent = n ? String(n) : "";
  const link = $("artifacts-link");
  link.href = `?repo=${state.repo}&run=${state.runId}&attempt=${state.attempt}&view=artifacts`;
}

function markActive(jobId) {
  for (const a of document.querySelectorAll(".sidebar .job")) {
    const active = jobId === "artifacts" ? a.id === "artifacts-link" : a.dataset.job === String(jobId);
    a.classList.toggle("active", active);
    if (active) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  }
}

function showEmpty(message) {
  $("job-title").textContent = "";
  $("log").replaceChildren();
  notice(message);
}

// --- job log ----------------------------------------------------------------

function findArtifact(name) {
  const matches = state.artifacts.filter((a) => a.name === name);
  return matches.find((a) => !a.expired) ?? matches[0] ?? null;
}

// `key` is `repo`'s verified key: cached results are dropped whenever it changes.
function loadSealedLog(repo, artifact, key) {
  if (!state.sealedLogs.has(artifact.id)) {
    const p = artifactZip(settings.token, repo, artifact.id).then((zip) => openSealedLog(zip, key, artifact.name));
    // Evict only this load, not a newer one that replaced it.
    p.catch(() => state.sealedLogs.get(artifact.id) === p && state.sealedLogs.delete(artifact.id));
    state.sealedLogs.set(artifact.id, p);
  }
  return state.sealedLogs.get(artifact.id);
}

function loadJobLog(job) {
  if (!state.jobLogs.has(job.id)) {
    const p = jobLog(settings.token, state.repo, job.id); // state.jobLogs is cleared with the repo
    p.catch(() => state.jobLogs.get(job.id) === p && state.jobLogs.delete(job.id));
    state.jobLogs.set(job.id, p);
  }
  return state.jobLogs.get(job.id);
}

/**
 * A finished job's public log with its sealed segments merged in, as the log
 * view and the export show it. Throws when the public log cannot be fetched.
 */
async function loadJobSections(job, wanted = () => true) {
  const { runId, repo } = state;
  const text = await loadJobLog(job);
  // The caller moved on (another job, run or Forget): skip opening sealed logs.
  if (!wanted()) return null;
  // Only markers naming this repo and run are replaced with sealed output.
  const sections = parseJobLog(text, { repo, run: runId });
  const names = [...new Set(sections.flatMap((sec) => sec.lines.filter((l) => l.marker).map((l) => l.marker.artifact)))];
  const segments = new Map();
  const summaries = new Map();
  const reasons = new Map();
  let badKey = null;
  // The key is checked against this repo's repo-key.pub before any use, and
  // only when there is sealed output to open.
  let key = null;
  const openable = names.some((name) => findArtifact(name) && !findArtifact(name).expired);
  if (openable && hasKey(repo)) {
    try {
      key = await verifiedKey(repo);
    } catch (err) {
      badKey = err;
    }
    if (!wanted()) return null;
  }
  await Promise.all(names.map(async (name) => {
    const art = findArtifact(name);
    if (!art) return reasons.set(name, "sealed log not found: it was not uploaded or has been deleted");
    if (art.expired) return reasons.set(name, "sealed log expired: artifacts are kept for 7 days");
    if (badKey?.code === "wrong-repo") return reasons.set(name, "the saved key is for a different repository");
    if (badKey) return reasons.set(name, "the repo key could not be checked");
    if (!key) return reasons.set(name, `add the repo key for ${repo} in Settings to see it`);
    try {
      const opened = await loadSealedLog(repo, art, key);
      for (const [k, v] of opened.segments) segments.set(k, v);
      for (const [k, v] of opened.summaries) summaries.set(k, v);
    } catch (err) {
      if (err?.code === "bad-key") badKey = err;
      reasons.set(name, err?.code === "bad-key" ? "the repo key cannot open it" : err?.status === 410 ? "sealed log expired" : err?.message ?? String(err));
    }
  }));
  const merged = mergeSealed(sections, segments, (a) => reasons.get(a) ?? "segment missing from the sealed log");
  merged.forEach((sec) => {
    sec.summaries = sec.sealedKeys.filter((k) => summaries.has(k)).map((k) => summaries.get(k));
  });
  return { merged, names, segments, reasons, badKey };
}

async function showJob(job, seq) {
  markActive(job.id);
  state.job = job;
  updateExportControls();
  $("artifacts").hidden = true;
  $("log").hidden = false;
  $("log-tools").hidden = false;
  const s = jobState(job);
  $("job-title").replaceChildren(
    el("span", { class: `icon icon-${s}`, "aria-label": STATE_LABEL[s] ?? s, role: "img" }),
    el("span", {}, job.name),
    el("span", { class: "job-title-dur" }, duration(job.started_at, job.completed_at)),
  );
  $("log").replaceChildren(el("p", { class: "loading" }, "Loading log…"));
  notice("");
  if (s === "skipped") {
    showEmpty("This job was skipped, so it has no log.");
    return;
  }
  if (job.status !== "completed") {
    $("log").replaceChildren();
    notice("This job has not finished yet; its log is available once it completes. Reload the page then.");
    return;
  }
  const loaded = await loadJobSections(job, () => seq === state.seq);
  if (!loaded || seq !== state.seq) return;
  const { merged, names, segments, reasons, badKey } = loaded;

  const lost = names.map((n) => reasons.get(n)).find((r) => r && /expired|not found/.test(r));
  if (badKey) report(badKey);
  else if (!hasKey(state.repo) && names.length) banner(`Add the repo key for ${state.repo} in Settings to see sealed output.`, { settingsAction: true });
  else if (lost) banner(`Sealed output for this job is unavailable: ${lost}. Only the public log is shown.`);
  else clearBanner();
  const markerCount = merged.reduce((n, sec) => n + sec.sealedKeys.length, 0);
  if (!markerCount) notice("Nothing sealed in this job: this is the public log.");
  else if (!segments.size) notice(`Sealed output unavailable (${[...new Set(reasons.values())].join("; ") || "unknown"}): showing the public log.`);
  renderLog(merged, job);
}

// --- export -----------------------------------------------------------------

function setExportStatus(message) {
  $("export-status").textContent = message ?? "";
  $("export-status").hidden = !message;
}

// Abandons an export in flight (leaving the run, Forget) and resets its controls.
function cancelExport() {
  state.gen++;
  state.exporting = false;
  setExportStatus("");
  updateExportControls();
}

function updateExportControls() {
  const noKey = !hasKey(state.repo);
  $("export-job").disabled = noKey || state.exporting || !state.job;
  $("export-all").disabled = noKey || state.exporting || !state.jobs.length;
  $("export-hint").hidden = !noKey;
}

// One job for exportText: its merged sections, or why it has none.
async function exportJob(job, wanted) {
  const base = { name: job.name, conclusion: job.conclusion ?? job.status };
  if (job.conclusion === "skipped") return { ...base, unavailable: "job was skipped" };
  if (job.status !== "completed") return { ...base, unavailable: "job has not finished" };
  try {
    const loaded = await loadJobSections(job, wanted);
    if (!loaded) return { ...base, unavailable: "export cancelled" };
    return { ...base, sections: loaded.merged, badKey: loaded.badKey };
  } catch (err) {
    return { ...base, unavailable: err?.status === 404 || err?.status === 410 ? "log not found: it may have expired" : err?.message ?? String(err) };
  }
}

// Downloads the current job's (or every job's) log as plain text. Forget, new
// settings or another run bump state.gen, which abandons the export.
async function exportLogs(all) {
  if (!hasKey(state.repo)) {
    banner(`Add the repo key for ${state.repo} in Settings to export logs with their sealed output.`, { settingsAction: true });
    return;
  }
  const jobs = all ? [...state.jobs] : [state.job].filter(Boolean);
  if (state.exporting || !state.run || !jobs.length) return;
  const gen = state.gen;
  const { run, attempt, repo } = state;
  state.exporting = true;
  updateExportControls();
  const results = new Array(jobs.length);
  let done = 0;
  let badKey = null;
  const progress = () => setExportStatus(all ? `Exporting ${done}/${jobs.length} jobs…` : "Exporting…");
  progress();
  try {
    let next = 0;
    const worker = async () => {
      while (next < jobs.length && gen === state.gen && !badKey) {
        const i = next++;
        results[i] = await exportJob(jobs[i], () => gen === state.gen);
        badKey ??= results[i].badKey;
        done++;
        if (gen === state.gen) progress();
      }
    };
    await Promise.all(Array.from({ length: Math.min(EXPORT_WORKERS, jobs.length) }, worker));
    if (gen !== state.gen) return;
    if (badKey) {
      report(badKey);
      return;
    }
    const filename = exportFilename({ repo, workflow: run.name, runNumber: run.run_number, attempt, job: all ? "" : jobs[0].name });
    download(exportText(results), filename, "text/plain;charset=utf-8");
  } finally {
    if (gen === state.gen) {
      state.exporting = false;
      setExportStatus("");
      updateExportControls();
    }
  }
}

// GitHub log commands rendered the way the Actions UI shows them.
const COMMANDS = [
  ["##[error]", "l-error", "Error: "],
  ["##[warning]", "l-warning", "Warning: "],
  ["##[notice]", "l-notice", "Notice: "],
  ["##[debug]", "l-debug", ""],
  ["##[command]", "l-command", ""],
  ["[command]", "l-command", ""],
  ["##[group]", "l-group", ""],
];

function lineParts(line) {
  for (const [prefix, cls, label] of COMMANDS) {
    if (line.text.startsWith(prefix)) return { cls, text: label + line.text.slice(prefix.length) };
  }
  return { cls: "", text: line.text };
}

const plain = stripAnsi;

function sectionFailed(sec) {
  return sec.lines.some((l) => l.text.startsWith("##[error]"));
}

function renderLog(sections, job) {
  const view = { sections, rendered: new Map(), query: "", job };
  state.view = view;
  const failedJob = ["failure", "timed_out", "cancelled"].includes(job.conclusion);
  const anyFailed = sections.some(sectionFailed);
  const nodes = sections.map((sec, i) => {
    const n = i + 1;
    const failed = sectionFailed(sec);
    const stamps = sec.lines.filter((l) => l.ts);
    const dur = stamps.length > 1 ? duration(stamps[0].ts, stamps[stamps.length - 1].ts) : "";
    const sealedCount = sec.lines.filter((l) => l.sealed && !l.unavailable).length;
    const body = el("div", { class: "lines", role: "log" });
    const summary = el("summary", { class: failed ? "step-h failed" : "step-h" },
      el("span", { class: "chev", "aria-hidden": "true" }),
      el("span", { class: failed ? "icon icon-failure" : "icon icon-step", "aria-hidden": "true" }),
      el("span", { class: "step-title" }, sec.title),
      sealedCount ? el("span", { class: "step-sealed", title: `${sealedCount} sealed lines` }, "sealed") : null,
      el("span", { class: "step-dur" }, dur),
    );
    const details = el("details", { class: "step", id: `S${n}` }, summary, body);
    for (const md of sec.summaries ?? []) {
      details.append(el("div", { class: "step-summary" },
        el("div", { class: "step-summary-h" }, "Step summary"),
        el("pre", {}, md)));
    }
    // Open failed steps; for a failed job with no error line, open the last step.
    details.open = failed || (failedJob && !anyFailed && i === sections.length - 1) || sections.length === 1;
    details.addEventListener("toggle", () => {
      if (details.open) renderSection(view, i);
    });
    return details;
  });
  $("log").replaceChildren(...nodes);
  nodes.forEach((d, i) => d.open && renderSection(view, i));
  if ($("search").value) applySearch();
  jumpToHash();
}

function renderSection(view, i, force = false) {
  if (state.view !== view) return;
  const q = view.query;
  if (!force && view.rendered.get(i) === q) return;
  view.rendered.set(i, q);
  const sec = view.sections[i];
  const render = ansiRenderer();
  const n = i + 1;
  const out = [];
  const showAll = !q;
  sec.lines.forEach((line, j) => {
    // Render every line, so colour state carries, but only emit matches.
    const parts = lineParts(line);
    const html = render(parts.text);
    if (line.text.startsWith("##[endgroup]")) return;
    if (!showAll && !plain(line.text).toLowerCase().includes(q)) return;
    const id = `L${n}-${j + 1}`;
    const cls = ["line", parts.cls, line.sealed ? "sealed" : "", line.unavailable ? "unavailable" : ""].filter(Boolean).join(" ");
    const ts = line.ts ? line.ts.slice(11, 19) : "";
    out.push(
      `<div class="${cls}" id="${id}"><a class="ln" href="#${id}">${j + 1}</a>` +
      `<span class="gut"${line.sealed ? ' title="Sealed output"' : ""}></span>` +
      `<span class="ts">${ts}</span><span class="tx">${html}</span></div>`,
    );
  });
  // Every string above is escaped (ansi_up escapes text; the rest is numbers/classes).
  const body = $(`S${n}`).querySelector(".lines");
  body.innerHTML = out.join("");
}

let searchTimer = 0;
function applySearch() {
  const view = state.view;
  if (!view) return;
  const q = $("search").value.trim().toLowerCase();
  view.query = q;
  let total = 0;
  view.sections.forEach((sec, i) => {
    const d = $(`S${i + 1}`);
    if (!q) {
      d.hidden = false;
      if (d.open) renderSection(view, i);
      return;
    }
    const hits = sec.lines.filter((l) => !l.text.startsWith("##[endgroup]") && plain(l.text).toLowerCase().includes(q)).length;
    total += hits;
    d.hidden = hits === 0;
    if (hits) {
      d.open = true;
      renderSection(view, i);
    }
  });
  if (q) notice(total ? `${total} matching line${total === 1 ? "" : "s"}.` : "No lines match.");
  else notice("");
}

function jumpToHash() {
  const m = /^#L(\d+)-(\d+)$/.exec(location.hash);
  if (!m || !state.view) return;
  const i = Number(m[1]) - 1;
  const d = $(`S${m[1]}`);
  if (!d) return;
  if (state.view.query) {
    $("search").value = "";
    applySearch();
  }
  d.hidden = false;
  d.open = true;
  renderSection(state.view, i);
  for (const old of document.querySelectorAll(".line.hl")) old.classList.remove("hl");
  const line = document.getElementById(`L${m[1]}-${m[2]}`);
  if (line) {
    line.classList.add("hl");
    line.scrollIntoView({ block: "center" });
  }
}

// --- artifacts --------------------------------------------------------------

function visibleArtifacts() {
  return state.artifacts.filter((a) => !a.name.startsWith("sealed-log-"));
}

function download(data, filename, type = "application/octet-stream") {
  const url = URL.createObjectURL(new Blob([data], { type }));
  downloadUrls.add(url);
  const a = el("a", { href: url, download: filename });
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => revokeDownload(url), 60_000);
}

function revokeDownload(url) {
  if (downloadUrls.delete(url)) URL.revokeObjectURL(url);
}

function decodeText(data) {
  if (data.length > PREVIEW_MAX) return null;
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(data);
    // NUL bytes mean binary even when the bytes happen to be valid UTF-8.
    return text.includes("\0") ? null : text;
  } catch {
    return null;
  }
}

function showArtifacts() {
  markActive("artifacts");
  state.view = null;
  state.job = null;
  updateExportControls();
  $("log").hidden = true;
  $("log").replaceChildren();
  $("log-tools").hidden = true;
  $("artifacts").hidden = false;
  $("job-title").replaceChildren(el("span", { class: "icon icon-box", "aria-hidden": "true" }), el("span", {}, "Artifacts"));
  notice("");
  clearBanner();
  const list = visibleArtifacts();
  if (!list.length) {
    $("artifacts").replaceChildren(el("p", { class: "muted" }, "This run has no artifacts besides its sealed logs."));
    return;
  }
  if (!hasKey(state.repo)) banner(`Add the repo key for ${state.repo} in Settings to unseal artifacts.`, { settingsAction: true });
  $("artifacts").replaceChildren(...list.map(artifactCard));
}

function artifactCard(art) {
  const repo = state.repo;
  const out = el("div", { class: "art-body" });
  const button = el("button", { type: "button", class: "btn btn-small", disabled: art.expired }, art.expired ? "Expired" : "Unseal");
  button.addEventListener("click", async () => {
    if (!hasKey(repo)) {
      banner(`Add the repo key for ${repo} in Settings to unseal artifacts.`, { settingsAction: true });
      return;
    }
    button.disabled = true;
    button.textContent = "Unsealing…";
    try {
      // Checked against this repo's repo-key.pub before it is used.
      const key = await verifiedKey(repo);
      if (!out.isConnected || !key) return;
      if (!state.sealedArtifacts.has(art.id)) {
        const p = artifactZip(settings.token, repo, art.id).then((zip) => openSealedArtifact(zip, key));
        p.catch(() => state.sealedArtifacts.get(art.id) === p && state.sealedArtifacts.delete(art.id));
        state.sealedArtifacts.set(art.id, p);
      }
      const opened = await state.sealedArtifacts.get(art.id);
      // Settings were forgotten or changed meanwhile: render nothing.
      if (!out.isConnected) return;
      clearBanner();
      out.replaceChildren(...artifactEntries(opened));
      button.textContent = "Unsealed";
    } catch (err) {
      if (!out.isConnected) return;
      report(err);
      button.disabled = false;
      button.textContent = "Unseal";
    }
  });
  const when = art.expired ? "expired" : art.expires_at ? `expires ${new Date(art.expires_at).toLocaleDateString()}` : "";
  return el("section", { class: "art" },
    el("div", { class: "art-head" },
      el("span", { class: "icon icon-box", "aria-hidden": "true" }),
      el("span", { class: "art-name" }, art.name),
      el("span", { class: "art-meta" }, [bytes(art.size_in_bytes ?? 0), when].filter(Boolean).join(" · ")),
      button),
    out);
}

function artifactEntries(opened) {
  const files = opened.entries.filter((e) => e.type !== "dir");
  const shown = files.slice(0, ENTRY_ROWS_MAX);
  // Previews render only when an entry is opened, within a per-artifact budget.
  let budget = PREVIEW_BUDGET;
  const head = el("div", { class: "art-actions" },
    el("span", { class: "muted" }, `${files.length} file${files.length === 1 ? "" : "s"}`),
    el("button", { type: "button", class: "btn btn-small", onclick: () => download(opened.gzip, `${opened.name}.tar.gz`, "application/gzip") }, "Download all (.tar.gz)"));
  const rows = shown.map((e) => {
    if (e.type === "symlink") {
      return el("div", { class: "entry" }, el("div", { class: "entry-h" }, el("span", { class: "entry-path" }, e.path), el("span", { class: "muted" }, `→ ${e.linkname}`)));
    }
    const name = e.path.slice(e.path.lastIndexOf("/") + 1);
    const h = el("div", { class: "entry-h" },
      el("span", { class: "entry-path" }, e.path),
      el("span", { class: "muted" }, bytes(e.data.length)),
      el("button", {
        type: "button",
        class: "btn btn-small",
        onclick: (ev) => {
          ev.preventDefault();
          download(e.data, name);
        },
      }, "Download"));
    const details = el("details", { class: "entry" }, el("summary", { title: "Show a preview" }, h));
    details.addEventListener("toggle", () => {
      if (!details.open || details.dataset.loaded) return;
      details.dataset.loaded = "1";
      let body;
      if (e.data.length > PREVIEW_MAX) body = el("p", { class: "preview-note muted" }, "Too large to preview (over 1 MB); download it instead.");
      else if (e.data.length > budget) body = el("p", { class: "preview-note muted" }, "Preview limit for this artifact reached (5 MB); download the file instead.");
      else {
        const text = decodeText(e.data);
        if (text === null) body = el("p", { class: "preview-note muted" }, "Binary file; download it to view.");
        else {
          budget -= e.data.length;
          body = el("pre", { class: "preview" }, text);
        }
      }
      details.append(body);
    });
    return details;
  });
  if (files.length > shown.length) {
    rows.push(el("p", { class: "preview-note muted" }, `${files.length - shown.length} more files not listed; use Download all.`));
  }
  return [head, ...rows];
}

// --- startup ----------------------------------------------------------------

// Pages cannot send frame-ancestors or X-Frame-Options, so refuse to run framed.
function framed() {
  try {
    return window.top !== window.self;
  } catch {
    return true;
  }
}

function init() {
  if (framed()) {
    document.body.replaceChildren(el("p", { class: "framed" }, "CI Logs does not run inside a frame. Open https://ci.lascade.com directly."));
    return;
  }
  $("open-settings").addEventListener("click", openSettings);
  $("banner-action").addEventListener("click", openSettings);
  $("settings-form").addEventListener("submit", saveSettings);
  $("cancel-settings").addEventListener("click", () => $("settings").close());
  // Closing (Cancel, Escape) abandons a key check still in flight.
  $("settings").addEventListener("close", cancelPendingSave);
  $("forget").addEventListener("click", forgetSettings);
  $("pick-form").addEventListener("submit", submitPick);
  $("home-settings").addEventListener("click", openSettings);
  $("export-hint").addEventListener("click", openSettings);
  $("export-job").addEventListener("click", () => exportLogs(false));
  $("export-all").addEventListener("click", () => exportLogs(true));
  $("filter-workflow").addEventListener("change", (ev) => changeFilter("workflow", ev.target.value));
  $("filter-status").addEventListener("change", (ev) => changeFilter("status", ev.target.value));
  $("show-all-repos").addEventListener("change", (ev) => navigate({ all: ev.target.checked ? 1 : null }, { reset: true }));
  for (const id of ["brand", "home-back"]) {
    $(id).addEventListener("click", (ev) => {
      if (!plainClick(ev)) return;
      ev.preventDefault();
      navigate({}, { reset: true });
    });
  }
  $("artifacts-link").addEventListener("click", (ev) => {
    ev.preventDefault();
    navigate({ view: "artifacts", job: null });
  });
  $("search").addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(applySearch, 150);
  });
  $("show-ts").addEventListener("change", (ev) => $("log").classList.toggle("show-ts", ev.target.checked));
  window.addEventListener("popstate", route);
  window.addEventListener("hashchange", jumpToHash);
  route();
}

init();
