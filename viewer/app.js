// CI Logs: opens sealed Actions logs and artifacts of Lascade-Co/actions.
import { getRun, listJobs, jobLog, listArtifacts, artifactZip, listRuns, listWorkflows, REPO } from "./lib/github.js";
import { parseJobLog, mergeSealed } from "./lib/joblog.js";
import { openSealedLog, openSealedArtifact, checkRepoKey } from "./lib/sealed.js";
import { ansiRenderer, stripAnsi } from "./lib/ansi.js";
import { runRow, runState, pageInfo, readListParams, statusOptions, duration, PER_PAGE, SEARCH_LIMIT } from "./lib/runs.js";
import { exportText, exportFilename } from "./lib/export.js";

const KEY = "ci.repoKey";
const TOKEN = "ci.githubToken";
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

const settings = { key: load(KEY), token: load(TOKEN) };

// --- banner and notice ------------------------------------------------------

function banner(message, { settingsAction = false } = {}) {
  $("banner-text").textContent = message;
  $("banner-action").hidden = !settingsAction;
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
  banner(err?.message ?? String(err), { settingsAction: status === 401 || code === "bad-key" || code === "no-key" });
}

// --- settings dialog --------------------------------------------------------

function fieldError(id, message) {
  $(id).textContent = message ?? "";
  $(id).hidden = !message;
}

function openSettings() {
  $("repo-key").value = settings.key;
  $("gh-token").value = settings.token;
  fieldError("key-error");
  fieldError("token-error");
  if (!$("settings").open) $("settings").showModal();
}

function saveSettings(ev) {
  ev.preventDefault();
  const rawKey = $("repo-key").value.trim();
  const token = $("gh-token").value.trim();
  let key = "";
  let ok = true;
  if (rawKey) {
    try {
      key = checkRepoKey(rawKey);
      fieldError("key-error");
    } catch (err) {
      fieldError("key-error", err.message);
      ok = false;
    }
  } else fieldError("key-error");
  if (!token) {
    fieldError("token-error", "A GitHub token is required to read the run's logs.");
    ok = false;
  } else if (/\s/.test(token)) {
    fieldError("token-error", "The token contains spaces; paste it again.");
    ok = false;
  } else fieldError("token-error");
  if (!ok) return;
  const changed = key !== settings.key || token !== settings.token;
  settings.key = key;
  settings.token = token;
  const saved = store(KEY, key) && store(TOKEN, token);
  $("settings").close();
  if (!saved) banner("This browser blocks local storage, so the settings last only until you close the tab.");
  if (changed) {
    clearDecrypted();
    route();
  }
}

function forgetSettings() {
  store(KEY, "");
  store(TOKEN, "");
  settings.key = "";
  settings.token = "";
  $("repo-key").value = "";
  $("gh-token").value = "";
  clearDecrypted();
  $("settings").close();
  route();
}

// --- state ------------------------------------------------------------------

const state = {
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
  const runParam = p.get("run");
  if (!runParam) {
    showRuns(p);
    return;
  }
  if (!/^\d+$/.test(runParam)) {
    showRuns(new URLSearchParams(), `“${runParam}” is not a run ID.`);
    return;
  }
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
    if (!state.run || state.runId !== runParam || (wanted && wanted !== state.attempt) || (!wanted && state.attempt !== state.run.run_attempt)) {
      clearBanner();
      $("run").hidden = false;
      $("job-title").textContent = "Loading run…";
      $("log").replaceChildren();
      notice("");
      const run = await getRun(settings.token, runParam);
      const attempt = wanted ?? run.run_attempt ?? 1;
      const [jobs, artifacts] = await Promise.all([
        listJobs(settings.token, runParam, attempt),
        listArtifacts(settings.token, runParam),
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

function showRuns(p, error) {
  $("run").hidden = true;
  $("run-meta").replaceChildren();
  $("home").hidden = false;
  document.title = "CI Logs";
  fieldError("pick-error", error);
  $("home-hint-text").textContent = !settings.token
    ? "Add your repo key and a GitHub token in Settings to open runs."
    : "Add the repo key in Settings to see sealed output.";
  $("home-hint").hidden = Boolean(settings.token && settings.key);
  clearBanner();
  const q = readListParams(p);
  $("filter-status").replaceChildren(...statusOptions(q.status).map(([value, label]) => el("option", { value }, label)));
  $("filter-status").value = q.status;
  fillWorkflows(q.workflow);
  loadRuns(q);
}

async function loadRuns(q) {
  const seq = ++state.seq;
  const list = $("runs");
  list.setAttribute("aria-busy", "true");
  // Keep the old rows (dimmed) while the next page loads, so nothing jumps.
  list.classList.add("stale");
  if (!list.children.length) runsMessage("Loading runs…");
  try {
    const { total, runs } = await listRuns(settings.token, { page: q.page, perPage: PER_PAGE, workflow: q.workflow, status: q.status });
    if (seq !== state.seq) return;
    const now = Date.now();
    const rows = runs.map((r) => runRow(r, now)).filter(Boolean);
    list.replaceChildren(...rows.map(runItem));
    runsMessage(rows.length ? "" : q.page > 1 ? "No runs on this page." : "No runs.");
    // GitHub pages through at most 1000 runs of a status search.
    const info = pageInfo(total, q.page, PER_PAGE, q.status ? SEARCH_LIMIT : Infinity);
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

// Loads the workflow list once (it costs a request) and selects `selected`.
async function fillWorkflows(selected) {
  const select = $("filter-workflow");
  if (!state.workflows) {
    const p = listWorkflows(settings.token);
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
  if (pending !== state.workflows && state.workflows !== null) return;
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
        navigate({ run: row.id, attempt: row.attempt > 1 ? row.attempt : null }, { reset: true });
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
  const v = $("pick-input").value.trim();
  const m = /\/actions\/runs\/(\d+)(?:\/attempts\/(\d+))?/.exec(v) ?? /^(\d+)$/.exec(v);
  if (!m) {
    fieldError("pick-error", "Paste a run link (…/actions/runs/<id>) or a numeric run ID.");
    return;
  }
  $("pick-input").value = "";
  navigate({ run: m[1], attempt: m[2] ?? null }, { reset: true });
}

// --- run header and jobs ----------------------------------------------------

function renderRunMeta() {
  const { run, attempt } = state;
  const parts = [
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
          href: `?run=${run.id}&attempt=${a}`,
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
        href: `?run=${state.runId}&attempt=${state.attempt}&job=${job.id}`,
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
  link.href = `?run=${state.runId}&attempt=${state.attempt}&view=artifacts`;
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

function loadSealedLog(artifact) {
  if (!state.sealedLogs.has(artifact.id)) {
    const p = artifactZip(settings.token, artifact.id).then((zip) => openSealedLog(zip, settings.key, artifact.name));
    // Evict only this load, not a newer one that replaced it.
    p.catch(() => state.sealedLogs.get(artifact.id) === p && state.sealedLogs.delete(artifact.id));
    state.sealedLogs.set(artifact.id, p);
  }
  return state.sealedLogs.get(artifact.id);
}

function loadJobLog(job) {
  if (!state.jobLogs.has(job.id)) {
    const p = jobLog(settings.token, job.id);
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
  const runId = state.runId;
  const text = await loadJobLog(job);
  // The caller moved on (another job, run or Forget): skip opening sealed logs.
  if (!wanted()) return null;
  const sections = parseJobLog(text, { run: runId });
  const names = [...new Set(sections.flatMap((sec) => sec.lines.filter((l) => l.marker).map((l) => l.marker.artifact)))];
  const segments = new Map();
  const summaries = new Map();
  const reasons = new Map();
  let badKey = null;
  await Promise.all(names.map(async (name) => {
    const art = findArtifact(name);
    if (!art) return reasons.set(name, "sealed log not found: it was not uploaded or has been deleted");
    if (art.expired) return reasons.set(name, "sealed log expired: artifacts are kept for 7 days");
    if (!settings.key) return reasons.set(name, "add the repo key in Settings to see it");
    try {
      const opened = await loadSealedLog(art);
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
  else if (!settings.key && names.length) banner("Add the repo key in Settings to see sealed output.", { settingsAction: true });
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

function updateExportControls() {
  const noKey = !settings.key;
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
  if (!settings.key) {
    banner("Add the repo key in Settings to export logs with their sealed output.", { settingsAction: true });
    return;
  }
  const jobs = all ? [...state.jobs] : [state.job].filter(Boolean);
  if (state.exporting || !state.run || !jobs.length) return;
  const gen = state.gen;
  const { run, attempt } = state;
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
    const filename = exportFilename({ workflow: run.name, runNumber: run.run_number, attempt, job: all ? "" : jobs[0].name });
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
  if (!settings.key) banner("Add the repo key in Settings to unseal artifacts.", { settingsAction: true });
  $("artifacts").replaceChildren(...list.map(artifactCard));
}

function artifactCard(art) {
  const out = el("div", { class: "art-body" });
  const button = el("button", { type: "button", class: "btn btn-small", disabled: art.expired }, art.expired ? "Expired" : "Unseal");
  button.addEventListener("click", async () => {
    if (!settings.key) {
      banner("Add the repo key in Settings to unseal artifacts.", { settingsAction: true });
      return;
    }
    button.disabled = true;
    button.textContent = "Unsealing…";
    try {
      if (!state.sealedArtifacts.has(art.id)) {
        const p = artifactZip(settings.token, art.id).then((zip) => openSealedArtifact(zip, settings.key));
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
  $("forget").addEventListener("click", forgetSettings);
  $("pick-form").addEventListener("submit", submitPick);
  $("home-settings").addEventListener("click", openSettings);
  $("export-hint").addEventListener("click", openSettings);
  $("export-job").addEventListener("click", () => exportLogs(false));
  $("export-all").addEventListener("click", () => exportLogs(true));
  $("filter-workflow").addEventListener("change", (ev) => changeFilter("workflow", ev.target.value));
  $("filter-status").addEventListener("change", (ev) => changeFilter("status", ev.target.value));
  $("brand").addEventListener("click", (ev) => {
    if (!plainClick(ev)) return;
    ev.preventDefault();
    navigate({}, { reset: true });
  });
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
  $("repo-name").textContent = REPO;
  route();
}

init();
