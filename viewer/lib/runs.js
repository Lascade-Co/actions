// Pure helpers for the recent-runs list: one row per run, paging and the API
// path for a page of runs.

/** Runs per page, as on GitHub's Actions tab. */
export const PER_PAGE = 25;

/** GitHub returns at most this many runs for a search filter (status, event, branch). */
export const SEARCH_LIMIT = 1000;

/** Values GitHub accepts for the runs `status` filter. */
export const STATUSES = [
  "completed", "action_required", "cancelled", "failure", "neutral", "skipped", "stale", "success",
  "timed_out", "in_progress", "queued", "requested", "waiting", "pending",
];

/** The status filter choices the list offers. */
export const STATUS_FILTERS = [
  ["", "All statuses"],
  ["success", "Success"],
  ["failure", "Failure"],
  ["cancelled", "Cancelled"],
  ["timed_out", "Timed out"],
  ["skipped", "Skipped"],
  ["action_required", "Action required"],
  ["in_progress", "In progress"],
  ["queued", "Queued"],
  ["waiting", "Waiting"],
];

/** The status choices for `current`: the offered ones, plus `current` when it is another valid status. */
export function statusOptions(current) {
  if (!current || STATUS_FILTERS.some(([v]) => v === current)) return STATUS_FILTERS;
  return [...STATUS_FILTERS, [current, current]];
}

const DIGITS = /^\d+$/;

/** "9s", "3m 5s" or "2h 5m" between two ISO times; "" when either is missing. */
export function duration(a, b) {
  if (!a || !b) return "";
  let s = Math.max(0, Math.round((Date.parse(b) - Date.parse(a)) / 1000));
  const h = Math.floor(s / 3600);
  s -= h * 3600;
  const m = Math.floor(s / 60);
  s -= m * 60;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}

const RTF = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

/** "just now", "5 minutes ago", "yesterday"; the date after 30 days. */
export function relativeTime(iso, now = Date.now()) {
  const t = Date.parse(iso ?? "");
  if (Number.isNaN(t)) return "";
  const s = Math.max(0, (now - t) / 1000);
  if (s < 45) return "just now";
  if (s < 45 * 60) return RTF.format(-Math.max(1, Math.round(s / 60)), "minute");
  if (s < 22 * 3600) return RTF.format(-Math.round(s / 3600), "hour");
  if (s < 30 * 86400) return RTF.format(-Math.round(s / 86400), "day");
  return new Date(t).toISOString().slice(0, 10);
}

const ICON_CONCLUSIONS = new Set(["success", "failure", "cancelled", "skipped", "timed_out", "action_required", "neutral"]);

/** The status icon for a run or job: its conclusion once completed, else running or queued. */
export function runState(run) {
  if (run.status !== "completed") return run.status === "in_progress" ? "running" : "queued";
  if (run.conclusion === "startup_failure") return "failure";
  return ICON_CONCLUSIONS.has(run.conclusion) ? run.conclusion : "neutral";
}

/** The owner of every repo the viewer opens. */
export const OWNER = "Lascade-Co";

/** The repo whose links predate `repo=`: an old marker or link without it means this one. */
export const DEFAULT_REPO = "actions";

/**
 * A GitHub repo name: 1-100 of [A-Za-z0-9._-], and not "." or "..", which a URL
 * path would resolve as directory steps.
 */
export function isRepoName(value) {
  return typeof value === "string" && /^[A-Za-z0-9._-]{1,100}$/.test(value) && !/^\.+$/.test(value);
}

/**
 * What the URL asks for: the repo picker (no repo), a repo's runs, or one run.
 * `?run=` without `repo=` is a run of the actions repo (links from before repos).
 * A bad value falls back to the view above it, with `error` naming it.
 */
export function readRoute(params) {
  const repoParam = params.get("repo") || "";
  const runParam = params.get("run") || "";
  if (repoParam && !isRepoName(repoParam)) {
    return { view: "repos", repo: null, run: null, error: `“${repoParam}” is not a repository name.` };
  }
  const repo = repoParam || (runParam ? DEFAULT_REPO : null);
  if (!repo) return { view: "repos", repo: null, run: null };
  if (!runParam) return { view: "runs", repo, run: null };
  if (!DIGITS.test(runParam)) return { view: "runs", repo, run: null, error: `“${runParam}” is not a run ID.` };
  return { view: "run", repo, run: runParam };
}

const PICK_HINT = "Paste a run link (github.com/Lascade-Co/<repo>/actions/runs/<id>) or a numeric run ID.";
const GITHUB_RUN = /^(?:https?:\/\/)?(?:www\.)?github\.com\/([^/?#]+)\/([^/?#]+)\/actions\/runs\/(\d+)(?:\/attempts\/(\d+)|\/job\/(\d+))?(?:[/?#].*)?$/i;

/**
 * The run that the "open a run" box names: a GitHub run link of a Lascade-Co
 * repo, a viewer link, or a bare run id (of `currentRepo`, else actions).
 * @returns {{repo: string, run: string, attempt: string|null, job: string|null} | {error: string}}
 */
export function parsePick(value, currentRepo) {
  const v = String(value ?? "").trim();
  if (DIGITS.test(v)) return { repo: isRepoName(currentRepo) ? currentRepo : DEFAULT_REPO, run: v, attempt: null, job: null };
  const m = GITHUB_RUN.exec(v);
  if (m) {
    if (m[1].toLowerCase() !== OWNER.toLowerCase()) return { error: `Only ${OWNER} repositories can be opened here.` };
    if (!isRepoName(m[2])) return { error: PICK_HINT };
    return { repo: m[2], run: m[3], attempt: m[4] ?? null, job: m[5] ?? null };
  }
  if (/^https:\/\/ci\.lascade\.com\/\?/i.test(v)) {
    let p = null;
    try {
      p = new URL(v).searchParams;
    } catch {
      // Not a URL after all.
    }
    const r = p ? readRoute(p) : null;
    if (r?.view === "run" && !r.error) {
      const attempt = p.get("attempt");
      return { repo: r.repo, run: r.run, attempt: attempt && DIGITS.test(attempt) ? attempt : null, job: null };
    }
  }
  return { error: PICK_HINT };
}

/**
 * One picker row for a repo from the API, or null when its name is not valid.
 * It is sealed when it has the sealed-ci topic or is in `sealedNames` (the
 * topic search's results).
 */
export function repoRow(repo, now = Date.now(), sealedNames = new Set()) {
  const name = repo?.name;
  if (!isRepoName(name)) return null;
  const pushedAt = repo.pushed_at || null;
  return {
    name,
    description: repo.description ? String(repo.description) : "",
    pushedAt,
    when: relativeTime(pushedAt, now),
    sealed: (Array.isArray(repo.topics) && repo.topics.includes("sealed-ci")) || sealedNames.has(name),
    href: `?repo=${name}`,
  };
}

/** One list row for a run of `repo` from the API, or null when its id is not a number. */
export function runRow(run, now = Date.now(), repo = DEFAULT_REPO) {
  const id = String(run.id);
  if (!DIGITS.test(id)) return null;
  const attempt = Number(run.run_attempt) || 1;
  const startedAt = run.run_started_at || run.created_at || null;
  const base = `?repo=${isRepoName(repo) ? repo : DEFAULT_REPO}&run=${id}`;
  return {
    id,
    title: run.display_title || run.name || `Run ${id}`,
    workflow: run.name || "",
    number: run.run_number,
    event: run.event || "",
    branch: run.head_branch || "",
    status: run.status,
    conclusion: run.conclusion ?? null,
    state: runState(run),
    href: attempt > 1 ? `${base}&attempt=${attempt}` : base,
    startedAt,
    when: relativeTime(startedAt, now),
    duration: run.status === "completed" ? duration(startedAt, run.updated_at) : "",
    attempt,
  };
}

/** The last page GitHub serves when it pages through at most `limit` results. */
export function maxPage(perPage = PER_PAGE, limit = Infinity) {
  return Math.ceil(limit / perPage);
}

/**
 * Page count and prev/next for `total` results; `limit` caps what GitHub will
 * page through. A page past the end is clamped to the last page.
 */
export function pageInfo(total, page, perPage = PER_PAGE, limit = Infinity) {
  const pages = Math.max(1, Math.ceil(Math.min(Number(total) || 0, limit) / perPage));
  const p = Math.min(Math.max(1, page), pages);
  return { page: p, pages, hasPrev: p > 1, hasNext: p < pages };
}

function positive(value, what, max = Infinity) {
  const s = String(value);
  if (!DIGITS.test(s) || Number(s) < 1 || Number(s) > max) throw new Error(`Invalid ${what}: ${s}`);
  return Number(s);
}

/**
 * The path, under /repos/{repo}/actions, of one page of runs: the repo's runs or,
 * with `workflow` (a numeric id), that workflow's runs. Rejects values that are
 * not ids or known filter values.
 */
export function runsQuery({ page = 1, perPage = PER_PAGE, workflow = "", status = "", event = "", branch = "" } = {}) {
  const q = new URLSearchParams({ per_page: String(positive(perPage, "per page", 100)), page: String(positive(page, "page")) });
  if (status) {
    if (!STATUSES.includes(status)) throw new Error(`Invalid status filter: ${status}`);
    q.set("status", status);
  }
  if (event) {
    if (!/^[a-z_]+$/.test(event)) throw new Error(`Invalid event filter: ${event}`);
    q.set("event", event);
  }
  if (branch) q.set("branch", String(branch));
  let base = "/runs";
  if (workflow) {
    if (!DIGITS.test(String(workflow))) throw new Error(`Invalid workflow id: ${workflow}`);
    base = `/workflows/${workflow}/runs`;
  }
  return `${base}?${q}`;
}

/** The list's page, workflow and status from the URL; invalid values fall back to the defaults. */
export function readListParams(params) {
  const page = params.get("page") ?? "";
  const workflow = params.get("workflow") ?? "";
  const status = params.get("status") ?? "";
  return {
    page: DIGITS.test(page) && Number(page) >= 1 ? Number(page) : 1,
    workflow: DIGITS.test(workflow) ? workflow : "",
    status: STATUSES.includes(status) ? status : "",
  };
}
