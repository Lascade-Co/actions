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

/** One list row for a run from the API, or null when its id is not a number. */
export function runRow(run, now = Date.now()) {
  const id = String(run.id);
  if (!DIGITS.test(id)) return null;
  const attempt = Number(run.run_attempt) || 1;
  const startedAt = run.run_started_at || run.created_at || null;
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
    href: attempt > 1 ? `?run=${id}&attempt=${attempt}` : `?run=${id}`,
    startedAt,
    when: relativeTime(startedAt, now),
    duration: run.status === "completed" ? duration(startedAt, run.updated_at) : "",
    attempt,
  };
}

/** Page count and prev/next for `total` results; `limit` caps what GitHub will page through. */
export function pageInfo(total, page, perPage = PER_PAGE, limit = Infinity) {
  const pages = Math.max(1, Math.ceil(Math.min(Number(total) || 0, limit) / perPage));
  return { page, pages, hasPrev: page > 1, hasNext: page < pages };
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
