// GitHub REST calls for the public repo Lascade-Co/actions.
//
// Every request goes to api.github.com with the token as a bearer header. Logs
// and artifact zips answer with a 302 to blob storage; fetch follows it and, per
// the Fetch standard, drops the Authorization header on that cross-origin hop,
// so the token never reaches blob storage. Only Accept (a CORS-safelisted header)
// is sent besides Authorization, so the redirected request needs no preflight.
// Listing runs and workflows also works without a token (the repo is public),
// at GitHub's unauthenticated limit of 60 requests an hour.
import { runsQuery } from "./runs.js";

export const REPO = "Lascade-Co/actions";
const API = `https://api.github.com/repos/${REPO}/actions`;

export class GitHubError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "GitHubError";
    this.status = status;
  }
}

function id(value, what) {
  const s = String(value);
  if (!/^\d+$/.test(s)) throw new GitHubError(`Invalid ${what}: ${s}`, 0);
  return s;
}

function fix(status, body, headers, authed) {
  const msg = body?.message ? ` (${body.message})` : "";
  if (status === 401) return `GitHub rejected the token${msg}: open Settings and paste a valid fine-grained token.`;
  if (status === 403 || status === 429) {
    const reset = Number(headers.get("x-ratelimit-reset"));
    const after = headers.get("x-ratelimit-remaining") === "0" && reset ? `: try again after ${new Date(reset * 1000).toLocaleTimeString()}` : "";
    if (!authed) {
      return `GitHub refused the request${msg}${after}. Without a token GitHub allows 60 requests an hour; add a GitHub token in Settings to raise the limit.`;
    }
    if (after) return `GitHub API rate limit reached${after}.`;
    return `GitHub refused the request${msg}: check the token in Settings has public repository read access.`;
  }
  if (status === 404) return `Not found in ${REPO}${msg}: check the run ID, or the log is not ready yet.`;
  if (status === 410) return `The artifact has expired${msg}: artifacts are kept for 7 days.`;
  return `GitHub returned HTTP ${status}${msg}.`;
}

async function request(token, url) {
  let res;
  try {
    const headers = token ? { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json" } : { Accept: "application/vnd.github+json" };
    res = await fetch(url, {
      headers,
      redirect: "follow",
      credentials: "omit",
      referrerPolicy: "no-referrer",
    });
  } catch (err) {
    throw new GitHubError(`Network error reaching GitHub (${err?.message ?? err}): check your connection and try again.`, 0);
  }
  if (!res.ok) {
    let body = null;
    try {
      body = await res.json();
    } catch {
      // Not JSON (blob storage errors are XML).
    }
    throw new GitHubError(fix(res.status, body, res.headers, Boolean(token)), res.status);
  }
  return res;
}

async function paginate(token, path, key) {
  const items = [];
  for (let page = 1; ; page++) {
    const res = await request(token, `${API}${path}?per_page=100&page=${page}`);
    const body = await res.json();
    const batch = body[key] ?? [];
    items.push(...batch);
    if (batch.length < 100 || items.length >= (body.total_count ?? Infinity)) return items;
  }
}

export async function getRun(token, runId) {
  return (await request(token, `${API}/runs/${id(runId, "run id")}`)).json();
}

export function listJobs(token, runId, attempt) {
  return paginate(token, `/runs/${id(runId, "run id")}/attempts/${id(attempt, "attempt")}/jobs`, "jobs");
}

export async function jobLog(token, jobId) {
  return (await request(token, `${API}/jobs/${id(jobId, "job id")}/logs`)).text();
}

export function listArtifacts(token, runId) {
  return paginate(token, `/runs/${id(runId, "run id")}/artifacts`, "artifacts");
}

export async function artifactZip(token, artifactId) {
  const res = await request(token, `${API}/artifacts/${id(artifactId, "artifact id")}/zip`);
  return new Uint8Array(await res.arrayBuffer());
}

/** One page of runs, newest first: `{total, runs}`. `token` may be empty. */
export async function listRuns(token, { page = 1, perPage, workflow = "", status = "", event = "", branch = "" } = {}) {
  let path;
  try {
    path = runsQuery({ page, perPage, workflow, status, event, branch });
  } catch (err) {
    throw new GitHubError(err.message, 0);
  }
  const body = await (await request(token, `${API}${path}`)).json();
  return { total: Number(body.total_count) || 0, runs: body.workflow_runs ?? [] };
}

/** Every workflow in the repo. `token` may be empty. */
export function listWorkflows(token) {
  return paginate(token, "/workflows", "workflows");
}
