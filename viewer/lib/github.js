// GitHub REST calls for the public repos of Lascade-Co.
//
// Every request goes to api.github.com with the token as a bearer header. Logs
// and artifact zips answer with a 302 to blob storage; fetch follows it and, per
// the Fetch standard, drops the Authorization header on that cross-origin hop,
// so the token never reaches blob storage. Only Accept (a CORS-safelisted header)
// is sent besides Authorization, so the redirected request needs no preflight.
// Listing repos, runs and workflows and reading a repo's public key also work
// without a token (the repos are public), at GitHub's unauthenticated limit of
// 60 requests an hour.
//
// Every repo name is checked before it goes into a URL path. The owner is fixed,
// and "." or ".." (which the URL parser would resolve as path steps) are refused.
import { runsQuery, isRepoName, OWNER, DEFAULT_REPO } from "./runs.js";

const ROOT = "https://api.github.com";
const JSON_TYPE = "application/vnd.github+json";
const RAW_TYPE = "application/vnd.github.raw";
const RECIPIENT_RE = /^age1[02-9ac-hj-np-z]{58}$/;

export class GitHubError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "GitHubError";
    this.status = status;
  }
}

/** Returns `repo` when it is a valid repository name; throws a GitHubError otherwise. */
export function checkRepo(repo) {
  if (!isRepoName(repo)) throw new GitHubError(`Invalid repository name: ${String(repo).slice(0, 120)}`, 0);
  return repo;
}

const api = (repo) => `${ROOT}/repos/${OWNER}/${checkRepo(repo)}/actions`;
const inRepo = (repo) => ({ where: `${OWNER}/${repo}` });

function id(value, what) {
  const s = String(value);
  if (!/^\d+$/.test(s)) throw new GitHubError(`Invalid ${what}: ${s}`, 0);
  return s;
}

function fix(status, body, headers, authed, where) {
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
  if (status === 404) return `Not found in ${where}${msg}: check the run ID, or the log is not ready yet.`;
  if (status === 410) return `The artifact has expired${msg}: artifacts are kept for 7 days.`;
  return `GitHub returned HTTP ${status}${msg}.`;
}

async function request(token, url, { accept = JSON_TYPE, where = OWNER } = {}) {
  let res;
  try {
    const headers = token ? { Authorization: `Bearer ${token}`, Accept: accept } : { Accept: accept };
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
    throw new GitHubError(fix(res.status, body, res.headers, Boolean(token), where), res.status);
  }
  return res;
}

async function paginate(token, repo, path, key) {
  const base = api(repo);
  const items = [];
  for (let page = 1; ; page++) {
    const res = await request(token, `${base}${path}?per_page=100&page=${page}`, inRepo(repo));
    const body = await res.json();
    const batch = body[key] ?? [];
    items.push(...batch);
    if (batch.length < 100 || items.length >= (body.total_count ?? Infinity)) return items;
  }
}

export async function getRun(token, repo, runId) {
  return (await request(token, `${api(repo)}/runs/${id(runId, "run id")}`, inRepo(repo))).json();
}

export async function listJobs(token, repo, runId, attempt) {
  return paginate(token, repo, `/runs/${id(runId, "run id")}/attempts/${id(attempt, "attempt")}/jobs`, "jobs");
}

export async function jobLog(token, repo, jobId) {
  return (await request(token, `${api(repo)}/jobs/${id(jobId, "job id")}/logs`, inRepo(repo))).text();
}

export async function listArtifacts(token, repo, runId) {
  return paginate(token, repo, `/runs/${id(runId, "run id")}/artifacts`, "artifacts");
}

export async function artifactZip(token, repo, artifactId) {
  const res = await request(token, `${api(repo)}/artifacts/${id(artifactId, "artifact id")}/zip`, inRepo(repo));
  return new Uint8Array(await res.arrayBuffer());
}

/** One page of the repo's runs, newest first: `{total, runs}`. `token` may be empty. */
export async function listRuns(token, repo, { page = 1, perPage, workflow = "", status = "", event = "", branch = "" } = {}) {
  const base = api(repo);
  let path;
  try {
    path = runsQuery({ page, perPage, workflow, status, event, branch });
  } catch (err) {
    throw new GitHubError(err.message, 0);
  }
  const body = await (await request(token, `${base}${path}`, inRepo(repo))).json();
  return { total: Number(body.total_count) || 0, runs: body.workflow_runs ?? [] };
}

/** Every workflow in the repo. `token` may be empty. */
export async function listWorkflows(token, repo) {
  return paginate(token, repo, "/workflows", "workflows");
}

/** The org's public repos with the sealed-ci topic (at most 100). `token` may be empty. */
export async function listSealedRepos(token) {
  const url = `${ROOT}/search/repositories?q=org:${OWNER}+topic:sealed-ci+is:public&per_page=100`;
  const body = await (await request(token, url)).json();
  return Array.isArray(body.items) ? body.items : [];
}

/** One page (100) of the org's public repos: `{repos, hasNext}`. `token` may be empty. */
export async function listPublicRepos(token, page = 1) {
  const p = String(page);
  if (!/^\d+$/.test(p) || Number(p) < 1) throw new GitHubError(`Invalid page: ${p}`, 0);
  const body = await (await request(token, `${ROOT}/orgs/${OWNER}/repos?type=public&per_page=100&page=${p}`)).json();
  const repos = Array.isArray(body) ? body : [];
  return { repos, hasNext: repos.length === 100 };
}

/** Where a repo commits its seal public key: actions keeps it next to the toolkit. */
export function publicKeyPath(repo) {
  return checkRepo(repo) === DEFAULT_REPO ? "scripts/seal/repo-key.pub" : ".github/seal/repo-key.pub";
}

/**
 * The repo's seal public key: the one `age1…` line of its repo-key.pub, read
 * raw through the contents API. Throws naming the file when it is missing or is
 * not a single age recipient. `token` may be empty.
 */
export async function repoPublicKey(token, repo) {
  const path = publicKeyPath(repo);
  const where = `${OWNER}/${repo}`;
  let res;
  try {
    res = await request(token, `${ROOT}/repos/${OWNER}/${repo}/contents/${path}`, { accept: RAW_TYPE, where });
  } catch (err) {
    if (err.status === 404) throw new GitHubError(`${where} has no ${path}, so its sealed output cannot be checked.`, 404);
    throw err;
  }
  const text = (await res.text()).trim();
  if (!RECIPIENT_RE.test(text)) throw new GitHubError(`${path} in ${where} is not a single age recipient.`, 0);
  return text;
}
