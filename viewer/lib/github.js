// GitHub REST calls for the public repo Lascade-Co/actions.
//
// Every request goes to api.github.com with the token as a bearer header. Logs
// and artifact zips answer with a 302 to blob storage; fetch follows it and, per
// the Fetch standard, drops the Authorization header on that cross-origin hop,
// so the token never reaches blob storage. Only Accept (a CORS-safelisted header)
// is sent besides Authorization, so the redirected request needs no preflight.

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

function fix(status, body, headers) {
  const msg = body?.message ? ` (${body.message})` : "";
  if (status === 401) return `GitHub rejected the token${msg}: open Settings and paste a valid fine-grained token.`;
  if (status === 403 || status === 429) {
    const reset = Number(headers.get("x-ratelimit-reset"));
    if (headers.get("x-ratelimit-remaining") === "0" && reset) {
      return `GitHub API rate limit reached: try again after ${new Date(reset * 1000).toLocaleTimeString()}.`;
    }
    return `GitHub refused the request${msg}: check the token in Settings has public repository read access.`;
  }
  if (status === 404) return `Not found in ${REPO}${msg}: check the run ID, or the log is not ready yet.`;
  if (status === 410) return `The artifact has expired${msg}: artifacts are kept for 7 days.`;
  return `GitHub returned HTTP ${status}${msg}.`;
}

async function request(token, url) {
  let res;
  try {
    res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json" },
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
    throw new GitHubError(fix(res.status, body, res.headers), res.status);
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
