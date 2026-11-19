import { test, afterEach } from "node:test";
import assert from "node:assert/strict";
import { getRun, listJobs, jobLog, listArtifacts, artifactZip, listRuns, listWorkflows, GitHubError } from "./github.js";

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

function mock(handler) {
  const calls = [];
  globalThis.fetch = async (url, init = {}) => {
    calls.push({ url: String(url), init });
    return handler(String(url), init);
  };
  return calls;
}

const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

test("listJobs paginates the attempt's jobs and sends only the bearer token", async () => {
  const calls = mock((url) => {
    const page = Number(new URL(url).searchParams.get("page"));
    const jobs = page === 1 ? Array.from({ length: 100 }, (_, i) => ({ id: i })) : [{ id: 100 }];
    return json({ total_count: 101, jobs });
  });
  const jobs = await listJobs("tok", 42, 3);
  assert.equal(jobs.length, 101);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/actions/actions/runs/42/attempts/3/jobs?per_page=100&page=1");
  for (const c of calls) {
    assert.deepEqual(c.init.headers, { Authorization: "Bearer tok", Accept: "application/vnd.github+json" });
    assert.equal(c.init.redirect, "follow");
    assert.equal(c.init.credentials, "omit");
  }
});

test("listArtifacts paginates", async () => {
  mock((url) => {
    const page = Number(new URL(url).searchParams.get("page"));
    return json({ total_count: 150, artifacts: Array.from({ length: page === 1 ? 100 : 50 }, (_, i) => ({ id: page * 1000 + i })) });
  });
  assert.equal((await listArtifacts("tok", 7)).length, 150);
});

test("jobLog and artifactZip only ever call api.github.com", async () => {
  const calls = mock((url) => (url.endsWith("/logs") ? new Response("log text") : new Response(new Uint8Array([80, 75]))));
  assert.equal(await jobLog("tok", 9), "log text");
  assert.deepEqual(await artifactZip("tok", 5), new Uint8Array([80, 75]));
  assert.deepEqual(calls.map((c) => c.url), [
    "https://api.github.com/repos/Lascade-Co/actions/actions/jobs/9/logs",
    "https://api.github.com/repos/Lascade-Co/actions/actions/artifacts/5/zip",
  ]);
});

test("getRun returns the run", async () => {
  mock(() => json({ id: 1, run_attempt: 2 }));
  assert.equal((await getRun("tok", 1)).run_attempt, 2);
});

test("errors carry the HTTP status and a fix", async () => {
  mock(() => json({ message: "Bad credentials" }, 401));
  await assert.rejects(listJobs("tok", 1, 1), (err) => {
    assert.ok(err instanceof GitHubError);
    assert.equal(err.status, 401);
    assert.match(err.message, /token/i);
    return true;
  });
  mock(() => json({ message: "Artifact has expired" }, 410));
  await assert.rejects(artifactZip("tok", 1), (err) => err.status === 410 && /expired/i.test(err.message));
  mock(() => {
    throw new TypeError("Failed to fetch");
  });
  await assert.rejects(getRun("tok", 1), (err) => err.status === 0 && /network/i.test(err.message));
});

test("rejects a run id that is not a number", async () => {
  mock(() => json({}));
  await assert.rejects(getRun("tok", "1/../../x"), /run id/i);
});

test("listRuns fetches one page of the repo's runs with filters", async () => {
  const calls = mock(() => json({ total_count: 1234, workflow_runs: [{ id: 1 }, { id: 2 }] }));
  const out = await listRuns("tok", { page: 2, status: "failure" });
  assert.deepEqual(out, { total: 1234, runs: [{ id: 1 }, { id: 2 }] });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/actions/actions/runs?per_page=25&page=2&status=failure");
  assert.deepEqual(calls[0].init.headers, { Authorization: "Bearer tok", Accept: "application/vnd.github+json" });
  assert.equal(calls[0].init.credentials, "omit");
});

test("listRuns uses the workflow's runs endpoint when a workflow is selected", async () => {
  const calls = mock(() => json({ total_count: 0, workflow_runs: [] }));
  assert.deepEqual(await listRuns("tok", { page: 1, workflow: "77" }), { total: 0, runs: [] });
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/actions/actions/workflows/77/runs?per_page=25&page=1");
});

test("listRuns refuses a workflow that is not an id before fetching", async () => {
  const calls = mock(() => json({}));
  await assert.rejects(listRuns("tok", { workflow: "1/../../x" }), /workflow/i);
  assert.equal(calls.length, 0);
});

test("listRuns and listWorkflows work without a token and send no Authorization header", async () => {
  const calls = mock((url) => (url.includes("/workflows?") ? json({ total_count: 1, workflows: [{ id: 5, name: "CI" }] }) : json({ total_count: 0, workflow_runs: [] })));
  await listRuns("", {});
  assert.deepEqual(await listWorkflows(""), [{ id: 5, name: "CI" }]);
  assert.equal(calls[1].url, "https://api.github.com/repos/Lascade-Co/actions/actions/workflows?per_page=100&page=1");
  for (const c of calls) assert.deepEqual(c.init.headers, { Accept: "application/vnd.github+json" });
});

test("a rate limit without a token suggests adding one", async () => {
  mock(() => new Response(JSON.stringify({ message: "API rate limit exceeded" }), { status: 403, headers: { "x-ratelimit-remaining": "0", "x-ratelimit-reset": "1800000000" } }));
  await assert.rejects(listRuns("", {}), (err) => err.status === 403 && /60/.test(err.message) && /token in Settings/.test(err.message));
  mock(() => json({ message: "Forbidden" }, 429));
  await assert.rejects(listRuns("", {}), (err) => err.status === 429 && /60/.test(err.message));
});
