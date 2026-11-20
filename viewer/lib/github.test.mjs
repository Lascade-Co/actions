import { test, afterEach } from "node:test";
import assert from "node:assert/strict";
import { getRun, listJobs, jobLog, listArtifacts, artifactZip, listRuns, listWorkflows, listSealedRepos, listPublicRepos, repoPublicKey, publicKeyPath, checkRepo, GitHubError } from "./github.js";

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
  const jobs = await listJobs("tok", "actions", 42, 3);
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
  assert.equal((await listArtifacts("tok", "actions", 7)).length, 150);
});

test("jobLog and artifactZip only ever call api.github.com", async () => {
  const calls = mock((url) => (url.endsWith("/logs") ? new Response("log text") : new Response(new Uint8Array([80, 75]))));
  assert.equal(await jobLog("tok", "actions", 9), "log text");
  assert.deepEqual(await artifactZip("tok", "actions", 5), new Uint8Array([80, 75]));
  assert.deepEqual(calls.map((c) => c.url), [
    "https://api.github.com/repos/Lascade-Co/actions/actions/jobs/9/logs",
    "https://api.github.com/repos/Lascade-Co/actions/actions/artifacts/5/zip",
  ]);
});

test("getRun returns the run", async () => {
  mock(() => json({ id: 1, run_attempt: 2 }));
  assert.equal((await getRun("tok", "actions", 1)).run_attempt, 2);
});

test("errors carry the HTTP status and a fix", async () => {
  mock(() => json({ message: "Bad credentials" }, 401));
  await assert.rejects(listJobs("tok", "actions", 1, 1), (err) => {
    assert.ok(err instanceof GitHubError);
    assert.equal(err.status, 401);
    assert.match(err.message, /token/i);
    return true;
  });
  mock(() => json({ message: "Artifact has expired" }, 410));
  await assert.rejects(artifactZip("tok", "actions", 1), (err) => err.status === 410 && /expired/i.test(err.message));
  mock(() => {
    throw new TypeError("Failed to fetch");
  });
  await assert.rejects(getRun("tok", "actions", 1), (err) => err.status === 0 && /network/i.test(err.message));
});

test("rejects a run id that is not a number", async () => {
  mock(() => json({}));
  await assert.rejects(getRun("tok", "actions", "1/../../x"), /run id/i);
});

test("listRuns fetches one page of the repo's runs with filters", async () => {
  const calls = mock(() => json({ total_count: 1234, workflow_runs: [{ id: 1 }, { id: 2 }] }));
  const out = await listRuns("tok", "actions", { page: 2, status: "failure" });
  assert.deepEqual(out, { total: 1234, runs: [{ id: 1 }, { id: 2 }] });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/actions/actions/runs?per_page=25&page=2&status=failure");
  assert.deepEqual(calls[0].init.headers, { Authorization: "Bearer tok", Accept: "application/vnd.github+json" });
  assert.equal(calls[0].init.credentials, "omit");
});

test("listRuns uses the workflow's runs endpoint when a workflow is selected", async () => {
  const calls = mock(() => json({ total_count: 0, workflow_runs: [] }));
  assert.deepEqual(await listRuns("tok", "actions", { page: 1, workflow: "77" }), { total: 0, runs: [] });
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/actions/actions/workflows/77/runs?per_page=25&page=1");
});

test("listRuns refuses a workflow that is not an id before fetching", async () => {
  const calls = mock(() => json({}));
  await assert.rejects(listRuns("tok", "actions", { workflow: "1/../../x" }), /workflow/i);
  assert.equal(calls.length, 0);
});

test("listRuns and listWorkflows work without a token and send no Authorization header", async () => {
  const calls = mock((url) => (url.includes("/workflows?") ? json({ total_count: 1, workflows: [{ id: 5, name: "CI" }] }) : json({ total_count: 0, workflow_runs: [] })));
  await listRuns("", "actions", {});
  assert.deepEqual(await listWorkflows("", "actions"), [{ id: 5, name: "CI" }]);
  assert.equal(calls[1].url, "https://api.github.com/repos/Lascade-Co/actions/actions/workflows?per_page=100&page=1");
  for (const c of calls) assert.deepEqual(c.init.headers, { Accept: "application/vnd.github+json" });
});

test("a rate limit without a token suggests adding one", async () => {
  mock(() => new Response(JSON.stringify({ message: "API rate limit exceeded" }), { status: 403, headers: { "x-ratelimit-remaining": "0", "x-ratelimit-reset": "1800000000" } }));
  await assert.rejects(listRuns("", "actions", {}), (err) => err.status === 403 && /60/.test(err.message) && /token in Settings/.test(err.message));
  mock(() => json({ message: "Forbidden" }, 429));
  await assert.rejects(listRuns("", "actions", {}), (err) => err.status === 429 && /60/.test(err.message));
});

test("checkRepo accepts Lascade-Co repo names and rejects the rest", () => {
  for (const ok of ["actions", "connector", "my.repo_2-x", "a", "x".repeat(100), "..a", ".github"]) assert.equal(checkRepo(ok), ok);
  for (const bad of ["", ".", "..", "...", "a/b", "../x", "x".repeat(101), "a b", "a%2fb", "ä", null, undefined]) {
    assert.throws(() => checkRepo(bad), (err) => err instanceof GitHubError && /repository/i.test(err.message), String(bad));
  }
});

test("every call uses the repo it is given", async () => {
  const calls = mock((url) => {
    if (url.includes("/logs")) return new Response("x");
    if (url.includes("/zip")) return new Response(new Uint8Array([1]));
    if (url.includes("/jobs")) return json({ total_count: 0, jobs: [] });
    if (url.includes("/artifacts")) return json({ total_count: 0, artifacts: [] });
    if (url.includes("/workflows?")) return json({ total_count: 0, workflows: [] });
    if (url.includes("/runs?")) return json({ total_count: 0, workflow_runs: [] });
    return json({ id: 3 });
  });
  await getRun("tok", "connector", 3);
  await listJobs("tok", "connector", 3, 1);
  await jobLog("tok", "connector", 4);
  await listArtifacts("tok", "connector", 3);
  await artifactZip("tok", "connector", 5);
  await listRuns("tok", "connector", {});
  await listWorkflows("tok", "connector");
  assert.equal(calls.length, 7);
  for (const c of calls) assert.ok(c.url.startsWith("https://api.github.com/repos/Lascade-Co/connector/actions/"), c.url);
});

test("an invalid repo is refused before any request", async () => {
  const calls = mock(() => json({}));
  for (const repo of ["..", ".", "a/b", "x?y"]) {
    await assert.rejects(getRun("tok", repo, 1), /repository/i);
    await assert.rejects(listRuns("tok", repo, {}), /repository/i);
    await assert.rejects(listWorkflows("tok", repo), /repository/i);
    await assert.rejects(jobLog("tok", repo, 1), /repository/i);
    await assert.rejects(repoPublicKey("tok", repo), /repository/i);
  }
  assert.equal(calls.length, 0);
});

test("a 404 names the repo it came from", async () => {
  mock(() => json({ message: "Not Found" }, 404));
  await assert.rejects(getRun("tok", "connector", 1), (err) => err.status === 404 && /Lascade-Co\/connector/.test(err.message));
});

test("listSealedRepos searches the org's public repos with the sealed-ci topic", async () => {
  const calls = mock(() => json({ total_count: 2, items: [{ name: "connector" }, { name: "actions" }] }));
  assert.deepEqual(await listSealedRepos(""), [{ name: "connector" }, { name: "actions" }]);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://api.github.com/search/repositories?q=org:Lascade-Co+topic:sealed-ci+is:public&per_page=100");
  assert.deepEqual(calls[0].init.headers, { Accept: "application/vnd.github+json" });
  assert.equal(calls[0].init.credentials, "omit");
});

test("listPublicRepos fetches one page of the org's public repos", async () => {
  const calls = mock((url) => {
    const page = Number(new URL(url).searchParams.get("page"));
    return json(Array.from({ length: page === 1 ? 100 : 3 }, (_, i) => ({ name: `r${page}-${i}` })));
  });
  const one = await listPublicRepos("tok", 1);
  assert.equal(one.repos.length, 100);
  assert.equal(one.hasNext, true);
  const two = await listPublicRepos("tok", 2);
  assert.equal(two.repos.length, 3);
  assert.equal(two.hasNext, false);
  assert.equal(calls[1].url, "https://api.github.com/orgs/Lascade-Co/repos?type=public&per_page=100&page=2");
  assert.deepEqual(calls[1].init.headers, { Authorization: "Bearer tok", Accept: "application/vnd.github+json" });
  await assert.rejects(listPublicRepos("tok", "1&x=y"), /page/i);
});

const PUB = "age1mjtk4us0x8lfpu0j6s23j74gng52zank4hrwngtrsvtw6yrzzvyq4cf63g";

test("repoPublicKey reads .github/seal/repo-key.pub raw from the repo", async () => {
  const calls = mock(() => new Response(`${PUB}\n`));
  assert.equal(await repoPublicKey("tok", "connector"), PUB);
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/connector/contents/.github/seal/repo-key.pub");
  assert.deepEqual(calls[0].init.headers, { Authorization: "Bearer tok", Accept: "application/vnd.github.raw" });
  assert.equal(calls[0].init.credentials, "omit");
  assert.equal(calls[0].init.redirect, "follow");
});

test("repoPublicKey reads scripts/seal/repo-key.pub for the actions repo", async () => {
  const calls = mock(() => new Response(PUB));
  assert.equal(await repoPublicKey("", "actions"), PUB);
  assert.equal(calls[0].url, "https://api.github.com/repos/Lascade-Co/actions/contents/scripts/seal/repo-key.pub");
  assert.deepEqual(calls[0].init.headers, { Accept: "application/vnd.github.raw" });
});

test("repoPublicKey rejects a missing or malformed key file, naming it", async () => {
  mock(() => json({ message: "Not Found" }, 404));
  await assert.rejects(repoPublicKey("tok", "connector"), (err) => err.status === 404 && /\.github\/seal\/repo-key\.pub/.test(err.message) && /connector/.test(err.message));
  for (const body of ["", "not a key", `${PUB}\n${PUB}\n`, `${PUB}x`, "AGE-SECRET-KEY-1ABC"]) {
    mock(() => new Response(body));
    await assert.rejects(repoPublicKey("tok", "connector"), (err) => /\.github\/seal\/repo-key\.pub/.test(err.message) && /single age recipient/.test(err.message), JSON.stringify(body));
  }
});

test("the actions repo's key path does not depend on case", () => {
  assert.equal(publicKeyPath("Actions"), "scripts/seal/repo-key.pub");
  assert.equal(publicKeyPath("actions"), "scripts/seal/repo-key.pub");
  assert.equal(publicKeyPath("Connector"), ".github/seal/repo-key.pub");
});
