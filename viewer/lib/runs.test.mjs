import { test } from "node:test";
import assert from "node:assert/strict";
import { runRow, pageInfo, maxPage, runsQuery, relativeTime, duration, runState, readListParams, statusOptions, isRepoName, readRoute, parsePick, repoRow, repoId, canonicalRepoName, homeHint, SEARCH_LIMIT, STATUS_FILTERS } from "./runs.js";

const NOW = Date.parse("2026-11-20T12:00:00Z");

const RUN = {
  id: 123456789,
  name: "Vinext deploy",
  display_title: "fix: something",
  run_number: 42,
  event: "repository_dispatch",
  head_branch: "main",
  status: "completed",
  conclusion: "failure",
  run_attempt: 1,
  run_started_at: "2026-11-20T11:00:00Z",
  updated_at: "2026-11-20T11:03:05Z",
  created_at: "2026-11-20T10:59:00Z",
};

test("runRow maps a run to the fields of one list row", () => {
  assert.deepEqual(runRow(RUN, NOW, "actions"), {
    id: "123456789",
    title: "fix: something",
    workflow: "Vinext deploy",
    number: 42,
    event: "repository_dispatch",
    branch: "main",
    status: "completed",
    conclusion: "failure",
    state: "failure",
    href: "?repo=actions&run=123456789",
    startedAt: "2026-11-20T11:00:00Z",
    when: "1 hour ago",
    duration: "3m 5s",
    attempt: 1,
  });
});

test("runRow adds the attempt to the link only when it is above 1", () => {
  const row = runRow({ ...RUN, run_attempt: 3 }, NOW, "connector");
  assert.equal(row.href, "?repo=connector&run=123456789&attempt=3");
  assert.equal(row.attempt, 3);
});

test("runRow falls back for missing titles and has no duration while running", () => {
  const row = runRow({ ...RUN, display_title: "", status: "in_progress", conclusion: null, run_started_at: null }, NOW);
  assert.equal(row.title, "Vinext deploy");
  assert.equal(row.state, "running");
  assert.equal(row.duration, "");
  assert.equal(row.startedAt, "2026-11-20T10:59:00Z");
});

test("runRow rejects a run whose id is not a number", () => {
  assert.equal(runRow({ ...RUN, id: "1/../x" }, NOW), null);
});

test("runState maps statuses without an icon to queued or neutral", () => {
  assert.equal(runState({ status: "completed", conclusion: "success" }), "success");
  assert.equal(runState({ status: "completed", conclusion: "startup_failure" }), "failure");
  assert.equal(runState({ status: "completed", conclusion: "stale" }), "neutral");
  assert.equal(runState({ status: "completed", conclusion: null }), "neutral");
  assert.equal(runState({ status: "in_progress" }), "running");
  for (const s of ["queued", "waiting", "requested", "pending"]) assert.equal(runState({ status: s }), "queued");
});

test("relativeTime reads like GitHub", () => {
  assert.equal(relativeTime("2026-11-20T11:59:50Z", NOW), "just now");
  assert.equal(relativeTime("2026-11-20T11:55:00Z", NOW), "5 minutes ago");
  assert.equal(relativeTime("2026-11-20T09:00:00Z", NOW), "3 hours ago");
  assert.equal(relativeTime("2026-11-19T11:00:00Z", NOW), "yesterday");
  assert.equal(relativeTime("2026-11-10T12:00:00Z", NOW), "10 days ago");
  assert.equal(relativeTime("2026-08-01T12:00:00Z", NOW), "2026-08-01");
  assert.equal(relativeTime(null, NOW), "");
});

test("duration formats seconds, minutes and hours", () => {
  assert.equal(duration("2026-11-20T11:00:00Z", "2026-11-20T11:00:09Z"), "9s");
  assert.equal(duration("2026-11-20T11:00:00Z", "2026-11-20T13:05:00Z"), "2h 5m");
  assert.equal(duration(null, "2026-11-20T13:05:00Z"), "");
});

test("pageInfo counts pages and clamps the page", () => {
  assert.deepEqual(pageInfo(0, 1, 25), { page: 1, pages: 1, hasPrev: false, hasNext: false });
  assert.deepEqual(pageInfo(51, 2, 25), { page: 2, pages: 3, hasPrev: true, hasNext: true });
  assert.deepEqual(pageInfo(51, 3, 25), { page: 3, pages: 3, hasPrev: true, hasNext: false });
  // A page past the end is clamped to the last page.
  assert.deepEqual(pageInfo(51, 9, 25), { page: 3, pages: 3, hasPrev: true, hasNext: false });
});

test("pageInfo caps pages at a result limit when given one", () => {
  assert.equal(SEARCH_LIMIT, 1000);
  assert.equal(pageInfo(5000, 1, 25).pages, 200);
  assert.equal(pageInfo(5000, 1, 25, SEARCH_LIMIT).pages, 40);
  assert.equal(pageInfo(5000, 40, 25, SEARCH_LIMIT).hasNext, false);
  assert.equal(pageInfo(5000, 41, 25, SEARCH_LIMIT).page, 40);
});

test("runsQuery builds the runs path for the repo or one workflow", () => {
  assert.equal(runsQuery({}), "/runs?per_page=25&page=1");
  assert.equal(runsQuery({ page: 3, status: "failure" }), "/runs?per_page=25&page=3&status=failure");
  assert.equal(runsQuery({ page: 2, workflow: "987" }), "/workflows/987/runs?per_page=25&page=2");
  assert.equal(
    runsQuery({ page: 1, perPage: 10, event: "push", branch: "feat/a b" }),
    "/runs?per_page=10&page=1&event=push&branch=feat%2Fa+b",
  );
});

test("runsQuery rejects a workflow that is not an id, an unknown status and a bad page", () => {
  assert.throws(() => runsQuery({ workflow: "../../x" }), /workflow/i);
  assert.throws(() => runsQuery({ status: "bogus" }), /status/i);
  assert.throws(() => runsQuery({ page: 0 }), /page/i);
  assert.throws(() => runsQuery({ page: "2x" }), /page/i);
  assert.throws(() => runsQuery({ perPage: 1000 }), /per page/i);
});

test("readListParams keeps only valid page, workflow and status values", () => {
  assert.deepEqual(readListParams(new URLSearchParams("page=2&workflow=77&status=failure")), { page: 2, workflow: "77", status: "failure" });
  assert.deepEqual(readListParams(new URLSearchParams("page=-1&workflow=x&status=nope")), { page: 1, workflow: "", status: "" });
  assert.deepEqual(readListParams(new URLSearchParams("")), { page: 1, workflow: "", status: "" });
});

test("statusOptions adds a status from the URL that the filter does not offer", () => {
  assert.deepEqual(statusOptions(""), STATUS_FILTERS);
  assert.deepEqual(statusOptions("failure"), STATUS_FILTERS);
  assert.deepEqual(statusOptions("completed"), [...STATUS_FILTERS, ["completed", "completed"]]);
});

test("isRepoName accepts GitHub repo names only", () => {
  for (const ok of ["actions", "connector", "a.b_c-d", ".github", "x".repeat(100)]) assert.equal(isRepoName(ok), true, ok);
  for (const bad of ["", ".", "..", "....", "a/b", "a b", "x".repeat(101), "a?b", "a#b", "a%2F", null, undefined, 5]) assert.equal(isRepoName(bad), false, String(bad));
});

const route = (qs) => readRoute(new URLSearchParams(qs));

test("readRoute maps the URL to the repo picker, a repo's runs or a run", () => {
  assert.deepEqual(route(""), { view: "repos", repo: null, run: null });
  assert.deepEqual(route("repo=connector"), { view: "runs", repo: "connector", run: null });
  assert.deepEqual(route("repo=connector&run=12&attempt=2"), { view: "run", repo: "connector", run: "12" });
  // Old links without a repo are runs of the actions repo.
  assert.deepEqual(route("run=12"), { view: "run", repo: "actions", run: "12" });
  assert.deepEqual(route("run=&repo="), { view: "repos", repo: null, run: null });
});

test("readRoute refuses a bad repo or run id", () => {
  assert.deepEqual(route("repo=..&run=1"), { view: "repos", repo: null, run: null, error: "“..” is not a repository name." });
  assert.deepEqual(route("repo=a/b"), { view: "repos", repo: null, run: null, error: "“a/b” is not a repository name." });
  assert.deepEqual(route("repo=connector&run=1x"), { view: "runs", repo: "connector", run: null, error: "“1x” is not a run ID." });
  assert.deepEqual(route("run=x"), { view: "runs", repo: "actions", run: null, error: "“x” is not a run ID." });
});

test("parsePick opens GitHub run links of any Lascade-Co repo", () => {
  assert.deepEqual(parsePick("https://github.com/Lascade-Co/connector/actions/runs/123", ""), { repo: "connector", run: "123", attempt: null, job: null });
  assert.deepEqual(parsePick("https://github.com/lascade-co/connector/actions/runs/123/attempts/2", ""), { repo: "connector", run: "123", attempt: "2", job: null });
  assert.deepEqual(parsePick("github.com/Lascade-Co/actions/actions/runs/5/job/77", "x"), { repo: "actions", run: "5", attempt: null, job: "77" });
  assert.deepEqual(parsePick("https://github.com/Lascade-Co/connector/actions/runs/123/job/9?pr=1#step:2:3", ""), { repo: "connector", run: "123", attempt: null, job: "9" });
  for (const tail of ["/usage", "/workflow", "/artifacts/9", "/attempts/2/usage"]) {
    assert.equal(parsePick(`https://github.com/Lascade-Co/connector/actions/runs/123${tail}`, "").run, "123", tail);
  }
  assert.equal(parsePick("https://github.com/Lascade-Co/connector/actions/runs/123/attempts/2/usage", "").attempt, "2");
  assert.match(parsePick("https://github.com/Lascade-Co/connector/actions/runs/123abc", "").error, /run link/);
});

test("parsePick opens viewer links and bare run ids", () => {
  assert.deepEqual(parsePick("https://ci.lascade.com/?repo=connector&run=9&attempt=3", ""), { repo: "connector", run: "9", attempt: "3", job: null });
  assert.deepEqual(parsePick("https://ci.lascade.com/?run=9&attempt=1", "connector"), { repo: "actions", run: "9", attempt: "1", job: null });
  // A bare id is a run of the repo being browsed, else of actions.
  assert.deepEqual(parsePick(" 42 ", "connector"), { repo: "connector", run: "42", attempt: null, job: null });
  assert.deepEqual(parsePick("42", null), { repo: "actions", run: "42", attempt: null, job: null });
});

test("parsePick refuses other owners, bad repo names and junk", () => {
  assert.match(parsePick("https://github.com/evil/actions/actions/runs/1", "").error, /Lascade-Co/);
  assert.match(parsePick("https://github.com/Lascade-Co/../actions/runs/1", "").error, /run link/);
  assert.match(parsePick("https://github.com/Lascade-Co/connector/pulls", "").error, /run link/);
  assert.match(parsePick("https://ci.lascade.com/?repo=a%2Fb&run=1", "").error, /run link/);
  assert.match(parsePick("https://evil.example/?repo=x&run=1", "").error, /run link/);
  assert.match(parsePick("", "").error, /run link/);
});

test("repoRow maps a repo from the API to a picker row", () => {
  const repo = { name: "connector", description: "Data <b>sync</b>", pushed_at: "2026-11-20T11:00:00Z", topics: ["sealed-ci"] };
  assert.deepEqual(repoRow(repo, NOW), {
    name: "connector",
    description: "Data <b>sync</b>",
    pushedAt: "2026-11-20T11:00:00Z",
    when: "1 hour ago",
    sealed: true,
    href: "?repo=connector",
  });
  assert.equal(repoRow({ ...repo, topics: [] }, NOW).sealed, false);
  assert.equal(repoRow({ ...repo, topics: undefined }, NOW, new Set(["connector"])).sealed, true);
  assert.equal(repoRow({ ...repo, description: null }, NOW).description, "");
  assert.equal(repoRow({ ...repo, name: "../x" }, NOW), null);
});

test("repoId folds case, as GitHub matches repo names", () => {
  assert.equal(repoId("Connector"), "connector");
  assert.equal(repoId("actions"), "actions");
});

test("canonicalRepoName returns the API's name only when it differs by case alone", () => {
  assert.equal(canonicalRepoName("Connector", "connector"), "connector");
  assert.equal(canonicalRepoName("connector", "connector"), null);
  assert.equal(canonicalRepoName("connector", "other"), null);
  assert.equal(canonicalRepoName("connector", undefined), null);
  assert.equal(canonicalRepoName("x", "X/../y"), null);
});

test("homeHint asks for a key only for sealed repos", () => {
  const h = (o) => homeHint({ repo: "connector", token: true, hasKey: false, sealed: null, ...o });
  assert.deepEqual(h({ sealed: true }), { text: "Add the repo key for connector in Settings to see its sealed output.", settings: true });
  assert.deepEqual(h({ sealed: false }), { text: "This repository is not sealed; logs are public.", settings: false });
  // Unknown (the key file could not be read): say nothing rather than guess.
  assert.deepEqual(h({ sealed: null }), { text: "", settings: false });
  assert.deepEqual(h({ sealed: true, hasKey: true }), { text: "", settings: false });
  assert.match(h({ token: false, sealed: true }).text, /^Without a token .*the repo key for connector/);
  assert.equal(h({ token: false, sealed: true }).settings, true);
  const unsealed = h({ token: false, sealed: false });
  assert.match(unsealed.text, /^This repository is not sealed; logs are public\. Without a token/);
  assert.doesNotMatch(unsealed.text, /repo key/);
  assert.doesNotMatch(h({ token: false, sealed: null }).text, /repo key/);
  // The picker has no repo.
  assert.match(homeHint({ repo: null, token: false }).text, /^Without a token/);
  assert.deepEqual(homeHint({ repo: null, token: true }), { text: "", settings: false });
});

test("maxPage is the last page GitHub serves for a search filter", () => {
  assert.equal(maxPage(25, SEARCH_LIMIT), 40);
  assert.equal(maxPage(30, SEARCH_LIMIT), 34);
  assert.equal(maxPage(25), Infinity);
});
