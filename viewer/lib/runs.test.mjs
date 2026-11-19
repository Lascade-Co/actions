import { test } from "node:test";
import assert from "node:assert/strict";
import { runRow, pageInfo, runsQuery, relativeTime, duration, runState, readListParams, statusOptions, SEARCH_LIMIT, STATUS_FILTERS } from "./runs.js";

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
  assert.deepEqual(runRow(RUN, NOW), {
    id: "123456789",
    title: "fix: something",
    workflow: "Vinext deploy",
    number: 42,
    event: "repository_dispatch",
    branch: "main",
    status: "completed",
    conclusion: "failure",
    state: "failure",
    href: "?run=123456789",
    startedAt: "2026-11-20T11:00:00Z",
    when: "1 hour ago",
    duration: "3m 5s",
    attempt: 1,
  });
});

test("runRow adds the attempt to the link only when it is above 1", () => {
  const row = runRow({ ...RUN, run_attempt: 3 }, NOW);
  assert.equal(row.href, "?run=123456789&attempt=3");
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
  assert.deepEqual(pageInfo(51, 9, 25), { page: 9, pages: 3, hasPrev: true, hasNext: false });
});

test("pageInfo caps pages at a result limit when given one", () => {
  assert.equal(SEARCH_LIMIT, 1000);
  assert.equal(pageInfo(5000, 1, 25).pages, 200);
  assert.equal(pageInfo(5000, 1, 25, SEARCH_LIMIT).pages, 40);
  assert.equal(pageInfo(5000, 40, 25, SEARCH_LIMIT).hasNext, false);
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
