// Builds the plain-text export of one or more jobs' logs: the public log with
// sealed segments merged in place of their markers, as the viewer shows them.
import { stripAnsi } from "./ansi.js";

/**
 * Text of the jobs, in the order given. Each job is
 * `{name, conclusion, sections}` (merged sections, each with optional
 * `summaries`) or `{name, conclusion, unavailable: reason}`. Public lines keep
 * their timestamp; sealed and unavailable lines are written as they are.
 */
export function exportText(jobs) {
  const out = [];
  jobs.forEach((job, i) => {
    if (i) out.push("");
    out.push(`==== ${stripAnsi(job.name)} (${job.conclusion || "unknown"}) ====`);
    if (job.unavailable || !job.sections) {
      out.push(`log unavailable (${job.unavailable || "no log"})`);
      return;
    }
    for (const sec of job.sections) {
      out.push(`---- ${stripAnsi(sec.title)} ----`);
      for (const line of sec.lines) {
        const text = stripAnsi(line.text);
        out.push(line.ts && !line.sealed ? `${line.ts} ${text}` : text);
      }
      for (const md of sec.summaries ?? []) {
        out.push("---- summary ----");
        out.push(...stripAnsi(md).replace(/\r?\n$/, "").split(/\r?\n/));
      }
    }
  });
  return out.length ? `${out.join("\n")}\n` : "";
}

const MAX_NAME = 200;

function part(s) {
  return String(s ?? "").replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^[.-]+|[.-]+$/g, "");
}

/** `<workflow>-run<n>-attempt<n>[-<job>].log`, using only [A-Za-z0-9._-]. */
export function exportFilename({ workflow, runNumber, attempt, job }) {
  const head = `${part(workflow) || "run"}-run${part(runNumber)}-attempt${part(attempt)}`;
  const tail = job ? part(job) : "";
  const name = tail ? `${head}-${tail}` : head;
  return `${name.slice(0, MAX_NAME - 4).replace(/[.-]+$/, "")}.log`;
}
