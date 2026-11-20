// Builds the plain-text export of one or more jobs' logs: the public log with
// sealed segments merged in place of their markers, as the viewer shows them.
import { stripAnsi } from "./ansi.js";

// 8-bit (C1) forms: CSI with its parameters, and OSC, DCS, SOS, PM and APC
// strings ended by BEL, ST or ESC \ (an OSC 52 here would write the clipboard).
const C1_SEQ = /\u009b[0-?]*[ -/]*[@-~]|[\u0090\u0098\u009d\u009e\u009f][^\x07\x1b\u009c]*(?:\x07|\u009c|\x1b\\)/g;
// C0 and C1 controls other than tab and newline; a lone CR is dropped too.
const CONTROLS = /[\x00-\x08\x0b-\x1f\x7f-\x9f]/g;

/** One line as plain text: no escape sequences and no control characters but tab. */
export function plainLine(text) {
  return stripAnsi(String(text).replace(C1_SEQ, "")).replace(CONTROLS, "");
}

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
    out.push(`==== ${plainLine(job.name)} (${plainLine(job.conclusion || "unknown")}) ====`);
    if (job.unavailable || !job.sections) {
      out.push(`log unavailable (${plainLine(job.unavailable || "no log")})`);
      return;
    }
    for (const sec of job.sections) {
      out.push(`---- ${plainLine(sec.title)} ----`);
      for (const line of sec.lines) {
        const text = plainLine(line.text);
        out.push(line.ts && !line.sealed ? `${line.ts} ${text}` : text);
      }
      for (const md of sec.summaries ?? []) {
        out.push("---- summary ----");
        out.push(...String(md).replace(/\r?\n$/, "").split(/\r?\n/).map(plainLine));
      }
    }
  });
  return out.length ? `${out.join("\n")}\n` : "";
}

const MAX_NAME = 200;

function part(s) {
  return String(s ?? "").replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^[.-]+|[.-]+$/g, "");
}

/** `[<repo>-]<workflow>-run<n>-attempt<n>[-<job>].log`, using only [A-Za-z0-9._-]. */
export function exportFilename({ repo, workflow, runNumber, attempt, job }) {
  const prefix = part(repo);
  const head = `${prefix ? `${prefix}-` : ""}${part(workflow) || "run"}-run${part(runNumber)}-attempt${part(attempt)}`;
  const tail = job ? part(job) : "";
  const name = tail ? `${head}-${tail}` : head;
  return `${name.slice(0, MAX_NAME - 4).replace(/[.-]+$/, "")}.log`;
}
