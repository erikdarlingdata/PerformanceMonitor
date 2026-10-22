/* Runs the pure parts of the Compare picker on the Top Queries, Top Procedures and Query Store grids (#6300) under Node and prints one
   line of JSON: how each choice shifts the window, how rows match, the deltas, NEW and GONE, the sort, and what is drawn when the
   earlier window has no data or a read fails.
       node top-compare-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder beside a package.json that marks it as an ES module (#5279). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
globalThis.document = { createElement: () => ({}), createTextNode: () => ({}) };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "top-compare-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const tc = await import(pathToFileURL(path.join(scratch, "pages", "top-compare.js")).href);
  const DAY = 86400000;
  const end = Date.parse("2026-10-21T12:00:00Z");
  const out = {};

  out.shifts = {
    off: tc.compareShiftMs("off") / DAY,
    yesterday: tc.compareShiftMs("yesterday") / DAY,
    lastweek: tc.compareShiftMs("lastweek") / DAY,
    sameday: tc.compareShiftMs("sameday") / DAY,
    yesterdayAsOf: tc.comparisonAsOf("yesterday", end),
    lastweekAsOf: tc.comparisonAsOf("lastweek", end),
  };

  const q = (hash, db, execs, dur, cpu, extra) => ({ database_name: db, query_hash: hash, execution_count: execs, avg_elapsed_ms: dur, avg_cpu_ms: cpu, query_text: "select " + hash, ...extra });
  const current = [q("0xA", "d", 10, 120, 50), q("0xB", "d", 100, 10, 5), q("0xN", "d", 5, 40, 20), q("0xS", "d", 20, 100, 10)];
  const baseline = [q("0xA", "d", 10, 100, 100), q("0xB", "d", 100, 20, 5), q("0xG", "d", 7, 30, 10), q("0xS", "other", 20, 100, 10)];
  const rows = tc.compareRows("queries", current, baseline);
  out.queries = rows.map((r) => ({ name: r.name, db: r.database_name, status: r.status, execs: r.execution_count, base: r.baseline_execution_count, dur: r.duration_delta_pct, cpu: r.cpu_delta_pct, exec: r.execution_delta_pct, durSev: r.duration_delta_sev, cpuSev: r.cpu_delta_sev }));
  out.noReads = rows.every((r) => !Object.keys(r).some((k) => /read/i.test(k)));

  /* Two rows of one key in a window are one row: executions summed, averages weighted by executions. */
  const dup = tc.compareRows("queries", [q("0xA", "d", 10, 100, 10, { host_object: "p1" }), q("0xA", "d", 30, 200, 30, { host_object: "p2" })], [q("0xA", "d", 40, 100, 10)]);
  out.merged = { count: dup.length, execs: dup[0].execution_count, avg: dup[0].avg_duration_ms, status: dup[0].status };

  const procs = tc.compareRows("procedures",
    [{ database_name: "d", full_name: "dbo.p", execution_count: 4, avg_elapsed_ms: 8, avg_cpu_ms: 2 }],
    [{ database_name: "d", full_name: "dbo.p", execution_count: 2, avg_elapsed_ms: 4, avg_cpu_ms: 4 }, { database_name: "d", full_name: "dbo.gone", execution_count: 1, avg_elapsed_ms: 1, avg_cpu_ms: 1 }]);
  out.procedures = procs.map((r) => ({ name: r.name, status: r.status, dur: r.duration_delta_pct, execSev: r.execution_delta_sev }));

  const qs = tc.compareRows("querystore", [{ database_name: "d", query_hash: "0xQ", execution_count: 3, avg_duration_ms: 30, avg_cpu_ms: 3, query_id: 9, plan_id: 2 }], [{ database_name: "d", query_hash: "0xQ", execution_count: 3, avg_duration_ms: 20, avg_cpu_ms: 3, query_id: 9, plan_id: 1 }]);
  out.querystore = qs.map((r) => ({ status: r.status, dur: r.duration_delta_pct, cpu: r.cpu_delta_pct }));

  /* The earlier window empty, errored or holding no executions is never drawn as every row NEW. */
  const data = (r) => ({ kind: "data", data: { queries: r } });
  const plan = (c, b) => { const p = tc.planComparison("queries", c, b); return { mode: p.mode, notice: p.notice || null, rows: p.rows ? p.rows.length : null }; };
  out.plans = {
    ok: plan(data(current), data(baseline)),
    baselineEmpty: plan(data(current), { kind: "empty", message: "none" }),
    baselineError: plan(data(current), { kind: "error", message: "boom" }),
    baselineNoExecutions: plan(data(current), data([q("0xA", "d", 0, 0, 0)])),
    baselineNoRows: plan(data(current), data([])),
    currentError: plan({ kind: "error", message: "x" }, data(baseline)),
    currentEmpty: plan({ kind: "empty", message: "x" }, data(baseline)),
    bothEmpty: plan({ kind: "empty", message: "x" }, { kind: "empty", message: "y" }),
  };
  out.formats = { up: tc.formatDelta(12.34), down: tc.formatDelta(-4), none: tc.formatDelta(null), zeroBase: tc.deltaPct(5, 0) };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
