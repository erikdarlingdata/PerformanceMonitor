/* Runs the pure parts of the single-read pages' time range code (#5562, lane L2b) under Node and prints one line of JSON:
   the collector interval a catalog serves for a collector (a per-server schedule row wins), the "collected every N minutes" note
   (shown below 3 samples, never widening the range), the rolling-only refusals (R5), the whole-hours window a held range gives a
   read, and the Fleet Sweep timeline's trim to the exact range.
       node page-range-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder beside a package.json that marks it as an ES module (#5279). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
globalThis.document = { createElement: () => ({}), createTextNode: () => ({}) };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "page-range-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (...parts) => import(pathToFileURL(path.join(scratch, ...parts)).href);
  const pageRange = await load("page-range.js");
  const tr = await load("time-range.js");

  const MIN = 60000;
  const HOUR = 3600000;
  const catalog = (interval) => ({
    reads: [
      { name: "get_wait_trend", params: [{ name: "hours", collector: "wait_stats", collector_interval_minutes: interval, max_hours: 168 }] },
      { name: "get_cpu_utilization", params: [{ name: "hours", collector: "cpu_utilization", collector_interval_minutes: 1, max_hours: 168 }] },
      { name: "get_alert_history", params: [{ name: "hours", collector: null, collector_interval_minutes: null, max_hours: 2160 }] },
    ],
  });
  const out = {};

  out.interval = {
    waitStatsDefault: pageRange.collectorIntervalFromCatalog(catalog(1), "wait_stats") / MIN,
    waitStatsServerOverride: pageRange.collectorIntervalFromCatalog(catalog(5), "wait_stats") / MIN,
    unknownCollector: pageRange.collectorIntervalFromCatalog(catalog(1), "no_such"),
    noCollector: pageRange.collectorIntervalFromCatalog(catalog(1), null),
    noCatalog: pageRange.collectorIntervalFromCatalog(null, "wait_stats"),
  };

  const note = (spanMin, intervalMin) => tr.sampleIntervalNote(spanMin * MIN, intervalMin * MIN);
  out.note = {
    fiveMinutesAtFive: note(5, 5),
    fourteenMinutesAtFive: note(14, 5),
    fifteenMinutesAtFive: note(15, 5),
    fiveMinutesAtOne: note(5, 1),
    thirtyMinutesAtHour: note(30, 60),
    threeHoursAtHour: note(180, 60),
    noInterval: tr.sampleIntervalNote(5 * MIN, null),
  };

  const rolling = (spec, spanMs, minSpanMs, stepMs) => tr.rollingOnlyRefusal(spec, spanMs, minSpanMs, stepMs);
  out.rolling = {
    ok: rolling(tr.relativeSpec(24 * HOUR), 24 * HOUR),
    underAnHour: rolling(tr.relativeSpec(30 * MIN), 30 * MIN),
    fractionalHours: rolling(tr.relativeSpec(90 * MIN), 90 * MIN),
    calendar: rolling(tr.calendarSpec("yesterday"), 24 * HOUR),
    finished: rolling(tr.fixedSpec(0, 2 * HOUR), 2 * HOUR),
    since: rolling(tr.sinceSpec(0), 2 * HOUR),
    daysOk: rolling(tr.relativeSpec(30 * 24 * HOUR), 30 * 24 * HOUR, 24 * HOUR, 24 * HOUR),
    daysHours: rolling(tr.relativeSpec(36 * HOUR), 36 * HOUR, 24 * HOUR, 24 * HOUR),
    daysBelowFloor: rolling(tr.relativeSpec(12 * HOUR), 12 * HOUR, 24 * HOUR, 24 * HOUR),
  };

  const now = Date.parse("2026-10-08T12:00:00Z");
  const w = (spec) => pageRange.windowOfSpec(spec, now, "UTC");
  out.window = {
    thirtyMinutes: w(tr.relativeSpec(30 * MIN)),
    rolling: w(tr.relativeSpec(4 * HOUR)),
    finished: w(tr.fixedSpec(now - 26 * HOUR, now - 2 * HOUR)),
  };

  /* R9: a calendar period is exempt from the 5 minute floor and refused only at zero length (exactly midnight). */
  const today = (iso) => pageRange.windowResultOfSpec(tr.calendarSpec("today"), Date.parse(iso), "UTC");
  out.calendarFloor = {
    twoMinutesIn: today("2026-10-08T00:02:00Z"),
    atMidnight: today("2026-10-08T00:00:00Z"),
    typedUnderFloor: tr.resolveSpec(tr.relativeSpec(3 * MIN), now, "UTC").error.code,
  };

  /* A server's catalog is read once, kept until its time is up and then read again. The entry expires on lookup, not on a timer:
     a pending timer would keep this process (and a test waiting on it) alive for the whole time to live. */
  let catalogReads = 0;
  let clock = now;
  const realNow = Date.now;
  Date.now = () => clock;
  globalThis.fetch = async () => {
    catalogReads++;
    return { ok: true, status: 200, text: async () => JSON.stringify(catalog(1)) };
  };
  try {
    await pageRange.serverCatalog("SRV1");
    await pageRange.serverCatalog("SRV1");
    const afterTwoLookups = catalogReads;
    clock += 4 * MIN;
    await pageRange.serverCatalog("SRV1");
    const afterFourMinutes = catalogReads;
    clock += 2 * MIN;
    await pageRange.serverCatalog("SRV1");
    out.catalogTtl = { afterTwoLookups, afterFourMinutes, afterSixMinutes: catalogReads };
  } finally {
    Date.now = realNow;
  }

  const sweeps = await load("pages", "sweeps.js");
  const stamp = (minutesAgo) => ({ swept_at: new Date(now - minutesAgo * MIN).toISOString() });
  const trimmed = sweeps.timelineSweeps([stamp(5), stamp(20), stamp(50), { swept_at: "not a time" }], pageRange.windowOfSpec(tr.relativeSpec(30 * MIN), now, "UTC"));
  out.sweeps = { kept: trimmed.length, all: sweeps.timelineSweeps([stamp(5), stamp(500)], null).length };

  const jobs = await load("pages", "job-history.js");
  const runs = [{ run_time: new Date(now - 5 * MIN).toISOString() }, { run_time: new Date(now - 50 * MIN).toISOString() }, { run_time: "?" }];
  out.jobs = { kept: jobs.trimRunsToStart({ runs }, pageRange.windowOfSpec(tr.relativeSpec(30 * MIN), now, "UTC")).runs.length };

  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
