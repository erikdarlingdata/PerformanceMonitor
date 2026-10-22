/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6295: the System Events tab's Corruption Events and Contention Events charts, the same series the desktop viewer draws.
 * They read the chart form of get_health_parser_system_health (chart: true), whose time buckets carry each counter's
 * largest value and each sick spinlock type's backoffs beside the CPU means. Corruption Events: bad pages, dump requests,
 * access violations, write access violations. Contention Events: non-yielding tasks, latch warnings, sick spinlocks by
 * type (the CPU comparison is the page's existing CPU chart).
 *
 * A counter that was collected as 0 draws as 0. A counter no snapshot carried (null) is a gap, and a chart with no bucket
 * that carried it says so instead of drawing a zero line. server-tabs.js appends systemEventsChartSpecs() to the fanout
 * that already makes the CPU chart, so every chart here shares that one fetch.
 */
import { emptyStrip } from "../util.js";
import { VIZ } from "../panels.js";

/** The counter charts, in the viewer's order. `key` is the chart read's per-bucket field. */
export const COUNTER_CHARTS = [
  { group: "Corruption Events", title: "Bad Pages", key: "bad_pages_detected", unit: "events", empty: "No bad pages were reported" },
  { group: "Corruption Events", title: "Dump Requests", key: "interval_dump_requests", unit: "events", empty: "No dump request count was reported" },
  { group: "Corruption Events", title: "Access Violations", key: "is_access_violation_occurred", unit: "events", empty: "No access violation count was reported" },
  { group: "Corruption Events", title: "Write Access Violations", key: "write_access_violation_count", unit: "events", empty: "No write access violation count was reported" },
  { group: "Contention Events", title: "Non-Yielding Tasks", key: "non_yielding_tasks_reported", unit: "events", empty: "No non-yielding task count was reported" },
  { group: "Contention Events", title: "Latch Warnings", key: "latch_warnings", unit: "events", empty: "No latch warning count was reported" },
];

/** True when at least one bucket carries a number for `key`. A 0 counts: it is a collected zero. */
export function counterHasData(entries, key) {
  return (entries || []).some((e) => e && e[key] != null && !Number.isNaN(Number(e[key])));
}

/** The sick spinlock chart's points: one row per bucket, one field per named type (s0, s1, ...), and the series for them.
    A type a bucket did not report is absent from that row, so its line breaks there. */
export function spinlockPoints(data) {
  const types = Array.isArray(data && data.sick_spinlock_types) ? data.sick_spinlock_types : [];
  const entries = (data && data.entries) || [];
  const points = entries.map((e) => {
    const row = { event_time: e.event_time };
    types.forEach((type, i) => {
      const hit = (e.sick_spinlocks || []).find((s) => s && s.type === type);
      row["s" + i] = hit ? hit.backoffs : null;
    });
    return row;
  });
  return { points, series: types.map((type, i) => ({ key: "s" + i, label: type })) };
}

/* "Counter" viz: one counter field of the chart read as a line of whole numbers, or the descriptor's empty text when no
   bucket carried it. */
VIZ.healthCounter = (data, desc) => {
  const key = desc.series[0].key;
  if (!counterHasData(getEntries(data, desc), key)) return emptyStrip(desc.emptyText);
  return VIZ.line(data, { ...desc, format: "int" });
};

/* "Spinlocks" viz: a line per sick spinlock type the read named, or the empty text when none was reported. */
VIZ.healthSpinlocks = (data, desc) => {
  const { points, series } = spinlockPoints(data);
  if (!series.length) return emptyStrip(desc.emptyText);
  return VIZ.line({ spinlocks: points }, { ...desc, rowsKey: "spinlocks", series, format: "int" });
};

/** The sentence over the counter charts. It is shown only while a chart is drawn (#6439): on an Azure SQL Database, or a server
    with no system_health row, every card says the data is absent and "charted below" would be false. */
export const COUNTERS_INTRO =
  "The corruption and contention counters are charted below (each point is the largest value in its time bucket); the raw parsed rows follow.";

/** True when at least one counter chart or the sick spinlock chart of this chart read draws a line, the test for COUNTERS_INTRO. */
export function counterChartsDrawn(data) {
  const entries = (data && data.entries) || [];
  return COUNTER_CHARTS.some((c) => counterHasData(entries, c.key)) || spinlockPoints(data).series.length > 0;
}

function getEntries(data, desc) {
  return (data && data[desc.rowsKey]) || [];
}

/** The fanout specs for the seven charts. `label` is the page's window label. */
export function systemEventsChartSpecs(label) {
  const specs = COUNTER_CHARTS.map((c) => ({
    title: c.group + ": " + c.title,
    subtitle: label,
    viz: "healthCounter",
    span: 1,
    /* A window with no system_health row leaves these cards out: the CPU chart and the entries table beside them carry the empty text. */
    hideWhenNoRows: true,
    rowsKey: "entries",
    xKey: "event_time",
    series: [{ key: c.key, label: c.title }],
    unit: c.unit,
    emptyText: c.empty + " in this window.",
  }));
  specs.push({
    title: "Contention Events: Sick Spinlocks",
    subtitle: label,
    viz: "healthSpinlocks",
    span: 1,
    /* A window with no system_health row leaves these cards out: the CPU chart and the entries table beside them carry the empty text. */
    hideWhenNoRows: true,
    rowsKey: "entries",
    xKey: "event_time",
    unit: "backoffs",
    emptyText: "No sick spinlock was reported in this window.",
  });
  return specs;
}
