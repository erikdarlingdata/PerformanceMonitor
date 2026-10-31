/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readTool, mount, readErrorStrip, emptyStrip, noticeStrip } from "../../util.js";
import { getPanelSignal, VIZ, newestFirst } from "../../panels.js";
import { READ_FIELDS } from "../../read-fields.js";
import { dailyHealthCalendar } from "../daily-calendar.js";
import { fillOverviewStrips } from "../overview-strips.js";
import { panelShell, renderInto } from "./shared.js";

/** #6291: the Overview's five stacked strips (CPU, waits, blocking and deadlocks, buffer pool, file I/O latency): see overview-strips.js. */
export function overviewStripsPanel(server, ctx) {
  const { panel, body } = panelShell("Overview timeline", ctx.label, 2, "get_file_io_trend");
  fillOverviewStrips(body, server, ctx);
  return panel;
}

/**
 * The Overview tab's two daily-summary panels, today's tile and the Daily Health Calendar (#2484), over ONE
 * fetch of get_daily_summary_range (#3905).
 *
 * They used to be two reads: get_daily_summary for the tile and the range for the grid. On the largest
 * production store that pair was 26 of the tab's 28 seconds, mostly for store-side reasons fixed in the
 * statement and the coverage gate, but the tile's read also recomputed a day the range had just computed. The
 * range already holds today: its last day is the anchor day, `to_date`, banded against the same clock off the
 * same aggregate. So the tile is that row, and the store runs one statement per load instead of two. One
 * figure can change, and the change makes the two panels agree: the tile's unique_queries for today is now
 * the range's own count, which comes from the rollup tier's materialized hours when the month routes to a
 * rollup (#3653 A6), where the single-day read counted raw. Before, the tile and the grid's last row could
 * print different numbers for the same day on the same page. The row is matched on the server's `to_date`
 * rather than on a date the page computes (R1). A day with nothing collected yet has no row, and the tile
 * says so rather than rendering dashes.
 *
 * #3653 A6: a day the store's rollup tier never materialized for this server answers `unique_queries: null`
 * (NOT 0 — the tier did not carry the day; nothing says the server ran no queries) and the read names every
 * such day in `days_missing[]`. The cell renders that null as "not materialized" through DAILY_RANGE_COLUMNS,
 * but one blank in one column of a thirty-row grid is easy to read past, so the read's list is rendered as ONE
 * notice line above the rows. A composite rather than a tablePanel() descriptor because tablePanel()'s `noteKey` carries
 * a server-authored STRING (#3278) and this disclosure is a server-authored LIST: the dates are the server's,
 * the sentence around them is the page's, and no figure here is one the page derived (R1). The span is the
 * calendar's month (the fetch reaches 31 days so the current month is whole), deliberately not ctx.hours — the page's
 * range tops out well short of a month, and a calendar drawn over six hours is not a calendar. #6294: the calendar
 * panel is a month calendar (daily-calendar.js) whose "Show as table" is this grid over the shown month.
 */
export function dailySummaryPanels(server) {
  const tile = panelShell("Daily Summary", "today (UTC)", 2, "get_daily_summary_range");
  const calendar = panelShell("Daily Health Calendar", "month (UTC)", 2, "get_daily_summary_range");
  const signal = getPanelSignal();
  (async () => {
    /* 31 days, not 30 (#6294): the month calendar's current month starts on the 1st, which is up to 30 days before today. */
    const res = await readTool("get_daily_summary_range", { server, days_back: 31 }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") {
      mount(tile.body, readErrorStrip(res.message));
      return mount(calendar.body, readErrorStrip(res.message));
    }
    if (res.kind === "empty") {
      /* The range's "empty" sentence is about the span and ends in MCP parameter advice (widen days_back, move
         as_of); for today's tile the fact is simpler. "unavailable" (nothing ever collected) says the same thing
         to both panels, so it is the server's sentence on both. */
      mount(tile.body, emptyStrip(res.status === "empty" ? "Nothing has been collected for this server today (UTC)." : res.message));
      /* "empty" is a span with no day while the server has history elsewhere, so the calendar still opens, every day "no data", and an
         earlier month can be stepped to (#6294); "unavailable" is a server nothing was ever collected for, which has no month to show. */
      if (res.status !== "empty") return mount(calendar.body, emptyStrip(res.message));
      return renderInto(calendar.body, () => dailyHealthCalendar({ server, initial: null, read: (tool, params) => readTool(tool, params, signal), renderTable: (month) => dailyRangeGrid(month.rows, month.missing) }));
    }

    /* Each panel renders on its own, the way fanout() guards its specs: a render fault in one says so in that
       panel instead of leaving both on their loading strips. */
    const data = res.data || {};
    const days = Array.isArray(data.days) ? data.days : [];
    renderInto(tile.body, () => {
      const today = days.find((day) => day.summary_date === data.to_date);
      return today
        ? VIZ.stat(today, { stats: DAILY_STATS })
        : emptyStrip("Nothing has been collected for this server yet " + (data.to_date ? "on " + data.to_date : "today") + " (UTC).");
    });

    /* The month calendar (#6294, daily-calendar.js) over the same fetch; "Show as table" is the grid this panel was, over the shown month. */
    renderInto(calendar.body, () => dailyHealthCalendar({ server, initial: data, read: (tool, params) => readTool(tool, params, signal), renderTable: (month) => dailyRangeGrid(month.rows, month.missing) }));
  })();
  return [tile.panel, calendar.panel];
}

/** The Daily Health Calendar's table: one row per collected day, newest first, with the #3653 A6 notice above it for the days whose
 *  unique_queries the rollup tier never carried (a server-authored LIST, so a composite rather than tablePanel()'s string `noteKey`). */
function dailyRangeGrid(rows, missing) {
  const grid = VIZ.table(
    { days: rows },
    {
      rowsKey: "days",
      columns: DAILY_RANGE_COLUMNS,
      /* Click-through 10: newest day first, as the other grids open. */
      orderRows: newestFirst("summary_date"),
      emptyText:
        "No collected days in this month. A day with any collection appears here, so a missing day is a gap in collection.",
    }
  );
  if (!missing.length) return grid;
  const plural = missing.length === 1 ? "" : "s";
  return [
    noticeStrip(
      "Unique queries not materialized for " + missing.length + " day" + plural + " (" + missing.join(", ") + "): " +
        "the rollup tier that answers this range never carried " + (plural ? "those days" : "that day") +
        " for this server, so the cell says so rather than 0. The service's start-up repair closes such days the next time it runs."
    ),
    grid,
  ];
}

/* ─────────────────────────── stat descriptors ─────────────────────────── */

export const OVERVIEW_STATS = READ_FIELDS.get_server_summary.stat.stats;

const DAILY_STATS = [
  { key: "summary_date", label: "Date", format: "text", small: true },
  /* One health card, not two. The daily-summary row carried overall_health as the band's LABEL
     (DarlingHealthReader: OverallHealth => DailyHealthBandCalculator.Label(HealthBand)) — the same value
     health_band carries — so an "overall_health" card duplicated this one AND, formatted as num1, rendered
     the label "Critical" as NaN. #2807. A numeric health SCORE would be a new backend field, not this label. */
  { key: "health_band", label: "Band", format: "text", small: true },
  { key: "top_wait_type", label: "Top wait", format: "text", small: true },
  { key: "total_wait_time_sec", label: "Total wait", format: "int" },
  { key: "unique_queries", label: "Unique queries", format: "int" },
  { key: "blocking_events", label: "Blocking", format: "int" },
  { key: "deadlock_count", label: "Deadlocks", format: "int" },
  { key: "alert_count", label: "Alerts", format: "int" },
  { key: "collection_errors", label: "Collection errors", format: "int" },
];

/* #2484: one row per collected day, the Performance Calendar's month grid as a table. Band first, because
   the point of the read is to scan for the day that stands out and then drill in with get_daily_summary. */
const DAILY_RANGE_COLUMNS = [
  { key: "summary_date", label: "Date" },
  { key: "health_band", label: "Band" },
  { key: "top_wait_type", label: "Top wait" },
  { key: "total_wait_time_sec", label: "Total wait (s)", format: "int" },
  /* #3653 A6: null is "not carried at this tier" (the rollup never materialized this server's day), which is
     the opposite claim from 0 and from the page's "—" ("there was none of this"), so it gets its own words;
     dailySummaryPanels lists the same days above the grid from the read's days_missing[]. */
  {
    key: "unique_queries",
    label: "Unique queries",
    render: (row) =>
      row.unique_queries == null
        ? el("span", { class: "muted", title: "The rollup tier that answers this day never materialized it for this server; the day is listed above the grid." }, ["not materialized"])
        : document.createTextNode(Number(row.unique_queries).toLocaleString(undefined, { maximumFractionDigits: 0 })),
  },
  { key: "blocking_events", label: "Blocking", format: "int" },
  { key: "max_block_duration_ms", label: "Peak block", format: "ms" },
  { key: "deadlock_count", label: "Deadlocks", format: "int" },
  { key: "high_cpu_events", label: "High CPU", format: "int" },
  { key: "memory_pressure_events", label: "Mem pressure", format: "int" },
  { key: "alert_count", label: "Alerts", format: "int" },
  { key: "collection_errors", label: "Collection errors", format: "int" },
];

