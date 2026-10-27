/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6294: the Overview tab's Daily Health Calendar as a MONTH calendar, the desktop viewer's Performance Calendar.
 *
 * Weeks are rows, each day is a cell in its health band's colour, the buttons step a month back and forward, and a click on a
 * day opens its detail: the reasons the day has its band and a drill button per fitting tab, each opening the matching server
 * tab over that UTC day. Every day is a UTC day, because the store buckets health on date_trunc('day', ...) of naive UTC
 * collection times, and the page says so.
 *
 * Where each fact comes from (R1: the page derives none):
 *   - the cell colours: `health_band` of one get_daily_summary_range row per collected day. A month is read with `days_back` and
 *     `as_of` (the last second of its last day); the current month is read ending now, as the tile's own fetch is. No new read.
 *   - the day detail: get_daily_summary for that day, whose `reasons` and `drills` the service builds with the same calculator
 *     the desktop calendar uses.
 * A day with no row is "no data", never Healthy: the range lists a day when ANY collection happened, so a missing day is a
 * collection gap. A day the store purged comes back as band NoData and is drawn the same way, with the read's own note.
 */
import { el, errorStrip, emptyStrip, noticeStrip, storeHash } from "../util.js";

const DAY_MS = 86400000;
const MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
const BANDS = new Set(["Healthy", "Warning", "Critical"]);

/* ─────────────────────────── month arithmetic (pure) ─────────────────────────── */

const pad = (n) => String(n).padStart(2, "0");

/** The number of days in a month, `month0` being 0 for January. A leap February has 29. */
export function daysInMonth(year, month0) {
  return new Date(Date.UTC(year, month0 + 1, 0)).getUTCDate();
}

/** `yyyy-MM-dd` for a UTC day. */
export function isoDay(year, month0, day) {
  return String(year).padStart(4, "0") + "-" + pad(month0 + 1) + "-" + pad(day);
}

/**
 * The month as weeks of seven cells, a cell being the day of the month or null for the days of the neighbouring months that
 * pad the first and last week. `firstDow` is the weekday a week starts on (0 Sunday ... 6 Saturday). Only the weeks the month
 * needs are returned (four to six).
 */
export function monthLayout(year, month0, firstDow = 0) {
  const length = daysInMonth(year, month0);
  const firstWeekday = new Date(Date.UTC(year, month0, 1)).getUTCDay();
  const offset = (firstWeekday - firstDow + 7) % 7;
  const cells = [];
  for (let i = 0; i < offset; i++) cells.push(null);
  for (let d = 1; d <= length; d++) cells.push(d);
  while (cells.length % 7 !== 0) cells.push(null);
  const weeks = [];
  for (let i = 0; i < cells.length; i += 7) weeks.push(cells.slice(i, i + 7));
  return { year, month0, length, offset, weeks };
}

/** The month before or after `{ year, month0 }`. */
export function shiftMonth({ year, month0 }, by) {
  const index = year * 12 + month0 + by;
  return { year: Math.floor(index / 12), month0: ((index % 12) + 12) % 12 };
}

/** The month a `yyyy-MM-dd` day is in. */
export function monthOfDay(iso) {
  const m = /^(\d{4})-(\d{2})-\d{2}/.exec(String(iso || ""));
  return m ? { year: Number(m[1]), month0: Number(m[2]) - 1 } : null;
}

const sameMonth = (a, b) => a && b && a.year === b.year && a.month0 === b.month0;

/**
 * The parameters that read one month of get_daily_summary_range. The current month (and any later one) ends now, so it names no
 * `as_of` and asks for 31 days, which reach back past its first day on any day of it (the caller keeps the month's own days). An
 * earlier month ends on its last second, 23:59:59 and not midnight, because the read clamps the still-forming last day against
 * the anchor and a midnight anchor would give that day a zero-length window.
 */
export function monthReadParams(server, month, todayIso) {
  const todayMonth = monthOfDay(todayIso);
  if (!todayMonth || month.year * 12 + month.month0 >= todayMonth.year * 12 + todayMonth.month0) return { server, days_back: 31 };
  const length = daysInMonth(month.year, month.month0);
  return { server, days_back: length, as_of: isoDay(month.year, month.month0, length) + "T23:59:59Z" };
}

/** A read's `days` (and its `days_missing`) cut to one month, the days keyed by day of the month. */
export function monthDays(days, missing, month) {
  const byDay = new Map();
  for (const row of Array.isArray(days) ? days : []) {
    const at = monthOfDay(row && row.summary_date);
    if (sameMonth(at, month)) byDay.set(Number(String(row.summary_date).slice(8, 10)), row);
  }
  const prefix = String(month.year).padStart(4, "0") + "-" + pad(month.month0 + 1) + "-";
  const gaps = (Array.isArray(missing) ? missing : []).filter((d) => String(d).startsWith(prefix));
  return { byDay, rows: [...byDay.values()], missing: gaps };
}

/** What a cell shows for one day's row, or for no row: the band (Healthy, Warning, Critical or NoData) and its word. */
export function cellOf(row) {
  const band = row && BANDS.has(row.health_band) ? row.health_band : "NoData";
  return { band, word: band === "NoData" ? "no data" : band };
}

/** The weekday a week starts on in the reader's locale (0 Sunday ... 6 Saturday); Sunday where the browser cannot say. */
export function localeFirstDow() {
  try {
    const locale = new Intl.Locale(typeof navigator !== "undefined" && navigator.language ? navigator.language : "en-US");
    const info = typeof locale.getWeekInfo === "function" ? locale.getWeekInfo() : locale.weekInfo;
    if (info && Number.isInteger(info.firstDay)) return info.firstDay % 7;
  } catch {
    /* an older browser: the fallback below */
  }
  return 0;
}

/* ─────────────────────────── opening the matching tab at a day ─────────────────────────── */

/* The drill each DayDrillTarget opens: the server tab, and the panel of it brought into view (a prefix: the Top Queries title
   names its ranking). Deadlocks and Blocked Process Reports are both on the Blocking tab, Top Queries on the CPU tab. */
const DRILLS = {
  Deadlocks: { label: "View Deadlocks", tab: "blocking", panel: "Deadlocks" },
  Blocking: { label: "View Blocking", tab: "blocking", panel: "Blocked Process Reports" },
  TopQueries: { label: "View Top Queries", tab: "cpu", panel: "Top Queries by" },
};

/** The range a drill holds for a UTC day: the whole day, or from its start to now for today (a range may not end in the future). */
export function dayRange(iso, nowMs) {
  const startMs = Date.parse(iso + "T00:00:00Z");
  return { startMs, endMs: Math.min(startMs + DAY_MS, nowMs) };
}

/**
 * Hold the UTC day `iso` as the server page's range and route to the drill's tab, scrolling its panel into view. Returns the
 * reason it could not (a day the tab reads too far back for, today before it is five minutes old) or null.
 */
export async function openServerTabOnDay(server, iso, target) {
  const drill = DRILLS[target];
  if (!drill) return "That drill is not one this page knows.";
  try {
    const mod = await import("./server.js");
    const { startMs, endMs } = dayRange(iso, Date.now());
    const refused = mod.applyCustomRange(server, startMs, endMs, Date.now(), { redraw: false });
    if (refused) return refused;
    window.addEventListener(
      "hashchange",
      () => {
        const heads = [...document.querySelectorAll(".panel > h3")].filter((h) => h.firstChild && String(h.firstChild.textContent).startsWith(drill.panel));
        const last = heads[heads.length - 1];
        if (last && last.parentNode.scrollIntoView) last.parentNode.scrollIntoView({ block: "start" });
      },
      { once: true }
    );
    const hash = storeHash("#/server/" + encodeURIComponent(server) + "/" + drill.tab);
    if (location.hash === hash) window.dispatchEvent(new Event("hashchange"));
    else location.hash = hash;
    return null;
  } catch (e) {
    return "Could not open that day: " + (e && e.message ? e.message : "the page refused.");
  }
}

/* ─────────────────────────── the calendar ─────────────────────────── */

const numberText = (n) => Number(n).toLocaleString(undefined, { maximumFractionDigits: 0 });

/** The one line of figures under a day's reasons, from the row's own fields. */
function metricsLine(d) {
  const wait = d.top_wait_type ? d.top_wait_type : "none";
  const queries = d.unique_queries == null ? "not materialized at this tier" : numberText(d.unique_queries);
  return (
    "Top wait: " + wait + "   High-CPU samples: " + numberText(d.high_cpu_events ?? 0) +
    "   Total wait: " + numberText(d.total_wait_time_sec ?? 0) + " s   Unique queries: " + queries
  );
}

/**
 * The calendar node. Options:
 *   server        the server's name.
 *   initial       the Overview's own read of the current month: `{ days, days_missing, to_date }`, or null when it held no days.
 *   read          (tool, params) => the readTool result `{ kind, data, status, message }`; the page passes readTool.
 *   renderTable   ({ rows, missing }) => nodes: the month as the table (the page's existing grid), for "Show as table".
 *   openDay       (server, iso, target) => Promise of the refusal text or null; defaults to openServerTabOnDay.
 *   nowMs, firstDow  the clock and the week's first weekday (defaults: Date.now(), the locale's).
 */
export function dailyHealthCalendar({ server, initial, read, renderTable, openDay = openServerTabOnDay, nowMs = Date.now(), firstDow = localeFirstDow() }) {
  const now = new Date(nowMs);
  const todayIso = (initial && initial.to_date) || isoDay(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const currentMonth = monthOfDay(todayIso);
  const loaded = new Map(); // "y-m" -> { month data } | { failure }
  const keyOf = (m) => m.year + "-" + m.month0;
  let view = currentMonth;
  let selected = null;
  let asTable = false;
  let generation = 0;
  /* A fault inside a draw says so in the panel instead of leaving the last drawing in place. */
  const safely = () => draw().catch((e) => swap(grid, errorStrip("Could not draw the calendar: " + (e && e.message ? e.message : String(e)))));

  if (initial) loaded.set(keyOf(currentMonth), monthDays(initial.days, initial.days_missing, currentMonth));

  const root = el("div", { class: "daycal" });
  const head = el("div", { class: "daycal-head" });
  const grid = el("div", { class: "daycal-body" });
  const detail = el("div", { class: "daycal-detail", "aria-live": "polite" });
  root.appendChild(head);
  root.appendChild(grid);
  root.appendChild(detail);

  const swap = (host, ...children) => {
    while (host.firstChild) host.removeChild(host.firstChild);
    for (const c of children.flat()) if (c) host.appendChild(c);
  };

  async function load(month) {
    const key = keyOf(month);
    if (loaded.has(key)) return loaded.get(key);
    const res = await read("get_daily_summary_range", monthReadParams(server, month, todayIso));
    /* A read cancelled because the page was left says nothing about the month: nothing is cached, and nothing is drawn. */
    if (res.kind === "aborted") return { aborted: true };
    let entry;
    if (res.kind === "error") entry = { failure: res.message };
    /* "empty" is a month with no collected day while the server has history elsewhere: a month of "no data" cells, which is the
       true answer, and the read's sentence says so under it. "unavailable" is a server nothing was ever collected for. */
    else if (res.kind === "empty") entry = { ...monthDays([], [], month), note: res.message, neverCollected: res.status === "unavailable" };
    else entry = monthDays((res.data || {}).days, (res.data || {}).days_missing, month);
    if (!entry.failure) loaded.set(key, entry);
    return entry;
  }

  function drawHead() {
    const atCurrent = sameMonth(view, currentMonth);
    const button = (label, aria, onClick, disabled) =>
      el("button", { type: "button", class: "daycal-btn", "aria-label": aria, disabled: disabled ? "" : null, onClick }, [label]);
    swap(head,
      button("‹", "Previous month", () => go(shiftMonth(view, -1)), false),
      el("strong", { class: "daycal-month", text: MONTH_NAMES[view.month0] + " " + view.year + " (UTC)" }),
      button("›", "Next month", () => go(shiftMonth(view, 1)), atCurrent),
      atCurrent ? null : button("This month", "Back to this month", () => go(currentMonth), false),
      button(asTable ? "Show as calendar" : "Show as table", asTable ? "Show the month as a calendar" : "Show the month as a table", () => {
        asTable = !asTable;
        void safely();
      }, false)
    );
  }

  function pick(day) {
    selected = isoDay(view.year, view.month0, day);
    void safely();
  }

  async function drawDetail() {
    if (!selected || monthOfDay(selected) === null || !sameMonth(monthOfDay(selected), view) || asTable) return swap(detail);
    const iso = selected;
    const title = el("h4", { class: "daycal-detail-title", text: DAY_NAMES[new Date(iso + "T00:00:00Z").getUTCDay()] + ", " + iso + " (UTC)" });
    /* #6353: a day after the calendar's today has not happened, which is not a gap: no read, no "No Data", no reason for one. */
    if (iso > todayIso) return swap(detail, title, el("div", { class: "daycal-band band-Future", text: "Not yet" }), el("div", { class: "muted", text: "This day has not happened yet." }));
    swap(detail, title, el("div", { class: "muted", text: "Loading the day..." }));
    const mine = generation;
    const res = await read("get_daily_summary", { server, summary_date: iso });
    if (mine !== generation || selected !== iso || res.kind === "aborted") return;
    if (res.kind === "error") return swap(detail, title, errorStrip(res.message));
    if (res.kind === "empty") {
      /* No row, or a purged day: said in the page's words for the first, the service's for the second (it names the horizon). */
      const purged = res.status === "unavailable";
      return swap(detail, title, el("div", { class: "daycal-band band-NoData", text: "No Data" }), emptyStrip(purged ? res.message : "No collection this day."));
    }
    const d = res.data || {};
    const band = cellOf(d).band;
    const reasons = Array.isArray(d.reasons) ? d.reasons : [];
    const drills = Array.isArray(d.drills) ? d.drills : [];
    const message = el("div", { class: "muted daycal-drill-say" });
    swap(detail,
      title,
      el("div", { class: "daycal-band band-" + band, text: cellOf(d).word }),
      el("div", { class: "daycal-why", text: "Why this day is " + (band === "NoData" ? "No Data" : band) + ":" }),
      el("ul", { class: "daycal-reasons" }, reasons.map((r) => el("li", { text: r }))),
      d.data_note ? noticeStrip(d.data_note) : null,
      el("div", { class: "muted daycal-metrics", text: metricsLine(d) }),
      el("div", { class: "daycal-drills" }, drills.filter((t) => DRILLS[t]).map((t) =>
        el("button", { type: "button", class: "daycal-btn", onClick: async () => { message.textContent = (await openDay(server, iso, t)) || ""; } }, [DRILLS[t].label])
      )),
      message
    );
  }

  function drawGrid(entry) {
    const layout = monthLayout(view.year, view.month0, firstDow);
    const names = [];
    for (let i = 0; i < 7; i++) names.push(DAY_NAMES[(firstDow + i) % 7].slice(0, 3));
    const table = el("div", { class: "daycal-grid", role: "grid", "aria-label": MONTH_NAMES[view.month0] + " " + view.year + ", UTC days" }, [
      el("div", { class: "daycal-week daycal-names", role: "row" }, names.map((n) => el("div", { class: "daycal-name", role: "columnheader", text: n }))),
      ...layout.weeks.map((week) =>
        el("div", { class: "daycal-week", role: "row" }, week.map((day) => {
          if (day === null) return el("div", { class: "daycal-cell daycal-pad", role: "gridcell" });
          const iso = isoDay(view.year, view.month0, day);
          const row = entry.byDay.get(day);
          /* #6353: after the calendar's today a day has not happened (ISO days compare as text): not "no data", not a collection gap. */
          const future = iso > todayIso;
          const c = future ? { band: "Future", word: "not yet" } : cellOf(row);
          const tip = future ? iso + " (UTC): not yet. This day has not happened." : iso + " (UTC): " + c.word + (row && row.data_note ? ". " + row.data_note : c.band === "NoData" && !row ? ". No collection recorded for this day." : "");
          return el("button", {
            type: "button",
            role: "gridcell",
            class: "daycal-cell band-" + c.band + (iso === todayIso ? " today" : "") + (iso === selected ? " selected" : ""),
            title: tip,
            "aria-label": tip,
            "aria-pressed": iso === selected ? "true" : "false",
            "data-day": iso,
            "data-band": c.band,
            onClick: () => pick(day),
          }, [el("span", { class: "daycal-num", text: String(day) }), el("span", { class: "daycal-word", text: c.word })]);
        }))
      ),
    ]);
    swap(grid,
      entry.note ? (entry.neverCollected ? emptyStrip(entry.note) : noticeStrip(entry.note)) : null,
      table,
      el("div", { class: "muted daycal-legend", text: "Days are UTC days. A day with no data was not collected, which is not the same as healthy." })
    );
  }

  async function draw() {
    const mine = ++generation;
    drawHead();
    const key = keyOf(view);
    if (!loaded.has(key)) swap(grid, el("div", { class: "muted", text: "Loading " + MONTH_NAMES[view.month0] + " " + view.year + "..." }));
    const entry = await load(view);
    if (mine !== generation || entry.aborted) return;
    if (entry.failure) {
      swap(detail);
      return swap(grid, errorStrip(entry.failure));
    }
    if (asTable) {
      swap(detail);
      return swap(grid, renderTable({ rows: entry.rows, missing: entry.missing }));
    }
    drawGrid(entry);
    await drawDetail();
  }

  function go(month) {
    view = month;
    selected = null;
    void safely();
  }

  safely();
  return root;
}
