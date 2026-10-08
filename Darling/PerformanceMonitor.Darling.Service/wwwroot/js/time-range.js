/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The time range grammar for the web (#5562): one line of text, a preset or a calendar period becomes a start and an end.
 * This is the twin of PerformanceMonitor.Ui/TimeRange (TimeRangeParser, TimeRangeSpec, ResolvedTimeRange, TimeRangePresets):
 * the same grammar, the same resolution rules and the same label text, pinned by the shared cases in
 * Darling.Tests/Fixtures/time-range-cases.json, which both implementations run.
 *
 * PURE: no DOM, no clock, no imports. Every function is given the now (epoch milliseconds) and the zone (an IANA name such as
 * "America/New_York"; the page passes the browser's zone, browserZone()). All wall-clock work goes through Intl with an
 * explicit timeZone, so the module never depends on the process zone and a test can drive any zone.
 *
 * Rules (settled, #5562): relative lengths (45m, 4h, 10d, 1mo = 30 days) are real elapsed time; calendar periods use the zone's
 * wall-clock midnights, weeks start Monday, and a boundary is the earliest instant labelled at or after the wall midnight
 * so neighbouring periods tile; typed wall times take the widest instants they can mean (a start the earliest, an end the
 * latest; a repeated hour widens, a skipped time maps to the change instant); nothing shorter than 5 minutes is accepted
 * and nothing is widened; there is no upper cap here (a page decides what its reads reach).
 *
 * A spec is a plain object: { kind: "relative", spanMs }, { kind: "calendar", period }, { kind: "fixed", startMs, endMs } or
 * { kind: "since", startMs }. resolveSpec turns it into a range for a now and a zone; parseRange turns text into one.
 */

const MINUTE_MS = 60000;
const HOUR_MS = 3600000;
const DAY_MS = 86400000;

/** The shortest range there is. A shorter one is refused, never widened. */
export const MINIMUM_SPAN_MS = 5 * MINUTE_MS;

/** A tab note appears when the span holds fewer than this many samples. */
export const MIN_SAMPLES = 3;

/** The data-start note appears when the range starts more than this long before the data does. */
export const DATA_START_SLACK_MS = 90 * MINUTE_MS;

const MAX_RELATIVE_MS = 36500 * DAY_MS;
const LAST_DAY_MS = Date.UTC(9999, 11, 31, 23, 59, 59, 999);
const EXAMPLES = "Try 45m, 3 days, last month, Oct 1 - Oct 2 or 1:00 am - 7:00 am.";

/* ───────────────────────────────── zone arithmetic (Intl, explicit zone) ───────────────────────────────── */

const formatters = new Map();

function formatterFor(zone) {
  let f = formatters.get(zone);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone: zone,
      hourCycle: "h23",
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
      second: "numeric",
    });
    formatters.set(zone, f);
  }
  return f;
}

/** The browser's zone as an IANA name ("UTC" when the browser will not say). */
export function browserZone() {
  try {
    return new Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

/** True when Intl knows the zone name. */
export function isKnownZone(zone) {
  try {
    formatterFor(zone);
    return true;
  } catch {
    return false;
  }
}

/** The wall clock of an instant in a zone, as "wall ms": the same fields read as if they were UTC. Exact in a repeated hour. */
export function toWall(ms, zone) {
  const base = Math.floor(ms / 1000) * 1000;
  const p = {};
  for (const part of formatterFor(zone).formatToParts(new Date(base))) p[part.type] = Number(part.value);
  return Date.UTC(p.year, p.month - 1, p.day, p.hour % 24, p.minute, p.second) + (ms - base);
}

/** The zone's UTC offset at an instant, in milliseconds (east of UTC is positive). */
export function offsetAt(ms, zone) {
  return toWall(ms, zone) - ms;
}

/** "+hh:mm" or "-hh:mm" ("+00:00" in UTC). */
export function formatOffset(offsetMs) {
  const sign = offsetMs < 0 ? "-" : "+";
  const totalMinutes = Math.round(Math.abs(offsetMs) / MINUTE_MS);
  return sign + pad2(Math.floor(totalMinutes / 60)) + ":" + pad2(totalMinutes % 60);
}

/** The instants whose wall clock reads `wallMs`: none (a clock change skipped it), one, or two (it happened twice), earliest first. */
function instantsOfWall(wallMs, zone) {
  const found = [];
  for (const o of new Set([offsetAt(wallMs - DAY_MS, zone), offsetAt(wallMs + DAY_MS, zone)])) {
    const u = wallMs - o;
    if (offsetAt(u, zone) === o) found.push(u);
  }
  return found.sort((a, b) => a - b);
}

/** The instant a clock change skips a wall time over: the first instant on the far side of the gap, found by halving the day either side. */
function skippedChangeInstant(wallMs, zone) {
  let lo = wallMs - DAY_MS;
  let hi = wallMs + DAY_MS;
  const before = offsetAt(lo, zone);
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (offsetAt(mid, zone) === before) lo = mid;
    else hi = mid;
  }
  return hi;
}

/**
 * The instant a typed wall-clock bound means, as the widest range it can name: side "from" is the earliest instant whose wall
 * clock reads at or after the value, "to" the latest at or before it. A time that happens once is that instant; in the repeated
 * hour from takes the first occurrence and to the second; a time that never happened gives the change instant for both.
 */
export function wallToUtcBound(wallMs, zone, side) {
  const found = instantsOfWall(wallMs, zone);
  if (found.length === 0) return skippedChangeInstant(wallMs, zone);
  if (found.length === 1) return found[0];
  return side === "from" ? found[0] : found[found.length - 1];
}

/** " +hh:mm"-style offset of an instant whose wall time happens twice, else null. */
function ambiguousOffsetSuffix(ms, zone) {
  return instantsOfWall(toWall(ms, zone), zone).length === 2 ? formatOffset(offsetAt(ms, zone)) : null;
}

/* ───────────────────────────────── small helpers ───────────────────────────────── */

function pad2(n) {
  return n < 10 ? "0" + n : String(n);
}

const MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function daysInMonth(year, month) {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

/** Round half to even, the way Math.Round does in the C# twin. */
function roundEven(x) {
  const f = Math.floor(x);
  const d = x - f;
  if (d < 0.5) return f;
  if (d > 0.5) return f + 1;
  return f % 2 === 0 ? f : f + 1;
}

function floorDay(wallMs) {
  return Math.floor(wallMs / DAY_MS) * DAY_MS;
}

/** "2026-10-01T04:00:00Z": whole seconds, the form a spec id carries. */
export function isoZ(ms) {
  return new Date(Math.floor(ms / 1000) * 1000).toISOString().replace(".000Z", "Z");
}

/** The length of a span as the picker words it: "3d" for three days and some hours, "7h 1m" under a day, "45m". Floors. */
export function formatLength(spanMs) {
  const totalMinutes = Math.floor(Math.max(0, spanMs) / MINUTE_MS);
  if (totalMinutes >= 1440) return Math.floor(totalMinutes / 1440) + "d";
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours === 0) return minutes + "m";
  return minutes === 0 ? hours + "h" : hours + "h " + minutes + "m";
}

function shortMessage(actualMs) {
  return actualMs <= 0
    ? "The shortest range is 5 minutes."
    : "That range is only " + formatLength(actualMs) + " long. The shortest range is 5 minutes.";
}

/* ───────────────────────────────── specs ───────────────────────────────── */

/** A length counted back from now. */
export function relativeSpec(spanMs) {
  return { kind: "relative", spanMs };
}

/** A calendar period in the zone: today, yesterday, week-to-date, previous-week, month-to-date, previous-month, year-to-date, previous-year. */
export function calendarSpec(period) {
  return { kind: "calendar", period };
}

/** A start and an end instant. */
export function fixedSpec(startMs, endMs) {
  return { kind: "fixed", startMs, endMs };
}

/** A fixed start with an end that is always now. */
export function sinceSpec(startMs) {
  return { kind: "since", startMs };
}

const PERIOD_IDS = ["today", "yesterday", "week-to-date", "previous-week", "month-to-date", "previous-month", "year-to-date", "previous-year"];

const PERIOD_NAMES = {
  today: "Today",
  yesterday: "Yesterday",
  "week-to-date": "Week to Date",
  "previous-week": "Previous Week",
  "month-to-date": "Month to Date",
  "previous-month": "Previous Month",
  "year-to-date": "Year to Date",
  "previous-year": "Previous Year",
};

/** The nine short presets, in the order the picker lists them. */
export const ROLLING_PRESETS = [
  relativeSpec(5 * MINUTE_MS),
  relativeSpec(15 * MINUTE_MS),
  relativeSpec(30 * MINUTE_MS),
  relativeSpec(HOUR_MS),
  relativeSpec(4 * HOUR_MS),
  relativeSpec(DAY_MS),
  relativeSpec(2 * DAY_MS),
  relativeSpec(7 * DAY_MS),
  relativeSpec(30 * DAY_MS),
];

/** The eight calendar periods, in the order the picker lists them. */
export const CALENDAR_PRESETS = PERIOD_IDS.map(calendarSpec);

/** A stable text for a spec: "5m", "4h", "1d", "1w", "1mo", "today", "previous-week", "fixed:<start>/<end>", "since:<start>". */
export function specId(spec) {
  switch (spec.kind) {
    case "relative": {
      const s = spec.spanMs;
      if (s === 7 * DAY_MS) return "1w";
      if (s === 30 * DAY_MS) return "1mo";
      if (s % DAY_MS === 0) return s / DAY_MS + "d";
      if (s % HOUR_MS === 0) return s / HOUR_MS + "h";
      return roundEven(s / MINUTE_MS) + "m";
    }
    case "calendar":
      return spec.period;
    case "fixed":
      return "fixed:" + isoZ(spec.startMs) + "/" + isoZ(spec.endMs);
    default:
      return "since:" + isoZ(spec.startMs);
  }
}

const ISO_Z_RX = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$/;

function parseIsoZ(text) {
  const m = ISO_Z_RX.exec(String(text).toUpperCase());
  return m ? Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]) : null;
}

/** Reads an id back: a preset id, a count and unit ("90m", "3d", "2w", "2mo"), "fixed:..." or "since:...". Null for anything else. */
export function specFromId(id) {
  if (typeof id !== "string" || id.trim() === "") return null;
  const text = id.trim().toLowerCase();
  if (PERIOD_IDS.includes(text)) return calendarSpec(text);
  if (text.startsWith("since:")) {
    const at = parseIsoZ(text.slice(6));
    return at == null ? null : sinceSpec(at);
  }
  if (text.startsWith("fixed:")) {
    const parts = text.slice(6).split("/");
    const a = parts.length === 2 ? parseIsoZ(parts[0]) : null;
    const b = parts.length === 2 ? parseIsoZ(parts[1]) : null;
    return a == null || b == null ? null : fixedSpec(a, b);
  }
  const m = /^(\d+)([a-z]+)$/.exec(text);
  if (!m) return null;
  const count = Number(m[1]);
  if (!(count > 0) || count > 1000000) return null;
  switch (m[2]) {
    case "m": return relativeSpec(count * MINUTE_MS);
    case "h": return relativeSpec(count * HOUR_MS);
    case "d": return relativeSpec(count * DAY_MS);
    case "w": return relativeSpec(7 * count * DAY_MS);
    case "mo": return relativeSpec(30 * count * DAY_MS);
    default: return null;
  }
}

/** The spec for a count of whole hours (the legacy `hours` the reads and old settings carry), or null. */
export function fromLegacyHours(hours) {
  return Number.isFinite(hours) && hours > 0 ? relativeSpec(hours * HOUR_MS) : null;
}

/** The whole hours of a relative spec, or null when it is not a whole number of hours (or not relative). */
export function wholeHours(spec) {
  return spec.kind === "relative" && spec.spanMs % HOUR_MS === 0 ? spec.spanMs / HOUR_MS : null;
}

function pastLength(spanMs) {
  if (spanMs === 7 * DAY_MS) return "week";
  if (spanMs === 30 * DAY_MS) return "30 days";
  let count;
  let unit;
  if (spanMs % DAY_MS === 0) {
    count = spanMs / DAY_MS;
    unit = "day";
  } else if (spanMs % HOUR_MS === 0) {
    count = spanMs / HOUR_MS;
    unit = "hour";
  } else {
    count = roundEven(spanMs / MINUTE_MS);
    unit = "minute";
  }
  return count === 1 ? unit : count + " " + unit + "s";
}

/** The name the picker lists: "Past 4 hours", "Previous Week"; a fixed or since range reads as a generic name. */
export function specName(spec) {
  switch (spec.kind) {
    case "relative": return "Past " + pastLength(spec.spanMs);
    case "calendar": return PERIOD_NAMES[spec.period];
    case "fixed": return "Custom range";
    default: return "Since a start";
  }
}

/* ───────────────────────────────── resolution ───────────────────────────────── */

function resolvePeriod(period, nowMs, zone) {
  const wallNow = new Date(toWall(nowMs, zone));
  const today = Date.UTC(wallNow.getUTCFullYear(), wallNow.getUTCMonth(), wallNow.getUTCDate());
  const monday = today - ((new Date(today).getUTCDay() + 6) % 7) * DAY_MS;
  const y = wallNow.getUTCFullYear();
  const firstOfMonth = Date.UTC(y, wallNow.getUTCMonth(), 1);
  const firstOfYear = Date.UTC(y, 0, 1);
  const b = (wall) => wallToUtcBound(wall, zone, "from");
  switch (period) {
    case "today": return { start: b(today), end: nowMs, live: true };
    case "yesterday": return { start: b(today - DAY_MS), end: b(today), live: false };
    case "week-to-date": return { start: b(monday), end: nowMs, live: true };
    case "previous-week": return { start: b(monday - 7 * DAY_MS), end: b(monday), live: false };
    case "month-to-date": return { start: b(firstOfMonth), end: nowMs, live: true };
    case "previous-month": return { start: b(Date.UTC(y, wallNow.getUTCMonth() - 1, 1)), end: b(firstOfMonth), live: false };
    case "year-to-date": return { start: b(firstOfYear), end: nowMs, live: true };
    default: return { start: b(Date.UTC(y - 1, 0, 1)), end: b(firstOfYear), live: false };
  }
}

function formatBound(ms, ctx, includeSeconds) {
  const wall = new Date(toWall(ms, ctx.zone));
  const hour12 = wall.getUTCHours() % 12 === 0 ? 12 : wall.getUTCHours() % 12;
  let clock = hour12 + ":" + pad2(wall.getUTCMinutes());
  if (includeSeconds && wall.getUTCSeconds() !== 0) clock += ":" + pad2(wall.getUTCSeconds());
  const day = MONTH_ABBR[wall.getUTCMonth()] + " " + wall.getUTCDate() + (ctx.showYear ? ", " + wall.getUTCFullYear() : "");
  const text = day + ", " + clock + (wall.getUTCHours() < 12 ? " am" : " pm");
  const suffix = ambiguousOffsetSuffix(ms, ctx.zone);
  return suffix == null ? text : text + " " + suffix;
}

function wallYear(ms, zone) {
  return new Date(toWall(ms, zone)).getUTCFullYear();
}

function buildRange(spec, startMs, endMs, live, zone, nowMs) {
  const nowYear = wallYear(nowMs, zone);
  const ctx = { zone, showYear: wallYear(startMs, zone) !== nowYear || wallYear(endMs, zone) !== nowYear };
  const seconds = spec.kind === "fixed";
  const a = "UTC" + formatOffset(offsetAt(startMs, zone));
  const b = "UTC" + formatOffset(offsetAt(endMs, zone));
  const zoneText = a === b ? a : a + " to " + b;
  const length = formatLength(endMs - startMs);
  const startText = formatBound(startMs, ctx, seconds);
  const endText = formatBound(endMs, ctx, seconds);
  return {
    spec,
    id: specId(spec),
    startMs,
    endMs,
    live,
    zone,
    nowMs,
    spanMs: endMs - startMs,
    length,
    startText,
    endText,
    zoneText,
    label: length + "  " + startText + " - " + endText + " (" + zoneText + ")",
  };
}

/**
 * The instants a spec means at `nowMs` in `zone`: `{ ok: true, range }` or `{ ok: false, error: { code, message } }` (shorter than
 * 5 minutes, ends before it starts, reaches back too far). The range carries startMs, endMs, live (the end slides with now),
 * spanMs, length, startText, endText, zoneText and label ("3d  Oct 5, 12:00 am - Oct 8, 7:01 am (UTC-04:00)").
 */
export function resolveSpec(spec, nowMs, zone) {
  const fail = (code, message) => ({ ok: false, error: { code, message } });
  let start;
  let end;
  let live;
  switch (spec.kind) {
    case "relative":
      if (!(spec.spanMs > 0)) return fail("too_short", shortMessage(0));
      if (spec.spanMs > MAX_RELATIVE_MS) return fail("too_far_back", "That reaches back more than 100 years.");
      start = nowMs - spec.spanMs;
      end = nowMs;
      live = true;
      break;
    case "calendar":
      ({ start, end, live } = resolvePeriod(spec.period, nowMs, zone));
      break;
    case "since":
      start = spec.startMs;
      end = nowMs;
      live = true;
      break;
    default:
      start = spec.startMs;
      end = spec.endMs;
      live = false;
      break;
  }
  if (end < start) {
    return spec.kind === "since"
      ? fail("start_in_future", "That start has not happened yet.")
      : fail("end_before_start", "The end is before the start.");
  }
  if (end - start < MINIMUM_SPAN_MS) return fail("too_short", shortMessage(end - start));
  return { ok: true, range: buildRange(spec, start, end, live, zone, nowMs) };
}

/** The current length of a spec ("7h 1m" for Today), or null when it cannot be used right now (a calendar period under 5 minutes). */
export function currentLength(spec, nowMs, zone) {
  const r = resolveSpec(spec, nowMs, zone);
  return r.ok ? r.range.length : null;
}

/* ───────────────────────────────── the typed-range parser ───────────────────────────────── */

class ParseFailure extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

const RELATIVE_RX = /^(?:(?:last|past|previous)\s+)?(\d+(?:\.\d+)?)\s*([a-z]+)$/;
const SEPARATOR_RX = /\s+(?:-|to|until)\s+/;
const UNIX_RX = /^(?:\d{10}|\d{13})$/;
const ISO_T_RX = /(\d{4}-\d{1,2}-\d{1,2})t(\d)/g;
const TIME12_RX = /(?:^|\s)(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*(am|pm)$/;
const TIME24_RX = /(?:^|\s)(\d{1,2}):(\d{2})(?::(\d{2}))?$/;
const TIME_WORD_RX = /(?:^|\s)(noon|midnight)$/;
const MONTH_DAY_RX = /^([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s+(\d{4}))?$/;
const DAY_MONTH_RX = /^(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)(?:\s+(\d{4}))?$/;
const SLASH_RX = /^(\d{1,2})\/(\d{1,2})(?:\/(\d{4}|\d{2}))?$/;
const ISO_RX = /^(\d{4})-(\d{1,2})-(\d{1,2})$/;

const PERIODS = new Map([
  ["today", "today"],
  ["yesterday", "yesterday"],
  ["this week", "week-to-date"],
  ["week to date", "week-to-date"],
  ["wtd", "week-to-date"],
  ["last week", "previous-week"],
  ["previous week", "previous-week"],
  ["this month", "month-to-date"],
  ["month to date", "month-to-date"],
  ["mtd", "month-to-date"],
  ["last month", "previous-month"],
  ["previous month", "previous-month"],
  ["this year", "year-to-date"],
  ["year to date", "year-to-date"],
  ["ytd", "year-to-date"],
  ["last year", "previous-year"],
  ["previous year", "previous-year"],
]);

const MONTHS = new Map([
  ["jan", 1], ["january", 1], ["feb", 2], ["february", 2], ["mar", 3], ["march", 3],
  ["apr", 4], ["april", 4], ["may", 5], ["jun", 6], ["june", 6], ["jul", 7], ["july", 7],
  ["aug", 8], ["august", 8], ["sep", 9], ["sept", 9], ["september", 9],
  ["oct", 10], ["october", 10], ["nov", 11], ["november", 11], ["dec", 12], ["december", 12],
]);

const UNITS = new Map([
  ["m", 60], ["min", 60], ["mins", 60], ["minute", 60], ["minutes", 60],
  ["h", 3600], ["hr", 3600], ["hrs", 3600], ["hour", 3600], ["hours", 3600],
  ["d", 86400], ["day", 86400], ["days", 86400],
  ["w", 604800], ["wk", 604800], ["wks", 604800], ["week", 604800], ["weeks", 604800],
  ["mo", 2592000], ["month", 2592000], ["months", 2592000],
]);

function normalize(text) {
  if (text == null || String(text).trim() === "") return "";
  return String(text).trim().toLowerCase().replace(/[–—−]/g, "-").replace(/,/g, " ").replace(/\s+/g, " ").trim();
}

/** One side of a typed range, before it has a year, a day or a zone. Times are milliseconds into the day, or null. */
function newPoint() {
  return { isNow: false, instantMs: null, hasDate: false, year: null, month: 0, day: 0, timeMs: null };
}

function withYear(p, year) {
  if (year < 1 || year > 9999 || p.day > daysInMonth(year, p.month)) throw new ParseFailure("bad_date", "That date does not exist.");
  return Date.UTC(year, p.month - 1, p.day);
}

/** The calendar day of a point that has a date. Without a year it is the latest such day-and-time not after now. */
function dateFor(p, wallNow, timeMs) {
  if (p.year != null) return withYear(p, p.year);
  const nowYear = new Date(wallNow).getUTCFullYear();
  for (let year = nowYear; year >= nowYear - 8; year--) {
    if (p.day <= daysInMonth(year, p.month)) {
      const candidate = Date.UTC(year, p.month - 1, p.day);
      if (candidate + (timeMs ?? 0) <= wallNow) return candidate;
    }
  }
  throw new ParseFailure("bad_date", "That date does not exist.");
}

/** An end typed without a year that falls before its start means the next year, but only when that end has already happened. */
function rollEndIntoNextYear(end, startDate, endDate, wallNow) {
  const startYear = new Date(startDate).getUTCFullYear();
  if (endDate >= startDate || startYear >= 9999) return endDate;
  try {
    const next = withYear(end, startYear + 1);
    return next <= wallNow ? next : endDate;
  } catch (e) {
    if (e instanceof ParseFailure) return endDate;
    throw e;
  }
}

function splitSides(s) {
  let parts = s.split(SEPARATOR_RX);
  if (parts.length === 1) {
    const dashes = s.split("-");
    if (dashes.length === 2) parts = dashes;
  }
  if (parts.length > 2) throw new ParseFailure("bad_range", "Use one start and one end. " + EXAMPLES);
  return parts.map((part) => {
    const side = part.trim();
    if (side.length === 0) throw new ParseFailure("unrecognized", "A range needs something on both sides of the dash. " + EXAMPLES);
    return side;
  });
}

function parseDate(d, point, wallNow, original) {
  point.hasDate = true;
  if (d === "today" || d === "yesterday") {
    const day = new Date(floorDay(wallNow) - (d === "today" ? 0 : DAY_MS));
    point.year = day.getUTCFullYear();
    point.month = day.getUTCMonth() + 1;
    point.day = day.getUTCDate();
    return;
  }

  let month;
  let dayOfMonth;
  let yearText = "";
  const md = MONTH_DAY_RX.exec(d);
  const dm = DAY_MONTH_RX.exec(d);
  const slash = SLASH_RX.exec(d);
  const iso = ISO_RX.exec(d);
  if (md && MONTHS.has(md[1])) {
    month = MONTHS.get(md[1]);
    dayOfMonth = Number(md[2]);
    yearText = md[3] ?? "";
  } else if (dm && MONTHS.has(dm[2])) {
    month = MONTHS.get(dm[2]);
    dayOfMonth = Number(dm[1]);
    yearText = dm[3] ?? "";
  } else if (slash) {
    month = Number(slash[1]);
    dayOfMonth = Number(slash[2]);
    yearText = slash[3] ?? "";
    if (yearText.length === 2) yearText = "20" + yearText;
  } else if (iso) {
    yearText = iso[1];
    month = Number(iso[2]);
    dayOfMonth = Number(iso[3]);
  } else {
    throw new ParseFailure("unrecognized", "\"" + original.trim() + "\" is not a date or a time. " + EXAMPLES);
  }

  if (month < 1 || month > 12 || dayOfMonth < 1 || dayOfMonth > 31) {
    throw new ParseFailure("bad_date", "\"" + original.trim() + "\" is not a date that exists.");
  }
  if (yearText.length > 0) {
    const year = Number(yearText);
    if (year < 1900 || year > 9999 || dayOfMonth > daysInMonth(year, month)) {
      throw new ParseFailure("bad_date", "\"" + original.trim() + "\" is not a date that exists.");
    }
    point.year = year;
  } else if (dayOfMonth > daysInMonth(2024, month)) {
    throw new ParseFailure("bad_date", "\"" + original.trim() + "\" is not a date that exists.");
  }
  point.month = month;
  point.day = dayOfMonth;
}

function parsePoint(text, wallNow) {
  let p = text.trim();
  if (p === "now") {
    const now = newPoint();
    now.isNow = true;
    return now;
  }
  if (UNIX_RX.test(p)) {
    const number = Number(p);
    const unix = newPoint();
    unix.instantMs = p.length === 13 ? number : number * 1000;
    return unix;
  }

  p = p.replace(ISO_T_RX, "$1 $2");
  if (p.startsWith("at ")) p = p.slice(3);
  else if (p.startsWith("on ")) p = p.slice(3);
  p = p.replaceAll(" at ", " ");

  const point = newPoint();
  let datePart = p;
  const m12 = TIME12_RX.exec(p);
  const m24 = TIME24_RX.exec(p);
  const mWord = TIME_WORD_RX.exec(p);
  if (m12) {
    const hour = Number(m12[1]);
    const minute = m12[2] != null ? Number(m12[2]) : 0;
    const second = m12[3] != null ? Number(m12[3]) : 0;
    if (hour < 1 || hour > 12 || minute > 59 || second > 59) {
      throw new ParseFailure("bad_time", "\"" + m12[0].trim() + "\" is not a time on a 12-hour clock.");
    }
    const pm = m12[4] === "pm";
    point.timeMs = (((hour % 12) + (pm ? 12 : 0)) * 3600 + minute * 60 + second) * 1000;
    datePart = p.slice(0, m12.index);
  } else if (m24) {
    const hour = Number(m24[1]);
    const minute = Number(m24[2]);
    const second = m24[3] != null ? Number(m24[3]) : 0;
    if (hour > 23 || minute > 59 || second > 59) {
      throw new ParseFailure("bad_time", "\"" + m24[0].trim() + "\" is not a time on a 24-hour clock.");
    }
    point.timeMs = (hour * 3600 + minute * 60 + second) * 1000;
    datePart = p.slice(0, m24.index);
  } else if (mWord) {
    point.timeMs = mWord[1] === "noon" ? 12 * HOUR_MS : 0;
    datePart = p.slice(0, mWord.index);
  }

  datePart = datePart.trim();
  if (datePart.length === 0) {
    if (point.timeMs == null) throw new ParseFailure("unrecognized", "\"" + text.trim() + "\" is not a date or a time. " + EXAMPLES);
    return point;
  }
  parseDate(datePart, point, wallNow, text);
  return point;
}

function checkCalendarEnd(wallMs) {
  if (wallMs > LAST_DAY_MS) throw new ParseFailure("bad_date", "That date is out of range.");
}

function fixedFromWall(startWall, endWall, zone) {
  checkCalendarEnd(endWall);
  return fixedSpec(wallToUtcBound(startWall, zone, "from"), wallToUtcBound(endWall, zone, "to"));
}

function sinceFromPoint(start, wallNow, zone) {
  if (start.isNow) throw new ParseFailure("bad_range", "The start cannot be now.");
  if (start.instantMs != null) return sinceSpec(start.instantMs);
  let wall;
  if (start.hasDate) {
    wall = dateFor(start, wallNow, start.timeMs) + (start.timeMs ?? 0);
  } else {
    /* A time alone is the latest such time that is not in the future. */
    wall = floorDay(wallNow) + start.timeMs;
    if (wall > wallNow) wall -= DAY_MS;
  }
  return sinceSpec(wallToUtcBound(wall, zone, "from"));
}

function rangeFromPoints(a, b, wallNow, zone) {
  let aDate;
  let bDate;
  if (a.hasDate && b.hasDate) {
    if (a.year == null && b.year == null) {
      aDate = dateFor(a, wallNow, a.timeMs);
      bDate = withYear(b, new Date(aDate).getUTCFullYear());
      bDate = rollEndIntoNextYear(b, aDate, bDate, wallNow);
    } else if (a.year == null) {
      bDate = withYear(b, b.year);
      aDate = withYear(a, new Date(bDate).getUTCFullYear());
      if (aDate > bDate) aDate = withYear(a, new Date(bDate).getUTCFullYear() - 1);
    } else {
      aDate = withYear(a, a.year);
      bDate = withYear(b, b.year ?? new Date(aDate).getUTCFullYear());
      if (b.year == null) bDate = rollEndIntoNextYear(b, aDate, bDate, wallNow);
    }
    if (bDate < aDate) throw new ParseFailure("end_before_start", "The end is before the start.");
  } else if (a.hasDate) {
    aDate = dateFor(a, wallNow, a.timeMs);
    bDate = aDate;
    if (b.timeMs < (a.timeMs ?? 0)) bDate = aDate + DAY_MS;
  } else if (b.hasDate) {
    bDate = dateFor(b, wallNow, b.timeMs);
    aDate = bDate;
    if (b.timeMs != null && a.timeMs > b.timeMs) aDate = bDate - DAY_MS;
  } else {
    /* Two times: the latest day whose range has ended by now; an end earlier on the clock than the start is the next day. */
    const rollsOver = b.timeMs < a.timeMs ? 1 : 0;
    aDate = floorDay(wallNow);
    for (let back = 0; back < 3; back++) {
      aDate = floorDay(wallNow) - back * DAY_MS;
      if (aDate + rollsOver * DAY_MS + b.timeMs <= wallNow) break;
    }
    bDate = aDate + rollsOver * DAY_MS;
  }
  const startWall = aDate + (a.timeMs ?? 0);
  const endWall = b.timeMs == null ? bDate + DAY_MS : bDate + b.timeMs;
  return fixedFromWall(startWall, endWall, zone);
}

function parseSpec(text, nowMs, zone) {
  let s = normalize(text);
  if (s.length === 0) throw new ParseFailure("empty", "Type a range. " + EXAMPLES);

  if (PERIODS.has(s)) return calendarSpec(PERIODS.get(s));

  const relative = RELATIVE_RX.exec(s);
  if (relative && UNITS.has(relative[2])) {
    const seconds = Number(relative[1]) * UNITS.get(relative[2]);
    if (seconds > 36500 * 86400) throw new ParseFailure("too_far_back", "That reaches back more than 100 years.");
    return relativeSpec(roundEven(seconds) * 1000);
  }

  const wallNow = toWall(nowMs, zone);
  if (s.startsWith("since ")) return sinceFromPoint(parsePoint(s.slice(6), wallNow), wallNow, zone);
  if (s.startsWith("from ")) s = s.slice(5);

  const sides = splitSides(s);
  if (sides.length === 1) {
    const only = parsePoint(sides[0], wallNow);
    if (only.isNow || only.instantMs != null || !only.hasDate || only.timeMs != null) {
      throw new ParseFailure("needs_end", "Give an end as well, like \"Oct 1 12pm - 3pm\", or say \"since Oct 1 12pm\".");
    }
    /* One date alone is the whole day. */
    const day = dateFor(only, wallNow, null);
    return fixedFromWall(day, day + DAY_MS, zone);
  }

  const a = parsePoint(sides[0], wallNow);
  const b = parsePoint(sides[1], wallNow);
  if (a.isNow) throw new ParseFailure("bad_range", "The start cannot be now. Use \"since\" for a range that grows.");
  if (b.isNow) return sinceFromPoint(a, wallNow, zone);
  if (a.instantMs != null || b.instantMs != null) {
    if (a.instantMs == null || b.instantMs == null) {
      throw new ParseFailure("bad_range", "Use two Unix times, or two dates and times, not one of each.");
    }
    return fixedSpec(a.instantMs, b.instantMs);
  }
  return rangeFromPoints(a, b, wallNow, zone);
}

/**
 * Reads a line of text as a range at `nowMs` in `zone`. Returns `{ ok, range, spec, echo, error, errorCode }`: `echo` is the
 * range's one-line label (empty on an error), `error` the plain reason and `errorCode` the stable code (empty, unrecognized,
 * bad_date, bad_time, bad_range, needs_end, too_short, end_before_start, start_in_future, too_far_back).
 */
export function parseRange(text, nowMs, zone) {
  const refused = (code, message) => ({ ok: false, range: null, spec: null, echo: "", error: message, errorCode: code });
  try {
    const spec = parseSpec(text, nowMs, zone);
    const r = resolveSpec(spec, nowMs, zone);
    return r.ok
      ? { ok: true, range: r.range, spec, echo: r.range.label, error: null, errorCode: null }
      : refused(r.error.code, r.error.message);
  } catch (e) {
    if (e instanceof ParseFailure) return refused(e.code, e.message);
    if (e instanceof RangeError) return refused("bad_date", "That date is out of range.");
    throw e;
  }
}

/* ───────────────────────────────── notes, reach, and the reads' window ───────────────────────────────── */

/** "3 minutes" / "1 hour" / "30 seconds": a collector interval in words. */
export function intervalText(intervalMs) {
  let count;
  let unit;
  if (intervalMs % HOUR_MS === 0) {
    count = intervalMs / HOUR_MS;
    unit = "hour";
  } else if (intervalMs % MINUTE_MS === 0) {
    count = intervalMs / MINUTE_MS;
    unit = "minute";
  } else {
    count = Math.max(1, Math.round(intervalMs / 1000));
    unit = "second";
  }
  return count === 1 ? unit : count + " " + unit + "s";
}

/** "Data here is collected every 5 minutes." when the span holds fewer than 3 samples at the interval, else null. */
export function sampleIntervalNote(spanMs, intervalMs) {
  if (!(intervalMs > 0) || spanMs >= intervalMs * MIN_SAMPLES) return null;
  return "Data here is collected every " + intervalText(intervalMs) + ".";
}

/** "Data starts Oct 3, 2:00 pm" when the range starts more than 90 minutes before the data does, else null. */
export function dataStartNote(range, dataStartMs) {
  if (dataStartMs == null || range.startMs >= dataStartMs - DATA_START_SLACK_MS) return null;
  const nowYear = wallYear(range.nowMs, range.zone);
  const showYear = wallYear(range.startMs, range.zone) !== nowYear || wallYear(range.endMs, range.zone) !== nowYear;
  return "Data starts " + formatBound(dataStartMs, { zone: range.zone, showYear }, false);
}

/** An instant as the text a date-time box and the typed grammar both read, in the zone: "2026-10-01 13:00" (seconds only when not zero). */
export function wallText(ms, zone) {
  const w = new Date(toWall(ms, zone));
  const date = w.getUTCFullYear() + "-" + pad2(w.getUTCMonth() + 1) + "-" + pad2(w.getUTCDate());
  const clock = pad2(w.getUTCHours()) + ":" + pad2(w.getUTCMinutes()) + (w.getUTCSeconds() ? ":" + pad2(w.getUTCSeconds()) : "");
  return date + " " + clock;
}

/**
 * The reach of one read, in hours: the `max_hours` its /api/catalog entry carries (on the entry, or on its `hours` param), else the
 * default (168). The catalog entry is the element of `reads` the page's active read names; with none, the default applies.
 */
export function reachHours(catalogEntry, fallbackHours = 168) {
  const param = catalogEntry && Array.isArray(catalogEntry.params) ? catalogEntry.params.find((p) => p && p.name === "hours") : null;
  for (const holder of [catalogEntry, param]) {
    const max = holder ? Number(holder.max_hours) : NaN;
    if (Number.isFinite(max) && max > 0) return max;
  }
  return fallbackHours;
}

/** The main collector's interval of one read in milliseconds, from the `collector_interval_minutes` its /api/catalog entry carries (on the entry or on its `hours` param), else null (no note). */
export function collectorIntervalMs(catalogEntry) {
  const param = catalogEntry && Array.isArray(catalogEntry.params) ? catalogEntry.params.find((p) => p && p.name === "hours") : null;
  for (const holder of [catalogEntry, param]) {
    const minutes = holder ? Number(holder.collector_interval_minutes) : NaN;
    if (Number.isFinite(minutes) && minutes > 0) return minutes * MINUTE_MS;
  }
  return null;
}

/** "7 days" / "90 days" / "36 hours": a reach in words. */
export function reachText(hours) {
  return hours % 24 === 0 ? hours / 24 + " day" + (hours === 24 ? "" : "s") : hours + " hour" + (hours === 1 ? "" : "s");
}

/** The reason a range is longer than the reach (hours), or null when it fits. */
export function reachRefusal(spanMs, hours) {
  return spanMs > hours * HOUR_MS ? "This page's reads reach at most " + reachText(hours) + " back." : null;
}

/** The shortest range a rolling-only page offers: one hour, because its reads take whole hours back from now (#5562 R5). */
export const ROLLING_ONLY_MIN_SPAN_MS = HOUR_MS;

/**
 * The reason a range is not one a rolling-only page can read, or null (#5562 R5). A FinOps read is "this many hours back from
 * now", so it cannot honor a calendar period, a finished range, a since-range or a length that is not a whole number of hours;
 * the page refuses them rather than showing a range its read would answer differently.
 */
export function rollingOnlyRefusal(spec, spanMs, minSpanMs = ROLLING_ONLY_MIN_SPAN_MS, stepMs = HOUR_MS) {
  if (!spec || spec.kind !== "relative") {
    return "This page reads a length back from now, so pick a rolling length such as 24h or 7d.";
  }
  if (spanMs < minSpanMs) return "This page reads at least " + formatLength(minSpanMs) + " back from now.";
  if (spanMs % stepMs !== 0) {
    return stepMs % DAY_MS === 0 ? "This page reads whole days back from now, such as 7d or 30d." : "This page reads whole hours back from now, such as 6h or 3d.";
  }
  return null;
}

/** The reason a range is shorter than a page's own floor, or null. */
export function minSpanRefusal(spanMs, minSpanMs) {
  return minSpanMs > 0 && spanMs < minSpanMs ? "This page reads at least " + formatLength(minSpanMs) + "." : null;
}

/**
 * The window a read takes for a resolved range: reads take whole `hours` and an `as_of` end. `hours` rounds the span UP to a whole
 * number of hours (at least 1), so the fetch starts at or before the range start and the page trims to the exact pair; `asOf` is
 * the end as ISO text, or null for a live range, whose reads are anchored at the server's own clock.
 */
export function readWindow(range, nowMs = Date.now()) {
  /* A fixed range whose end is at or after now is a live range ending now (#5562 review r1 M2): it is sent with no as_of, which a
     read would refuse as "in the future", and it keeps refreshing until the clock passes the end. */
  const endsNow = range.live || range.endMs >= nowMs;
  const endMs = endsNow ? Math.max(range.startMs, Math.min(range.endMs, nowMs)) : range.endMs;
  const spanMs = endsNow && !range.live ? Math.max(0, endMs - range.startMs) : range.spanMs;
  return {
    hours: Math.max(1, Math.ceil(spanMs / HOUR_MS)),
    asOf: endsNow ? null : new Date(range.endMs).toISOString(),
    live: endsNow,
    startMs: range.startMs,
    endMs: endsNow && !range.live ? endMs : range.endMs,
  };
}
