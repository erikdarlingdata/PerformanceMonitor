/* Runs the shipped month calendar (wwwroot/js/pages/daily-calendar.js, #6294) with the real util.js on a small fake DOM and a scripted read,
   and prints as one line of JSON what a person would see. WebDailyCalendarBehaviourTests starts it as
       node web-daily-calendar-harness.mjs <path to wwwroot/js> <scenario>
   Scenarios: layout (month arithmetic: first-weekday offset, month lengths, leap years), params (the read each month takes),
   calendar (cells, month navigation, a day's detail and its drills, the table toggle). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.dataset = {}; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  get firstChild() { return this.children[0] || null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  every(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.every(pred, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/SRV1/overview" };

const jsDir = process.argv[2];
const scenario = process.argv[3];
const settle = () => new Promise((r) => setTimeout(r, 15));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "daily-calendar-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const cal = await import(pathToFileURL(path.join(scratch, "pages", "daily-calendar.js")).href);
  const out = {};

  if (scenario === "layout") {
    const shape = (y, m, dow) => { const l = cal.monthLayout(y, m, dow); return { offset: l.offset, length: l.length, weeks: l.weeks.length, first: l.weeks[0].indexOf(1), last: l.weeks[l.weeks.length - 1].filter((d) => d !== null).pop() }; };
    out.feb2024Sun = shape(2024, 1, 0);   // a leap February that starts on a Thursday
    out.feb2024Mon = shape(2024, 1, 1);
    out.feb2026Sun = shape(2026, 1, 0);   // 28 days starting on a Sunday: exactly four weeks
    out.feb2026Mon = shape(2026, 1, 1);   // the same month with Monday first needs a fifth week
    out.oct2026Sun = shape(2026, 9, 0);
    out.mar2026Sat = shape(2026, 2, 6);
    out.lengths = [2000, 2100, 2024, 2026].map((y) => cal.daysInMonth(y, 1)).concat([cal.daysInMonth(2026, 3), cal.daysInMonth(2026, 11)]);
    out.everyWeekHasSeven = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].every((m) => [0, 1, 6].every((dow) => cal.monthLayout(2026, m, dow).weeks.every((w) => w.length === 7)));
    out.shifts = [cal.shiftMonth({ year: 2026, month0: 0 }, -1), cal.shiftMonth({ year: 2026, month0: 11 }, 1), cal.shiftMonth({ year: 2026, month0: 5 }, -18)];
    out.iso = cal.isoDay(2026, 2, 5);
  } else if (scenario === "params") {
    out.current = cal.monthReadParams("SRV1", { year: 2026, month0: 9 }, "2026-10-21");
    out.future = cal.monthReadParams("SRV1", { year: 2026, month0: 11 }, "2026-10-21");
    out.sept = cal.monthReadParams("SRV1", { year: 2026, month0: 8 }, "2026-10-21");
    out.leapFeb = cal.monthReadParams("SRV1", { year: 2024, month0: 1 }, "2026-10-21");
    out.dec = cal.monthReadParams("SRV1", { year: 2025, month0: 11 }, "2026-01-02");
    out.dayRangePast = cal.dayRange("2026-10-03", Date.parse("2026-10-21T12:00:00Z"));
    out.dayRangeToday = cal.dayRange("2026-10-21", Date.parse("2026-10-21T12:00:00Z"));
  } else if (scenario === "calendar") {
    const row = (date, band, extra = {}) => ({ summary_date: date, health_band: band, top_wait_type: "CXPACKET", total_wait_time_sec: 12, unique_queries: 5, high_cpu_events: 0, ...extra });
    const reads = [];
    const opened = [];
    const tables = [];
    const read = async (tool, params) => {
      reads.push({ tool, params });
      if (tool === "get_daily_summary_range") {
        // September: one Warning day; every other day has no row.
        return { kind: "data", data: { days: [row("2026-09-30", "Warning")], days_missing: [] } };
      }
      if (params.summary_date === "2026-10-03") {
        return { kind: "data", data: { ...row("2026-10-03", "Critical", { deadlock_count: 2 }), reasons: ["2 deadlocks", "1 alert"], drills: ["Deadlocks", "TopQueries"] } };
      }
      return { kind: "empty", status: "empty", message: "No data collected for SRV1 on " + params.summary_date + "." };
    };
    const initial = {
      to_date: "2026-10-21",
      days: [row("2026-09-30", "Healthy"), row("2026-10-03", "Critical"), row("2026-10-04", "Healthy"), row("2026-10-05", "NoData", { data_note: "Purged." })],
      days_missing: ["2026-10-04", "2026-09-29"],
    };
    const root = cal.dailyHealthCalendar({
      server: "SRV1", initial, read, nowMs: Date.parse("2026-10-21T08:00:00Z"), firstDow: 0,
      renderTable: (m) => { tables.push({ days: m.rows.map((r) => r.summary_date), missing: m.missing }); return new FakeNode("div"); },
      openDay: async (server, iso, target) => { opened.push([server, iso, target]); return target === "Deadlocks" ? null : "refused"; },
    });
    await settle();
    const buttons = () => root.every((n) => n.tag === "button");
    const named = (label) => buttons().find((b) => b.getAttribute("aria-label") === label || b.textContent === label);
    const click = (b) => { for (const fn of b.listeners.click || []) fn({}); };
    const cells = () => Object.fromEntries(root.every((n) => n.getAttribute("data-day")).map((c) => [c.getAttribute("data-day"), c.getAttribute("data-band") + "|" + c.children.map((x) => x.textContent).join("/")]));
    const text = () => root.textContent;

    const oct = cells();
    out.octDays = Object.keys(oct).length;
    out.oct = { d1: oct["2026-10-01"], d3: oct["2026-10-03"], d4: oct["2026-10-04"], d5: oct["2026-10-05"], d21: oct["2026-10-21"], d31: oct["2026-10-31"] };
    out.septDayInOctoberGrid = "2026-09-30" in oct; // the September row the current-month fetch carries is not drawn in October
    out.title = root.every((n) => n.className === "daycal-month")[0].textContent;
    out.nextDisabledNow = named("Next month").getAttribute("disabled") !== null;
    out.readsBeforeNavigation = reads.length;

    // A day: its detail comes from get_daily_summary and names the reasons and the drills.
    click(root.every((n) => n.getAttribute("data-day") === "2026-10-03")[0]);
    await settle();
    out.detail = { reasons: root.every((n) => n.tag === "li").map((n) => n.textContent), drills: root.every((n) => n.className === "daycal-btn" && /^View /.test(n.textContent)).map((n) => n.textContent), read: reads[reads.length - 1] };
    const viewDeadlocks = named("View Deadlocks");
    click(viewDeadlocks);
    await settle();
    click(named("View Top Queries"));
    await settle();
    out.opened = opened;
    out.refusal = root.every((n) => n.className === "muted daycal-drill-say").map((n) => n.textContent);

    // A day with no row: "no data", never a band.
    click(root.every((n) => n.getAttribute("data-day") === "2026-10-10")[0]);
    await settle();
    out.noDataDetail = { text: text().includes("No collection this day."), healthy: root.every((n) => n.className === "daycal-band band-Healthy").length };

    // Previous month: read with the month's own days_back and as_of, drawn from that read only.
    click(named("Previous month"));
    await settle();
    const sept = cells();
    out.sept = { days: Object.keys(sept).length, d30: sept["2026-09-30"], d29: sept["2026-09-29"], title: root.every((n) => n.className === "daycal-month")[0].textContent };
    out.septRead = reads[reads.length - 1];
    out.nextEnabledInSept = named("Next month").getAttribute("disabled") === null;
    out.thisMonthButton = named("This month") !== undefined;

    // Table toggle: the month's rows only, and its own missing days.
    click(named("Show as table"));
    await settle();
    out.tableSept = tables[tables.length - 1];
    click(named("Show as calendar"));
    await settle();
    click(named("This month"));
    await settle();
    click(named("Show as table"));
    await settle();
    out.tableOct = tables[tables.length - 1];
    out.totalReads = reads.length; // October came back from the first fetch, not a second read
  } else if (scenario === "future") {
    /* #6353: a day after the calendar's today. The calendar's today is the read's to_date (the service's UTC day), so a browser clock
       that is already on the next UTC day (nowMs) must not make the 22nd a "today" or a past day. */
    const reads = [];
    const read = async (tool, params) => { reads.push({ tool, params }); return { kind: "empty", status: "empty", message: "No data collected for SRV1 on " + params.summary_date + "." }; };
    const initial = { to_date: "2026-10-21", days: [], days_missing: [] };
    const root = cal.dailyHealthCalendar({ server: "SRV1", initial, read, nowMs: Date.parse("2026-10-22T00:30:00Z"), firstDow: 0, renderTable: () => new FakeNode("div"), openDay: async () => null });
    await settle();
    const dayCell = (iso) => root.every((n) => n.getAttribute("data-day") === iso)[0];
    const click = (b) => { for (const fn of b.listeners.click || []) fn({}); };
    const shape = (iso) => { const c = dayCell(iso); return { band: c.getAttribute("data-band"), word: c.children.map((x) => x.textContent).join("/"), tip: c.getAttribute("title") }; };
    out.today = shape("2026-10-21");
    out.tomorrow = shape("2026-10-22");
    out.lastDay = shape("2026-10-31");
    out.yesterday = shape("2026-10-20");
    const readsBefore = reads.length;
    click(dayCell("2026-10-25"));
    await settle();
    out.futureDetail = { text: root.textContent, bands: root.every((n) => /daycal-band/.test(n.className)).map((n) => n.className + "|" + n.textContent), readAsked: reads.length > readsBefore };
    click(dayCell("2026-10-21"));
    await settle();
    out.todayDetail = { text: root.textContent, readAsked: reads.length > readsBefore && reads[reads.length - 1].tool === "get_daily_summary" };
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
