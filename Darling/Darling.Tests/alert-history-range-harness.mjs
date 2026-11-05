/* Runs the shipped Alert History page (wwwroot/js/pages/alerts.js) with the real shared renderer (panels.js, util.js,
   mute-context.js) on a small fake DOM and a recording fetch, and prints as one line of JSON what the page asked for
   and drew. AlertHistoryRangeBehaviourTests starts it as
       node alert-history-range-harness.mjs <path to wwwroot/js> <scenario> [IANA time zone]
   Only the DOM, fetch and charts.js are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { pickRange, pickerText, pickerItems, popupShape, withDocumentListeners } from "./web-picker-driver.mjs";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {}; this.value = ""; this.checked = false; this.open = false; this.isRoot = false;
  }
  get firstChild() { return this.children[0] || null; }
  get nextSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i >= 0 ? this.parent.children[i + 1] || null : null; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children.forEach((c) => (c.parent = null)); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c); else this.children.splice(i, 0, c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  remove() { if (this.parent) this.parent.removeChild(this); }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  querySelector(sel) { const [a, b] = sel.split(" "); const first = this.all(a)[0] || null; return b && first ? first.all(b)[0] || null : first; }
  querySelectorAll(tag) { return this.all(tag); }
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = withDocumentListeners({ createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) });
globalThis.location = { hash: "#/alerts" };

const jsDir = process.argv[2];
const scenario = process.argv[3];
// Node on Windows ignores a TZ variable set before it starts, but honours one set on process.env from inside, so the zone is an argument
// (#5570 r2 H1): the page's time parsing must give the same answer in every browser zone, and a harness stuck in UTC cannot show that.
if (process.argv[4]) process.env.TZ = process.argv[4];

const urls = [];
let alertsReply = { alerts: [], truncated: false };
const servers = { servers: [{ server_name: "srv-a", display_name: "Server A" }, { server_name: "srv-b" }] };
globalThis.fetch = async (url) => {
  const u = String(url);
  urls.push(u);
  let body = {};
  if (u.startsWith("/api/session")) body = { can_edit: true };
  else if (u.includes("/api/catalog")) body = { reads: [{ name: "get_alert_history", params: [{ name: "hours", collector: null, collector_interval_minutes: null, max_hours: 2160 }] }] };
  else if (u.includes("/list_servers")) body = servers;
  else if (u.includes("/get_alert_history")) body = typeof alertsReply === "function" ? alertsReply(u) : alertsReply;
  else if (u.includes("/list_investigation_requests")) body = investigationsReply;
  else if (u.includes("/get_investigation_reports")) body = u.includes("request_id=") ? journalReply : reportsReply;
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
/* #5632 Phase A: the Investigations panel's two reads, in the shapes plan v1 section 6 names. */
const REQ = "11111111-1111-1111-1111-111111111111";
let investigationsReply = { status: "ok", requests: [
  { request_id: REQ, opened_at: "2026-10-14T12:00:00.0000000", server_name: "srv-a", family: "blocking", state: "reported", alert_count: 3 },
  { request_id: "22222222-2222-2222-2222-222222222222", opened_at: "2026-10-14T11:00:00.0000000", server_name: "srv-b", family: "resources", state: "open", alert_count: 1 },
] };
let reportsReply = { status: "ok", reports: [
  { request_id: REQ, submitted_at: "2026-10-14T12:05:00", verdict: "false_positive", summary: "older report" },
  { request_id: REQ, submitted_at: "2026-10-14T12:10:00", verdict: "real", summary: "a long blocking chain from the nightly ETL" },
] };
let journalReply = { status: "ok", journal: [
  { request_id: REQ, event_time: "2026-10-14T12:00:00", event: "opened", actor: "service" },
  { request_id: REQ, event_time: "2026-10-14T12:10:00", event: "reported", actor: "agent:agent-a" },
] };
const readsOf = () => urls.filter((u) => u.includes("/get_alert_history")).map((u) => Object.fromEntries(new URL(u, "http://x").searchParams));

const T0 = Date.now();
const row = (i, over = {}) => ({
  // The server's shape: alert_time is r.AlertTime.ToString("o") of a naive-UTC timestamp, so seven fraction digits and NO zone suffix
  // (#5570 r2 H1). toISOString() would add a Z, which hides a reader that parses the text as local time.
  alert_time: new Date(T0 - 60000 - i * 60000).toISOString().replace("Z", "0000"),
  server_id: 1, server_name: "srv-a", stored_server_name: "srv-a", metric_name: "High CPU " + i,
  current_value: 90, threshold_value: 80, severity: "warning", severity_source: "fired", dismissed: false, ...over,
});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "alert-range-"));
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert; every stand-in below is
     written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return null; }\n'
  );
  const { renderAlerts } = await import(pathToFileURL(path.join(scratch, "pages", "alerts.js")).href);

  const newMain = () => { const m = new FakeNode("main"); m.isRoot = true; return m; };
  const selects = (main) => main.all("select");
  const byLabel = (main, label) => selects(main).find((s) => s.attrs["aria-label"] === label);
  const checkbox = (main) => main.all("input").find((i) => i.attrs.type === "checkbox");
  const pick = async (sel, value) => { sel.value = String(value); sel.fire("change"); await new Promise((r) => setTimeout(r, 20)); };
  const bodyRows = (main) => { const t = main.all("table")[0]; return t ? t.children[1].children : []; };
  // The Metric column is found by its header, not a fixed index: W14 (#5644) put Triage and Mute before Time, so the
  // column position moves with the grid's leading columns (and with whether the seat can edit).
  const metricIndex = (main) => {
    const t = main.all("table")[0];
    const i = t ? t.children[0].children[0].children.findIndex((th) => th.textContent.trim().startsWith("Metric")) : -1;
    if (i < 0) throw new Error("the Alert History grid has no Metric header");
    return i;
  };
  const metrics = (main) => { const i = metricIndex(main); return bodyRows(main).map((tr) => tr.children[i].textContent); };
  const settle = () => new Promise((r) => setTimeout(r, 20));
  const out = {};

  const scenarios = {
    async reads() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const main = newMain();
      await renderAlerts(main);
      out.first = readsOf().pop();
      urls.length = 0; pickRange(main, "Time range", "168h"); await settle(); out.window = readsOf().pop();
      urls.length = 0; await pick(byLabel(main, "Row limit"), 1000); out.limit = readsOf().pop();
      urls.length = 0; await pick(byLabel(main, "Server"), "srv-b"); out.server = readsOf().pop();
      const cb = checkbox(main);
      urls.length = 0; cb.checked = true; cb.fire("change"); await settle(); out.dismissed = readsOf().pop();
      /* What the popup greys out depends on the date: Year to Date is inside the 90 day reach until day 90 of the year, so from
         1 January to 31 March it is offered instead of greyed out. The clock is pinned to mid-June so the lists are the same on
         every day (#7055). */
      const realNow = Date.now;
      Date.now = () => Date.UTC(2026, 5, 15, 12, 0, 0);
      try {
        out.windowOptions = pickerItems(main, "Time range").filter((i) => !i.disabled).map((i) => i.name);
        out.windowGreyed = pickerItems(main, "Time range").filter((i) => i.disabled).map((i) => i.name);
      } finally {
        Date.now = realNow;
      }
      out.windowShape = popupShape(main, "Time range");
      out.refused = pickRange(main, "Time range", "120d");
      urls.length = 0; pickRange(main, "Time range", "yesterday"); await settle(); out.finished = readsOf().pop();
      urls.length = 0; pickRange(main, "Time range", "30m"); await settle(); out.thirtyMinutes = readsOf().pop();
      out.limitOptions = byLabel(main, "Row limit").children.map((o) => o.attrs.value);
      out.serverOptions = byLabel(main, "Server").children.map((o) => o.attrs.value);
    },
    async survive() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const first = newMain();
      await renderAlerts(first);
      pickRange(first, "Time range", "4h"); await settle();
      await pick(byLabel(first, "Row limit"), 500);
      await pick(byLabel(first, "Server"), "srv-a");
      const cb = checkbox(first); cb.checked = true; cb.fire("change"); await settle();
      // the 60 s poll: the same still-open page
      urls.length = 0; await renderAlerts(first); out.poll = readsOf();
      // a visit elsewhere and back: a fresh main
      const second = newMain();
      urls.length = 0; await renderAlerts(second);
      out.back = readsOf();
      out.shown = {
        window: pickerText(second, "Time range"), limit: byLabel(second, "Row limit").value,
        server: byLabel(second, "Server").value, dismissed: checkbox(second).checked,
      };
    },
    async truncated() {
      alertsReply = { alerts: [row(1), row(2)], truncated: true };
      const a = newMain(); await renderAlerts(a); out.truncated = a.textContent.includes("More alerts exist");
      alertsReply = { alerts: [row(1)], truncated: false };
      const b = newMain(); await renderAlerts(b); out.notTruncated = b.textContent.includes("More alerts exist");
      alertsReply = { alerts: [row(1), row(2)], truncated: true };
      await renderAlerts(b); out.pollTruncated = b.textContent.includes("More alerts exist");
      alertsReply = { alerts: [row(1)], truncated: false };
      await renderAlerts(b); out.pollCleared = b.textContent.includes("More alerts exist");
    },
    async dismissed() {
      alertsReply = { alerts: [row(1), row(2, { dismissed: true })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.classes = bodyRows(main).map((tr) => tr.className);
      // a dismissed row that arrives on a later reconcile is marked too
      alertsReply = { alerts: [row(0, { dismissed: true }), row(1), row(2, { dismissed: true })], truncated: false };
      await renderAlerts(main);
      out.afterPoll = bodyRows(main).map((tr) => tr.className);
    },
    async dismissedLater() {
      alertsReply = { alerts: [row(1), row(2)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.before = bodyRows(main).map((tr) => tr.className);
      out.beforeStatus = bodyRows(main).map((tr) => tr.textContent.includes("Dismissed"));
      // the same alerts on the next poll, the second one dismissed in between
      alertsReply = { alerts: [row(1), row(2, { dismissed: true })], truncated: false };
      await renderAlerts(main);
      out.after = bodyRows(main).map((tr) => tr.className);
      out.afterStatus = bodyRows(main).map((tr) => tr.textContent.includes("Dismissed"));
      out.afterTitle = bodyRows(main).map((tr) => tr.getAttribute("title"));
      out.order = metrics(main);
      // and restored again
      alertsReply = { alerts: [row(1), row(2)], truncated: false };
      await renderAlerts(main);
      out.restored = bodyRows(main).map((tr) => tr.className);
    },
    async sharedName() {
      alertsReply = { alerts: [row(1, { server_id: 1 }), row(1, { server_id: 2 })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.rows = bodyRows(main).length;
    },
    async reconcile() {
      alertsReply = { alerts: [row(1), row(3), row(5)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      const keep = bodyRows(main)[0];
      out.before = metrics(main);
      // a narrower window: the oldest row leaves, a newer one lands first, one lands in the middle
      alertsReply = { alerts: [row(0), row(1), row(2), row(3)], truncated: false };
      pickRange(main, "Time range", "4h"); await settle();
      out.after = metrics(main);
      out.keptNode = bodyRows(main)[1] === keep;
      alertsReply = { alerts: [row(3)], truncated: false };
      pickRange(main, "Time range", "1h"); await settle();
      out.shrunk = metrics(main);
    },
    async trim() {
      // Review r1 H2: the read takes whole hours, so a 5 or 30 minute range lists only the alerts inside it. Rows are 1, 20 and 90 minutes old.
      alertsReply = { alerts: [row(0), row(19), row(89)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.day = metrics(main);
      pickRange(main, "Time range", "5m"); await settle(); out.fiveMinutes = metrics(main);
      pickRange(main, "Time range", "30m"); await settle(); out.thirtyMinutes = metrics(main);
      pickRange(main, "Time range", "4h"); await settle(); out.fourHours = metrics(main);
      out.tzOffsetMinutes = new Date().getTimezoneOffset();
    },
    async investigations() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      const invReads = () => urls.filter((u) => u.includes("investigation"));
      out.readsBeforeOpen = invReads().length;
      const panel = main.all("details").find((d) => d.textContent.startsWith("Investigations"));
      out.panelFound = !!panel;
      panel.open = true; panel.fire("toggle"); await settle();
      out.reads = invReads().map((u) => u.split("?")[0]);
      out.alertTableFirst = main.all("table")[0].textContent.includes("High CPU 1");
      const table = panel.all("table")[0];
      out.rows = table ? table.children[1].children.map((tr) => tr.textContent) : [];
      const journal = table.children[1].children[0].all("details")[0];
      journal.open = true; journal.fire("toggle"); await settle();
      out.journal = journal.textContent;
      out.journalRead = invReads().filter((u) => u.includes("request_id=")).length;
      urls.length = 0; await renderAlerts(main); out.pollReads = invReads().length;
      panel.open = false; urls.length = 0; await renderAlerts(main); out.closedPollReads = invReads().length;
      out.writes = urls.filter((u) => !u.startsWith("/api/read") && !u.startsWith("/api/session") && !u.includes("/api/catalog"));
    },
    /* #5834: each read is a page; the panel says when the list, the reports read or a journal held more. */
    async investigationsTruncated() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const openPanel = async (truncated) => {
        investigationsReply = { ...investigationsReply, truncated };
        reportsReply = { ...reportsReply, truncated };
        journalReply = { ...journalReply, journal_truncated: truncated };
        const main = newMain(); await renderAlerts(main);
        const panel = main.all("details").find((d) => d.textContent.startsWith("Investigations"));
        panel.open = true; panel.fire("toggle"); await settle();
        const journal = panel.all("table")[0].children[1].children[0].all("details")[0];
        journal.open = true; journal.fire("toggle"); await settle();
        return { panel: panel.textContent, journal: journal.textContent };
      };
      out.clear = await openPanel(false);
      out.cut = await openPanel(true);
    },
    async mute() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.links = main.all("a").map((a) => a.textContent + " " + a.attrs.href);
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
