/* Runs the shipped Alert History page (wwwroot/js/pages/alerts.js) with the real shared renderer (panels.js, util.js,
   mute-context.js) on a small fake DOM and a recording fetch, and prints as one line of JSON what the page asked for
   and drew. AlertHistoryRangeBehaviourTests starts it as
       node alert-history-range-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM, fetch and charts.js are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

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
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/alerts" };

const jsDir = process.argv[2];
const scenario = process.argv[3];

const urls = [];
let alertsReply = { alerts: [], truncated: false };
const servers = { servers: [{ server_name: "srv-a", display_name: "Server A" }, { server_name: "srv-b" }] };
globalThis.fetch = async (url) => {
  const u = String(url);
  urls.push(u);
  let body = {};
  if (u.startsWith("/api/session")) body = { can_edit: true };
  else if (u.includes("/list_servers")) body = servers;
  else if (u.includes("/get_alert_history")) body = typeof alertsReply === "function" ? alertsReply(u) : alertsReply;
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
const readsOf = () => urls.filter((u) => u.includes("/get_alert_history")).map((u) => Object.fromEntries(new URL(u, "http://x").searchParams));

const row = (i, over = {}) => ({
  alert_time: new Date(Date.UTC(2026, 0, 1, 12, 0, 0) - i * 60000).toISOString(),
  server_id: 1, server_name: "srv-a", stored_server_name: "srv-a", metric_name: "High CPU " + i,
  current_value: 90, threshold_value: 80, severity: "warning", severity_source: "fired", dismissed: false, ...over,
});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "alert-range-"));
try {
  fs.mkdirSync(path.join(scratch, "pages"), { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "mute-context.js", "views-api.js", "refresh-policy.js", "read-fields.js"]) {
    if (fs.existsSync(path.join(jsDir, f))) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  }
  fs.copyFileSync(path.join(jsDir, "pages", "alerts.js"), path.join(scratch, "pages", "alerts.js"));
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
  const metrics = (main) => bodyRows(main).map((tr) => tr.children[2].textContent);
  const settle = () => new Promise((r) => setTimeout(r, 20));
  const out = {};

  const scenarios = {
    async reads() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const main = newMain();
      await renderAlerts(main);
      out.first = readsOf().pop();
      urls.length = 0; await pick(byLabel(main, "Time range"), 168); out.window = readsOf().pop();
      urls.length = 0; await pick(byLabel(main, "Row limit"), 1000); out.limit = readsOf().pop();
      urls.length = 0; await pick(byLabel(main, "Server"), "srv-b"); out.server = readsOf().pop();
      const cb = checkbox(main);
      urls.length = 0; cb.checked = true; cb.fire("change"); await settle(); out.dismissed = readsOf().pop();
      out.windowOptions = byLabel(main, "Time range").children.map((o) => o.attrs.value);
      out.limitOptions = byLabel(main, "Row limit").children.map((o) => o.attrs.value);
      out.serverOptions = byLabel(main, "Server").children.map((o) => o.attrs.value);
    },
    async survive() {
      alertsReply = { alerts: [row(1)], truncated: false };
      const first = newMain();
      await renderAlerts(first);
      await pick(byLabel(first, "Time range"), 4);
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
        window: byLabel(second, "Time range").value, limit: byLabel(second, "Row limit").value,
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
      await pick(byLabel(main, "Time range"), 4);
      out.after = metrics(main);
      out.keptNode = bodyRows(main)[1] === keep;
      alertsReply = { alerts: [row(3)], truncated: false };
      await pick(byLabel(main, "Time range"), 1);
      out.shrunk = metrics(main);
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
