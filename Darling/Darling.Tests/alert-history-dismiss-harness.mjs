/* Dismiss Selected / Dismiss All on the shipped Alert History page; same fake DOM as alert-history-range-harness.mjs.
   Runs the shipped Alert History page (wwwroot/js/pages/alerts.js) with the real shared renderer (panels.js, util.js,
   mute-context.js) on a small fake DOM and a recording fetch, and prints as one line of JSON what the page asked for
   and drew. AlertHistoryDismissBehaviourTests starts it as
       node alert-history-dismiss-harness.mjs <path to wwwroot/js> <scenario>
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
  querySelectorAll(tag) { return this.all(tag); }
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
const posts = [];
let canEdit = true;
let postReply = (req) => ({ status: 200, body: { requested: req.alerts.length, dismissed: req.alerts.length, already_dismissed: 0, unknown: 0 } });
globalThis.confirm = () => true;
globalThis.fetch = async (url, init) => {
  const u = String(url);
  if (init && init.method === "POST") {
    const req = JSON.parse(init.body);
    posts.push({ url: u, contentType: init.headers["Content-Type"], req });
    const r = postReply(req);
    return { status: r.status, ok: r.status < 300, text: async () => JSON.stringify(r.body) };
  }
  urls.push(u);
  let body = {};
  if (u.startsWith("/api/session")) body = { can_edit: canEdit };
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
  for (const f of ["util.js", "panels.js", "mute-context.js", "views-api.js", "refresh-policy.js", "read-fields.js", "grid-tools.js"]) {
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

  const boxes = (main) => bodyRows(main).map((tr) => tr.all("input")[0]);
  const button = (main, text) => main.all("button").find((b) => b.textContent.startsWith(text));
  const check = (cb) => { cb.checked = true; cb.fire("change"); };
  const statusOf = (main) => main.all("span").find((s) => s.className.includes("dismiss-status"));
  const tick = () => new Promise((r) => setTimeout(r, 30));
  const scenarios = {
    async readOnly() {
      canEdit = false;
      alertsReply = { alerts: [row(1), row(2)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.checkboxes = bodyRows(main).filter((tr) => tr.all("input").length).length;
      out.hasSelected = !!button(main, "Dismiss Selected");
      out.hasAll = !!button(main, "Dismiss All");
    },
    async selected() {
      alertsReply = { alerts: [row(1), row(2), row(3)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.boxes = boxes(main).length;
      out.selectedDisabledBefore = button(main, "Dismiss Selected").disabled;
      check(boxes(main)[0]); check(boxes(main)[2]);
      // the reply after the dismiss no longer lists the two rows
      alertsReply = { alerts: [row(2)], truncated: false };
      button(main, "Dismiss Selected").fire("click"); await tick();
      out.posts = posts;
      out.status = statusOf(main).textContent;
      out.rowsAfter = bodyRows(main).length;
      out.reads = readsOf().length;
    },
    async showDismissed() {
      alertsReply = { alerts: [row(1), row(2)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      const cb = main.all("input").find((i) => i.attrs["aria-label"] === "Show dismissed alerts");
      cb.checked = true; cb.fire("change"); await tick();
      check(boxes(main)[0]);
      alertsReply = { alerts: [row(1, { dismissed: true }), row(2)], truncated: false };
      button(main, "Dismiss Selected").fire("click"); await tick();
      out.rows = bodyRows(main).length;
      out.dismissedWord = bodyRows(main)[0].textContent.includes("Dismissed");
      out.firstHasBox = bodyRows(main)[0].all("input").length;
    },
    async all() {
      alertsReply = { alerts: [row(1), row(2), row(3, { dismissed: true }), row(4, { server_name: "srv-b", server_id: 2 })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      // the filter box narrows to srv-b: Dismiss All acts on what is shown
      const f = main.all("input").find((i) => i.attrs.type === "text"); f.value = "srv-b"; f.fire("input");
      button(main, "Dismiss All").fire("click"); await tick();
      out.filtered = posts.map((p) => p.req.alerts.map((a) => a.server_id));
      posts.length = 0;
      f.value = ""; f.fire("input");
      alertsReply = { alerts: [], truncated: false };
      button(main, "Dismiss All").fire("click"); await tick();
      out.all = posts.map((p) => ({ url: p.url, contentType: p.contentType, keys: p.req.alerts }));
    },
    async chunks() {
      const many = Array.from({ length: 1500 }, (_, i) => row(i + 1));
      alertsReply = { alerts: many, truncated: false };
      const main = newMain(); await renderAlerts(main);
      alertsReply = { alerts: [], truncated: false };
      button(main, "Dismiss All").fire("click"); await tick();
      out.sizes = posts.map((p) => p.req.alerts.length);
      out.status = statusOf(main).textContent;
    },
    async survives() {
      alertsReply = { alerts: [row(1), row(2)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      check(boxes(main)[1]);
      // the 60 s poll lands on the same page, then the page is left and re-opened
      alertsReply = { alerts: [row(0), row(1), row(2)], truncated: false };
      await renderAlerts(main);
      out.afterPoll = boxes(main).map((b) => b.checked);
      const second = newMain(); await renderAlerts(second);
      out.afterReopen = boxes(second).map((b) => b.checked);
      out.label = button(second, "Dismiss Selected").textContent;
      // a changed filter clears it
      const sel = byLabel(second, "Row limit"); await pick(sel, 500);
      out.afterFilter = boxes(second).map((b) => b.checked);
    },
    async refused() {
      alertsReply = { alerts: [row(1), row(2)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      check(boxes(main)[0]);
      postReply = () => ({ status: 403, body: { error: "This seat is read-only." } });
      button(main, "Dismiss Selected").fire("click"); await tick();
      out.status = statusOf(main).textContent;
      out.rows = bodyRows(main).length;
      out.stillChecked = boxes(main)[0].checked;
      postReply = () => ({ status: 401, body: { error: "Session expired" } });
      button(main, "Dismiss Selected").fire("click"); await tick();
      out.status401 = statusOf(main).textContent;
    },
    async counts() {
      alertsReply = { alerts: [row(1), row(2), row(3), row(4)], truncated: false };
      const main = newMain(); await renderAlerts(main);
      postReply = (req) => ({ status: 200, body: { requested: 4, dismissed: 1, already_dismissed: 0, unknown: 3 } });
      alertsReply = { alerts: [], truncated: false };
      button(main, "Dismiss All").fire("click"); await tick();
      out.status = statusOf(main).textContent;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
