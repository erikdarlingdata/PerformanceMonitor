/* The advice and fix script in an expanded alert on the shipped Alert History page (#5241); same fake DOM as alert-history-range-harness.mjs.
   Runs the shipped Alert History page (wwwroot/js/pages/alerts.js) with the real shared renderer (panels.js, util.js,
   mute-context.js) on a small fake DOM and a recording fetch, and prints as one line of JSON what the page asked for
   and drew. AlertHistoryAdviceTests starts it as
       node alert-history-advice-harness.mjs <path to wwwroot/js> <scenario>
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
  get parentNode() { return this.parent; }
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
const confirms = [];
globalThis.confirm = (m) => { confirms.push(m); return true; };
let readFails = false;
let postGate = null;
globalThis.fetch = async (url, init) => {
  const u = String(url);
  if (init && init.method === "POST") {
    if (postGate) await postGate;
    const req = JSON.parse(init.body);
    posts.push({ url: u, contentType: init.headers["Content-Type"], req });
    const r = postReply(req);
    return { status: r.status, ok: r.status < 300, text: async () => JSON.stringify(r.body) };
  }
  urls.push(u);
  if (readFails && u.includes("/get_alert_history")) return { status: 500, ok: false, text: async () => JSON.stringify({ error: "read failed" }) };
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
  for (const f of ["alerts.js", "analysis-findings.js", "plan-viewer.js"]) {
    if (fs.existsSync(path.join(jsDir, "pages", f))) fs.copyFileSync(path.join(jsDir, "pages", f), path.join(scratch, "pages", f));
  }
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

  const tick = () => new Promise((r) => setTimeout(r, 30));
  const walk = (n, acc = []) => { acc.push(n); n.children.forEach((c) => walk(c, acc)); return acc; };
  const open = async (main) => {
    const d = main.all("details")[0];
    d.open = true; d.fire("toggle"); await tick();
    return d;
  };
  const copied = [];
  Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { copied.push(t); } } }, configurable: true });
  const advice = {
    heading: "Advice", fields: [], is_code_block: false,
    body: "Investigation: the plan regressed\n\nRemediation: force the better plan",
  };
  const script = { heading: "Remediation T-SQL", fields: [], is_code_block: true, body: "EXEC sys.sp_query_store_force_plan 1, 2;" };
  const diagnosis = { heading: "Diagnosis", fields: [{ label: "Story", value: "PLAN_REGRESSION" }], body: null, is_code_block: false };
  const scenarios = {
    async withDetails() {
      alertsReply = { alerts: [row(1, { detail_text: "Diagnosis\n  Story: PLAN_REGRESSION\n  Severity: 1.20", details: [diagnosis, advice, script] })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      out.requested = readsOf()[0];
      const d = await open(main);
      const all = walk(d);
      out.paragraphs = all.filter((n) => n.tag === "p").map((n) => n.textContent);
      out.pres = all.filter((n) => n.tag === "pre").map((n) => n.textContent);
      out.buttons = all.filter((n) => n.tag === "button").map((n) => n.textContent);
      out.fields = all.filter((n) => n.className === "detail-field").map((n) => n.textContent);
      out.headings = all.filter((n) => n.className === "detail-heading").map((n) => n.textContent);
      const copyBtn = all.find((n) => n.tag === "button");
      if (copyBtn) { copyBtn.fire("click"); await tick(); }
      out.copied = copied.slice();
      out.buttonAfter = copyBtn ? copyBtn.textContent : null;
    },
    async withoutDetails() {
      alertsReply = { alerts: [row(1, { detail_text: "Diagnosis\n  Story: x\n  Severity: 1.20" })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      const d = await open(main);
      const all = walk(d);
      out.paragraphs = all.filter((n) => n.tag === "p").length;
      out.pres = all.filter((n) => n.tag === "pre").length;
      out.buttons = all.filter((n) => n.tag === "button").length;
      out.fields = all.filter((n) => n.className === "detail-field").map((n) => n.textContent);
      out.headings = all.filter((n) => n.className === "detail-heading").map((n) => n.textContent);
    },
    async hostile() {
      alertsReply = { alerts: [row(1, { detail_text: "x", details: [{ heading: "<img src=x onerror=alert(1)>", fields: [], is_code_block: true, body: "<script>alert(1)</script>" }] })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      const d = await open(main);
      const all = walk(d);
      out.pre = all.filter((n) => n.tag === "pre").map((n) => n.textContent);
      out.headings = all.filter((n) => n.className === "detail-heading").map((n) => n.textContent);
      out.imgs = all.filter((n) => n.tag === "img" || n.tag === "script").length;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
