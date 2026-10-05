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
let detailsReply = () => ({ status: 200, body: { details: [] } });
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
  if (u.includes("/get_alert_details")) {
    const r = detailsReply(Object.fromEntries(new URL(u, "http://x").searchParams));
    return { status: r.status, ok: r.status < 300, text: async () => JSON.stringify(r.body) };
  }
  if (readFails && u.includes("/get_alert_history")) return { status: 500, ok: false, text: async () => JSON.stringify({ error: "read failed" }) };
  let body = {};
  if (u.startsWith("/api/session")) body = { can_edit: canEdit };
  else if (u.includes("/list_servers")) body = servers;
  else if (u.includes("/get_alert_history")) body = typeof alertsReply === "function" ? alertsReply(u) : alertsReply;
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
const detailReadsOf = () => urls.filter((u) => u.includes("/get_alert_details")).map((u) => Object.fromEntries(new URL(u, "http://x").searchParams));
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
  const analysisMetric = "Analysis: plan_regression [524101aa]";
  const analysisRow = (over = {}) => row(1, { metric_name: analysisMetric, detail_text: "Diagnosis\n  Story: PLAN_REGRESSION\n  Severity: 1.20", ...over });
  const shape = (d) => {
    const all = walk(d);
    return {
      paragraphs: all.filter((n) => n.tag === "p").map((n) => n.textContent),
      pres: all.filter((n) => n.tag === "pre").map((n) => n.textContent),
      buttons: all.filter((n) => n.tag === "button").map((n) => n.textContent),
      fields: all.filter((n) => n.className === "detail-field").map((n) => n.textContent),
      headings: all.filter((n) => n.className === "detail-heading").map((n) => n.textContent),
    };
  };
  const scenarios = {
    async withDetails() {
      alertsReply = { alerts: [analysisRow()], truncated: false };
      detailsReply = () => ({ status: 200, body: { details: [diagnosis, advice, script] } });
      const main = newMain(); await renderAlerts(main);
      out.requested = readsOf()[0];
      out.callsBeforeOpen = detailReadsOf().length;
      const d = main.all("details")[0];
      d.open = true; d.fire("toggle");
      out.loadingWhileFetching = walk(d).some((n) => n.className && String(n.className).includes("loading"));
      await tick();
      Object.assign(out, shape(d));
      out.detailRequest = detailReadsOf()[0];
      const copyBtn = walk(d).find((n) => n.tag === "button");
      if (copyBtn) { copyBtn.fire("click"); await tick(); }
      out.copied = copied.slice();
      out.buttonAfter = copyBtn ? copyBtn.textContent : null;
      /* Closing and re-opening the same row, then a fresh render of the same page (the poll rebuild), ask nothing more. */
      d.open = false; d.fire("toggle"); d.open = true; d.fire("toggle"); await tick();
      const again = newMain(); await renderAlerts(again);
      const d2 = await open(again);
      out.callsAfterReopenAndRerender = detailReadsOf().length;
      out.rerenderPres = shape(d2).pres;
    },
    async withoutDetails() {
      alertsReply = { alerts: [analysisRow({ metric_name: "Analysis: other [bbbb]", server_id: 2 })], truncated: false };
      detailsReply = () => ({ status: 200, body: { details: [] } });
      const main = newMain(); await renderAlerts(main);
      const d = await open(main);
      const s = shape(d);
      out.calls = detailReadsOf().length;
      out.paragraphs = s.paragraphs.length; out.pres = s.pres.length; out.buttons = s.buttons.length;
      out.fields = s.fields; out.headings = s.headings;
    },
    async engineAlert() {
      alertsReply = { alerts: [row(1, { detail_text: "Diagnosis\n  Story: x\n  Severity: 1.20" })], truncated: false };
      const main = newMain(); await renderAlerts(main);
      const d = await open(main);
      const s = shape(d);
      out.calls = detailReadsOf().length;
      out.pres = s.pres.length; out.fields = s.fields; out.headings = s.headings;
    },
    async failedFetch() {
      alertsReply = { alerts: [analysisRow({ server_id: 3 })], truncated: false };
      detailsReply = () => ({ status: 500, body: { error: "read failed" } });
      const main = newMain(); await renderAlerts(main);
      const d = await open(main);
      const s = shape(d);
      out.calls = detailReadsOf().length;
      out.pres = s.pres.length; out.buttons = s.buttons.length; out.fields = s.fields; out.headings = s.headings;
    },
    async hostile() {
      const evil = "<img src=x onerror=alert(1)>";
      alertsReply = { alerts: [analysisRow({ server_id: 4, detail_text: "x" })], truncated: false };
      detailsReply = () => ({ status: 200, body: { details: [
        { heading: evil, fields: [{ label: "<b>label</b>", value: "<script>alert(2)</script>" }], is_code_block: false, body: "<svg onload=alert(3)> prose" },
        { heading: "Fix", fields: [], is_code_block: true, body: "<script>alert(1)</script>" },
      ] } });
      const main = newMain(); await renderAlerts(main);
      const d = await open(main);
      const all = walk(d);
      out.pre = all.filter((n) => n.tag === "pre").map((n) => n.textContent);
      out.headings = all.filter((n) => n.className === "detail-heading").map((n) => n.textContent);
      out.fields = all.filter((n) => n.className === "detail-field").map((n) => n.textContent);
      out.paragraphs = all.filter((n) => n.tag === "p").map((n) => n.textContent);
      out.imgs = all.filter((n) => ["img", "script", "svg", "b"].includes(n.tag)).length;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
