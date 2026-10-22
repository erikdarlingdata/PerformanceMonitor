/* Runs the shipped Known Patterns page (wwwroot/js/pages/known-patterns.js) with the real shared renderer (util.js, alerts-api.js)
   on a small fake DOM and a recording fetch, and prints as one line of JSON what the page asked for and drew.
   KnownPatternsPageBehaviourTests starts it as
       node known-patterns-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM and fetch are stand-ins (the same fake DOM as alert-history-dismiss-harness.mjs). */
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
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
  find(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.find(pred, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/known-patterns" };
globalThis.window = { confirm: () => true, location: globalThis.location, history: { replaceState() {} } };

const jsDir = process.argv[2];
const scenario = process.argv[3];

const writes = [];
let canEdit = true;
let entries = [];
let writeReply = (req) => ({ status: 200, body: { status: "created", id: 1 } });
const confirms = [];
window.confirm = (m) => { confirms.push(m); return true; };
globalThis.fetch = async (url, init) => {
  const u = String(url);
  if (init && init.method && init.method !== "GET") {
    const req = init.body ? JSON.parse(init.body) : null;
    writes.push({ method: init.method, url: u, contentType: init.headers["Content-Type"], req });
    const r = writeReply(req, init.method);
    return { status: r.status, ok: r.status < 300, text: async () => JSON.stringify(r.body) };
  }
  let body = {};
  if (u.startsWith("/api/session")) body = { can_edit: canEdit };
  else if (u.startsWith("/api/admin/servers")) body = { servers: [{ server_id: 7, server_name: "srv-a", display_name: "Server A" }, { server_id: 9, server_name: "srv-b" }] };
  else if (u.includes("/get_known_patterns")) body = { entries_returned: entries.length, known_patterns: entries };
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};

const entry = (over = {}) => ({
  id: 5, server_id: 7, metric_name: "High CPU", time_of_day: "02:00", time_zone: "America/New_York", window_minutes: 45,
  note: "Nightly backup.", created_by: "web", created_at_utc: "2026-05-01T00:00:00.0000000Z", expires_at_utc: null, expired: false, ...over,
});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "known-patterns-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return null; }\n'
  );
  const page = await import(pathToFileURL(path.join(scratch, "pages", "known-patterns.js")).href);

  const newMain = () => { const m = new FakeNode("main"); m.isRoot = true; return m; };
  const tick = () => new Promise((r) => setTimeout(r, 30));
  const button = (main, text) => main.all("button").find((b) => b.textContent === text);
  const field = (main, key) => main.find((n) => n.attrs["data-field"] === key)[0];
  const role = (main, name) => main.find((n) => n.attrs["data-role"] === name)[0];
  const type = (main, key, value) => { const f = field(main, key); f.value = value; f.fire(f.tag === "select" ? "change" : "input"); };
  const bodyRows = (main) => { const t = main.all("table")[0]; return t ? t.children[1].children : []; };
  const out = {};

  const scenarios = {
    async save() {
      const main = newMain(); await page.renderKnownPatterns(main);
      button(main, "New known pattern").fire("click");
      out.zoneDefault = field(main, "time_zone").attrs.value;
      out.browserZone = page.browserZone();
      out.serverChoices = field(main, "server_id").children.map((o) => o.attrs.value + "=" + o.textContent);
      type(main, "server_id", "7");
      type(main, "metric_name", " High CPU ");
      type(main, "time_of_day", "02:00");
      type(main, "time_zone", "America/New_York");
      type(main, "window_minutes", "45");
      type(main, "note", "Nightly backup runs here.");
      type(main, "expires_at_utc", "2030-01-02T03:04");
      entries = [entry({ note: "Nightly backup runs here." })];
      button(main, "Save").fire("click"); await tick();
      out.writes = writes;
      out.formClosed = !field(main, "note");
      out.rows = bodyRows(main).length;
      out.expiresIsUtcIso = /^2030-01-0[12]T\d\d:\d\d:00\.000Z$/.test(writes[0].req.expires_at_utc);
    },
    async anyScope() {
      const main = newMain(); await page.renderKnownPatterns(main);
      button(main, "New known pattern").fire("click");
      type(main, "note", "Known quiet.");
      button(main, "Save").fire("click"); await tick();
      out.body = writes[0].req;
    },
    async refusal() {
      writeReply = () => ({ status: 400, body: { status: "invalid", error: "A known pattern needs a note: what the operator knows about it." } });
      const main = newMain(); await page.renderKnownPatterns(main);
      button(main, "New known pattern").fire("click");
      type(main, "metric_name", "High CPU");
      button(main, "Save").fire("click"); await tick();
      out.refusal = role(main, "refusal") ? role(main, "refusal").textContent : null;
      out.formStillOpen = !!field(main, "note");
      out.typedKept = field(main, "metric_name").attrs.value;
      out.writes = writes.length;
    },
    async noteIsText() {
      const hostile = "<img src=x onerror=alert(1)> & <b>bold</b>";
      entries = [entry({ note: hostile })];
      const main = newMain(); await page.renderKnownPatterns(main);
      const row = bodyRows(main)[0];
      out.noteCell = row.children[2].textContent;
      out.hostile = hostile;
      out.imgNodes = main.all("img").length;
      out.boldNodes = main.all("b").length;
    },
    async edit() {
      entries = [entry({ expires_at_utc: "2030-01-02T03:04:00.0000000" })];
      writeReply = () => ({ status: 200, body: { status: "updated", id: 5 } });
      const main = newMain(); await page.renderKnownPatterns(main);
      button(main, "Edit").fire("click");
      out.prefilled = {
        server: field(main, "server_id").value, metric: field(main, "metric_name").attrs.value, time: field(main, "time_of_day").attrs.value,
        zone: field(main, "time_zone").attrs.value, window: field(main, "window_minutes").attrs.value, note: field(main, "note").value,
      };
      type(main, "note", "Nightly backup, moved.");
      button(main, "Save").fire("click"); await tick();
      out.writes = writes.map((w) => ({ method: w.method, url: w.url, req: w.req }));
    },
    async remove() {
      entries = [entry()];
      writeReply = () => ({ status: 200, body: { status: "deleted", id: 5 } });
      const main = newMain(); await page.renderKnownPatterns(main);
      entries = [];
      button(main, "Delete").fire("click"); await tick();
      out.writes = writes.map((w) => ({ method: w.method, url: w.url, contentType: w.contentType }));
      out.confirmAsked = confirms.length;
      out.rowsAfter = bodyRows(main).length;
    },
    async readOnly() {
      canEdit = false;
      entries = [entry()];
      const main = newMain(); await page.renderKnownPatterns(main);
      out.newButton = !!button(main, "New known pattern");
      out.editButton = !!button(main, "Edit");
      out.deleteButton = !!button(main, "Delete");
      out.rows = bodyRows(main).length;
    },
    async counter() {
      const main = newMain(); await page.renderKnownPatterns(main);
      button(main, "New known pattern").fire("click");
      type(main, "note", "x".repeat(501));
      out.over = role(main, "note-counter").textContent;
      out.overClass = role(main, "note-counter").className;
      type(main, "note", "x".repeat(10));
      out.under = role(main, "note-counter").textContent;
    },
    async pure() {
      out.timeOnlyZone = page.formToBody({ time_of_day: "02:00", time_zone: "", note: "n" }, "Europe/Paris");
      out.noTimeNoZone = page.formToBody({ time_zone: "Europe/Paris", window_minutes: "30", note: "n" }, "Europe/Paris");
      out.interpret400 = page.interpretWrite(400, { status: "invalid", error: "The refusal." });
      out.interpret404 = page.interpretWrite(404, { status: "not_found" }).kind;
      out.interpret403 = page.interpretWrite(403, {}).kind;
    },
  };
  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
