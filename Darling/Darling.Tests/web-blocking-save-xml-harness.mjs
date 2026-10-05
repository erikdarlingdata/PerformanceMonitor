/* Runs the web viewer's Blocking tab "Deadlock Graphs" and "Blocked Process Reports" panels (server-tabs.js with util.js,
   panels.js, charts.js and grid-tools.js) against scripted get_deadlock_detail and get_blocked_process_xml answers, clicks a
   row's Save XML button, and prints the headers it drew and the downloads it made (Blob type, text, file name) as one line
   of JSON. WebBlockingSaveXmlBehaviourTests starts it as
       node web-blocking-save-xml-harness.mjs <path to wwwroot/js> <scenario>
   The DOM and fetch are stand-ins; everything else is the shipped code. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.listeners = {};
    this.open = false;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
    if (name === "open") this.open = true;
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, listener) {
    (this.listeners[type] = this.listeners[type] || []).push(listener);
  }
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 1000, height: 320 };
  }
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

globalThis.Node = FakeNode;
const downloads = [];
let lastBlob = null;
globalThis.URL.createObjectURL = (blob) => {
  lastBlob = blob;
  return "blob:test";
};
globalThis.URL.revokeObjectURL = () => {};
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (tag) => {
    const n = new FakeNode(tag);
    if (tag === "a") n.click = () => downloads.push({ name: n.download, blob: lastBlob });
    return n;
  },
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

const fetches = [];
let answer = () => ({ status: 200, body: {} });
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  const reply = answer(new URL(String(url), "http://viewer.test"));
  const raw = reply.body === undefined ? "" : JSON.stringify(reply.body);
  return { status: reply.status, ok: reply.status >= 200 && reply.status < 300, text: async () => raw };
};

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "blocking-save-xml-"));
let modules;
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "charts.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  /* Modules that server-tabs.js and panels.js import: copied when present, so the harness works whichever PR lands first. */
  for (const f of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js")]) {
    if (fs.existsSync(path.join(jsDir, f))) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  }
  /* #5246: the Graph cell of the Deadlock Graphs grid. */
  fs.mkdirSync(path.join(scratch, "pages"), { recursive: true });
  fs.copyFileSync(path.join(jsDir, "pages", "deadlock-graph.js"), path.join(scratch, "pages", "deadlock-graph.js"));
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const data = (body) => ({ status: 200, body });
const tool = (url) => url.pathname.replace("/api/read/", "");

const deadlock = (over) => ({
  collection_time: "2026-07-01T10:00:06.0000000",
  deadlock_time: "2026-07-01T10:00:05.0000000",
  victim_process_id: "process1",
  dedup_key: "k1",
  processes: [],
  processes_truncated: 0,
  deadlock_graph_xml: "<deadlock-list><deadlock/></deadlock-list>",
  deadlock_graph_xml_truncated: false,
  ...over,
});
const report = { event_time: "2026-07-01T11:02:03.0000000", database_name: "AppDb", blocked_spid: 55, blocking_spid: 60, wait_time_ms: 900, blocked_process_report_xml: "<blocked-process-report/>" };

const all = (node, tag, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (node.tag === tag) found.push(node);
  node.children.forEach((c) => all(c, tag, found));
  return found;
};

async function buildPanels(deadlocks, reports) {
  answer = (url) => {
    const t = tool(url);
    if (t === "get_deadlock_detail") return data({ server: "SRV1", deadlocks });
    if (t === "get_blocked_process_xml") return data({ server: "SRV1", reports });
    return data({});
  };
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "blocking");
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("SRV1", { hours: 24, label: "last 24 hours" }).filter((n) => n && typeof n === "object"));
  for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));
  const find = (title) => root.children.find((n) => n.textContent.startsWith(title)) || null;
  return { deadlockPanel: find("Deadlock Graphs"), bprPanel: find("Blocked Process Reports") };
}

const saveButtons = (panel) => all(panel, "button").filter((b) => b.textContent === "Save XML");
const headers = (panel) => all(panel, "th").map((th) => th.textContent);
async function describeDownload(d) {
  return { name: d.name, type: d.blob.type, text: await d.blob.text() };
}

const out = { rejections };
if (scenario === "save") {
  const { deadlockPanel, bprPanel } = await buildPanels([deadlock()], [report]);
  out.bprHeaders = headers(bprPanel);
  out.deadlockHeaders = headers(deadlockPanel);
  const bpr = saveButtons(bprPanel)[0];
  const dl = saveButtons(deadlockPanel)[0];
  out.bprDisabledBefore = !!bpr.disabled;
  bpr.listeners.click[0]();
  dl.listeners.click[0]();
  out.downloads = await Promise.all(downloads.map(describeDownload));
  out.fetches = fetches.filter((f) => f.includes("get_blocked_process_xml") || f.includes("get_deadlock_detail")).length;
} else if (scenario === "truncated") {
  const { deadlockPanel } = await buildPanels([deadlock({ deadlock_graph_xml_truncated: true, deadlock_graph_xml: "<deadlock-list><dead" })], [report]);
  const dl = saveButtons(deadlockPanel)[0];
  out.disabled = !!dl.disabled;
  out.title = dl.getAttribute("title");
  out.hasClick = !!(dl.listeners.click && dl.listeners.click.length);
  out.downloads = downloads.length;
} else if (scenario === "noxml") {
  const { bprPanel } = await buildPanels([deadlock()], [{ ...report, blocked_process_report_xml: null }]);
  const b = saveButtons(bprPanel)[0];
  out.disabled = !!b.disabled;
  out.hasClick = !!(b.listeners.click && b.listeners.click.length);
} else {
  throw new Error("unknown scenario " + scenario);
}
console.log(JSON.stringify(out));
