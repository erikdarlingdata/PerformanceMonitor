/* Runs the web viewer's Blocking tab "Deadlock Graphs" panel (server-tabs.js with util.js, panels.js, charts.js and
   read-fields.js) against a scripted get_deadlock_detail answer, expands and collapses a deadlock's process sub-grid
   as a click on its <details> would, rebuilds the panel the way the 60 s poll does, and prints what it drew as one
   line of JSON. WebDeadlockProcessesBehaviourTests starts it as
       node web-deadlock-processes-harness.mjs <path to wwwroot/js> <scenario>
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
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
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

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "deadlock-processes-"));
let modules;
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const data = (body) => ({ status: 200, body });
const tool = (url) => url.pathname.replace("/api/read/", "");

const PROCESSES = [
  { victim: true, spid: 55, wait_time_ms: 1500, log_used: 200, transaction_count: 1, priority: 0, deadlock_type: "Regular", database_name: "AppDb", lock_mode: "X", login_name: "app_login", host_name: "HOST1", client_app: "TestApp", isolation_level: "read committed (2)", transaction_name: "user_transaction", wait_resource: "KEY: 5:72", sql_text: "UPDATE dbo.t SET c = 1" },
  { victim: false, spid: 60, wait_time_ms: 1200, log_used: 300, transaction_count: 1, priority: 0, deadlock_type: "Regular", database_name: "AppDb", login_name: "app_login", sql_text: "UPDATE dbo.t SET c = 2" },
];
const DEADLOCK = (key, processes, truncated = 0) => ({
  collection_time: "2026-07-01T10:00:06.0000000",
  deadlock_time: "2026-07-01T10:00:05.0000000",
  victim_process_id: "process1",
  dedup_key: key,
  processes,
  processes_truncated: truncated,
  deadlock_graph_xml: "<deadlock/>",
  deadlock_graph_xml_truncated: false,
});

const bodies = {
  two: { server: "SRV1", deadlocks: [DEADLOCK("k1", PROCESSES), DEADLOCK("k2", PROCESSES.slice(0, 1))] },
  noprocesses: { server: "SRV1", deadlocks: [DEADLOCK("k1", [])] },
  allcut: { server: "SRV1", deadlocks: [DEADLOCK("k1", PROCESSES), DEADLOCK("k5", [], 6)] },
  capped: { server: "SRV1", deadlocks: [DEADLOCK("k1", PROCESSES, 3)] },
};

/* #5244: one graph whose processes span databases A, B and C (and one with none), one with A only, one with an unknown database
   only. `graph` is the already laid-out graph the service sends; the process rows are filtered, the graph is not. */
const PROC = (spid, database) => ({ victim: spid === 1, spid, wait_time_ms: 10, deadlock_type: "Regular", ...(database === undefined ? {} : { database_name: database }) });
const GRAPH = (n) => ({ process_count: n, processes: Array.from({ length: n }, (_, i) => ({ id: "p" + i, spid: i + 1, x: 0, y: 0 })), edges: [], cycles: [] });
const WITH_GRAPH = (key, processes) => ({ ...DEADLOCK(key, processes), graph: GRAPH(processes.length) });
bodies.spread = {
  server: "SRV1",
  deadlocks: [
    WITH_GRAPH("s1", [PROC(1, "A"), PROC(2, "B"), PROC(3, "C")]),
    WITH_GRAPH("s2", [PROC(4, "A")]),
    WITH_GRAPH("s3", [PROC(5)]),
  ],
};

const all = (node, tag, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (node.tag === tag) found.push(node);
  node.children.forEach((c) => all(c, tag, found));
  return found;
};

/* One build of the tab, settled, and the Deadlock Graphs panel out of it. */
async function buildPanel(body) {
  answer = (url) => (tool(url) === "get_deadlock_detail" ? data(body) : data({}));
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "blocking");
  const nodes = tab.build("SRV1", { hours: 24, label: "last 24 hours" }).filter((n) => n && typeof n === "object");
  const root = new FakeNode("main");
  modules.util.mount(root, nodes);
  for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));
  const panel = root.children.find((n) => n.textContent.startsWith("Deadlock Graphs")) || null;
  return panel;
}

const toggle = (details, open) => {
  details.open = open;
  (details.listeners.toggle || []).forEach((l) => l());
};
const processDetails = (panel) => all(panel, "details").filter((d) => /process/.test(d.children[0].textContent));
const state = (panel) => {
  const found = processDetails(panel);
  return {
    summaries: found.map((d) => d.children[0].textContent),
    open: found.map((d) => d.open),
    subHeaders: found.map((d) => all(d, "th").map((th) => th.textContent)),
    subRows: found.map((d) => all(d, "tr").map((tr) => all(tr, "td").map((td) => td.textContent)).filter((c) => c.length)),
  };
};

const out = { fetches, rejections };
if (scenario === "closed") {
  const panel = await buildPanel(bodies.two);
  Object.assign(out, state(panel));
} else if (scenario === "expandSurvivesRebuild") {
  let panel = await buildPanel(bodies.two);
  toggle(processDetails(panel)[0], true);
  out.afterToggle = state(panel).open;
  panel = await buildPanel(bodies.two);
  out.afterRebuild = state(panel).open;
} else if (scenario === "collapseSurvivesRebuild") {
  let panel = await buildPanel(bodies.two);
  toggle(processDetails(panel)[1], true);
  toggle(processDetails(panel)[1], false);
  panel = await buildPanel(bodies.two);
  out.afterRebuild = state(panel).open;
} else if (scenario === "otherServerKeyed") {
  let panel = await buildPanel(bodies.two);
  toggle(processDetails(panel)[0], true);
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "blocking");
  answer = (url) => (tool(url) === "get_deadlock_detail" ? data(bodies.two) : data({}));
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("SRV2", { hours: 24, label: "x" }).filter((n) => n && typeof n === "object"));
  for (let i = 0; i < 200; i++) await new Promise((r) => setTimeout(r, 0));
  out.otherServerOpen = state(root.children.find((n) => n.textContent.startsWith("Deadlock Graphs"))).open;
} else if (scenario === "noprocesses") {
  const panel = await buildPanel(bodies.noprocesses);
  out.subgrids = processDetails(panel).length;
  out.text = panel.textContent;
} else if (scenario === "allcut") {
  const panel = await buildPanel(bodies.allcut);
  out.subgrids = processDetails(panel).length;
  out.text = panel.textContent;
} else if (scenario === "capped") {
  const panel = await buildPanel(bodies.capped);
  Object.assign(out, state(panel));
} else if (scenario.startsWith("spread:") || scenario.startsWith("clean:")) {
  /* spread:<names split on |>, or spread:none for no filter; clean: is the same over the two deadlocks whose processes are all in A or B. */
  const names = scenario.slice(scenario.indexOf(":") + 1);
  if (names !== "none") {
    globalThis.location = { hash: "#/server/SRV1/blocking" };
    modules.util.setActiveDatabaseFilter({ server: "SRV1", databases: names.split("|") });
  }
  const panel = await buildPanel(scenario.startsWith("clean:") ? { server: "SRV1", deadlocks: [bodies.spread.deadlocks[1], WITH_GRAPH("c1", [PROC(1, "A"), PROC(2, "B")])] } : bodies.spread);
  const procs = processDetails(panel).filter((d) => !/^Graph/.test(d.children[0].textContent));
  const st = {
    summaries: procs.map((d) => d.children[0].textContent),
    subRows: procs.map((d) => all(d, "tr").map((tr) => all(tr, "td").map((td) => td.textContent)).filter((c) => c.length)),
  };
  const classOf = (n) => n.className || "";
  const find = (node, pred, found = []) => {
    if (!node || typeof node !== "object") return found;
    if (pred(node)) found.push(node);
    node.children.forEach((c) => find(c, pred, found));
    return found;
  };
  out.summaries = st.summaries;
  out.subDatabases = st.subRows.map((rows) => rows.map((cells) => cells[3]));
  out.graphs = find(panel, (n) => n.tag === "details" && /^Graph/.test(n.children[0].textContent)).map((d) => d.children[0].textContent);
  out.cellTexts = find(panel, (n) => n.tag === "td" && /hidden by the database filter/.test(n.textContent)).map((n) => n.textContent);
  out.notes = find(panel, (n) => classOf(n) === "deadlock-rows-note").map((n) => n.textContent);
  const chips = find(panel, (n) => n.dataset && n.dataset.dbScope);
  out.chips = chips.map((c) => ({ state: c.dataset.dbScope, text: c.textContent, title: c.attrs.title }));
} else {
  throw new Error("unknown scenario " + scenario);
}
console.log(JSON.stringify(out));
