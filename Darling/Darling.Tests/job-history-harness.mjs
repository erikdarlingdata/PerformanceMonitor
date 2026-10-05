/* Runs the shipped Job History page (wwwroot/js/pages/job-history.js) against a recording fetch and a node-tree DOM, and
   prints as one line of JSON what it asked for and drew: the first build, a status pick, a typed job name, a rebuild
   (the 60 s poll), a one-server pick, the two notices, and the Agent line (stopped, running, unknown, a roll-up).
       node job-history-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.handlers = {};
    this.focused = false;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parent = this;
    this.children.push(child);
    return child;
  }
  // Like the browser, a removed subtree that holds the focused box fires blur DURING the removal, while it is still attached.
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i < 0) return child;
    const focused = [];
    const find = (n) => {
      if (n.focused && n.handlers.blur) focused.push(n);
      n.children.forEach(find);
    };
    find(child);
    for (const n of focused) {
      n.focused = false;
      n.handlers.blur();
    }
    this.children.splice(i, 1);
    child.parent = null;
    return child;
  }
  get isConnected() {
    let n = this;
    while (n.parent) n = n.parent;
    return n.isRoot === true;
  }
  get selectionStart() {
    return this.caret ? this.caret[0] : this.value.length;
  }
  get selectionEnd() {
    return this.caret ? this.caret[1] : this.value.length;
  }
  setSelectionRange(a, b) {
    this.caret = [a, b];
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, fn) {
    this.handlers[type] = fn;
  }
  focus() {
    this.focused = true;
  }
  closest() {
    return null;
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
let historyBody = "{}";
const serversBody = JSON.stringify({ servers: [{ server_name: "srv-a", display_name: "Server A", engine_kind: "sqlserver" }, { server_name: "srv-b" }, { server_name: "pg-c", engine_kind: "postgres" }, { server_name: "pg-d", engine_kind: "aurora-postgres" }] });
globalThis.fetch = async (url, init) => {
  fetches.push({ url: String(url), signal: init && init.signal ? true : false });
  const body = String(url).includes("/api/read/list_servers") ? serversBody : historyBody;
  return { status: 200, ok: true, text: async () => body };
};

const all = (node, tag, out = []) => {
  if (node.tag === tag) out.push(node);
  node.children.forEach((c) => all(c, tag, out));
  return out;
};
const byLabel = (root, label) => all(root, "select").concat(all(root, "input")).find((n) => n.attrs["aria-label"] === label);
const settle = () => new Promise((r) => setTimeout(r, 30));
const historyCalls = () =>
  fetches.filter((f) => f.url.includes("/api/read/get_job_history")).map((f) => ({ q: Object.fromEntries(new URL(f.url, "http://viewer.test").searchParams), signal: f.signal }));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "job-history-"));
try {
  fs.mkdirSync(path.join(scratch, "pages"), { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  /* Copy every module under js/ and js/pages/ rather than a hand-kept list: a new page module that server-tabs.js
     imports (another PR's pages/*.js) then needs no edit here. Only imported files load, so the rest are inert;
     charts.js is replaced by the stub below. */
  for (const dir of ["", "pages"]) {
    for (const f of fs.readdirSync(path.join(jsDir, dir))) {
      if (f.endsWith(".js")) fs.copyFileSync(path.join(jsDir, dir, f), path.join(scratch, dir, f));
    }
  }
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const { renderJobHistory, JOB_HISTORY_COLUMNS } = await import(pathToFileURL(path.join(scratch, "pages", "job-history.js")).href);

  const run = {
    runs: [
      { run_time: "2026-03-01T10:00:00Z", server: "srv-a", job_name: "Nightly", category: "Maintenance", step: "(Job outcome)", status: "Failed", duration_seconds: 5, duration_formatted: "00:00:05", retries: 0, last_success: null, message: "<b>boom</b>" },
      { run_time: "2026-03-01T09:00:00Z", server: "srv-b", job_name: "Backup", category: "Backup", step: "1: Full", status: "Succeeded", duration_seconds: 9, duration_formatted: "00:00:09", retries: 0, last_success: null, message: "" },
    ],
    shown: 2, truncated: false, window_truncated: false,
  };
  historyBody = JSON.stringify(run);

  const main = new FakeNode("div");
  main.isRoot = true;
  const out = {};
  const snapshot = (root) => ({
    calls: historyCalls(),
    selects: Object.fromEntries(["Server", "Window", "Status", "Category", "Rows"].map((l) => [l, byLabel(root, l).value])),
    job: byLabel(root, "Job name").value,
    jobFocused: byLabel(root, "Job name").focused,
    categories: byLabel(root, "Category").children.map((o) => o.attrs.value),
    servers: byLabel(root, "Server").children.map((o) => o.attrs.value),
    text: root.textContent,
    headers: all(root, "th").map((t) => t.textContent.replace(/[ ▲▼]+$/, "")),
    bodyRows: all(root, "tr").length - 1,
  });

  fetches.length = 0;
  renderJobHistory(main);
  await settle();
  out.first = snapshot(main);

  fetches.length = 0;
  byLabel(main, "Status").value = "Failed";
  byLabel(main, "Status").handlers.change();
  byLabel(main, "Category").value = "Maintenance";
  byLabel(main, "Category").handlers.change();
  const jobBox = byLabel(main, "Job name");
  jobBox.value = "Nightly";
  jobBox.handlers.input();
  jobBox.handlers.focus();
  await settle();
  out.picked = snapshot(main);

  // The poll rebuilds the page: the same choices, the typed text and the focus come back.
  fetches.length = 0;
  const again = new FakeNode("div");
  again.isRoot = true;
  renderJobHistory(again);
  await settle();
  out.rebuilt = snapshot(again);

  // One server, then back to the fleet.
  fetches.length = 0;
  byLabel(again, "Server").value = "srv-a";
  byLabel(again, "Server").handlers.change();
  await settle();
  out.oneServer = snapshot(again);
  fetches.length = 0;
  byLabel(again, "Server").value = "";
  byLabel(again, "Server").handlers.change();
  await settle();
  out.fleet = snapshot(again);

  // The poll rebuilds the page INTO THE SAME container while text is being typed: the old box leaves (the browser fires
  // blur during that removal), and the new box takes back the text, the focus and the caret, with one read.
  fetches.length = 0;
  const typing = byLabel(again, "Job name");
  typing.value = "Other";
  typing.caret = [2, 3];
  typing.handlers.input();
  typing.handlers.focus();
  typing.focused = true;
  renderJobHistory(again);
  await settle();
  const inPlace = byLabel(again, "Job name");
  out.inPlace = { calls: historyCalls(), job: inPlace.value, focused: inPlace.focused, caret: inPlace.caret || null };

  // The notices.
  historyBody = JSON.stringify({ ...run, truncated: true, window_truncated: true, effective_start: "2026-03-01T08:00:00Z" });
  const noticed = new FakeNode("div");
  noticed.isRoot = true;
  renderJobHistory(noticed);
  await settle();
  out.notices = { text: noticed.textContent, noticeCount: all(noticed, "div").filter((d) => d.className === "strip notice").length };

  // An empty answer carries its window facts under hints.
  historyBody = JSON.stringify({ status: "empty", message: "No job runs matched in the requested time range.", hints: { effective_start: "2026-03-01T08:00:00Z", window_truncated: true } });
  const emptyPartial = new FakeNode("div");
  emptyPartial.isRoot = true;
  renderJobHistory(emptyPartial);
  await settle();
  out.emptyPartial = { text: emptyPartial.textContent, noticeCount: all(emptyPartial, "div").filter((d) => d.className === "strip notice").length };

  // Row colours, the runs-shown line, and the server choices (SQL Server targets only).
  historyBody = JSON.stringify({ ...run, runs: [
    { ...run.runs[0], status: "Failed" }, { ...run.runs[1], status: "Retry" }, { ...run.runs[1], status: "Canceled" },
    { ...run.runs[1], status: "Succeeded", is_long_running: true }, { ...run.runs[1], status: "Succeeded", is_long_running: false } ] });
  const coloured = new FakeNode("div");
  coloured.isRoot = true;
  renderJobHistory(coloured);
  await settle();
  out.coloured = { rowClasses: all(coloured, "tbody").flatMap((t) => t.children.map((tr) => tr.attrs.class || tr.className || "")), text: coloured.textContent };

  historyBody = JSON.stringify({ ...run, truncated: false, window_truncated: false });
  const quiet = new FakeNode("div");
  quiet.isRoot = true;
  renderJobHistory(quiet);
  await settle();
  out.quiet = { noticeCount: all(quiet, "div").filter((d) => d.className === "strip notice").length };

  // The Agent line: a one-server answer's flat fields, the fleet's agents list, and an empty answer's hints.
  const agentLine = async (body) => {
    historyBody = JSON.stringify(body);
    const root = new FakeNode("div");
    root.isRoot = true;
    renderJobHistory(root);
    await settle();
    const line = all(root, "div").find((d) => String(d.className || "").startsWith("agent-line "));
    return line ? { text: line.textContent, cls: line.className } : null;
  };
  out.agent = {
    stopped: await agentLine({ ...run, server: "srv-a", agent_running: false, agent_status_desc: "Stopped" }),
    running: await agentLine({ ...run, server: "srv-a", agent_running: true, agent_status_desc: "Running" }),
    unknown: await agentLine({ ...run, server: "srv-a", agent_running: null, agent_status_desc: "unknown (no recent status)" }),
    rollup: await agentLine({ ...run, agents_total: 4, agents_running: 2, agents_not_running: [{ server: "srv-b", agent_running: false }, { server: "srv-d", agent_running: null }] }),
    allRunning: await agentLine({ ...run, agents_total: 2, agents_running: 2, agents_not_running: [] }),
    emptyStopped: await agentLine({ status: "empty", message: "No job runs matched in the requested time range.", hints: { effective_start: "2026-03-01T08:00:00Z", window_truncated: false, server: "srv-a", agent_running: false } }),
    emptyFleet: await agentLine({ status: "empty", message: "No job runs matched in the requested time range.", hints: { agents_total: 3, agents_running: 2, agents_not_running: [{ server: "srv-a", agent_running: false }] } }),
    noService: await agentLine({ ...run, server: "srv-x", agent_running: null, agent_status_desc: "no SQL Agent service found" }),
    rollupNoService: await agentLine({ ...run, agents_total: 2, agents_running: 1, agents_not_running: [{ server: "srv-x", agent_running: null, agent_status_desc: "no SQL Agent service found" }] }),
    rollupMixed: await agentLine({ ...run, agents_total: 3, agents_running: 1, agents_not_running: [{ server: "srv-b", agent_running: false, agent_status_desc: "Stopped" }, { server: "srv-x", agent_running: null, agent_status_desc: "no SQL Agent service found" }] }),
    emptyNoService: await agentLine({ status: "empty", message: "No job runs matched in the requested time range.", hints: { effective_start: "2026-03-01T08:00:00Z", window_truncated: false, server: "srv-x", agent_running: null, agent_status_desc: "no SQL Agent service found" } }),
    absent: await agentLine(run),
  };

  out.columns = JOB_HISTORY_COLUMNS.map((c) => c.label);
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
