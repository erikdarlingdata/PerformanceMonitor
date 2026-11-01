/* Runs the shipped Investigations page (wwwroot/js/pages/investigations.js) against a recording fetch and a node-tree DOM,
   and prints as one line of JSON what it asked for and drew: the first list, a state pick, a picked request's report, brief
   and journal (markup in the data stays text), a rebuild (the 60 s poll) that keeps the open request, the empty and error
   lines, and the truncation notices.
       node investigations-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { withDocumentListeners } from "./web-picker-driver.mjs";

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
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    child.parent = null;
    return child;
  }
  get isConnected() {
    let n = this;
    while (n.parent) n = n.parent;
    return n.isRoot === true;
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
  focus() {}
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
const created = [];
globalThis.document = withDocumentListeners({
  createElement: (tag) => {
    created.push(tag);
    return new FakeNode(tag);
  },
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
});

const fetches = [];
const bodies = { session: { can_edit: true, signed_in_admin: false } };
globalThis.fetch = async (url, init) => {
  const u = String(url);
  fetches.push(u);
  /* The seat the page learns from /api/session: can edit, but not signed in as an admin (#6265). */
  if (u.split("?")[0].endsWith("/api/session")) return { status: 200, ok: true, text: async () => JSON.stringify(bodies.session) };
  if (u.split("?")[0] === "/api/agent-changes") return { status: 200, ok: true, text: async () => JSON.stringify(bodies.agentChanges) };
  const tool = u.split("/api/read/")[1].split("?")[0];
  const b = bodies[tool];
  if (b && b.fail) return { status: 500, ok: false, text: async () => JSON.stringify({ error: "the store is down" }) };
  return { status: 200, ok: true, text: async () => JSON.stringify(b) };
};

const all = (node, tag, out = []) => {
  if (node.tag === tag) out.push(node);
  node.children.forEach((c) => all(c, tag, out));
  return out;
};
const byLabel = (root, label) => all(root, "select").find((n) => n.attrs["aria-label"] === label);
const settle = () => new Promise((r) => setTimeout(r, 30));
const calls = (tool) =>
  fetches.filter((f) => f.includes("/api/read/" + tool)).map((f) => Object.fromEntries(new URL(f, "http://viewer.test").searchParams));
const rowsOf = (root, id) => all(root, "tbody").flatMap((t) => t.children);
const cells = (tr) => tr.children.map((c) => c.textContent);

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "investigations-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const mod = await import(pathToFileURL(path.join(scratch, "pages", "investigations.js")).href);
  const { renderInvestigations, REQUEST_COLUMNS, JOURNAL_COLUMNS, claimedAt, listParams, requestIdFromQuery } = mod;

  const A = "11111111-1111-1111-1111-111111111111";
  const B = "22222222-2222-2222-2222-222222222222";
  const list = (truncated) => ({
    status: "ok",
    requests_returned: 2,
    truncated,
    requests: [
      { request_id: A, route_id: 3, server_name: "srv-new", family: "blocking", first_metric: "Blocking", alert_count: 4, state: "reported",
        opened_at: "2026-10-14T10:00:00Z", expires_at: "2026-10-14T14:00:00Z", claimed_by: "agent-a", lease_until: "2026-10-14T10:20:00Z",
        claim_count: 1, reported_at: "2026-10-14T10:12:00Z" },
      { request_id: B, route_id: 3, server_name: "srv-old", family: "resources", first_metric: "High CPU", alert_count: 1, state: "open",
        opened_at: "2026-10-14T09:00:00Z", expires_at: "2026-10-14T13:00:00Z", claimed_by: null, lease_until: null, claim_count: 0, reported_at: null },
    ],
  });
  const reportsA = {
    status: "ok", requests_returned: 1, truncated: false, journal_truncated: false,
    requests: [],
    reports: [
      { request_id: A, report_format: 1, agent_id: "agent-a", submitted_at: "2026-10-14T10:12:00Z", verdict: "needs_person", root_cause: "ROOTCAUSE <b>bold</b>",
        root_cause_found: false, summary: "SUMMARY <img src=x onerror=alert(1)>", evidence: [{ tool: "get_blocking", key_numbers: "head blocker 412 s" }],
        labels: ["unreviewed", "self_attributed"] },
    ],
    journal: [
      { event_time: "2026-10-14T10:12:00Z", request_id: A, server_id: 7, event: "reported", actor: "agent:agent-a", reason: null, detail: null },
      { event_time: "2026-10-14T10:05:00Z", request_id: A, server_id: 7, event: "claimed", actor: "agent:agent-a", reason: null, detail: { claim: 1 } },
      { event_time: "2026-10-14T10:00:00Z", request_id: A, server_id: 7, event: "opened", actor: "service", reason: "<script>x</script>", detail: null },
    ],
    brief: { brief_format: 1, data_detail: "maskLiterals", request: { id: A, server: "srv-new" }, alerts: [{ metric: "Blocking" }] },
  };
  bodies.list_investigation_requests = list(false);
  bodies.get_investigation_reports = reportsA;

  const main = new FakeNode("div");
  main.isRoot = true;
  const out = {};

  fetches.length = 0;
  renderInvestigations(main);
  await settle();
  out.first = {
    listCalls: calls("list_investigation_requests"),
    reportCalls: calls("get_investigation_reports"),
    headers: all(main, "th").map((t) => t.textContent.replace(/[ ▲▼]+$/, "")),
    rows: rowsOf(main).map(cells),
    selects: { State: byLabel(main, "State").value, Family: byLabel(main, "Family").value },
    stateChoices: byLabel(main, "State").children.map((o) => o.attrs.value),
  };

  // Picking a row reads that one request and draws its report, brief and journal.
  fetches.length = 0;
  rowsOf(main)[0].handlers.click({ type: "click" });
  await settle();
  const slotText = () => all(main, "div").filter((d) => d.className === "jh-detail").map((d) => d.textContent).join("");
  out.picked = {
    reportCalls: calls("get_investigation_reports"),
    text: slotText(),
    pre: all(main, "pre").map((p) => p.textContent),
    journalRows: all(main, "tbody").pop().children.map(cells),
    journalHeaders: JOURNAL_COLUMNS.map((c) => c.label),
    createdImgOrScript: created.filter((t) => t === "img" || t === "script").length,
    /* A seat that can edit but is not signed in as an admin sees the sign-in sentence in place of the Close form (#6265). */
    signInLines: all(main, "div").filter((d) => d.attrs["data-close-sign-in"] === "1").map((d) => d.textContent),
    closeInputs: all(main, "input").filter((i) => i.attrs["data-close-reason"] === "1").length,
    closeButtons: all(main, "button").filter((b) => b.attrs["data-close-request"] != null).length,
  };

  // A poll rebuild keeps the open request.
  fetches.length = 0;
  renderInvestigations(main);
  await settle();
  out.rebuilt = { reportCalls: calls("get_investigation_reports"), hasDetail: slotText().includes("Request on srv-new") };

  // Close empties the pane.
  all(main, "button").find((b) => b.textContent === "Close").handlers.click();
  out.closed = slotText();

  // A state pick sends the state to the read and drops the open request.
  fetches.length = 0;
  byLabel(main, "State").value = "claimed";
  byLabel(main, "State").handlers.change();
  byLabel(main, "Family").value = "blocking";
  byLabel(main, "Family").handlers.change();
  await settle();
  out.filtered = { listCalls: calls("list_investigation_requests"), reportCalls: calls("get_investigation_reports"), pane: slotText() };

  // A request with no report says so, and the unreviewed notice is not shown for it.
  bodies.get_investigation_reports = { status: "ok", requests: [], reports: [], journal: [], journal_truncated: true, brief: null };
  rowsOf(main)[1].handlers.click({ type: "click" });
  await settle();
  out.noReport = slotText();

  // The notices and the empty and error lines.
  bodies.list_investigation_requests = list(true);
  renderInvestigations(main);
  await settle();
  out.truncated = main.textContent.includes("Showing the newest 2 requests");
  bodies.list_investigation_requests = { status: "empty", message: "No investigation request matches these filters." };
  renderInvestigations(main);
  await settle();
  out.empty = main.textContent.includes("No investigation request matches these filters.");
  bodies.list_investigation_requests = { fail: true };
  renderInvestigations(main);
  await settle();
  out.error = main.textContent.includes("the store is down");

  // #6979: #/investigations?request=<id> opens one request: its report and journal and only its own agent changes, without the list.
  bodies.get_investigation_reports = { ...reportsA, requests: [list(false).requests[0]] };
  bodies.agentChanges = {
    enabled: true, can_decide: false, refusals: [],
    changes: [
      { called_id: 5, called_at_utc: "2026-10-14T10:08:00Z", request_id: A.toUpperCase(), tool: "set_config", kind: "config", result: "done", undone_by: null },
      { called_id: 6, called_at_utc: "2026-10-14T09:08:00Z", request_id: "33333333-3333-3333-3333-333333333333", tool: "other_tool", kind: "config", result: "failed", undone_by: null },
    ],
  };
  fetches.length = 0;
  renderInvestigations(main, A);
  await settle();
  out.one = {
    listCalls: calls("list_investigation_requests"),
    reportCalls: calls("get_investigation_reports"),
    agentChangeReads: fetches.filter((f) => f.split("?")[0] === "/api/agent-changes").length,
    text: slotText(),
    links: all(main, "a").map((a) => a.attrs.href),
    closeButtons: all(main, "button").filter((b) => b.textContent === "Close").length,
  };
  // An id that is not a request says so plainly, and draws no report.
  bodies.get_investigation_reports = { status: "not_found", message: "No investigation request has id 99999999-9999-9999-9999-999999999999." };
  renderInvestigations(main, "99999999-9999-9999-9999-999999999999");
  await settle();
  out.oneMissing = slotText();
  // A request with no change in the newest calls says so, and still links to the tab.
  bodies.get_investigation_reports = { ...reportsA, requests: [list(false).requests[1]] };
  renderInvestigations(main, B);
  await settle();
  out.oneNoChange = { text: slotText(), links: all(main, "a").map((a) => a.attrs.href) };
  out.queryIds = [requestIdFromQuery("request=" + A), requestIdFromQuery("request=%20" + B + "%20&x=1"), requestIdFromQuery(""), requestIdFromQuery("request="), requestIdFromQuery("other=1")];

  out.columns = REQUEST_COLUMNS.map((c) => c.key);
  out.claimedAt = claimedAt(reportsA.journal, A);
  out.claimedAtNever = claimedAt([{ event: "opened", event_time: "2026-10-14T10:00:00Z" }], A);
  out.params = { none: listParams({ state: "", family: "" }), both: listParams({ state: "open", family: "jobs" }) };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
