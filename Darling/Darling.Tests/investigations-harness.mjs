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

/* The nodes the page called focus() on, so a scenario can tell whether the focus was moved into the reason box (#7987). */
const focusCalls = [];

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
  focus() {
    focusCalls.push(this);
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
/* The close writes (PUT .../close) the page sent, and a gate that holds the service's answer back while a scenario looks at the page in between. */
const closes = [];
let closeGate = null;
/* When set, the service's answer to the next close writes: a refusal such as a 409 with its sentence (#7987). */
let closeAnswer = null;
/* #8007: a gate that holds the detail reads (get_investigation_reports) open, and the signal of each read sent while it is set, so a scenario
   can drop the picked request while its detail read is still out and see whether the read was aborted. A held read whose signal is
   aborted rejects with an AbortError once the gate opens, as a real fetch does. */
let detailGate = null;
const detailSignals = [];
/* #8007: how many detail reads the stand-in has answered, so a scenario can tell that a read had answered before the request was dropped. */
let detailAnswers = 0;
/* #8007: a gate that holds the session read (/api/session) open, so a scenario can have a detail read answer while the session read it is
   paired with is still out. */
let sessionGate = null;
globalThis.fetch = async (url, init) => {
  const u = String(url);
  fetches.push(u);
  if (init && init.method === "PUT") {
    closes.push({ url: u, body: init.body ? JSON.parse(init.body) : null });
    if (closeGate) await closeGate;
    if (closeAnswer) return { status: closeAnswer.status, ok: false, text: async () => JSON.stringify(closeAnswer.body) };
    return { status: 200, ok: true, text: async () => JSON.stringify({ status: "closed" }) };
  }
  /* The seat the page learns from /api/session: can edit, but not signed in as an admin (#6265). */
  if (u.split("?")[0].endsWith("/api/session")) {
    if (sessionGate) await sessionGate;
    return { status: 200, ok: true, text: async () => JSON.stringify(bodies.session) };
  }
  if (u.split("?")[0] === "/api/agent-changes") {
    /* The service filters by ?request=<id> in SQL (#6979), so the stand-in returns only that request's rows and the page does no filtering. */
    const wanted = new URLSearchParams(u.split("?")[1] || "").get("request");
    const all = bodies.agentChanges;
    const body = wanted && all ? { ...all, changes: all.changes.filter((c) => String(c.request_id).toLowerCase() === wanted.toLowerCase()) } : all;
    return { status: 200, ok: true, text: async () => JSON.stringify(body) };
  }
  const tool = u.split("/api/read/")[1].split("?")[0];
  if (tool === "get_investigation_reports" && detailGate) {
    const signal = init && init.signal ? init.signal : null;
    detailSignals.push(signal);
    await detailGate;
    if (signal && signal.aborted) throw Object.assign(new Error("The operation was aborted."), { name: "AbortError" });
  }
  if (tool === "get_investigation_reports") detailAnswers++;
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
  const slotText = () => all(main, "div").filter((d) => d.className === "jh-detail").map((d) => d.textContent).join("");

  /* #8007: the first pick, with the page's first session read held. The detail read answers at once, so its fetch is past aborting, and the
     pair the pane waits on (the detail read and the session read) is still out when a State pick drops the request. Only the detail ticket
     can stop that answer: once the session read answers, the pane must still be empty. The State pick is then undone, so the list is as before. */
  let openSession;
  sessionGate = new Promise((r) => (openSession = r));
  detailAnswers = 0;
  rowsOf(main)[0].handlers.click({ type: "click" });
  await settle();
  const answeredBeforeDrop = detailAnswers;
  const paneWhileSessionOut = slotText();
  byLabel(main, "State").value = "claimed";
  byLabel(main, "State").handlers.change();
  await settle();
  const paneAfterDrop = slotText();
  openSession();
  sessionGate = null;
  await settle();
  out.droppedWhileSessionOut = {
    detailReads: calls("get_investigation_reports").length,
    answeredBeforeDrop,
    paneWhileSessionOut,
    paneAfterDrop,
    pane: slotText(),
  };
  byLabel(main, "State").value = "";
  byLabel(main, "State").handlers.change();
  await settle();

  // Picking a row reads that one request and draws its report, brief and journal.
  fetches.length = 0;
  rowsOf(main)[0].handlers.click({ type: "click" });
  await settle();
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

  /* #7700: the same seat on a store with web.allowAdminWrites off. The cached session is edited in place: the service refuses a close,
     so no Close form is drawn and the section says why. */
  const { getSession } = await import(pathToFileURL(path.join(scratch, "views-api.js")).href);
  const session = await getSession();
  Object.assign(session, { signed_in_admin: true, managed_store: true, admin_writes_off: true });
  rowsOf(main)[0].handlers.click({ type: "click" });
  await settle();
  const closeParts = () => ({
    offLines: all(main, "div").filter((d) => d.attrs["data-close-off"] === "1").map((d) => d.textContent),
    signInLines: all(main, "div").filter((d) => d.attrs["data-close-sign-in"] === "1").length,
    closeInputs: all(main, "input").filter((i) => i.attrs["data-close-reason"] === "1").length,
    closeButtons: all(main, "button").filter((b) => b.attrs["data-close-request"] != null).length,
  });
  out.writesOff = closeParts();
  session.admin_writes_off = false;
  rowsOf(main)[0].handlers.click({ type: "click" });
  await settle();
  out.writesOn = closeParts();
  session.signed_in_admin = false;
  session.managed_store = false;

  // A poll rebuild keeps the open request.
  fetches.length = 0;
  renderInvestigations(main);
  await settle();
  out.rebuilt = { reportCalls: calls("get_investigation_reports"), hasDetail: slotText().includes("Request on srv-new") };

  /* #7956: a signed-in admin types a close reason, and the poll rebuilds the page before the Close button is pressed: the new form
     holds the same text. Then a Close whose PUT is still out when the poll redraws the form: the new button is disabled, a press on it
     sends nothing more, and the answer (one PUT) clears the draft. */
  session.signed_in_admin = true;
  rowsOf(main)[0].handlers.click({ type: "click" });
  await settle();
  const reasonBox = () => all(main, "input").find((i) => i.attrs["data-close-reason"] === "1");
  const closeButton = () => all(main, "button").find((b) => b.attrs["data-close-request"] != null);
  reasonBox().value = "The blocker was a one-off import";
  if (reasonBox().handlers.input) reasonBox().handlers.input({ type: "input" });
  renderInvestigations(main);
  await settle();
  out.draft = { kept: reasonBox() ? reasonBox().value : null, boxes: all(main, "input").filter((i) => i.attrs["data-close-reason"] === "1").length };
  let release;
  closeGate = new Promise((r) => (release = r));
  closes.length = 0;
  closeButton().handlers.click({ type: "click" });
  await settle();
  renderInvestigations(main);
  await settle();
  const rebuiltButton = closeButton();
  out.closing = {
    url: closes[0] ? closes[0].url : null,
    reasonSent: closes[0] && closes[0].body ? closes[0].body.reason : null,
    putsBeforeReply: closes.length,
    rebuiltDisabled: rebuiltButton.disabled === true,
  };
  rebuiltButton.handlers.click({ type: "click" });
  await settle();
  out.closing.putsAfterSecondPress = closes.length;
  release();
  closeGate = null;
  await settle();
  out.closing.putsAfterReply = closes.length;
  out.closing.draftAfterClose = reasonBox() ? reasonBox().value : null;
  out.closing.buttonEnabledAfter = closeButton() ? closeButton().disabled !== true : null;

  /* #7987: request A's close is out when request B is picked and its reason typed. A's answer (closed) re-reads the list, and B's form
     is drawn again with the reason as typed: only A's own draft is dropped. */
  closeGate = new Promise((r) => (release = r));
  closes.length = 0;
  closeButton().handlers.click({ type: "click" });
  await settle();
  rowsOf(main)[1].handlers.click({ type: "click" });
  await settle();
  reasonBox().value = "The CPU spike was one report run by hand";
  reasonBox().handlers.input({ type: "input" });
  release();
  closeGate = null;
  await settle();
  out.otherClose = {
    closedUrl: closes[0] ? closes[0].url : null,
    puts: closes.length,
    paneRequest: closeButton() ? closeButton().attrs["data-close-request"] : null,
    reasonKept: reasonBox() ? reasonBox().value : null,
  };

  /* #7987: the service refuses the close (a 409 with its sentence) after the poll has redrawn the form: the rebuilt button is enabled
     again, the sentence is shown under it, the reason stays in the box, and the list is not re-read. */
  reasonBox().value = "The CPU spike was one report run by hand";
  reasonBox().handlers.input({ type: "input" });
  closeAnswer = { status: 409, body: { error: "This request was closed by someone else a moment ago." } };
  closeGate = new Promise((r) => (release = r));
  closes.length = 0;
  fetches.length = 0;
  closeButton().handlers.click({ type: "click" });
  await settle();
  renderInvestigations(main);
  await settle();
  out.refused = { rebuiltDisabled: closeButton().disabled === true, reasonSent: closes[0] && closes[0].body ? closes[0].body.reason : null };
  release();
  closeGate = null;
  await settle();
  closeAnswer = null;
  const statusLine = all(main, "div").find((d) => d.attrs["data-close-status"] === "1");
  out.refused.puts = closes.length;
  out.refused.buttonEnabledAfter = closeButton() ? closeButton().disabled !== true : null;
  out.refused.status = statusLine ? statusLine.textContent : null;
  out.refused.reasonKept = reasonBox() ? reasonBox().value : null;
  out.refused.listReads = calls("list_investigation_requests").length;

  /* #7987: the page is left (the route's beginPageScope) while the reason box has the focus, and opened again: the reason is still in
     the box, and the focus is not moved into it. The poll's rebuild on the page does move it back (#7956), which is the control. */
  const { beginPageScope } = await import(pathToFileURL(path.join(scratch, "util.js")).href);
  const boxFocuses = () => focusCalls.filter((n) => n.attrs["data-close-reason"] === "1").length;
  reasonBox().handlers.focus({ type: "focus" });
  focusCalls.length = 0;
  renderInvestigations(main);
  await settle();
  out.leftFocused = { pollRefocused: boxFocuses() };
  focusCalls.length = 0;
  beginPageScope();
  renderInvestigations(main);
  await settle();
  out.leftFocused.returnRefocused = boxFocuses();
  out.leftFocused.reasonKept = reasonBox() ? reasonBox().value : null;
  session.signed_in_admin = false;

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

  /* #7957: a Close under a State filter. The closed request no longer matches the filter, so the re-read's list does not hold it, and the
     pane that showed it must go: with another request still listed, with the read answering empty, and with the read answering no rows. */
  session.signed_in_admin = true;
  const C = "44444444-4444-4444-4444-444444444444";
  const rowB = list(false).requests[1];
  const rowC = { ...rowB, request_id: C, server_name: "srv-third" };
  const openList = (...rows) => ({ status: "ok", requests_returned: rows.length, truncated: false, requests: rows });
  bodies.list_investigation_requests = openList(rowB, rowC);
  byLabel(main, "Family").value = "";
  byLabel(main, "Family").handlers.change();
  byLabel(main, "State").value = "open";
  byLabel(main, "State").handlers.change();
  await settle();
  const closeUnderFilter = async (listBefore, listAfter) => {
    bodies.list_investigation_requests = listBefore;
    renderInvestigations(main);
    await settle();
    rowsOf(main)[0].handlers.click({ type: "click" });
    await settle();
    const paneBefore = slotText();
    bodies.list_investigation_requests = listAfter;
    closes.length = 0;
    fetches.length = 0;
    closeButton().handlers.click({ type: "click" });
    await settle();
    return {
      paneBefore,
      puts: closes.length,
      closedUrl: closes[0] ? closes[0].url : null,
      listCalls: calls("list_investigation_requests"),
      pane: slotText(),
      rows: rowsOf(main).map((r) => cells(r)[2]),
      emptyLine: main.textContent.includes("No investigation request matches these filters."),
    };
  };
  out.closedUnderFilter = await closeUnderFilter(openList(rowB, rowC), openList(rowC));
  out.closedLastUnderFilter = await closeUnderFilter(openList(rowB), { status: "empty", message: "No investigation request matches these filters." });
  out.closedNoRowsUnderFilter = await closeUnderFilter(openList(rowB), openList());
  session.signed_in_admin = false;
  bodies.list_investigation_requests = list(false);

  /* #8007: request A's detail read is still out when A is dropped: by a State pick (the filter change) and by a list re-read that no longer
     holds it (the poll's rebuild with A gone from the list). The read then answers. The pane must stay empty with no Close form for A, and
     the read's fetch must have been aborted when A was dropped. */
  session.signed_in_admin = true;
  const droppedDetail = async (drop) => {
    bodies.list_investigation_requests = list(false);
    bodies.get_investigation_reports = reportsA;
    renderInvestigations(main);
    await settle();
    let open;
    detailGate = new Promise((r) => (open = r));
    detailSignals.length = 0;
    rowsOf(main)[0].handlers.click({ type: "click" });
    await settle();
    const paneWhileOut = slotText();
    drop();
    await settle();
    const abortedBeforeAnswer = detailSignals.map((s) => (s ? s.aborted : false));
    open();
    detailGate = null;
    await settle();
    return {
      detailReads: detailSignals.length,
      paneWhileOut,
      abortedBeforeAnswer,
      pane: slotText(),
      closeButtons: all(main, "button").filter((b) => b.attrs["data-close-request"] != null).length,
    };
  };
  out.droppedByFilter = await droppedDetail(() => {
    byLabel(main, "State").value = "claimed";
    byLabel(main, "State").handlers.change();
  });
  out.droppedByReRead = await droppedDetail(() => {
    bodies.list_investigation_requests = openList(rowB, rowC);
    renderInvestigations(main);
  });
  session.signed_in_admin = false;
  bodies.list_investigation_requests = list(false);

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
    agentChangeUrls: fetches.filter((f) => f.split("?")[0] === "/api/agent-changes"),
    text: slotText(),
    links: all(main, "a").map((a) => a.attrs.href),
    closeButtons: all(main, "button").filter((b) => b.textContent === "Close").length,
  };
  /* #7700: the single-request view passes the switch sentence too. A signed-in admin on a store with the switch off, then a seat that is
     not signed in on the same store: both get the switch sentence (signing in would not help) and neither gets the sign-in sentence,
     the reason box or the Close button. */
  Object.assign(session, { signed_in_admin: true, managed_store: true, admin_writes_off: true });
  renderInvestigations(main, A);
  await settle();
  out.oneWritesOff = closeParts();
  session.signed_in_admin = false;
  renderInvestigations(main, A);
  await settle();
  out.oneWritesOffSignedOut = closeParts();
  Object.assign(session, { managed_store: false, admin_writes_off: false });
  // An id that is not a request says so plainly, and draws no report.
  bodies.get_investigation_reports = { status: "not_found", message: "No investigation request has id 99999999-9999-9999-9999-999999999999." };
  renderInvestigations(main, "99999999-9999-9999-9999-999999999999");
  await settle();
  out.oneMissing = slotText();
  // A request with no change says so, and still links to the tab.
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
