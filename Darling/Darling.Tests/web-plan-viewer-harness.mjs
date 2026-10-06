/* #4843: runs the shipped stored-plan viewer (wwwroot/js/pages/plan-viewer.js with util.js and grid-tools.js) on a small
   fake DOM and prints what a scenario did as one line of JSON. PlanViewerBehaviourTests starts it as
       node web-plan-viewer-harness.mjs <path to the js folder> <scenario>
   Only the DOM, fetch, the clipboard and the download plumbing are stand-ins. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.style = {}; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  click() { for (const fn of this.listeners.click || []) fn({ preventDefault() {}, target: this }); }
  all(pred, acc = []) { if (pred(this)) acc.push(this); for (const c of this.children) c.all && c.all(pred, acc); return acc; }
  byText(t) { return this.all((n) => n.tag === "button" && n.textContent === t)[0] || null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
const downloads = [];
globalThis.document = { body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
URL.createObjectURL = (blob) => { downloads.push({ blob }); return "blob:fake"; };
URL.revokeObjectURL = () => {};
const origAppend = body.appendChild.bind(body);
body.appendChild = (a) => { if (a.tag === "a") a.click = () => { downloads[downloads.length - 1].name = a.download; }; return origAppend(a); };
const clip = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clip.push(t); } } }, configurable: true, writable: true });
globalThis.location = { hash: "#/server/a/queries" };

const fetches = [];
let reply = { status: 200, body: "{}" };
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  return { status: reply.status, ok: reply.status < 400, text: async () => reply.body };
};
/* What the read route answers for a stored plan: 200 JSON with the XML and a server-decided truncated flag. */
const planReply = (xml, truncated = false) => { reply = { status: 200, body: JSON.stringify({ query_hash: "H1", database_name: null, plan_xml: xml, truncated }) }; };
const noPlanReply = () => { reply = { status: 200, body: JSON.stringify({ status: "unavailable", message: "No stored plan found for query_hash 'H1'." }) }; };

const root = process.argv[2];
const viewer = await import(pathToFileURL(root + "/pages/plan-viewer.js").href);
const flush = () => new Promise((r) => setTimeout(r, 5));
const out = {};
const row = { query_hash: "0xABCDEF0123456789", database_name: "Orders", query_text: "select 1" };
const XML = '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan"><BatchSequence><Batch><Statements><StmtSimple StatementText="select * from t where a &gt; 1 and b &lt; &quot;&lt;script&gt;&quot;"><QueryPlan CachedPlanSize="16"><RelOp NodeId="0" PhysicalOp="Index Scan"/></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>';
const pre = (cell) => cell.all((n) => n.tag === "pre")[0] || null;
const params = () => new URL(fetches[fetches.length - 1], "http://viewer.test");

const scenarios = {
  async open() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    out.buttonBefore = cell.byText("Plan") !== null;
    cell.byText("Plan").click();
    out.loading = cell.textContent;
    await flush();
    const u = params();
    out.path = u.pathname;
    out.query = Object.fromEntries(u.searchParams);
    out.pre = pre(cell).textContent;
    out.preTag = pre(cell).tag;
    out.hasCopy = cell.byText("Copy") !== null;
    out.hasDownload = cell.byText("Download .sqlplan") !== null;
    out.noHashCell = viewer.storedPlanCell("srv-a", { query_hash: null }).textContent;
    cell.byText("Hide plan").click();
    out.closed = pre(cell) === null;
  },
  async textOnly() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    const p = pre(cell);
    out.preChildren = p.children.length;
    out.scriptElements = cell.all((n) => n.tag === "script").length;
    out.hasHtmlAttr = cell.all((n) => "innerHTML" in n && n.innerHTML !== undefined).length;
    out.text = p.textContent;
    out.lines = p.textContent.split("\n").length;
    out.pretty = viewer.prettyPrintXml('<a><b x="1>2">t</b><c/><d><e/></d></a>');
    out.notXml = viewer.prettyPrintXml("not xml <at all");
  },
  async download() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    cell.byText("Download .sqlplan").click();
    const d = downloads[downloads.length - 1];
    out.name = d.name;
    out.type = d.blob.type;
    out.body = await d.blob.text();
    out.bodyIsStored = out.body === XML;
    out.safeName = viewer.planFileName("a/b c");
  },
  async copy() {
    planReply(XML);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    cell.byText("Copy").click();
    await flush();
    out.copied = clip[0];
    out.matchesShown = clip[0] === pre(cell).textContent;
  },
  async truncated() {
    planReply(XML.slice(0, 120), true);
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    const dl = cell.byText("Download .sqlplan");
    out.disabled = dl.attrs.disabled !== undefined && dl.disabled === true;
    dl.click();
    out.downloads = downloads.length;
    out.notice = cell.all((n) => n.className === "strip notice").map((n) => n.textContent)[0];
    out.shown = pre(cell).textContent.includes("(truncated)");
    out.copyOn = cell.byText("Copy") !== null && cell.byText("Copy").attrs.disabled === undefined;
  },
  async noPlan() {
    noPlanReply();
    const cell = viewer.storedPlanCell("srv-a", row);
    cell.byText("Plan").click();
    await flush();
    out.text = cell.textContent;
    out.hasPre = pre(cell) !== null;
    out.hasDownload = cell.byText("Download .sqlplan") !== null;
  },
  async rebuild() {
    planReply(XML);
    const first = viewer.storedPlanCell("srv-a", row);
    first.byText("Plan").click();
    await flush();
    const before = fetches.length;
    /* The 60 s rebuild draws a brand-new cell for the same row. */
    const second = viewer.storedPlanCell("srv-a", row);
    out.open = second.byText("Hide plan") !== null;
    out.showsPlan = pre(second) !== null && pre(second).textContent.includes("RelOp");
    out.refetched = fetches.length - before;
    const other = viewer.storedPlanCell("srv-b", row);
    out.otherServerOpen = other.byText("Hide plan") !== null;
    /* A rebuild while the read is still out: the new cell fills in when it lands. */
    viewer.resetPlanViewer();
    let release;
    const gate = new Promise((r) => { release = r; });
    globalThis.fetch = async () => { await gate; return { status: 200, ok: true, text: async () => JSON.stringify({ query_hash: "H1", database_name: null, plan_xml: XML, truncated: false }) }; };
    const c1 = viewer.storedPlanCell("srv-a", row);
    c1.byText("Plan").click();
    const c2 = viewer.storedPlanCell("srv-a", row);
    out.loadingInNew = c2.textContent.includes("Loading");
    release();
    await flush();
    out.filledNew = pre(c2) !== null;
    out.keys = viewer.openPlanKeys();
  },
  async kinds() {
    planReply(XML);
    const snap = { collection_time: "2026-03-04T05:06:07.1234560", session_id: 57, request_id: 3, has_query_plan: true, has_live_query_plan: true };
    const cols = viewer.activePlanColumns("srv-a");
    out.activeKeys = cols.map((c) => c.key);
    out.activeLabels = cols.map((c) => c.label);
    out.planColsNoFilter = [...cols, viewer.queryStorePlanColumn("srv-a"), viewer.procedurePlanColumn("srv-a")].every((c) => c.filter === false);
    /* An open panel's XML is the cell's text, so every plan column also stays out of the column filter and Copy row / Copy all. */
    out.planColsNoFilterNoCopy = [...cols, viewer.queryStorePlanColumn("srv-a"), viewer.procedurePlanColumn("srv-a"), viewer.planColumn("srv-a"),
      ...viewer.blockingPlanColumns("srv-a"), viewer.deadlockPlanColumn("srv-a")]
      .every((c) => c.filter === false && c.copy === false && c.csv === false);
    out.activeHide = cols.every((c) => c.hideWhenEmpty === true && c.sortable === false && c.csv === false);
    const est = cols[0].render(snap);
    est.byText("Plan").click();
    await flush();
    let u = params();
    out.activePath = u.pathname;
    out.activeQuery = Object.fromEntries(u.searchParams);
    const live = cols[1].render({ ...snap, request_id: null });
    live.byText("Live plan").click();
    await flush();
    u = params();
    out.liveQuery = Object.fromEntries(u.searchParams);
    out.noFlagCell = cols[0].render({ ...snap, has_query_plan: undefined }).textContent;
    out.noLiveFlagCell = cols[1].render({ ...snap, has_live_query_plan: undefined }).textContent;

    const qs = viewer.queryStorePlanColumn("srv-a");
    out.qsKey = qs.key;
    const qsCell = qs.render({ database_name: "Orders", query_id: 42, plan_id: 7 });
    qsCell.byText("Plan").click();
    await flush();
    u = params();
    out.qsPath = u.pathname;
    out.qsQuery = Object.fromEntries(u.searchParams);
    const qsNoPlanId = qs.render({ database_name: "Orders", query_id: 42, plan_id: null });
    qsNoPlanId.byText("Plan").click();
    await flush();
    out.qsNoPlanIdQuery = Object.fromEntries(params().searchParams);
    out.qsNoKey = qs.render({ database_name: "Orders", query_id: null }).textContent;

    const pc = viewer.procedurePlanColumn("srv-a");
    out.procKey = pc.key;
    out.procHide = pc.hideWhenEmpty === true;
    const pCell = pc.render({ sql_handle: "0x0300050011223344" });
    pCell.byText("Plan").click();
    await flush();
    u = params();
    out.procPath = u.pathname;
    out.procQuery = Object.fromEntries(u.searchParams);
    out.procNoHandle = pc.render({ sql_handle: null }).textContent;

    /* Blocking (#5236): two columns, each gated on its own flag. The query string is the row's own strings, in the
       order the read documents: the blocked side sends no `side`, the blocking side sends side=blocking, and a row with
       no ecid reads as 0. */
    const bp = viewer.blockingPlanColumns("srv-a");
    out.blockingKeys = bp.map((c) => c.key);
    out.blockingLabels = bp.map((c) => c.label);
    out.blockingHide = bp.every((c) => c.hideWhenEmpty === true && c.sortable === false && c.csv === false);
    const brow = { event_time: "2026-03-04T05:06:07.1234567", blocked_spid: 57, blocked_ecid: 1, blocking_spid: 61, blocking_ecid: 2, has_blocked_plan: true, has_blocking_plan: true };
    bp[0].render(brow).byText("Blocked plan").click();
    await flush();
    u = params();
    out.blockedPath = u.pathname;
    out.blockedSearch = u.search;
    bp[1].render(brow).byText("Blocking plan").click();
    await flush();
    out.blockingSearch = params().search;
    bp[0].render({ ...brow, blocked_ecid: null, blocking_ecid: undefined, event_time: "2026-03-04T05:06:07.1234560" }).byText("Blocked plan").click();
    await flush();
    out.blockedNoEcidSearch = params().search;
    /* The row's database_name goes along when it has one (last), and a row without one sends none (the searches above). */
    bp[0].render({ ...brow, database_name: "Orders" }).byText("Blocked plan").click();
    await flush();
    out.blockedDbSearch = params().search;

    /* Deadlocks (#5236): the victim's plan, by the two stamps and the victim's process id; a row with no victim id
       leaves it off. */
    const dp = viewer.deadlockPlanColumn("srv-a");
    out.deadlockKey = dp.key;
    out.deadlockLabel = dp.label;
    out.deadlockHide = dp.hideWhenEmpty === true && dp.sortable === false && dp.csv === false;
    const drow = { collection_time: "2026-03-04T05:06:08.5000000", deadlock_time: "2026-03-04T05:06:07.1230000", victim_process_id: "process1a2b", has_victim_plan: true };
    dp.render(drow).byText("Victim plan").click();
    await flush();
    u = params();
    out.deadlockPath = u.pathname;
    out.deadlockSearch = u.search;
    dp.render({ ...drow, victim_process_id: "" }).byText("Victim plan").click();
    await flush();
    out.deadlockNoVictimSearch = params().search;
    dp.render({ ...drow, database_name: "Orders" }).byText("Victim plan").click();
    await flush();
    out.deadlockDbSearch = params().search;

    out.keys = viewer.openPlanKeys();
    out.keysUnique = new Set(out.keys).size === out.keys.length;
  },
  async kindsDoNotShareAPanel() {
    planReply(XML);
    /* Two different sources whose key parts look alike must each own a panel. */
    const a = viewer.planSourceCell("srv-a", { kind: "procedure", sql_handle: "42" });
    const b = viewer.planSourceCell("srv-a", { kind: "query_store", database_name: "42", query_id: 1, plan_id: null });
    a.byText("Plan").click();
    out.aOpen = a.byText("Hide plan") !== null;
    out.bOpenAfterA = b.byText("Hide plan") !== null;
    await flush();
    const est = viewer.planSourceCell("srv-a", { kind: "active_snapshot", collection_time: "T", session_id: 1, request_id: 0, live: false });
    const live = viewer.planSourceCell("srv-a", { kind: "active_snapshot", collection_time: "T", session_id: 1, request_id: 0, live: true });
    est.byText("Plan").click();
    out.liveOpenAfterEst = live.byText("Hide plan") !== null;
    await flush();
    out.keys = viewer.openPlanKeys();
    /* #5236: one blocked process report row holds two plans, the blocked side's and the blocking side's, and they are two
       panels. Two deadlocks that carry the same stamps but name different victims are two panels as well. */
    const rowKey = { kind: "blocking", event_time: "T", blocked_spid: 1, blocked_ecid: 0, blocking_spid: 2, blocking_ecid: 0 };
    const blocked = viewer.planSourceCell("srv-a", { ...rowKey, side: "blocked" });
    const blocking = viewer.planSourceCell("srv-a", { ...rowKey, side: "blocking" });
    blocked.byText("Plan").click();
    out.blockingOpenAfterBlocked = blocking.byText("Hide plan") !== null;
    await flush();
    blocking.byText("Plan").click();
    out.bothSidesOpen = blocked.byText("Hide plan") !== null && blocking.byText("Hide plan") !== null;
    await flush();
    out.sideKeys = viewer.openPlanKeys().filter((k) => k.includes("@blocking"));
    const stamps = { kind: "deadlock_victim", collection_time: "C", deadlock_time: "D" };
    const victim1 = viewer.planSourceCell("srv-a", { ...stamps, victim_process_id: "process1" });
    const victim2 = viewer.planSourceCell("srv-a", { ...stamps, victim_process_id: "process2" });
    victim1.byText("Plan").click();
    out.victim2OpenAfterVictim1 = victim2.byText("Hide plan") !== null;
    await flush();
    victim2.byText("Plan").click();
    out.bothVictimsOpen = victim1.byText("Hide plan") !== null && victim2.byText("Hide plan") !== null;
    await flush();
    out.victimKeys = viewer.openPlanKeys().filter((k) => k.includes("@deadlock_victim"));
    /* #5236: the database is part of the key. The same report key, and the same deadlock stamps and victim, in two databases are
       two panels, and the row's own database is the one in each key. */
    const reportKey = { kind: "blocking", event_time: "T", blocked_spid: 1, blocked_ecid: 0, blocking_spid: 2, blocking_ecid: 0, side: "blocked" };
    const inOne = viewer.planSourceCell("srv-a", { ...reportKey, database_name: "DbOne" });
    const inTwo = viewer.planSourceCell("srv-a", { ...reportKey, database_name: "DbTwo" });
    inOne.byText("Plan").click();
    out.dbTwoOpenAfterDbOne = inTwo.byText("Hide plan") !== null;
    await flush();
    inTwo.byText("Plan").click();
    out.bothDatabasesOpen = inOne.byText("Hide plan") !== null && inTwo.byText("Hide plan") !== null;
    await flush();
    out.databaseKeys = viewer.openPlanKeys().filter((k) => k.includes("DbOne") || k.includes("DbTwo"));
    const sameVictim = { kind: "deadlock_victim", collection_time: "C", deadlock_time: "D", victim_process_id: "process1" };
    const victimInOne = viewer.planSourceCell("srv-a", { ...sameVictim, database_name: "DbOne" });
    const victimInTwo = viewer.planSourceCell("srv-a", { ...sameVictim, database_name: "DbTwo" });
    victimInOne.byText("Plan").click();
    out.victimDbTwoOpenAfterDbOne = victimInTwo.byText("Hide plan") !== null;
    await flush();
    out.hashKey = (() => { viewer.resetPlanViewer(); viewer.openStoredPlan("srv-a", "0xABC", "Orders"); return viewer.openPlanKeys()[0]; })();
  },
  async stems() {
    planReply(XML);
    const cells = [
      viewer.planSourceCell("s", { kind: "active_snapshot", collection_time: "2026-03-04T05:06:07.1234560", session_id: 57, request_id: 0, live: true }),
      viewer.planSourceCell("s", { kind: "query_store", database_name: "Orders", query_id: 42, plan_id: 7 }),
      viewer.planSourceCell("s", { kind: "procedure", sql_handle: "0x03000500AA" }),
      /* #5236: the blocked and blocking sides of one report, then a deadlock's victim. */
      viewer.planSourceCell("s", { kind: "blocking", event_time: "2026-03-04T05:06:07.1234567", blocked_spid: 57, blocked_ecid: 0, blocking_spid: 61, blocking_ecid: 0, side: "blocked" }),
      viewer.planSourceCell("s", { kind: "blocking", event_time: "2026-03-04T05:06:07.1234567", blocked_spid: 57, blocked_ecid: 0, blocking_spid: 61, blocking_ecid: 0, side: "blocking" }),
      viewer.planSourceCell("s", { kind: "deadlock_victim", collection_time: "2026-03-04T05:06:08.5000000", deadlock_time: "2026-03-04T05:06:07.1230000", victim_process_id: "process1a2b" }),
    ];
    out.names = [];
    for (const c of cells) {
      c.byText("Plan").click();
      await flush();
      c.byText("Download .sqlplan").click();
      out.names.push(downloads[downloads.length - 1].name);
    }
  },
  async rebuildKinds() {
    planReply(XML);
    const src = { kind: "query_store", database_name: "Orders", query_id: 42, plan_id: 7 };
    const first = viewer.planSourceCell("srv-a", src);
    first.byText("Plan").click();
    await flush();
    const before = fetches.length;
    const second = viewer.planSourceCell("srv-a", src);
    out.open = second.byText("Hide plan") !== null;
    out.showsPlan = pre(second) !== null;
    out.refetched = fetches.length - before;

    /* #5236: the blocking and deadlock columns' panels survive the 60 s rebuild the same way (each rebuild draws the
       row's cells again from the row). */
    const bcol = viewer.blockingPlanColumns("srv-a")[1];
    const dcol = viewer.deadlockPlanColumn("srv-a");
    const brow = { event_time: "2026-03-04T05:06:07.1234567", blocked_spid: 57, blocked_ecid: 0, blocking_spid: 61, blocking_ecid: 0, has_blocking_plan: true };
    const drow = { collection_time: "2026-03-04T05:06:08.5000000", deadlock_time: "2026-03-04T05:06:07.1230000", victim_process_id: "process1a2b", has_victim_plan: true };
    bcol.render(brow).byText("Blocking plan").click();
    dcol.render(drow).byText("Victim plan").click();
    await flush();
    const beforeRows = fetches.length;
    const blockingAgain = bcol.render(brow);
    const victimAgain = dcol.render(drow);
    out.blockingOpen = blockingAgain.byText("Hide plan") !== null;
    out.blockingShowsPlan = pre(blockingAgain) !== null && pre(blockingAgain).textContent.includes("RelOp");
    out.victimOpen = victimAgain.byText("Hide plan") !== null;
    out.victimShowsPlan = pre(victimAgain) !== null && pre(victimAgain).textContent.includes("RelOp");
    out.refetchedRows = fetches.length - beforeRows;
  },
  async noPlanKind() {
    reply = { status: 200, body: JSON.stringify({ status: "unavailable", message: "No stored Query Store plan found for query_id 42 in database 'Orders'." }) };
    const c = viewer.planSourceCell("srv-a", { kind: "query_store", database_name: "Orders", query_id: 42, plan_id: null });
    c.byText("Plan").click();
    await flush();
    out.text = c.textContent;
    out.hasPre = pre(c) !== null;
  },
  async nullSource() {
    out.cell = viewer.planSourceCell("srv-a", null).textContent;
    /* #5236: a blocking or deadlock row without its presence flag (a DMV row never has one, nor does a report whose plans
       were not in the cache) is a dash, and so is a flagged row missing a value the read keys on. */
    const bp = viewer.blockingPlanColumns("srv-a");
    const dp = viewer.deadlockPlanColumn("srv-a");
    const bRow = { event_time: "2026-03-04T05:06:07.1234567", blocked_spid: 57, blocked_ecid: 0, blocking_spid: 61, blocking_ecid: 0 };
    const dRow = { collection_time: "2026-03-04T05:06:08.5000000", deadlock_time: "2026-03-04T05:06:07.1230000", victim_process_id: "process1a2b" };
    out.dmvCells = bp.map((c) => c.render({ ...bRow, source: "dmv", event_time: null }).textContent);
    out.unflaggedBlockingCells = bp.map((c) => c.render(bRow).textContent);
    out.unflaggedVictimCell = dp.render(dRow).textContent;
    out.noRowCells = [bp[0].render(null).textContent, bp[1].render(undefined).textContent, dp.render(null).textContent];
    out.noKeyCells = [
      bp[0].render({ ...bRow, has_blocked_plan: true, event_time: null }).textContent,
      bp[1].render({ ...bRow, has_blocking_plan: true, event_time: "" }).textContent,
      bp[0].render({ ...bRow, has_blocked_plan: true, blocking_spid: null }).textContent,
      bp[1].render({ ...bRow, has_blocking_plan: true, blocked_spid: undefined }).textContent,
      dp.render({ ...dRow, has_victim_plan: true, deadlock_time: null }).textContent,
      dp.render({ ...dRow, has_victim_plan: true, collection_time: undefined }).textContent,
    ];
    out.fetched = fetches.length;
  },
  async blockingButtons() {
    planReply(XML);
    const bp = viewer.blockingPlanColumns("srv-a");
    const key = { event_time: "2026-03-04T05:06:07.1234567", blocked_spid: 57, blocked_ecid: 0, blocking_spid: 61, blocking_ecid: 0 };
    const cells = (row) => bp.map((c) => c.render(row).textContent);
    out.both = cells({ ...key, has_blocked_plan: true, has_blocking_plan: true });
    out.blockedOnly = cells({ ...key, has_blocked_plan: true });
    out.blockingOnly = cells({ ...key, has_blocking_plan: true });
    out.neither = cells(key);
    /* The server sends a flag as true or leaves it off, so a button needs exactly true: nothing else draws one. */
    out.looseFlags = [false, null, 0, 1, "true", "yes", {}, []].map((v) => cells({ ...key, has_blocked_plan: v, has_blocking_plan: v }).join("|"));
    /* The flags alone decide (not `source`). */
    out.xeFlagged = cells({ ...key, source: "xe", has_blocked_plan: true, has_blocking_plan: true });
    out.dmvFlagless = cells({ ...key, source: "dmv" });
    /* Each button opens its own plan: the blocked one first, the blocking one after, two reads and two panels. */
    const row = { ...key, has_blocked_plan: true, has_blocking_plan: true };
    const blockedCell = bp[0].render(row);
    const blockingCell = bp[1].render(row);
    blockedCell.byText("Blocked plan").click();
    await flush();
    out.blockedSearch = params().search;
    out.blockingClosed = blockingCell.byText("Blocking plan") !== null && pre(blockingCell) === null;
    out.blockedShows = pre(blockedCell) !== null;
    blockingCell.byText("Blocking plan").click();
    await flush();
    out.blockingSearch = params().search;
    out.bothShow = pre(blockedCell) !== null && pre(blockingCell) !== null;
    out.reads = fetches.length;
    out.panels = viewer.openPlanKeys().length;
    /* A second row of the same report time but another pair is a different panel. */
    const other = bp[0].render({ ...row, blocking_spid: 62 });
    out.otherPairClosed = other.byText("Blocked plan") !== null;
  },
  async victimButton() {
    planReply(XML);
    const dp = viewer.deadlockPlanColumn("srv-a");
    const base = { collection_time: "2026-03-04T05:06:08.5000000", deadlock_time: "2026-03-04T05:06:07.1230000", victim_process_id: "process1a2b" };
    out.flagged = dp.render({ ...base, has_victim_plan: true }).textContent;
    out.unflagged = dp.render(base).textContent;
    out.looseFlags = [false, null, 0, 1, "true", "yes", {}, []].map((v) => dp.render({ ...base, has_victim_plan: v }).textContent);
    /* Two deadlocks with the same stamps and different victims each open a plan of their own. */
    const first = dp.render({ ...base, has_victim_plan: true });
    const second = dp.render({ ...base, victim_process_id: "process9z9z", has_victim_plan: true });
    first.byText("Victim plan").click();
    await flush();
    out.firstSearch = params().search;
    out.secondClosed = second.byText("Victim plan") !== null;
    second.byText("Victim plan").click();
    await flush();
    out.secondSearch = params().search;
    out.bothShow = pre(first) !== null && pre(second) !== null;
    out.panels = viewer.openPlanKeys().length;
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
