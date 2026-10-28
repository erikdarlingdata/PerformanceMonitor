/* #6793: runs the shipped app.js (the real route(), scheduler tick, refresh-policy.js, refresh-control.js and util.js read
   counter) and the real Job History, Query Tag Usage and Server pages against a stand-in DOM, a virtual clock and a fetch
   that records every request and its AbortSignal. Prints what a scenario measured as one line of JSON.
   WebRouteChangeAbortBehaviourTests starts it as
       node web-route-change-abort-harness.mjs <path to wwwroot/js> <scenario>
   A read of the two slow pages never answers by itself: it ends only when the page aborts it or when `slowReadMs` of virtual
   time has passed (140 s, the measured Query Tag Usage read). Every other read answers at once with a 500. */
import { pathToFileURL } from "node:url";
import path from "node:path";

const jsDir = path.resolve(process.argv[2]);
const scenario = process.argv[3];

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; this.style = {}; this.className = "";
    this.listeners = {}; this.parentNode = null; this.hidden = false; this.value = ""; this.textContent = "";
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { if (c && typeof c === "object") c.parentNode = this; this.children.push(c); return c; }
  append(...cs) { cs.forEach((c) => this.appendChild(c)); }
  prepend(c) { this.children.unshift(c); }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  replaceChildren(...cs) { this.children = []; cs.forEach((c) => this.appendChild(c)); }
  remove() {}
  setAttribute(n, v) { this.attrs[n] = String(v); }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  removeAttribute(n) { delete this.attrs[n]; }
  addEventListener(t, f) { (this.listeners[t] ||= []).push(f); }
  removeEventListener() {}
  dispatch(t) { (this.listeners[t] || []).forEach((f) => f({ target: this, preventDefault() {}, stopPropagation() {} })); }
  focus() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
  find(pred) {
    if (pred(this)) return this;
    for (const c of this.children) { const f = c && c.find ? c.find(pred) : null; if (f) return f; }
    return null;
  }
}

const docListeners = {};
const byId = {};
for (const id of ["main", "server-list", "view-list", "statusbar", "refresh-hint"]) byId[id] = new FakeNode("div");
const brand = new FakeNode("div");
byId["auto-refresh-toggle"] = new FakeNode("button");
brand.appendChild(byId["auto-refresh-toggle"]);

globalThis.document = {
  createElement: (t) => new FakeNode(t), createElementNS: (_n, t) => new FakeNode(t),
  createTextNode: (t) => { const n = new FakeNode("#text"); n.textContent = String(t); return n; },
  createDocumentFragment: () => new FakeNode("#frag"),
  getElementById: (id) => byId[id] || null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener(t, f) { (docListeners[t] ||= []).push(f); }, removeEventListener() {}, body: new FakeNode("body"), documentElement: new FakeNode("html"),
  cookie: "", hidden: false,
};
globalThis.Node = FakeNode;
globalThis.window = globalThis;
globalThis.scrollTo = () => {};
globalThis.location = { hash: "#/server/alpha", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const winListeners = {};
globalThis.addEventListener = (t, f) => { (winListeners[t] ||= []).push(f); };
globalThis.removeEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
globalThis.requestAnimationFrame = () => 0;
globalThis.cancelAnimationFrame = () => {};
let storage = new Map();
globalThis.localStorage = {
  getItem: (k) => (storage.has(k) ? storage.get(k) : null),
  setItem: (k, v) => { storage.set(k, String(v)); },
  removeItem: (k) => { storage.delete(k); },
};
globalThis.sessionStorage = globalThis.localStorage;

let now = 1_000_000;
Date.now = () => now;
let tick = null;
globalThis.setInterval = (fn) => { tick = fn; return 1; };
globalThis.clearInterval = () => {};
globalThis.setTimeout = () => 0;
globalThis.clearTimeout = () => {};

let fetches = 0; let serverRenders = 0;
const fetchLog = [];        // { at, url, signal, abortedAtStart, aborted }
let slowReadMs = 140_000;
const isSlow = (url) => url.includes("get_job_history") || url.includes("get_resource_use_by_tag") || (slowRegistry && url.includes("list_servers")) || (slowTrend && url.includes("get_server_trend")) || (slowFanout && url.includes("get_memory_grants")) || (slowDatabases && url.includes("/api/server-databases")) || (slowPanels && url.includes("/api/read/") && !url.includes("list_investigation_requests"));
let slowPanels = false;     // a scenario sets this to leave every server panel read out
let dailyEmpty = false;     // a scenario sets this: get_daily_summary_range answers "empty", and every second read (the calendar's month read) is held
let dailyReads = 0;
let rangeBody = null;       // a scenario sets this: the body get_daily_summary_range answers with, whose last day is the read's to_date
let slowTrend = false;      // a scenario sets this to leave the server page's trend reads out
let slowFanout = false;     // a scenario sets this to leave the Memory tab's fanout() read (get_memory_grants) out
let slowDatabases = false;  // a scenario sets this to leave the FinOps Database box's name list read out
let slowRegistry = false;   // a scenario sets this to leave the list_servers read out
let holdFleet = false;      // a scenario sets this to leave /api/fleet out until landFleet()
let fleetBody = null;       // a body for /api/fleet, or null for the default 500
let tagBody = null;         // a body for get_resource_use_by_tag, or null for the default 500
const json = (body, status = 200) => ({ status, body });
/* What a read answers once it is due. Anything a scenario does not name answers 500, as before. */
const answerFor = (url) => {
  if (url.includes("/api/fleet")) return fleetBody ? json(fleetBody) : json({}, 500);
  if (dailyEmpty && url.includes("get_daily_summary_range")) return json({ status: "empty", message: "No day collected in this span." });
  if (rangeBody && url.includes("get_daily_summary_range")) return json(rangeBody);
  if (url.includes("get_resource_use_by_tag") && tagBody) return json(tagBody);
  if (url.includes("/api/read/list_servers")) return json({ servers: [{ server_name: "alpha", display_name: "alpha" }] });
  if (url.includes("/api/session")) return json({ can_edit: false });
  if (url.includes("/api/catalog")) return json({ reads: [{ name: "get_view_panel_read" }, { name: "get_notebook_cell_read" }], viz: [], compose: {} });
  if (url.includes("/api/views/view1")) return json({ id: "view1", name: "A view", definition: { panels: [{ title: "View panel", read: "get_view_panel_read", viz: "table" }] } });
  if (url.includes("/api/views/note1")) return json({ id: "note1", name: "A notebook", definition: { kind: "notebook", cells: [{ type: "panel", title: "Cell", read: "get_notebook_cell_read", viz: "table" }] } });
  return json({}, 500);
};
let pending = [];
globalThis.fetch = (url, opts = {}) => {
  url = String(url);
  fetches++; if (url.includes("get_server_summary")) serverRenders++;
  const rec = { at: now, url, signal: opts.signal || null, abortedAtStart: !!(opts.signal && opts.signal.aborted), aborted: false };
  fetchLog.push(rec);
  /* app.js asks /api/session before it starts (#6840: is this a front door?). A store answers it at once, and the page must not wait on the
     scenario's clock to start, so it is answered now. It stays counted in fetchLog like every other read. */
  if (url.split("?")[0] === "/api/session") {
    const a = answerFor(url);
    return Promise.resolve({ ok: true, status: a.status, json: async () => a.body, text: async () => JSON.stringify(a.body) });
  }
  return new Promise((resolve, reject) => {
    const answer = () => {
      const a = answerFor(url);
      resolve({ ok: a.status < 400, status: a.status, json: async () => a.body, text: async () => (a.status === 500 && !Object.keys(a.body).length ? "" : JSON.stringify(a.body)) });
    };
    const heldMonth = dailyEmpty && url.includes("get_daily_summary_range") && (++dailyReads % 2 === 0);
    const hold = (holdFleet && url.includes("/api/fleet")) || heldMonth;
    const p = { at: hold ? Infinity : isSlow(url) ? now + slowReadMs : now, done: answer, hold, url };
    pending.push(p);
    if (opts.signal) opts.signal.addEventListener("abort", () => {
      rec.aborted = true;
      pending = pending.filter((x) => x !== p);
      reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
    }, { once: true });
  });
};
/* Let the held /api/fleet read answer now and run what its callers do next, without releasing any other read. */
const landFleet = async () => {
  const held = pending.filter((p) => p.hold);
  pending = pending.filter((p) => !p.hold);
  held.forEach((p) => p.done());
  await drain();
};
const releaseDue = () => { const due = pending.filter((p) => p.at <= now); pending = pending.filter((p) => p.at > now); due.forEach((p) => p.done()); };
const drain = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r)); };
async function advance(seconds) {
  for (let i = 0; i < seconds; i++) { now += 1000; releaseDue(); await drain(); tick(); await drain(); }
}
const settle = async () => { for (let i = 0; i < 10; i++) { releaseDue(); await drain(); } };
const count = (needle) => fetchLog.filter((r) => r.url.includes(needle)).length;
const fire = (list, type) => (list[type] || []).forEach((f) => f({}));

async function boot(hash) {
  location.hash = hash;
  await import(pathToFileURL(path.join(jsDir, "app.js")).href + "?run=1");
  await drain();
}
const go = async (hash) => { location.hash = hash; fire(winListeners, "hashchange"); await drain(); };
const refreshBtn = () => brand.find((x) => x.attrs.id === "page-refresh-now");
const hint = () => byId["refresh-hint"].textContent;

const scenarios = {
  /* The Job History read is out (140 s long) when the user goes to the Server page. */
  async leaveSlowPage() {
    await boot("#/job-history");
    await advance(2);
    const slow = fetchLog.find((r) => r.url.includes("get_job_history"));
    const before = { started: !!slow, aborted: slow.aborted };
    await go("#/server/alpha");
    const afterLeave = { aborted: slow.aborted, serverRenders };
    await advance(3);
    const btn = refreshBtn();
    const out = { before, afterLeave, busyAfterSettle: btn.disabled === true };
    const r0 = serverRenders;
    btn.dispatch("click"); await settle();
    out.refreshClickReads = serverRenders - r0;
    return out;
  },
  /* The poll of the page that was moved to: the 1 minute poll re-reads it while the old read would still be running. */
  async pollAfterLeaving() {
    await boot("#/job-history");
    await advance(2);
    await go("#/server/alpha");
    await advance(3);
    const r0 = serverRenders;
    await advance(70); // well inside the slow read's 140 s
    return { pollReads: serverRenders - r0, slowStillOut: pending.some((p) => p.at > now) };
  },
  /* The tab was hidden and comes back after the page is due: the visibility handler must refresh the page. */
  async tabReturnAfterLeaving() {
    await boot("#/job-history");
    await advance(2);
    await go("#/server/alpha");
    await advance(3);
    document.hidden = true; fire(docListeners, "visibilitychange");
    await advance(100); // hidden: the scheduler settles the render and schedules the next one, and re-renders nothing
    const r0 = serverRenders;
    document.hidden = false; fire(docListeners, "visibilitychange");
    await advance(5);
    return { returnReads: serverRenders - r0 };
  },
  /* The old read ends 140 s after it started; the page moved to must not be timed by it. */
  async backOffAfterLeaving() {
    await boot("#/job-history");
    await advance(2);
    await go("#/server/alpha");
    await advance(150); // past the end of the old read
    const text = hint();
    return { hint: text, serverRenders };
  },
  /* Query Tag Usage shows 24 hour totals: the 60 s poll never re-reads it. */
  async tagUsagePoll() {
    slowReadMs = 0; // the read answers at once, so nothing but the poll's own choice can keep it from running again
    await boot("#/query-tag-usage");
    await advance(2);
    const first = count("get_resource_use_by_tag");
    await advance(400); // six and a half poll intervals
    const out = { first, afterPoll: count("get_resource_use_by_tag"), hint: hint() };
    return out;
  },
  /* The page's own server picker still re-reads it, and a second read aborts the first. */
  async tagUsagePickerStillReads() {
    await boot("#/query-tag-usage");
    await advance(2);
    const select = byId.main.find((x) => x.attrs["aria-label"] === "Server");
    const firstRead = fetchLog.find((r) => r.url.includes("get_resource_use_by_tag"));
    select.value = "alpha"; select.dispatch("change"); await drain();
    return { reads: count("get_resource_use_by_tag"), firstAborted: firstRead.aborted };
  },
  /* The page's own re-load (a filter change here) still aborts its older read: the Status picker asks for a different read. */
  async sameRouteLoadAbortsOlderLoad() {
    await boot("#/job-history");
    await advance(2);
    const older = fetchLog.find((r) => r.url.includes("get_job_history"));
    const status = byId.main.find((x) => x.attrs["aria-label"] === "Status");
    status.value = "failed"; status.dispatch("change"); await drain();
    return { olderAborted: older.aborted, reads: count("get_job_history") };
  },
  /* The server page is left for a saved view: the view's first grid must send its panel reads, on a signal that is not aborted. */
  async leaveServerForView() {
    await boot("#/server/alpha");
    await advance(2);
    await go("#/view/view1"); await settle();
    const reads = fetchLog.filter((r) => r.url.includes("get_view_panel_read"));
    return { reads: reads.length, abortedAtStart: reads.some((r) => r.abortedAtStart), aborted: reads.some((r) => r.aborted) };
  },
  /* The same for a notebook, whose cells are drawn by renderNotebookDoc and never set the panel signal themselves. */
  async leaveServerForNotebook() {
    await boot("#/server/alpha");
    await advance(2);
    await go("#/notebook/note1"); await settle();
    const reads = fetchLog.filter((r) => r.url.includes("get_notebook_cell_read"));
    return { reads: reads.length, abortedAtStart: reads.some((r) => r.abortedAtStart), aborted: reads.some((r) => r.aborted) };
  },
  /* FinOps has its servers cached, its poll re-reads them (that read is slow), and the user leaves and comes back. */
  async finopsLeaveAndReturn() {
    await boot("#/finops");
    await advance(2);
    const first = count("list_servers");
    slowRegistry = true;
    await advance(65); // the poll re-reads the registry, and that read stays out
    const poll = fetchLog.filter((r) => r.url.includes("list_servers")).at(-1);
    const pollOut = count("list_servers") === first + 1 && !poll.aborted;
    await go("#/server/alpha"); await settle();
    const pollAborted = poll.aborted;
    const before = count("list_servers");
    await go("#/finops"); await settle();
    const loading = !!byId.main.find((x) => (x.textContent || "").includes("Loading servers"));
    return { first, pollOut, pollAborted, readsOnReturn: count("list_servers") - before, loadingStrip: loading };
  },
  /* First visit to a server page: the fleet read is still out when the user leaves, and lands afterwards. */
  async fleetReadLandsAfterLeaving() {
    holdFleet = true;
    await boot("#/server/alpha");
    await advance(2);
    const panelReads = () => fetchLog.filter((r) => r.url.includes("/api/read/")).length;
    await go("#/investigations"); await settle();
    const before = panelReads();
    holdFleet = false;
    slowPanels = true; // a server panel read started now would stay out for 140 s
    await landFleet();
    tick(); await drain();
    const btn = refreshBtn();
    const started = panelReads() - before;
    /* Whether the page moved to is still polled: advance past its due time (a minute) and count that it re-rendered. */
    const polls = () => count("list_investigation_requests");
    const p0 = polls();
    await advance(70);
    return { panelReadsStartedAfterLeave: started, refreshDisabled: btn.disabled === true, pollReads: polls() - p0 };
  },
  /* The Memory tab's chart panels are hand-built and read get_server_trend with the signal of their batch: the trend read is still out when the user leaves. */
  async leaveServerWithTrendReadOut() {
    slowTrend = true;
    await boot("#/server/alpha/memory");
    await advance(2);
    const trends = () => fetchLog.filter((r) => r.url.includes("get_server_trend"));
    const before = { reads: trends().length, abortedAtStart: trends().some((r) => r.abortedAtStart), aborted: trends().some((r) => r.aborted) };
    await go("#/investigations"); await settle();
    return { before, reads: trends().length, allAborted: trends().length > 0 && trends().every((r) => r.aborted) };
  },
  /* The Memory tab's Memory Grants panels are built by fanout(), one shared read, and carried no signal: the read stayed out on the store after the leave. */
  async leaveServerWithFanoutReadOut() {
    slowFanout = true;
    await boot("#/server/alpha/memory");
    await advance(2);
    const grants = () => fetchLog.filter((r) => r.url.includes("get_memory_grants"));
    const before = { reads: grants().length, aborted: grants().some((r) => r.aborted) };
    const { readErrorCount } = await import(pathToFileURL(path.join(jsDir, "util.js")).href);
    const strips0 = readErrorCount();
    /* The strips are counted right after the leave and before anything is released: an aborted read that drew "Could not render" would show here, and the next page's own answers have not landed yet. */
    await go("#/investigations");
    const errorStripsDrawn = readErrorCount() - strips0;
    await settle();
    return { before, reads: grants().length, perRead: grants().map((r) => (r.url.includes("chart") ? "chart" : "fanout") + ":" + r.aborted), allAborted: grants().length > 0 && grants().every((r) => r.aborted), errorStripsDrawn };
  },
  /* The Health tab's calendar opens on an "empty" range and reads its month itself: that read was unsignalled and stayed out after the leave. */
  async leaveHealthWithCalendarReadOut() {
    dailyEmpty = true;
    await boot("#/server/alpha");
    await advance(2);
    const ranges = () => fetchLog.filter((r) => r.url.includes("get_daily_summary_range"));
    const before = { reads: ranges().length, aborted: ranges().some((r) => r.aborted) };
    await go("#/investigations"); await settle();
    const afterLeave = { reads: ranges().length, monthReadAborted: ranges().length > 1 && ranges()[1].aborted };
    await go("#/server/alpha"); await advance(2);
    return { before, afterLeave, monthReadsAfterReturn: ranges().length - afterLeave.reads };
  },
  /* The Overview tab draws today's tile and the month calendar from ONE get_daily_summary_range read, and asks no get_daily_summary beside it. */
  async overviewReadsTheRangeOnce() {
    rangeBody = { to_date: "2026-10-03", days: [{ summary_date: "2026-10-01", top_wait_type: "WAIT_OLD" }, { summary_date: "2026-10-03", top_wait_type: "WAIT_TODAY" }] };
    await boot("#/server/alpha/overview");
    await advance(2);
    const has = (t) => !!byId.main.find((x) => x.textContent === t);
    const read = (re) => fetchLog.filter((r) => re.test(r.url));
    return {
      rangeReads: read(/get_daily_summary_range/).length,
      singleDayReads: read(/get_daily_summary(?!_range)/).length,
      todayInTile: has("WAIT_TODAY"),
      calendarDrawn: !!byId.main.find((x) => x.textContent === "Daily Health Calendar"),
      anyAborted: fetchLog.some((r) => r.aborted),
    };
  },
  /* The FinOps Locking tab's Database box reads the server's database names with the tab's own signal. */
  async finopsDatabaseBoxReadOut() {
    slowDatabases = true;
    await boot("#/finops/alpha/locking");
    await advance(2);
    const names = () => fetchLog.filter((r) => r.url.includes("/api/server-databases"));
    const before = { reads: names().length, aborted: names().some((r) => r.aborted) };
    await go("#/investigations"); await settle();
    return { before, reads: names().length, allAborted: names().length > 0 && names().every((r) => r.aborted) };
  },
  /* Query Tag Usage stamps the footer's "Updated" time when its read lands, not when the page starts to render. */
  async tagUsageFooterStamp() {
    fleetBody = { cards: [], total_servers: 0, generated_at: "2020-01-01T00:00:00Z" };
    tagBody = { sections: [] };
    const stamp = (ms) => "Updated " + new Date(ms).toLocaleString();
    const bar = () => { const n = byId.statusbar.find((x) => typeof x.textContent === "string" && x.textContent.startsWith("Updated ")); return n ? n.textContent : null; };
    const t0 = now;
    await boot("#/query-tag-usage");
    await advance(2);
    const during = bar();
    await advance(150);
    const after = bar();
    return { stampedAtStart: during === stamp(t0), duringRead: during, stampedWhenReadLanded: after === stamp(t0 + 140_000), afterRead: after };
  },
};
if (!Object.hasOwn(scenarios, scenario)) throw new Error("unknown scenario " + scenario);
const __r = await scenarios[scenario](); if (process.env.DBG) console.error(fetchLog.map(r=>(r.at-1000000)/1000+" "+r.url).join("\n")); console.log(JSON.stringify(__r));
process.exit(0);
