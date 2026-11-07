/* Runs the shipped app.js (the real scheduler tick, refresh-policy.js, refresh-control.js and util.js read counter)
   against a stand-in DOM, a controllable clock and a counting fetch, and prints what the server/FinOps page
   interval selector did as one line of JSON. WebPageRefreshIntervalBehaviourTests starts it as
       node web-page-refresh-harness.mjs <path to wwwroot/js>
   Every scenario plays on its own fresh module copy through a query string on the import. */
import { pathToFileURL } from "node:url";
import path from "node:path";

const jsDir = path.resolve(process.argv[2]);

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
  insertBefore(c, ref) { const i = ref ? this.children.indexOf(ref) : -1; if (c && typeof c === "object") c.parentNode = this; if (i < 0) this.children.push(c); else this.children.splice(i, 0, c); return c; }
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

const byId = {};
for (const id of ["main", "server-list", "view-list", "statusbar", "refresh-hint"]) byId[id] = new FakeNode("div");
const brand = new FakeNode("div");
byId["auto-refresh-toggle"] = new FakeNode("button");
brand.appendChild(byId["auto-refresh-toggle"]);

/* Every boot's start() adds its own visibilitychange listener; a scenario fires only the newest, the live module copy's. */
const docListeners = {};
globalThis.document = {
  createElement: (t) => new FakeNode(t), createElementNS: (_n, t) => new FakeNode(t),
  createTextNode: (t) => { const n = new FakeNode("#text"); n.textContent = String(t); return n; },
  createDocumentFragment: () => new FakeNode("#frag"),
  getElementById: (id) => byId[id] || null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener(t, f) { (docListeners[t] ||= []).push(f); }, removeEventListener() {}, body: new FakeNode("body"), documentElement: new FakeNode("html"),
  cookie: "", hidden: false,
};
globalThis.Node = FakeNode;
/* The in-place refresh (#6933) watches a staged panel for its loading strip to go. These scenarios do not read what lands, so the observer never fires. */
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.window = globalThis;
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
/* The page's timers are tracked against the same clock and fired when it passes their due time (releaseDue), never by wall-clock
   time, so a scenario's counts do not depend on how long its flushes take. */
const timerDelay = (ms) => ms;
let timers = new Map(); let timerSeq = 0;
globalThis.setTimeout = (fn, ms = 0, ...args) => { const handle = ++timerSeq; timers.set(handle, { at: now + timerDelay(ms), run: () => fn(...args) }); return handle; };
globalThis.clearTimeout = (handle) => { timers.delete(handle); };
const fireDueTimers = () => { for (const [handle, t] of [...timers]) if (t.at <= now) { timers.delete(handle); t.run(); } };

let fetches = 0; let renders = 0; const fetchLog = [];
let fetchDelayMs = 0; // simulated time a read takes: each fetch holds open until the clock passes this
let pending = [];
globalThis.fetch = (url) => {
  /* app.js asks /api/session before it starts (#6840: is this a front door?). A store answers it at once and it is not a
     page read, so it is neither counted nor held open by the simulated read time: the page then starts as it always did. */
  if (String(url).split("?")[0] === "/api/session") {
    return Promise.resolve({ ok: true, status: 200, json: async () => ({ can_edit: false }), text: async () => JSON.stringify({ can_edit: false }) });
  }
  fetches++; if (String(url).includes('get_server_summary')) renders++; fetchLog.push(Math.round((now - 1000000) / 1000) + ' ' + url);
  return new Promise((resolve) => {
    pending.push({ at: now + fetchDelayMs, done: () => resolve({ ok: false, status: 500, json: async () => ({}), text: async () => "" }) });
  });
};
const releaseDue = () => { fireDueTimers(); const due = pending.filter((p) => p.at <= now); pending = pending.filter((p) => p.at > now); due.forEach((p) => p.done()); };
const drain = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r)); };

// Advances the clock one second at a time, running the scheduler tick, and returns the fetches that started.
async function advance(seconds) {
  const before = fetches;
  for (let i = 0; i < seconds; i++) { now += 1000; releaseDue(); await drain(); tick(); await drain(); }
  return fetches - before;
}

let n = 0; let bootDelay = 0;
async function boot(stored, hash = "#/server/alpha") {
  storage = new Map();
  if (stored) storage.set("darling.pageRefresh", stored);
  pending.forEach((p) => p.done()); pending = []; timers = new Map(); fetchDelayMs = bootDelay; tick = null;
  await drain();
  location.hash = hash;
  await import(pathToFileURL(path.join(jsDir, "app.js")).href + "?run=" + (++n));
  await drain();
  return () => brand.find((x) => x.tag === "select");
}

const out = {};
const select = async (find, value) => { const s = find(); s.value = value; s.dispatch("change"); await drain(); };
const settle = async () => { for (let i = 0; i < 10; i++) { releaseDue(); await drain(); } };
const clickRefresh = async () => { await settle(); brand.find((x) => x.attrs.id === "page-refresh-now").dispatch("click"); await settle(); };

// Shared module state means each scenario re-imports app.js, whose start() adds one more control under brand:
// reset brand between boots so find() reaches the live one.
const fresh = async (stored, hash) => { brand.children = [byId["auto-refresh-toggle"]]; byId["auto-refresh-toggle"].parentNode = brand; return boot(stored, hash); };

// Every interval: how many seconds pass before a page read starts after the page has settled.
async function secondsToNextRead(choice) {
  const find = await fresh(choice);
  await advance(1); // settles the first render (instant reads)
  const base = renders;
  for (let s = 1; s <= 400; s++) { await advance(1); if (renders > base) return s; }
  return null;
}
out.options = (await (async () => { const f = await fresh(null); return f().children.map((o) => o.attrs.value + "=" + o.textContent); })());
out.selectedByDefault = (await (async () => { const f = await fresh(null); return f().value; })());
out.next30 = await secondsToNextRead("30s");
out.next1m = await secondsToNextRead("1m");
out.next5m = await secondsToNextRead("5m");

// Off fires no page reads; the shell roll-up (sidebar, views, status bar) keeps running.
{
  await fresh("off");
  await advance(2);
  const r0 = renders; const f0 = fetches;
  await advance(900);
  out.offPageReads = renders - r0;
  out.offShellReads = fetches - f0;
}

// Refresh fires one immediate page refresh (and none when nothing else is due).
{
  const find = await fresh("off");
  await advance(2);
  const before = renders;
  await clickRefresh();
  out.refreshClickReads = renders - before;
  out.refreshClickRerendersPage = out.refreshClickReads > 0;
  await advance(2);
  const rendersBefore = renders;
  await advance(300);
  out.refreshAfterOffReads = renders - rendersBefore;
}

// A fixed custom range: the automatic poll keeps the panels on screen (no page reads); the Refresh button re-reads.
{
  // The range lives in server.js, which every scenario shares, so it uses a server of its own.
  await fresh("1m", "#/server/fixedrange");
  await advance(2);
  const serverPage = await import(pathToFileURL(path.join(jsDir, "pages", "server.js")).href);
  const endMs = -86_400_000; // a window long past, against the nowMs given below
  const err = serverPage.applyCustomRange("fixedrange", endMs - 7_200_000, endMs, endMs + 3 * 86_400_000);
  await settle();
  const base = renders;
  await advance(130); // two 1-minute poll ticks
  out.fixedRangeApplyError = err;
  out.fixedRangePollReads = renders - base;
  const afterPoll = renders;
  await clickRefresh();
  out.fixedRangeRefreshReads = renders - afterPoll;
}

// Refresh while reads are running is a no-op: a double-click sends one shell refresh and one render.
{
  await fresh("off");
  await advance(2);
  fetchDelayMs = 5000;
  const log0 = fetchLog.length; const r0 = renders;
  const btn = brand.find((x) => x.attrs.id === "page-refresh-now");
  btn.dispatch("click"); btn.dispatch("click"); await drain();
  const views = () => fetchLog.slice(log0).filter((l) => l.includes("/api/views")).length;
  out.doubleClickViews = views();
  out.doubleClickRenders = renders - r0;
  out.busyWhileReading = btn.disabled === true && btn.attrs["aria-busy"] === "true" && btn.attrs.title === "Refreshing…";
  await advance(10);
  out.doubleClickRenders = renders - r0;
  out.idleAfterSettle = btn.disabled === false && btn.getAttribute("aria-busy") === null;
  fetchDelayMs = 0;
}

// A choice saved by another tab (storage event) applies here.
{
  const find = await fresh("1m");
  await advance(2);
  storage.set("darling.pageRefresh", "off");
  (winListeners.storage || []).forEach((f) => f({ key: "darling.pageRefresh" }));
  await drain();
  out.storageSelectValue = find().value;
  const r0 = renders;
  await advance(300);
  out.storageOffRenders = renders - r0;
  out.storageHint = byId["refresh-hint"].textContent;
}

// Persistence: choosing writes the key; a new boot reads it back into the selector.
{
  const find = await fresh(null);
  await select(find, "30s");
  out.saved = storage.get("darling.pageRefresh") ?? null;
  const key = storage.get("darling.pageRefresh");
  const find2 = await fresh(key);
  out.restoredValue = find2().value;
}

// 30 s under backoff: reads that take 20 s (> half the interval) push the next page render out to 4x the render
// time (rather than 30 s), and a render never starts while the previous one's reads are outstanding.
{
  bootDelay = 20000;
  await fresh("30s");
  const starts = [];
  let hintSlow = false;
  for (let s = 0; s < 300; s++) {
    const r = renders;
    await advance(1);
    if (renders > r) starts.push(s + 1);
    if (/slow page/.test(byId["refresh-hint"].textContent)) hintSlow = true;
  }
  out.backoffStarts = starts;
  out.backoffGaps = starts.slice(1).map((v, i) => v - starts[i]);
  out.backoffHintSaysSlow = hintSlow;
  bootDelay = 100000; // reads outlast many 30 s intervals
  await fresh("30s");
  const b = renders;
  await advance(150);
  out.stackedRenders = renders - b;
  bootDelay = 0;
}

// Hint text through updateRefreshHint.
{
  await fresh("30s");
  await advance(2);
  out.hint30 = byId["refresh-hint"].textContent;
  await fresh("off");
  await advance(2);
  out.hintOff = byId["refresh-hint"].textContent;
  await fresh("30s", "#/fleet");
  await advance(2);
  out.controlHiddenOnFleet = brand.find((x) => x.attrs.id === "page-refresh-control").hidden;
  out.intervalVisibilityOnFleet = brand.find((x) => x.attrs.id === "page-refresh-select").style.visibility;
  await fresh("30s", "#/finops/alpha/utilization");
  await advance(2);
  out.controlHiddenOnFinops = brand.find((x) => x.attrs.id === "page-refresh-control").hidden;
  out.intervalVisibilityOnFinops = brand.find((x) => x.attrs.id === "page-refresh-select").style.visibility;
}

// #7935: a refresh that is due for the page and the shell together (Resume, and a tab return while the page was due) re-renders the page
// in the same pass as the shell's reads start, so the page's /api/fleet joins the sidebar's and the store is asked for one fleet roll-up.
// The shell's reads count as page reads, so a page decided after they start waits a tick or more and then sends its own fleet read.
{
  const fleetReads = (from) => fetchLog.slice(from).filter((l) => l.includes("/api/fleet")).length;
  // Resume: the toggle is a fresh node, so only this boot's click listener is on it.
  byId["auto-refresh-toggle"] = new FakeNode("button");
  await fresh("1m", "#/server/alpha");
  await advance(2);
  const toggle = byId["auto-refresh-toggle"];
  toggle.dispatch("click"); await drain(); // paused
  await advance(2);
  const log0 = fetchLog.length; const r0 = renders;
  toggle.dispatch("click"); await drain(); // resumed: one refresh now
  out.resumeFleetReads = fleetReads(log0);
  // The page's first read on this route is the fleet read, and its panel reads follow that answer: let the pass's fetches answer
  // with no tick in between, so a render that started in the pass shows now and one that waited for a tick does not.
  now += 1000; releaseDue(); await drain();
  out.resumeRendersBeforeTick = renders - r0;
  await advance(3);
  out.resumeRendersAfter = renders - r0;
  out.resumeFleetReadsAfter = fleetReads(log0);
  // Tab return with the page due: hidden through a whole interval, then visible again.
  await fresh("1m", "#/server/alpha");
  await advance(2);
  document.hidden = true;
  await advance(70);
  const log1 = fetchLog.length; const r1 = renders;
  document.hidden = false;
  docListeners.visibilitychange.at(-1)(); await drain();
  out.returnFleetReads = fleetReads(log1);
  now += 1000; releaseDue(); await drain();
  out.returnRendersBeforeTick = renders - r1;
  await advance(3);
  out.returnRendersAfter = renders - r1;
  out.returnFleetReadsAfter = fleetReads(log1);
}

// #7935: a scheduler tick where the page and the shell are both due re-renders the page in the same pass as the shell's reads
// start, so one /api/fleet serves the sidebar and the page. The ticks that follow while that render's reads are still in flight
// do not start it again (#4191): every read of the render is sent once.
{
  const fleetReads = (from) => fetchLog.slice(from).filter((l) => l.includes("/api/fleet")).length;
  const repeated = (from) => { const seen = new Map(); for (const l of fetchLog.slice(from)) { const u = l.slice(l.indexOf(" ") + 1); seen.set(u, (seen.get(u) || 0) + 1); } return [...seen].filter(([, n]) => n > 1).map(([u]) => u); };
  await fresh("1m", "#/server/alpha");
  // The boot render's fleet read answers at the first tick and its panel reads at the second, so the page is due 62 s after boot
  // and the shell 60 s after. Ticks run to 59 s with nothing due; the next one lands three seconds late and finds both due.
  await advance(59);
  fetchDelayMs = 5000; // this pass's reads take 5 s, so the ticks that follow land while the render's reads are in flight
  const log0 = fetchLog.length; const r0 = renders;
  now += 3000; releaseDue(); await drain(); tick(); await drain();
  out.tickShellReads = fetchLog.slice(log0).filter((l) => l.includes("/api/views")).length;
  out.tickFleetReads = fleetReads(log0);
  // The page's panel reads follow the fleet answer: let the pass's reads answer with no tick in between, so a render that started
  // in the pass shows now and one that waited for a later tick does not.
  now += 5000; releaseDue(); await drain();
  out.tickRendersBeforeTick = renders - r0;
  await advance(12); // the panel reads answer at 70 s; ticks at 66 to 74 s see the render in flight, then settled
  out.tickRendersAfter = renders - r0;
  out.tickFleetReadsAfter = fleetReads(log0);
  out.tickRepeatedReads = repeated(log0);
  fetchDelayMs = 0;
}

console.log(JSON.stringify(out));
process.exit(0);
