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

globalThis.document = {
  createElement: (t) => new FakeNode(t), createElementNS: (_n, t) => new FakeNode(t),
  createTextNode: (t) => { const n = new FakeNode("#text"); n.textContent = String(t); return n; },
  createDocumentFragment: () => new FakeNode("#frag"),
  getElementById: (id) => byId[id] || null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener() {}, removeEventListener() {}, body: new FakeNode("body"), documentElement: new FakeNode("html"),
  cookie: "", hidden: false,
};
globalThis.Node = FakeNode;
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
globalThis.setTimeout = () => 0;
globalThis.clearTimeout = () => {};

let fetches = 0; let renders = 0; const fetchLog = [];
let fetchDelayMs = 0; // simulated time a read takes: each fetch holds open until the clock passes this
let pending = [];
globalThis.fetch = (url) => {
  fetches++; if (String(url).includes('get_server_summary')) renders++; fetchLog.push(Math.round((now - 1000000) / 1000) + ' ' + url);
  return new Promise((resolve) => {
    pending.push({ at: now + fetchDelayMs, done: () => resolve({ ok: false, status: 500, json: async () => ({}), text: async () => "" }) });
  });
};
const releaseDue = () => { const due = pending.filter((p) => p.at <= now); pending = pending.filter((p) => p.at > now); due.forEach((p) => p.done()); };
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
  pending.forEach((p) => p.done()); pending = []; fetchDelayMs = bootDelay; tick = null;
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
  await fresh("30s", "#/finops/alpha/utilization");
  await advance(2);
  out.controlHiddenOnFinops = brand.find((x) => x.attrs.id === "page-refresh-control").hidden;
}

console.log(JSON.stringify(out));
process.exit(0);
