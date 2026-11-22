/* #9220: runs the shipped app.js (the real route(), scheduler tick, util.js read counter and page scope) and the real Server page
   behind a FRONT DOOR of three stores, as a first visit: the shell reads each store's /api/fleet, the page starts its panel reads
   before the fleet card answers (#8554), and each store's fleet answer lands at its own virtual time. A fetch records its
   AbortSignal, so the harness counts the reads the page aborted, and AbortController.prototype.abort records who called it.
   Prints one scenario's result as one line of JSON. FirstVisitFrontDoorTests starts it as
       node web-first-visit-front-door-harness.mjs <path to wwwroot/js> <scenario>
   Panel reads answer after PANEL_MS of virtual time. A fleet read answers after the delay its store was given. */
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
}

const docListeners = {};
const byId = {};
for (const id of ["main", "server-list", "view-list", "statusbar", "refresh-hint", "store-picker"]) byId[id] = new FakeNode("div");
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
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.window = globalThis;
globalThis.scrollTo = () => {};
const rewrites = [];
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost", reload() { reloads++; } };
let reloads = 0;
globalThis.history = { pushState() {}, replaceState(_s, _t, url) { rewrites.push(String(url)); location.hash = String(url).replace(/^[^#]*/, ""); }, back() {} };
const winListeners = {};
globalThis.addEventListener = (t, f) => { (winListeners[t] ||= []).push(f); };
globalThis.removeEventListener = () => {};
globalThis.dispatchEvent = (e) => { (winListeners[e.type] || []).forEach((f) => f(e)); return true; };
globalThis.Event = class { constructor(type) { this.type = type; } };
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
globalThis.requestAnimationFrame = () => 0;
globalThis.cancelAnimationFrame = () => {};
const storage = new Map();
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

/* Who called abort(): the first stack line outside this harness and the platform, for each abort of a signal a fetch carries. */
const abortCalls = [];
const realAbort = AbortController.prototype.abort;
AbortController.prototype.abort = function (...a) {
  const frames = String(new Error().stack).split("\n").slice(2).map((l) => l.trim()).filter((l) => l.includes("wwwroot") || l.includes("/js/") || l.includes("front-door-"));
  abortCalls.push({ at: now, frames: frames.slice(0, 4).map((l) => l.replace(/\(.*[\\/]js[\\/]/, "(")) });
  return realAbort.apply(this, a);
};

const PANEL_MS = process.env.PANEL_MS ? Number(process.env.PANEL_MS) : 400;
const STORES = ["use1", "use2", "use3"];
let fleetDelay = { use1: 100, use2: 300, use3: 600 };
let ownStore = "use1";
let serverCard = process.env.CARD ? JSON.parse(process.env.CARD) : null;
const card = (id, name, extra = {}) => ({
  server_id: id, display_name: name, server_name: name, band: "Healthy", band_label: "Healthy", is_online: true,
  status: "ok", last_collection: null, tags: [], is_silenced: false, metric_count: 0, measured_metric_count: 0, ...extra,
});
/* CARDS (JSON, optional): the fleet's cards as [{server_name, display_name}], for a server whose route name is only part of its own name. */
const cardsEnv = process.env.CARDS ? JSON.parse(process.env.CARDS) : null;
const fleetOf = (names, extra) => ({
  total_servers: names.length, critical_count: 0, warning_count: 0, offline_count: 0, offline_servers: 0, tags: [],
  cards: cardsEnv ? cardsEnv.map((c, i) => card(i + 1, c.display_name, { server_name: c.server_name, ...(i === 0 ? extra : {}) })) : names.map((n, i) => card(i + 1, n, i === 0 ? extra : {})), generated_at: "2026-11-21T12:00:00Z", worst_servers: [], additional_problem_count: 0,
});
const json = (body, status = 200) => ({ status, body });
const storeOf = (opts) => (opts.headers || {})["X-Darling-Store"] || "";
const answerFor = (url, opts) => {
  const p = url.split("?")[0];
  if (p === "/api/session") return json({ can_edit: false, front_door: true });
  if (p === "/api/front-door") return json({ stores: STORES.map((n) => ({ name: n, health: "ok", server_count: 1 })) });
  if (p === "/api/front-door/locate") return json({ store: ownStore, server_name: "alpha" });
  if (p === "/api/fleet") return json(fleetOf(["alpha"], serverCard));
  if (p === "/api/catalog") return json({ reads: [], viz: [], compose: {} });
  return json({});
};
const delayFor = (url, opts) => {
  const p = url.split("?")[0];
  if (p === "/api/fleet") return fleetDelay[storeOf(opts)] ?? 100;
  if (p === "/api/session" || p === "/api/front-door") return 0;
  if (p === "/api/front-door/locate") return 50;
  if (p.startsWith("/api/read/")) return PANEL_MS;
  return 0;
};
let pending = [];
const fetchLog = [];
globalThis.fetch = (url, opts = {}) => {
  url = String(url);
  const rec = { at: now, url, store: storeOf(opts), signal: opts.signal || null, aborted: false, answeredAt: null };
  fetchLog.push(rec);
  return new Promise((resolve, reject) => {
    const a = answerFor(url, opts);
    const p = { at: now + delayFor(url, opts), done: () => { rec.answeredAt = now; resolve({ ok: a.status < 400, status: a.status, json: async () => a.body, text: async () => JSON.stringify(a.body) }); } };
    pending.push(p);
    if (opts.signal) opts.signal.addEventListener("abort", () => {
      rec.aborted = true; rec.abortedAt = now;
      pending = pending.filter((x) => x !== p);
      reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
    }, { once: true });
  });
};
const releaseDue = () => { const due = pending.filter((p) => p.at <= now); pending = pending.filter((p) => p.at > now); due.forEach((p) => p.done()); };
const drain = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r)); };
/* Moves the virtual clock in 10 ms steps, answering what is due and running the scheduler's one-second tick on each second. */
async function advance(ms) {
  for (let t = 0; t < ms; t += 10) {
    now += 10; releaseDue(); await drain();
    if ((now - 1_000_000) % 1000 === 0 && tick) { tick(); await drain(); }
  }
}
const fire = (list, type) => (list[type] || []).forEach((f) => f({}));
async function boot(hash) {
  location.hash = hash;
  await import(pathToFileURL(path.join(jsDir, "app.js")).href + "?run=1");
  await drain();
}
const go = async (hash) => { location.hash = hash; fire(winListeners, "hashchange"); await drain(); };

const panelReads = () => fetchLog.filter((r) => r.url.startsWith("/api/read/"));
const summary = () => {
  const reads = panelReads();
  const byUrl = {};
  for (const r of reads) byUrl[r.url] = (byUrl[r.url] || 0) + 1;
  return {
    reads: reads.length,
    aborted: reads.filter((r) => r.aborted).map((r) => ({ url: r.url.slice(0, 60), startedAt: r.at - 1_000_000, abortedAt: r.abortedAt - 1_000_000 })),
    sentMoreThanOnce: Object.entries(byUrl).filter(([, n]) => n > 1).map(([u, n]) => u.slice(0, 60) + " x" + n),
    fleetReads: fetchLog.filter((r) => r.url === "/api/fleet").map((r) => r.store + "@" + (r.at - 1_000_000) + "->" + (r.answeredAt == null ? "-" : r.answeredAt - 1_000_000)),
    abortCalls: abortCalls.map((c) => ({ at: c.at - 1_000_000, frames: c.frames })),
    rewrites, reloads,
  };
};

const serverOf = (r) => new URLSearchParams(r.url.split("?")[1] || "").get("server");
const tally = (reads) => { const t = {}; for (const r of reads) { const n = serverOf(r); t[n] = (t[n] || 0) + 1; } return t; };
const tallyBy = (reads, param) => { const t = {}; for (const r of reads) { const n = new URLSearchParams(r.url.split("?")[1] || "").get(param); t[n] = (t[n] || 0) + 1; } return t; };
/* The text of every range picker button in the page on screen. */
const pickerTexts = () => {
  const found = [];
  const walk = (n) => { if (!n || typeof n !== "object") return; if (n.className === "trp-button" || (n.attrs && n.attrs.class === "trp-button")) found.push(n.textContent); (n.children || []).forEach(walk); };
  walk(byId.main);
  return found;
};
const ROUTE = process.env.ROUTE || "alpha";
const FULL = process.env.FULL || "alpha";

/* argv: <js dir> <scenario> <tab or "">. The own store (use1) answers its fleet at OWN_MS, the others at 300 and 600. */
const tabArg = process.argv[4] || "";
const hashFor = (store, server) => "#/" + (store ? "s/" + store + "/" : "") + "server/" + server + (tabArg ? "/" + tabArg : "");
const startScenario = (own) => { fleetDelay = { use1: own, use2: 300, use3: 600 }; };

const scenarios = {
  /* A first visit through a front door of three stores. The server's own store answers its fleet read at OWN_MS, after the panel reads
     have started and, with a short PANEL_MS, after they have all finished. */
  async firstVisit() {
    startScenario(Number(process.env.OWN_MS || 1500));
    await boot(hashFor("use1", "alpha"));
    await advance(4000);
    return summary();
  },
  /* The same first visit by a link with no store in it: the front door is asked which store has the server. */
  async firstVisitBareLink() {
    startScenario(Number(process.env.OWN_MS || 1500));
    await boot(hashFor("", "alpha"));
    await advance(4000);
    return summary();
  },
  /* #9220: a first visit by a PARTIAL route name (ROUTE is part of the card's server_name FULL and equals neither it nor the display name).
     The fleet card of the own store answers at OWN_MS. Reports the server name each panel read went out under, and where the link ended. */
  async partialFirstVisit() {
    startScenario(Number(process.env.OWN_MS || 700));
    await boot(hashFor("use1", ROUTE));
    await advance(4000);
    return { ...summary(), byServer: tally(panelReads()), finalHash: location.hash };
  },
  /* The partial-name first visit, then the page's own 60 s poll, then a tab click by the link the page ended on. Reads sent after each
     step are tallied by the name they carry. */
  async partialThenPollAndTab() {
    startScenario(Number(process.env.OWN_MS || 700));
    await boot(hashFor("use1", ROUTE));
    await advance(4000);
    const first = panelReads().length;
    await advance(65000);
    const afterPoll = panelReads().slice(first);
    const second = panelReads().length;
    await go("#/s/use1/server/" + encodeURIComponent(FULL) + "/" + (process.env.NEXT_TAB || "queries"));
    await advance(2500);
    return { ...summary(), afterPoll: tally(afterPoll), afterTabClick: tally(panelReads().slice(second)), finalHash: location.hash };
  },
  /* #9220: a first visit by a PARTIAL route name while the reader holds a custom range under that name (a range set from the link before the
     card lands). The range is a live 36 hours, not one of the presets. Reports the range each panel read carried, the picker's text
     after the move, and the reads the page's own 60 s poll sent under the own name. */
  async partialCustomRange() {
    const server = await import(pathToFileURL(path.join(jsDir, "pages", "server.js")).href);
    const HOUR = 3600000;
    server.applyCustomRange(ROUTE, now - 36 * HOUR, now, now, { redraw: false });
    startScenario(Number(process.env.OWN_MS || 700));
    await boot(hashFor("use1", ROUTE));
    await advance(12000);
    /* What the first visit alone did, before the poll: the summary and the tallies are taken here. */
    const first = panelReads().length;
    const visit = { ...summary(), byServer: tally(panelReads()), byHours: tallyBy(panelReads(), "hours"), pickerText: pickerTexts() };
    await advance(65000);
    const afterPoll = panelReads().slice(first);
    return {
      ...visit,
      afterPoll: tally(afterPoll), afterPollHours: tallyBy(afterPoll, "hours"), finalHash: location.hash,
    };
  },
  /* A first visit by a partial name that fits two servers: the page says so and sends no panel read after the card answers. */
  async partialAmbiguous() {
    startScenario(Number(process.env.OWN_MS || 700));
    await boot(hashFor("use1", ROUTE));
    await advance(4000);
    const late = panelReads().filter((r) => serverOf(r) !== null && r.at - 1_000_000 > Number(process.env.OWN_MS || 700));
    return { ...summary(), startedAfterTheCard: late.length, finalHash: location.hash };
  },
  /* A first visit, then a real navigation to another server while its reads are still out. */
  async leaveForAnotherServer() {
    startScenario(Number(process.env.OWN_MS || 700));
    await boot(hashFor("use1", "alpha"));
    await advance(1000);
    const before = summary();
    await go("#/s/use1/server/beta" + (tabArg ? "/" + tabArg : ""));
    await advance(10);
    const alpha = panelReads().filter((r) => r.url.includes("server=alpha"));
    return { startedForAlpha: alpha.length, abortedForAlpha: alpha.filter((r) => r.aborted).length, answeredForAlpha: alpha.filter((r) => r.answeredAt != null).length, abortedBeforeLeaving: before.aborted.length };
  },
};
if (!Object.hasOwn(scenarios, scenario)) throw new Error("unknown scenario " + scenario);
const result = await scenarios[scenario]();
if (process.env.DBG) console.error(fetchLog.map((r) => (r.at - 1_000_000) + " " + r.url.slice(0, 70) + " sig=" + !!r.signal + " ab=" + r.aborted + " ans=" + r.answeredAt).join("\n"));
console.log(JSON.stringify(result));
process.exit(0);
