/* #4774: runs the web viewer's real page scheduler (wwwroot/js/app.js) on a virtual clock and prints what a
   scenario measured as one line of JSON. WebRenderSettleTests starts it as
       node web-scheduler-harness.mjs <path to app.js> <path to refresh-policy.js> <scenario>
   app.js is an entry-point module, so its import lines become the stand-ins below and its closing start() call is
   made by the scenario, which decides when the page boots. Everything else is app.js as shipped, and the back-off
   rule is the real refresh-policy.js. Time is virtual: one step is one second, reads finish at the step they are
   due, and then the 1-second scheduler tick fires. */
import fs from "node:fs";
import vm from "node:vm";
import { pathToFileURL } from "node:url";

const [appPath, policyPath, scenario] = process.argv.slice(-3);
const policy = await import(pathToFileURL(policyPath).href);

const stripped = fs.readFileSync(appPath, "utf8")
  .replace(/^import .*;[ \t]*\r?$/gm, "")
  .replace(/^start\(\);[ \t]*\r?$/m, "");
if (/^import /m.test(stripped) || /^start\(\);/m.test(stripped)) {
  throw new Error("app.js layout changed: the harness cannot separate its imports and its start() call");
}

const MINUTE = 60000;
const PAUSED_KEY = "darling.autoRefreshPaused";

let now = 0;
let inFlight = 0;
let readMs = 2000;
const pending = [];
const timers = [];
const renders = [];
const settledAt = [];
const saved = new Map();
const listeners = { document: {}, window: {} };
const elements = new Map();

function element(id) {
  if (!elements.has(id)) {
    const handlers = {};
    elements.set(id, {
      id,
      handlers,
      textContent: "",
      setAttribute() {},
      addEventListener(type, fn) { (handlers[type] ??= []).push(fn); },
      querySelectorAll() { return []; },
      classList: { toggle() {} },
    });
  }
  return elements.get(id);
}

/* A page render opens one read that stays out for readMs of virtual time. */
function renderPage() {
  renders.push({ at: now });
  inFlight++;
  pending.push(now + readMs);
}

const sandbox = {
  document: {
    hidden: false,
    getElementById: element,
    querySelectorAll: () => [],
    addEventListener(type, fn) { (listeners.document[type] ??= []).push(fn); },
  },
  window: { addEventListener(type, fn) { (listeners.window[type] ??= []).push(fn); } },
  location: { hash: "#/fleet" },
  localStorage: {
    getItem: (k) => (saved.has(k) ? saved.get(k) : null),
    setItem: (k, v) => { saved.set(k, String(v)); },
    removeItem: (k) => { saved.delete(k); },
  },
  setInterval: (fn, every) => { timers.push({ fn, every, next: now + every }); },
  __now: () => now,
  el: () => ({}),
  mount() {},
  apiGet: async () => ({ kind: "empty" }),
  apiGetFleet: async () => ({ kind: "error" }),
  bandClass: () => "",
  localTime: String,
  hasInFlightReads: () => inFlight > 0,
  isSessionExpired: () => false,
  onSessionExpired() {},
  navigateServer() {},
  renderFleet: renderPage,
  renderAg: renderPage,
  renderSweeps: renderPage,
  renderServer: renderPage,
  renderAlerts: renderPage,
  renderAlertRuleList: renderPage,
  renderViewList: renderPage,
  renderView: renderPage,
  renderTriage: renderPage,
  renderEditor: renderPage,
  renderNotebookEditor: renderPage,
  renderAlertEditor: renderPage,
  currentViewRefresh: () => null,
  onViewRefreshChange() {},
  clearViewRefresh() {},
  REFRESH_CHOICES: policy.REFRESH_CHOICES,
  nextRefreshDelayMs: policy.nextRefreshDelayMs,
  isBackedOff: policy.isBackedOff,
  defaultRefreshChoice: policy.defaultRefreshChoice,
  refreshLabel: policy.refreshLabel,
  PAGE_REFRESH_CHOICES: policy.PAGE_REFRESH_CHOICES,
  loadPageRefreshChoice: policy.loadPageRefreshChoice,
  savePageRefreshChoice: policy.savePageRefreshChoice,
  buildPageRefreshControl: () => ({ root: {}, select: {} }),
  getSession: async () => null,
  listViews: async () => [],
};

vm.createContext(sandbox);
vm.runInContext("Date.now = () => __now();", sandbox);
vm.runInContext(stripped, sandbox, { filename: "app.js" });
/* The sidebar, view list and AG nav are shell reads that the page render never waits on. */
vm.runInContext("refreshSidebar = async () => {}; refreshViewList = async () => {}; refreshAgNav = async () => {};", sandbox);

const get = (name) => vm.runInContext(name, sandbox);
const emit = (target, type) => { for (const fn of listeners[target][type] ?? []) fn({}); };

function step() {
  now += 1000;
  for (let i = pending.length - 1; i >= 0; i--) {
    if (pending[i] <= now) { pending.splice(i, 1); inFlight--; }
  }
  const wasRendering = get("pageRendering");
  for (const t of timers) { while (t.next <= now) { t.next += t.every; t.fn(); } }
  if (wasRendering && !get("pageRendering")) settledAt.push(now);
}
const advance = (ms) => { for (let t = 0; t < ms; t += 1000) step(); };
const hide = () => { sandbox.document.hidden = true; emit("document", "visibilitychange"); };
const show = () => { sandbox.document.hidden = false; emit("document", "visibilitychange"); };
const click = (id) => { for (const fn of element(id).handlers.click ?? []) fn({}); };
const boot = () => vm.runInContext("start()", sandbox);

/* What the page looks like at this instant. nextRefreshInMs is how far past the last settle the next refresh sits. */
const snapshot = () => ({
  at: now,
  rendering: get("pageRendering"),
  lastRenderMs: get("pageLastRenderMs"),
  renders: renders.length,
  secondRenderAt: renders.length > 1 ? renders[1].at : null,
  hint: element("refresh-hint").textContent,
  nextRefreshInMs: settledAt.length ? get("pageNextRefreshAt") - settledAt[settledAt.length - 1] : null,
});

const scenarios = {
  /* The first render's reads are out when the tab hides; they finish 2 s later; the tab stays hidden 10 minutes. */
  hiddenTab() {
    boot();
    hide();
    advance(10 * MINUTE);
    const away = snapshot();
    show();
    advance(5000);
    return { away, back: snapshot() };
  },
  /* The same, but the tab is back before the page is due again (2 s render + 60 s interval = due at 62 s). */
  hiddenBriefly() {
    boot();
    hide();
    advance(20000);
    const away = snapshot();
    show();
    advance(20000);
    const early = snapshot();
    advance(23000);
    return { away, early, back: snapshot() };
  },
  /* A page opened in a background tab: it was hidden before it ever rendered. */
  backgroundTab() {
    sandbox.document.hidden = true;
    boot();
    advance(10 * MINUTE);
    const away = snapshot();
    show();
    advance(5000);
    return { away, back: snapshot() };
  },
  /* Auto-refresh paused and resumed with this tab's own button; resuming forces one refresh at once. */
  pausedWithButton() {
    boot();
    click("auto-refresh-toggle");
    advance(10 * MINUTE);
    const away = snapshot();
    click("auto-refresh-toggle");
    advance(5000);
    return { away, back: snapshot() };
  },
  /* The pause flag lives in localStorage, so another tab can set and clear it: this tab gets no click to react to. */
  pausedByAnotherTab() {
    boot();
    saved.set(PAUSED_KEY, "1");
    advance(10 * MINUTE);
    const away = snapshot();
    saved.delete(PAUSED_KEY);
    advance(5000);
    return { away, back: snapshot() };
  },
  /* A render that is still loading, with no hide and no pause: 45 s of reads on a 60 s page. */
  stillLoading() {
    readMs = 45000;
    boot();
    advance(44000);
    const away = snapshot();
    advance(2000);
    return { away, back: snapshot() };
  },
};

if (!Object.hasOwn(scenarios, scenario)) throw new Error("unknown scenario " + scenario);
console.log(JSON.stringify(scenarios[scenario]()));
