/* #7393: runs the shipped keepScroll (wwwroot/js/refresh-in-place.js) over grids the real shared table renderer (panels.js,
   VIZ.table) draws on a small fake DOM, and prints what a scenario found as one line of JSON. GridScrollKeepTests starts it as
       node web-grid-scroll-harness.mjs <path to the js folder> <scenario>
   Only the DOM is a stand-in. The fake wrap clamps scrollTop to its content like a browser box does: the content is
   `rowHeight` per body row, the box is `boxHeight` tall. */
import { pathToFileURL } from "node:url";

const rowHeight = 30;
const boxHeight = 300;

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.dataset = {}; this.style = {}; this._top = 0; this._left = 0; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  get isWrap() { return (" " + this.className + " ").includes(" table-wrap "); }
  get maxTop() { return Math.max(0, this.find("tbody").children.length * rowHeight - boxHeight); }
  get scrollTop() { return this._top; }
  set scrollTop(v) { this._top = Math.max(0, Math.min(v, this.maxTop)); }
  get scrollLeft() { return this._left; }
  set scrollLeft(v) { this._left = Math.max(0, v); }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  insertBefore(c, ref) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; const i = ref ? this.children.indexOf(ref) : -1; if (i < 0) this.children.push(c); else this.children.splice(i, 0, c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn, capture) { (this.listeners[t] ||= []).push({ fn, capture: capture === true }); }
  removeEventListener(t, fn, capture) { this.listeners[t] = (this.listeners[t] || []).filter((l) => !(l.fn === fn && l.capture === (capture === true))); }
  /* A DOM event: the capture listeners from the root down, the target's own, then the bubbling ones back up. */
  fire(type, init = {}) {
    const ev = { type, target: this, preventDefault() {}, ...init };
    const path = []; for (let n = this; n; n = n.parent) path.unshift(n);
    const run = (n, capture) => (n.listeners[type] || []).filter((l) => l.capture === capture).forEach((l) => l.fn(ev));
    path.slice(0, -1).forEach((n) => run(n, true));
    (this.listeners[type] || []).slice().forEach((l) => l.fn(ev));
    path.slice(0, -1).reverse().forEach((n) => run(n, false));
  }
  get classList() {
    const self = this;
    return {
      add(c) { if (!(" " + self.className + " ").includes(" " + c + " ")) self.className = (self.className + " " + c).trim(); },
      remove(c) { self.className = self.className.split(" ").filter((x) => x && x !== c).join(" "); },
    };
  }
  /* Only the class selectors the pages use here (".name"). */
  querySelectorAll(sel, acc = []) { for (const c of this.children) { if ((" " + c.className + " ").includes(" " + sel.slice(1) + " ")) acc.push(c); c.querySelectorAll(sel, acc); } return acc; }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  closest(sel) { for (let n = this; n; n = n.parent) if (sel === "[data-grid]" && n.getAttribute && n.getAttribute("data-grid") !== null) return n; return null; }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
  wraps(acc = []) { for (const c of this.children) { if (c.isWrap) acc.push(c); c.wraps && c.wraps(acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const page = new FakeNode("main");
globalThis.document = {
  createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t),
  querySelectorAll: (sel) => (sel === ".table-wrap" ? page.wraps() : []),
  documentElement: { offsetHeight: 0 },
  getElementById: () => page,
  /* The Locking tab listens for Escape on the document. */
  listeners: {},
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); },
  removeEventListener(t, fn) { this.listeners[t] = (this.listeners[t] || []).filter((f) => f !== fn); },
  fire(t, ev) { (this.listeners[t] || []).slice().forEach((f) => f({ type: t, ...ev })); },
};
globalThis.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };
/* A poll's staged page waits on MutationObserver (whenSettled): the stand-in runs its callback when the scenario says the reads returned. */
const observers = [];
globalThis.MutationObserver = class { constructor(cb) { this.cb = cb; this.off = false; observers.push(this); } observe() {} disconnect() { this.off = true; } };
const readsReturned = () => observers.filter((o) => !o.off).forEach((o) => o.cb());
globalThis.location = { hash: "#/server/a/queries" };
globalThis.scrollY = 0;
globalThis.scrollTo = () => {};

const root = process.argv[2];
const { VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { keepScroll, showPageWhenSettled } = await import(pathToFileURL(root + "/refresh-in-place.js").href);

const cols = [{ key: "name", label: "Name" }, { key: "n", label: "N", format: "int" }];
const makeRows = (count) => Array.from({ length: count }, (_, i) => ({ name: "r" + i, n: i }));
const grid = (id, count) => VIZ.table({ rows: makeRows(count) }, { rowsKey: "rows", columns: cols, title: id, id });
const wrapOf = (node) => (node.isWrap ? node : node.wraps()[0]);
/* A poll draws a new grid in place of the old one, as swapUnit does: the old box goes, a fresh one (scrolled to the top) takes its place. */
const swap = (oldNode, count, id) => { const fresh = grid(id, count); page.insertBefore(fresh, oldNode); page.removeChild(oldNode); return fresh; };
const out = {};

const scenarios = {
  swapKeepsPosition() {
    const g = grid("waits", 100); page.appendChild(g);
    wrapOf(g).scrollTop = 700; wrapOf(g).scrollLeft = 40;
    const fresh = keepScrollSwap(g, 100, "waits");
    out.top = wrapOf(fresh).scrollTop; out.left = wrapOf(fresh).scrollLeft;
  },
  shorterGridClamps() {
    const g = grid("waits", 100); page.appendChild(g);
    wrapOf(g).scrollTop = 2000;
    const fresh = keepScrollSwap(g, 20, "waits"); // 20 rows is 600 high in a 300 box: the most it can scroll is 300
    out.top = wrapOf(fresh).scrollTop; out.max = wrapOf(fresh).maxTop;
  },
  twoGridsOneName() {
    const a = grid("waits", 100); const b = grid("waits", 100); page.appendChild(a); page.appendChild(b);
    wrapOf(a).scrollTop = 100; wrapOf(b).scrollTop = 900;
    keepScroll(() => { const fa = grid("waits", 100); const fb = grid("waits", 100); page.insertBefore(fa, a); page.insertBefore(fb, a); page.removeChild(a); page.removeChild(b); });
    const w = page.wraps();
    out.first = w[0].scrollTop; out.second = w[1].scrollTop;
  },
  otherGridUntouched() {
    const a = grid("waits", 100); const b = grid("other", 100); page.appendChild(a); page.appendChild(b);
    wrapOf(a).scrollTop = 500; wrapOf(b).scrollTop = 300; // b is scrolled too and is not swapped: it has to stay at 300
    const fa = keepScrollSwap(a, 100, "waits");
    out.swapped = wrapOf(fa).scrollTop; out.other = wrapOf(b).scrollTop; out.sameBox = page.wraps()[1] === wrapOf(b);
  },
  neighbourSlidesIntoReplacedSlot() {
    /* Two grids with one name, the first scrolled to 500 and the second to 300. The poll swaps the first for a grid with another name, so
       the second one now stands in the first's place (waits|0) and must keep its own 300, not be handed the first's 500. */
    const a = grid("waits", 100); const b = grid("waits", 100); page.appendChild(a); page.appendChild(b);
    wrapOf(a).scrollTop = 500; wrapOf(b).scrollTop = 300;
    const fresh = keepScrollSwap(a, 100, "io");
    out.b = wrapOf(b).scrollTop; out.sameBox = page.wraps()[1] === wrapOf(b); out.fresh = wrapOf(fresh).scrollTop;
  },
  wrapRemovedAheadOfSameName() {
    /* Two panels draw grids with one name. The first one's poll returns no rows, so it draws a strip and has no wrap left: the
       second takes its place (waits|0) and must not be given the first's position. */
    const a = grid("waits", 100); const b = grid("waits", 100); page.appendChild(a); page.appendChild(b);
    wrapOf(a).scrollTop = 500; wrapOf(b).scrollTop = 200;
    keepScroll(() => { const strip = new FakeNode("div"); strip.className = "strip"; page.insertBefore(strip, a); page.removeChild(a); });
    out.wraps = page.wraps().length; out.b = wrapOf(b).scrollTop; out.sameBox = page.wraps()[0] === wrapOf(b);
  },
  /* A page whose grid fills from a read of its own (Finops, Fleet Sweeps, Proposals) and is polled: the page on screen stays until the
     poll's reads have drawn, then the new page goes in with the grid back where the reader had it. */
  pollWaitsThenKeepsPosition() {
    const first = new FakeNode("div"); first.appendChild(grid("waits", 100));
    showPageWhenSettled(page, "k", [first], false);
    const live = page.wraps()[0]; live.scrollTop = 700;
    const staged = new FakeNode("div"); const strip = new FakeNode("div"); strip.className = "strip loading"; staged.appendChild(strip);
    showPageWhenSettled(page, "k", [staged], true);
    out.during = page.wraps()[0] === live; out.duringTop = live.scrollTop;
    staged.removeChild(strip); staged.appendChild(grid("waits", 100));
    readsReturned();
    out.replaced = page.wraps()[0] !== live; out.top = page.wraps()[0].scrollTop; out.wraps = page.wraps().length;
  },
  pollForAPageTheReaderLeftIsDropped() {
    const first = new FakeNode("div"); first.appendChild(grid("waits", 100));
    showPageWhenSettled(page, "k", [first], false);
    const staged = new FakeNode("div"); const strip = new FakeNode("div"); strip.className = "strip loading"; staged.appendChild(strip);
    showPageWhenSettled(page, "k", [staged], true);
    const other = new FakeNode("div"); other.className = "other-page";
    showPageWhenSettled(page, "other", [other], false);
    staged.removeChild(strip); staged.appendChild(grid("waits", 100));
    readsReturned();
    out.wraps = page.wraps().length; out.stillOther = page.children[0] === other;
  },
  aPageThatIsNotThePollsOwnShowsAtOnce() {
    const first = new FakeNode("div"); first.appendChild(grid("waits", 100));
    showPageWhenSettled(page, "k", [first], false);
    const staged = new FakeNode("div"); const strip = new FakeNode("div"); strip.className = "strip loading"; staged.appendChild(strip);
    showPageWhenSettled(page, "k", [staged], false); // a sub-tab click or span change, not the poll
    out.shown = page.children[0] === staged; out.wraps = page.wraps().length;
  },
  /* #7393: the reader acts on the page on screen while a poll's page is staged. What they did is newer than the state the staged page was
     built from, so the waiting swap is dropped and the page they acted on stays. */
  readerClickDropsTheWaitingSwap() {
    const first = new FakeNode("div"); first.appendChild(grid("waits", 100));
    showPageWhenSettled(page, "k", [first], false);
    const live = page.wraps()[0];
    const staged = new FakeNode("div"); const strip = new FakeNode("div"); strip.className = "strip loading"; staged.appendChild(strip);
    showPageWhenSettled(page, "k", [staged], true);
    first.fire("click"); // a click on the live page
    staged.removeChild(strip); staged.appendChild(grid("waits", 100));
    readsReturned();
    out.liveStays = page.wraps()[0] === live; out.firstStays = page.children[0] === first;
  },
  readerEnterOnARowDropsTheWaitingSwap() {
    const first = new FakeNode("div"); first.appendChild(grid("waits", 100));
    showPageWhenSettled(page, "k", [first], false);
    const staged = new FakeNode("div"); const strip = new FakeNode("div"); strip.className = "strip loading"; staged.appendChild(strip);
    showPageWhenSettled(page, "k", [staged], true);
    first.fire("keydown", { key: "Enter" });
    staged.removeChild(strip); staged.appendChild(grid("waits", 100));
    readsReturned();
    out.firstStays = page.children[0] === first;
  },
  aKeyThatChangesNothingLeavesTheWaitingSwap() {
    const first = new FakeNode("div"); first.appendChild(grid("waits", 100));
    showPageWhenSettled(page, "k", [first], false);
    const staged = new FakeNode("div"); const strip = new FakeNode("div"); strip.className = "strip loading"; staged.appendChild(strip);
    showPageWhenSettled(page, "k", [staged], true);
    first.fire("keydown", { key: "Tab" });
    staged.removeChild(strip); staged.appendChild(grid("waits", 100));
    readsReturned();
    out.swapped = page.children[0] === staged;
    out.listenersLeft = Object.values(page.listeners).reduce((n, l) => n + l.length, 0); // none waiting once the swap is done
  },
  /* The shipped Fleet Sweeps page (pages/sweeps.js) over stubbed reads: a poll starts while sweep A shows, the reader clicks timeline row B on
     the page on screen before the poll's reads return, then the reads return. The detail and the highlighted row must agree. */
  async sweepsClickDuringPoll() {
    const { localTime } = await import(pathToFileURL(root + "/util.js").href);
    const { renderSweeps } = await import(pathToFileURL(root + "/pages/sweeps.js").href);
    const sweeps = [
      { sweep_id: "1", swept_at: new Date(Date.now() - 10 * 60000).toISOString() },
      { sweep_id: "2", swept_at: new Date(Date.now() - 40 * 60000).toISOString() },
    ].map((x) => ({ ...x, instruments_alive: true, alerts_enabled: true, servers_reported: 1, servers_expected: 1, report: {} }));
    const held = []; let hold = false;
    globalThis.fetch = (path) => {
      const body = path === "/api/sweeps/latest" ? sweeps[0]
        : path.startsWith("/api/sweeps/") ? sweeps.find((x) => path.endsWith("/" + x.sweep_id))
        : path.startsWith("/api/sweeps?") ? { sweeps }
        : { status: "empty", message: "none" };
      const reply = { ok: true, status: 200, text: async () => JSON.stringify(body) };
      return hold ? new Promise((resolve) => held.push(() => resolve(reply))) : Promise.resolve(reply);
    };
    const flush = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0)); };
    const detailText = () => {
      const found = [];
      const walk = (n) => n.children.forEach((c) => { if (c.tag === "h3" && c.textContent.startsWith("Sweep at")) found.push(c.textContent); walk(c); });
      walk(page);
      return found[0] || "";
    };

    await renderSweeps(page, { poll: false }); await flush();
    out.firstIsLatest = detailText() === "Sweep at " + localTime(sweeps[0].swept_at);

    hold = true;
    renderSweeps(page, { poll: true }); // the poll: its detail read is built for "latest" at once
    hold = false;
    const rowB = page.querySelectorAll(".sweep-row")[1];
    rowB.fire("click"); // the reader picks sweep B on the page on screen
    held.forEach((release) => release());
    await flush(); readsReturned(); await flush();
    const rows = page.querySelectorAll(".sweep-row");
    out.detailIsB = detailText() === "Sweep at " + localTime(sweeps[1].swept_at);
    out.activeIsB = rows.length === 2 && !rows[0].className.includes("active") && rows[1].className.includes("active");
  },
  /* #7393: the shipped FinOps page (pages/finops.js) on its Locking & Contention tab over stubbed reads. A poll builds the next page while
     the old one stays on screen, so the page on screen must keep its signal, its reads and its Escape key for as long as it is on screen;
     a build that never lands must end; a build that lands ends the page it replaced. `fetch` honours the abort signal as a browser does. */
  async finopsPollLifecycle() {
    const { renderFinops } = await import(pathToFileURL(root + "/pages/finops.js").href);
    const { beginPageScope } = await import(pathToFileURL(root + "/util.js").href);
    const calls = []; const held = []; let hold = false;
    const objects = [{ database_name: "d", schema_name: "s", table_name: "t", index_name: "i" }];
    globalThis.fetch = (url, init) => {
      const u = String(url); const signal = init && init.signal;
      const tool = (u.match(/\/api\/read\/([a-z_]+)/) || [])[1];
      calls.push({ u, signal });
      const body = tool === "list_servers" ? { server_count: 1, servers: [{ server_name: "srv", display_name: "srv" }] }
        : u.startsWith("/api/server-databases") ? { server: "srv", databases: u.includes("search=") ? ["Db1", "Db2"] : ["Db1"], truncated: !u.includes("search=") }
        : tool === "get_object_locking" ? (u.includes("detail_table") ? { detail: {} } : { objects })
        : {};
      const reply = { status: 200, ok: true, text: async () => JSON.stringify(body) };
      if (!hold || tool === "list_servers") return Promise.resolve(reply);
      return new Promise((resolve, reject) => {
        held.push(() => resolve(reply));
        if (signal) signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
      });
    };
    const flush = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0)); };
    const sigOf = (prefix, n = 0) => calls.filter((c) => c.u.startsWith(prefix))[n].signal;
    const find = (pred, node = page, acc = []) => { if (pred(node)) acc.push(node); node.children.forEach((c) => find(pred, c, acc)); return acc; };
    const pane = () => page.querySelectorAll(".locking-detail")[0];
    const paneOpen = () => pane().textContent.includes("Index detail");

    renderFinops(page, "srv", "locking", { poll: false }); await flush();
    const liveSig = sigOf("/api/server-databases", 0);
    out.liveStartsAlive = liveSig.aborted === false;
    hold = true;
    find((n) => n.tag === "tr" && n.style.cursor === "pointer")[0].fire("click"); // the reader opens an index detail
    const detailSig = calls.filter((c) => c.u.includes("detail_table"))[0].signal;
    const liveHead = page.children[0];
    renderFinops(page, "srv", "locking", { poll: true }); await flush(); // the poll: its reads are held, so its page waits
    const stagedSig = sigOf("/api/server-databases", 1);
    out.stillTheLivePage = page.children[0] === liveHead;
    out.liveSignalWhileWaiting = liveSig.aborted; out.detailReadWhileWaiting = detailSig.aborted;
    out.stagedSignalWhileWaiting = stagedSig.aborted;
    out.paneOpenBeforeEscape = paneOpen();
    document.fire("keydown", { key: "Escape" });
    out.paneOpenAfterEscape = paneOpen();

    const scenario = process.argv[4];
    if (scenario === "drop") {
      /* The reader types in the live Database box: the swap is dropped, and the search answer must still draw into the box on screen. */
      hold = false; // the search is a read of its own, answered at once
      const box = find((n) => n.tag === "input" && n.attrs.type === "text")[0];
      box.value = "Db"; box.fire("input");
      out.stagedSignalAfterDrop = stagedSig.aborted; out.liveSignalAfterDrop = liveSig.aborted;
      await new Promise((r) => setTimeout(r, 330)); await flush();
      out.liveOptions = find((n) => n.tag === "datalist")[0].children.map((o) => o.attrs.value);
    } else if (scenario === "land") {
      held.forEach((release) => release()); await flush(); readsReturned();
      out.swapped = page.children[0] !== liveHead;
      out.liveSignalAfterSwap = liveSig.aborted; out.stagedSignalAfterSwap = stagedSig.aborted;
    } else if (scenario === "leave") {
      beginPageScope();
      out.liveSignalAfterLeave = liveSig.aborted; out.stagedSignalAfterLeave = stagedSig.aborted;
    }
  },
  topGridStaysTop() {
    const g = grid("waits", 100); page.appendChild(g);
    const fresh = keepScrollSwap(g, 100, "waits");
    out.top = wrapOf(fresh).scrollTop;
  },
  differentGridNotRestored() {
    const g = grid("waits", 100); page.appendChild(g);
    wrapOf(g).scrollTop = 600;
    const fresh = keepScrollSwap(g, 100, "io"); // another grid replaces it: nothing to restore
    out.top = wrapOf(fresh).scrollTop;
  },
};
function keepScrollSwap(oldNode, count, id) {
  let fresh;
  keepScroll(() => { fresh = swap(oldNode, count, id); });
  return fresh;
}

if (!scenarios[process.argv[3]]) { console.error("unknown scenario " + process.argv[3]); process.exit(2); }
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
