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
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
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
};
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
scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
