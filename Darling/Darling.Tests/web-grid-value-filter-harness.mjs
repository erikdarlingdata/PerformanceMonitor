/* #5565: runs the shipped table renderer's value-list column filter (wwwroot/js/panels.js) on a small fake DOM and a fake
   localStorage, and prints what a scenario did as one line of JSON. GridValueFilterBehaviourTests starts it as
       node web-grid-value-filter-harness.mjs <path to the js folder> <scenario> [<fixture file> <case id>] */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); } }; this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.value = ""; }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  click() { this.fire("click", { target: this }); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  focus() { globalThis.document.activeElement = this; this.fire("focus"); }
  contains(n) { for (; n; n = n.parent) if (n === this) return true; return false; }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
let downloads = [];
globalThis.document = { activeElement: null, body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
const origCreate = URL.createObjectURL;
URL.createObjectURL = (blob) => { downloads.push({ blob }); return "blob:fake"; };
URL.revokeObjectURL = () => {};
const origAppend = body.appendChild.bind(body);
body.appendChild = (a) => { if (a.tag === "a") a.click = () => { downloads[downloads.length - 1].name = a.download; }; return origAppend(a); };

/* A fake localStorage: `fail` makes a read or a write throw, as a browser that refuses storage does. */
const makeStorage = () => {
  const items = new Map();
  const s = { items, failRead: false, failWrite: false, reads: 0, writes: 0 };
  s.getItem = (k) => { s.reads++; if (s.failRead) throw new Error("storage refused"); return items.has(k) ? items.get(k) : null; };
  s.setItem = (k, v) => { s.writes++; if (s.failWrite) throw new Error("quota"); items.set(k, String(v)); };
  s.removeItem = (k) => { if (s.failWrite) throw new Error("quota"); items.delete(k); };
  return s;
};
let storage = makeStorage();
globalThis.localStorage = storage;
const warnings = [];
console.warn = (m) => warnings.push(String(m));

const root = process.argv[2];
let generation = 0;
/* A fresh copy of panels.js: its module state (the filters in the page) starts empty, as after a restart of the browser. */
const loadPanels = async () => await import(pathToFileURL(root + "/panels.js").href + "?gen=" + generation++);
let mod = await loadPanels();
let VIZ = mod.VIZ;
const reload = async () => { mod = await loadPanels(); VIZ = mod.VIZ; };

const cols = [
  { key: "u", label: "Login" },
  { key: "i", label: "Ix", format: "int" },
];
let site = 0;
const newRoute = () => { globalThis.location = { hash: "#/server/a/vf" + site++ }; };
const buildRows = (values) => values.map((u, i) => ({ u, i }));
const build = (values, extraCols) => VIZ.table({ rows: buildRows(values) }, { rowsKey: "rows", columns: extraCols || cols, title: "VF", tools: false });
const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const ths = (w) => w.find("table").find("thead").find("tr").children;
const tbodyOf = (w) => w.find("table").children[1];
const filterBtn = (w, i) => ths(w)[i].children.find((c) => c.tag === "button");
const open = (w, i) => filterBtn(w, i).click();
const byClass = (w, cls) => all(w).filter((n) => (n.className || "").split(" ").includes(cls));
const textBox = (w) => byClass(w, "grid-filter-input")[0];
const searchBox = (w) => byClass(w, "gfv-search")[0];
const chips = (w) => byClass(w, "grid-filter-chip").map((n) => n.textContent);
const count = (w) => (byClass(w, "grid-filter-count")[0] || { textContent: "" }).textContent;
const valueItems = (w) => byClass(w, "gfv-item").filter((n) => !n.className.split(" ").includes("gfv-all"));
const itemBox = (n) => n.children.find((c) => c.tag === "input");
const itemText = (n) => n.children.find((c) => c.tag === "span").textContent;
const allBox = (w) => itemBox(byClass(w, "gfv-all")[0]);
const shownIx = (w) => tbodyOf(w).children.filter((t) => t.style.display !== "none").map((t) => Number(t.children[1].textContent.replace(/,/g, "")));
const type = (w, text) => { const b = textBox(w); b.value = text; b.fire("input"); };
const search = (w, text) => { const b = searchBox(w); b.value = text; b.fire("input"); };
const tick = (item, on) => { const b = itemBox(item); b.checked = on; b.fire("change"); };
const findItem = (w, value) => valueItems(w).find((n) => !n.className.split(" ").includes("gfv-blank") && itemText(n).toUpperCase() === String(value).toUpperCase());
const blankItem = (w) => valueItems(w).find((n) => n.className.split(" ").includes("gfv-blank"));
const notes = (w) => byClass(w, "gfv-note").map((n) => n.textContent).filter((t) => t !== "");
/* What a reader's next visit would see: the kept copy of the filters, read the way the page reads it. */
const stored = () => { const t = storage.items.get("pm.gridFilters.v1"); return t === undefined ? null : JSON.parse(t); };
const storedState = () => {
  const doc = stored();
  if (!doc) return { mode: "None", values: [], blank: false };
  for (const [, cols2] of doc.grids) for (const f of Object.values(cols2)) if (f.values) return { mode: f.values.mode === "hide" ? "Hide" : "ShowOnly", values: f.values.set, blank: f.values.blank };
  return { mode: "None", values: [], blank: false };
};

const out = {};
const scenarios = {
  /* One case of the shared fixture: every step's actual figures, for the test to compare with the step's expect. */
  async fixtureCase() {
    const { readFileSync } = await import("node:fs");
    const fixture = JSON.parse(readFileSync(process.argv[4], "utf8"));
    const kase = fixture.cases.find((c) => String(c.id) === process.argv[5]);
    newRoute();
    const gen = (rows) => (Array.isArray(rows) ? rows : Array.from({ length: rows.generate.count }, (_, i) => rows.generate.prefix + i));
    let data = gen(kase.rows);
    let w = build(data);
    open(w, 0);
    out.steps = [];
    for (const step of kase.steps) {
      switch (step.action) {
        case "open": break;
        case "search": search(w, step.text); break;
        case "untick":
          for (const v of step.values) tick(findItem(w, v), false);
          if (step.blank) tick(blankItem(w), false);
          break;
        case "tick":
          for (const v of step.values) tick(findItem(w, v), true);
          if (step.blank) tick(blankItem(w), true);
          break;
        case "tickOnly": {
          const b = allBox(w); b.checked = false; b.fire("change");
          for (const v of step.values) tick(findItem(w, v), true);
          if (step.blank) tick(blankItem(w), true);
          break;
        }
        case "selectAll": { const b = allBox(w); b.checked = step.ticked; b.fire("change"); break; }
        case "refresh":
          data = step.rows ? gen(step.rows) : [...data, ...step.addRows];
          w = build(data);
          break;
        case "textMatch": {
          const sel = byClass(w, "grid-filter-op")[0];
          if (sel) { sel.value = step.operator.toLowerCase(); sel.fire("change"); }
          type(w, step.text);
          break;
        }
        default: throw new Error("unknown action " + step.action);
      }
      const items = valueItems(w);
      const real = items.filter((n) => !n.className.split(" ").includes("gfv-blank"));
      const n = notes(w);
      out.steps.push({
        action: step.action,
        listBlank: items.some((n2) => n2.className.split(" ").includes("gfv-blank")),
        listValues: real.map(itemText),
        listCount: items.length,
        note: n.length ? n[0] : null,
        state: storedState(),
        shown: shownIx(w).map((ix) => data[ix]),
        shownCount: shownIx(w).length,
      });
    }
  },
  /* A value is built from text only: markup in a cell is a name to tick, never an element. */
  textOnly() {
    newRoute();
    const evil = ["<b>x</b>", "<img src=x onerror=alert(1)>", "plain"];
    const w = build(evil);
    open(w, 0);
    out.items = valueItems(w).map(itemText);
    out.elements = all(w).filter((n) => ["b", "img", "script"].includes(n.tag)).length;
    out.labels = valueItems(w).map((n) => itemBox(n).attrs["aria-label"]);
    tick(findItem(w, "<b>x</b>"), false);
    out.chip = chips(w);
    out.shown = shownIx(w);
  },
  /* Which columns get a list: text only, not opted out, nothing over 256 characters. */
  offered() {
    const has = (w, i) => { open(w, i); const r = byClass(w, "grid-filter-values").length > 0; filterBtn(w, i).click(); return r; };
    const mix = [
      { key: "u", label: "Login" },
      { key: "i", label: "Ix", format: "int" },
      { key: "t", label: "Seen", format: "time" },
      { key: "q", label: "Query", valueList: false },
      { key: "l", label: "Long" },
      { key: "e", label: "Edge" },
    ];
    const rows = [
      { u: "sa", i: 1, t: "2026-01-01T00:00:00Z", q: "select 1", l: "x".repeat(257), e: "y".repeat(256) },
      { u: "app", i: 2, t: "2026-01-02T00:00:00Z", q: "select 2", l: "short", e: "z" },
    ];
    newRoute();
    const w = VIZ.table({ rows }, { rowsKey: "rows", columns: mix, title: "Offered", tools: false });
    out.offered = mix.map((_, i) => has(w, i));
  },
  /* Escape and focus: the list lives inside the filter box, so it closes with it. */
  async keyboard() {
    newRoute();
    const w = build(["sa", "app"]);
    open(w, 0);
    out.labels = [searchBox(w).attrs["aria-label"], allBox(w).attrs["aria-label"], byClass(w, "gfv-list")[0].attrs["aria-label"]];
    const panel = textBox(w).parent;
    panel.fire("keydown", { key: "Escape" });
    out.closed = byClass(w, "gfv-search").length === 0;
    out.refocus = document.activeElement === filterBtn(w, 0);
  },
  /* The kept copy: a round trip, a restart, Clear all, the bounds, and a store that is bad or refused. */
  async kept() {
    newRoute();
    const hash = globalThis.location.hash;
    let w = build(["sa", "app", "job_svc", ""]);
    open(w, 0);
    tick(findItem(w, "job_svc"), false);
    out.stored = stored();
    out.chip = chips(w);
    // a refresh and a tab switch (another route and back) in the same page
    w = build(["sa", "app", "job_svc", "", "newuser"]);
    out.afterRefresh = shownIx(w);
    globalThis.location = { hash: "#/server/a/other-tab" };
    out.otherTab = shownIx(build(["sa", "app", "job_svc", ""]));
    globalThis.location = { hash };
    out.backOnTab = shownIx(build(["sa", "app", "job_svc", ""]));
    // a restart: a new copy of the page script reads the same storage
    await reload();
    w = build(["sa", "app", "job_svc", ""]);
    out.afterRestart = shownIx(w);
    out.restartChip = chips(w);
    out.restartButtonOn = filterBtn(w, 0).getAttribute("aria-pressed");
    open(w, 0);
    out.restartTicks = valueItems(w).map((n) => [itemText(n), itemBox(n).checked]);
    // Clear all removes the page's copy and the kept one
    byClass(w, "grid-filter-clear-all")[0].click();
    out.afterClearAll = shownIx(w);
    out.storedAfterClearAll = stored();
    out.keyAfterClearAll = storage.items.has("pm.gridFilters.v1");
    await reload();
    out.restartAfterClearAll = shownIx(build(["sa", "app", "job_svc", ""]));
  },
  async keptBad() {
    const bads = ["{not json", "[]", "null", "42", '{"v":2,"grids":[]}', '{"v":1,"grids":"x"}', '{"v":1,"grids":[["k",5],[7,{}],["k2",{"c":{"op":"contains","text":"","values":{"mode":"hide","set":[1],"blank":false}}}]]}'];
    out.drawn = [];
    for (const b of bads) {
      storage = makeStorage();
      globalThis.localStorage = storage;
      storage.items.set("pm.gridFilters.v1", b);
      await reload();
      newRoute();
      out.drawn.push(shownIx(build(["a", "b"])).length);
    }
    // a refusing store: reading, then writing, never stops the grid; the warning comes once
    storage = makeStorage();
    globalThis.localStorage = storage;
    storage.failRead = true;
    storage.failWrite = true;
    await reload();
    newRoute();
    const w = build(["sa", "app", "job_svc"]);
    open(w, 0);
    tick(findItem(w, "job_svc"), false);
    tick(findItem(w, "app"), false);
    out.refusedShown = shownIx(w);
    out.warnings = warnings.length;
    // no storage object at all
    delete globalThis.localStorage;
    await reload();
    newRoute();
    const w2 = build(["sa", "app", "job_svc"]);
    open(w2, 0);
    tick(findItem(w2, "job_svc"), false);
    out.noStorageShown = shownIx(w2);
  },
  async keptBounds() {
    // 250 stored grids: only the newest 200 are kept once the page writes
    const grids = Array.from({ length: 250 }, (_, i) => ["#/server/a/g" + i + "|VF|" + "k", { c: { op: "contains", text: "t" + i } }]);
    storage.items.set("pm.gridFilters.v1", JSON.stringify({ v: 1, grids }));
    await reload();
    newRoute();
    const w = build(["sa", "app"]);
    open(w, 0);
    tick(findItem(w, "app"), false);
    const doc = stored();
    out.gridCount = doc.grids.length;
    out.firstKept = doc.grids[0][0];
    out.lastKept = doc.grids[doc.grids.length - 1][0].startsWith("#/server/a/vf");
    // a value part with more than 1,000 values is not kept (a cut set would hide the wrong rows)
    storage.items.clear();
    await reload();
    newRoute();
    const many = Array.from({ length: 3000 }, (_, i) => "x" + i);
    const w2 = build(many);
    open(w2, 0);
    const a = allBox(w2); a.checked = false; a.fire("change");
    search(w2, "x2");
    const b = allBox(w2); b.checked = true; b.fire("change");
    out.bigShown = shownIx(w2).length;
    out.bigStored = stored();
    out.bigChip = chips(w2);
    // a stored set longer than 1,000 is refused on the way in too
    storage.items.set("pm.gridFilters.v1", JSON.stringify({ v: 1, grids: [["k", { c: { op: "contains", text: "", values: { mode: "hide", set: Array.from({ length: 1001 }, (_, i) => "v" + i), blank: false } } }]] }));
    await reload();
    newRoute();
    out.refusedLong = shownIx(build(["a", "b"])).length;
  },
  /* Chips and the pressed state read as "hides N values" / "shows N values", beside a text match. */
  chips() {
    newRoute();
    const w = build(["sa", "app", "job_svc", "etl", "x"]);
    open(w, 0);
    tick(findItem(w, "job_svc"), false);
    out.one = chips(w);
    tick(findItem(w, "etl"), false);
    out.two = chips(w);
    out.pressed = filterBtn(w, 0).getAttribute("aria-pressed");
    const b = allBox(w); b.checked = false; b.fire("change");
    tick(findItem(w, "sa"), true);
    tick(findItem(w, "x"), true);
    out.shows = chips(w);
    type(w, "x");
    out.both = chips(w);
    out.shownBoth = shownIx(w);
    out.countText = count(w);
    byClass(w, "grid-filter-x")[0].click();
    out.afterX = shownIx(w);
    out.afterXStored = stored();
  },
  /* Select All covers what the search matches, the box mirrors the ticks, and the notes read right. */
  selectAllUnderSearch() {
    newRoute();
    const w = build(["sa", "app", "APP", "app2", "job_svc", ""]);
    open(w, 0);
    search(w, "app");
    out.listed = valueItems(w).map(itemText);
    const b = allBox(w); b.checked = false; b.fire("change");
    out.state = storedState();
    out.shown = shownIx(w);
    out.allChecked = allBox(w).checked;
    out.allIndeterminate = allBox(w).indeterminate;
    search(w, "");
    out.allIndeterminateFull = allBox(w).indeterminate;
    out.blankTicked = itemBox(blankItem(w)).checked;
    search(w, "zzz");
    out.noMatch = byClass(w, "gfv-empty").map((n) => n.textContent);
  },
  /* A refresh in place (Alert History) re-lists the open box. */
  reconciled() {
    newRoute();
    const w = build(["sa", "app"]);
    open(w, 0);
    out.before = valueItems(w).map(itemText);
    const tbody = tbodyOf(w);
    tbody.children[0].children[0].textContent = "zed";
    mod.reapplyGridSort(tbody);
    out.after = valueItems(w).map(itemText);
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
