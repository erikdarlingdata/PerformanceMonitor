/* Runs the shipped server page (wwwroot/js/pages/server.js) with the real database-filter picker (pages/database-filter.js), the
   real multi-picker.js, viewer-local.js, util.js and panels.js on a small fake DOM and a recording fetch, and prints as one line of
   JSON what the page drew and what the panels would read (#5245). WebDatabaseFilterPickerBehaviourTests starts it as
       node web-database-filter-picker-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM, fetch, localStorage, charts.js and the tab registry (pages/server-tabs.js: one tab that records the filter it was
   built under) are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {}; this.value = ""; this.checked = false; this.disabled = false; this.hidden = false; this.isRoot = false;
  }
  get firstChild() { return this.children[0] || null; }
  get nextSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i >= 0 ? this.parent.children[i + 1] || null : null; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children.forEach((c) => (c.parent = null)); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c); else this.children.splice(i, 0, c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  remove() { if (this.parent) this.parent.removeChild(this); }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
  every(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.every(pred, acc); } return acc; }
  byClass(cls) { return this.every((n) => (" " + n.className + " ").includes(" " + cls + " ")); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/SRV1/cpu" };

const jsDir = process.argv[2];
const scenario = process.argv[3];

/* ── the browser's storage, optionally holding an earlier choice ── */
const KEY = "darling.local.dbFilter.v1";
const data = {};
globalThis.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null),
  setItem: (k, v) => { data[k] = String(v); },
  removeItem: (k) => { delete data[k]; },
};
const stored = () => (data[KEY] ? JSON.parse(data[KEY]).servers : {});
const storeChoice = (id, names) => { data[KEY] = JSON.stringify({ v: 1, servers: { [id]: names } }); };

/* ── the scripted fetch ── */
const urls = [];
let card = { server_id: 7, server_name: "SRV1", display_name: "SRV1", is_postgres: false, engine_description: "SQL Server" };
let fleetOk = true;
let inventory = [];
let inventoryStatus = 200;
let inventoryCut = false; // the route answered truncated: true
let searchHandler = null; // (text) => { names, cut, delay }: the route's answer to a request that carries search=
globalThis.fetch = async (url) => {
  const u = String(url);
  urls.push(u);
  if (u.startsWith("/api/fleet")) {
    if (!fleetOk) return { status: 500, ok: false, text: async () => JSON.stringify({ error: "down" }) };
    return { status: 200, ok: true, text: async () => JSON.stringify({ cards: [card] }) };
  }
  if (u.startsWith("/api/server-databases")) {
    if (inventoryStatus !== 200) return { status: inventoryStatus, ok: false, text: async () => JSON.stringify({ error: "no list" }) };
    const q = new URL(u, "http://x").searchParams.get("search");
    if (q !== null && searchHandler) {
      const a = searchHandler(q);
      if (a.delay) await new Promise((r) => setTimeout(r, a.delay));
      return { status: 200, ok: true, text: async () => JSON.stringify({ server: "SRV1", databases: a.names, truncated: a.cut === true }) };
    }
    return { status: 200, ok: true, text: async () => JSON.stringify({ server: "SRV1", databases: inventory, truncated: inventoryCut }) };
  }
  return { status: 200, ok: true, text: async () => "{}" };
};
const settle = () => new Promise((r) => setTimeout(r, 25));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "db-filter-picker-"));
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert; every stand-in below is
     written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  fs.writeFileSync(
    path.join(scratch, "pages", "server-tabs.js"),
    'import { el, getActiveDatabaseFilter } from "../util.js";\n' +
      "const tab = { id: 'cpu', label: 'CPU', build(server) { (globalThis.__builds ||= []).push({ server, filter: getActiveDatabaseFilter() }); return el('div', { class: 'stub-panel' }); } };\n" +
      "export const SERVER_TABS = [tab];\nexport const POSTGRES_TABS = [tab];\n" +
      "export function serverTabsFor(card) { return card && card.is_postgres === true ? POSTGRES_TABS : SERVER_TABS; }\n" +
      "export function isPostgresTarget(card) { return !!card && card.is_postgres === true; }\n" +
      "export function findServerTab(id, tabs) { const r = tabs || SERVER_TABS; return r.find((t) => t.id === id) || r[0]; }\n" +
      "export function tabNote() { return null; }\n"
  );

  const names = (n, prefix = "db") => Array.from({ length: n }, (_, i) => prefix + String(i).padStart(2, "0"));
  const six = ["A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>"];
  const long = (n) => Array.from({ length: n }, (_, i) => String(i).padStart(2, "0") + "中".repeat(60)); // 557 encoded bytes each

  const out = {};
  const scenarios = {
    /* A PostgreSQL card gets no control, and no filter is active. */
    async postgres() {
      card = { ...card, is_postgres: true, engine_description: "PostgreSQL" };
      storeChoice(7, ["A"]);
      const { page, builds } = await open();
      out.buttons = page.byClass("db-filter-button").length;
      out.slotChildren = page.byClass("db-filter-slot")[0].children.length;
      out.filters = builds().map((b) => b.filter);
    },
    /* No card (the fleet read failed): the button is disabled, and no filter is active. */
    async unavailable() {
      fleetOk = false;
      storeChoice(7, ["A"]);
      const { page, builds } = await open();
      const b = page.byClass("db-filter-button");
      out.labels = b.map((x) => x.textContent);
      out.disabled = b.map((x) => x.getAttribute("disabled") !== null);
      out.filters = builds().map((x) => x.filter);
    },
    /* Apply with every offered name checked stores exactly that set, never "All"; Apply redraws once; the label reads
       "Databases: 2" until the inventory loads, then "Databases: 2 of 37"; "All databases" and an empty Apply clear. */
    async applyExact() {
      inventory = names(37);
      storeChoice(7, ["db01", "db02"]);
      const { page, builds } = await open();
      const ctl = () => ({ button: page.byClass("db-filter-button")[0], popover: page.byClass("db-filter-popover")[0] });
      out.labelBeforeLoad = ctl().button.textContent;
      out.filterAtFirstBuild = builds()[0].filter;
      out.popoverHiddenAtStart = ctl().popover.hidden;
      out.inventoryFetchedBeforeOpen = urls.filter((u) => u.startsWith("/api/server-databases")).length;
      ctl().button.fire("click");
      await settle();
      out.inventoryUrl = urls.filter((u) => u.startsWith("/api/server-databases"))[0];
      out.popoverOpen = !ctl().popover.hidden;
      out.labelAfterLoad = ctl().button.textContent;
      out.selectAllButtons = ctl().popover.all("button").filter((b) => b.textContent === "Select All").length;
      out.checkedAtOpen = checked(ctl().popover).length;
      for (const box of boxes(ctl().popover)) if (!box.checked) { box.checked = true; box.fire("change"); }
      out.checkedAfterAll = checked(ctl().popover).length;
      const before = builds().length;
      press(ctl().popover, "Apply");
      await settle();
      out.stored = stored()["7"];
      out.storedCount = (stored()["7"] || []).length;
      out.redraws = builds().length - before;
      out.filterAfterApply = builds().pop().filter;
      out.labelAfterApply = ctl().button.textContent;
      out.popoverClosed = ctl().popover.hidden;
      // "All databases" removes the entry and redraws once.
      ctl().button.fire("click"); await settle();
      const b2 = builds().length;
      press(ctl().popover, "All databases"); await settle();
      out.afterAll = { stored: stored()["7"] ?? null, redraws: builds().length - b2, filter: builds().pop().filter, label: ctl().button.textContent };
      // An Apply with nothing checked clears too.
      storeChoice(7, ["db03"]);
      ctl().button.fire("click"); await settle();
      for (const box of boxes(ctl().popover)) if (box.checked) { box.checked = false; box.fire("change"); }
      press(ctl().popover, "Apply"); await settle();
      out.afterEmpty = { stored: stored()["7"] ?? null, label: ctl().button.textContent };
    },
    /* A stored name the inventory no longer offers stays checked and listed, and survives an Apply. */
    async storedMissing() {
      inventory = ["A", "B"];
      storeChoice(7, ["Gone", "A"]);
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      out.listed = boxes(popover).map((b) => b.attrs["aria-label"]);
      out.checked = checked(popover).map((b) => b.attrs["aria-label"]);
      press(popover, "Apply"); await settle();
      out.stored = stored()["7"];
    },
    /* #5245: a database whose name is only U+0085 (whitespace to the service, so the service would drop it) can be checked, and
       Apply refuses it with the sentence about the name, not the one about size; nothing is stored. A name that is only U+FEFF
       is a real name to the service, so it is kept. */
    async blankName() {
      inventory = ["Real", "\u0085", "\ufeff"];
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      const box = (n) => boxes(popover).find((b) => b.attrs["aria-label"] === n);
      box("\u0085").checked = true; box("\u0085").fire("change");
      press(popover, "Apply"); await settle();
      out.message = popover.byClass("db-filter-message")[0].textContent;
      out.stored = stored()["7"] || null;
      box("\u0085").checked = false; box("\u0085").fire("change");
      box("\ufeff").checked = true; box("\ufeff").fire("change");
      press(popover, "Apply"); await settle();
      out.storedBom = (stored()["7"] || []).map((n) => n.length + ":" + n.charCodeAt(0).toString(16));
    },
    /* The 51st check is refused with a sentence; 50 are kept. */
    async limit51() {
      inventory = names(60);
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      for (const box of boxes(popover).slice(0, 50)) { box.checked = true; box.fire("change"); }
      out.checkedBefore = checked(popover).length;
      const fifty1 = boxes(popover)[50];
      fifty1.checked = true; fifty1.fire("change"); // a hand-fired change past the cap
      out.fiftyFirstChecked = fifty1.checked;
      out.fiftyFirstDisabled = fifty1.disabled;
      out.checkedAfter = checked(popover).length;
      out.hint = popover.byClass("mp-hint")[0].textContent;
      press(popover, "Apply"); await settle();
      out.storedCount = (stored()["7"] || []).length;
    },
    /* A check past 4,096 encoded bytes is refused: 50 long non-ASCII names, each 557 encoded bytes, hold 7. */
    async byteBudget() {
      inventory = long(50);
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      for (const box of boxes(popover)) { box.checked = true; box.fire("change"); }
      out.checked = checked(popover).length;
      out.hint = popover.byClass("mp-hint")[0].textContent;
      out.disabledLeft = boxes(popover).filter((b) => !b.checked && b.disabled).length;
      out.uncheckedLeft = boxes(popover).filter((b) => !b.checked).length;
      press(popover, "Apply"); await settle();
      out.storedCount = (stored()["7"] || []).length;
      out.bytes = (stored()["7"] || []).reduce((s, n) => s + "&database_name=".length + encodeURIComponent(n).length, 0);
    },
    /* The six awkward names come out as six single names, and appear as text, never as markup. */
    async awkward() {
      inventory = six;
      const { page, builds } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      out.listed = popover.byClass("mp-item").map((l) => l.children[1].textContent);
      out.markupNodes = popover.every((n) => n.tag === "img").length;
      for (const box of boxes(popover)) { box.checked = true; box.fire("change"); }
      press(popover, "Apply"); await settle();
      out.stored = stored()["7"];
      out.filter = builds().pop().filter;
      out.label = page.byClass("db-filter-button")[0].textContent;
      // Reopening lists them again as text.
      page.byClass("db-filter-button")[0].fire("click"); await settle();
      out.relisted = popover.byClass("mp-item").map((l) => l.children[1].textContent);
      out.relistedChecked = checked(popover).length;
      out.markupNodesAfter = page.every((n) => n.tag === "img").length;
    },
    /* #5245: the route cut the list at its cap (truncated: true). The popover says so and that the search box narrows the
       names listed, and the label reads "of 40+" because 40 is the cap, not the server's count. A list that is not cut says
       nothing and its label keeps the plain count. */
    async cutList() {
      inventory = names(40);
      inventoryCut = true;
      storeChoice(7, ["db01"]);
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      out.note = popover.byClass("db-filter-cut").map((n) => n.textContent);
      out.offered = boxes(popover).length;
      out.label = page.byClass("db-filter-button")[0].textContent;
    },
    async uncutList() {
      inventory = names(40);
      storeChoice(7, ["db01"]);
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      out.note = popover.byClass("db-filter-cut").map((n) => n.textContent);
      out.label = page.byClass("db-filter-button")[0].textContent;
    },
    /* #5314: a cut list's search box asks the route for the typed text, after a pause, and shows the newest answer. A database past
       the cap (named "zz-past-the-cap", not among the 40 loaded) is found; the cut note then says to type a name. A burst of
       keystrokes sends ONE ask, for the last text. Clearing the box shows the first page again. */
    async cutSearch() {
      inventory = names(40);
      inventoryCut = true;
      searchHandler = (q) => ({ names: q.startsWith("zz") ? ["zz-past-the-cap"] : [], cut: false });
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      out.noteBefore = popover.byClass("db-filter-cut").map((n) => n.textContent);
      out.searchUrlsBefore = searchUrls().length;
      type(popover, "z"); type(popover, "zz"); type(popover, "zz-past");
      out.searchUrlsDuringPause = searchUrls().length;
      await sleep(450);
      out.searchUrls = searchUrls();
      out.listedAfter = boxes(popover).map((b) => b.attrs["aria-label"]);
      out.noteAfter = popover.byClass("db-filter-cut").map((n) => n.textContent);
      out.label = page.byClass("db-filter-button")[0].textContent;
      // The found name can be checked and applied, though it was never in the first page.
      boxes(popover)[0].checked = true; boxes(popover)[0].fire("change");
      type(popover, "");
      await sleep(100);
      out.listedCleared = boxes(popover).length;
      out.searchUrlsAfterClear = searchUrls().length;
      out.checkedAfterClear = checked(popover).map((b) => b.attrs["aria-label"]);
      press(popover, "Apply"); await settle();
      out.stored = stored()["7"];
    },
    /* Only the newest answer shows: the answer for the first text is slow and lands after the second text's answer. */
    async cutSearchNewestWins() {
      inventory = names(40);
      inventoryCut = true;
      searchHandler = (q) => (q === "a1" ? { names: ["a1-old-answer"], cut: false, delay: 400 } : { names: ["a2-new-answer"], cut: false });
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      type(popover, "a1");
      await sleep(300); // the ask for "a1" is in flight (250 ms pause), its answer is 400 ms away
      type(popover, "a2");
      await sleep(350); // the ask for "a2" has been answered
      out.listedMid = boxes(popover).map((b) => b.attrs["aria-label"]);
      await sleep(400); // the slow "a1" answer has landed by now
      out.listedEnd = boxes(popover).map((b) => b.attrs["aria-label"]);
      out.searchUrls = searchUrls();
    },
    /* #5314: the route matched the text itself (PostgreSQL ILIKE), so its answer is listed as sent. "İstanbul" is the real case:
       JS lowercases it to "i" + a combining dot, so a second local filter on "ist" would hide a name the route returned. While a newer
       keystroke's answer is still in flight, the last answer is narrowed locally as before, and the newest answer replaces it. */
    async cutSearchServerFold() {
      inventory = names(40);
      inventoryCut = true;
      searchHandler = (q) => (q === "ist" ? { names: ["İstanbul", "istanbul2"], cut: false }
        : q === "ista" ? { names: ["İstanbul", "istanbul2"], cut: false, delay: 300 } : { names: [], cut: false });
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      type(popover, "ist");
      await sleep(450);
      out.listedAnswered = boxes(popover).map((b) => b.attrs["aria-label"]);
      type(popover, "ista");
      await sleep(300); // the ask for "ista" is in flight (250 ms pause): the last answer is narrowed here
      out.listedInFlight = boxes(popover).map((b) => b.attrs["aria-label"]);
      await sleep(400);
      out.listedNewest = boxes(popover).map((b) => b.attrs["aria-label"]);
    },
    /* A search that is itself cut says so, and says to type more of the name. */
    async cutSearchCutAndFails() {
      inventory = names(40);
      inventoryCut = true;
      searchHandler = (q) => (q === "m" ? { names: names(40, "m"), cut: true } : { names: [], cut: false });
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      type(popover, "m");
      await sleep(450);
      out.noteCut = popover.byClass("db-filter-cut").map((n) => n.textContent);
      out.offered = boxes(popover).length;
    },
    /* A list that was NOT cut already holds every name: typing filters those locally and asks the route for nothing. */
    async uncutSearch() {
      inventory = names(40);
      searchHandler = () => ({ names: ["should-not-be-asked"], cut: false });
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      type(popover, "db1");
      await sleep(450);
      out.searchUrls = searchUrls();
      out.listed = boxes(popover).map((b) => b.attrs["aria-label"]);
      out.note = popover.byClass("db-filter-cut").map((n) => n.textContent);
    },
    /* The inventory read failed: the sentence shows and the stored names stay listed, so the choice can still be undone. */
    async loadFails() {
      inventoryStatus = 500;
      storeChoice(7, ["Kept"]);
      const { page } = await open();
      const popover = page.byClass("db-filter-popover")[0];
      page.byClass("db-filter-button")[0].fire("click");
      await settle();
      out.message = popover.byClass("db-filter-message")[0].textContent;
      out.listed = boxes(popover).map((b) => b.attrs["aria-label"]);
      out.label = page.byClass("db-filter-button")[0].textContent;
    },
  };

  const boxes = (popover) => popover.all("input").filter((i) => i.attrs.type === "checkbox");
  const checked = (popover) => boxes(popover).filter((b) => b.checked);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const searchUrls = () => urls.filter((u) => u.startsWith("/api/server-databases") && u.includes("search="));
  /* Types `text` into the picker's search box (the input the page holds now) the way a keystroke does. */
  const type = (popover, text) => {
    const box = popover.every((n) => n.tag === "input" && n.attrs.type === "search")[0];
    box.value = text;
    box.fire("input");
  };
  const press = (popover, text) => popover.all("button").find((b) => b.textContent === text).fire("click");
  const builds = () => globalThis.__builds || [];

  /* Renders the real server page for SRV1 and waits for its fleet read and first panel batch. */
  async function open() {
    const server = await import(pathToFileURL(path.join(scratch, "pages", "server.js")).href);
    const main = new FakeNode("main");
    main.isRoot = true;
    server.renderServer(main, "SRV1", "cpu", {});
    await settle();
    return { page: main, builds };
  }

  await scenarios[scenario]();
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
