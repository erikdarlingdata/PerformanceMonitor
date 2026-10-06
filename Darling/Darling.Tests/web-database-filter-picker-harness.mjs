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
globalThis.fetch = async (url) => {
  const u = String(url);
  urls.push(u);
  if (u.startsWith("/api/fleet")) {
    if (!fleetOk) return { status: 500, ok: false, text: async () => JSON.stringify({ error: "down" }) };
    return { status: 200, ok: true, text: async () => JSON.stringify({ cards: [card] }) };
  }
  if (u.startsWith("/api/server-databases")) {
    if (inventoryStatus !== 200) return { status: inventoryStatus, ok: false, text: async () => JSON.stringify({ error: "no list" }) };
    return { status: 200, ok: true, text: async () => JSON.stringify({ server: "SRV1", databases: inventory }) };
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
