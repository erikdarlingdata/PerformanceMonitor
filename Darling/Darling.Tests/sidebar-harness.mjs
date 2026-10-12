/* Runs the shipped sidebar server list (wwwroot/js/sidebar.js) and the grouping rules it shares with the Fleet page
   (wwwroot/js/fleet-groups.js) under Node against a fake DOM and a stubbed localStorage, and prints the result of a
   scenario as one line of JSON. SidebarSearchGroupTests starts it as
       node sidebar-harness.mjs <path to wwwroot/js> <scenario>
   The modules are copied unchanged into a scratch folder and imported as the browser would. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.checked = false;
    this.handlers = {};
    this.text = text == null ? null : String(text);
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  focus() { globalThis.document.activeElement = this; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
const data = {};
globalThis.localStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null),
  setItem: (k, v) => { data[k] = String(v); },
  removeItem: (k) => { delete data[k]; },
};

const all = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  for (const c of node.children) all(c, pred, out);
  return out;
};
const fire = (node, type, e = {}) => { for (const fn of node.handlers[type] || []) fn(e); };
const names = (root) => all(root, (n) => n.className === "name").map((n) => n.textContent);
const headers = (root) => all(root, (n) => n.className === "sidebar-group-header").map((n) => n.children.map((c) => c.textContent).join(" ").replace(/^[\u25B8\u25BE] /, ""));
const lines = (root) => root.children.map((n) => (n.className === "sidebar-group-header" ? "G:" : "S:") + (n.className === "sidebar-group-header" ? n.children[1].textContent : n.children[1].textContent));

const tag = (id, name, parent_id, sort_order) => ({ id, name, parent_id, sort_order, colour: null });
const forest = [tag(1, "East", null, 0), tag(2, "Prod", 1, 0), tag(3, "West", null, 1), tag(4, "Spare", null, 2)];
const card = (server_id, display_name, server_name, tags, band = "Healthy") => ({ server_id, display_name, server_name, tags, band });
const cards = [
  card(1, "Alpha", "alpha-host.example.test", [{ id: 2, name: "Prod" }]),
  card(2, "Bravo", "bravo-host.example.test", [{ id: 2, name: "Prod" }, { id: 3, name: "West" }]),
  card(3, "Charlie", "charlie-host.example.test", [{ id: 1, name: "East" }]),
  card(4, "Delta", "delta-host.example.test", []),
  card(5, "Echo", "echo-host.example.test", [{ id: 3, name: "West" }]),
];
const fleet = { cards, tags: forest };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "sidebar-"));
const out = {};
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const groups = await load("fleet-groups.js");
  const side = await load("sidebar.js");
  const local = await load("viewer-local.js");

  const byName = (a, b) => a.display_name.localeCompare(b.display_name);
  const rowsFor = (term, grouped, o = {}) => groups.sidebarRows(cards, forest, {
    term, grouped, sortFn: byName, isFavorite: o.isFavorite || (() => false), isCollapsed: o.isCollapsed || (() => false),
  });
  const shape = (m) => m.rows.map((r) => (r.kind === "group" ? "G:" + r.name + "(" + r.count + (r.collapsed ? ",c" : "") + ")@" + r.depth : "S:" + r.card.display_name + "@" + r.depth));

  if (scenario === "filter") {
    const match = (term) => names2(rowsFor(term, false));
    const names2 = (m) => m.rows.map((r) => r.card.display_name);
    out.byDisplay = match("ALPH");
    out.byServerName = match("charlie-host");
    out.byTag = match("west");
    out.byTagCase = match("PROD");
    out.padded = match("  delta ");
    out.empty = match("");
    out.none = rowsFor("zzz", false);
  } else if (scenario === "groupedOrder") {
    out.plain = shape(rowsFor("", true));
    out.withFavourites = shape(rowsFor("", true, { isFavorite: (c) => c.server_id === 5 || c.server_id === 4 }));
    out.collapsedEast = shape(rowsFor("", true, { isCollapsed: (id) => id === 1 }));
    out.collapsedUntagged = shape(rowsFor("", true, { isCollapsed: (id) => id === "untagged" }));
    out.filtered = shape(rowsFor("west", true));
    out.noMatch = rowsFor("zzz", true);
  } else if (scenario === "paint") {
    const host = new FakeNode("div");
    const list = new FakeNode("div");
    // app.js's paintSidebar: the last fleet and the CURRENT route, read fresh; it is also what typing calls
    const paintSidebar = () => side.paintServerList(list, fleet, null);
    side.initSidebarSearch(host, paintSidebar);
    // app.js repaints the sidebar on every viewer-local change (onLocalChange(paintSidebar)); this mirrors it
    local.onChange(paintSidebar);
    const input = all(host, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    const toggle = all(host, (n) => n.tag === "input" && n.attrs.type === "checkbox")[0];
    side.paintServerList(list, fleet, null);
    out.flat = names(list);
    out.inputFirst = host.children[0] === input;
    input.value = "Prod";
    fire(input, "input");
    out.typed = names(list);
    // the 60 s poll: a fresh payload painted into the same list, then a route change repaint with an active server
    side.paintServerList(list, { cards: cards.slice().reverse(), tags: forest }, "bravo-host.example.test");
    out.afterPoll = names(list);
    out.termAfterPoll = side.sidebarSearchTerm();
    out.activeAfterPoll = all(list, (n) => /\bactive\b/.test(n.className)).map((n) => n.dataset.display);
    input.value = "zzz";
    fire(input, "input");
    out.noMatchText = list.textContent;
    out.noMatchRows = list.children.length;
    input.value = "";
    fire(input, "input");
    out.cleared = names(list);
    out.inputFirstAfterRepaints = host.children[0] === input && all(host, (n) => n.attrs.type === "search")[0] === input;
    // grouped view through the toggle
    toggle.checked = true;
    fire(toggle, "change");
    out.groupedLines = lines(list);
    out.stored = JSON.parse(data["darling.local.sidebar.v1"]);
    // collapse the East group (it holds Prod): its header toggles and the choice is stored by id
    const east = all(list, (n) => n.className === "sidebar-group-header" && n.dataset.group === "1")[0];
    fire(east, "click");
    out.afterCollapse = lines(list);
    out.storedAfterCollapse = JSON.parse(data["darling.local.sidebar.v1"]);
    out.storedText = data["darling.local.sidebar.v1"];
  } else if (scenario === "activeRoute") {
    // M1: a route change only toggles classes on the existing rows (app.js updateServerActive); typing afterwards
    // must still paint the CURRENT route's server as active, not the one of the last full paint.
    const host = new FakeNode("div");
    const list = new FakeNode("div");
    let route = "alpha-host.example.test";
    const paintSidebar = () => side.paintServerList(list, fleet, route);
    side.initSidebarSearch(host, paintSidebar);
    const input = all(host, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    paintSidebar();
    out.before = all(list, (n) => /\bactive\b/.test(n.className)).map((n) => n.dataset.display);
    route = "echo-host.example.test"; // the route changed; no repaint
    input.value = "o";
    fire(input, "input");
    out.afterTyping = all(list, (n) => /\bactive\b/.test(n.className)).map((n) => n.dataset.display);
    out.rows = names(list);
  } else if (scenario === "searchOpensCollapsed") {
    // M2: a collapsed group never hides a match while a term is typed; the stored choice is untouched
    const host = new FakeNode("div");
    const list = new FakeNode("div");
    const paintSidebar = () => side.paintServerList(list, fleet, null);
    side.initSidebarSearch(host, paintSidebar);
    local.onChange(paintSidebar);
    const input = all(host, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    const toggle = all(host, (n) => n.tag === "input" && n.attrs.type === "checkbox")[0];
    toggle.checked = true;
    fire(toggle, "change");
    local.toggleSidebarGroup(1); // East collapsed (Prod is under it, Alpha is in Prod)
    out.collapsed = lines(list);
    out.collapsedHeaders = headers(list);
    input.value = "alpha";
    fire(input, "input");
    out.searching = lines(list);
    out.searchingHeaders = headers(list);
    out.storedWhileSearching = JSON.parse(data["darling.local.sidebar.v1"]).collapsedGroups;
    input.value = "  ";
    fire(input, "input");
    out.blankTerm = lines(list); // whitespace only is not a search: East is collapsed again
    input.value = "";
    fire(input, "input");
    out.cleared = lines(list);
    out.clearedHeaders = headers(list);
    // the pure helper: the default (no term) keeps the collapse, a term opens it
    out.helperNoTerm = shape(rowsFor("", true, { isCollapsed: (id) => id === 1 }));
    out.helperTerm = shape(rowsFor("alpha", true, { isCollapsed: (id) => id === 1 }));
  } else if (scenario === "headerFocus") {
    // L1: Enter on a group header toggles it and the repainted header of the same group keeps keyboard focus
    const host = new FakeNode("div");
    const list = new FakeNode("div");
    const paintSidebar = () => side.paintServerList(list, fleet, null);
    side.initSidebarSearch(host, paintSidebar);
    local.onChange(paintSidebar);
    local.setSidebarGrouped(true);
    const headerFor = (id) => all(list, (n) => n.className === "sidebar-group-header" && n.dataset.group === id)[0];
    const west = headerFor("3");
    west.focus();
    fire(west, "keydown", { key: "Enter", preventDefault() {} });
    const after = headerFor("3");
    out.replaced = after !== west;
    out.focusIsSameGroup = document.activeElement === after && after.dataset.group === "3";
    out.collapsed = after.attrs["aria-expanded"];
    fire(after, "keydown", { key: " ", preventDefault() {} });
    out.secondToggle = document.activeElement === headerFor("3") && headerFor("3").attrs["aria-expanded"] === "true";
    // focus elsewhere (the search box) is not stolen by a repaint
    const input = all(host, (n) => n.tag === "input" && n.attrs.type === "search")[0];
    input.focus();
    fire(input, "input");
    out.searchKeepsFocus = document.activeElement === input;
  } else if (scenario === "collapsedCount") {
    // N1: a collapsed header shows its SUBTREE count (distinct servers); an expanded one keeps the direct count
    const plain = rowsFor("", true);
    const collapsed = rowsFor("", true, { isCollapsed: (id) => id === 1 });
    const pick = (m, name) => m.rows.find((r) => r.kind === "group" && r.name === name);
    out.eastExpanded = [pick(plain, "East").count, pick(plain, "East").subtreeCount];
    out.eastCollapsed = [pick(collapsed, "East").count, pick(collapsed, "East").subtreeCount];
    // a multi-tag server under two children of one parent counts once in the parent
    const f2 = [tag(1, "P", null, 0), tag(2, "A", 1, 0), tag(3, "B", 1, 1)];
    const c2 = [card(1, "One", "one", [{ id: 2, name: "A" }, { id: 3, name: "B" }]), card(2, "Two", "two", [{ id: 3, name: "B" }])];
    const m2 = groups.sidebarRows(c2, f2, { term: "", grouped: true, sortFn: byName, isFavorite: () => false, isCollapsed: (id) => id === 1 });
    out.parentOnly = [m2.rows[0].count, m2.rows[0].subtreeCount];
    // painted text
    const list = new FakeNode("div");
    side.paintServerList(list, { cards, tags: forest }, null); // flat: no headers
    local.setSidebarGrouped(true);
    local.toggleSidebarGroup(1);
    side.paintServerList(list, { cards, tags: forest }, null);
    out.collapsedHeader = headers(list)[0];
    local.toggleSidebarGroup(1);
    side.paintServerList(list, { cards, tags: forest }, null);
    out.expandedHeader = headers(list)[0];
  } else if (scenario === "roundTrip") {
    local.setSidebarGrouped(true);
    local.toggleSidebarGroup(2);
    local.toggleSidebarGroup("untagged");
    local.toggleSidebarGroup("favourites");
    local.toggleSidebarGroup("bogus");
    local.toggleSidebarGroup(-3);
    local.setSidebarCollapsed(true); // a second field of the same key must not drop the first
    local.reload();
    out.grouped = local.isSidebarGrouped();
    out.collapsed = [1, 2, "untagged", "favourites", "bogus"].filter(local.isSidebarGroupCollapsed);
    out.sidebarCollapsed = local.isSidebarCollapsed();
    out.stored = JSON.parse(data["darling.local.sidebar.v1"]);
    local.toggleSidebarGroup(2);
    local.reload();
    out.afterExpand = [2, "untagged"].filter(local.isSidebarGroupCollapsed);
    out.fleetKeys = Object.keys(data).filter((k) => k.startsWith("darling.fleet."));
    out.storesNoNames = !/Prod|West|East|Alpha/.test(data["darling.local.sidebar.v1"]);
  } else if (scenario === "corruptStorage") {
    const cases = {
      garbage: "{not json",
      arrayRoot: "[1,2]",
      foreignVersion: JSON.stringify({ v: 9, grouped: true, collapsedGroups: [1] }),
      wrongTypes: JSON.stringify({ v: 1, grouped: "yes", collapsedGroups: "1" }),
      badEntries: JSON.stringify({ v: 1, grouped: 1, collapsedGroups: [1.5, -2, "Prod", null, {}, "untagged"] }),
    };
    out.results = {};
    for (const [name, raw] of Object.entries(cases)) {
      data["darling.local.sidebar.v1"] = raw;
      local.reload();
      const list = new FakeNode("div");
      side.paintServerList(list, fleet, null);
      out.results[name] = {
        grouped: local.isSidebarGrouped(),
        collapsed: [1, 2, -2, "Prod", "untagged", "favourites"].filter(local.isSidebarGroupCollapsed),
        painted: names(list),
      };
    }
  } else {
    throw new Error("unknown scenario " + scenario);
  }
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
