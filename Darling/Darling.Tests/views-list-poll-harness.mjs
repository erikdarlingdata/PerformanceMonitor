/* Runs the shipped Custom Views list (wwwroot/js/pages/views.js) under Node against a recording fetch and a fake DOM, and prints as
   one line of JSON what the 60 s poll does to a list the reader is working on (#7925): the import JSON pasted into the import box, the
   open Import and template menus and the server picked for a template, the per-view definition reads the poll costs, a poll whose
   list read fails, a fresh navigation to the list, a poll whose fleet read has a server added since the first paint, a poll after a
   failed first paint, and a poll after the session probe recovers. ViewsListPollBehaviourTests starts it as
       node views-list-poll-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.open = false;
    this.hidden = false;
    this.disabled = false;
    this.handlers = {};
    this.text = text == null ? null : String(text);
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) {
    if (child.parent) child.parent.removeChild(child);
    child.parent = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    child.parent = null;
    return child;
  }
  insertBefore(next, ref) {
    if (next.parent) next.parent.removeChild(next);
    next.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i >= 0) this.children.splice(i, 0, next);
    else this.children.push(next);
    return next;
  }
  replaceChild(next, old) {
    const i = this.children.indexOf(old);
    if (i >= 0) {
      if (next.parent) next.parent.removeChild(next);
      next.parent = this;
      this.children[i] = next;
      old.parent = null;
    }
    return old;
  }
  get isConnected() {
    let n = this;
    while (n.parent) n = n.parent;
    return n.isRoot === true;
  }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  removeEventListener(type, fn) { this.handlers[type] = (this.handlers[type] || []).filter((f) => f !== fn); }
  closest() { return null; }
  focus() { globalThis.document.activeElement = this; }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  documentElement: { offsetHeight: 0 },
  createElement: (tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  querySelectorAll: () => [],
};
globalThis.window = globalThis;
globalThis.scrollY = 0;
globalThis.scrollTo = () => {};
globalThis.location = { hash: "#/views", pathname: "/", search: "", href: "http://localhost/#/views", origin: "http://localhost" };
const stored = {};
const storage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(stored, k) ? stored[k] : null),
  setItem: (k, v) => { stored[k] = String(v); },
  removeItem: (k) => { delete stored[k]; },
};
globalThis.localStorage = storage;
globalThis.sessionStorage = storage;

const all = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  for (const c of node.children) all(c, pred, out);
  return out;
};
const withClass = (node, cls) => all(node, (n) => (" " + n.className + " ").includes(" " + cls + " "));
const fire = (node, type, e = {}) => { for (const fn of node.handlers[type] || []) fn(e); };
const settle = () => new Promise((r) => setTimeout(r, 20));

/* The list payload carries kind and version but not the definition; the per-view read carries the definition the chips are counted from. */
const view = (id, name, version, o = {}) => ({ id, name, kind: "dashboard", version, folder: null, personal: false, created_by: "alice", updated_by: "alice", updated_at: "2026-11-06T10:00:00Z", mine: true, ...o });
let list = [view(1, "Waits by server", 1), view(2, "CPU trend", 1), view(3, "Blocking", 1)];
const definitions = {
  1: { panels: [{ viz: "line" }, { viz: "bar" }] },
  2: { panels: [{ viz: "line" }] },
  3: { panels: [{ viz: "pie" }, { viz: "pie" }, { viz: "stat" }] },
};
let listFails = false;
let sessionFails = false;
const fleet = [{ server_id: 1, server_name: "SQL01", display_name: "SQL01" }, { server_id: 2, server_name: "SQL02", display_name: "SQL02" }];
const fetches = [];
let onListRead = null;
globalThis.fetch = async (url) => {
  const u = String(url).split("?")[0];
  fetches.push(u);
  const reply = (body, status = 200) => ({ ok: status < 400, status, text: async () => JSON.stringify(body) });
  if (u === "/api/session") return sessionFails ? reply({ error: "no session" }, 500) : reply({ can_edit: true, can_own: true });
  if (u === "/api/fleet") return reply({ cards: fleet.slice() });
  if (u === "/api/views") {
    if (onListRead) onListRead();
    return listFails ? reply({ error: "the store is down" }, 500) : reply(list);
  }
  const m = /^\/api\/views\/(\d+)$/.exec(u);
  if (m) {
    const v = list.find((x) => String(x.id) === m[1]);
    return reply({ ...v, definition: definitions[m[1]] });
  }
  return reply([]);
};
const viewReadsSince = (from) => fetches.slice(from).filter((f) => /^\/api\/views\/\d+$/.test(f)).map((f) => f.split("/").pop());
const chipsOf = (main) => withClass(main, "view-card").map((c) => withClass(c, "vc-chip").map((n) => n.textContent).join(","));
const namesOf = (main) => withClass(main, "vc-name-text").map((n) => n.textContent);
const importBox = (main) => withClass(main, "import-box")[0] || null;
const importPanel = (main) => withClass(main, "import-panel")[0] || null;
const templateMenu = (main) => withClass(main, "template-menu")[0] || null;
const templateServer = (main) => { const menu = templateMenu(main); return menu ? all(menu, (n) => n.tag === "select")[0] || null : null; };
/* The option values of the first server picker under `node`, or none when `node` has no picker. */
const pickerServers = (node) => { const sel = node ? all(node, (n) => n.tag === "select")[0] : null; return sel ? sel.children.map((o) => o.getAttribute("value")) : []; };
const hero = (main) => withClass(main, "views-hero")[0] || null;
const heroTitle = (main) => { const h = hero(main); return h ? withClass(h, "hero-title").map((n) => n.textContent)[0] || null : null; };
const starterRow = (main) => withClass(main, "hero-suggest")[0] || null;
const errorStrips = (main) => all(main, (n) => (" " + n.className + " ").includes(" error ") && !(" " + n.className + " ").includes(" refresh-error ")).map((n) => n.textContent);
const newViewLinks = (main) => all(main, (n) => n.tag === "a" && n.textContent === "New view").length;

/* A second copy of the page script, with its own session cache: the first good session answer is kept for the life of the page. */
const loadPage = (dir) => {
  fs.cpSync(jsDir, dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "package.json"), '{ "type": "module" }');
  return import(pathToFileURL(path.join(dir, "pages", "views.js")).href);
};

const PASTED = '{"definition": {"panels": [{"viz": "line"}]}}';
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "views-list-poll-"));
const scratch2 = fs.mkdtempSync(path.join(os.tmpdir(), "views-list-poll-"));
const out = {};
try {
  const page = await loadPage(scratch);
  const main = new FakeNode("main");
  main.isRoot = true;

  /* The first paint: the list, then the per-view reads that fill the chips. */
  await page.renderViewList(main);
  await settle();
  out.first = { names: namesOf(main), chips: chipsOf(main), viewReads: viewReadsSince(0).sort(), hasImportBox: importBox(main) !== null, hasPicker: templateServer(main) !== null };

  /* The reader pastes JSON that lacks a name, opens the import panel and the template menu, and picks the second server for a template. */
  const box = importBox(main);
  box.value = PASTED;
  fire(box, "input");
  importPanel(main).open = true;
  templateMenu(main).open = true;
  templateServer(main).value = "SQL02";
  const panelBefore = importPanel(main);
  const menuBefore = templateMenu(main);

  /* The 60 s poll, over a list where one view was renamed and one has a new version. */
  list = [view(1, "Waits by server", 1), view(2, "CPU trend, renamed", 1), view(3, "Blocking", 2)];
  definitions[3] = { panels: [{ viz: "pie" }, { viz: "stat" }] };
  let loadingDuringPoll = null;
  onListRead = () => { loadingDuringPoll = main.textContent.includes("Loading views"); };
  const beforePoll = fetches.length;
  await page.renderViewList(main, { poll: true });
  await settle();
  onListRead = null;
  const boxAfter = importBox(main);
  out.poll = {
    loadingDuringPoll,
    names: namesOf(main),
    chips: chipsOf(main),
    viewReads: viewReadsSince(beforePoll).sort(),
    boxKept: boxAfter === box,
    boxValue: boxAfter ? boxAfter.value : null,
    importOpen: importPanel(main) === panelBefore && importPanel(main).open === true,
    menuOpen: templateMenu(main) === menuBefore && templateMenu(main).open === true,
    serverPick: templateServer(main) ? templateServer(main).value : null,
    refreshError: withClass(main, "refresh-error").length,
  };

  /* A poll whose list read fails: the list on screen stays, with the failure noted under the heading. */
  listFails = true;
  const beforeFailed = fetches.length;
  await page.renderViewList(main, { poll: true });
  await settle();
  listFails = false;
  out.failedPoll = {
    names: namesOf(main),
    boxValue: importBox(main) ? importBox(main).value : null,
    refreshError: withClass(main, "refresh-error").map((n) => n.textContent),
    viewReads: viewReadsSince(beforeFailed).length,
  };

  /* The next poll that succeeds takes the failure notice down again. */
  await page.renderViewList(main, { poll: true });
  await settle();
  out.recoveredPoll = { refreshError: withClass(main, "refresh-error").length, boxValue: importBox(main) ? importBox(main).value : null };

  /* A fresh navigation to the list (not the poll) draws it anew: an empty import box. */
  await page.renderViewList(main);
  await settle();
  out.navigated = { names: namesOf(main), boxValue: importBox(main) ? importBox(main).value : null };

  /* The reader opens the template menu, picks the second server and pastes JSON again. The next poll's fleet read answers with a
     server added since the first paint: the picker lists it, the menu is still the same open element, and the pick and the text stay. */
  const menuAgain = templateMenu(main);
  menuAgain.open = true;
  templateServer(main).value = "SQL02";
  importBox(main).value = PASTED;
  fleet.push({ server_id: 3, server_name: "SQL03", display_name: "SQL03" });
  const beforeFleetPoll = fetches.length;
  await page.renderViewList(main, { poll: true });
  await settle();
  out.fleetGrows = {
    fleetReads: fetches.slice(beforeFleetPoll).filter((f) => f === "/api/fleet").length,
    servers: pickerServers(templateMenu(main)),
    serverPick: templateServer(main) ? templateServer(main).value : null,
    menuOpen: templateMenu(main) === menuAgain && templateMenu(main).open === true,
    boxValue: importBox(main) ? importBox(main).value : null,
    names: namesOf(main),
  };

  /* A list whose first paint fails, over a store with no views and a fleet with no server yet. */
  const main2 = new FakeNode("main");
  main2.isRoot = true;
  list = [];
  fleet.length = 0;
  listFails = true;
  await page.renderViewList(main2);
  await settle();
  out.failedFirstPaint = { errors: errorStrips(main2), hero: hero(main2) !== null };

  /* A poll that fails again draws the error page again: there is no earlier list to keep under a refresh notice. */
  await page.renderViewList(main2, { poll: true });
  await settle();
  out.failedAgainPoll = { errors: errorStrips(main2), refreshError: withClass(main2, "refresh-error").length, hero: hero(main2) !== null };

  /* The poll after that answers an empty list: the page shows the empty-store hero, drawn for a fleet with no server. */
  listFails = false;
  await page.renderViewList(main2, { poll: true });
  await settle();
  out.emptyAfterFailure = {
    hero: hero(main2) !== null,
    heroTitle: heroTitle(main2),
    errors: errorStrips(main2),
    refreshError: withClass(main2, "refresh-error").length,
    heroServers: pickerServers(starterRow(main2)),
  };

  /* The reader opens the hero's template menu. The next poll's fleet read has a server: the hero's starter row and its menu gain the
     picker, the hero is the same element, and the menu stays open. */
  const heroBefore = hero(main2);
  const heroMenu = heroBefore ? templateMenu(heroBefore) : null;
  if (heroMenu) heroMenu.open = true;
  fleet.push({ server_id: 1, server_name: "SQL01", display_name: "SQL01" });
  await page.renderViewList(main2, { poll: true });
  await settle();
  out.heroFleetGrows = {
    heroKept: hero(main2) !== null && hero(main2) === heroBefore,
    heroMenuOpen: heroMenu !== null && hero(main2) !== null && templateMenu(hero(main2)) === heroMenu && heroMenu.open === true,
    heroMenuServers: pickerServers(heroMenu),
    heroServers: pickerServers(starterRow(main2)),
    starterChips: withClass(main2, "hero-chip").length,
    headServers: pickerServers(withClass(main2, "page-head")[0] || null),
  };

  /* A seat whose first session probe failed is drawn read-only. A failed probe is kept for a few seconds and then asked again
     (#7928): the poll after the probe recovers draws the editing affordances. The second copy of the page script starts with an
     empty session cache, and the clock is moved past the retry wait for that poll. */
  const page2 = await loadPage(scratch2);
  const main3 = new FakeNode("main");
  main3.isRoot = true;
  list = [view(1, "Waits by server", 1), view(2, "CPU trend", 1), view(3, "Blocking", 1)];
  fleet.push({ server_id: 2, server_name: "SQL02", display_name: "SQL02" });
  sessionFails = true;
  await page2.renderViewList(main3);
  await settle();
  out.readOnlyFirstPaint = { names: namesOf(main3), templateMenus: withClass(main3, "template-menu").length, newView: newViewLinks(main3), hasImportBox: importBox(main3) !== null };
  sessionFails = false;
  const realNow = Date.now;
  Date.now = () => realNow() + 10 * 60 * 1000;
  try {
    await page2.renderViewList(main3, { poll: true });
    await settle();
  } finally {
    Date.now = realNow;
  }
  out.canEditPoll = { names: namesOf(main3), templateMenus: withClass(main3, "template-menu").length, newView: newViewLinks(main3), hasImportBox: importBox(main3) !== null, servers: pickerServers(templateMenu(main3)) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
  fs.rmSync(scratch2, { recursive: true, force: true });
}
process.stdout.write(JSON.stringify(out));
