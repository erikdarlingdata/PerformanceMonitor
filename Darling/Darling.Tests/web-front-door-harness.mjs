/* Runs the shipped front-door pages (#6840: util.js, store-picker.js, sidebar.js, pages/fleet.js, pages/mute-rules.js and
   pages/known-patterns.js under wwwroot/js) under Node against a fake DOM and a recording fetch, and prints one scenario's
   result as one line of JSON. FrontDoorWebPagesTests starts it as
       node web-front-door-harness.mjs <path to wwwroot/js> <scenario>
   Each scenario is its own process because the front-door switch and the picked store are module state. */
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
    this.handlers = {};
    this.isConnected = true;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) { const i = this.children.indexOf(child); if (i >= 0) this.children.splice(i, 1); return child; }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], addEventListener() {},
};
globalThis.window = globalThis;
let reloads = 0;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost", reload: () => { reloads++; } };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const mem = new Map();
globalThis.localStorage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, String(v)), removeItem: (k) => mem.delete(k) };
globalThis.sessionStorage = globalThis.localStorage;
globalThis.addEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });

const all = (node, pred, out = []) => { if (pred(node)) out.push(node); for (const c of node.children) all(c, pred, out); return out; };
const settle = () => new Promise((r) => setTimeout(r, 25));
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });

/* Every request the pages send: url, method and the header names and values. */
const calls = [];
let handler = (url, init) => reply(200, {});
globalThis.fetch = async (url, init = {}) => {
  calls.push({ url, method: init.method || "GET", headers: { ...(init.headers || {}) }, initKeys: Object.keys(init).sort() });
  return handler(url, init);
};
const storeOf = (init) => (init.headers || {})["X-Darling-Store"];

const card = (id, name) => ({
  server_id: id, display_name: name, server_name: name, band: "Healthy", band_label: "Healthy", is_online: true,
  status: "ok", last_collection: null, tags: [], is_silenced: false, metric_count: 0, measured_metric_count: 0,
});
const fleetBody = (names) => ({
  total_servers: names.length, critical_count: 0, warning_count: 0, offline_count: 0, offline_servers: 0, tags: [],
  cards: names.map((n, i) => card(i + 1, n)), generated_at: "2026-10-28T12:00:00Z", worst_servers: [], additional_problem_count: 0,
});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "front-door-web-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const util = await load("util.js");
  const picker = await load("store-picker.js");

  const everyHelper = async () => {
    handler = () => reply(200, { ok: true });
    await util.apiGet("/api/a");
    await util.apiGetJoined("/api/read/b");
    await util.apiGetFleet();
    await util.apiSend("POST", "/api/c", { x: 1 });
    await util.apiWrite("PUT", "/api/d", { x: 1 });
    await util.apiSendRead("POST", "/api/e", { x: 1 });
  };

  const scenarios = {
    plain: async () => {
      await everyHelper();
      const mute = await load("pages/mute-rules.js");
      const known = await load("pages/known-patterns.js");
      await mute.createRule({ a: 1 });
      await known.createEntry({ a: 1 });
      return {
        frontDoor: util.frontDoor(), store: util.currentStore(), link: util.storeHash("#/server/x"),
        calls: calls.map((c) => ({ url: c.url, headers: c.headers, initKeys: c.initKeys })),
      };
    },
    headers: async () => {
      util.setFrontDoor(true);
      util.setStore("use1");
      await everyHelper();
      const mute = await load("pages/mute-rules.js");
      const known = await load("pages/known-patterns.js");
      await mute.createRule({ a: 1 });
      await known.createEntry({ a: 1 });
      await util.apiGetFleet("use2");
      return { calls: calls.map((c) => ({ url: c.url, store: c.headers["X-Darling-Store"] ?? null })) };
    },
    noStore: async () => {
      util.setFrontDoor(true);
      await everyHelper();
      return { calls: calls.map((c) => ({ url: c.url, store: c.headers["X-Darling-Store"] ?? null })) };
    },
    joined: async () => {
      util.setFrontDoor(true);
      handler = async () => { await settle(); return reply(200, { v: 1 }); };
      util.setStore("use1");
      const a = util.apiGetJoined("/api/read/same");
      const b = util.apiGetJoined("/api/read/same");
      util.setStore("use2");
      const c = util.apiGetJoined("/api/read/same");
      await Promise.all([a, b, c]);
      return { calls: calls.map((x) => ({ url: x.url, store: x.headers["X-Darling-Store"] ?? null })) };
    },
    address: async () => {
      util.setFrontDoor(true);
      util.setStore("use1");
      location.hash = "#/s/use2/server/web%2F01/queries";
      return {
        split: [
          util.splitStoreHash("#/s/use1/server/x/queries"),
          util.splitStoreHash("#/s/use1"),
          util.splitStoreHash("#/s/use1/"),
          util.splitStoreHash("#/s/a%20b%2Fc/alerts"),
          util.splitStoreHash("#/server/x"),
          util.splitStoreHash(""),
          util.splitStoreHash("#/s//fleet"),
          util.splitStoreHash("#/s/%E0%A4%A/fleet"),
        ],
        link: util.storeHash("#/server/x"),
        linkOther: util.storeHash("#/server/x", "use9"),
        linkEncoded: util.storeHash("#/fleet", "a b/c"),
        notAHash: util.storeHash("server/x"),
        routeHash: util.routeHash(),
        roundTrip: util.splitStoreHash(util.storeHash("#/finops/web%2F01/io", "a b/c")),
      };
    },
    locate: async () => {
      const answers = {};
      const ask = async (name, status, body) => {
        handler = () => (status === 0 ? Promise.reject(new Error("down")) : reply(status, body));
        answers[name] = await picker.locateServer(name);
      };
      await ask("found", 200, { store: "use1", server_name: "web01" });
      await ask("ambiguous", 409, { status: "ambiguous", message: "web02 is in use1 and use2.", stores: ["use1", "use2"] });
      await ask("missing", 404, { status: "not_found", message: "No store has web03." });
      await ask("broken", 500, {});
      await ask("network", 0, null);
      const url = calls[0].url;
      util.setFrontDoor(true);
      const strip = picker.ambiguousStrip(answers.ambiguous, "#/server/web02/queries");
      const links = all(strip, (n) => n.tag === "a").map((a) => ({ text: a.textContent, href: a.attrs.href }));
      const evil = picker.ambiguousStrip({ message: "<b>x</b>", stores: ["<img src=x>"] }, "#/server/web02");
      const evilLinks = all(evil, (n) => n.tag === "a").map((a) => ({ text: a.textContent, href: a.attrs.href }));
      return { answers, url, headerSent: calls[0].headers["X-Darling-Store"] ?? null, message: strip.textContent, links, evilText: evil.textContent, evilLinks };
    },
    init: async () => {
      handler = (url) => (url === "/api/front-door"
        ? reply(200, { front_door: true, stores: [
            { name: "use1", health: "ok", server_count: 3, last_ok_utc: "2026-10-28T12:00:00Z" },
            { name: "use2", health: "stale", server_count: 2, last_ok_utc: null },
            { name: "<b>use3</b>", health: "unreachable", server_count: 0, last_ok_utc: null }] })
        : reply(200, {}));
      const host = new FakeNode("div");
      const pick = async (hash, remembered) => {
        mem.clear();
        if (remembered) mem.set("darling.frontDoor.store", remembered);
        util.setStore(null);
        await picker.initFrontDoor(host, hash);
        return util.currentStore();
      };
      const fromHash = await pick("#/s/use2/fleet", "use1");
      const fromMemory = await pick("#/fleet", "use2");
      const first = await pick("", null);
      const unknownInHash = await pick("#/s/nope/fleet", null);
      const select = all(host, (n) => n.tag === "select")[0];
      const options = all(select, (n) => n.tag === "option").map((o) => o.textContent);
      util.setStore("use1");
      location.hash = "#/s/use1/server/web01/queries";
      select.value = "use2";
      for (const fn of select.handlers.change || []) fn();
      return { fromHash, fromMemory, first, unknownInHash, frontDoor: util.frontDoor(), options, hashAfter: location.hash, reloads, remembered: mem.get("darling.frontDoor.store"), storeAfter: util.currentStore(), hostHidden: host.hidden === true };
    },
    fleet: async () => {
      util.setFrontDoor(true);
      util.setStore("use1");
      handler = (url, init) => {
        if (url === "/api/session") return reply(200, { can_edit: true, front_door: true });
        if (url === "/api/fleet") {
          const s = storeOf(init);
          if (s === "use2") return reply(502, { error: "use2 did not answer in time" });
          if (s === "use1") return reply(200, fleetBody(["alpha", "bravo"]));
          if (s === "<i>use3</i>") return reply(200, fleetBody(["charlie"]));
        }
        return reply(200, {});
      };
      /* The picker's own read supplies the store list the page draws sections from. */
      const savedHandler = handler;
      handler = (url, init) => (url === "/api/front-door"
        ? reply(200, { front_door: true, stores: [
            { name: "use1", health: "ok", server_count: 2, last_ok_utc: null },
            { name: "use2", health: "unreachable", server_count: 5, last_ok_utc: null },
            { name: "<i>use3</i>", health: "stale", server_count: 1, last_ok_utc: null }] })
        : savedHandler(url, init));
      await picker.refreshStores();
      calls.length = 0;
      const page = await load("pages/fleet.js");
      const main = new FakeNode("main");
      await page.renderFleet(main);
      await settle();
      const sections = all(main, (n) => n.tag === "section").map((s) => ({
        store: s.dataset.store,
        head: s.children[0].textContent,
        text: s.children[1].textContent,
        titles: all(s, (n) => n.className === "title").map((n) => n.textContent),
        errors: all(s, (n) => n.attrs.role === "alert").map((n) => n.textContent),
        cards: all(s, (n) => String(n.className).startsWith("server-card")).length,
        badges: all(s, (n) => n.className === "silence-btn").length,
      }));
      /* a click on a card of another store opens the server in THAT store */
      const cardsOf = (store) => all(main, (n) => n.tag === "section" && n.dataset.store === store).flatMap((s) => all(s, (n) => String(n.className).startsWith("server-card")));
      const hashes = {};
      for (const store of ["use1", "<i>use3</i>"]) {
        for (const fn of cardsOf(store)[0].handlers.click || []) fn({});
        hashes[store] = location.hash;
      }
      return { sections, fleetCalls: calls.filter((c) => c.url === "/api/fleet").map((c) => c.headers["X-Darling-Store"] ?? null).sort(), hashes };
    },
    /* What the browser keeps, per store (#6840). A tag id is handed out by each store on its own, so the collapsed sidebar groups
       follow the store. A server id is a hash of the server's name, the same in every store, so favourites, acknowledgements and
       database filters follow the server and show in every store. */
    browserState: async () => {
      handler = (url) => (url === "/api/front-door"
        ? reply(200, { front_door: true, stores: [
            { name: "use1", health: "ok", server_count: 1, last_ok_utc: null },
            { name: "use2", health: "ok", server_count: 1, last_ok_utc: null }] })
        : reply(200, {}));
      const local = await load("viewer-local.js");
      mem.clear();
      mem.set("darling.local.sidebar.v1", JSON.stringify({ v: 1, collapsed: false, grouped: true, collapsedGroups: [9] }));
      local.reload();
      await picker.initFrontDoor(new FakeNode("div"), "#/s/use1/fleet");
      const view = () => ({
        store: util.currentStore(),
        favorite: local.isFavorite(3),
        acknowledged: local.isAcknowledged(3),
        dbFilter: local.getDatabaseFilter(3),
        collapsed5: local.isSidebarGroupCollapsed(5),
        collapsed6: local.isSidebarGroupCollapsed(6),
        collapsed9: local.isSidebarGroupCollapsed(9),
        grouped: local.isSidebarGrouped(),
      });
      local.toggleFavorite(3);
      local.acknowledge(3, 1000);
      local.setDatabaseFilter(3, ["db1"]);
      local.toggleSidebarGroup(5);
      const inUse1 = view();
      picker.chooseStore("use2");
      const inUse2 = view();
      local.toggleSidebarGroup(6);
      const inUse2After = view();
      picker.chooseStore("use1");
      const backInUse1 = view();
      const legacy = JSON.parse(mem.get("darling.local.sidebar.v1"));
      return { inUse1, inUse2, inUse2After, backInUse1, legacyGroups: legacy.collapsedGroups, legacyGrouped: legacy.grouped, keys: [...mem.keys()].sort() };
    },
    browserStatePlain: async () => {
      const local = await load("viewer-local.js");
      mem.clear();
      local.reload();
      local.toggleFavorite(3);
      local.acknowledge(3, 1000);
      local.setDatabaseFilter(3, ["db1"]);
      local.toggleSidebarGroup(5);
      return { sidebar: JSON.parse(mem.get("darling.local.sidebar.v1")), keys: [...mem.keys()].sort() };
    },
    sidebar: async () => {
      util.setFrontDoor(true);
      util.setStore("use1");
      const sidebar = await load("sidebar.js");
      const container = new FakeNode("div");
      const groups = [
        { store: { name: "use1", health: "ok" }, res: { kind: "data", data: fleetBody(["alpha"]) } },
        { store: { name: "use2", health: "unreachable" }, res: { kind: "error", message: "use2 is not answering" } },
        { store: { name: "<i>use3</i>", health: "stale" }, res: { kind: "data", data: fleetBody(["charlie"]) } },
      ];
      sidebar.paintStoreServerLists(container, groups, "alpha", "use1");
      const headers = all(container, (n) => n.className === "sidebar-store-header").map((n) => n.textContent);
      const items = all(container, (n) => String(n.className).startsWith("server-item")).map((n) => ({ server: n.dataset.server, active: n.className.includes("active") }));
      const errors = all(container, (n) => n.className.includes("sidebar-store-error")).map((n) => n.textContent);
      const itemOf = (name) => all(container, (n) => n.dataset && n.dataset.server === name)[0];
      for (const fn of itemOf("charlie").handlers.click || []) fn({});
      const otherHash = location.hash;
      for (const fn of itemOf("alpha").handlers.click || []) fn({});
      return { headers, items, errors, otherHash, ownHash: location.hash, stars: all(container, (n) => String(n.className).includes("fav")).length };
    },
  };
  if (!scenarios[scenario]) throw new Error("unknown scenario " + scenario);
  Object.assign(out, await scenarios[scenario]());
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
process.exit(0);
