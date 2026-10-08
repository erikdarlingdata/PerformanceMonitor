/* Runs the pick-list order, the Deadlocks tile explanation, the hide-empty stat tiles and the underscore counts of the shipped
   web viewer under Node (Darling web click-through, part D). WebClickthroughPickListTests starts it as
       node web-clickthrough-pick-harness.mjs <path to wwwroot/js> <scenario>
   `fetch` and the DOM are stand-ins; everything else is the shipped code. */
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
    this.listeners = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, listener) {
    (this.listeners[type] = this.listeners[type] || []).push(listener);
  }
  focus() {
    globalThis.document.activeElement = this;
  }
  setSelectionRange(start, end) {
    this.selectionStart = start;
    this.selectionEnd = end;
  }
  /* Enough of querySelector for ".class": the first descendant that carries the class. */
  querySelector(selector) {
    const wanted = String(selector).replace(/^\./, "");
    const walk = (n) => {
      for (const c of n.children || []) {
        if (String(c.className).split(" ").includes(wanted)) return c;
        const hit = walk(c);
        if (hit) return hit;
      }
      return null;
    };
    return walk(this);
  }
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 1000, height: 320 };
  }
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}


globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

/* list_servers answers in the registry's order (not the sidebar's); everything else answers an empty object. */
const REGISTRY = ["PG18", "AG1", "AG2", "PGEXT", "SQL2016"].map((n) => ({
  server_name: n,
  display_name: n,
  engine_kind: n.startsWith("PG") ? "postgres" : "sqlserver",
}));
/* /api/fleet answers its cards in the reverse of the sidebar's order, with ids and a favourite-able fifth server. */
const FLEET_CARDS = [["SQL2016", 5], ["PGEXT", 4], ["AG2", 3], ["AG1", 2], ["PG18", 1]].map(([n, id]) => ({
  server_name: n, display_name: n, server_id: id,
}));
const fetches = [];
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  await new Promise((r) => setTimeout(r, 5));
  const raw = String(url).includes("/api/read/list_servers")
    ? JSON.stringify({ server_count: REGISTRY.length, servers: REGISTRY })
    : String(url).includes("/api/fleet")
      ? JSON.stringify({ cards: FLEET_CARDS, tags: [] })
      : JSON.stringify({});
  return { status: 200, ok: true, text: async () => raw };
};

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "clickthrough-pick-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    order: await load("server-order.js"),
    local: await load("viewer-local.js"),
    plain: await load("plain-text.js"),
    panels: await load("panels.js"),
    fleet: await load(path.join("pages", "fleet.js")),
    jobs: await load(path.join("pages", "job-history.js")),
    finops: await load(path.join("pages", "finops.js")),
    alerts: await load(path.join("pages", "alerts.js")),
    editor: await load("editor.js"),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => { for (let i = 0; i < 80; i++) await new Promise((r) => setTimeout(r, 2)); };
const optionTexts = (root, label) =>
  all(root, (n) => n.tag === "select" && n.attrs["aria-label"] === label)
    .map((s) => s.children.map((o) => o.text));

const cards = [["PG18", 1], ["AG1", 2], ["AG2", 3], ["PGEXT", 4], ["SQL2016", 5]].map(([n, id]) => ({
  server_name: n, display_name: n, server_id: id,
}));

const scenarios = {
  order: async () => {
    modules.order.rememberFleet(cards);
    const names = (rows) => rows.map((r) => r.server_name || r.value);
    const plain = names(modules.order.orderServers(REGISTRY));
    const options = names(modules.order.orderServers(REGISTRY.map((r) => ({ value: r.server_name, label: r.display_name }))));
    const fromCards = names(modules.order.orderServers([...cards].reverse()));
    modules.local.toggleFavorite(4);
    const withFavorite = names(modules.order.orderServers(REGISTRY));
    const sidebarSameFunction = modules.order.byDisplayName({ display_name: "a" }, { display_name: "b" }) < 0;
    return { plain, options, fromCards, withFavorite, sidebarSameFunction };
  },
  pages: async () => {
    modules.order.rememberFleet(cards);
    const jobs = new FakeNode("main");
    modules.jobs.renderJobHistory(jobs);
    const fin = new FakeNode("main");
    modules.finops.renderFinops(fin, "", "utilization");
    const alerts = new FakeNode("main");
    modules.alerts.renderAlerts(alerts);
    await settle();
    return {
      jobs: optionTexts(jobs, "Server")[0] || [],
      finops: optionTexts(fin, "Server")[0] || [],
      alerts: all(alerts, (n) => n.tag === "select").map((s) => s.children.map((o) => o.text)),
    };
  },
  fleetOptions: async () => {
    const plain = (await modules.editor.loadFleetOptions()).map((o) => o.label);
    modules.local.toggleFavorite(5);
    const withFavorite = (await modules.editor.loadFleetOptions()).map((o) => o.label);
    return { plain, withFavorite };
  },
  deadlocks: async () => {
    const sub = modules.fleet.deadlockCoverageSub({ servers_total: 9, servers_read: 6 });
    const full = modules.fleet.deadlockCoverageSub({ servers_total: 9, servers_read: 9 });
    const tile = modules.fleet.rollup({
      total_servers: 9, healthy_count: 1, warning_count: 0, critical_count: 0, offline_count: 0,
      total_blocking_events: 0, total_deadlocks: 0, deadlock_coverage: { servers_total: 9, servers_read: 6, note: "NOTE" },
    });
    const subNodes = all(tile, (n) => n.className === "sub partial").map((n) => ({ text: n.text, title: n.attrs.title || n.title || null }));
    return { text: sub.text, title: sub.title, fullTitle: full.title ?? null, subNodes };
  },
  stats: async () => {
    const data = { a: { n: 3, which: null, elapsed: null, newest: null, why: null }, nullable: { x: null, note: "n/a (reason)" } };
    const desc = {
      viz: "stat",
      stats: [
        { key: "a.n", label: "Count", format: "int" },
        { key: "a.which", label: "Which read", format: "text", hideWhenEmpty: true },
        { key: "a.elapsed", label: "Ran for", format: "ms", hideWhenEmpty: true },
        { key: "a.keep", label: "Plain dash", format: "int" },
        { key: "nullable.x", label: "Has why", format: "text", hideWhenEmpty: true, nullKey: "nullable.note" },
      ],
    };
    const node = modules.panels.VIZ.stat(data, desc);
    return { labels: all(node, (n) => n.className === "label").map((n) => n.text) };
  },
  plain: async () => ({
    out: JSON.parse(process.env.HARNESS_INPUT).map((t) => modules.plain.plainText(t)),
  }),
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
