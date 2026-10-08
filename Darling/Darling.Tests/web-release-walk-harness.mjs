/* Runs the shipped Fleet and Availability Group pages and plain-text.js under Node against a fake DOM and a fake fetch
   and prints a scenario's result as one line of JSON (release walk W1, W5, W7, W11). ReleaseWalkWordingTests starts it as
       node web-release-walk-harness.mjs <path to wwwroot/js> <scenario>
   The pages and every module are copied unchanged into a scratch folder and imported as the browser would. */
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
globalThis.confirm = () => true;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const mem = new Map();
globalThis.localStorage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, String(v)), removeItem: (k) => mem.delete(k) };
globalThis.sessionStorage = globalThis.localStorage;
globalThis.addEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });

const all = (node, pred, out = []) => { if (pred(node)) out.push(node); for (const c of node.children) all(c, pred, out); return out; };
const settle = () => new Promise((r) => setTimeout(r, 25));
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });

const offlineCard = (over) => ({
  server_id: 10, display_name: "srv-dark", server_name: "srv-dark", band: "Offline", band_label: "Offline", is_online: false,
  status: "Offline", last_collection: null, tags: [], is_silenced: false, metric_count: 0, measured_metric_count: 0, ...over,
});
const agView = (name, server) => ({
  ag_name: name, server_name: server, severity: "Healthy", severity_label: "Healthy", is_stale: false, primary_replica: server,
  collection_time: new Date().toISOString(), replicas: [], databases: [],
});
let cards = [];
let agBody = null;
let failReads = false;
globalThis.fetch = async (url) => {
  if (url === "/api/session") return reply(200, { can_edit: true });
  if (url === "/api/fleet") return reply(200, { total_servers: cards.length, critical_count: 0, warning_count: 0, offline_count: cards.length, tags: [], cards });
  if (url === "/api/ag") return reply(200, agBody);
  if (failReads && String(url).startsWith("/api/read/")) return reply(503, { error: "The store took too long to answer this read. Try again in a moment." });
  return reply(200, {});
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "release-walk-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const imp = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const texts = (root) => all(root, (n) => n.text != null && n.text !== "").map((n) => n.text);
  const byClass = (root, cls) => all(root, (n) => n.className === cls || String(n.className).split(" ").includes(cls)).map((n) => n.textContent);

  const scenarios = {
    /* W1: an offline server with no last_collection (dark past the fleet read's two-day window) says when it last collected. */
    fleetDark: async () => {
      cards = [offlineCard({})];
      const page = await imp("pages/fleet.js");
      const main = new FakeNode("main");
      await page.renderFleet(main);
      await settle();
      return { dark: texts(main).filter((x) => x.includes("no recent collection") || x.includes("last collected")) };
    },
    /* W1b: the service now sends the real last collection for a server dark past the window, so the card shows its age. */
    fleetDarkRealAge: async () => {
      cards = [offlineCard({ last_collection: new Date(Date.now() - 12 * 86400000).toISOString() })];
      const page = await imp("pages/fleet.js");
      const main = new FakeNode("main");
      await page.renderFleet(main);
      await settle();
      return { dark: texts(main).filter((x) => x.includes("no recent collection") || x.includes("last collect")) };
    },
    /* The same card with a last collection inside the window keeps its exact age. */
    fleetInWindow: async () => {
      cards = [offlineCard({ last_collection: new Date(Date.now() - 5 * 3600 * 1000).toISOString() })];
      const page = await imp("pages/fleet.js");
      const main = new FakeNode("main");
      await page.renderFleet(main);
      await settle();
      return { dark: texts(main).filter((x) => x.includes("no recent collection") || x.includes("last collect")) };
    },
    /* W5: two reporting servers that see the same group. */
    ag: async () => {
      agBody = {
        generated_at: new Date().toISOString(), distinct_ag_count: 1, reporting_server_count: 2, availability_group_count: 2,
        availability_groups: [agView("ag_fixture", "srv-a"), agView("ag_fixture", "srv-b")],
      };
      const page = await imp("pages/ag.js");
      const main = new FakeNode("main");
      await page.renderAg(main);
      await settle();
      return { labels: byClass(main, "lbl"), notes: byClass(main, "meta") };
    },
    /* W5: one view per group needs no explanation of the cards. */
    agOnePerGroup: async () => {
      agBody = {
        generated_at: new Date().toISOString(), distinct_ag_count: 2, reporting_server_count: 2, availability_group_count: 2,
        availability_groups: [agView("ag_one", "srv-a"), agView("ag_two", "srv-b")],
      };
      const page = await imp("pages/ag.js");
      const main = new FakeNode("main");
      await page.renderAg(main);
      await settle();
      return { labels: byClass(main, "lbl"), notes: byClass(main, "meta") };
    },
    /* W12: a problem list that could not be read (the service answered 503) stays on the page with the read's error. It was
       hidden as if the server had no problems. */
    serverTabReadFails: async () => {
      failReads = true;
      const mod = await imp("pages/server-tabs.js");
      const wanted = "Analysis could not read these data families";
      for (const tab of mod.SERVER_TABS) {
        let built;
        try {
          built = tab.build("srv-a", { hours: 24, label: "last 24 hours" });
        } catch {
          continue;
        }
        const nodes = Array.isArray(built) ? built : [built];
        const root = new FakeNode("div");
        for (const n of nodes) if (n && n.children) root.appendChild(n);
        const panels = all(root, (n) => String(n.className).split(" ").includes("card") && n.textContent.includes(wanted));
        if (panels.length === 0) continue;
        await settle();
        const panel = panels[0];
        return { found: true, display: panel.style.display ?? "", text: panel.textContent };
      }
      return { found: false };
    },
    /* W7 and W11: the page's words for server answers. */
    plain: async () => {
      const mod = await imp("plain-text.js");
      const inputs = JSON.parse(process.env.HARNESS_INPUT);
      return { out: inputs.map((t) => mod.plainText(t)), line: mod.NOT_COLLECTED_LINE };
    },
  };
  if (!scenarios[scenario]) throw new Error("unknown scenario " + scenario);
  Object.assign(out, await scenarios[scenario]());
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
process.exit(0);
