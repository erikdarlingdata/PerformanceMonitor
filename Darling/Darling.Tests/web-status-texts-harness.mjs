/* Runs the shipped web page script for status texts that used to say two things at once (wwwroot/js/pages/server-tabs.js,
   active-live.js, read-fields.js) and prints, as one line of JSON: the Queries tab's note and the live-read notice (one
   banner each, no repeated advice), the Overview CPU tile's label, and the Running Jobs panel's empty text for a server
   collecting normally and for one whose collection is stale.
       node web-status-texts-harness.mjs <path to wwwroot/js> [edit|readonly]
   The second argument is the signed-in seat the Active Queries live bar is drawn for (default edit). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
const seat = process.argv[3] === "readonly" ? "readonly" : process.argv[3] === "failed" ? "failed" : "edit";

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
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
  addEventListener() {}
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
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

/* The fleet roll-up the server page loads its card from: one server whose collection is stale (is_online false), one collecting. */
const FLEET = {
  cards: [
    { server_name: "page-stale", display_name: "page-stale", is_online: false },
    { server_name: "page-online", display_name: "page-online", is_online: true },
    { server_name: "page-flip", display_name: "page-flip", is_online: true },
  ],
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = u.pathname === "/api/fleet"
    ? FLEET
    : u.pathname === "/api/session"
    ? (seat === "failed" ? {} : { can_edit: seat === "edit", managed_store: false, admin_writes_off: false })
    : u.pathname === "/api/active-queries/live/available"
    ? { can_queue: true, can_edit: true, managed_store: false }
    : u.pathname.endsWith("/get_running_jobs")
    ? { status: "empty", message: "No running SQL Agent jobs found (or the running_jobs collector has not run yet)." }
    : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "status-texts-"));
try {
  /* The whole js tree is copied, then the chart module is replaced by a stand-in, as the other panel harnesses do. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
  const live = await import(pathToFileURL(path.join(scratch, "pages", "active-live.js")).href);
  const fields = await import(pathToFileURL(path.join(scratch, "read-fields.js")).href);
  const templates = await import(pathToFileURL(path.join(scratch, "view-templates.js")).href);

  const find = (node, pred) => (pred(node) ? node : node.children.map((c) => find(c, pred)).find(Boolean) || null);

  const runningJobsText = async (offline, server) => {
    const tab = tabs.SERVER_TABS.find((t) => t.id === "activity");
    const panels = tab.build(server, { hours: 24, label: "24h", offline }).flat();
    await new Promise((r) => setTimeout(r, 50));
    const panel = panels.find((p) => p && p.tag && find(p, (n) => n.tag === "h3" && n.textContent.startsWith("Running Jobs")));
    return panel ? panel.textContent : null;
  };

  /* The server page itself: what its Running Jobs panel says once the page has the server's fleet card, which is what hands the
     tab its ctx.offline (pages/server.js). The panel's own section is the heading's parent. */
  const pageSrv = await import(pathToFileURL(path.join(scratch, "pages", "server.js")).href);
  const pageRunningJobsText = async (server) => {
    const holder = new FakeNode("div");
    pageSrv.renderServer(holder, server, "activity", {});
    await new Promise((r) => setTimeout(r, 300));
    const section = find(holder, (n) => n.children.some((c) => c.tag === "h3" && c.textContent.startsWith("Running Jobs")));
    return section ? section.textContent : null;
  };

  /* The poll where a server first goes stale: the page already holds an online card for it, paints from that, and the fresh card
     (is_online false) arrives afterwards. The Running Jobs panel has to end up built from the fresh card. */
  const pageFlipText = async () => {
    const first = new FakeNode("div");
    pageSrv.renderServer(first, "page-flip", "activity", {});
    await new Promise((r) => setTimeout(r, 300));
    FLEET.cards.find((c) => c.server_name === "page-flip").is_online = false;
    const second = new FakeNode("div");
    pageSrv.renderServer(second, "page-flip", "activity", { poll: true });
    await new Promise((r) => setTimeout(r, 300));
    const section = find(second, (n) => n.children.some((c) => c.tag === "h3" && c.textContent.startsWith("Running Jobs")));
    return section ? section.textContent : null;
  };

  /* The Active Queries live bar for the seat this run signed in as: what it says, and how many buttons it draws. */
  const liveBarFor = async () => {
    const bar = live.activeQueriesLiveBar("srv-bar", []);
    await new Promise((r) => setTimeout(r, 50));
    const buttons = [];
    const walk = (n) => { if (n.tag === "button") buttons.push(n.textContent); n.children.forEach(walk); };
    walk(bar);
    return { text: bar.textContent, buttons, status: bar.dataset.status || null };
  };

  /* The starter dashboard "Server health at a glance": its Overview panel's CPU stat label, as a created view would save it. */
  const starter = templates.DASHBOARD_TEMPLATES.find((t) => t.key === "server-health").make("srv");
  const starterOverview = starter.definition.panels.find((p) => p.read === "get_server_summary");

  const queries = tabs.SERVER_TABS.find((t) => t.id === "queries");
  const summary = fields.READ_FIELDS.get_server_summary;
  const noteStrip = tabs.tabNote(queries, 24);
  console.log(
    JSON.stringify({
      queriesNoteText: noteStrip ? noteStrip.textContent : null,
      liveNotice: live.liveUnavailableNote(false),
      managedLiveNotice: live.liveUnavailableNote(true),
      tileLabel: summary.stat.stats.find((s) => s.key === "cpu_percent").label,
      starterTileLabel: starterOverview.stats.find((x) => x.key === "cpu_percent").label,
      columnLabel: summary.table.columns.find((c) => c.key === "cpu_percent").label,
      jobsOnline: await runningJobsText(false, "srv-online"),
      jobsStale: await runningJobsText(true, "srv-stale"),
      pageJobsStale: await pageRunningJobsText("page-stale"),
      pageJobsOnline: await pageRunningJobsText("page-online"),
      pageJobsFlip: await pageFlipText(),
      liveBar: await liveBarFor(),
    })
  );
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
