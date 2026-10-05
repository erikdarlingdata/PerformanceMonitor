/* Runs the shipped Collection Health tab of both engines (wwwroot/js/pages/server-tabs.js) against a scripted
   /api/read answer and prints, as one line of JSON, what each tab read and whether its collection-caveats panel is
   drawn: hidden for a payload without `stored_caveats`, a grid of the four columns for a payload with rows.
       node web-collection-caveats-harness.mjs <path to wwwroot/js> */
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

const fetches = [];
let healthBody = {};
let healthHangs = false;
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  fetches.push(u.pathname.replace("/api/read/", "") + "?" + u.searchParams.toString());
  if (healthHangs && u.pathname.endsWith("/get_collection_health")) return new Promise(() => {});
  const body = u.pathname.endsWith("/get_collection_health") ? healthBody : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "collection-caveats-"));
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  /* Modules that other open PRs add to what server-tabs.js and panels.js import: copied when present, so this harness
     keeps working whichever lands first. */
  for (const f of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js"), path.join("pages", "query-store-history.js")]) {
    if (fs.existsSync(path.join(jsDir, f))) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  }
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
  const TITLE = "Analysis could not read these data families";
  const find = (node, pred) => (pred(node) ? node : node.children.map((c) => find(c, pred)).find(Boolean) || null);
  const collect = (node, tag, out = []) => {
    if (node.tag === tag) out.push(node);
    node.children.forEach((c) => collect(c, tag, out));
    return out;
  };

  const run = async (registry, id, body, server = "srv-a", hangs = false) => {
    healthBody = body;
    healthHangs = hangs;
    fetches.length = 0;
    const tab = registry.find((t) => t.id === id);
    const panels = tab.build(server, { hours: 24, label: "24h" }).flat();
    await new Promise((r) => setTimeout(r, 50));
    const panel = panels.find((p) => p && p.tag && find(p, (n) => n.tag === "h3" && n.textContent.startsWith(TITLE)));
    const rows = panel ? collect(panel, "tr").slice(1).map((tr) => collect(tr, "td").map((td) => td.textContent)) : [];
    const heads = panel ? collect(panel, "th").map((th) => th.textContent) : [];
    return {
      present: !!panel,
      hidden: !!panel && panel.style.display === "none",
      heads,
      rows,
      healthReads: fetches.filter((f) => f.startsWith("get_collection_health?")),
      anyCaveatTool: fetches.some((f) => f.includes("caveat")),
    };
  };

  const none = { collectors: [], sweep_pressure: {} };
  const some = {
    ...none,
    stored_caveats: [
      { family: "plans", reason: "timeout", first_seen_utc: "2026-01-01T00:00:00.0000000Z", last_seen_utc: "2026-01-02T00:00:00.0000000Z" },
      { family: "waits", reason: "missing_schema", first_seen_utc: "2026-01-01T00:00:00.0000000Z", last_seen_utc: "2026-01-02T00:00:00.0000000Z" },
    ],
  };
  const out = {};
  for (const [name, registry, id] of [["sqlserver", tabs.SERVER_TABS, "health"], ["postgres", tabs.POSTGRES_TABS, "overview"]]) {
    out[name] = {
      empty: await run(registry, id, none),
      rows: await run(registry, id, some),
      // A read that never answers: a server never seen before must not show the card or its loading strip.
      pending: await run(registry, id, none, "srv-pending", true),
      // A rebuild (the 60 s poll) of a server whose last answer had rows keeps the card up while the read runs.
      rebuild: await (async () => {
        await run(registry, id, some, "srv-rebuild");
        return run(registry, id, some, "srv-rebuild", true);
      })(),
    };
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
