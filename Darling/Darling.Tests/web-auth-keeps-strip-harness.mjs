/* Runs three shipped pages (wwwroot/js/pages/ag.js, known-patterns.js and analysis-findings.js) against a fetch whose reads answer 401,
   with the shell's answer to that stood in for: util.onSessionExpired mounts a sign-in strip into main, as app.js does (#7959). Prints as
   one line of JSON what main holds once the page's render has returned, and whether the page drew its empty state anyway.
   WebAuthKeepsSignInStripTests starts it as
       node web-auth-keeps-strip-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM and fetch are stand-ins; the pages, util.js and refresh-in-place.js are the shipped code. */
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
    this.handlers = {};
    this.parent = null;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  get parentNode() {
    return this.parent;
  }
  get isConnected() {
    let n = this;
    while (n.parent) n = n.parent;
    return n.isRoot === true;
  }
  appendChild(child) {
    if (child.parent) child.parent.children = child.parent.children.filter((x) => x !== child);
    child.parent = this;
    this.children.push(child);
    return child;
  }
  insertBefore(child, ref) {
    if (child.parent) child.parent.children = child.parent.children.filter((x) => x !== child);
    child.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(child);
    else this.children.splice(i, 0, child);
    return child;
  }
  removeChild(child) {
    this.children = this.children.filter((x) => x !== child);
    child.parent = null;
    return child;
  }
  remove() {
    if (this.parent) this.parent.removeChild(this);
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, fn) {
    this.handlers[type] = fn;
  }
  removeEventListener() {}
  focus() {}
  closest() {
    return null;
  }
  querySelectorAll() {
    return [];
  }
  set textContent(value) {
    this.children.forEach((c) => (c.parent = null));
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

/* Every node the pages make, so an empty line drawn into a box the strip has already detached from main is still seen. */
const created = [];
globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  body: new FakeNode("body"),
  createElement: (tag) => {
    const n = new FakeNode(tag);
    created.push(n);
    return n;
  },
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  querySelector: () => null,
  querySelectorAll: () => [],
  addEventListener() {},
  removeEventListener() {},
};
globalThis.window = globalThis;
globalThis.location = { hash: "#/ag", pathname: "/", search: "" };
globalThis.MutationObserver = class { observe() {} disconnect() {} };

const EXPIRED = { status: 401, ok: false, text: async () => JSON.stringify({ error: "Your session has expired. Sign in again." }) };
const ok = (body) => ({ status: 200, ok: true, text: async () => JSON.stringify(body) });
const fetches = [];
let answer = () => EXPIRED;
globalThis.fetch = async (url) => {
  const u = String(url);
  fetches.push(u);
  return answer(u.split("?")[0]);
};

const cls = (n) => String(n.className || n.attrs.class || "");
const find = (n, pred) => (pred(n) ? n : n.children.map((c) => find(c, pred)).find(Boolean) || null);
const settle = () => new Promise((r) => setTimeout(r, 30));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "auth-keeps-strip-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const util = await load("util.js");

  /* The shell's answer to the first 401 (app.js showSignedOutState): the whole of main becomes the sign-in strip. */
  const main = new FakeNode("main");
  main.isRoot = true;
  const strip = util.el("div", { class: "strip error", role: "alert", text: "Your session has expired. Sign in again." });
  let expiries = 0;
  util.onSessionExpired(() => {
    expiries++;
    util.mount(main, strip);
  });
  const report = (emptyText, extra) => ({
    expiries,
    stripKept: main.children.length === 1 && main.children[0] === strip,
    mainText: main.textContent,
    emptyTextDrawn: created.some((n) => cls(n) === "strip empty" && n.textContent.includes(emptyText)),
    reads: fetches.map((f) => f.split("?")[0]),
    ...extra,
  });

  const group = {
    ag_name: "AG1", group_id: "g1", server_name: "srv-a", severity: "Healthy", severity_label: "Healthy", primary_replica: "srv-a", collection_time: "2026-11-06T10:00:00Z",
    replicas: [{ replica_server_name: "srv-a", role: "PRIMARY", is_primary: true, is_local: true, severity: "Healthy" }], databases: [],
  };
  const agData = { generated_at: "2026-11-06T10:00:00Z", availability_groups: [group], availability_group_count: 1, distinct_ag_count: 1, reporting_server_count: 1 };

  const scenarios = {
    /* The groups read and the fleet read both answer 401. */
    ag: async () => {
      const { renderAg } = await load(path.join("pages", "ag.js"));
      await renderAg(main);
      await settle();
      return report("No Availability Groups observed", { groupsDrawn: main.textContent.includes("AG1") });
    },
    /* The groups read answers one group and the fleet read answers 401: the cards must not be drawn over the strip either. */
    agFleet: async () => {
      const { renderAg } = await load(path.join("pages", "ag.js"));
      answer = (p) => (p === "/api/ag" ? ok(agData) : EXPIRED);
      await renderAg(main);
      await settle();
      return report("No Availability Groups observed", { groupsDrawn: main.textContent.includes("AG1") });
    },
    /* The session and the server list answer as they did while the session was valid; the known patterns read answers 401. */
    knownPatterns: async () => {
      location.hash = "#/known-patterns";
      const { renderKnownPatterns } = await load(path.join("pages", "known-patterns.js"));
      answer = (p) => (p === "/api/session" ? ok({ can_edit: true }) : p === "/api/admin/servers" ? ok({ servers: [] }) : EXPIRED);
      await renderKnownPatterns(main);
      await settle();
      return report("No known patterns are recorded.");
    },
    /* The Recommendations tab of a server page: its findings read answers 401. The body it was filling is left as it was. */
    recommendations: async () => {
      const { analysisFindingsTab } = await load(path.join("pages", "analysis-findings.js"));
      const root = analysisFindingsTab.build("srv-a", { hours: 24, label: "last 24 hours" })[0];
      await settle();
      const body = find(root, (n) => cls(n) === "panel-body");
      return report("No findings in this window.", {
        bodyText: body ? body.textContent : null,
        bodyEmptied: body ? body.children.length === 0 : null,
        bodyLoading: body ? body.children.some((c) => cls(c) === "strip loading") : null,
      });
    },
  };
  if (!(scenario in scenarios)) throw new Error("unknown scenario " + scenario);
  console.log(JSON.stringify(await scenarios[scenario]()));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
