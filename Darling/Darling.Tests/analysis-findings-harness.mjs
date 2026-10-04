/* Runs the shipped Recommendations tab (wwwroot/js/pages/analysis-findings.js) against a recording fetch and a
   node-tree DOM, and prints the result of a scenario as one line of JSON.
       node analysis-findings-harness.mjs <path to wwwroot/js> <scenario> */
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
    this.text = text == null ? null : String(text);
    this.open = false;
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { this.children.push(c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  setAttribute(n, v) { this.attrs[n] = String(v); if (n === "open") this.open = true; }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  addEventListener(t, fn) { this.handlers[t] = fn; }
  set textContent(v) { this.children = []; this.text = String(v); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  body: new FakeNode("body"),
};

const all = (n, pred, out = []) => { if (pred(n)) out.push(n); n.children.forEach((c) => all(c, pred, out)); return out; };
const byTag = (n, tag) => all(n, (x) => x.tag === tag);
const clipboard = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clipboard.push(t); } } }, configurable: true });

const fetches = [];
let body = "{}";
globalThis.fetch = async (url) => { fetches.push(String(url)); return { status: 200, ok: true, text: async () => body }; };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "analysis-findings-"));
try {
  fs.mkdirSync(path.join(scratch, "pages"), { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "pages", "analysis-findings.js"), path.join(scratch, "pages", "analysis-findings.js"));
  const mod = await import(pathToFileURL(path.join(scratch, "pages", "analysis-findings.js")).href);

  const finding = (over) => ({
    severity: 2, category: "cpu", story_path: "path", incident_id: "inc-1", occurrences: 3,
    last_seen: "2026-01-01T00:00:00Z", time_range: { start: "2026-01-01T00:00:00Z", end: "2026-01-01T01:00:00Z" },
    advice: { headline: "Head", investigation: "Look here", remediation: "Do this" }, ...over,
  });
  const FIX = "ALTER DATABASE <b>x</b> SET AUTO_SHRINK OFF;";
  const sample = [
    finding({ finding_id: 1, severity: 0.9, advice: { headline: "Minor", investigation: "i", remediation: "r" } }),
    finding({ finding_id: 2, severity: 2.1, advice: { headline: "Major", investigation: "i", remediation: "r" } }),
    finding({ finding_id: 3, severity: 0.2, incident_id: "", advice: { headline: "Solo info", investigation: "i", remediation: "r" } }),
    finding({ finding_id: 4, severity: 1.6, incident_id: "inc-2", remediation_command: FIX, structured_remediation: { eligible: true }, advice: { headline: "Config", investigation: "i", remediation: "r" } }),
  ];
  const settle = () => new Promise((r) => setTimeout(r, 20));
  const run = async (server) => {
    fetches.length = 0;
    body = JSON.stringify({ findings: sample });
    const root = mod.analysisFindingsTab.build(server, { hours: 24, label: "last 24 hours" });
    await settle();
    return root[0];
  };
  const groupsOf = (root) => byTag(root, "details");
  const sum = (d) => d.children[0].textContent;

  const scenarios = {
    read: async () => {
      await run("srv-a");
      const u = new URL(fetches[0], "http://x.test");
      return { path: u.pathname, params: Object.fromEntries(u.searchParams) };
    },
    grouping: async () => {
      const root = await run("srv-a");
      const groups = groupsOf(root);
      return {
        headers: groups.map(sum),
        cardsPerGroup: groups.map((g) => byTag(g, "h4").length),
        open: groups.map((g) => g.open),
        order: ["Critical", "Warning", "Info"].map((l) => root.textContent.indexOf(l)),
        badgeClasses: byTag(root, "span").filter((s) => /^badge /.test(s.className)).map((s) => s.className),
      };
    },
    fix: async () => {
      const root = await run("srv-a");
      const pre = byTag(root, "pre");
      const copy = byTag(root, "button").find((b) => b.textContent === "Copy fix");
      copy.handlers.click();
      await settle();
      return { pre: pre.map((p) => p.textContent), preChildren: pre.map((p) => p.children.length), clipboard, label: copy.textContent, buttons: byTag(root, "button").length };
    },
    link: async () => {
      const root = await run("srv-a");
      return byTag(root, "a").map((a) => a.attrs.href);
    },
    rebuild: async () => {
      let root = await run("srv-a");
      let g = groupsOf(root);
      // Reader closes the Critical group and opens the Info group, then the 60 s poll rebuilds the tab.
      g[0].open = false; g[0].handlers.toggle();
      g[2].open = true; g[2].handlers.toggle();
      root = await run("srv-a");
      const again = groupsOf(root).map((x) => x.open);
      root = await run("srv-b");
      const other = groupsOf(root).map((x) => x.open);
      return { again, other };
    },
    empty: async () => {
      body = JSON.stringify({ status: "empty", message: "No findings in the requested time range." });
      const root = mod.analysisFindingsTab.build("srv-e", { hours: 24, label: "x" });
      await settle();
      return { text: root[0].textContent };
    },
  };
  console.log(JSON.stringify(await scenarios[scenario]()));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
