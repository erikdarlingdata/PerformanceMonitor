/* #5914: the report-link rule of the Investigations panel on the shipped Alert History page script (wwwroot/js/pages/alerts.js),
   run under Node with a fake DOM. ReportLinksPageTests starts it as
       node report-links-harness.mjs <path to wwwroot/js> <path to report-link-vectors.json>
   and reads one line of JSON. It feeds every vector of the shared list (the same file InvestigationReportLinksTests reads for the
   C# rule) through reportLinkProblem and reportLinksCell, and reports what each drew: the problem text, each anchor's href, target
   and rel, and the cell's text. Only the DOM and charts.js are stand-ins; panels.js, util.js and mute-context.js are the real files. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {};
  }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children.forEach((c) => (c.parent = null)); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/alerts" };

const jsDir = process.argv[2];
const vectorsFile = process.argv[3];

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "report-links-"));
try {
  /* The whole js tree is copied (#5279), then the stand-in is written after it so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return null; }\n'
  );
  const page = await import(pathToFileURL(path.join(scratch, "pages", "alerts.js")).href);

  const out = { vectors: [] };
  for (const v of JSON.parse(fs.readFileSync(vectorsFile, "utf8")).vectors) {
    const url = v.padTo ? v.url.padEnd(v.padTo, "a") : v.url;
    const cell = page.reportLinksCell([{ kind: "jira", key: "OPS-1", url }]);
    out.vectors.push({
      name: v.name,
      problem: page.reportLinkProblem(url),
      anchors: cell.all("a").map((a) => ({ href: a.attrs.href, target: a.attrs.target, rel: a.attrs.rel, title: a.attrs.title ?? null, text: a.textContent })),
      text: cell.textContent,
    });
  }

  /* Anything that is not a link list draws an empty cell; a non-object entry is skipped; a missing url is drawn as text. */
  const empty = (links) => { const c = page.reportLinksCell(links); return { anchors: c.all("a").length, text: c.textContent }; };
  out.noLinks = empty(null);
  out.emptyList = empty([]);
  out.notAnArray = empty("https:" + "//example.invalid/x");
  out.nonObjectEntries = empty([null, 7, "x"]);
  out.missingUrl = empty([{ kind: "jira", key: "OPS-2" }]);

  /* The page's row builder hands the newest report's links to the Links column. */
  const rows = page.investigationRows(
    { requests: [{ request_id: "r1", state: "reported" }, { request_id: "r2", state: "open" }] },
    { reports: [
        { request_id: "r1", submitted_at: "2026-01-01T00:00:00Z", verdict: "old", links: [{ kind: "jira", url: "https:" + "//old.example.invalid/1" }] },
        { request_id: "r1", submitted_at: "2026-01-02T00:00:00Z", verdict: "new", links: [{ kind: "pull_request", key: "#42", url: "https:" + "//git.example.invalid/pm/pull/42" }] },
      ] });
  out.rows = rows.map((r) => ({ id: r.request_id, verdict: r.verdict, links: r.links }));

  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
