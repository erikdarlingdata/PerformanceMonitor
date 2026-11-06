/* #7813: what the Automatic Tuning panel prints for an empty get_plan_corrections reply. The panel keeps the tool's own sentence (it has
   no emptyWins), so the page must show the whole sentence that names the case, with nothing behind "More detail". Loads the shipped page
   scripts and prints as one line of JSON what the strip shows for each sentence given. AutomaticTuningEmptyHarnessTests starts it as
       node automatic-tuning-empty-harness.mjs <path to wwwroot/js> <sentence> [<sentence> ...] */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
const sentences = process.argv.slice(3);

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
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  addEventListener(type, fn) {
    this.handlers[type] = fn;
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
  addEventListener() {},
  removeEventListener() {},
};

const hasTag = (node, tag) => node.tag === tag || node.children.some((c) => hasTag(c, tag));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "automatic-tuning-empty-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = async (...p) => import(pathToFileURL(path.join(scratch, ...p)).href);
  const { panelEmptyStrip } = await load("panels.js");

  /* The Automatic Tuning panel of the Queries tab: a table with its own emptyText and no emptyWins, so an empty reply shows the tool's message. */
  const automaticTuning = {
    title: "Automatic Tuning",
    viz: "table",
    windowNote: false,
    rowsKey: "automatic_tuning",
    emptyText: "No per-database FORCE_LAST_GOOD_PLAN state recorded.",
  };

  const out = sentences.map((message) => {
    const strip = panelEmptyStrip(automaticTuning, { status: "empty", server: "srv", message });
    return { text: strip.textContent, detail: hasTag(strip, "details") };
  });
  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
