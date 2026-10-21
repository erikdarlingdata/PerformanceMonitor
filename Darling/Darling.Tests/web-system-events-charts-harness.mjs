/* Runs the web System Events charts (#6295): pages/system-events-charts.js with the shipped panels.js and charts.js, against a
   stand-in DOM, and prints what each chart drew as one line of JSON. WebSystemEventsChartsBehaviourTests starts it as
       node web-system-events-charts-harness.mjs <path to wwwroot/js>
   The DOM is a node tree of plain objects. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.listeners = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parent = this;
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
  setPointerCapture() {}
  focus() {}
  get offsetWidth() { return 0; }
  get offsetHeight() { return 0; }
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
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  body: new FakeNode("body"),
};
globalThis.location = { hash: "#/server/A/events" };

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "sys-events-charts-")));
let sec, panels;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  panels = await import(pathToFileURL(path.join(scratch, "panels.js")).href);
  sec = await import(pathToFileURL(path.join(scratch, "pages", "system-events-charts.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}
process.on("exit", () => fs.rmSync(scratch, { recursive: true, force: true }));

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const cls = (n) => String(n.className || n.attrs.class || "");

const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const iso = (ms) => new Date(ms).toISOString().slice(0, 19);
const entry = (i, extra) => ({ event_time: iso(T0 + i * 600000), ...extra });

/* What a spec's viz drew: its text (the empty line, or the chart's labels) and whether an svg chart is in it. */
const drew = (spec, data) => {
  const node = panels.VIZ[spec.viz](data, { ...spec, read: "get_health_parser_system_health", windowHours: 24 });
  const texts = find(node, (n) => n.tag === "text" || n.tag === "span").map((n) => n.textContent);
  return { text: node.textContent, texts, hasChart: find(node, (n) => n.tag === "svg").length > 0 };
};

const specs = sec.systemEventsChartSpecs("last 24 h");
const byTitle = Object.fromEntries(specs.map((s) => [s.title, s]));
const out = { titles: specs.map((s) => s.title), spans: specs.map((s) => s.span), hides: specs.every((s) => s.hideWhenNoRows === true) };

/* A counter collected as 0 is a real 0: the chart draws. A counter no bucket carried says so. */
const zeros = { entries: [0, 1, 2].map((i) => entry(i, { bad_pages_detected: 0, latch_warnings: null })), sick_spinlock_types: [] };
out.zeroDrawn = drew(byTitle["Corruption Events: Bad Pages"], zeros);
out.absent = drew(byTitle["Contention Events: Latch Warnings"], zeros);
out.hasData = {
  zero: sec.counterHasData(zeros.entries, "bad_pages_detected"),
  absent: sec.counterHasData(zeros.entries, "latch_warnings"),
  missingField: sec.counterHasData(zeros.entries, "no_such_field"),
  none: sec.counterHasData([], "bad_pages_detected"),
};

/* Spinlocks: one series per named type, a gap where a bucket did not report it; none named is the empty line. */
const spin = {
  sick_spinlock_types: ["SOS_CACHESTORE", "LOCK_HASH"],
  entries: [
    entry(0, { sick_spinlocks: [{ type: "SOS_CACHESTORE", backoffs: 120 }] }),
    entry(1, { sick_spinlocks: [] }),
    entry(2, { sick_spinlocks: [{ type: "LOCK_HASH", backoffs: 5 }, { type: "SOS_CACHESTORE", backoffs: 300 }] }),
  ],
};
out.spinPoints = sec.spinlockPoints(spin);
out.spinDrawn = drew(byTitle["Contention Events: Sick Spinlocks"], spin);
out.spinNone = drew(byTitle["Contention Events: Sick Spinlocks"], { sick_spinlock_types: [], entries: [entry(0, { sick_spinlocks: [] })] });

console.log(JSON.stringify(out));
