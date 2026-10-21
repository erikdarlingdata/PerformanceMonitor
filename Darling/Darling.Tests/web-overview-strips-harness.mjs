/* Runs the web viewer's shipped pages/overview-strips.js (the Overview's five stacked strips, #6291) against a stand-in DOM and
   prints what one scenario drew as one line of JSON. WebOverviewStripsBehaviourTests starts it as
       node web-overview-strips-harness.mjs <path to wwwroot/js> <scenario>
   Scenarios: crosshair, zoom, empty. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 2];
const scenario = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; this.style = {}; this.className = "";
    this.listeners = {}; this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { this.children.push(c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  setAttribute(n, v) { this.attrs[n] = String(v); }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  addEventListener(t, l) { (this.listeners[t] = this.listeners[t] || []).push(l); }
  setPointerCapture() {}
  replaceChild(next, old) { const i = this.children.indexOf(old); if (i >= 0) this.children[i] = next; return old; }
  getBoundingClientRect() { return { left: 0, top: 0, width: Number(this.attrs.width) || 1000, height: Number(this.attrs.height) || 300 }; }
  set textContent(v) { this.children = []; this.text = String(v); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.location = { hash: "#/server/A/overview" };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "overview-strips-"));
let strips;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  strips = await import(pathToFileURL(path.join(scratch, "pages", "overview-strips.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const find = (node, pred, out = []) => { if (pred(node)) out.push(node); node.children.forEach((c) => find(c, pred, out)); return out; };
const T0 = Date.UTC(2026, 0, 1, 12, 0, 0);
const MIN = 60000;
const iso = (m) => new Date(T0 + m * MIN).toISOString().slice(0, 19);

/* Six reads that each sample on their own clock: CPU every minute, waits every 5, memory every 10, file I/O every 15 (two files),
   blocking at minutes 12 and 47, no deadlocks (an empty envelope). */
const data = (d) => ({ kind: "data", data: d });
const reads = {
  /* the collector was down from minute 31 to 44: a gap in the CPU samples */
  cpu: data({ samples: Array.from({ length: 61 }, (_, i) => i).filter((i) => i < 31 || i > 44).map((i) => ({ sample_time: iso(i), sql_server_cpu: 10 + i, other_process_cpu: 5 })) }),
  waits: data({ trend: Array.from({ length: 13 }, (_, i) => ({ time: iso(i * 5), wait_time_ms_per_second: 100 + i * 10 })) }),
  blocking: data({ trend: [{ time: iso(12), count: 3 }, { time: iso(47), count: 1 }] }),
  deadlocks: { kind: "empty", message: "No deadlocks in this window." },
  memory: data({ trend: Array.from({ length: 7 }, (_, i) => ({ time: iso(i * 10), buffer_pool_mb: 2000 + i })) }),
  io: data({ trend: Array.from({ length: 5 }, (_, i) => [
    { time: iso(i * 15), database_name: "A", file_type: "ROWS", reads: 900, avg_read_latency_ms: 2 },
    { time: iso(i * 15), database_name: "B", file_type: "ROWS", reads: 100, avg_read_latency_ms: 12 },
  ]).flat() }),
};
const domain = { from: T0, to: T0 + 60 * MIN };
const readoutTexts = (root) => Object.fromEntries(find(root, (n) => n.attrs["data-strip"]).map((g) => [g.attrs["data-strip"], find(g, (n) => n.attrs.class === "strip-readout")[0].textContent]));
const plotL = 54;
const plotW = 1000 - 54 - 12;
const xAt = (m) => plotL + (m / 60) * plotW;

const out = {};
if (scenario === "crosshair") {
  const built = strips.buildStrips(reads);
  const drawn = strips.drawStrips(built, domain, 1000, null);
  out.stripCount = find(drawn.node, (n) => n.attrs["data-strip"]).length;
  const at = (m) => drawn.crosshairAt(T0 + m * MIN);
  const pick = (found, strip) => found.filter((f) => f.strip === strip).map((f) => ({ series: f.series, t: f.t == null ? null : (f.t - T0) / MIN, value: f.value }));
  /* minute 22.4: CPU has a sample at 22, waits at 20, memory at 20; file I/O (15, 30) and blocking (12, 47) have none near it */
  const f1 = at(22.4);
  out.at22 = { cpu: pick(f1, "cpu"), waits: pick(f1, "waits"), memory: pick(f1, "memory"), io: pick(f1, "io"), blocking: pick(f1, "blocking") };
  out.at22Text = readoutTexts(drawn.node);
  /* minute 38 is inside the CPU gap: CPU has no sample, while waits (35) and memory (40) answer from their own times */
  const f3 = at(38);
  out.at38 = { cpu: pick(f3, "cpu"), waits: pick(f3, "waits"), memory: pick(f3, "memory") };
  out.at38Text = readoutTexts(drawn.node);
  /* minute 12.2 sits on the blocking bucket; the deadlock series (empty) has no sample */
  out.at12 = { blocking: pick(at(12.2), "blocking") };
  /* minute 30: the file I/O bucket of both files, weighted 900:100 -> (2*900 + 12*100)/1000 = 3 ms */
  out.at30 = { io: pick(at(30), "io") };
  /* a pointer move over the plot drives the same lookup */
  drawn.node.listeners.pointermove[0]({ clientX: xAt(30) });
  out.pointerText = readoutTexts(drawn.node);
  out.hoverLines = find(drawn.node, (n) => n.attrs.class === "hover-line").length;
} else if (scenario === "zoom") {
  const built = strips.buildStrips(reads);
  let zoomed = null;
  const drawn = strips.drawStrips(built, domain, 1000, (from, to) => { zoomed = { from: (from - T0) / MIN, to: (to - T0) / MIN }; });
  /* a drag across the plot at minutes 20..40: one svg takes the pointer for all five strips */
  drawn.node.listeners.pointerdown[0]({ button: 0, clientX: xAt(20), pointerId: 1 });
  drawn.node.listeners.pointermove[0]({ clientX: xAt(30) });
  out.brushShown = find(drawn.node, (n) => n.attrs.class === "brush-rect")[0].style.display !== "none";
  drawn.node.listeners.pointerup[0]({ clientX: xAt(40) });
  out.zoomed = zoomed;
  /* a click (no drag) zooms nothing */
  zoomed = null;
  drawn.node.listeners.pointerdown[0]({ button: 0, clientX: xAt(10), pointerId: 1 });
  drawn.node.listeners.pointerup[0]({ clientX: xAt(10) + 2 });
  out.clickZoomed = zoomed;
  /* the full window: the CPU gap breaks each of its two lines (SQL, Total) in two, so no line runs across minutes 31..44 */
  out.cpuPolylinesFull = find(find(drawn.node, (n) => n.attrs["data-strip"] === "cpu")[0], (n) => n.tag === "polyline").length;
  /* redrawn on the held span, every strip is clipped to it: the CPU line carries minutes 20..40 plus one neighbour either side */
  const z = strips.drawStrips(built, { from: T0 + 20 * MIN, to: T0 + 40 * MIN }, 1000, null);
  const cpu = find(z.node, (n) => n.attrs["data-strip"] === "cpu")[0];
  out.cpuPointsZoomed = find(cpu, (n) => n.tag === "polyline")[0].attrs.points.split(" ").length;
  const wait = find(z.node, (n) => n.attrs["data-strip"] === "waits")[0];
  out.waitPointsZoomed = find(wait, (n) => n.tag === "polyline")[0].attrs.points.split(" ").length;
  out.axisLabels = find(z.node, (n) => n.tag === "text" && /^\d\d:\d\d$/.test(n.textContent)).map((n) => n.textContent);
  out.stripsDrawnZoomed = find(z.node, (n) => n.attrs["data-strip"]).length;
} else if (scenario === "empty") {
  const none = {
    cpu: { kind: "empty", message: "No CPU samples in this window." },
    waits: { kind: "error", message: "boom" },
    blocking: { kind: "empty", message: "x" },
    deadlocks: { kind: "empty", message: "y" },
    memory: reads.memory,
    io: { kind: "empty", message: "No file I/O samples in this window." },
  };
  const built = strips.buildStrips(none);
  const drawn = strips.drawStrips(built, domain, 1000, null);
  const g = (key) => find(drawn.node, (n) => n.attrs["data-strip"] === key)[0];
  const none_ = (key) => find(g(key), (n) => n.attrs.class === "strip-none")[0].textContent;
  out.cpu = { lines: find(g("cpu"), (n) => n.tag === "polyline").length, text: none_("cpu") };
  out.waits = { lines: find(g("waits"), (n) => n.tag === "polyline").length, text: none_("waits") };
  out.blocking = { bars: find(g("blocking"), (n) => n.tag === "rect").length, text: none_("blocking") };
  out.memoryLines = find(g("memory"), (n) => n.tag === "polyline").length;
}
console.log(JSON.stringify(out));
