/* Runs the web viewer's shipped pages/overview-strips.js (the Overview's five stacked strips, #6291) against a stand-in DOM and
   prints what one scenario drew as one line of JSON. WebOverviewStripsBehaviourTests starts it as
       node web-overview-strips-harness.mjs <path to wwwroot/js> <scenario>
   Scenarios: crosshair, zoom, empty, baselines, firstload, localtime. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 2];
const scenario = process.argv[process.argv.length - 1];
/* The axis and the readout are in the browser's local time (#6360): pin a zone that is not UTC, so a runner in UTC cannot pass by luck. */
process.env.TZ = "America/New_York";

class FakeNode {
  constructor(tag, text) {
    this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; this.style = {}; this.className = "";
    this.listeners = {}; this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  setAttribute(n, v) { this.attrs[n] = String(v); }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  addEventListener(t, l) { (this.listeners[t] = this.listeners[t] || []).push(l); }
  setPointerCapture() {}
  replaceChild(next, old) { const i = this.children.indexOf(old); if (i >= 0) this.children[i] = next; return old; }
  getBoundingClientRect() {
    /* The strips' host is 328 px wide (a phone) once it is in the page and has no width before: a browser lays out nothing that is not attached. */
    if (this.className === "chart overview-strips-host") return { left: 0, top: 0, width: this.parent ? 328 : 0, height: 482 };
    return { left: 0, top: 0, width: Number(this.attrs.width) || 1000, height: Number(this.attrs.height) || 300 };
  }
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
globalThis.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };
const observers = [];
globalThis.ResizeObserver = class { constructor(cb) { this.cb = cb; observers.push(this); } observe() {} disconnect() {} };

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
/* #7644: the reply carries each lane as hourly steps. A flat bucket is one step that covers the whole harness window. */
const laneOf = (bk) => (bk ? { steps: [{ from: new Date(T0 - 60 * MIN).toISOString(), to: new Date(T0 + 180 * MIN).toISOString(), ...bk }] } : null);

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
  out.at12Text = readoutTexts(drawn.node);
  /* minute 30: the file I/O bucket of both files, weighted 900:100 -> (2*900 + 12*100)/1000 = 3 ms */
  out.at30 = { io: pick(at(30), "io") };
  /* a pointer move over the plot drives the same lookup */
  drawn.node.listeners.pointermove[0]({ clientX: xAt(30) });
  out.pointerText = readoutTexts(drawn.node);
  out.hoverLines = find(drawn.node, (n) => n.attrs.class === "hover-line").length;
} else if (scenario === "snapshot") {
  /* #7219: a window with no blocked-process reports is counted from DMV snapshot rows; the read's `source` key says so, and the strip
     must not call those rows reports. */
  const snap = { ...reads, blocking: data({ source: "DMV snapshot", trend: [{ time: iso(12), count: 3 }] }) };
  const built = strips.buildStrips(snap);
  out.series = built.find((s) => s.key === "blocking").series.map((s) => s.label);
  const drawn = strips.drawStrips(built, domain, 1000, null);
  drawn.crosshairAt(T0 + 12.2 * MIN);
  out.at12Text = readoutTexts(drawn.node);
  out.reportsSeries = strips.buildStrips({ ...reads, blocking: data({ source: "blocked-process-report", trend: [{ time: iso(12), count: 3 }] }) })
    .find((s) => s.key === "blocking").series.map((s) => s.label);
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
  out.axisLabels = find(z.node, (n) => n.tag === "text" && /^\d\d:\d\d/.test(n.textContent)).map((n) => n.textContent);
  out.stripsDrawnZoomed = find(z.node, (n) => n.attrs["data-strip"]).length;
} else if (scenario === "baselines") {
  const window = { windowStart: T0, windowEnd: T0 + 60 * MIN };
  const baseline = (cpu, waits, io, blocking) => ({ kind: "data", data: { baselines: { cpu: laneOf(cpu), waits: laneOf(waits), io: laneOf(io), blocking: laneOf(blocking) } } });
  const b = (mean, sd) => ({ mean, effective_std_dev: sd, sample_count: 40, tier: "Full" });
  const fresh = () => strips.buildStrips(reads);
  const byKey = (list, key) => list.find((s) => s.key === key);
  const summary = (list) => Object.fromEntries(list.map((s) => [s.key, {
    band: s.band ? s.band.map((x) => ({ lower: x.lower, upper: x.upper, mean: x.mean })) : null,
    markers: s.markers ? s.markers.length : null,
    note: s.baselineNote,
  }]));

  /* The Viewer's rule: a band of mean +- 2 effective sigma over the window; a marker above the band AND at or above the floor, or below it. */
  const a = strips.applyBaselines(fresh(), baseline(b(20, 5), b(120, 10), b(1, 0.2), b(0.1, 0.3)), window, {});
  out.full = summary(a);
  out.cpuMarkedValues = byKey(a, "cpu").markers.map((m) => m.v);
  out.waitMarkedValues = byKey(a, "waits").markers.map((m) => m.v);
  out.ioMarkerCount = byKey(a, "io").markers.length;

  /* The floor: 1.5 ms is above a 0.7 ms band but under the 2 ms floor, so only 2.5 ms is marked (each bucket holds 1000 reads, over the 250 read floor); a point under the band is marked at any value. */
  const lone = [{ key: "io", series: [{ points: [{ t: T0 + 1, v: 1.5, reads: 1000 }, { t: T0 + 2, v: 2.5, reads: 1000 }, { t: T0 + 3, v: 0.1, reads: 1000 }] }] }];
  strips.applyBaselines(lone, baseline(null, null, b(0.5, 0.1), null), window, {});
  out.floorMarkedValues = lone[0].markers.map((m) => m.v);

  /* The band's lower edge is clamped at 0, never negative. */
  const low = [{ key: "waits", series: [{ points: [{ t: 1, v: 5 }] }] }];
  strips.applyBaselines(low, baseline(null, b(3, 2), null, null), window, {});
  out.clampedLower = low[0].band[0].lower;

  /* Not enough history for one lane: that strip has NO band and NO markers and says so; the others still draw. */
  const partial = strips.applyBaselines(fresh(), baseline(b(20, 5), null, b(1, 0.2), b(0.1, 0.3)), window, {});
  out.partial = summary(partial);

  /* A zero or empty-history bucket is no baseline either (never a band at 0). */
  const flat = strips.applyBaselines(fresh(), baseline({ mean: 0, effective_std_dev: 0, sample_count: 0 }, b(120, 0), null, null), window, {});
  out.flat = summary(flat);

  /* The read said "empty" (no history anywhere) or failed: no bands, a note each, and the lines are untouched. */
  out.emptyRead = summary(strips.applyBaselines(fresh(), { kind: "empty", message: "x" }, window, {}));
  out.failedRead = summary(strips.applyBaselines(fresh(), { kind: "error", message: "boom" }, window, {}));

  /* A database filter narrows the I/O and blocking reads, whose baseline is server-wide: neither is drawn against a filtered line. */
  out.filtered = summary(strips.applyBaselines(fresh(), baseline(b(20, 5), b(120, 10), b(1, 0.2), b(0.1, 0.3)), window, { databaseFiltered: true }));

  /* Drawn: the band, its dashed mean, one dot per marker, and the note for the lane without a baseline. */
  const drawn = strips.drawStrips(partial, domain, 1000, null);
  const g = (key) => find(drawn.node, (n) => n.attrs["data-strip"] === key)[0];
  const count = (key, cls) => find(g(key), (n) => n.attrs.class === cls).length;
  out.drawn = {
    cpu: { band: count("cpu", "strip-band"), mean: count("cpu", "strip-band-mean"), dots: count("cpu", "strip-anomaly") },
    waits: { band: count("waits", "strip-band"), dots: count("waits", "strip-anomaly"), note: find(g("waits"), (n) => n.attrs.class === "strip-baseline-note").map((n) => n.textContent) },
    blocking: { band: count("blocking", "strip-band"), dots: count("blocking", "strip-anomaly") },
    memory: { band: count("memory", "strip-band") },
  };
} else if (scenario === "localtime") {
  /* 12:00-13:00 UTC on 1 January is 07:00-08:00 in New York: the axis and the crosshair readout read that, as the zoom chip does. */
  const built = strips.buildStrips(reads);
  const drawn = strips.drawStrips(built, domain, 1000, null);
  out.axisLabels = find(drawn.node, (n) => n.tag === "text" && /^\d\d:\d\d/.test(n.textContent)).map((n) => n.textContent);
  drawn.crosshairAt(T0 + 22 * MIN);
  out.readout = readoutTexts(drawn.node).cpu;
} else if (scenario === "firstload") {
  /* A reload: the panel's host is built before it is in the page, so the first draw measures no width. Once it is attached and laid out
     (328 px, a phone) the observer's first report must redraw the strips at that width, not leave the 1000 px default scaled down. */
  const now = Date.now();
  const sample = (m) => new Date(now - m * MIN).toISOString().slice(0, 19);
  const bodies = {
    get_cpu_utilization: { samples: Array.from({ length: 30 }, (_, i) => ({ sample_time: sample(i * 2), sql_server_cpu: 10 + i, other_process_cpu: 5 })) },
    get_server_trend: { trend: Array.from({ length: 12 }, (_, i) => ({ time: sample(i * 5), wait_time_ms_per_second: 100 + i })) },
    get_blocking_trend: { trend: [{ time: sample(20), count: 2 }] },
    get_deadlock_trend: { trend: [] },
    get_memory_trend: { trend: Array.from({ length: 6 }, (_, i) => ({ time: sample(i * 10), buffer_pool_mb: 2000 + i })) },
    get_file_io_trend: { trend: Array.from({ length: 4 }, (_, i) => ({ time: sample(i * 15), database_name: "A", file_type: "ROWS", reads: 10, avg_read_latency_ms: 2 })) },
  };
  globalThis.fetch = async (url) => {
    const tool = String(url).split("/api/read/")[1].split("?")[0];
    return { status: 200, ok: true, text: async () => JSON.stringify(bodies[tool] || {}) };
  };
  const body = document.createElement("div");
  strips.fillOverviewStrips(body, "A", { hours: 1, label: "1h", signal: null });
  await new Promise((r) => setTimeout(r, 50));
  const svgs = () => find(body, (n) => n.tag === "svg");
  const sizes = () => svgs().map((n) => ({ width: n.attrs.width, viewBox: n.attrs.viewBox }));
  out.afterFill = sizes();
  observers.forEach((o) => o.cb([]));
  out.afterObserver = sizes();
} else if (scenario === "latebaseline") {
  /* #7644: the baseline read is slow (up to 23 s on a busy server). The six series reads answer at once; the strips must be drawn while the
     baseline is still pending, and the band appears when it arrives. A second fill where the baseline read fails keeps the lines. */
  const now = Date.now();
  const sample = (m) => new Date(now - m * MIN).toISOString().slice(0, 19);
  const bodies = {
    get_cpu_utilization: { samples: Array.from({ length: 30 }, (_, i) => ({ sample_time: sample(i * 2), sql_server_cpu: 10 + i, other_process_cpu: 5 })) },
    get_server_trend: { trend: Array.from({ length: 12 }, (_, i) => ({ time: sample(i * 5), wait_time_ms_per_second: 100 + i })) },
    get_blocking_trend: { trend: [{ time: sample(20), count: 2 }] },
    get_deadlock_trend: { trend: [] },
    get_memory_trend: { trend: Array.from({ length: 6 }, (_, i) => ({ time: sample(i * 10), buffer_pool_mb: 2000 + i })) },
    get_file_io_trend: { trend: Array.from({ length: 4 }, (_, i) => ({ time: sample(i * 15), database_name: "A", file_type: "ROWS", reads: 10, avg_read_latency_ms: 2 })) },
  };
  /* The page asks for the real last hour, so the step covers the current clock, not the harness's January window. */
  const nowStep = { from: new Date(now - 120 * MIN).toISOString(), to: new Date(now + 60 * MIN).toISOString(), mean: 20, effective_std_dev: 5, sample_count: 40, tier: "Full" };
  const baselineBody = { status: "ok", baselines: { cpu: { steps: [nowStep] } } };
  let releaseBaseline;
  const baselineGate = new Promise((r) => { releaseBaseline = r; });
  let failBaseline = false;
  globalThis.fetch = async (url) => {
    const tool = String(url).split("/api/read/")[1].split("?")[0];
    if (tool === "get_overview_baselines") {
      await baselineGate;
      if (failBaseline) return { status: 500, ok: false, text: async () => "boom" };
      return { status: 200, ok: true, text: async () => JSON.stringify(baselineBody) };
    }
    return { status: 200, ok: true, text: async () => JSON.stringify(bodies[tool] || {}) };
  };
  const body = document.createElement("div");
  strips.fillOverviewStrips(body, "A", { hours: 1, label: "1h", signal: null });
  await new Promise((r) => setTimeout(r, 50));
  const stats = (root) => ({
    lines: find(root, (n) => n.tag === "polyline").length,
    bands: find(root, (n) => n.attrs.class === "strip-band").length,
    notes: find(root, (n) => n.attrs.class === "strip-baseline-note").length,
  });
  out.whilePending = stats(body);
  releaseBaseline();
  await new Promise((r) => setTimeout(r, 50));
  out.afterBaseline = stats(body);
  /* The baseline read fails: the lines are drawn, with a note and no band. */
  failBaseline = true;
  const body2 = document.createElement("div");
  strips.fillOverviewStrips(body2, "A", { hours: 1, label: "1h", signal: null });
  await new Promise((r) => setTimeout(r, 50));
  out.afterFailure = stats(body2);
} else if (scenario === "waitsaxis") {
  /* the waits sample runs 100..220 ms/sec; the strip's lower axis label must be 0, not the series minimum */
  const drawn = strips.drawStrips(strips.buildStrips(reads), domain, 1000, null);
  const labels = (key) => find(find(drawn.node, (n) => n.attrs["data-strip"] === key)[0], (n) => n.tag === "text" && n.parent && n.parent.attrs.class === "axis").map((n) => n.textContent);
  out.waits = labels("waits");
  out.memory = labels("memory");
} else if (scenario === "iogaps") {
  /* #7572: 15 minute buckets with reads at minutes 0, 15 and 45 only. The strip must not run a line from minute 15 to minute 45, and the lone
     bucket at 45 is a dot. The bucket at 45 holds ONE read at 81 ms in the first run (no red dot: under the 250 read floor) and 300 reads in the second. */
  const ioRows = (reads45) => [
    { time: iso(0), database_name: "A", file_type: "ROWS", reads: 400, avg_read_latency_ms: 0.5 },
    { time: iso(15), database_name: "A", file_type: "ROWS", reads: 300, avg_read_latency_ms: 0.6 },
    { time: iso(30), database_name: "A", file_type: "ROWS", reads: 0, avg_read_latency_ms: null },
    { time: iso(45), database_name: "A", file_type: "ROWS", reads: reads45, avg_read_latency_ms: 81 },
  ];
  const window = { windowStart: T0, windowEnd: T0 + 60 * MIN };
  const baseline = { kind: "data", data: { baselines: { io: laneOf({ mean: 1, effective_std_dev: 0.5, sample_count: 40, tier: "Full" }) } } };
  const one = (reads45) => {
    const built = strips.applyBaselines(strips.buildStrips({ ...reads, io: data({ bucket_minutes: 15, trend: ioRows(reads45) }) }), baseline, window, {});
    const io = built.find((s) => s.key === "io");
    const drawn = strips.drawStrips(built, domain, 1000, null);
    const g = find(drawn.node, (n) => n.attrs["data-strip"] === "io")[0];
    return {
      markers: io.markers.length,
      lines: find(g, (n) => n.tag === "polyline").map((n) => n.attrs.points.split(" ").length),
      dots: find(g, (n) => n.tag === "circle" && n.attrs.class !== "strip-anomaly").length,
      anomalyDots: find(g, (n) => n.attrs.class === "strip-anomaly").length,
    };
  };
  out.oneRead = one(1);
  out.manyReads = one(300);
  /* A collector scheduled every 5 minutes on a chart of 1 minute buckets (the Chart budget for a day): six rows 5 minutes apart. Every step is longer
     than the bucket, but it is the rows' own cadence, so the strip stays one line. The last row holds `last` reads at 81 ms: the floor is per bucket
     width, 250 per 15 minutes, so a 1 minute bucket needs 17 reads and a 30 minute bucket needs 500. */
  const slow = (bucketMinutes, last, stepMinutes = 5) => {
    const rows = Array.from({ length: 6 }, (_, i) => ({ time: iso(i * stepMinutes), database_name: "A", file_type: "ROWS", reads: i === 5 ? last : 1000, avg_read_latency_ms: i === 5 ? 81 : 0.5 }));
    const built = strips.applyBaselines(strips.buildStrips({ ...reads, io: data({ bucket_minutes: bucketMinutes, trend: rows }) }), baseline, { windowStart: T0, windowEnd: T0 + 60 * MIN }, {});
    const g = find(strips.drawStrips(built, domain, 1000, null).node, (n) => n.attrs["data-strip"] === "io")[0];
    return {
      lines: find(g, (n) => n.tag === "polyline").map((n) => n.attrs.points.split(" ").length),
      dots: find(g, (n) => n.tag === "circle" && n.attrs.class !== "strip-anomaly").length,
      markers: built.find((s) => s.key === "io").markers.length,
    };
  };
  out.slowCollector = slow(1, 1000);
  /* The same 5 minute collector: a row holds 5 minutes of reads, so the floor is 84 (250 per 15 minutes over 5), not the 17 of one 1 minute bucket. */
  out.floorSlowCollector = { short: slow(1, 83).markers, enough: slow(1, 84).markers };
  out.floorOneMinute = { enough: slow(1, 17, 1).markers, short: slow(1, 16, 1).markers };
  out.floorThirtyMinute = { short: slow(30, 499, 30).markers, enough: slow(30, 500, 30).markers };
  out.points = strips.weightedReadLatency(ioRows(1)).map((p) => p.reads);
} else if (scenario === "steps") {
  /* #7644: a window across two hour buckets. Hour A (T0 - 30 min to T0 + 30 min) is a band around 10 (mean 10, sigma 1: 8 to 12), hour B
     (T0 + 30 min to T0 + 90 min) is a band around 50 (48 to 52). A third hour (after T0 + 90 min) has no step. */
  const hour = (fromMin, toMin, mean) => ({ from: new Date(T0 + fromMin * MIN).toISOString(), to: new Date(T0 + toMin * MIN).toISOString(), mean, effective_std_dev: 1, sample_count: 40, tier: "Full" });
  const window = { windowStart: T0, windowEnd: T0 + 120 * MIN };
  const res = { kind: "data", data: { baselines: { cpu: { steps: [hour(-30, 30, 10), hour(30, 90, 50)] } } } };
  const pt = (m, v) => ({ t: T0 + m * MIN, v });
  /* 11 at +0 is inside hour A's band; 49 at +10 is above it (marked); 11 at +40 is under hour B's band (marked); 49 at +50 is inside hour B's band;
     100 at +100 and 0 at +110 are in the hour with no step, so nothing judges them. */
  const line = [pt(0, 11), pt(10, 49), pt(40, 11), pt(50, 49), pt(100, 100), pt(110, 0)];
  const cpu = [{ key: "cpu", series: [{ points: line }] }];
  strips.applyBaselines(cpu, res, window, {});
  out.steps = cpu[0].band.map((x) => ({ from: (x.from - T0) / MIN, to: (x.to - T0) / MIN, lower: x.lower, upper: x.upper }));
  out.markedTimes = cpu[0].markers.map((m) => (m.t - T0) / MIN);
  /* An hour whose step is unusable (no samples, or no spread) is dropped, so its points mark nothing. */
  const half = { kind: "data", data: { baselines: { cpu: { steps: [hour(-30, 30, 10), { ...hour(30, 90, 50), sample_count: 0 }] } } } };
  const cpu2 = [{ key: "cpu", series: [{ points: line }] }];
  strips.applyBaselines(cpu2, half, window, {});
  out.droppedStepBands = cpu2[0].band.length;
  out.droppedStepMarked = cpu2[0].markers.map((m) => (m.t - T0) / MIN);
} else if (scenario === "linuxcpu") {
  /* #7682: SQL Server on Linux before 2025 CU1 cannot report host CPU: other_process_cpu is null on every sample. The CPU strip draws the SQL line
     alone and says so; a Total line over a null Other would sit on the SQL line and claim the host was measured. */
  const linux = { ...reads, cpu: data({ samples: Array.from({ length: 61 }, (_, i) => ({ sample_time: iso(i), sql_server_cpu: 10 + i, other_process_cpu: null, total_cpu: null })) }) };
  const built = strips.buildStrips(linux);
  const drawn = strips.drawStrips(built, domain, 1000, null);
  const g = find(drawn.node, (n) => n.attrs["data-strip"] === "cpu")[0];
  out.series = built.find((s) => s.key === "cpu").series.map((s) => s.label);
  out.lines = find(g, (n) => n.tag === "polyline").length;
  out.notes = find(g, (n) => n.attrs.class === "strip-baseline-note").map((n) => n.textContent);
  /* #7682: the strip's texts each start at their own point. The host note once sat at the title's x and y, so the two drew over each other. */
  const starts = find(g, (n) => n.tag === "text" && n.textContent).map((n) => n.attrs.x + "," + n.attrs.y);
  out.sharedTextStarts = starts.length - new Set(starts).size;
  out.measuredSeries = strips.buildStrips(reads).find((s) => s.key === "cpu").series.map((s) => s.label);
  out.measuredNotes = find(find(strips.drawStrips(strips.buildStrips(reads), domain, 1000, null).node, (n) => n.attrs["data-strip"] === "cpu")[0], (n) => n.attrs.class === "strip-baseline-note").length;
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
