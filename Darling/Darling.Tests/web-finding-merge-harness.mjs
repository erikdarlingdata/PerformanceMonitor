/* #7582: runs the web viewer's Analysis Findings grid (shared.js findingsPanel over panels.js) on a small fake DOM with a stub fetch,
   and prints what a person reads on each row as one line of JSON.
       node web-finding-merge-harness.mjs <path to the js folder> */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = "";
    this.dataset = {}; this.style = {}; this.open = false;
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a" };

let body = "{}";
globalThis.fetch = async () => ({ status: 200, ok: true, text: async () => body });

const root = process.argv[2];
const { findingsPanel } = await import(pathToFileURL(root + "/pages/server-tabs/shared.js").href);
const { localTime } = await import(pathToFileURL(root + "/util.js").href);

const finding = (over) => ({
  category: "waits", story_path: "X", severity: 0.2, peak_severity: 0.2, occurrences: 1, incident_id: "inc-1",
  first_seen: "2026-11-01T00:00:00Z", last_seen: "2026-11-01T01:00:00Z", time_range: { start: "2026-11-01T00:00:00Z", end: "2026-11-01T01:00:00Z" },
  advice: { headline: "Head" }, ...over,
});
const LATCH = "Exclusive latch contention on a non-buffer resource";
const scenarios = {
  /* The three rows of the issue: one sentence, Info / Info / Warning, three chains from one root. */
  sameSentence: () => [
    finding({ story_path: "LATCH_EX", severity: 0.3, peak_severity: 0.3, occurrences: 6, first_seen: "2026-11-01T10:03:00Z", last_seen: "2026-11-01T12:11:00Z", advice: { headline: LATCH } }),
    finding({ story_path: "LATCH_EX → TEMPDB_USAGE", severity: 0.4, peak_severity: 0.4, occurrences: 8, first_seen: "2026-10-31T18:24:00Z", last_seen: "2026-11-01T07:18:00Z", advice: { headline: LATCH } }),
    finding({ story_path: "LATCH_EX → CXPACKET", severity: 0.9, peak_severity: 0.9, occurrences: 19, first_seen: "2026-10-31T21:54:00Z", last_seen: "2026-11-01T06:48:00Z", advice: { headline: LATCH } }),
  ],
  /* The newest occurrence scored 0.62 (Info) and the peak was 1.3 (Warning): the row reads Warning. */
  peak: () => [finding({ severity: 0.62, peak_severity: 1.3, occurrences: 5, advice: { headline: "The server's wait profile shifted to about 2.9x" } })],
  /* Two sentences stay two rows. */
  twoSentences: () => [finding({ advice: { headline: "Sentence one" } }), finding({ advice: { headline: "Sentence two" }, severity: 1.6, peak_severity: 1.6 })],
  /* Same sentence, same chain, two incidents: one row, Critical, no chain list to open. */
  sameChain: () => [
    finding({ story_path: "ANOMALY_WRITE_LATENCY", severity: 1, peak_severity: 1, occurrences: 17, incident_id: "a", advice: { headline: "Write latency spiked to 93.3 ms" } }),
    finding({ story_path: "ANOMALY_WRITE_LATENCY", severity: 1.7, peak_severity: 1.7, occurrences: 4, incident_id: "b", advice: { headline: "Write latency spiked to 93.3 ms" } }),
  ],
};

const name = process.argv[3];
body = JSON.stringify({ findings: scenarios[name]() });
const panel = findingsPanel("srv", { hours: 24, label: "last 24 hours" }, "none");
await new Promise((r) => setTimeout(r, 50));
const heads = panel.findAll((n) => n.tag === "th").map((n) => n.textContent.trim().replace(/[▲▼]/g, "").trim());
const bodyRows = panel.findAll((n) => n.tag === "tr").filter((n) => n.parent && n.parent.tag === "tbody");
const at = (label) => heads.findIndex((h) => h.startsWith(label));
const rows = bodyRows.map((r) => {
  const cell = (label) => r.children[at(label)];
  const finding = cell("Finding");
  const details = finding.findAll((n) => n.tag === "details");
  return {
    finding: details.length ? details[0].children[0].textContent : finding.textContent,
    severity: cell("Severity").textContent,
    occurrences: cell("Occurrences").textContent,
    firstSeen: cell("First seen").textContent,
    lastSeen: cell("Last seen").textContent,
    chains: details.length ? details[0].children[1].children.map((c) => c.textContent) : [],
  };
});
console.log(JSON.stringify({
  rows,
  expected: {
    first: localTime("2026-10-31T18:24:00Z"), last: localTime("2026-11-01T12:11:00Z"),
    sameChainFirst: localTime("2026-11-01T00:00:00Z"), sameChainLast: localTime("2026-11-01T01:00:00Z"),
  },
}));
