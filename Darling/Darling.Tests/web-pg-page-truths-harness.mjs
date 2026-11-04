/* Runs the shipped PostgreSQL Waits, Activity and I/O tabs (wwwroot/js/pages/server-tabs.js) against scripted answers and prints, as
   one line of JSON, what the page drew (#7409): whether the Wait Events, Sampled Waits and Wait Trend panels are hidden when the
   read answers "not collected", the two tab banners, and the stat tiles of the I/O Summary and Blocking Sampling panels (text and
   whether the tile's value is drawn in the small size).
       node web-pg-page-truths-harness.mjs <path to wwwroot/js> */
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
    this.hidden = false;
    this.parentNode = null;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    child.parentNode = null;
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
    if (name === "class") this.className = String(value);
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

/* One scripted answer per read name. A scenario replaces the table before it builds a tab. */
let answers = {};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.split("/").pop();
  const body = answers[name] || {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const NOT_COLLECTED = { status: "not_collected", message: "This read does not apply to this kind of server." };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pg-page-truths-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
  const find = (node, pred) => (pred(node) ? node : node.children.map((c) => find(c, pred)).find(Boolean) || null);
  const collect = (node, pred, out = []) => {
    if (pred(node)) out.push(node);
    node.children.forEach((c) => collect(c, pred, out));
    return out;
  };

  const tabOf = (id) => tabs.POSTGRES_TABS.find((t) => t.id === id);
  const draw = async (id, scripted) => {
    answers = scripted;
    const panels = tabOf(id).build("srv-a", { hours: 24, label: "24h" }).flat();
    await new Promise((r) => setTimeout(r, 150));
    return panels.filter((p) => p && p.tag);
  };
  const panelTitled = (panels, title) => panels.find((p) => find(p, (n) => n.tag === "h3" && n.textContent.startsWith(title)));
  const hiddenState = (panel) => (panel ? panel.hidden === true || panel.style.display === "none" : null);
  const tiles = (panel) => {
    if (!panel) return null;
    const out = {};
    for (const stat of collect(panel, (n) => n.className === "stat")) {
      const [value, label] = stat.children;
      out[label.textContent] = { text: value.textContent, small: value.className.split(" ").includes("small") };
    }
    return out;
  };

  /* Waits tab on a target whose reads answer "not collected" (Amazon Aurora for the two sampled panels, a stock server for Wait Events). */
  const waitsGone = await draw("waits", {
    get_pg_wait_stats: NOT_COLLECTED,
    get_pg_wait_sampling: NOT_COLLECTED,
    get_pg_wait_trend: NOT_COLLECTED,
  });
  /* The same tab where every read has rows: all three stay drawn. */
  const waitsFull = await draw("waits", {
    get_pg_wait_stats: { waits: [{ event_type: "IO", wait_event: "DataFileRead", samples: 3 }] },
    get_pg_wait_sampling: { waits: [{ event_type: "IO", wait_event: "DataFileRead", queryid: null, samples: 3, backends: 1 }] },
    get_pg_wait_trend: { points: [{ collection_time: "2026-11-01T00:00:00Z", estimated_wait_ms_per_second: 4 }, { collection_time: "2026-11-01T00:05:00Z", estimated_wait_ms_per_second: 6 }] },
  });
  const titles = ["Wait Events", "Sampled Waits", "Wait Trend"];

  /* I/O Summary: nothing read (null Busiest), and a window ranked by read count. */
  const ioNothing = await draw("io", {
    get_pg_io_stats: { combinations_returned: 3, total_reads: 0, total_read_time_ms: 0, busiest_by_read_time: null, busiest_rank_note: null, write_counters_tracked_anywhere: true, combinations: [] },
  });
  const ioByReads = await draw("io", {
    get_pg_io_stats: { combinations_returned: 3, total_reads: 90, total_read_time_ms: 0, busiest_by_read_time: "client backend/relation/normal", busiest_rank_note: "(by reads)", write_counters_tracked_anywhere: true, combinations: [] },
  });
  const ioByTime = await draw("io", {
    get_pg_io_stats: { combinations_returned: 3, total_reads: 90, total_read_time_ms: 12, busiest_by_read_time: "client backend/relation/normal", busiest_rank_note: null, write_counters_tracked_anywhere: true, combinations: [] },
  });

  /* Blocking Sampling: captures and nothing blocked (zeros sent), and no captures at all (nothing sent but the count). */
  const blockedNone = await draw("activity", {
    get_pg_blocking: { captures_total: 1437, captures_with_blocking: 0, pct_of_captures_with_blocking: 0, worst_chain_victims: 0, cycles_sampled: 0, finding: "No blocking was present in any of the 1437 captures.", chains: [], cycles: [] },
  });
  const blockedUnavailable = await draw("activity", {
    get_pg_blocking: { captures_total: 0, captures_with_blocking: 0, finding: "No captures yet.", chains: [], cycles: [] },
  });
  const blockedWithChain = await draw("activity", {
    get_pg_blocking: { captures_total: 40, captures_with_blocking: 3, pct_of_captures_with_blocking: 7.5, worst_chain_victims: 2, worst_chain_root_state: "idle in transaction", cycles_sampled: 0, chains: [], cycles: [] },
  });

  console.log(JSON.stringify({
    notCollected: Object.fromEntries(titles.map((t) => [t, hiddenState(panelTitled(waitsGone, t))])),
    withRows: Object.fromEntries(titles.map((t) => [t, hiddenState(panelTitled(waitsFull, t))])),
    wait: { note: tabOf("waits").note, noteInfo: tabOf("waits").noteInfo },
    activity: { note: tabOf("activity").note, noteInfo: tabOf("activity").noteInfo },
    ioNothing: tiles(panelTitled(ioNothing, "I/O Summary")),
    ioByReads: tiles(panelTitled(ioByReads, "I/O Summary")),
    ioByTime: tiles(panelTitled(ioByTime, "I/O Summary")),
    blockedNone: tiles(panelTitled(blockedNone, "Blocking Sampling")),
    blockedUnavailable: tiles(panelTitled(blockedUnavailable, "Blocking Sampling")),
    blockedWithChain: tiles(panelTitled(blockedWithChain, "Blocking Sampling")),
  }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
