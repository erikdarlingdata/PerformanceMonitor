/* #5246: runs the shipped deadlock graph drawing (wwwroot/js/pages/deadlock-graph.js with util.js) on a small fake DOM and
   prints what a scenario did as one line of JSON. WebDeadlockGraphBehaviourTests starts it as
       node web-deadlock-graph-harness.mjs <path to the js folder> <scenario>
   Only the DOM is a stand-in. The graph objects are shaped as DarlingWebDeadlockGraph.BuildGraph sends them. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.style = {}; this.open = false; this.dataset = {}; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); if (k === "open") this.open = true; }
  getAttribute(k) { return k === "class" ? this.className : (k in this.attrs ? this.attrs[k] : null); }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, ev = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, target: this, ...ev }); }
  all(pred, acc = []) { if (pred(this)) acc.push(this); for (const c of this.children) c.all && c.all(pred, acc); return acc; }
  tags(tag) { return this.all((n) => n.tag === tag); }
  hasClass(c) { return (" " + this.className + " ").includes(" " + c + " "); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createElementNS: (ns, t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };

const root = process.argv[2];
const mod = await import(pathToFileURL(root + "/pages/deadlock-graph.js").href);
const out = {};

const proc = (id, spid, x, y, extra = {}) => ({ id, spid, ecid: 0, victim: false, x, y, wait_time_ms: 120, priority: 0, lock_mode: "X", contended_object: "AppDb.dbo.t", sql_text: "update dbo.t set a = 1", ...extra });
const graph = () => ({
  width: 700, height: 300, node_width: 240, node_height: 150, is_parallel: false,
  processes: [proc("p1", 51, 40, 40, { victim: true, sql_text: "select 1 -- <script>alert(1)</script>" }), proc("p2", 52, 420, 40, { sql_text_cut: true })],
  edges: [
    { waiter: "p1", owner: "p2", self: false, resource_kind: "keylock", resource_label: "KEY AppDb.dbo.t", request_mode: "X", owner_mode: "U" },
    { waiter: "p2", owner: "p1", self: false, resource_kind: "keylock", resource_label: "KEY AppDb.dbo.t", request_mode: "X", owner_mode: "X" },
  ],
  cycles: [{ index: 1, node_count: 2, x: 20, y: 20, width: 660, height: 260 }],
});
const row = (extra = {}) => ({ dedup_key: "k1", deadlock_time: "2026-01-01T00:00:00Z", graph: graph(), ...extra });
const open = (cell) => { cell.open = true; cell.fire("toggle"); };

switch (process.argv[3]) {
  case "lazy": {
    const cell = mod.deadlockGraphCell("srv", row());
    out.tag = cell.tag;
    out.summary = cell.tags("summary")[0].textContent;
    out.svgBefore = cell.tags("svg").length;
    open(cell);
    out.svgAfter = cell.tags("svg").length;
    cell.open = false; cell.fire("toggle"); open(cell);
    out.svgAfterReopen = cell.tags("svg").length;
    break;
  }
  case "draws": {
    const cell = mod.deadlockGraphCell("srv", row());
    open(cell);
    out.nodes = cell.all((n) => n.hasClass("dlg-node")).length;
    out.victims = cell.all((n) => n.hasClass("dlg-node") && n.hasClass("victim")).length;
    out.paths = cell.all((n) => n.hasClass("dlg-edge")).length;
    out.markerEnds = cell.all((n) => n.attrs["marker-end"]).length;
    out.cycleFrames = cell.all((n) => n.hasClass("dlg-cycle")).length;
    out.labels = cell.all((n) => n.hasClass("dlg-edge-label")).map((n) => n.children[0] ? n._text : n._text);
    out.viewBox = cell.tags("svg")[0].attrs.viewBox;
    out.titles = cell.tags("title").map((n) => n.textContent);
    out.selectedAtOpen = cell.all((n) => n.hasClass("selected")).map((n) => n.children.find((c) => c.hasClass("dlg-spid")).textContent);
    out.head = cell.all((n) => n.hasClass("dlg-side-head"))[0].textContent;
    break;
  }
  case "click": {
    const cell = mod.deadlockGraphCell("srv", row());
    open(cell);
    const nodes = cell.all((n) => n.hasClass("dlg-node"));
    nodes[1].fire("click");
    out.head = cell.all((n) => n.hasClass("dlg-side-head"))[0].textContent;
    out.pre = cell.tags("pre")[0].textContent;
    out.cutNote = cell.all((n) => n.hasClass("dlg-prop") && n.textContent.includes("cut here")).length;
    out.selected = cell.all((n) => n.hasClass("selected")).length;
    nodes[0].fire("keydown", { key: "Enter" });
    out.headAfterEnter = cell.all((n) => n.hasClass("dlg-side-head"))[0].textContent;
    break;
  }
  case "hostile": {
    const g = graph();
    g.processes[0].sql_text = '<script>alert(1)</script><img src=x onerror=alert(1)>';
    g.processes[0].proc_name = "<b>proc</b>";
    const cell = mod.deadlockGraphCell("srv", row({ graph: g }));
    open(cell);
    out.scripts = cell.all((n) => n.tag === "script" || n.tag === "img" || n.tag === "b").length;
    out.pre = cell.tags("pre")[0].textContent;
    break;
  }
  case "twoCycles": {
    const g = graph();
    g.cycles = [{ index: 1, node_count: 2, x: 20, y: 20, width: 330, height: 260 }, { index: 2, node_count: 2, x: 380, y: 20, width: 300, height: 260 }];
    const cell = mod.deadlockGraphCell("srv", row({ graph: g }));
    open(cell);
    out.cycleFrames = cell.all((n) => n.hasClass("dlg-cycle")).length;
    out.cycleLabels = cell.all((n) => n.hasClass("dlg-cycle-label")).map((n) => n._text);
    out.summary = cell.tags("summary")[0].textContent;
    break;
  }
  case "selfLoop": {
    const g = {
      width: 400, height: 300, node_width: 240, node_height: 150, is_parallel: true,
      processes: [proc("p1", 60, 80, 80, { victim: true })],
      edges: [{ waiter: "p1", owner: "p1", self: true, resource_kind: "exchangeEvent", resource_label: "exchangeEvent", request_mode: "", owner_mode: "" }],
      cycles: [{ index: 1, node_count: 1, x: 20, y: 20, width: 360, height: 260 }],
    };
    const cell = mod.deadlockGraphCell("srv", row({ graph: g }));
    open(cell);
    const path = cell.all((n) => n.hasClass("dlg-edge"))[0];
    out.paths = cell.all((n) => n.hasClass("dlg-edge")).length;
    out.curve = path.attrs.d.includes(" C ");
    out.cycleFrames = cell.all((n) => n.hasClass("dlg-cycle")).length;
    out.summary = cell.tags("summary")[0].textContent;
    break;
  }
  case "tooLarge": {
    const cell = mod.deadlockGraphCell("srv", { graph: { too_large: true, process_count: 75 } });
    out.tag = cell.tag;
    out.text = cell.textContent;
    break;
  }
  case "truncatedXml": {
    out.preview = mod.deadlockGraphCell("srv", { deadlock_graph_xml_truncated: true }).textContent;
    out.none = mod.deadlockGraphCell("srv", {}).textContent;
    break;
  }
  case "openSurvivesRebuild": {
    const first = mod.deadlockGraphCell("srv", row());
    open(first);
    const rebuilt = mod.deadlockGraphCell("srv", row());
    out.reopened = rebuilt.open;
    out.svg = rebuilt.tags("svg").length;
    const other = mod.deadlockGraphCell("other", row());
    out.otherServerOpen = other.open;
    out.otherServerSvg = other.tags("svg").length;
    break;
  }
  default:
    throw new Error("unknown scenario " + process.argv[3]);
}
console.log(JSON.stringify(out));
