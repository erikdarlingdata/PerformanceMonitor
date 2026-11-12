/* Runs the web viewer's shipped composed-panel card (wwwroot/js/compose.js and charts.js) against a stand-in DOM and a
   scripted /api/compose/run answer, and prints what each run sent and drew as one line of JSON (#7923).
   WebComposedViewHoldBehaviourTests starts it as
       node web-composed-view-hold-harness.mjs <path to wwwroot/js>
   A saved dashboard's poll builds each panel again (a new card for the same slot). The harness brushes a zoom and drills on
   a card, builds the same panel again, and reports the body of that second run and the chips it drew. */
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
  dispatch(type, ev) {
    (this.listeners[type] || []).forEach((l) => l(ev || {}));
  }
  bubble(type, ev) {
    const e = { preventDefault() {}, stopPropagation() { this.stopped = true; }, ...(ev || {}) };
    for (let n = this; n && !e.stopped; n = n.parent) (n.listeners[type] || []).forEach((l) => l(e));
  }
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
globalThis.location = { hash: "#/views/saved" };

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "composed-hold-")));
let compose;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  await import(pathToFileURL(path.join(scratch, "grid-tools.js")).href);
  fs.writeFileSync(path.join(scratch, "panels.js"), "export function navigateServer() {}\nexport function gridTable() { return document.createElement(\"div\"); }\n");
  fs.writeFileSync(path.join(scratch, "views-api.js"), "export async function getCatalog() { return { compose: {} }; }\n");
  compose = await import(pathToFileURL(path.join(scratch, "compose.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const cls = (n) => String(n.className || n.attrs.class || "");
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const bodies = [];
globalThis.fetch = async (url, init) => {
  bodies.push(init && init.body ? JSON.parse(init.body) : {});
  return {
    status: 200,
    ok: true,
    text: async () => JSON.stringify({
      sql: "select 1",
      rows: Array.from({ length: 11 }, (_, i) => [["AAA", 1000 + i * 100], ["BBB", i % 5]].map(([w, v]) => ({ bucket: new Date(T0 + i * MIN).toISOString().slice(0, 19), wait: w, value: v }))).flat(),
    }),
  };
};
const settle = async () => {
  for (let i = 0; i < 30; i++) await new Promise((r) => setImmediate(r));
};

const panel = (title) => ({ source: "waits", viz: "line", measure: "wait_ms", title, groupBy: ["wait"] });
const SCOPE = { server: "A", hours: 4 };
const draw = async (title, slot, scope = SCOPE) => {
  const card = compose.renderComposedPanelCard(panel(title), scope, null, slot);
  await settle();
  return card;
};
const last = () => bodies[bodies.length - 1];
const report = (card) => ({
  windowStart: last().windowStart ?? null,
  windowEnd: last().windowEnd ?? null,
  filters: ((last().panel && last().panel.filters) || []).map((f) => f.dimension + "=" + f.value),
  zoomChip: find(card, (n) => cls(n).includes("zoom-chip")).length,
  drillChip: find(card, (n) => cls(n).includes("drill-chip") && !cls(n).includes("zoom-chip")).length,
});
const brush = async (card) => {
  const overlay = find(card, (n) => (n.listeners.pointerdown || []).length)[0];
  overlay.listeners.pointerdown[0]({ button: 0, clientX: 300, pointerId: 1 });
  overlay.listeners.pointerup[0]({ clientX: 700 });
  await settle();
};
const drillOn = async (card, label) => {
  const item = find(card, (n) => cls(n).includes("drillable") && n.tag === "span" && n.children[1] && n.children[1].textContent === label)[0];
  item.children[1].bubble("click");
  await settle();
};
const clearChip = async (card, which) => {
  const chip = find(card, (n) => cls(n).includes(which))[0];
  if (!chip) return; /* nothing to clear: the report shows the chip missing */
  find(chip, (n) => cls(n).includes("drill-clear"))[0].dispatch("click", {});
  await settle();
};

const out = {};

// 1. a brush-zoom survives the poll's build of the same panel
let card = await draw("Waits one", 0);
out.initial = report(card);
await brush(card);
out.zoomed = report(card);
card = await draw("Waits one", 0);
out.zoomRebuilt = report(card);

// 2. a drill survives it too, and the held zoom stays beside it
await drillOn(card, "AAA");
out.drilled = report(card);
card = await draw("Waits one", 0);
out.drillRebuilt = report(card);

// 3. the reader's own clear of the zoom, then of the drill, is held: the next build draws neither
await clearChip(card, "zoom-chip");
out.zoomCleared = report(card);
card = await draw("Waits one", 0);
out.zoomClearedRebuilt = report(card);
await clearChip(card, "drill-chip");
card = await draw("Waits one", 0);
out.allClearedRebuilt = report(card);

// 4. the hold reaches neither another panel, another server, another page nor another range
card = await draw("Waits one", 0);
await brush(card);
await drillOn(card, "AAA");
out.otherPanel = report(await draw("Waits two", 1));
out.otherServer = report(await draw("Waits one", 0, { server: "B", hours: 4 }));
globalThis.location.hash = "#/views/other";
out.otherPage = report(await draw("Waits one", 0));
globalThis.location.hash = "#/views/saved";
card = await draw("Waits one", 0);
await brush(card);
await drillOn(card, "AAA");
out.otherRange = report(await draw("Waits one", 0, { server: "A", hours: 24 }));

// 5. a panel with no slot (the editor preview) holds nothing
card = compose.renderComposedPanelCard(panel("Preview"), SCOPE, null, null);
await settle();
await brush(card);
out.previewZoomed = report(card);
card = compose.renderComposedPanelCard(panel("Preview"), SCOPE, null, null);
await settle();
out.previewRebuilt = report(card);

// 6. a view variable edit drops the hold, the same values (in any key order) keep it
const withValues = (values) => ({ server: "A", hours: 4, values });
card = await draw("Waits vars", 5, withValues({ a: "1", b: "2" }));
await brush(card);
await drillOn(card, "AAA");
out.varsDrilled = report(card);
out.varsSame = report(await draw("Waits vars", 5, withValues({ a: "1", b: "2" })));
out.varsReordered = report(await draw("Waits vars", 5, withValues({ b: "2", a: "1" })));
out.varsChanged = report(await draw("Waits vars", 5, withValues({ a: "1", b: "3" })));
out.varsChangedBack = report(await draw("Waits vars", 5, withValues({ a: "1", b: "2" })));

fs.rmSync(scratch, { recursive: true, force: true });
console.log(JSON.stringify(out));
process.exit(0);
