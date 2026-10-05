/* Runs the web viewer's composed-panel body (wwwroot/js/compose.js and util.js) against a scripted
   /api/compose/run answer and prints the strips it drew as one line of JSON. ComposeDataFloorLiveTests starts it as
       node web-compose-notice-harness.mjs <path to wwwroot/js> <path to a scenario JSON file>
   The scenario file holds {panel, scope, answer}: the panel spec and scope the page would send, and the body the run
   endpoint returned for them. The modules are copied into a scratch folder beside stand-ins for charts.js (the SVG
   renderer, which needs a real browser), panels.js and views-api.js (reached only by clicks and annotation names),
   then imported. `fetch` and the DOM are stand-ins: a node tree of plain objects, and a fetch that answers the run
   with the scenario's body. Everything else is the shipped code. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenarioPath] = process.argv.slice(-2);
const scenario = JSON.parse(fs.readFileSync(scenarioPath, "utf8"));

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
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

const fetches = [];
globalThis.fetch = async (url, init) => {
  fetches.push((init && init.method ? init.method : "GET") + " " + String(url));
  const isRun = String(url) === "/api/compose/run";
  const raw = JSON.stringify(isRun ? scenario.answer : {});
  return { status: 200, ok: true, text: async () => raw };
};

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "compose-notice-"));
let compose;
try {
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.copyFileSync(path.join(jsDir, "util.js"), path.join(scratch, "util.js"));
  fs.copyFileSync(path.join(jsDir, "compose.js"), path.join(scratch, "compose.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\n' +
      "export const CATEGORICAL_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "const stub = () => el('div', { class: 'chart-stub' });\n" +
      "export const renderLineChart = stub;\n" +
      "export const zoomChip = stub;\n" +
      "export const renderBarChart = stub;\n" +
      "export const renderPieChart = stub;\n" +
      "export const renderScatterChart = stub;\n" +
      /* The legend-isolate state compose.js reads and writes (#5247). The notice tests never click a legend, so nothing is hidden. */
      "export const getChartHidden = () => [];\n" +
      "export const setChartHidden = () => {};\n" +
      "export const nextHiddenKeys = () => [];\n" +
      "export const chartZoomScope = (hours) => '|' + String(hours);\n"
  );
  fs.writeFileSync(path.join(scratch, "panels.js"), "export function navigateServer() {}\nexport function gridTable() { return document.createElement(\"div\"); }\n");
  fs.writeFileSync(path.join(scratch, "views-api.js"), "export async function getCatalog() { return { compose: {} }; }\n");
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  compose = await load("compose.js");
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const root = new FakeNode("main");
await compose.renderComposedInto(root, scenario.panel, scenario.scope);
// Let the run and the mount settle (each is a few promise hops).
for (let i = 0; i < 50; i++) await new Promise((r) => setTimeout(r, 0));

const strips = (kind) => {
  const found = [];
  const walk = (n) => {
    if (!n || typeof n !== "object") return;
    if (n.className === "strip " + kind) found.push(n.textContent);
    n.children.forEach(walk);
  };
  walk(root);
  return found;
};

console.log(JSON.stringify({
  fetches,
  notices: strips("notice"),
  errors: strips("error"),
  rejections,
}));
