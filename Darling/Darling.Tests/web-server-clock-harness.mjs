/* #7243: loads the shipped server-tabs/sql-config.js with stand-ins for the DOM and prints, as one line of JSON, what the Agent Alerts
   "Last fired (server clock)" cell shows for a server-clock stamp, and what the column sorts by.
   WebServerClockColumnBehaviourTests starts it as: node web-server-clock-harness.mjs <path to wwwroot/js>
   Run it under TZ=America/New_York: a stamp wrongly read as UTC would then show five hours early. */
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];
class FakeNode {
  constructor(tag) { this.tag = tag; this.style = {}; this.children = []; this.attrs = {}; this.listeners = {}; this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false }; this._text = ""; }
  appendChild(c) { this.children.push(c); return c; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  addEventListener() {}
}
globalThis.Node = FakeNode;
globalThis.document = { body: new FakeNode("body"), createElement: (t) => new FakeNode(t), createElementNS: (ns, t) => new FakeNode(t), createTextNode: () => new FakeNode("#text") };
globalThis.location = { hash: "" };

const load = (rel) => import(pathToFileURL(path.join(jsDir, rel)).href);
const { AGENT_ALERT_SPECS } = await load("pages/server-tabs/sql-config.js");
const alerts = AGENT_ALERT_SPECS.find((s) => s.rowsKey === "alerts");
const col = alerts.columns.find((c) => c.key === "last_occurrence");
const { applyFormat } = await load("util.js");
/* What panels.js draws for a plain column: the column's display function, else its format, else the raw value. */
const shownFor = (row) => (typeof col.display === "function" ? String(col.display(row) ?? "—") : col.format ? applyFormat(col.format, row.last_occurrence) : String(row.last_occurrence));
const sortFor = (row) => (typeof col.sortValue === "function" ? col.sortValue(row) : row.last_occurrence);
const early = { last_occurrence: "2025-12-07T13:32:48" };
const late = { last_occurrence: "2025-12-07T13:32:50" };
console.log(JSON.stringify({
  shown: shownFor(early),
  empty: shownFor({ last_occurrence: null }),
  sortOrdered: sortFor(early) < sortFor(late),
}));
