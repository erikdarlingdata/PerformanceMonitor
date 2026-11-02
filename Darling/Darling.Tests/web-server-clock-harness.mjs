/* #7243: loads the shipped server-tabs/sql-config.js with stand-ins for the DOM and prints, as one line of JSON, what the Agent Alerts
   "Last fired (server clock)" cell shows for a server-clock stamp, and the order the column sorts a morning and an afternoon stamp of one day into.
   WebServerClockColumnBehaviourTests starts it as: node web-server-clock-harness.mjs <path to wwwroot/js>
   Run it under TZ=America/New_York: a stamp wrongly read as UTC would then show five hours early. The expected texts are built here from
   the same fields in the same locale, so the check holds on a machine set to any locale. */
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
const { applyFormat, getPath } = await load("util.js");
/* What panels.js draws for a plain column: the column's display function, else its format, else the raw value. */
const shownFor = (row) => (typeof col.display === "function" ? String(col.display(row) ?? "—") : col.format ? applyFormat(col.format, row.last_occurrence) : String(row.last_occurrence));
/* What panels.js sorts a column by (columnValue), then orders a text column by its lower-cased value (sortKeyOf): through the column's
   sort value, never the displayed text. */
const columnValue = (row) => (typeof col.sortValue === "function" ? col.sortValue(row) : getPath(row, col.key));
const sortKey = (row) => String(columnValue(row)).toLowerCase();
const sortedStamps = (rows) => [...rows].sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0)).map((r) => r.last_occurrence);
const shownStamp = "2025-12-07T13:32:48";
/* 9:00 AM sorts after 1:00 PM as text in en-US, so a sort that read the displayed text would put the afternoon first. */
const morning = { last_occurrence: "2025-12-07T09:00:00" };
const afternoon = { last_occurrence: "2025-12-07T13:00:00" };
const asWritten = new Date(Date.UTC(2025, 11, 7, 13, 32, 48));
console.log(JSON.stringify({
  zone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  shown: shownFor({ last_occurrence: shownStamp }),
  expected: asWritten.toLocaleString(undefined, { timeZone: "UTC" }),
  /* The stamp read as UTC and moved into the browser's zone: what localTime would state, five hours early under New York. */
  zoneShifted: asWritten.toLocaleString(),
  empty: shownFor({ last_occurrence: null }),
  sorted: sortedStamps([afternoon, morning]),
}));
