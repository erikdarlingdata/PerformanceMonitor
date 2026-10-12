/* #5644 W12: runs the web dashboard's real windowFloorStrip, cappedReadNote, partialWindowSubtitle and markPartialSubtitle (wwwroot/js/util.js)
   on a small fake DOM and prints what each drew as one line of JSON. WebPartialWindowBehaviourTests starts it as
       node web-partial-window-harness.mjs <path to the js folder>
   Only the DOM is a stand-in; util.js is the shipped module. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this.className = "";
    this.dataset = {};
    this.style = {};
  }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/events" };

const root = process.argv[2];
const { windowFloorStrip, cappedReadNote, partialWindowSubtitle, markPartialSubtitle, localizeIsoText, emptyStrip, noticeStrip, el, localTime } = await import(pathToFileURL(root + "/util.js").href);

const text = (n) => (n ? n.textContent : null);
const OLDEST = "2026-10-12T13:29:59.5027090Z";
const oldestLocal = localTime(OLDEST);
const out = { oldestLocal };

/* the server's own capped note (WebDataStartNote), as readTool leaves it: a sentence built for a grid */
const serverCapped = {
  window_truncated: true,
  oldest_shown_utc: OLDEST,
  truncation_note: "partial window: this grid shows only the newest rows, back to X, because it stops at its row limit. The window started at S. The grid covers X to E.",
};
out.gridServerNote = text(windowFloorStrip(serverCapped, { viz: "table" }));
out.chartServerNote = text(windowFloorStrip(serverCapped, { viz: "line" }));
/* a chart does not draw a plain data-start note: its axis shows the empty span */
out.chartDataStart = text(windowFloorStrip({ window_truncated: true, data_start_utc: OLDEST, truncation_note: "partial window: this panel's data starts at X." }, { viz: "line" }));
out.chartWindowNoteOff = text(windowFloorStrip(serverCapped, { viz: "line", windowNote: false }));

/* the active-query page: cut at its limit, newest first, no window_truncated */
const active = { truncated: true, total_snapshots: 219, snapshots_returned: 50, order: "collection_time_desc", oldest_returned_collection_time: OLDEST };
out.activeGrid = text(windowFloorStrip(active, { viz: "table" }));
out.activeChart = text(windowFloorStrip(active, { viz: "line" }));
out.activeStat = text(windowFloorStrip(active, { viz: "stat" }));
out.activeNoCounts = cappedReadNote({ truncated: true, order: "collection_time_desc", oldest_returned_collection_time: OLDEST }, { viz: "table" }).text;
out.notCut = text(windowFloorStrip({ ...active, truncated: false }, { viz: "table" }));
out.rankedByOther = text(windowFloorStrip({ ...active, order: "total_elapsed" }, { viz: "table" }));
out.noOldest = text(windowFloorStrip({ truncated: true, order: "collection_time_desc" }, { viz: "table" }));
out.serverNoteWins = text(windowFloorStrip({ ...active, ...serverCapped }, { viz: "table" }));
out.ownNote = text(windowFloorStrip(active, { viz: "table", noteKey: "truncation_note" }));

/* the heading: a capped panel drops its window label, any other keeps it */
out.subtitleActive = partialWindowSubtitle(active, { viz: "table" });
out.subtitleServer = partialWindowSubtitle(serverCapped, { viz: "line" });
out.subtitleDataStart = partialWindowSubtitle({ window_truncated: true, data_start_utc: OLDEST, truncation_note: "x" }, { viz: "line" });
out.subtitleCovered = partialWindowSubtitle({ truncated: false, rows: [] }, { viz: "table" });
const panel = () => el("div", { class: "panel card" }, [
  el("h3", {}, ["Active Queries", el("span", { class: "panel-sub", text: " last 24 hours" })]),
  el("div", { class: "panel-body" }, [el("span", { class: "panel-sub", text: " not the heading" })]),
]);
const capped = panel();
markPartialSubtitle(capped, active, { viz: "table" });
out.headingCapped = capped.children[0].textContent;
out.bodyUntouched = capped.children[1].textContent;
const covered = panel();
markPartialSubtitle(covered, { truncated: false }, { viz: "table" });
out.headingCovered = covered.children[0].textContent;
markPartialSubtitle(null, active, { viz: "table" });
const noSub = el("div", { class: "panel card" }, [el("h3", {}, ["No sub"])]);
markPartialSubtitle(noSub, active, { viz: "table" });
out.headingWithoutSub = noSub.children[0].textContent;

/* W20: ISO instants inside a sentence print in the grids local-time format */
const iso = "2026-10-09T14:37:35.0158630";
const isoZ = "2026-10-08T18:11:14Z";
out.isoLocal = localTime(iso);
out.isoZLocal = localTime(isoZ);
out.sentence = localizeIsoText("stored at " + iso + " and not collected since " + isoZ + ".");
out.noStamp = localizeIsoText("nothing to change 10/9/2026, 10:36:32 AM");
out.nonString = String(localizeIsoText(null));
out.emptyStripText = emptyStrip("last event " + iso).textContent;
out.noticeStripText = noticeStrip("since " + isoZ).textContent;
/* G1a: a caller-wording sentence that also carries an instant gets BOTH treatments (plainText rewrites the wording, then the instant goes local) */
const both = "since " + isoZ + ": 9 indexes match and 5 were returned. Rows are ordered by total wait time descending; raise limit to see more.";
out.bothEmpty = emptyStrip("TRUNCATED: " + both).textContent;
out.bothNotice = noticeStrip("TRUNCATED: " + both).textContent;
/* G1a: the restart banner names the server clock for a SQL Server start-time pair (the pair is not UTC, so no "Z" and no shift) */
out.bannerText = "baseline discontinuity at 10/12/2026, 8:15:00 AM (restart) — instance identity changed — start time 2026-10-09 14:00:00 → 2026-10-12 08:15:00 (server clock); every delta baseline for this server was forgotten";
out.bannerNotice = noticeStrip(out.bannerText).textContent;

console.log(JSON.stringify(out));
