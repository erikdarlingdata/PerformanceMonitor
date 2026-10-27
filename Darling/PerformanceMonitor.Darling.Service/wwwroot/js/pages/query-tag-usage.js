/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Query Tag Usage page (#5987): resource use rolled up by query tag, read through get_resource_use_by_tag. One table per section (plan cache,
 * Query Store, procedures, blocking reports, blocking snapshots, deadlocks), each with its tag rows, the `untagged` row and the section total.
 * The sections are NEVER added together (the plan cache and Query Store count the same executions, and procedures overlap both), so each table
 * stands alone and the page computes no figure: every number is the read's. The deadlock table says one deadlock counts once for each tag
 * among its processes. A click on a tag sends it back as the read's `tag` parameter, so the server answers with that tag's subtree; the
 * page never trims a list itself. Choosing Fleet omits the server, and the read answers with one block per server and a fleet line.
 * All server text reaches the DOM through el()/textContent.
 */

import { el, mount, clear, onPageLeave, readTool, readToolWithinKeptHistory, keptWindowStrip, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, makeActivatable, fmtInt, fmtNum, retryOnceWhenBusy, busyRetryStrip } from "../util.js";

import { pageRangePicker, windowOfSpec } from "../page-range.js";
import { plainText } from "../plain-text.js";
import { relativeSpec } from "../time-range.js";
import { orderServers, displayNameFor } from "../server-order.js";

const DEFAULT_HOURS = 24;
const FLEET = "";

/** The section order and heading, as the read names them. */
export const SECTIONS = [
  { name: "plan_cache", title: "Plan cache" },
  { name: "query_store", title: "Query Store" },
  { name: "procedures", title: "Procedures" },
  { name: "blocking_reports", title: "Blocking reports" },
  { name: "blocking_snapshots", title: "Blocking snapshots" },
  { name: "deadlocks", title: "Deadlocks" },
];

export const DEADLOCK_NOTE = "One deadlock counts once for each tag among its processes, so the deadlock rows do not add up to the total.";

const MEASURE_LABELS = {
  cpu_ms: "CPU (ms)",
  duration_ms: "Duration (ms)",
  logical_reads: "Logical reads",
  logical_writes: "Logical writes",
  executions: "Executions",
  peak_memory_pages: "Peak memory (pages)",
  blocked_ms: "Blocked (ms)",
  caused_ms: "Caused (ms)",
  blocked_reports: "Blocked reports",
  caused_reports: "Caused reports",
  blocked_snapshots: "Blocked snapshots",
  caused_snapshots: "Caused snapshots",
  deadlocks: "Deadlocks",
  processes: "Processes",
};

/** The page's state, kept across the 60 s rebuild. `tag` is the tag id (as text) the page is opened on, or "" for every tag. */
const state = { server: FLEET, spec: relativeSpec(DEFAULT_HOURS * 3600000), tag: "", tagLabel: "" };
let knownServers = [];
let pageSeq = 0;
let loadSeq = 0;
let pageAbort = null;
let loadAbort = null;
/* #6793: leaving the page (route() calls beginPageScope) aborts its reads, so a slow read does not run on after the user went elsewhere. */
onPageLeave(() => {
  if (pageAbort) pageAbort.abort();
  if (loadAbort) loadAbort.abort();
});

/** The label of a measure: its friendly name, or the key itself for one the page does not know. */
export function measureLabel(key) {
  return MEASURE_LABELS[key] || key;
}

/** The measure keys of a section block, in the order the read sent them (from the total, which every block carries). */
export function measureKeys(block) {
  const total = block && block.total && typeof block.total === "object" ? block.total : {};
  return Object.keys(total);
}

/**
 * One section block as table rows: each tag row (its subtree figures, and its tag id so a click can open it), then the `untagged` row when the
 * read sent one, then the section total. Nothing is computed here; a figure the read did not send is null and draws a dash.
 */
export function sectionRows(block) {
  const rows = [];
  for (const t of (block && block.tag_rows) || []) {
    rows.push({ kind: "tag", label: t.path || t.tag, tagId: t.tag_id, figures: t.subtree || {} });
  }
  if (block && block.untagged) rows.push({ kind: "untagged", label: "untagged", tagId: null, figures: block.untagged });
  if (block && block.total) rows.push({ kind: "total", label: block.total_scope === "tag_subtree" ? "Total (this tag)" : "Total", tagId: null, figures: block.total });
  return rows;
}

function fmtFigure(v) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  return Number.isInteger(n) ? fmtInt(n) : fmtNum(n, 1);
}

/** One section block as a table. A tag name is a button that opens that tag's subtree. */
export function sectionTable(block, onTag) {
  const keys = measureKeys(block);
  const head = el("tr", {}, [el("th", { text: "Tag" }), ...keys.map((k) => el("th", { class: "num", text: measureLabel(k) }))]);
  const body = sectionRows(block).map((r) => {
    const label =
      r.kind === "tag" && r.tagId != null
        ? el("span", { class: "tag-link", title: "Show this tag and its children", text: r.label })
        : el("span", { text: r.label });
    if (r.kind === "tag" && r.tagId != null) makeActivatable(label, () => onTag(r));
    return el(
      "tr",
      { class: r.kind === "total" ? "total-row" : r.kind === "untagged" ? "untagged-row" : null },
      [el("td", {}, [label]), ...keys.map((k) => el("td", { class: "num", text: fmtFigure(r.figures[k]) }))]
    );
  });
  return el("table", { class: "data" }, [el("thead", {}, [head]), el("tbody", {}, body)]);
}

/** "the last 24 hours", "the last 7 days", or "this time range" when the window is not a plain preset. */
export function rangeText(hours) {
  const h = Number(hours);
  if (!(h > 0)) return "this time range";
  if (h > 48 && h % 24 === 0) return "the last " + h / 24 + " days";
  return "the last " + (h === 1 ? "hour" : h + " hours");
}

/** The read's own sentence for a section that is not plain data, in plain words: no status word in front of it. */
function statusLine(block) {
  if (!block || block.status === "ok" || block.status === "empty" || !block.message) return null;
  return el("div", { class: "muted", text: plainText(block.message) });
}

/** A section with no rows in the window says so in one line; the table of zeros under it is not drawn. */
function isEmptyBlock(block) {
  return !!block && block.status === "empty";
}

/** One section: its heading, then one table (a server) or one table per server with the fleet line (Fleet). */
export function sectionCard(section, block, onTag, range = "this time range", servers = []) {
  const parts = [el("h3", {}, [section.title, el("span", { class: "panel-sub", text: block && block.source ? " " + block.source : "" })])];
  if (!block) {
    parts.push(emptyStrip("The read sent no " + section.title + " section."));
    return el("div", { class: "panel", "data-section": section.name }, parts);
  }
  if (section.name === "deadlocks") parts.push(el("div", { class: "muted", text: DEADLOCK_NOTE }));
  /* #6794: an hourly-rollup section says which edges the hour buckets moved (the note names the materialization ceiling). */
  if (block.window_edges_note) parts.push(el("div", { class: "muted", text: plainText(block.window_edges_note) }));
  else if (block.tier === "hourly" && block.window_end) parts.push(el("div", { class: "muted", text: "Read from the hourly rollup up to " + block.window_end + "." }));
  if (isEmptyBlock(block) && !Array.isArray(block.servers)) {
    parts.push(emptyStrip("None in " + range + "."));
    return el("div", { class: "panel", "data-section": section.name }, parts);
  }
  const note = statusLine(block);
  if (note) parts.push(note);
  if (Array.isArray(block.servers)) {
    for (const s of block.servers) {
      /* #6157: the heading is the display name of the server; the read names it by its connection key. */
      parts.push(el("h4", { text: displayNameFor(servers, s.server) }));
      if (isEmptyBlock(s)) {
        parts.push(emptyStrip("None in " + range + "."));
        continue;
      }
      const sn = statusLine(s);
      if (sn) parts.push(sn);
      parts.push(sectionTable(s, onTag));
    }
    if (block.fleet_total) {
      const keys = Object.keys(block.fleet_total);
      parts.push(
        el("div", { class: "fleet-total" }, [
          el("strong", { text: "Fleet total " }),
          el("span", { text: keys.map((k) => measureLabel(k) + " " + fmtFigure(block.fleet_total[k])).join(", ") }),
        ])
      );
    }
  } else {
    parts.push(sectionTable(block, onTag));
  }
  return el("div", { class: "panel", "data-section": section.name }, parts);
}

function currentWindow() {
  return windowOfSpec(state.spec) || windowOfSpec(relativeSpec(DEFAULT_HOURS * 3600000));
}

/** The read's parameters for the page's choices: no `server` for Fleet, no `tag` for every tag. */
export function readParams(w = currentWindow()) {
  const p = { hours: w.hours };
  if (w.asOf) p.as_of = w.asOf;
  if (state.server !== FLEET) p.server = state.server;
  if (state.tag) p.tag = state.tag;
  return p;
}

function serverRows(data) {
  if (Array.isArray(data)) return data;
  if (data && Array.isArray(data.servers)) return data.servers;
  return [];
}

function fillServers(sel) {
  clear(sel);
  const opts = [{ value: FLEET, label: "Fleet" }, ...knownServers.map((r) => ({ value: r.server_name, label: r.display_name || r.server_name }))];
  if (state.server !== FLEET && !opts.some((o) => o.value === state.server)) opts.push({ value: state.server, label: state.server });
  for (const o of opts) sel.appendChild(el("option", { value: o.value, text: o.label }));
  sel.value = state.server;
}

export function renderQueryTagUsage(main) {
  if (pageAbort) pageAbort.abort();
  if (loadAbort) loadAbort.abort();
  const controller = (pageAbort = new AbortController());
  const mine = ++pageSeq;
  const body = el("div", {}, [loadingStrip("Loading query tag usage…")]);
  const tagSlot = el("div", {});

  const rangePicker = pageRangePicker({
    read: "get_resource_use_by_tag",
    spec: state.spec,
    label: "Window",
    onChange: (spec) => {
      state.spec = spec;
      load();
    },
  }).picker;
  const serverSel = el("select", { class: "range-select-inline", "aria-label": "Server" });
  fillServers(serverSel);
  serverSel.addEventListener("change", () => {
    state.server = serverSel.value;
    load();
  });

  const openTag = (r) => {
    state.tag = String(r.tagId);
    state.tagLabel = r.label;
    load();
  };
  const clearTag = () => {
    state.tag = "";
    state.tagLabel = "";
    load();
  };

  mount(main, [
    el("div", { class: "page-head" }, [
      el("h2", { text: "Query Tag Usage" }),
      el("div", { class: "spacer" }),
      el("label", { class: "range-control" }, [el("span", { text: "Server" }), serverSel]),
      el("div", { class: "range-control" }, [el("span", { text: "Window" }), rangePicker.node]),
    ]),
    tagSlot,
    body,
  ]);

  async function load() {
    const ticket = ++loadSeq;
    if (loadAbort) loadAbort.abort();
    const signal = (loadAbort = new AbortController()).signal;
    mount(
      tagSlot,
      state.tag
        ? el("div", { class: "strip notice tag-subtree-strip" }, [
            el("span", { text: "Showing " + state.tagLabel + " and its children. " }),
            el("button", { type: "button", class: "btn small", text: "Show all tags", onClick: clearTag }),
          ])
        : []
    );
    mount(body, loadingStrip("Loading query tag usage…"));
    const w = currentWindow();
    /* #6800: the fleet read is a long read once its history says so, so it can be refused busy while other long reads hold every
       slot. The page says so and asks once more after the Retry-After, instead of showing the refusal as a failure. */
    const res = await retryOnceWhenBusy(
      () => readToolWithinKeptHistory("get_resource_use_by_tag", readParams(w), signal),
      (seconds) => {
        if (ticket === loadSeq) mount(body, busyRetryStrip(seconds));
      },
      signal
    );
    /* The server list the headings are named from may still be on its way: wait for it so the first paint is not the keys. */
    await serversReady;
    if (ticket !== loadSeq) return;
    if (res.kind === "aborted" || res.kind === "auth") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    const data = res.data || {};
    const kept = keptWindowStrip(res);
    const sections = Array.isArray(data.sections) ? data.sections : [];
    const range = rangeText(w.hours);
    if (res.kind === "empty" && sections.length === 0) return mount(body, [kept, emptyStrip("None in " + range + ".")]);
    const cards = SECTIONS.map((s) => sectionCard(s, sections.find((b) => b && b.section === s.name) || null, openTag, range, knownServers));
    mount(body, [
      kept,
      noticeStrip("Each section counts from its own source, so sections are not added together: the plan cache and Query Store count the same executions, and procedures overlap both."),
      ...cards,
    ]);
  }

  const serversReady = (async () => {
    const res = await readTool("list_servers", {}, controller.signal);
    if (mine !== pageSeq || res.kind !== "data") return;
    knownServers = orderServers(serverRows(res.data));
    fillServers(serverSel);
  })();
  load();
}
