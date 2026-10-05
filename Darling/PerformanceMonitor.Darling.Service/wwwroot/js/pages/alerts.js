/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Alert History page (#1562) — the alert log from get_alert_history (server omitted = whole fleet, each row naming
 * its server). A range, a row limit, a server and a Show dismissed choice (module-scope, so they survive the 60 s
 * poll) set what is read; a reply the tool cut at the row limit says so above the table.
 * A filter box narrows the already-fetched rows by server name client-side (no
 * re-fetch). Every value is untrusted and reaches the DOM through el()/textContent (R4 — never innerHTML),
 * the expanders included (B2): the channel / sent / muted / delivery-error columns collapse into one compact
 * Status column, and the Detail column shows a truncated one-liner that expands to the full payload (structured
 * "Story:/Severity:/Confidence:" findings render as labeled fields).
 *
 * #4194: the Detail expansion is built lazily, on first open (detailCell/detailBody), not up front for all 200
 * rows. The 60s poll (app.js's refresh loop calls renderAlerts(main) again, not a diff of its own) reconciles
 * the existing table by row key instead of rebuilding it (renderAlerts/refreshAlerts/drawAlerts/reconcileRows) -
 * the header and filter box also survive a poll tick untouched, so typing in the filter is no longer clobbered
 * every 60s. Sort order, filters and every rendered field are unchanged.
 */

import { el, mount, readTool, apiSend, buildQuery, loadingStrip, errorStrip, emptyStrip, noticeStrip, disclosure,
         ALERT_STATE_LABELS, alertDeliveryState } from "../util.js";
import { VIZ, reapplyGridSort, gridRowOf } from "../panels.js";
import { copyText } from "../grid-tools.js";
import { mutePrefillParams } from "../mute-context.js";
import { getSession } from "../views-api.js";

/* #3169: the state-carrying notification_type values, derived from the one shared map rather than listed a
   second time. The status label already carries each of them, so the channel chip beside it must not repeat
   them - "No channel configured (unconfigured)" is the shape this prevents. */
const STATE_ONLY_CHANNELS = new Set(Object.keys(ALERT_STATE_LABELS));

/* Most urgent first when ascending, the order the fleet bands use (Critical, Warning, then the calm states). */
const SEVERITY_RANK = { critical: 0, warning: 1, info: 2, resolution: 3 };

const ALERT_COLUMNS = [
  { key: "alert_time", label: "Time", format: "time" },
  { key: "server_name", label: "Server" },
  { key: "metric_name", label: "Metric" },
  { key: "severity", label: "Severity", sortValue: (a) => (a.severity == null ? null : (SEVERITY_RANK[a.severity] ?? 9)), render: (a) => severityCell(a) },
  { key: "current_value", label: "Value", format: "num1" },
  { key: "threshold_value", label: "Threshold", format: "num1" },
  { key: "status", label: "Status", render: (a) => statusCell(a) },
  { key: "detail_text", label: "Detail", render: (a) => detailCell(a), copyValue: (a) => detailFullText(a) },
  { key: "triage", label: "Triage", csv: false, render: (a) => triageCell(a) },
];

/* The Mute column exists only for a seat whose session reports can_edit (the server enforces the write gate;
   this is only the affordance). Both links open the Mute Rules create form pre-filled from the row. "Mute this
   alert" keys on the row's store id when it has one and otherwise on the stored server spelling, and fills the
   database / wait / job / query dimensions from the detail text (js/mute-context.js); the display name in
   server_name is never what a rule matches. "Mute similar" leaves the server blank (this metric anywhere). */
let canMute = false;
const MUTE_COLUMN = { key: "mute", label: "Mute", csv: false, render: (a) => muteCell(a) };
function alertColumns() {
  return canMute ? [SELECT_COLUMN].concat(ALERT_COLUMNS, [MUTE_COLUMN]) : ALERT_COLUMNS;
}

/* Dismiss (the web twin of the Viewer's Dismiss Selected / Dismiss All), for a seat that can edit. The checked
 * rows live at module scope keyed by alertKey(), so the 60 s rebuild keeps them; they are cleared after a
 * successful dismiss and whenever a filter changes. The write is POST /api/alert-history/dismiss with the
 * (alert_time, server_id, metric_name) of each row; one request may list at most DISMISS_CHUNK rows, so a longer
 * list goes in several requests and the counts are added up. */
const DISMISS_CHUNK = 1000;
const selected = new Map();
const SELECT_COLUMN = { key: "select", label: "Select", csv: false, copy: false, render: (a) => selectCell(a) };

/* A row can be dismissed when it is live and names its server by id (the dismiss key needs server_id). */
function dismissable(a) {
  return a.dismissed !== true && a.server_id != null;
}

function dismissKeyOf(a) {
  return { alert_time: a.alert_time, server_id: a.server_id, metric_name: a.metric_name };
}

/* Empties the checked set and unticks the boxes of the rows the table keeps (reconcileRows reuses their nodes). */
function clearSelection(state) {
  selected.clear();
  uncheckBoxes(state);
}

/* Syncs every listed checkbox to the checked set (reconcileRows reuses the nodes of rows the table keeps). */
function uncheckBoxes(state) {
  if (!state || !state.tbody) return;
  for (const tr of state.tbody.children) {
    const row = gridRowOf(tr);
    for (const box of tr.querySelectorAll("input")) box.checked = !!row && selected.has(alertKey(row));
  }
}

function selectCell(a) {
  if (!dismissable(a)) return el("span", { class: "muted", text: "" });
  const box = el("input", { type: "checkbox", "aria-label": "Select alert " + a.metric_name });
  box.checked = selected.has(alertKey(a));
  box.addEventListener("change", () => {
    if (box.checked) selected.set(alertKey(a), a); else selected.delete(alertKey(a));
    if (live) updateDismissBar(live);
  });
  return box;
}

function muteCell(a) {
  const link = (text, params) => el("a", { href: "#/mute-rules" + buildQuery(params), text });
  return el("span", {}, [
    link("Mute this alert", mutePrefillParams(a)),
    " · ",
    link("Mute similar", { metric_name: a.metric_name }),
  ]);
}

/* #3539 A8e: the tier the alert FIRED at, as the tool reports it — never re-derived here from the metric name
 * (R1). The service reads it off the row's persisted context and falls back to the name only for rows that
 * carry none; severity_source says which arm answered, and it rides as the cell's title because the two are
 * not equal evidence: "critical" from the row is what the operator was paged with, "critical" from the name
 * is what the map says about the name. The tone classes are the status cell's own. */
const SEVERITY_TONE = { critical: "Critical", warning: "Warning", resolution: "Healthy", info: "Unknown" };
const SEVERITY_SOURCE_TITLE = {
  fired: "The tier this alert fired at, read from the row",
  metric_name: "Implied by the metric name; this row carries no fired tier",
};

function severityCell(a) {
  if (a.severity == null) return el("span", { class: "muted", text: "—" });
  return el("span", {
    class: "status-cell sev-" + (SEVERITY_TONE[a.severity] || "Unknown"),
    text: String(a.severity),
    title: SEVERITY_SOURCE_TITLE[a.severity_source] || null,
  });
}

/* Deep-link into the #2710 triage page for this row — the SAME route the alert webhooks link to, anchored at
 * this row's own firing instant, so the in-app path and the delivered link land on an identical page. */
function triageCell(a) {
  return el("a", {
    href: "#/triage" + buildQuery({ server: a.server_name, metric: a.metric_name, at: a.alert_time }),
    text: "Open",
  });
}

/* Sent + Muted + Delivery Error collapse into one glyph+text status cell (B2). The channel is appended only
   when it means something on THIS surface: the store records "tray" (the Lite/Dashboard system-tray toast),
   which the headless web dashboard has no equivalent for, so a bare "tray" is dropped rather than shown as a
   meaningless delivery channel (#2781). A real channel (email / webhook / email+webhook) still renders. */
function statusCell(a) {
  let glyph, label, sev;
  const stateLabel = alertDeliveryState(a);
  if (a.muted) {
    glyph = "⊘";
    label = "Muted";
    sev = "Warning";
  } else if (a.send_error) {
    glyph = "✕";
    label = "Delivery failed";
    sev = "Critical";
  } else if (stateLabel !== null) {
    /* #3169: the row states a delivery STATE rather than naming a channel — no channel applies to it, none
       is configured, it was muted, or a channel was consulted and nothing landed. One neutral glyph and
       severity for all of them: whether an unconfigured deployment is a problem is the operator's call, not
       this page's. The label and the legacy decode both live in util.js, shared with triage.js and pinned
       against the WPF surfaces' constants. Extends #2781/#2814, which established the reading for the
       headless "tray" row that #3169 then stopped the service writing. */
    glyph = "•";
    label = stateLabel;
    sev = "Unknown";
  } else if (a.alert_sent) {
    glyph = "✓";
    label = "Delivered";
    sev = "Healthy";
  } else {
    glyph = "•";
    label = "Not sent";
    sev = "Unknown";
  }
  /* #3712: an analysis finding's routing_reason — why it paged, or why it went to the digest — rides as the
     cell's title the way severity_source rides on the severity cell: "why didn't this page" answered on
     hover, from the row, without opening the detail. A delivery error keeps precedence; it is the rarer and
     costlier fact. */
  return el("span", { class: "status-cell sev-" + sev, title: a.send_error || a.routing_reason || null }, [
    el("span", { class: "glyph", text: glyph }),
    el("span", { text: label }),
    /* The channel chip names a real channel only. The state-carrying values are already in the label
       above, so repeating them would read as "No channel configured (unconfigured)". */
    a.notification_type && !STATE_ONLY_CHANNELS.has(a.notification_type)
      ? el("span", { class: "channel", text: a.notification_type }) : null,
    /* A dismissed row (Show dismissed) says so in words; the dimming alone is the same cue a muted row has. */
    a.dismissed === true ? el("span", { class: "channel", text: "Dismissed" }) : null,
  ]);
}

/* The Detail cell (B2): a truncated one-liner that expands to the full payload. Structured finding detail_text
 * (indented "Story:/Severity:/Confidence:/..." lines) renders as labeled fields; anything else renders as a
 * plain text block. A delivery error is appended as its own labeled field so it is fully readable in the
 * expansion, not merely a hover title on the status glyph.
 *
 * #4194: the expansion used to be built for all 200 rows up front, even though it sits behind a collapsed
 * <details> almost nobody opens - parseDetailFields() plus one DOM row per parsed field, times 200, was most
 * of the page's ~19,500 DOM nodes. It is now built once, on first open (the native "toggle" event, which fires
 * on both open and close - `built` skips the close firing and any open after the first). The placeholder is a
 * bare, unstyled div so the eventual body's markup - div.detail-fields, div.detail-fields.detail-error - lands
 * exactly as it did before this change; only the disc-body -> placeholder -> body nesting is one level deeper. */
/* The whole detail as plain text, for Copy and the CSV: the stored detail, then the delivery error on its own line. */
function detailFullText(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  return [hasDetail ? String(a.detail_text) : null, a.send_error ? "Delivery error: " + a.send_error : null].filter((x) => x != null).join("\n");
}

function detailCell(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  if (!hasDetail && !a.send_error) return el("span", { class: "muted", text: "—" });

  const summary = hasDetail ? a.detail_text : "Delivery error: " + a.send_error;
  const placeholder = el("div", {});
  const node = disclosure(summary, placeholder, { max: 120 });

  let built = false;
  node.addEventListener("toggle", async () => {
    if (built || !node.open) return;
    built = true;
    /* #5241: an Analysis alert's advice and fix script are fetched here, on the first open, for this one row - not
     * carried by the 60 s list read for every row. Loading strip while it loads; an error or an empty answer falls
     * back to the detail_text rendering. Any other alert has no advice beyond detail_text and makes no request. */
    if (isAnalysisAlert(a)) {
      mount(placeholder, [loadingStrip()]);
      const items = await alertAdvice(a);
      mount(placeholder, detailBody(a, items));
      return;
    }
    mount(placeholder, detailBody(a));
  });
  return node;
}

/* The advice fetched for an opened Analysis row (#5241), by alertKey, for the page's lifetime: the 60 s poll rebuilds
 * the grid's nodes, and a row an operator already opened must not ask again. An answer (advice, or none) is kept; a
 * failed read is not, so opening the row again after a transient error asks again. */
const adviceCache = new Map();

function isAnalysisAlert(a) {
  return typeof a.metric_name === "string" && a.metric_name.startsWith("Analysis:");
}

function alertAdvice(a) {
  const key = alertKey(a);
  if (!adviceCache.has(key)) {
    const pending = readTool("get_alert_details", { server_id: a.server_id, metric_name: a.metric_name, alert_time: a.alert_time })
      .then((res) => {
        if (res.kind === "data") return Array.isArray(res.data?.details) && res.data.details.length > 0 ? res.data.details : null;
        if (res.kind === "empty") return null;
        adviceCache.delete(key);
        return null;
      }, () => { adviceCache.delete(key); return null; });
    adviceCache.set(key, pending);
  }
  return adviceCache.get(key);
}

/* The expansion body: unchanged shape from the pre-#4194 eager version (see detailCell above), just built lazily. */
function detailBody(a, details) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  /* #5241: an Analysis row's stored context holds advice; opening the row fetches it (detailCell) as `details`: the
   * structured items the desktop's Alert Detail window shows - heading, labelled fields, the advice prose and
   * the fix script. They replace the parsed detail_text, which is the same headings and fields without the prose.
   * A row without them (not an Analysis alert, no advice, a failed fetch) keeps the detail_text rendering below. */
  const items = Array.isArray(details) && details.length > 0 ? details : null;
  const fields = !items && hasDetail ? parseDetailFields(a.detail_text) : null;
  const body = [];
  if (items) {
    for (const item of items) body.push(...detailItem(item));
  } else if (fields) {
    body.push(el("div", { class: "detail-fields" }, fields.map(fieldRow)));
  } else if (hasDetail) {
    body.push(el("div", { class: "detail-raw", text: a.detail_text }));
  }
  if (a.send_error) {
    body.push(el("div", { class: "detail-fields detail-error" }, [fieldRow(["Delivery error", a.send_error])]));
  }
  return body;
}

/* One structured detail item (#5241): its heading and labelled fields in the same grid the parsed detail_text uses,
 * then its body - advice prose as a paragraph, a fix script as a <pre> with a Copy button (the Recommendations tab's
 * pattern, analysis-findings.js). Advise-only: the read never carries the desktop's Apply payload, and nothing here
 * runs a script. Every value reaches the DOM through el()'s text path (R4). */
function detailItem(item) {
  const nodes = [];
  const rows = [];
  if (item.heading) rows.push(fieldRow([null, item.heading]));
  for (const f of Array.isArray(item.fields) ? item.fields : []) rows.push(fieldRow([f.label, f.value]));
  if (rows.length > 0) nodes.push(el("div", { class: "detail-fields" }, rows));
  const text = item.body == null ? "" : String(item.body);
  if (text.trim().length === 0) return nodes;
  if (item.is_code_block) {
    const copy = el("button", { class: "btn small", type: "button", text: "Copy" });
    copy.addEventListener("click", async () => {
      const ok = await copyText(text);
      copy.textContent = ok ? "Copied" : "Copy failed";
    });
    nodes.push(el("pre", { class: "reco-fix", text }));
    nodes.push(el("div", { class: "reco-actions" }, [copy]));
  } else {
    nodes.push(el("p", { class: "detail-advice", text }));
  }
  return nodes;
}

/* One parsed pair -> a grid row of the two-column .detail-fields grid. A section heading (no label) is its own
 * full-width row: a lone value in the label/value grid would take the label column's cell and shift every later
 * label and value over by one, which put the labels in the wide value column's far edge, out of sight, leaving
 * bare values (Alert History's blocking and incident details, where every other row is label: value). */
function fieldRow([k, v]) {
  if (!k) return el("div", { class: "detail-heading", text: v });
  return el("div", { class: "detail-field" }, [
    el("span", { class: "fk", text: k }),
    el("span", { class: "fv", text: v }),
  ]);
}

/* Parse an indented "Key: value" detail block into [key, value] pairs; returns null (=> render as raw) unless
 * at least two lines match, so a plain one-sentence detail stays a plain block. The flattened alert context
 * writes each item as an unindented heading line ("Blocking chain (x2)", "Incident 1 of 2") over its indented
 * "Label: value" lines, with a blank line between items: an unindented non-matching line that opens a block
 * (the first line, or the first after a blank one) is a section heading, kept as its own [null, heading] entry
 * instead of being folded onto the previous value ("24.1s-34.9s Incident 1 of 2"). Any other non-matching line
 * is a continuation and folds into the previous field's value (wrapped story text). */
function parseDetailFields(text) {
  const fields = [];
  let matched = 0;
  let blockStart = true;
  for (const rawLine of String(text).split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line.length === 0) {
      blockStart = true;
      continue;
    }
    const m = /^([A-Za-z][A-Za-z ]{0,28}):\s*(.*)$/.exec(line);
    if (m) {
      fields.push([m[1], m[2]]);
      matched++;
    } else if (blockStart && !/^\s/.test(rawLine)) {
      fields.push([null, line]);
    } else if (fields.length) {
      fields[fields.length - 1][1] += " " + line;
    } else {
      fields.push([null, line]);
    }
    blockStart = false;
  }
  return matched >= 2 ? fields : null;
}

/* A row's identity for the #4194 keyed refresh: the same (server, metric, fired instant) triple triageCell()
 * above already treats as identifying one alert, with server_id in front so two servers that share a display
 * name never collide (a row without a server_id keys on its names and time alone) - there is no surrogate id in get_alert_history's payload. */
function alertKey(a) {
  return (a.server_id == null ? "" : a.server_id) + "\u0000" + a.server_name + "\u0000" + a.metric_name + "\u0000" + a.alert_time;
}

/* Builds ONE <tr>, styled identically to VIZ.table's own row (same columns, same cell()), by handing it a
 * single-row page and lifting the <tr> back out - reuses the shared renderer's formatting/severity classes
 * without duplicating them, and without vizTable itself having to know about incremental refresh. */
function alertRowNode(row) {
  const wrap = VIZ.table({ alerts: [row] }, { rowsKey: "alerts", columns: alertColumns(), tools: false });
  return markDismissed(wrap.querySelector("tbody tr"), row);
}

/* #4194: app.js's route() calls renderAlerts(main) fresh on every 60s poll tick as well as on first navigation
 * (see app.js's refresh loop). Reconciles the existing <tbody> to the new row set by key instead of tearing
 * down and rebuilding every row: unchanged rows keep their DOM node, only added/removed rows touch the DOM.
 * Order follows `rows` (already alert_time_desc from the read), so a row that moves position - it cannot,
 * since alert_time is immutable once written, but this stays correct if that ever changes - would still land
 * in the right place. */
function reconcileRows(tbody, rows, rowMap) {
  const nextKeys = new Set(rows.map(alertKey));
  for (const [key, tr] of rowMap) {
    if (!nextKeys.has(key)) {
      tr.remove();
      rowMap.delete(key);
    }
  }
  let anchor = tbody.firstChild;
  for (const row of rows) {
    const key = alertKey(row);
    let tr = rowMap.get(key);
    /* dismissed is not part of the key, so a kept node whose alert was dismissed (or restored) since the last
     * read is replaced by a freshly built one. */
    const old = tr && gridRowOf(tr);
    if (tr && old && (old.dismissed === true) !== (row.dismissed === true)) {
      if (tr === anchor) anchor = anchor.nextSibling;
      tr.remove();
      tr = null;
    }
    if (!tr) {
      tr = alertRowNode(row);
      rowMap.set(key, tr);
    }
    if (tr === anchor) {
      anchor = anchor.nextSibling;
    } else {
      tbody.insertBefore(tr, anchor);
    }
  }
}

/* The four read choices, kept at module scope so the 60 s poll (which calls renderAlerts again) and a visit to
 * another page and back keep them. The windows are the desktop Alert History's, less "All": the tool refuses
 * more than 168 hours, and a choice it would refuse is not offered. The row limits stop at the dispatch
 * layer's 1000-row ceiling. The server is the registry's server_name ("" = the whole fleet). */
const WINDOW_CHOICES = [
  { hours: 1, label: "Last 1 Hour" },
  { hours: 4, label: "Last 4 Hours" },
  { hours: 24, label: "Last 24 Hours" },
  { hours: 168, label: "Last 7 Days" },
];
const LIMIT_CHOICES = [200, 500, 1000];
const choices = { hours: 24, limit: 200, server: "", dismissed: false };

/* The get_alert_history parameters the current choices ask for. server_name and include_dismissed are left out
 * (buildQuery drops empty values) when they are at their defaults. */
function readParams() {
  return {
    hours_back: choices.hours,
    limit: choices.limit,
    server_name: choices.server || null,
    include_dismissed: choices.dismissed ? "true" : null,
  };
}

function windowLabel() {
  return (WINDOW_CHOICES.find((w) => w.hours === choices.hours) || WINDOW_CHOICES[2]).label.toLowerCase();
}

/* The server names list_servers reported on the last successful read; the picker keeps the chosen server in the
 * list even when that read fails or no longer names it, so the choice stays visible. */
let serverNames = [];

function serverRowsOf(data) {
  if (Array.isArray(data)) return data;
  if (data && Array.isArray(data.servers)) return data.servers;
  return [];
}

async function loadServerNames() {
  const res = await readTool("list_servers", {});
  if (res.kind === "error" || res.kind === "empty") return;
  serverNames = serverRowsOf(res.data)
    .map((r) => ({ name: r.server_name, label: r.display_name || r.server_name }))
    .filter((r) => r.name);
}

function pickerOptions(items) {
  return items.map((i) => el("option", { value: String(i.value), text: i.label }));
}

function picker(label, items, chosen) {
  const sel = el("select", { class: "range-select-inline", "aria-label": label }, pickerOptions(items));
  sel.value = String(chosen);
  return sel;
}

function control(label, select) {
  return el("label", { class: "range-control" }, [el("span", { text: label }), select]);
}

function serverItems() {
  const items = [{ value: "", label: "All servers" }].concat(serverNames.map((r) => ({ value: r.name, label: r.label })));
  if (choices.server && !serverNames.some((r) => r.name === choices.server)) items.push({ value: choices.server, label: choices.server });
  return items;
}

/* A dismissed row (include_dismissed) renders dimmed and italic, so it reads as acknowledged beside the live
 * ones; the class is set where each row node is built, the title says what it means, and the Status cell
 * carries the word. */
function markDismissed(tr, row) {
  if (!tr || !row) return tr;
  if (row.dismissed === true) {
    tr.className = ((tr.className || "") + " alert-dismissed").trim();
    tr.setAttribute("title", "Dismissed");
  }
  return tr;
}

/* Remembers the last mount so a poll tick landing on the SAME still-open page can reconcile in place instead of
 * rebuilding (module-level: renderAlerts gets no state of its own from app.js's route(), which just calls it
 * again). `headEl.isConnected` tells a poll tick apart from a fresh navigation: mount() on any OTHER page
 * detaches this page's headEl from the document, so a stale `live` falls through to a full rebuild below. */
let live = null;

/* The dismiss toolbar: Dismiss Selected (with the checked count), Dismiss All (every listed row the current
 * filters show), and a status line carrying the last outcome. Present only for a seat that can edit. */
function buildDismissBar(state) {
  const selBtn = el("button", { type: "button", class: "btn", text: "Dismiss Selected" });
  const allBtn = el("button", { type: "button", class: "btn", text: "Dismiss All" });
  const status = el("span", { class: "dismiss-status", text: "" });
  const bar = el("div", { class: "dismiss-bar" }, [selBtn, allBtn, status]);
  state.dismiss = { bar, selBtn, allBtn, status, busy: false };
  selBtn.addEventListener("click", () => dismissRows(state, [...selected.values()]));
  allBtn.addEventListener("click", () => {
    const rows = visibleRows(state).filter(dismissable);
    if (!rows.length) return;
    /* The prompt the Viewer asks before Dismiss All. Unlike the Viewer, which dismisses every live alert in the
       window, this page dismisses only the rows it lists, so a truncated page says the rest stay live. */
    let ask = "Dismiss " + rows.length + " alert(s)?\n\nDismissed alerts are hidden from this view but remain in the store.";
    if (state.truncated) ask += "\n\nMore alerts exist than the " + state.alerts.length + " listed; only the listed ones are dismissed and the rest stay live.";
    if (typeof globalThis.confirm === "function" && !globalThis.confirm(ask)) return;
    dismissRows(state, rows);
  });
  updateDismissBar(state);
  return bar;
}

function updateDismissBar(state) {
  const d = state.dismiss;
  if (!d) return;
  d.selBtn.textContent = selected.size ? "Dismiss Selected (" + selected.size + ")" : "Dismiss Selected";
  d.selBtn.disabled = d.busy || selected.size === 0;
  d.allBtn.disabled = d.busy || !visibleRows(state).some(dismissable);
}

/* The rows the current filter box lets through - what the table shows and what Dismiss All acts on. */
function visibleRows(state) {
  const q = state.filter.value.trim().toLowerCase();
  return q ? state.alerts.filter((a) => String(a.server_name || "").toLowerCase().includes(q)) : state.alerts;
}

function dismissOutcomeText(t) {
  let text = "Dismissed " + t.dismissed + " of " + t.requested;
  if (t.already_dismissed > 0) text += "; " + t.already_dismissed + " already dismissed";
  if (t.unknown > 0) text += "; " + t.unknown + " " + (t.unknown === 1 ? "was" : "were") + " already gone";
  return text;
}

async function dismissRows(state, rows) {
  const d = state.dismiss;
  if (!d || d.busy || !rows.length) return;
  d.busy = true;
  updateDismissBar(state);
  d.status.textContent = "Dismissing " + rows.length + "…";
  const totals = { requested: 0, dismissed: 0, already_dismissed: 0, unknown: 0 };
  let failure = null;
  for (let i = 0; i < rows.length && !failure; i += DISMISS_CHUNK) {
    const res = await apiSend("POST", "/api/alert-history/dismiss", { alerts: rows.slice(i, i + DISMISS_CHUNK).map(dismissKeyOf) });
    if (res.kind === "data" && res.data && typeof res.data === "object") {
      for (const k of Object.keys(totals)) totals[k] += Number(res.data[k]) || 0;
    } else if (res.status === 403) {
      failure = res.message && !/^Request failed/.test(res.message) ? res.message : "This session is read-only, so nothing was dismissed.";
    } else if (res.kind === "auth") {
      failure = res.message || "Your session has expired. Sign in again.";
    } else {
      failure = res.message || "The dismiss request failed.";
    }
  }
  d.busy = false;
  if (failure) {
    /* Never claim success; a partly done request says how far it got. */
    d.status.textContent = totals.requested ? failure + " (" + dismissOutcomeText(totals) + " before it failed)" : failure;
    d.status.setAttribute("class", "dismiss-status dismiss-error");
    updateDismissBar(state);
    if (totals.requested) await refreshAlerts(state);
    return;
  }
  /* Unselect only what was sent; a box ticked while the request was in flight stays checked. */
  for (const a of rows) selected.delete(alertKey(a));
  uncheckBoxes(state);
  d.status.setAttribute("class", "dismiss-status");
  d.status.textContent = dismissOutcomeText(totals);
  await refreshAlerts(state);
}

export async function renderAlerts(main) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refreshAlerts(live);
    return;
  }

  canMute = !!(await getSession()).can_edit;
  if (!canMute) selected.clear();
  await loadServerNames();
  const filter = el("input", {
    class: "filter-box",
    type: "text",
    placeholder: "Filter by server…",
    "aria-label": "Filter alerts by server",
  });

  const windowSel = picker("Time range", WINDOW_CHOICES.map((w) => ({ value: w.hours, label: w.label })), choices.hours);
  windowSel.setAttribute("title", "The web reads at most 7 days of alert history, so there is no All choice.");
  const limitSel = picker("Row limit", LIMIT_CHOICES.map((n) => ({ value: n, label: n + " rows" })), choices.limit);
  const serverSel = picker("Server", serverItems(), choices.server);
  const dismissedBox = el("input", { type: "checkbox", "aria-label": "Show dismissed alerts" });
  dismissedBox.checked = choices.dismissed;

  const meta = el("div", { class: "meta", text: "" });
  const noticeBox = el("div", {});
  const tableBox = el("div", {});
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Alert History" }),
    meta,
    el("div", { class: "spacer" }),
    control("Range", windowSel),
    control("Rows", limitSel),
    control("Server", serverSel),
    el("label", { class: "range-control" }, [dismissedBox, el("span", { text: "Show dismissed" })]),
    filter,
  ]);
  live = { main, headEl, meta, noticeBox, tableBox, filter, alerts: [], tbody: null, rowMap: null, seq: 0, dismiss: null };
  mount(main, canMute ? [headEl, buildDismissBar(live), noticeBox, tableBox] : [headEl, noticeBox, tableBox]);

  /* A changed choice refetches and reconciles the table that is already drawn, the same path a poll tick takes.
     It also drops the checked rows: they were picked from the list the old choice showed. */
  const changed = () => { clearSelection(live); return refreshAlerts(live); };
  windowSel.addEventListener("change", () => { choices.hours = Number(windowSel.value); changed(); });
  limitSel.addEventListener("change", () => { choices.limit = Number(limitSel.value); changed(); });
  serverSel.addEventListener("change", () => { choices.server = serverSel.value; changed(); });
  dismissedBox.addEventListener("change", () => { choices.dismissed = !!dismissedBox.checked; changed(); });
  filter.addEventListener("input", () => {
    clearSelection(live);
    drawAlerts(live);
    updateDismissBar(live);
  });

  mount(tableBox, loadingStrip("Loading alerts…"));
  await refreshAlerts(live);
}

/* Re-fetches with the current choices, then hands the new alerts to drawAlerts() to reconcile in place. Unlike
 * the first mount, a poll tick never shows the loading strip - the previous table stays exactly as it is until
 * the new page is ready, so a healthy 60s tick with no new alert produces no visible change and no DOM churn at
 * all. A changed window or server reconciles the same way: rows that left the window are removed, new ones land
 * in order. A reply that arrives after a newer request was made is dropped. */
async function refreshAlerts(state) {
  const seq = ++state.seq;
  const res = await readTool("get_alert_history", readParams());
  if (seq !== state.seq) return;
  state.meta.textContent = (choices.server ? choices.server : "fleet-wide") + " · " + windowLabel();
  mount(state.noticeBox, []);
  if (res.kind === "error" || res.kind === "empty") {
    state.alerts = [];
    state.tbody = null;
    state.rowMap = null;
    state.truncated = false;
    /* The checked set survives a failed read; it is pruned only against a successful one. */
    updateDismissBar(state);
    mount(state.tableBox, res.kind === "error" ? errorStrip(res.message) : emptyStrip(res.message));
    return;
  }
  state.alerts = res.data.alerts || [];
  state.truncated = res.data.truncated === true;
  /* A checked row that is no longer listed (dismissed elsewhere, aged out) is no longer selected. */
  const listed = new Set(state.alerts.filter(dismissable).map(alertKey));
  for (const key of [...selected.keys()]) if (!listed.has(key)) selected.delete(key);
  if (res.data.truncated === true) {
    mount(state.noticeBox, noticeStrip(
      "More alerts exist than shown: the newest " + state.alerts.length + " are listed. Raise the row limit or narrow the range to see the rest."));
  }
  drawAlerts(state);
  updateDismissBar(state);
}

/* Applies the (client-side, unchanged) server filter to state.alerts and either reconciles the existing table
 * or, the first time / after an empty or error state, builds it fresh and records tbody + rowMap for next time. */
function drawAlerts(state) {
  const q = state.filter.value.trim().toLowerCase();
  const rows = q ? state.alerts.filter((a) => String(a.server_name || "").toLowerCase().includes(q)) : state.alerts;

  if (!rows.length) {
    state.tbody = null;
    state.rowMap = null;
    mount(state.tableBox, emptyStrip(q ? 'No alerts match "' + state.filter.value + '".' : "No alerts in this window."));
    return;
  }

  if (state.tbody) {
    reconcileRows(state.tbody, rows, state.rowMap);
    reapplyGridSort(state.tbody);
    return;
  }

  const table = VIZ.table({ alerts: rows }, { rowsKey: "alerts", columns: alertColumns() });
  mount(state.tableBox, table);
  const tbody = table.querySelector("tbody");
  const rowMap = new Map();
  [...tbody.children].forEach((tr) => {
    const row = gridRowOf(tr);
    markDismissed(tr, row);
    rowMap.set(alertKey(row), tr);
  });
  state.tbody = tbody;
  state.rowMap = rowMap;
}
