/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Alert History page (#1562) — the fleet-wide alert log from get_alert_history (server omitted = whole fleet,
 * each row naming its server). A filter box narrows the already-fetched rows by server name client-side (no
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

import { el, mount, readTool, buildQuery, loadingStrip, errorStrip, emptyStrip, disclosure,
         ALERT_STATE_LABELS, alertDeliveryState } from "../util.js";
import { VIZ } from "../panels.js";

/* #3169: the state-carrying notification_type values, derived from the one shared map rather than listed a
   second time. The status label already carries each of them, so the channel chip beside it must not repeat
   them - "No channel configured (unconfigured)" is the shape this prevents. */
const STATE_ONLY_CHANNELS = new Set(Object.keys(ALERT_STATE_LABELS));

const ALERT_COLUMNS = [
  { key: "alert_time", label: "Time", format: "time" },
  { key: "server_name", label: "Server" },
  { key: "metric_name", label: "Metric" },
  { key: "severity", label: "Severity", render: (a) => severityCell(a) },
  { key: "current_value", label: "Value", format: "num1" },
  { key: "threshold_value", label: "Threshold", format: "num1" },
  { key: "status", label: "Status", render: (a) => statusCell(a) },
  { key: "detail_text", label: "Detail", render: (a) => detailCell(a) },
  { key: "triage", label: "Triage", render: (a) => triageCell(a) },
];

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
function detailCell(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  if (!hasDetail && !a.send_error) return el("span", { class: "muted", text: "—" });

  const summary = hasDetail ? a.detail_text : "Delivery error: " + a.send_error;
  const placeholder = el("div", {});
  const node = disclosure(summary, placeholder, { max: 120 });

  let built = false;
  node.addEventListener("toggle", () => {
    if (built || !node.open) return;
    built = true;
    mount(placeholder, detailBody(a));
  });
  return node;
}

/* The expansion body: unchanged shape from the pre-#4194 eager version (see detailCell above), just built lazily. */
function detailBody(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  const fields = hasDetail ? parseDetailFields(a.detail_text) : null;
  const body = [];
  if (fields) {
    body.push(el("div", { class: "detail-fields" }, fields.map(fieldRow)));
  } else if (hasDetail) {
    body.push(el("div", { class: "detail-raw", text: a.detail_text }));
  }
  if (a.send_error) {
    body.push(el("div", { class: "detail-fields detail-error" }, [fieldRow(["Delivery error", a.send_error])]));
  }
  return body;
}

function fieldRow([k, v]) {
  return el("div", { class: "detail-field" }, [
    k ? el("span", { class: "fk", text: k }) : null,
    el("span", { class: "fv", text: v }),
  ]);
}

/* Parse an indented "Key: value" detail block into [key, value] pairs; returns null (=> render as raw) unless
 * at least two lines match, so a plain one-sentence detail stays a plain block. A non-matching continuation
 * line folds into the previous field's value (wrapped story text). */
function parseDetailFields(text) {
  const lines = String(text)
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);
  const fields = [];
  let matched = 0;
  for (const line of lines) {
    const m = /^([A-Za-z][A-Za-z ]{0,28}):\s*(.*)$/.exec(line);
    if (m) {
      fields.push([m[1], m[2]]);
      matched++;
    } else if (fields.length) {
      fields[fields.length - 1][1] += " " + line;
    } else {
      fields.push(["", line]);
    }
  }
  return matched >= 2 ? fields : null;
}

/* A row's identity for the #4194 keyed refresh: the same (server, metric, fired instant) triple triageCell()
 * above already treats as identifying one alert - there is no surrogate id in get_alert_history's payload. */
function alertKey(a) {
  return a.server_name + "\u0000" + a.metric_name + "\u0000" + a.alert_time;
}

/* Builds ONE <tr>, styled identically to VIZ.table's own row (same columns, same cell()), by handing it a
 * single-row page and lifting the <tr> back out - reuses the shared renderer's formatting/severity classes
 * without duplicating them, and without vizTable itself having to know about incremental refresh. */
function alertRowNode(row) {
  const wrap = VIZ.table({ alerts: [row] }, { rowsKey: "alerts", columns: ALERT_COLUMNS });
  return wrap.querySelector("tbody tr");
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

/* Remembers the last mount so a poll tick landing on the SAME still-open page can reconcile in place instead of
 * rebuilding (module-level: renderAlerts gets no state of its own from app.js's route(), which just calls it
 * again). `headEl.isConnected` tells a poll tick apart from a fresh navigation: mount() on any OTHER page
 * detaches this page's headEl from the document, so a stale `live` falls through to a full rebuild below. */
let live = null;

export async function renderAlerts(main) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refreshAlerts(live);
    return;
  }

  const filter = el("input", {
    class: "filter-box",
    type: "text",
    placeholder: "Filter by server…",
    "aria-label": "Filter alerts by server",
  });

  const tableBox = el("div", {});
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Alert History" }),
    el("div", { class: "meta", text: "fleet-wide · last 24h" }),
    el("div", { class: "spacer" }),
    filter,
  ]);
  mount(main, [headEl, tableBox]);

  mount(tableBox, loadingStrip("Loading alerts…"));
  const res = await readTool("get_alert_history", { hours_back: 24, limit: 200 });
  if (res.kind === "error") return mount(tableBox, errorStrip(res.message));
  if (res.kind === "empty") return mount(tableBox, emptyStrip(res.message));

  live = { main, headEl, tableBox, filter, alerts: res.data.alerts || [], tbody: null, rowMap: null };
  filter.addEventListener("input", () => drawAlerts(live));
  drawAlerts(live);
}

/* A background poll tick: re-fetch, then hand the new alerts to drawAlerts() to reconcile in place. Unlike the
 * first mount, this never shows the loading strip - the previous table stays exactly as it is until the new
 * page is ready, so a healthy 60s tick with no new alert produces no visible change and no DOM churn at all. */
async function refreshAlerts(state) {
  const res = await readTool("get_alert_history", { hours_back: 24, limit: 200 });
  if (res.kind === "error" || res.kind === "empty") {
    state.tbody = null;
    state.rowMap = null;
    mount(state.tableBox, res.kind === "error" ? errorStrip(res.message) : emptyStrip(res.message));
    return;
  }
  state.alerts = res.data.alerts || [];
  drawAlerts(state);
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
    return;
  }

  const table = VIZ.table({ alerts: rows }, { rowsKey: "alerts", columns: ALERT_COLUMNS });
  mount(state.tableBox, table);
  const tbody = table.querySelector("tbody");
  const rowMap = new Map();
  [...tbody.children].forEach((tr, i) => rowMap.set(alertKey(rows[i]), tr));
  state.tbody = tbody;
  state.rowMap = rowMap;
}
