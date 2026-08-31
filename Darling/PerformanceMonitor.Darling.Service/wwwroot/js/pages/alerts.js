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
 */

import { el, mount, readTool, buildQuery, loadingStrip, errorStrip, emptyStrip, disclosure } from "../util.js";
import { VIZ } from "../panels.js";

const ALERT_COLUMNS = [
  { key: "alert_time", label: "Time", format: "time" },
  { key: "server_name", label: "Server" },
  { key: "metric_name", label: "Metric" },
  { key: "current_value", label: "Value", format: "num1" },
  { key: "threshold_value", label: "Threshold", format: "num1" },
  { key: "status", label: "Status", render: (a) => statusCell(a) },
  { key: "detail_text", label: "Detail", render: (a) => detailCell(a) },
  { key: "triage", label: "Triage", render: (a) => triageCell(a) },
];

/* Deep-link into the #2710 triage page for this row — the SAME route the alert webhooks link to, anchored at
 * this row's own firing instant, so the in-app path and the delivered link land on an identical page. */
function triageCell(a) {
  return el("a", {
    href: "#/triage" + buildQuery({ server: a.server_name, metric: a.metric_name, at: a.alert_time }),
    text: "Open",
  });
}

/* Channel (always tray) + Sent + Muted + Delivery Error collapse into one glyph+text status cell (B2). */
function statusCell(a) {
  let glyph, label, sev;
  if (a.muted) {
    glyph = "⊘";
    label = "Muted";
    sev = "Warning";
  } else if (a.send_error) {
    glyph = "✕";
    label = "Delivery failed";
    sev = "Critical";
  } else if (a.alert_sent) {
    glyph = "✓";
    label = "Sent";
    sev = "Healthy";
  } else {
    glyph = "•";
    label = "Not sent";
    sev = "Unknown";
  }
  return el("span", { class: "status-cell sev-" + sev, title: a.send_error || null }, [
    el("span", { class: "glyph", text: glyph }),
    el("span", { text: label }),
    a.notification_type ? el("span", { class: "channel", text: a.notification_type }) : null,
  ]);
}

/* The Detail cell (B2): a truncated one-liner that expands to the full payload. Structured finding detail_text
 * (indented "Story:/Severity:/Confidence:/..." lines) renders as labeled fields; anything else renders as a
 * plain text block. A delivery error is appended as its own labeled field so it is fully readable in the
 * expansion, not merely a hover title on the status glyph. */
function detailCell(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  if (!hasDetail && !a.send_error) return el("span", { class: "muted", text: "—" });

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

  const summary = hasDetail ? a.detail_text : "Delivery error: " + a.send_error;
  return disclosure(summary, body, { max: 120 });
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

export async function renderAlerts(main) {
  const filter = el("input", {
    class: "filter-box",
    type: "text",
    placeholder: "Filter by server…",
    "aria-label": "Filter alerts by server",
  });

  const tableBox = el("div", {});
  mount(main, [
    el("div", { class: "page-head" }, [
      el("h2", { text: "Alert History" }),
      el("div", { class: "meta", text: "fleet-wide · last 24h" }),
      el("div", { class: "spacer" }),
      filter,
    ]),
    tableBox,
  ]);

  mount(tableBox, loadingStrip("Loading alerts…"));
  const res = await readTool("get_alert_history", { hours_back: 24, limit: 200 });
  if (res.kind === "error") return mount(tableBox, errorStrip(res.message));
  if (res.kind === "empty") return mount(tableBox, emptyStrip(res.message));

  const alerts = res.data.alerts || [];

  const draw = () => {
    const q = filter.value.trim().toLowerCase();
    const rows = q ? alerts.filter((a) => String(a.server_name || "").toLowerCase().includes(q)) : alerts;
    if (!rows.length) {
      mount(tableBox, emptyStrip(q ? 'No alerts match "' + filter.value + '".' : "No alerts in this window."));
      return;
    }
    mount(tableBox, VIZ.table({ alerts: rows }, { rowsKey: "alerts", columns: ALERT_COLUMNS }));
  };

  filter.addEventListener("input", draw);
  draw();
}
