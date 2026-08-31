/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Alert triage page (#2710) — the landing page for the link every alert webhook carries. Deep-link only (no
 * sidebar entry): the route is #/triage?server=...&metric=...&at=...&dedup=..., the exact query the delivery
 * channels compute, echoed to GET /api/triage which assembles everything server-side in one response — the
 * matching alert-history row(s), notes about anything it could not resolve, and the alert-type-relevant
 * sections, each the output of the SAME /api/read tool the rest of the dashboard renders. Sections render
 * through the shared VIZ registry with derive.js-inferred field configs (the composer's auto-detect), so any
 * read the server maps in renders here without a hand-authored column list. Empty envelopes and per-section
 * errors render as the standard strips — a page about an incident must degrade loudly, never blankly.
 *
 * R4 (XSS): every value reaches the DOM through el()/textContent — alert detail text, section titles, and
 * every cell go through the shared builders; nothing touches innerHTML.
 */

import { el, mount, apiGet, buildQuery, loadingStrip, errorStrip, emptyStrip, noticeStrip, localTime, fmtNum } from "../util.js";
import { VIZ } from "../panels.js";
import { suggestViz, deriveVizConfig } from "../derive.js";

/* The get_alert_history wire shape's status collapse, matching the Alert History page's own cell. */
function alertStatusText(a) {
  if (a.muted) return "Muted";
  if (a.send_error) return "Delivery failed: " + a.send_error;
  if (a.alert_sent) return "Sent" + (a.notification_type ? " (" + a.notification_type + ")" : "");
  return "Not sent";
}

/* One matched alert-history row as a labeled field list (the lead card's body). */
function alertFields(a) {
  const rows = [
    ["Fired", localTime(a.alert_time)],
    ["Server", a.server_name || "—"],
    ["Metric", a.metric_name || "—"],
    ["Value", fmtNum(a.current_value, 1)],
    ["Threshold", fmtNum(a.threshold_value, 1)],
    ["Status", alertStatusText(a)],
  ];
  const box = el("div", { class: "detail-fields" }, rows.map(([k, v]) =>
    el("div", { class: "detail-field" }, [
      el("span", { class: "fk", text: k }),
      el("span", { class: "fv", text: v }),
    ])
  ));
  if (a.detail_text) {
    return [box, el("pre", { class: "code", text: a.detail_text })];
  }
  return [box];
}

/* One assembled section -> a panel card. `data` carries the tool's own JSON (envelope included); `error` is
 * the tool's bare message or the capture of a thrown section. The derive.js auto-config keeps this generic
 * over every read the server maps, at the cost of default formatting — the same trade the composer's
 * auto-detect makes, and the operator escape hatch is the same: open the read on the server page. */
function sectionCard(section) {
  const body = [];
  if (section.error) {
    body.push(errorStrip(section.error));
  } else if (
    section.data && typeof section.data.status === "string" && typeof section.data.message === "string"
  ) {
    /* The {status, message} empty envelope — the tool's own better sentence for "nothing here". */
    body.push(emptyStrip(section.data.message));
  } else if (section.data) {
    const viz = suggestViz(section.data);
    const cfg = deriveVizConfig(section.data, viz, null);
    const render = VIZ[viz];
    try {
      body.push(render(section.data, cfg));
    } catch (e) {
      body.push(errorStrip("Could not render this section: " + (e && e.message ? e.message : String(e))));
    }
  } else {
    body.push(emptyStrip("No data came back for this section."));
  }

  return el("div", { class: "panel card span-2" }, [
    el("h3", {}, [section.title, el("span", { class: "panel-sub", text: " " + section.read })]),
    el("div", { class: "panel-body" }, body),
  ]);
}

export async function renderTriage(main, queryString) {
  const params = new URLSearchParams(queryString || "");
  const server = params.get("server") || "";
  const metric = params.get("metric") || "";
  const at = params.get("at") || "";
  const dedup = params.get("dedup") || "";

  const box = el("div", {});
  mount(main, [
    el("div", { class: "page-head" }, [
      el("h2", { text: metric ? "Triage — " + metric : "Triage" }),
      el("div", { class: "meta", text: (server || "unknown server") + (at ? " · " + localTime(at) : "") }),
    ]),
    box,
  ]);

  mount(box, loadingStrip("Assembling triage context…"));
  const res = await apiGet("/api/triage" + buildQuery({ server, metric, at, dedup }));
  if (res.kind === "error") return mount(box, errorStrip(res.message));

  const t = res.data || {};
  const out = [];

  for (const note of t.notes || []) {
    out.push(noticeStrip(note));
  }

  if (t.alert) {
    out.push(
      el("div", { class: "panel card span-2" }, [
        el("h3", {}, ["Matched alert"]),
        el("div", { class: "panel-body" }, alertFields(t.alert)),
      ])
    );
  }

  if (Array.isArray(t.related_alerts) && t.related_alerts.length) {
    out.push(
      el("div", { class: "panel card span-2" }, [
        el("h3", {}, [
          "Nearby firings of this metric",
          el("span", { class: "panel-sub", text: " nearest the linked instant first" }),
        ]),
        el("div", { class: "panel-body" }, t.related_alerts.flatMap((a) => alertFields(a))),
      ])
    );
  }

  for (const section of t.sections || []) {
    out.push(sectionCard(section));
  }

  if (dedup) {
    out.push(noticeStrip("Incident fingerprint: " + dedup));
  }

  mount(box, el("div", { class: "grid" }, out));
}
