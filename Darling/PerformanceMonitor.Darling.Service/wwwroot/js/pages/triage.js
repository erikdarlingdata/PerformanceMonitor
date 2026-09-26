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

import { el, mount, apiGet, buildQuery, loadingStrip, errorStrip, emptyStrip, noticeStrip, localTime, fmtNum,
         alertDeliveryState } from "../util.js";
import { VIZ } from "../panels.js";
import { suggestViz, deriveVizConfig } from "../derive.js";
import { renderNotebookDoc } from "./views.js";
import { getSession, getCatalog } from "../views-api.js";

/* The get_alert_history wire shape's status collapse, matching the Alert History page's own cell — including
   its #2781 rule: the "tray" channel (the Lite/Dashboard system-tray toast) is meaningless on the headless web,
   so it is dropped rather than shown; a real channel still renders. The tray rule here is kept in lockstep
   with statusCell in alerts.js (the two collapse the status slightly differently otherwise). */
function alertStatusText(a) {
  if (a.muted) return "Muted";
  if (a.send_error) return "Delivery failed: " + a.send_error;
  /* #3169: a row that states a delivery STATE rather than naming a channel, including the one legacy
     signature that decodes. Same shared lookup statusCell in alerts.js uses, which is what keeps the two
     in lockstep now instead of a comment asking them to be. */
  const state = alertDeliveryState(a);
  if (state !== null) return state;
  const channel = a.notification_type ? " (" + a.notification_type + ")" : "";
  if (a.alert_sent) return "Delivered" + channel;
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

/* #4222: the alert notebook render. GET /api/alert-notebook returns {alert, status, notes, template, definition}
   bound to this link's server/metric/at/dedup — a per-family template server-side, so the page can render the
   forensic reads an investigation needs (v1 read cells) as a document, read-only, no fleet scope picker (the
   server is fixed). /api/triage is NOT called first: /api/alert-notebook is tried, and only on a 404 (it isn't on
   dev yet, or this build predates it) does the page fall back to the legacy /api/triage assembly below — so the
   route works whichever endpoint is live. Status refresh on the 60s poll is OUT of scope here: it needs the poll
   guard #4222(d) adds to app.js's refresh(), which is not in this slice. */
async function renderAlertNotebook(main, box, server, metric, at, dedup) {
  const res = await apiGet("/api/alert-notebook" + buildQuery({ server, metric, at, dedup }));
  if (res.kind === "error" && res.status === 404) return false; // not on this build — caller falls back
  if (res.kind === "error") {
    mount(box, errorStrip(res.message));
    return true;
  }
  const t = res.data || {};
  const notes = Array.isArray(t.notes) ? t.notes : [];
  const [session, catalog] = await Promise.all([getSession(), getCatalog()]);
  const canEdit = !!session.can_edit;

  let def = t.definition || { kind: "notebook", cells: [] };
  const provenance =
    "from alert template " + (t.template && t.template.id) + " v" + (t.template && t.template.version) +
    ", " + metric + " on " + server + " at " + at;

  function stripAsOf(d) {
    const cells = Array.isArray(d.cells) ? d.cells : [];
    return {
      ...d,
      cells: cells.map((c) => {
        if (c && c.type === "read" && c.params && c.params.as_of != null) {
          const { as_of, ...rest } = c.params;
          return { ...c, params: rest };
        }
        return c;
      }),
    };
  }

  const noteBox = el("div", {});
  const docHolder = el("div", {});
  mount(box, [noteBox, docHolder]);

  function paint() {
    mount(noteBox, notes.map((n) => noticeStrip(n)));
    renderNotebookDoc(docHolder, {
      mode: "alert",
      definition: def,
      alert: t.alert || null,
      status: t.status,
      notes,
      canEdit,
      catalog,
      provenance,
      onOpenLive: () => { def = stripAsOf(def); paint(); },
    });
  }
  paint();
  return true;
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

  const handled = await renderAlertNotebook(main, box, server, metric, at, dedup);
  if (handled) return;

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
