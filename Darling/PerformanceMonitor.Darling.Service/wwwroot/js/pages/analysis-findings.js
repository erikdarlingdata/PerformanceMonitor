/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The Recommendations server tab (#4843) — the web twin of the desktop viewer's analysis Recommendations view
 * (not the FinOps Recommendations tab). One read, get_analysis_findings, for the server the page is on: the
 * persisted findings grouped by incident (incident_id; a finding with none is its own group), each group headed
 * by its highest-severity finding, each card showing a severity badge, the advice text, the fix script in a
 * <pre> with a Copy button, and, for an incident finding, a link to that server's Queries tab.
 *
 * Advise-only: nothing here runs a fix, mutes, or asks the service for a fresh analysis.
 *
 * Which findings are "incidents" follows the desktop: a finding that carries a structured_remediation is a
 * standing config fix and gets no Active Queries link; every other finding with a time_range does. The link
 * opens the server's Queries tab, which has no from/to deep link on the web, so it carries the server only.
 *
 * State: which groups the reader has opened or closed lives in module scope (`openState`, keyed by server then
 * by group key), because the server page rebuilds this tab on every 60 s poll and on every Range change.
 *
 * R4 (XSS): every value, the fix script included, reaches the DOM through el()'s text path.
 */

import { el, readToolWithinKeptHistory, keptWindowStrip, noticeStrip, readErrorStrip, emptyStrip, loadingStrip, localTime, bandClass, dbScopeChip } from "../util.js";

/** The desktop's severity cutoffs: >= 1.5 Critical, >= 0.75 Warning, else Info. */
export function severityLabel(severity) {
  const s = Number(severity);
  if (s >= 1.5) return "Critical";
  if (s >= 0.75) return "Warning";
  return "Info";
}

/** Info has no band of its own in the colour vocabulary, so it takes the neutral one. */
function badgeClass(label) {
  return "badge " + bandClass(label === "Info" ? "Unknown" : label);
}

/** An incident finding: not a standing config fix (the read's is_config_fix, the desktop's rule; force-plan stays an incident), with a time window to point at. */
export function showsActiveQueriesLink(f) {
  return !f.is_config_fix && !!(f.time_range && f.time_range.start && f.time_range.end);
}

/**
 * The findings as incident groups, most severe group first. A finding with no incident_id is a group of its own.
 * Within a group the order the read gave (still-firing first, then severity) is kept after a severity sort.
 */
export function groupFindings(findings) {
  const order = [];
  const buckets = new Map();
  let solo = 0;
  for (const f of findings || []) {
    const key = f.incident_id ? "i:" + f.incident_id : "s:" + solo++;
    if (!buckets.has(key)) {
      buckets.set(key, []);
      order.push(key);
    }
    buckets.get(key).push(f);
  }
  const groups = order.map((key) => {
    const items = buckets.get(key).slice().sort((a, b) => (b.severity || 0) - (a.severity || 0));
    const primary = items[0];
    return { key, items, primary, label: severityLabel(primary.severity) };
  });
  return groups.map((g, i) => ({ g, i })).sort((a, b) => (b.g.primary.severity || 0) - (a.g.primary.severity || 0) || a.i - b.i).map((x) => x.g);
}

/** The group header: primary title, finding count when more than one, severity label. */
export function groupHeader(group) {
  const title = findingTitle(group.primary);
  const n = group.items.length;
  return title + " · " + (n > 1 ? n + " findings · " : "") + group.label.toUpperCase();
}

function findingTitle(f) {
  return (f.advice && f.advice.headline) || f.story_path || f.category || "Finding";
}

/* ─────────────────────────── state ─────────────────────────── */

/** server -> Map(group key -> open?). A group never touched takes its default: open unless Info-only. */
const openState = new Map();

function isOpen(server, group) {
  const m = openState.get(server);
  return m && m.has(group.key) ? m.get(group.key) : group.label !== "Info";
}

function setOpen(server, key, open) {
  if (!openState.has(server)) openState.set(server, new Map());
  openState.get(server).set(key, open);
}

/* ─────────────────────────── copy ─────────────────────────── */

/** Copies text with the async clipboard API, falling back to a hidden textarea. Resolves true when it copied. */
export async function copyText(text) {
  try {
    if (typeof navigator !== "undefined" && navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the textarea path */
  }
  try {
    const ta = el("textarea", { style: "position:fixed;left:-1000px;top:0", readonly: "readonly" });
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    const ok = typeof document.execCommand === "function" ? document.execCommand("copy") : false;
    document.body.removeChild(ta);
    return !!ok;
  } catch {
    return false;
  }
}

function copyButton(text) {
  const btn = el("button", { class: "btn small", type: "button", text: "Copy fix" });
  btn.addEventListener("click", async () => {
    const ok = await copyText(text);
    btn.textContent = ok ? "Copied" : "Copy failed";
  });
  return btn;
}

/* ─────────────────────────── rendering ─────────────────────────── */

/** The score after the label, or nothing when the row carries no numeric severity. */
function severityText(severity) {
  const n = severity == null || severity === "" ? NaN : Number(severity);
  return Number.isFinite(n) ? " " + n.toFixed(2) : "";
}

function card(server, f) {
  const label = severityLabel(f.severity);
  const advice = f.advice || {};
  const kids = [
    el("div", { class: "reco-head" }, [
      el("span", { class: badgeClass(label), text: label.toUpperCase() + severityText(f.severity) }),
      f.category ? el("span", { class: "muted", text: " " + f.category }) : null,
    ]),
    el("h4", { text: findingTitle(f) }),
    advice.investigation ? el("p", { class: "reco-advice", text: advice.investigation }) : null,
    advice.remediation ? el("p", { class: "reco-advice", text: advice.remediation }) : null,
    el("div", { class: "muted", text: "Seen " + f.occurrences + "× · last " + localTime(f.last_seen) + (f.recurring_at_this_hour ? " · recurs at this hour" : "") }),
  ];
  if (f.remediation_command) {
    kids.push(el("pre", { class: "reco-fix", text: f.remediation_command }));
  }
  const actions = [];
  if (f.remediation_command) actions.push(copyButton(f.remediation_command));
  if (showsActiveQueriesLink(f)) {
    actions.push(el("a", { class: "btn small", href: "#/server/" + encodeURIComponent(server) + "/queries", text: "Open in Active Queries" }));
  }
  if (actions.length) kids.push(el("div", { class: "reco-actions" }, actions));
  return el("div", { class: "card reco-card" }, kids);
}

function groupNode(server, group) {
  const details = el("details", { class: "reco-group" }, [
    el("summary", { class: "disc-summary" }, [groupHeader(group)]),
    el("div", { class: "disc-body" }, group.items.map((f) => card(server, f))),
  ]);
  if (isOpen(server, group)) details.setAttribute("open", "");
  details.addEventListener("toggle", () => setOpen(server, group.key, !!details.open));
  return details;
}

/** The nodes for one read result (kept apart from the fetch so a test can drive it). */
export function renderFindings(server, res) {
  if (res.kind === "error") return [readErrorStrip(res.message)];
  if (res.kind === "empty") return [keptWindowStrip(res), emptyStrip(res.message)];
  if (res.kind !== "data") return [];
  const data = res.data || {};
  const groups = groupFindings(data.findings);
  if (!groups.length) return [keptWindowStrip(res), emptyStrip("No findings in this window.")];
  return [
    keptWindowStrip(res),
    typeof data.truncation_note === "string" && data.truncation_note ? noticeStrip(data.truncation_note) : null,
    typeof data.findings_truncated_note === "string" && data.findings_truncated_note ? noticeStrip(data.findings_truncated_note) : null,
    ...groups.map((g) => groupNode(server, g)),
  ];
}

/** The Recommendations tab, in the shape server-tabs.js registers. */
export const analysisFindingsTab = {
  id: "recommendations",
  label: "Recommendations",
  build: (server, ctx) => {
    const body = el("div", { class: "panel-body" }, [loadingStrip()]);
    (async () => {
      const res = await readToolWithinKeptHistory("get_analysis_findings", { server, hours: ctx.hours, limit: 50, full_text: true }, ctx && ctx.signal);
      if (res.kind === "aborted") return;
      while (body.firstChild) body.removeChild(body.firstChild);
      for (const n of renderFindings(server, res)) if (n) body.appendChild(n);
    })();
    return [el("div", { class: "panel card span-2" }, [el("h3", {}, ["Recommendations", el("span", { class: "panel-sub", text: " " + ctx.label }), dbScopeChip("get_analysis_findings")]), body])];
  },
};
