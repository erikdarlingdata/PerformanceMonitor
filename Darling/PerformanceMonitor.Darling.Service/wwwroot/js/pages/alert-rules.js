/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Custom Alert Rules (#3285, Component 7): the list page — the twin of pages/views.js's renderViewList, for
 * user-authored alert rules instead of custom views. A rule is stored JSON { metric, predicate, hysteresis, scope }
 * behind the /api/alerts CRUD; this page lists the summaries as cards (name, enabled/paused, when it last fired,
 * and — enriched per card from the rule's definition — its metric, condition, and scope), and offers New rule + a
 * "start from a template" menu of the server-authored starter templates.
 *
 * Unlike a custom view, an alert rule has no read-only "rendered" surface — the card links straight to the editor
 * (alert-editor.js). Edit affordances (New / New from template) show only when the session reports can_edit (the
 * server is the authority for every write); the list itself is open to every seat. All user text (names,
 * descriptions, server names) reaches the DOM through el()/textContent (R4 — never innerHTML).
 */

import { el, mount, loadingStrip, errorStrip, emptyStrip, relTime } from "../util.js";
import * as api from "../alerts-api.js";

/** The scalar operator's human symbol for a condition chip (mirrors CustomAlertRuleDefinition.OpSymbol); the range
 *  ops (between/outside, #3351) render their band instead — see describeCondition. */
const OP_SYMBOLS = { gt: ">", ge: "≥", lt: "<", le: "≤" };

/* ─────────────────────────── list page ─────────────────────────── */

export async function renderAlertRuleList(main) {
  mount(main, [listHead(false, []), loadingStrip("Loading alert rules…")]);

  /* The templates read rides along with the session + list; a failed templates read is not fatal (the New-from-
     template menu just says it is unavailable — every other affordance still works). */
  const [session, res, templates] = await Promise.all([api.getSession(), api.listAlertRules(), loadTemplates()]);
  const canEdit = !!session.can_edit;

  if (res.kind === "error") {
    mount(main, [listHead(canEdit, templates), errorStrip(res.message)]);
    return;
  }

  const rules = Array.isArray(res.data) ? res.data : [];
  const nodes = [listHead(canEdit, templates)];

  /* Empty list -> a real first-run hero that sells the feature, not a bare strip. */
  if (!rules.length) {
    nodes.push(firstRunHero(canEdit, templates));
    mount(main, nodes);
    return;
  }

  nodes.push(el("div", { class: "view-cards" }, rules.map((r) => ruleCard(r))));
  mount(main, nodes);
}

/** The starter templates for the "New from template" menu, or [] on any failure (every caller handles empty). */
async function loadTemplates() {
  const res = await api.listAlertTemplates();
  if (res.kind !== "data" || !res.data || !Array.isArray(res.data.templates)) return [];
  return res.data.templates;
}

function listHead(canEdit, templates) {
  return el("div", { class: "page-head" }, [
    el("h2", { text: "Alert Rules" }),
    el("div", { class: "spacer" }),
    canEdit ? templateMenu(templates) : null,
    canEdit ? el("a", { class: "btn primary", href: "#/alert-rule/new", text: "New rule" }) : null,
  ]);
}

/* First-run hero: a centered pitch + a prominent New rule + the template menu. A read-only seat can't author, so
   it gets an explanatory variant (no New rule / templates). */
function firstRunHero(canEdit, templates) {
  if (!canEdit) {
    return el("div", { class: "views-hero" }, [
      el("div", { class: "hero-title", text: "No alert rules yet" }),
      el("div", {
        class: "hero-pitch",
        text:
          "Your session couldn't be confirmed, so authoring is read-only right now — reload the page to create a " +
          "rule. Once created, alert rules evaluate on every collection and deliver to your configured channels.",
      }),
    ]);
  }
  return el("div", { class: "views-hero" }, [
    el("div", { class: "hero-title", text: "Alert on any metric you can chart" }),
    el("div", {
      class: "hero-pitch",
      text:
        "Pick a metric, aggregate it over a recent window, and fire a Warning or Critical when it crosses a " +
        "threshold you set — with hysteresis so a single spike doesn't page, and a scope of all servers or a chosen " +
        "few. Start from a conservative template and tune it, or build one from scratch. No SQL to write.",
    }),
    el("div", { class: "hero-ctas" }, [
      el("a", { class: "btn primary hero-cta", href: "#/alert-rule/new", text: "New rule" }),
      templateMenu(templates),
    ]),
  ]);
}

/* The "New from template" menu: a <details> dropdown of the server-authored starter templates (#3325). Each links
   to the editor pre-filled from that template's definition, because the starters are deliberately conservative and
   want tuning before they are saved (unlike a dashboard template, which is created as-is). Built with
   el()/textContent like everything else. */
function templateMenu(templates) {
  if (!templates.length) {
    return el("details", { class: "template-menu" }, [
      el("summary", { class: "btn template-summary", text: "New from template ▾" }),
      el("div", { class: "template-list" }, [el("div", { class: "template-note", text: "Starter templates are unavailable right now." })]),
    ]);
  }
  const items = templates.map((t) =>
    el("a", { class: "template-item", href: "#/alert-rule/new/" + encodeURIComponent(t.key), title: t.description }, [
      el("span", { class: "template-label", text: t.name }),
      el("span", { class: "template-desc", text: t.description }),
    ])
  );
  return el("details", { class: "template-menu" }, [
    el("summary", { class: "btn template-summary", text: "New from template ▾" }),
    el("div", { class: "template-list" }, items),
  ]);
}

/* One rule -> a card linking to its editor. The summary carries name + enabled + last_fired synchronously; the
   metric / condition / scope chips are filled in progressively from the rule's own definition (enrichCard), so the
   card renders immediately and never blocks on that fetch. */
function ruleCard(r) {
  const meta = "v" + r.version + " · updated " + relTime(r.updated_at) + (r.updated_by ? " by " + r.updated_by : "");
  /* last_fired (#3360): the summary correlates the rule's fires (config_alert_log 'Custom:<id>' rows, resolves
     excluded) into one field. Non-null -> a relative time via the shared relTime helper; null -> "never fired". */
  const fired = r.last_fired ? "Last fired " + relTime(r.last_fired) : "Never fired";
  const chips = el("div", { class: "vc-chips" });
  const card = el("a", { class: "view-card card", href: "#/alert-rule/" + encodeURIComponent(r.id) }, [
    el("div", { class: "vc-name" }, [
      el("span", { class: "vc-name-text", text: r.name }),
      r.enabled === false ? el("span", { class: "paused-badge", text: "Paused" }) : null,
    ]),
    r.description ? el("div", { class: "vc-desc", text: r.description }) : null,
    chips,
    el("div", { class: "vc-fired" + (r.last_fired ? "" : " never"), text: fired }),
    el("div", { class: "vc-meta", text: meta }),
  ]);
  enrichCard(r.id, chips);
  return card;
}

/* Fill a card's chips from the rule's full definition (metric / condition / scope) — fetched per card and filled in
   progressively; a failed/empty fetch just leaves the (display:none-when-empty) chip row absent. */
async function enrichCard(id, chips) {
  const res = await api.getAlertRule(id);
  if (res.kind !== "data" || !res.data) return;
  const def = res.data.definition || {};
  const nodes = [];

  const metricChip = describeMetric(def.metric);
  if (metricChip) nodes.push(el("span", { class: "vc-chip", text: metricChip }));

  const condChip = describeCondition(def.predicate);
  if (condChip) nodes.push(el("span", { class: "vc-chip count", text: condChip }));

  nodes.push(el("span", { class: "vc-chip", text: describeScope(def.scope) }));

  mount(chips, nodes);
}

/** A compact metric label from a definition's metric ("max cpu_utilization" / a ratio's key), or null. */
function describeMetric(metric) {
  if (!metric || typeof metric !== "object") return null;
  const name = metric.ratio || metric.measure;
  if (!name) return null;
  const agg = metric.ratio ? "" : metric.aggregate ? metric.aggregate + " " : "";
  return agg + name;
}

/** A compact condition label — a scalar bar ("≥ 30000 / crit 120000") or a range band ("outside 10–100", or
 *  "outside 10–100 (crit outside 5–200)" / "between 10–100 (crit 40–60)" with a critical tier), or null. */
function describeCondition(pred) {
  if (!pred || typeof pred !== "object") return null;

  /* Range op (#3351): a two-sided band, rendered "outside 10–100" / "between 10–100" with an en dash (–)
     between the bounds — a range predicate carries lowerBound/upperBound and no warnThreshold. An optional
     critical band (#3372) appends " (crit ...)" so the chip conveys both tiers: the crit band is the more-severe
     extension (wider for 'outside', narrower for 'between'), the word repeated only for 'outside' to read clearly. */
  if (pred.op === "between" || pred.op === "outside") {
    if (pred.lowerBound == null || pred.upperBound == null) return null;
    let band = pred.op + " " + pred.lowerBound + "–" + pred.upperBound;
    if (pred.criticalLowerBound != null && pred.criticalUpperBound != null) {
      const critWord = pred.op === "outside" ? "outside " : "";
      band += " (crit " + critWord + pred.criticalLowerBound + "–" + pred.criticalUpperBound + ")";
    }
    return band;
  }

  if (pred.warnThreshold == null) return null;
  const sym = OP_SYMBOLS[pred.op] || pred.op || "?";
  let text = sym + " " + pred.warnThreshold;
  if (pred.criticalThreshold != null) text += " / crit " + pred.criticalThreshold;
  return text;
}

/** A compact scope label ("All servers" / "3 servers"). */
function describeScope(scope) {
  if (scope && scope.mode === "servers" && Array.isArray(scope.servers)) {
    const n = scope.servers.length;
    return n === 1 ? "1 server" : n + " servers";
  }
  return "All servers";
}
