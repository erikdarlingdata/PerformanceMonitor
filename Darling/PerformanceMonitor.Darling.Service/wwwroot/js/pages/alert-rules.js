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

import { el, mount, loadingStrip, errorStrip, emptyStrip, relTime, storeHash } from "../util.js";
import { holdsPage, keepScroll, showPage, showRefreshError } from "../refresh-in-place.js";
import * as api from "../alerts-api.js";

/** The scalar operator's human symbol for a condition chip (mirrors CustomAlertRuleDefinition.OpSymbol); the range
 *  ops (between/outside, #3351) render their band instead — see describeCondition. */
const OP_SYMBOLS = { gt: ">", ge: "≥", lt: "<", le: "≤" };

/* #7927: the key the list page is held under (refresh-in-place.js), so the 60 s poll can tell that the list on screen is its own. */
const LIST_KEY = "alert-rules";

/* #7927: each card's chips by rule id, with the version they were read at. The metric, condition and scope come from the rule's full
   definition, which the list payload does not carry, so a card whose version is unchanged draws its chips from here instead of
   reading the rule again on every poll. A poll over N unchanged rules costs the list and the templates reads only. */
const chipCache = new Map();

/* ─────────────────────────── list page ─────────────────────────── */

export async function renderAlertRuleList(main, opts) {
  /* #7927: the poll over the list on screen leaves it there while the list is read again, so an open "New from template" menu stays
     open; a "Loading alert rules…" strip in its place would close it. When the new list arrives only the cards are redrawn. */
  const inPlace = holdsPage(main, LIST_KEY, !!(opts && opts.poll === true));
  if (!inPlace) mount(main, [listHead(false, []), loadingStrip("Loading alert rules…")]);

  /* The templates read rides along with the session + list; a failed templates read is not fatal (the New-from-
     template menu just says it is unavailable — every other affordance still works). */
  const [session, res, templates] = await Promise.all([api.getSession(), api.listAlertRules(), loadTemplates()]);
  const canEdit = !!session.can_edit;

  /* The page is kept through the poll only while it shows the hero or the cards. A first paint that failed left an error page under the
     key, and that is drawn again: there is no earlier list to keep under a refresh notice, and an empty list must draw the hero. */
  const held = inPlace && holdsPage(main, LIST_KEY, true) && showsRules(main);

  if (res.kind === "error") {
    if (held && showRefreshError(main, LIST_KEY, res.message)) return;
    showPage(main, LIST_KEY, [listHead(canEdit, templates), errorStrip(res.message)]);
    return;
  }

  const rules = Array.isArray(res.data) ? res.data : [];
  /* The affordances follow the session: a seat whose can_edit differs from what the page was drawn for (a probe that failed at the first
     paint and has since answered, #7928) gets the page drawn again. Only the cards are swapped otherwise, and the templates answer goes
     into the menus on screen. */
  if (held && editShown(main) === canEdit && swapCards(main, rules)) {
    refreshTemplateMenus(main, templates);
    return;
  }

  const nodes = [listHead(canEdit, templates)];

  /* Empty list -> a real first-run hero that sells the feature, not a bare strip. */
  if (!rules.length) {
    nodes.push(firstRunHero(canEdit, templates));
    showPage(main, LIST_KEY, nodes);
    return;
  }

  nodes.push(el("div", { class: "view-cards" }, rules.map((r) => ruleCard(r))));
  showPage(main, LIST_KEY, nodes);
}

/* #7927: the poll's redraw of a list that is on screen. The cards are replaced in one step, with the reader's scroll position kept,
   and the head with its template menu stays as it is; an earlier poll's failure notice comes down. True only when the page on screen
   still fits the list: the cards with rules to draw into them, or the hero over a store that still has no rules, which stays as it is.
   False otherwise, and the caller draws the whole page: the hero when rules now exist, a list whose rules are all gone, and an error
   page, which shows neither. */
function swapCards(main, rules) {
  const children = () => Array.from(main.children || []);
  const live = children().find((n) => hasClass(n, "view-cards"));
  if (!live) return !rules.length && children().some((n) => hasClass(n, "views-hero"));
  if (!rules.length) return false;
  keepScroll(() => {
    const stale = children().find((n) => hasClass(n, "refresh-error"));
    if (stale) main.removeChild(stale);
    main.replaceChild(el("div", { class: "view-cards" }, rules.map((r) => ruleCard(r))), live);
  });
  return true;
}

/* True while the page shows the hero or the cards, the two pages a poll keeps on screen. An error page is neither. */
function showsRules(main) {
  return Array.from(main.children || []).some((n) => hasClass(n, "view-cards") || hasClass(n, "views-hero"));
}

/* The can_edit the page on screen was drawn for. The template menu is drawn only for a seat that can edit, in the head and in the hero
   alike, so its presence says which variant is up. */
function editShown(main) {
  return findAll(main, (n) => hasClass(n, "template-menu")).length > 0;
}

/* #7927: the templates the menu on screen was drawn from, by menu element, so a poll can tell an unchanged answer from a changed one. */
const menuTemplates = new WeakMap();

function templatesKey(templates) {
  return templates.map((t) => [t.key, t.name, t.description].join("\u0001")).join("\u0002");
}

/* #7927: the poll's templates answer, put into the menus on screen: the head's, and over the hero its own. The <details> keeps its
   element, so an open menu stays open; only its items follow the templates, and only when they changed. An empty answer (no templates,
   or a failed read) leaves the menus as they are. */
function refreshTemplateMenus(main, templates) {
  if (!templates.length) return;
  const key = templatesKey(templates);
  for (const menu of findAll(main, (n) => hasClass(n, "template-menu"))) {
    if (menuTemplates.get(menu) === key) continue;
    const list = findAll(menu, (n) => hasClass(n, "template-list"))[0];
    if (!list) continue;
    mount(list, Array.from(findAll(templateMenu(templates), (n) => hasClass(n, "template-list"))[0].children));
    menuTemplates.set(menu, key);
  }
}

function hasClass(n, name) {
  return typeof n.className === "string" && (" " + n.className + " ").includes(" " + name + " ");
}

function findAll(node, pred, out = []) {
  if (pred(node)) out.push(node);
  for (const c of Array.from(node.children || [])) findAll(c, pred, out);
  return out;
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
    canEdit ? el("a", { class: "btn primary", href: storeHash("#/alert-rule/new"), text: "New rule" }) : null,
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
          "Your session could not be confirmed yet, so authoring is not available — reload the page to try again. Once " +
          "a rule is created, alert rules evaluate on every collection and deliver to your configured channels.",
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
      el("a", { class: "btn primary hero-cta", href: storeHash("#/alert-rule/new"), text: "New rule" }),
      templateMenu(templates),
    ]),
  ]);
}

/* The "New from template" menu: a <details> dropdown of the server-authored starter templates (#3325). Each links
   to the editor pre-filled from that template's definition, because the starters are deliberately conservative and
   want tuning before they are saved (unlike a dashboard template, which is created as-is). Built with
   el()/textContent like everything else. */
function templateMenu(templates) {
  const items = templates.length
    ? templates.map((t) =>
        el("a", { class: "template-item", href: storeHash("#/alert-rule/new/") + encodeURIComponent(t.key), title: t.description }, [
          el("span", { class: "template-label", text: t.name }),
          el("span", { class: "template-desc", text: t.description }),
        ])
      )
    : [el("div", { class: "template-note", text: "Starter templates are unavailable right now." })];
  const menu = el("details", { class: "template-menu" }, [
    el("summary", { class: "btn template-summary", text: "New from template ▾" }),
    el("div", { class: "template-list" }, items),
  ]);
  menuTemplates.set(menu, templatesKey(templates));
  return menu;
}

/* One rule -> a card linking to its editor. The summary carries name + enabled + last_fired synchronously; the
   metric / condition / scope chips are filled in progressively from the rule's own definition (enrichCard), so the
   card renders immediately and never blocks on that fetch. */
function ruleCard(r) {
  const meta = "v" + r.version + " · updated " + relTime(r.updated_at) + (r.updated_by ? " by " + r.updated_by : "");
  /* #6235: an expired rule is not evaluated; a rule with a future expiry says when it ends. */
  const expiry = r.expires_at_utc
    ? (r.expired ? "Expired " : "Expires ") + String(r.expires_at_utc).slice(0, 16).replace("T", " ") + " UTC"
    : null;
  /* last_fired (#3360): the summary correlates the rule's fires (config_alert_log 'Custom:<id>' rows, resolves
     excluded) into one field. Non-null -> a relative time via the shared relTime helper; null -> "never fired". */
  const fired = r.last_fired ? "Last fired " + relTime(r.last_fired) : "Never fired";
  const chips = el("div", { class: "vc-chips" });
  const card = el("a", { class: "view-card card", href: storeHash("#/alert-rule/") + encodeURIComponent(r.id) }, [
    el("div", { class: "vc-name" }, [
      el("span", { class: "vc-name-text", text: r.name }),
      r.enabled === false ? el("span", { class: "paused-badge", text: "Paused" }) : null,
      r.expired ? el("span", { class: "paused-badge", text: "Expired" }) : null,
    ]),
    r.description ? el("div", { class: "vc-desc", text: r.description }) : null,
    chips,
    el("div", { class: "vc-fired" + (r.last_fired ? "" : " never"), text: fired }),
    expiry ? el("div", { class: "vc-meta", text: expiry }) : null,
    el("div", { class: "vc-meta", text: meta }),
  ]);
  enrichCard(r.id, r.version, chips);
  return card;
}

/* Fill a card's chips from the rule's full definition (metric / condition / scope) — fetched per card and filled in
   progressively; a failed/empty fetch just leaves the (display:none-when-empty) chip row absent. The chips a read gave
   are kept by rule id and version (#7927), so a card whose version the list still reports draws them without a read. */
async function enrichCard(id, version, chips) {
  const kept = chipCache.get(String(id));
  if (kept && kept.version === version) {
    mount(chips, kept.chips.map((c) => el("span", { class: c.cls, text: c.text })));
    return;
  }
  const res = await api.getAlertRule(id);
  if (res.kind !== "data" || !res.data) return;
  const def = res.data.definition || {};
  const texts = [];

  const metricChip = describeMetric(def.metric);
  if (metricChip) texts.push({ cls: "vc-chip", text: metricChip });

  const condChip = describeCondition(def.predicate);
  if (condChip) texts.push({ cls: "vc-chip count", text: condChip });

  texts.push({ cls: "vc-chip", text: describeScope(def.scope) });

  chipCache.set(String(id), { version, chips: texts });
  mount(chips, texts.map((c) => el("span", { class: c.cls, text: c.text })));
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
