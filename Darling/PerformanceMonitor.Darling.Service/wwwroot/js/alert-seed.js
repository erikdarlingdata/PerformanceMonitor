/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The "Create alert from this panel" handoff (#3285, Component 7) — a LEAF module so BOTH the dashboard composer
 * (editor.js) and the notebook composer (notebook.js) can offer the action, and the alert editor (alert-editor.js)
 * can pick it up, WITHOUT an import cycle. This module imports only util.js: editor.js and notebook.js import the
 * action builder here (they pass their own composedPanelToDesc in, so this module never imports editor.js), and
 * alert-editor.js imports the pending-seed getter here — a clean DAG, no editor.js <-> alert-editor.js cycle.
 *
 * The seed is a COPY, never a live binding (the plan's requirement): the button snapshots the panel's metric spec
 * into module state and navigates to #/alert-rule/new, where the editor consumes it once and clears it. Editing
 * the source panel afterwards does not touch the drafted rule, and editing the rule does not touch the panel.
 */

import { el } from "./util.js";

/* The one-shot handoff slot: a panel's metric spec waiting for the next #/alert-rule/new render to consume. */
let _pendingSeed = null;

/** Stash a metric seed ({metric, name}) for the next new-alert-rule editor to pre-fill from. */
export function setPendingAlertSeed(seed) {
  _pendingSeed = seed;
}

/** Take (and clear) the pending metric seed, or null when none is waiting. Consumed once by the editor. */
export function takePendingAlertSeed() {
  const seed = _pendingSeed;
  _pendingSeed = null;
  return seed;
}

/**
 * Extract the alert-metric keys from a stored composed-panel descriptor — ONLY the keys the compose authority
 * (ComposeSpec.TryParsePanel) reads, so nothing frontend-only (title / span / viz) rides along into a rule
 * definition (the backend rejects unknown keys, and supplies its own scalar viz). Deliberately omits the window:
 * a dashboard panel's chart window is not an alert window, so the target editor supplies `hours` itself. Shared
 * by the create-from-panel handoff and the alert editor's own serialize.
 */
export function metricSpecFromDesc(desc) {
  const d = desc || {};
  const metric = { source: d.source };
  if (d.ratio) metric.ratio = d.ratio;
  else if (d.measure) metric.measure = d.measure;
  if (d.aggregate) metric.aggregate = d.aggregate;
  if (d.unit) metric.unit = d.unit;
  if (Array.isArray(d.filters) && d.filters.length) metric.filters = d.filters;
  return metric;
}

/**
 * Whether a composed-panel model is a legal alert metric source: a SCALAR panel (no timeBucket/topN) that names a
 * measure or ratio. An alert metric is a single value, so only a scalar panel can seed one — a timeseries/ranked
 * panel would carry a shape the alert editor can't use. Mirrors the CustomAlertRuleDefinition Scalar rule.
 */
function isScalarMetricPanel(p) {
  return !!p && p.shape === "scalar" && !!(p.measure || p.ratio);
}

/**
 * The "Create alert from this panel" action row for a composed-panel editor — a button that copies THIS panel's
 * metric into a new alert-rule draft and opens the editor. Returns null unless the panel is a scalar metric (so a
 * timeseries/ranked panel shows nothing), which is why it is rebuilt through buildComposedPanelBody's panelExtraAction
 * hook: flipping the panel to/from "Single value" reveals/hides it live. `toDesc` is the caller's composedPanelToDesc
 * (passed in to keep this module cycle-free). The copy is a snapshot — a later edit to the panel never touches the draft.
 */
export function buildCreateAlertAction(p, toDesc) {
  if (!isScalarMetricPanel(p)) {
    return null;
  }
  const btn = el("button", {
    class: "btn small",
    type: "button",
    text: "Create alert from this metric",
    title: "Copy this metric into a new alert rule (a copy — later edits here don't change the rule)",
  });
  btn.addEventListener("click", () => {
    const metric = metricSpecFromDesc(toDesc(p));
    setPendingAlertSeed({ metric, name: (p.title || "").trim() });
    location.hash = "#/alert-rule/new";
  });
  return el("div", { class: "composed-panel-actions" }, [btn]);
}
