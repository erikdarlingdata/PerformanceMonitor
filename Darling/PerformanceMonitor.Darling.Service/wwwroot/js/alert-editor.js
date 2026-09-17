/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The #3285 custom-alert-rule EDITOR (Component 7) — the twin of editor.js's view composer, for a rule instead of
 * a dashboard. A rule is name + description + enabled + a definition { metric, predicate, hysteresis, scope,
 * evaluationIntervalSeconds? }, saved through the /api/alerts CRUD (alerts-api.js) that mirrors the /api/views CRUD
 * (version optimistic concurrency, 409 on a stale version or duplicate name). The metric picker is the SHARED
 * composer body (buildComposedPanelBody in metricOnly mode), restricted to a Scalar metric — no chart, group-by, or
 * shape, because an alert evaluates the metric to ONE value. The predicate/hysteresis/scope are alert-specific
 * controls here.
 *
 * The backend is the authority on the whole definition: POST /api/alerts/validate gives the inline verdict and
 * POST /api/alerts/test the live "current value / would-fire per server" preview, both debounced. The client also
 * mirrors the cheap structural rules (window ceiling, warn/critical ordering, the count-with-'<' trap) as an
 * immediate save-blocker so the operator gets a reason without a round-trip — exactly as the composer's saveBlocker
 * does, with the server re-validating on every write.
 *
 * All user text reaches the DOM via util.el()/textContent (R4 — never innerHTML). The 60s poll in app.js SKIPS
 * this route (the editor-route poll guard), so an in-progress edit is never clobbered by a background refresh.
 */

import { el, mount, loadingStrip, errorStrip, emptyStrip, noticeStrip } from "./util.js";
import {
  buildComposedPanelBody,
  loadFleetOptions,
  loadFleetTagOptions,
  newComposedPanel,
  descToComposedPanel,
  composedPanelToDesc,
  field,
  labeledBlock,
} from "./editor.js";
import { formatComposedValue } from "./compose.js";
import { takePendingAlertSeed, metricSpecFromDesc } from "./alert-seed.js";
import * as api from "./alerts-api.js";

/* Debounce for the validate + test round-trips, matching the composer's preview debounce. */
const CHECK_DEBOUNCE_MS = 350;

/* Alert-window bounds mirrored from CustomAlertRuleDefinition (MaxWindowHours / MinWindowHours / the cadence floor)
   so the editor reports them inline rather than only as a 400. An alert window is RECENT — never the 90-day compose
   chart ceiling. */
const MAX_WINDOW_HOURS = 24;
const MIN_WINDOW_HOURS = 1 / 60;
const MIN_INTERVAL_SECONDS = 30;
const DEFAULT_WINDOW_HOURS = 1;

/* The evaluation-window choices (all within [MinWindowHours, MaxWindowHours]); a loaded rule's own window is added
   if it is not one of these, so the select always shows exactly what is applied. */
const WINDOW_OPTIONS = [
  { hours: 5 / 60, label: "Last 5 minutes" },
  { hours: 15 / 60, label: "Last 15 minutes" },
  { hours: 30 / 60, label: "Last 30 minutes" },
  { hours: 1, label: "Last hour" },
  { hours: 2, label: "Last 2 hours" },
  { hours: 3, label: "Last 3 hours" },
  { hours: 6, label: "Last 6 hours" },
  { hours: 12, label: "Last 12 hours" },
  { hours: 24, label: "Last 24 hours" },
];

/* The comparison operators an alert predicate offers — matching the CustomAlertOp enum the backend parses. The
   four scalar comparisons test the value against a single bar (a warning threshold + an optional critical); the
   two range ops (#3351) test it against a two-sided band [lower, upper], firing Warning, with an optional second
   critical band (#3372) for a Warning/Critical two-tier. */
const OP_OPTIONS = [
  { value: "gt", label: "is greater than (>)" },
  { value: "ge", label: "is greater than or equal to (≥)" },
  { value: "lt", label: "is less than (<)" },
  { value: "le", label: "is less than or equal to (≤)" },
  { value: "outside", label: "is outside the range" },
  { value: "between", label: "is within the range" },
];

/* Range ops compare the value to a band [lower, upper] rather than a single threshold, so the editor swaps the
   Warning/Critical inputs for Lower/Upper-bound inputs when one is chosen (and maps them to lowerBound/upperBound
   instead of warnThreshold/criticalThreshold). */
const RANGE_OPS = new Set(["between", "outside"]);
const isRangeOp = (op) => RANGE_OPS.has(op);

/* ─────────────────────────── entry ─────────────────────────── */

export async function renderAlertEditor(main, id, templateKey) {
  mount(main, loadingStrip("Loading alert-rule editor…"));

  const session = await api.getSession();
  if (!session.can_edit) {
    mount(main, [
      alertBackHead(),
      emptyStrip(
        session.probe_failed
          ? "Couldn't confirm your session, so the editor is read-only for now. Reload the page to author a rule."
          : "Your account has read-only access, so the alert-rule editor is read-only."
      ),
    ]);
    return;
  }

  /* The metric picker needs the compose catalog; the scope picker needs the fleet server names AND the fleet tag
     forest (#3350 tag scope). */
  const [catalog, fleet, tags] = await Promise.all([api.getCatalog(), loadFleetOptions(), loadFleetTagOptions()]);

  let model;
  let editingId = null;
  let loadedVersion = null;

  if (id != null && id !== "new") {
    const res = await api.getAlertRule(id);
    if (res.kind !== "data" || !res.data) {
      mount(main, [alertBackHead(), errorStrip(res.message || "Could not load this alert rule.")]);
      return;
    }
    editingId = res.data.id;
    loadedVersion = res.data.version;
    model = ruleToModel(res.data);
  } else if (templateKey) {
    /* Start from a starter template (its definition pre-fills the form; the operator tunes the thresholds/scope
       before saving — the starters are deliberately conservative, not tuned). */
    const tplRes = await api.listAlertTemplates();
    const template =
      tplRes.kind === "data" && tplRes.data && Array.isArray(tplRes.data.templates)
        ? tplRes.data.templates.find((t) => t.key === templateKey)
        : null;
    model = template
      ? definitionToModel(template.definition, template.name, template.description, true)
      : blankModel();
  } else {
    /* A pending "Create alert from this metric" seed (from a composer panel), consumed once, else a blank rule. */
    const seed = takePendingAlertSeed();
    model = seed ? seededModel(seed) : blankModel();
  }

  buildAlertEditor(main, { model, editingId, loadedVersion, catalog, fleet, tags });
}

/* ─────────────────────────── model <-> definition ─────────────────────────── */

/** A fresh Scalar metric picker model (the composer's model, locked to a single value). */
function scalarMetricModel() {
  return { ...newComposedPanel(), shape: "scalar", groupBy: [], hours: null };
}

/** A brand-new blank rule model. */
function blankModel() {
  return {
    name: "",
    description: "",
    enabled: true,
    metric: scalarMetricModel(),
    windowHours: DEFAULT_WINDOW_HOURS,
    op: "gt",
    warn: "",
    critical: "",
    lower: "",
    upper: "",
    critLower: "",
    critUpper: "",
    breachSamples: 1,
    clearSamples: 1,
    scopeMode: "all",
    scopeServers: [],
    scopeTagId: null,
    intervalSeconds: "",
  };
}

/** A blank rule pre-filled from a "Create alert from this metric" seed ({metric, name}). */
function seededModel(seed) {
  const model = blankModel();
  if (seed.metric) {
    model.metric = { ...descToComposedPanel(seed.metric), shape: "scalar", groupBy: [], hours: null };
  }
  if (seed.name) model.name = seed.name;
  return model;
}

/** A stored rule ({name, description, enabled, definition}) -> the editor model. */
function ruleToModel(rule) {
  return definitionToModel(rule.definition || {}, rule.name, rule.description, rule.enabled !== false);
}

/** A rule DEFINITION (+ name/description/enabled) -> the editor model. Shared by the load path and the template
 *  path. The metric is parsed by the SAME descToComposedPanel the composer uses (it reads only the composed keys),
 *  then locked to Scalar; predicate/hysteresis/scope map to their controls. */
function definitionToModel(def, name, description, enabled) {
  const metricJson = def && def.metric ? def.metric : {};
  const pred = def && def.predicate ? def.predicate : {};
  const hyst = def && def.hysteresis ? def.hysteresis : {};
  const scope = def && def.scope ? def.scope : {};
  return {
    name: name || "",
    description: description || "",
    enabled: enabled !== false,
    metric: { ...descToComposedPanel(metricJson), shape: "scalar", groupBy: [], hours: null },
    windowHours: typeof metricJson.hours === "number" ? metricJson.hours : DEFAULT_WINDOW_HOURS,
    op: normalizeOp(pred.op),
    warn: pred.warnThreshold != null ? String(pred.warnThreshold) : "",
    critical: pred.criticalThreshold != null ? String(pred.criticalThreshold) : "",
    lower: pred.lowerBound != null ? String(pred.lowerBound) : "",
    upper: pred.upperBound != null ? String(pred.upperBound) : "",
    critLower: pred.criticalLowerBound != null ? String(pred.criticalLowerBound) : "",
    critUpper: pred.criticalUpperBound != null ? String(pred.criticalUpperBound) : "",
    breachSamples: intOr(hyst.breachSamples, 1),
    clearSamples: intOr(hyst.clearSamples, 1),
    scopeMode: scope.mode === "servers" ? "servers" : scope.mode === "tag" ? "tag" : "all",
    scopeServers: Array.isArray(scope.servers) ? scope.servers.map(String) : [],
    scopeTagId: typeof scope.tagId === "number" ? scope.tagId : null,
    intervalSeconds: typeof def.evaluationIntervalSeconds === "number" ? String(def.evaluationIntervalSeconds) : "",
  };
}

/** The editor model -> the stored rule definition. The metric carries ONLY the compose-authority keys (via
 *  metricSpecFromDesc) plus the alert window; predicate/hysteresis/scope/cadence map back to the definition shape
 *  the backend's CustomAlertRuleDefinition.TryParse validates. Blank optional fields are omitted. */
function modelToDefinition(model) {
  const metric = metricSpecFromDesc(composedPanelToDesc(model.metric));
  metric.hours = model.windowHours;

  let predicate;
  if (isRangeOp(model.op)) {
    /* Range op: a two-sided band, and NO warn/critical (the backend rejects a range predicate that carries them). */
    predicate = { op: model.op, lowerBound: parseNumOrNull(model.lower), upperBound: parseNumOrNull(model.upper) };
    /* Optional critical band (#3372): emit BOTH crit bounds or neither. A lone bound is left off (so the backend
       reads a warn-only rule) — the save-blocker reports the all-or-nothing rule inline before a save. */
    const critLower = parseNumOrNull(model.critLower);
    const critUpper = parseNumOrNull(model.critUpper);
    if (critLower != null && critUpper != null) {
      predicate.criticalLowerBound = critLower;
      predicate.criticalUpperBound = critUpper;
    }
  } else {
    predicate = { op: model.op, warnThreshold: parseNumOrNull(model.warn) };
    if (model.critical.trim() !== "") predicate.criticalThreshold = parseNumOrNull(model.critical);
  }

  const def = {
    metric,
    predicate,
    hysteresis: { breachSamples: model.breachSamples, clearSamples: model.clearSamples },
    scope: scopeToDefinition(model),
  };

  const interval = model.intervalSeconds.trim();
  if (interval !== "") def.evaluationIntervalSeconds = parseInt(interval, 10);
  return def;
}

/** The model's scope -> the definition's scope object: all servers, a specific-server list, or a fleet tag by
 *  its STABLE id (#3350). A "tag" mode with no chosen id still emits { mode:"tag", tagId:null } so the backend
 *  returns its own "scope.tagId required" verdict — the same way an empty "servers" list is left for the backend
 *  to reject; the save-blocker already blocks the save in both cases. */
function scopeToDefinition(model) {
  if (model.scopeMode === "servers") return { mode: "servers", servers: [...model.scopeServers] };
  if (model.scopeMode === "tag") return { mode: "tag", tagId: model.scopeTagId };
  return { mode: "all" };
}

/* ─────────────────────────── save blocker (mirrors the server's cheap rules) ─────────────────────────── */

/** The reason SAVE is blocked, or null when savable. The subtle cross-checks stay the server's job (POST
 *  /api/alerts/validate), but the immediate structural rules — name, a chosen metric, a numeric warning threshold,
 *  the window ceiling, warn/critical ordering, and the count-with-'<' trap — are mirrored here so the operator gets
 *  a reason inline, exactly as the composer's saveBlocker does. */
function alertSaveBlocker(model) {
  if (!model.name || !model.name.trim()) return "Enter a rule name.";

  const metric = model.metric;
  if (!metric.measure && !metric.ratio) return "Choose a metric.";
  if (metric.measure && !metric.aggregate) return "Choose an aggregate for the metric.";

  if (!(model.windowHours >= MIN_WINDOW_HOURS)) return "Choose an evaluation window.";
  if (model.windowHours > MAX_WINDOW_HOURS) return "The evaluation window can be at most 24 hours (an alert window is recent).";

  if (isRangeOp(model.op)) {
    /* Range op: both bounds required and ordered; no warn/critical (the same rules the backend enforces for a
       between/outside predicate). */
    const lower = parseNumOrNull(model.lower);
    const upper = parseNumOrNull(model.upper);
    if (lower == null) return "Enter a lower bound (a number).";
    if (upper == null) return "Enter an upper bound (a number).";
    if (!(lower < upper)) return "The lower bound must be less than the upper bound.";

    /* A range band that fires when the count is 0 has the same stalled-collector ambiguity as the '<'/'<=' count
       trap below (a dead collector reads COUNT 0, not "no data"), so the backend rejects it. 'outside' fires on 0
       when 0 < lower; 'between' when lower <= 0 <= upper. Mirror that here so the operator gets the reason inline. */
    if (metric.aggregate === "count" &&
        (model.op === "outside" ? (0 < lower || 0 > upper) : (lower <= 0 && 0 <= upper))) {
      return "A 'between' or 'outside' alert on a count whose band fires when the count is 0 can't tell zero events from a stalled collector — set the bounds so a count of 0 doesn't fire.";
    }

    /* Optional critical band (#3372): all-or-nothing, and nested per op — WIDER than the warning band for 'outside'
       (fires Critical further out), NARROWER for 'between' (fires Critical deeper in). The backend re-validates. */
    const hasCritLower = String(model.critLower).trim() !== "";
    const hasCritUpper = String(model.critUpper).trim() !== "";
    if (hasCritLower !== hasCritUpper) return "Enter BOTH a critical lower and upper bound, or leave both blank.";
    if (hasCritLower) {
      const critLower = parseNumOrNull(model.critLower);
      const critUpper = parseNumOrNull(model.critUpper);
      if (critLower == null) return "The critical lower bound must be a number.";
      if (critUpper == null) return "The critical upper bound must be a number.";
      if (model.op === "outside") {
        if (!(critLower <= lower && upper <= critUpper)) {
          return "For 'outside', the critical band must be wider than the warning band (critical lower ≤ lower, and upper ≤ critical upper).";
        }
      } else if (!(lower <= critLower && critLower <= critUpper && critUpper <= upper)) {
        return "For 'between', the critical band must be narrower than the warning band (lower ≤ critical lower ≤ critical upper ≤ upper).";
      }
    }
  } else {
    const warn = parseNumOrNull(model.warn);
    if (warn == null) return "Enter a warning threshold (a number).";

    /* A '<'/'<=' alert on a COUNT can't tell zero matching events from a stalled collector reading zero rows, so it
       would false-fire exactly when the true signal is "no data" — the same rule the backend enforces. */
    if (metric.aggregate === "count" && (model.op === "lt" || model.op === "le")) {
      return "A '<' or '≤' alert on a count can't tell zero events from a stalled collector — use '≥' instead.";
    }

    if (model.critical.trim() !== "") {
      const critical = parseNumOrNull(model.critical);
      if (critical == null) return "The critical threshold must be a number (or leave it blank).";
      const ordered = model.op === "gt" || model.op === "ge" ? critical >= warn : critical <= warn;
      if (!ordered) return "The critical threshold must be more extreme than the warning threshold, in the operator's direction.";
    }
  }

  if (!(model.breachSamples >= 1)) return "Breach samples must be at least 1.";
  if (!(model.clearSamples >= 1)) return "Clear samples must be at least 1.";

  if (model.scopeMode === "servers" && !model.scopeServers.length) {
    return "Pick at least one server, or scope the rule to all servers.";
  }

  if (model.scopeMode === "tag" && !(model.scopeTagId > 0)) {
    return "Pick a tag, or scope the rule to all servers.";
  }

  const interval = model.intervalSeconds.trim();
  if (interval !== "") {
    const n = parseInt(interval, 10);
    if (!(n >= MIN_INTERVAL_SECONDS)) return "The evaluation interval must be at least " + MIN_INTERVAL_SECONDS + " seconds.";
  }

  return null;
}

/** Whether the definition is complete enough to send to /api/alerts/validate + /test (a chosen metric + a numeric
 *  warning threshold). Below this the preview area shows a neutral hint rather than a stream of "required" errors. */
function readyToCheck(model) {
  const metric = model.metric;
  if (!metric.measure && !metric.ratio) return false;
  if (metric.measure && !metric.aggregate) return false;
  if (isRangeOp(model.op)) return parseNumOrNull(model.lower) != null && parseNumOrNull(model.upper) != null;
  return parseNumOrNull(model.warn) != null;
}

/* ─────────────────────────── the editor shell ─────────────────────────── */

function buildAlertEditor(main, ctx) {
  const { model } = ctx;

  const nameInput = el("input", { class: "editor-input", type: "text", placeholder: "Rule name (required)", "aria-label": "Rule name" });
  nameInput.value = model.name;
  nameInput.addEventListener("input", () => {
    model.name = nameInput.value;
    refreshSaveState();
  });

  const descInput = el("input", { class: "editor-input", type: "text", placeholder: "Description (optional)", "aria-label": "Rule description" });
  descInput.value = model.description;
  descInput.addEventListener("input", () => {
    model.description = descInput.value;
  });

  const enabledBox = el("input", { type: "checkbox", class: "editor-check", "aria-label": "Rule enabled" });
  enabledBox.checked = model.enabled;
  enabledBox.addEventListener("change", () => {
    model.enabled = enabledBox.checked;
  });

  const saveStatus = el("div", { class: "save-status" });
  const previewBox = el("div", { class: "alert-preview" });
  const saveBtn = el("button", { class: "btn primary", type: "button", text: ctx.editingId != null ? "Save changes" : "Create rule" });
  const cancelBtn = el("button", { class: "btn", type: "button", text: "Cancel" });

  cancelBtn.addEventListener("click", () => {
    location.hash = "#/alert-rules";
  });
  saveBtn.addEventListener("click", onSave);

  /* The metric picker: the SHARED composer body, locked to a Scalar metric. onChange re-checks save state and
     re-runs the debounced validate + test; the body owns its own config rebuild. No group/chart/shape controls,
     and no live compose preview (this editor renders its own would-fire preview below). */
  const metricBody = buildComposedPanelBody(model.metric, {
    catalog: ctx.catalog,
    getVariables: () => [],
    getScopeServer: () => null,
    onChange: onFieldChange,
    metricOnly: true,
  });

  let checkTimer = null;
  function scheduleCheck() {
    clearTimeout(checkTimer);
    checkTimer = setTimeout(runCheck, CHECK_DEBOUNCE_MS);
  }

  /* Every field change refreshes the (synchronous) save gate and reschedules the (async) authority preview. */
  function onFieldChange() {
    refreshSaveState();
    scheduleCheck();
  }

  function refreshSaveState() {
    const problem = alertSaveBlocker(model);
    saveBtn.disabled = problem != null;
    mount(saveStatus, problem ? el("span", { class: "muted", text: problem }) : null);
  }

  /* The authority preview: POST /api/alerts/validate for the inline verdict, then — only when valid — POST
     /api/alerts/test for the per-server current-value / would-fire table. Guarded by readyToCheck so an
     incomplete draft shows a neutral hint, not a stream of "required" errors. */
  async function runCheck() {
    if (!readyToCheck(model)) {
      mount(previewBox, emptyStrip("Choose a metric and a warning threshold to preview what this rule would do."));
      return;
    }
    const def = modelToDefinition(model);
    mount(previewBox, loadingStrip("Checking…"));

    const vres = await api.validateAlertRule(def);
    if (vres.kind === "error") {
      mount(previewBox, errorStrip(vres.message || "Could not validate this rule."));
      return;
    }
    if (vres.kind === "data" && vres.data && vres.data.valid === false) {
      mount(previewBox, errorStrip(vres.data.error || "This rule definition is not valid."));
      return;
    }

    const tres = await api.testAlertRule({ definition: def });
    mount(previewBox, renderTestResult(tres, model.metric.unit));
  }

  const deleteBtn = ctx.editingId != null ? buildDeleteButton(ctx.editingId, saveStatus) : null;

  async function onSave() {
    saveBtn.disabled = true;
    const problem = alertSaveBlocker(model);
    if (problem) {
      saveBtn.disabled = false;
      mount(saveStatus, el("span", { class: "strip error", text: problem }));
      return;
    }

    mount(saveStatus, el("span", { class: "muted", text: "Saving…" }));
    const body = {
      name: model.name.trim(),
      description: model.description ? model.description : null,
      enabled: model.enabled,
      definition: modelToDefinition(model),
    };

    let res;
    if (ctx.editingId != null) {
      body.version = ctx.loadedVersion;
      res = await api.updateAlertRule(ctx.editingId, body);
    } else {
      res = await api.createAlertRule(body);
    }

    if (res.kind === "data" && res.data) {
      location.hash = "#/alert-rules";
      return;
    }

    saveBtn.disabled = false;
    handleSaveError(res, saveStatus, ctx, main);
  }

  const footer = [saveBtn, cancelBtn];
  if (deleteBtn) footer.push(deleteBtn);
  footer.push(saveStatus);

  mount(main, [
    el("div", { class: "page-head" }, [
      el("a", { href: "#/alert-rules", text: "← Alert Rules" }),
      el("h2", { text: ctx.editingId != null ? "Edit alert rule" : "New alert rule" }),
    ]),
    el("div", { class: "editor-meta" }, [
      field("Name", nameInput),
      field("Description", descInput),
      field("Enabled", enabledBox, "field-enabled"),
    ]),
    el("h3", { class: "section-title", text: "Metric" }),
    el("div", { class: "panel-editor card" }, [metricBody]),
    el("h3", { class: "section-title", text: "Condition" }),
    el("div", { class: "alert-section card" }, [conditionSection(model, onFieldChange)]),
    el("h3", { class: "section-title", text: "Hysteresis" }),
    el("div", { class: "alert-section card" }, [hysteresisSection(model, onFieldChange)]),
    el("h3", { class: "section-title", text: "Scope" }),
    el("div", { class: "alert-section card" }, [scopeSection(model, ctx.fleet, ctx.tags, onFieldChange)]),
    advancedSection(model, onFieldChange),
    el("h3", { class: "section-title", text: "Preview" }),
    previewBox,
    el("div", { class: "editor-footer" }, footer),
  ]);

  refreshSaveState();
  runCheck();
}

/* ─────────────────────────── condition (window + predicate) ─────────────────────────── */

/* When the metric fires: the evaluation window, the comparison operator, and the threshold(s) — which DEPEND on
   the operator. A scalar op (gt/ge/lt/le) shows a Warning threshold + an optional (more-extreme) Critical, and the
   severity is those two thresholds (CustomAlertRuleDefinition.SeverityFor). A range op (between/outside, #3351)
   shows a Lower/Upper bound instead, plus an OPTIONAL Critical Lower/Upper band (#3372) for a second, more-severe
   tier; so the inputs — and the help text — swap in place when the op category changes. */
function conditionSection(model, onChange) {
  const windowSel = buildWindowSelect(model, onChange);

  const opSel = el("select", { class: "editor-select", "aria-label": "Comparison" });
  for (const o of OP_OPTIONS) opSel.appendChild(el("option", { value: o.value, text: o.label }));
  opSel.value = model.op;

  /* The threshold inputs + help live in redrawn sub-containers so switching between a scalar and a range op swaps
     Warning/Critical <-> Lower/Upper in place; modelToDefinition maps them to the right predicate keys. */
  const thresholdBox = el("div", { class: "composed-fields" });
  const helpBox = el("div", { class: "block-help" });

  /** A numeric input bound to model[key], re-checking save state + preview on every edit (like the old warn/crit). */
  function numField(label, key, placeholder) {
    const input = el("input", { class: "editor-input", type: "number", step: "any", placeholder, "aria-label": label });
    input.value = model[key];
    input.addEventListener("input", () => {
      model[key] = input.value;
      onChange();
    });
    return field(label, input);
  }

  function drawCondition() {
    if (isRangeOp(model.op)) {
      mount(thresholdBox, [
        numField("Lower bound", "lower", "lower bound"),
        numField("Upper bound", "upper", "upper bound"),
        numField("Critical lower bound", "critLower", "optional"),
        numField("Critical upper bound", "critUpper", "optional"),
      ]);
      helpBox.textContent =
        "The metric is aggregated to one value over the window, then tested against the band. 'Is outside the range' " +
        "fires when the value is below the lower bound or above the upper bound; 'is within the range' fires when it " +
        "falls inside (bounds inclusive). Leave the critical bounds blank for a single Warning tier, or set both for a " +
        "second, more-severe Critical tier: for 'is outside the range' the critical band must be WIDER than the " +
        "warning band (Critical fires when the value is even further outside); for 'is within the range' it must be " +
        "NARROWER (Critical fires when the value is even deeper inside).";
    } else {
      mount(thresholdBox, [numField("Warning threshold", "warn", "warning value"), numField("Critical threshold", "critical", "critical value (optional)")]);
      helpBox.textContent =
        "The metric is aggregated to one value over the window, then compared. Crossing the warning threshold fires " +
        "a Warning; crossing the (more-extreme) critical threshold fires a Critical. Leave critical blank for a " +
        "single Warning tier.";
    }
  }

  opSel.addEventListener("change", () => {
    const wasRange = isRangeOp(model.op);
    model.op = opSel.value;
    /* Only redraw the inputs when the op crosses the scalar<->range boundary (a scalar->scalar change keeps the
       same warn/crit inputs, so their in-progress values are preserved). */
    if (isRangeOp(model.op) !== wasRange) drawCondition();
    onChange();
  });

  drawCondition();

  return el("div", {}, [
    el("div", { class: "composed-fields" }, [field("Evaluate over", windowSel), field("When the value", opSel)]),
    thresholdBox,
    helpBox,
  ]);
}

/* The evaluation-window select (bound to model.windowHours): the presets, plus a loaded rule's own window when it is
   not a preset (kept selectable so it shows exactly what is applied). Capped at MaxWindowHours — an alert window is
   recent, never the 90-day compose chart span. */
function buildWindowSelect(model, onChange) {
  const sel = el("select", { class: "editor-select", "aria-label": "Evaluation window" });
  const added = new Set();
  for (const o of WINDOW_OPTIONS) {
    sel.appendChild(el("option", { value: String(o.hours), text: o.label }));
    added.add(String(o.hours));
  }
  const cur = String(model.windowHours);
  if (model.windowHours > 0 && !added.has(cur)) {
    sel.appendChild(el("option", { value: cur, text: windowLabel(model.windowHours) }));
  }
  sel.value = cur;
  sel.addEventListener("change", () => {
    model.windowHours = Number(sel.value) || DEFAULT_WINDOW_HOURS;
    onChange();
  });
  return sel;
}

/** A human label for an arbitrary window (hours), matching the preset style — sub-hour windows render as minutes. */
function windowLabel(hours) {
  if (hours < 1) {
    const mins = Math.round(hours * 60);
    return "Last " + mins + (mins === 1 ? " minute" : " minutes");
  }
  return "Last " + hours + (hours === 1 ? " hour" : " hours");
}

/* ─────────────────────────── hysteresis ─────────────────────────── */

/* How many consecutive evaluations gate a fire and a clear — so a single spike does not page and a single dip does
   not resolve prematurely. Both are integers >= 1 (the backend's floor). */
function hysteresisSection(model, onChange) {
  const breach = el("input", { class: "editor-input", type: "number", step: "1", min: "1", "aria-label": "Breach samples" });
  breach.value = String(model.breachSamples);
  breach.addEventListener("input", () => {
    model.breachSamples = clampIntAtLeast(breach.value, 1);
    onChange();
  });

  const clear = el("input", { class: "editor-input", type: "number", step: "1", min: "1", "aria-label": "Clear samples" });
  clear.value = String(model.clearSamples);
  clear.addEventListener("input", () => {
    model.clearSamples = clampIntAtLeast(clear.value, 1);
    onChange();
  });

  return el("div", {}, [
    el("div", { class: "composed-fields" }, [
      field("Breach samples (to fire)", breach),
      field("Clear samples (to resolve)", clear),
    ]),
    el("div", {
      class: "block-help",
      text: "The condition must hold for this many consecutive evaluations before the rule fires, and clear for this many before it resolves. 1 fires on the first breaching sample.",
    }),
  ]);
}

/* ─────────────────────────── scope ─────────────────────────── */

/* Which servers the rule evaluates against: all servers (the fleet), a chosen set, or the members of a fleet
   tag (#3350). Tag scope stores the tag's STABLE id (scope.tagId), so a rename never re-scopes the rule; the
   picker resolves name<->id from the /api/fleet tag forest. */
function scopeSection(model, fleet, tags, onChange) {
  const list = el("div", { class: "scope-list" });
  const box = el("div", {});

  const modeSel = el("select", { class: "editor-select", "aria-label": "Scope" }, [
    el("option", { value: "all", text: "All servers (fleet)" }),
    el("option", { value: "servers", text: "Specific servers" }),
    el("option", { value: "tag", text: "Servers with a tag" }),
  ]);
  modeSel.value = model.scopeMode;
  modeSel.addEventListener("change", () => {
    model.scopeMode = modeSel.value === "servers" ? "servers" : modeSel.value === "tag" ? "tag" : "all";
    redraw();
    onChange();
  });

  function redraw() {
    if (model.scopeMode === "tag") {
      mount(box, tagScope());
      return;
    }
    if (model.scopeMode !== "servers") {
      mount(box, el("div", { class: "block-help", text: "This rule evaluates against every monitored server." }));
      return;
    }
    if (!fleet.length) {
      mount(box, noticeStrip("No servers are in the fleet yet, so a specific-server scope has nothing to pick."));
      return;
    }
    const rows = fleet.map((s) => {
      const on = model.scopeServers.includes(s.value);
      return checkRow(s.label, on, (checked) => {
        if (checked) {
          if (!model.scopeServers.includes(s.value)) model.scopeServers.push(s.value);
        } else {
          model.scopeServers = model.scopeServers.filter((x) => x !== s.value);
        }
        onChange();
      });
    });
    mount(list, rows);
    mount(box, [list, el("div", { class: "block-help", text: "The rule fires per server; pick the servers it applies to." })]);
  }

  /* The tag picker: a single-select of the fleet tag forest, indented by depth. The rule evaluates every server
     under the chosen tag's whole subtree (the tag plus its descendant tags' servers), resolved fresh each sweep
     — so adding or removing a server, or a sub-tag, re-scopes the rule without editing it. */
  function tagScope() {
    if (!tags.length) {
      return noticeStrip("No fleet tags are defined yet. Create and assign tags in the desktop viewer's fleet view, then scope a rule to one.");
    }
    const sel = el("select", { class: "editor-select", "aria-label": "Tag" });
    sel.appendChild(el("option", { value: "", text: "— pick a tag —" }));
    for (const t of tags) {
      // Indent by depth (non-breaking spaces via textContent, never innerHTML) so the flat <select> shows the tree.
      const prefix = t.depth > 0 ? "  ".repeat(t.depth) + "└ " : "";
      sel.appendChild(el("option", { value: t.value, text: prefix + t.label }));
    }
    // A loaded rule may name a tag since deleted/renamed away; keep it selectable (like buildWindowSelect keeps a
    // non-preset window) so the operator sees what is applied rather than a silent reset to "pick a tag".
    if (model.scopeTagId != null && !tags.some((t) => t.value === String(model.scopeTagId))) {
      sel.appendChild(el("option", { value: String(model.scopeTagId), text: "Tag " + model.scopeTagId + " (not in fleet — deleted?)" }));
    }
    sel.value = model.scopeTagId != null ? String(model.scopeTagId) : "";
    sel.addEventListener("change", () => {
      model.scopeTagId = sel.value ? Number(sel.value) : null;
      onChange();
    });
    return el("div", {}, [
      field("Tag", sel),
      el("div", {
        class: "block-help",
        text: "The rule fires per server for every server assigned this tag OR any tag beneath it (the whole subtree). Membership is resolved fresh each evaluation, so adding or removing a server — or a sub-tag — re-scopes the rule automatically; a tag whose subtree has no servers matches nothing (the rule never fires).",
      }),
    ]);
  }

  redraw();
  return el("div", {}, [field("Scope", modeSel), box]);
}

function checkRow(label, checked, onToggle) {
  const cbox = el("input", { type: "checkbox", class: "editor-check" });
  cbox.checked = checked;
  cbox.addEventListener("change", () => onToggle(cbox.checked));
  return el("label", { class: "scope-item" }, [cbox, el("span", { text: label })]);
}

/* ─────────────────────────── advanced (cadence) ─────────────────────────── */

/* The optional per-rule evaluation cadence — how often the sweep evaluates this rule. Folded away because the
   default (the sweep's own interval) is right for almost every rule; a floor of MinEvaluationIntervalSeconds keeps
   a rule from asking to run faster than the collectors move. */
function advancedSection(model, onChange) {
  const input = el("input", { class: "editor-input", type: "number", step: "1", min: String(MIN_INTERVAL_SECONDS), placeholder: "default", "aria-label": "Evaluation interval (seconds)" });
  input.value = model.intervalSeconds;
  input.addEventListener("input", () => {
    model.intervalSeconds = input.value;
    onChange();
  });
  return el("details", { class: "alert-advanced" }, [
    el("summary", { text: "Advanced" }),
    el("div", { class: "cfg" }, [
      field("Evaluation interval (seconds)", input),
      el("div", { class: "block-help", text: "How often to evaluate this rule. Leave blank for the default cadence; at least " + MIN_INTERVAL_SECONDS + " seconds." }),
    ]),
  ]);
}

/* ─────────────────────────── test result (current value / would-fire) ─────────────────────────── */

/* The POST /api/alerts/test result: per-server CURRENT value + whether it would breach right now, in a table, with
   the hysteresis caveat above it. This is the instantaneous predicate only — the running evaluator also gates on
   hysteresis + per-server streak, which the note spells out. Every value reaches the DOM as text (R4). */
function renderTestResult(res, unit) {
  if (res.kind === "empty") {
    /* An invalid/not-found draft comes back as {status, message}; surface the message (validate already ran, so
       this is the belt-and-suspenders path). */
    return errorStrip(res.message || "This rule definition is not valid.");
  }
  if (res.kind === "error") {
    return errorStrip(res.message || "Could not evaluate this rule.");
  }

  const data = res.data || {};
  const results = Array.isArray(data.results) ? data.results : [];
  const nodes = [];
  if (typeof data.note === "string" && data.note) nodes.push(noticeStrip(data.note));

  if (data.status === "no_in_scope_servers" || !results.length) {
    nodes.push(emptyStrip("No monitored servers match this scope right now."));
    return el("div", {}, nodes);
  }

  const head = el("tr", {}, [
    el("th", { text: "Server" }),
    el("th", { class: "num", text: "Current value" }),
    el("th", { text: "Would fire now" }),
  ]);
  const rows = results.map((r) => {
    let verdict;
    if (r.no_data) verdict = el("span", { class: "muted", text: "no data" });
    else if (r.breaching) {
      const sev = r.severity || "Warning";
      verdict = el("span", { class: "status-cell sev-" + sev }, [el("span", { class: "glyph", text: "●" }), el("span", { text: "Yes — " + sev })]);
    } else {
      verdict = el("span", { class: "status-cell sev-Healthy" }, [el("span", { class: "glyph", text: "○" }), el("span", { text: "No" })]);
    }
    return el("tr", {}, [
      el("td", { text: r.server }),
      el("td", { class: "num", text: r.no_data ? "—" : formatComposedValue(r.current_value, unit || "") }),
      el("td", {}, [verdict]),
    ]);
  });
  nodes.push(el("div", { class: "table-wrap" }, [el("table", { class: "data" }, [el("thead", {}, [head]), el("tbody", {}, rows)])]));
  return el("div", {}, nodes);
}

/* ─────────────────────────── delete + errors ─────────────────────────── */

function buildDeleteButton(id, saveStatus) {
  const btn = el("button", { class: "btn small danger", type: "button", text: "Delete" });
  btn.addEventListener("click", async () => {
    if (!window.confirm("Delete this alert rule? Any open incident for it is resolved first. This cannot be undone.")) {
      return;
    }
    btn.disabled = true;
    mount(saveStatus, el("span", { class: "muted", text: "Deleting…" }));
    const res = await api.deleteAlertRule(id);
    if (res.kind === "data") {
      location.hash = "#/alert-rules";
      return;
    }
    btn.disabled = false;
    mount(saveStatus, errorStrip(res.message || "Could not delete this rule."));
  });
  return btn;
}

/* On a 409 while EDITING, the row changed under us (stale version) or the name now collides — offer to reload the
   latest (discarding local edits), mirroring the view/notebook composers. */
function handleSaveError(res, saveStatus, ctx, main) {
  const parts = [el("span", { text: res.message || "Could not save the alert rule." })];
  if (res.status === 409 && ctx.editingId != null) {
    const reload = el("button", { class: "btn small", type: "button", text: "Reload latest" });
    reload.addEventListener("click", () => renderAlertEditor(main, ctx.editingId));
    parts.push(reload);
  }
  mount(saveStatus, el("div", { class: "strip error" }, parts));
}

/* ─────────────────────────── small helpers ─────────────────────────── */

function alertBackHead() {
  return el("div", { class: "page-head" }, [
    el("a", { href: "#/alert-rules", text: "← Alert Rules" }),
    el("h2", { text: "Alert-rule editor" }),
  ]);
}

/** Map a stored/loose op to one of the four select values; defaults to "gt". */
function normalizeOp(raw) {
  switch (String(raw == null ? "" : raw).trim().toLowerCase()) {
    case "gt":
    case ">":
      return "gt";
    case "ge":
    case ">=":
      return "ge";
    case "lt":
    case "<":
      return "lt";
    case "le":
    case "<=":
      return "le";
    case "between":
      return "between";
    case "outside":
      return "outside";
    default:
      return "gt";
  }
}

/** A finite number from a text input, or null (blank or non-numeric). */
function parseNumOrNull(raw) {
  if (raw == null || String(raw).trim() === "") return null;
  const n = Number(raw);
  return isFinite(n) ? n : null;
}

/** An integer >= min from a loose value, falling back to min. */
function clampIntAtLeast(raw, min) {
  const n = parseInt(raw, 10);
  return isFinite(n) && n >= min ? n : min;
}

/** An integer from a value, or a fallback. */
function intOr(v, dflt) {
  const n = parseInt(v, 10);
  return isFinite(n) ? n : dflt;
}
