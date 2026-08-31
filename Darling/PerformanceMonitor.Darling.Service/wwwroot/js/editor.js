/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The #1563 custom-view COMPOSER — the editor for a view (name + description + an ordered panel
 * list). Each panel is a renderPanel descriptor built through the SAME viz registry the renderer uses, previewed
 * LIVE (debounced) through renderPanel so the operator sees exactly what will be stored.
 *
 *   read picker (grouped by catalog category) -> typed param inputs from the catalog (int/double -> number,
 *   bool -> checkbox, text -> input, server -> a fleet dropdown; a required param blocks preview + save until
 *   filled) -> viz picker -> width (span 1|2) -> title -> a vizcfg sub-editor SEEDED by client-side derivation
 *   (derive.js) from a live sample fetch, then hand-tunable (columns/series/stats + a format per field).
 *
 * ACCESS: editing is available to any AUTHENTICATED seat — the same reach as viewing (network operation is the
 * normal mode). The server gates writes by the host auth (token->cookie + CIDR over the network; tokenless on
 * loopback) plus a Content-Type check; can_edit only goes false if the session probe itself failed, which shows
 * the reload notice below. All user text reaches the DOM via el()/textContent
 * (R4 — el() throws on an html prop). Series COLOR is constrained to an <input type=color> (#rrggbb) + the
 * charts.js palette, NEVER free text, so it can't inject a style-attribute sink (reconciliation #4).
 *
 * The 60s poll in app.js SKIPS re-rendering this route (the editor-route poll guard), so an in-progress edit is
 * never clobbered by a background refresh.
 */

import { el, mount, apiGet, readTool } from "./util.js";
import { renderPanel, VIZ } from "./panels.js";
import { SERIES_COLORS, normalizeColor } from "./charts.js";
import { renderComposedPanelCard } from "./compose.js";
import * as api from "./views-api.js";
import * as derive from "./derive.js";

/** The FORMATTERS keys the format pickers offer (mirrors util.js FORMATTERS). */
const FORMAT_OPTIONS = ["text", "int", "num1", "num2", "pct", "ms", "mb", "time", "reltime", "bool"];
const PREVIEW_DEBOUNCE_MS = 350;

/** The view-level default time-range choices (hours) offered in the composer + the rendered view's chrome. */
const RANGE_OPTIONS = [
  { hours: 1, label: "Last hour" },
  { hours: 6, label: "Last 6 hours" },
  { hours: 24, label: "Last 24 hours" },
  { hours: 24 * 7, label: "Last 7 days" },
  { hours: 24 * 30, label: "Last 30 days" },
  { hours: 24 * 90, label: "Last 90 days" },
];
const DEFAULT_RANGE_HOURS = 24;

/** The default chart per composed-panel shape (a working chart the instant a measure is picked). */
const DEFAULT_VIZ_FOR_SHAPE = { timeseries: "line", topseries: "line", ranked: "bar", scalar: "stat" };

/** The most event-annotation sources a time-series panel may overlay (design D5) — mirrors the server's
 *  ComposeLimits.MaxAnnotations so the composer caps at the same number the run endpoint accepts. */
const MAX_ANNOTATIONS = 4;

/* ─────────────────────────── entry ─────────────────────────── */

export async function renderEditor(main, id) {
  mount(main, el("div", { class: "strip loading", text: "Loading composer…" }));

  const session = await api.getSession();
  if (!session.can_edit) {
    mount(main, [
      backHead(id),
      el("div", { class: "strip empty" }, [
        "Couldn't confirm your session, so the composer is read-only for now. Reload the page to edit; " +
          "you can still open and export views.",
      ]),
    ]);
    return;
  }

  const [catalog, fleet] = await Promise.all([api.getCatalog(), loadFleetOptions()]);

  let model;
  let editingId = null;
  let loadedVersion = null;

  if (id != null && id !== "new") {
    const res = await api.getView(id);
    if (res.kind !== "data" || !res.data) {
      mount(main, [backHead(id), el("div", { class: "strip error", text: res.message || "Could not load this view." })]);
      return;
    }
    editingId = res.data.id;
    loadedVersion = res.data.version;
    model = viewToModel(res.data);
  } else {
    model = { name: "", description: "", panels: [newPanel()], variables: [], rangeHours: null };
  }

  buildEditor(main, { model, editingId, loadedVersion, catalog, fleet });
}

/* Fleet server options for the `server` param dropdown (value = stored server_name, label = display name). Each
   option also carries the per-server platform (/api/fleet's D4 fields) the composed measure picker uses for D4
   auto-greying: engineEdition (null = not yet probed), isAzureSqlDb (edition 5), isAzureMi (edition 8). The server
   dropdown reads only value/label, so the extra fields are inert there. Exported so the notebook composer — which
   greys the same way — shares this one enrichment. */
export async function loadFleetOptions() {
  const res = await apiGet("/api/fleet");
  if (res.kind !== "data" || !res.data) return [];
  return [...(res.data.cards || [])]
    .map((c) => ({
      value: c.server_name || c.display_name,
      label: c.display_name,
      engineEdition: typeof c.engine_edition === "number" ? c.engine_edition : null,
      isAzureSqlDb: c.is_azure_sql_db === true,
      isAzureMi: c.is_azure_mi === true,
    }))
    .filter((o) => o.value)
    .sort((a, b) => a.label.localeCompare(b.label));
}

/* ─────────────────────────── model <-> stored definition ─────────────────────────── */

/** A new panel — a COMPOSED metric by default (the v2 headline); a read panel is the "advanced" alternative. */
function newPanel() {
  return newComposedPanel();
}

export function newComposedPanel() {
  return {
    kind: "source",
    source: "",
    measure: "",
    ratio: "",
    aggregate: "",
    unit: "",
    shape: "timeseries",
    timeBucket: "auto",
    topN: 10,
    includeOther: false,
    filters: [],
    groupBy: [],
    viz: "",
    span: 1,
    title: "",
    hours: null,
    thresholds: [],
    annotations: [],
    overlay: null, // { measure, aggregate, unit } or null (#1606 — scatter's y / a dual-axis right axis)
  };
}

function newReadPanel() {
  return { kind: "read", read: "", params: {}, viz: "", span: 1, title: "", vizcfg: {} };
}

function viewToModel(view) {
  const def = view.definition || {};
  const panels = Array.isArray(def.panels) ? def.panels.map(descToPanel) : [];
  return {
    name: view.name || "",
    description: view.description || "",
    panels: panels.length ? panels : [newPanel()],
    variables: parseVariables(def.variables),
    rangeHours: def.range && typeof def.range.hours === "number" ? def.range.hours : null,
  };
}

/** The view's stored template variables -> the model list ({name, dimension, default}); malformed entries dropped. */
function parseVariables(node) {
  if (!Array.isArray(node)) return [];
  return node
    .filter((v) => v && typeof v === "object" && v.name)
    .map((v) => ({ name: String(v.name), dimension: v.dimension ? String(v.dimension) : "", default: v.default != null ? String(v.default) : "" }));
}

/* A stored descriptor -> the editor's panel model, dispatched on kind: a v2 composed panel names a `source`, a v1
   read panel names a `read`. `kind` is an editor-only field (the stored def carries neither `kind` nor `shape` — the
   server infers the panel kind from `source`, and shape from timeBucket/topN). */
function descToPanel(d) {
  const src = d && typeof d === "object" ? d : {};
  return src.source != null ? descToComposedPanel(src) : descToReadPanel(src);
}

export function descToComposedPanel(d) {
  const hasBucket = d.timeBucket && d.timeBucket !== "none";
  const hasTopN = d.topN != null;
  return {
    kind: "source",
    source: d.source || "",
    measure: d.measure || "",
    ratio: d.ratio || "",
    aggregate: d.aggregate || "",
    unit: d.unit || "",
    /* #2734: BOTH keys is the rank-then-bucket mode. It must be recognized here or the round trip
       silently downgrades a stored top-N series to a plain one on the next save. */
    shape: hasBucket && hasTopN ? "topseries" : hasBucket ? "timeseries" : hasTopN ? "ranked" : "scalar",
    timeBucket: hasBucket ? d.timeBucket : "auto",
    topN: hasTopN ? d.topN : 10,
    includeOther: d.includeOther === true,
    filters: Array.isArray(d.filters) ? d.filters.map(descToFilter) : [],
    groupBy: Array.isArray(d.groupBy) ? d.groupBy.map(String) : [],
    viz: d.viz || "",
    span: d.span === 2 ? 2 : 1,
    title: d.title || "",
    hours: typeof d.hours === "number" ? d.hours : null,
    thresholds: cleanThresholds(d.thresholds),
    annotations: Array.isArray(d.annotations) ? d.annotations.map(String) : [],
    overlay:
      d.overlay && typeof d.overlay === "object" && d.overlay.measure
        ? { measure: String(d.overlay.measure), aggregate: d.overlay.aggregate ? String(d.overlay.aggregate) : "", unit: d.overlay.unit ? String(d.overlay.unit) : "" }
        : null,
  };
}

/* A stored filter -> the model's single-text-box form: an array value (eq/neq multi) joins to a comma string; a
   scalar / "$var" value stays as-is. panelToDesc reverses this. */
function descToFilter(f) {
  const src = f && typeof f === "object" ? f : {};
  const value = Array.isArray(src.value) ? src.value.join(", ") : src.value != null ? String(src.value) : "";
  return { dimension: src.dimension || "", op: src.op || "eq", value };
}

/* The v1 read panel: everything beyond title/read/params/viz/span is preserved verbatim as `vizcfg` and spread
   back on save, so nothing is lost on an edit round-trip. */
function descToReadPanel(d) {
  const { title, read, params, viz, span, kind, ...vizcfg } = d;
  return {
    kind: "read",
    read: read || "",
    params: params && typeof params === "object" && !Array.isArray(params) ? { ...params } : {},
    viz: viz || "",
    span: span === 2 ? 2 : 1,
    title: title || "",
    vizcfg: vizcfg || {},
  };
}

function panelToDesc(p) {
  return p.kind === "source" ? composedPanelToDesc(p) : readPanelToDesc(p);
}

/* A composed panel model -> the stored descriptor. `kind`/`shape`/preview-only fields are dropped: the server
   reads the panel kind from `source` and the mode from timeBucket/topN. measure XOR ratio; aggregate only for a
   scalar; timeBucket only for a time series; topN only for a ranked panel. */
export function composedPanelToDesc(p) {
  const d = { source: p.source, viz: p.viz, span: p.span === 2 ? 2 : 1, title: p.title || "" };
  if (p.ratio) d.ratio = p.ratio;
  else d.measure = p.measure;
  if (!p.ratio && p.aggregate) d.aggregate = p.aggregate;
  if (p.unit) d.unit = p.unit;
  if (p.shape === "timeseries") d.timeBucket = p.timeBucket || "auto";
  else if (p.shape === "ranked") d.topN = clampInt(p.topN, 1, 1000, 10);
  else if (p.shape === "topseries") {
    /* #2734 rank-then-bucket emits BOTH — dropping either one here is silent data loss, not a smaller panel. */
    d.timeBucket = p.timeBucket || "auto";
    d.topN = clampInt(p.topN, 1, 1000, 10);
    if (p.includeOther === true) d.includeOther = true;
  }
  const filters = (p.filters || []).map(filterToDesc).filter((f) => f.dimension && f.op);
  if (filters.length) d.filters = filters;
  if ((p.groupBy || []).length) d.groupBy = [...p.groupBy];
  if (p.hours != null) d.hours = p.hours;
  const thresholds = cleanThresholds(p.thresholds);
  if (thresholds.length) d.thresholds = thresholds;
  /* Event-annotation sources (design D5) are only meaningful on a time-series panel — the server rejects them
     otherwise — so emit them only for that shape. Kept in the model across shape changes (like thresholds), just
     not serialized off a ranked/scalar panel. */
  if (p.shape === "timeseries" || p.shape === "topseries") {
    const annotations = cleanAnnotations(p.annotations);
    if (annotations.length) d.annotations = annotations;
  }
  /* The overlay (#1606) is only meaningful on scatter and ungrouped line/area — the server rejects it
     anywhere else — so, like annotations, it stays in the model across viz flips but only serializes
     where it is legal. */
  if (p.overlay && p.overlay.measure && (p.viz === "scatter" || p.viz === "line" || p.viz === "area")) {
    const o = { measure: p.overlay.measure };
    if (p.overlay.aggregate) o.aggregate = p.overlay.aggregate;
    if (p.overlay.unit) o.unit = p.overlay.unit;
    d.overlay = o;
  }
  return d;
}

/** Normalize a thresholds list to at most 4 finite numbers (design D3) — drops the editor's null/blank entries so
 *  a stored def (and the preview) only ever carries real reference-line values, matching the server's bounds. */
function cleanThresholds(list) {
  if (!Array.isArray(list)) return [];
  return list.filter((n) => typeof n === "number" && isFinite(n)).slice(0, 4);
}

/** Normalize an annotation-source list to at most MAX_ANNOTATIONS distinct non-empty string keys (design D5),
 *  mirroring the server's dedupe + cap so a stored def (and the preview) matches what the run endpoint accepts. */
function cleanAnnotations(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  const out = [];
  for (const k of list) {
    if (typeof k !== "string" || !k || seen.has(k)) continue;
    seen.add(k);
    out.push(k);
    if (out.length >= MAX_ANNOTATIONS) break;
  }
  return out;
}

/* A model filter -> the stored form: an eq/neq value with commas splits into a multi-value array; everything else
   (a single literal, a LIKE pattern, a threshold, or a "$var" reference) stays a scalar string. */
function filterToDesc(f) {
  const raw = f.value == null ? "" : String(f.value);
  let value = raw;
  if ((f.op === "eq" || f.op === "neq") && raw.includes(",")) {
    value = raw
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length);
  }
  return { dimension: f.dimension, op: f.op, value };
}

function readPanelToDesc(p) {
  return {
    title: p.title || "",
    read: p.read,
    viz: p.viz,
    span: p.span === 2 ? 2 : 1,
    ...(p.vizcfg || {}),
    params: cleanParams(p.params),
  };
}

function modelToDefinition(model) {
  const def = { panels: model.panels.map(panelToDesc) };
  const variables = (model.variables || [])
    .filter((v) => v.name && v.dimension)
    .map((v) => (v.default ? { name: v.name, dimension: v.dimension, default: v.default } : { name: v.name, dimension: v.dimension }));
  if (variables.length) def.variables = variables;
  if (model.rangeHours) def.range = { hours: model.rangeHours };
  return def;
}

/** Drop null / undefined / empty-string param values so an optional-left-blank param uses the tool's default. */
function cleanParams(params) {
  const out = {};
  for (const [k, v] of Object.entries(params || {})) {
    if (v === null || v === undefined || v === "") continue;
    out[k] = v;
  }
  return out;
}

/* ─────────────────────────── the editor shell ─────────────────────────── */

function buildEditor(main, ctx) {
  const { model } = ctx;

  const nameInput = el("input", {
    class: "editor-input",
    type: "text",
    placeholder: "View name (required)",
    "aria-label": "View name",
  });
  nameInput.value = model.name;
  nameInput.addEventListener("input", () => {
    model.name = nameInput.value;
    refreshSaveState();
  });

  const descInput = el("input", {
    class: "editor-input",
    type: "text",
    placeholder: "Description (optional)",
    "aria-label": "View description",
  });
  descInput.value = model.description;
  descInput.addEventListener("input", () => {
    model.description = descInput.value;
  });

  const panelsBox = el("div", { class: "panels-box" });
  const saveStatus = el("div", { class: "save-status" });
  const saveBtn = el("button", {
    class: "btn primary",
    type: "button",
    text: ctx.editingId != null ? "Save changes" : "Create view",
  });
  const cancelBtn = el("button", { class: "btn", type: "button", text: "Cancel" });

  const backHash = ctx.editingId != null ? "#/view/" + encodeURIComponent(ctx.editingId) : "#/views";
  cancelBtn.addEventListener("click", () => {
    location.hash = backHash;
  });
  saveBtn.addEventListener("click", onSave);

  const addBtn = el("button", { class: "btn", type: "button", text: "+ Add panel" });
  addBtn.addEventListener("click", () => {
    model.panels.push(newPanel());
    rebuildPanels();
  });

  /* The scope a composed panel's LIVE preview runs against — the whole fleet + the view's default range + the
     declared variables (bound to their defaults). Read live at preview time, so it always reflects the current
     view-scope model. The rendered view lets the viewer change server/time/variable values; this is the author's
     preview baseline. */
  function previewScope() {
    const values = {};
    for (const v of model.variables || []) {
      if (v.name && v.dimension && v.dimension !== "server" && v.default) values[v.name] = v.default;
    }
    return {
      server: "All",
      hours: model.rangeHours || DEFAULT_RANGE_HOURS,
      variables: (model.variables || [])
        .filter((v) => v.name && v.dimension)
        .map((v) => ({ name: v.name, dimension: v.dimension, default: v.default || undefined })),
      values,
    };
  }

  const inner = { ...ctx, rebuildPanels, refreshSaveState, previewScope };

  const viewScopeSection = buildViewScopeSection(model, ctx.catalog, rebuildPanels);

  function rebuildPanels() {
    mount(panelsBox, model.panels.map((p, i) => buildPanelEditor(p, i, inner)));
    refreshSaveState();
  }

  function refreshSaveState() {
    const problem = saveBlocker(model, ctx.catalog);
    saveBtn.disabled = problem != null;
    mount(saveStatus, problem ? el("span", { class: "muted", text: problem }) : null);
  }

  async function onSave() {
    saveBtn.disabled = true;

    /* Auto-derive-on-save (B1): seed any field-less table/line/stat panel from a fresh live sample so the stored
       definition never carries an undefined columns/series/stats array (which would crash the shared renderer for
       EVERY seat). A panel with no live sample stays un-derived and is caught by the field-config backstop below. */
    mount(saveStatus, el("span", { class: "muted", text: "Preparing…" }));
    await ensureFieldConfigs(model, ctx.catalog);

    const problem = saveBlocker(model, ctx.catalog) || fieldConfigProblem(model);
    if (problem) {
      saveBtn.disabled = false;
      rebuildPanels(); // surface any field-config the auto-derive DID populate
      mount(saveStatus, el("span", { class: "strip error", text: problem }));
      return;
    }

    mount(saveStatus, el("span", { class: "muted", text: "Saving…" }));

    const body = {
      name: model.name.trim(),
      description: model.description ? model.description : null,
      definition: modelToDefinition(model),
    };

    let res;
    if (ctx.editingId != null) {
      body.version = ctx.loadedVersion;
      res = await api.updateView(ctx.editingId, body);
    } else {
      res = await api.createView(body);
    }

    if (res.kind === "data" && res.data) {
      location.hash = "#/view/" + encodeURIComponent(res.data.id);
      return;
    }

    saveBtn.disabled = false;
    handleSaveError(res, saveStatus, ctx, main);
  }

  mount(main, [
    el("div", { class: "page-head" }, [
      el("a", { href: backHash, text: "← Back" }),
      el("h2", { text: ctx.editingId != null ? "Edit view" : "New view" }),
    ]),
    el("div", { class: "editor-meta" }, [field("Name", nameInput), field("Description", descInput)]),
    viewScopeSection,
    el("h3", { class: "section-title", text: "Panels" }),
    panelsBox,
    addBtn,
    el("div", { class: "editor-footer" }, [saveBtn, cancelBtn, saveStatus]),
  ]);

  rebuildPanels();
}

/* On a 409 while EDITING, the row changed under us (stale version) or the name now collides — either way, offer
   to reload the latest (discarding local edits) so the operator re-applies against fresh state. */
function handleSaveError(res, saveStatus, ctx, main) {
  const parts = [el("span", { text: res.message || "Could not save the view." })];
  if (res.status === 409 && ctx.editingId != null) {
    const reload = el("button", { class: "btn small", type: "button", text: "Reload latest" });
    reload.addEventListener("click", () => renderEditor(main, ctx.editingId));
    parts.push(reload);
  }
  mount(saveStatus, el("div", { class: "strip error" }, parts));
}

/** The reason SAVE is blocked (name/panels + each panel's prerequisites), or null when the view is savable. This is
 *  the button-DISABLING gate; the field-config requirement is enforced separately in onSave (see
 *  fieldConfigProblem) AFTER an auto-derive pass, so the Save click is never blocked from running that derive. */
function saveBlocker(model, catalog) {
  if (!model.name || !model.name.trim()) return "Enter a view name.";
  if (!model.panels.length) return "Add at least one panel.";
  for (let i = 0; i < model.panels.length; i++) {
    const p = model.panels[i];
    const n = i + 1;
    const problem = p.kind === "source" ? composedBlocker(p, catalog) : readBlocker(p, catalog);
    if (problem) return "Panel " + n + ": " + problem;
  }
  return null;
}

/** A read panel's SAVE blocker (read/viz/required-params), or null. */
function readBlocker(p, catalog) {
  if (!p.read) return "choose a read.";
  if (!p.viz) return "choose a visualization.";
  const missing = missingRequired(p, catalog);
  if (missing.length) return "fill required " + missing.join(", ") + ".";
  return null;
}

/** A composed panel's SAVE blocker — the coherence rules mirrored from the server so the author gets an inline reason
 *  (the run endpoint stays the authority; anything subtler surfaces as a preview error). */
export function composedBlocker(p, catalog) {
  const measure = composedMeasure(p, catalog);
  if (!measure) return "choose a measure.";
  const isRatio = measure.kind === "ratio";
  if (!isRatio && !p.aggregate) return "choose an aggregate.";
  if (!p.viz) return "choose a chart type.";
  const groups = (p.groupBy || []).length;
  if (p.shape === "scalar" && groups > 0) return "a single value can't group — switch to Over time or Ranked.";
  if (p.shape === "ranked" && groups === 0) return "a ranked chart needs a group-by (the categories to rank).";
  if (p.shape === "topseries" && groups === 0) return "a top-N over time needs a group-by (the series to rank and keep).";
  const reason = vizModeReason(p.viz, p.shape, groups, p.overlay);
  if (reason) return reason;
  return null;
}

/** Which read-panel vizzes need a load-bearing field array: table -> columns, line -> series, stat -> stats.
 *  Composed panels never do (their fields come from the measure/group-by), so they are excluded here. */
function needsFieldConfig(p) {
  if (p.kind === "source") return false;
  return p.viz === "table" || p.viz === "line" || p.viz === "stat";
}

/** The panel's load-bearing field array for its viz, or null for a viz that has none. */
function fieldArray(p) {
  const c = p.vizcfg || {};
  if (p.viz === "table") return c.columns;
  if (p.viz === "line") return c.series;
  if (p.viz === "stat") return c.stats;
  return null;
}

/** Whether a table/line/stat panel has at least one configured field. */
function hasFieldConfig(p) {
  const arr = fieldArray(p);
  return Array.isArray(arr) && arr.length > 0;
}

/* Auto-derive-on-save feeder (B1): fetch a live sample for each field-less table/line/stat panel and seed its
   vizcfg via derive.js, so the author rarely hits the fieldConfigProblem backstop. Skips a panel with no read / an
   unfilled required param / no live sample — those the backstop reports. */
async function ensureFieldConfigs(model, catalog) {
  for (const p of model.panels) {
    if (!needsFieldConfig(p) || hasFieldConfig(p)) continue;
    if (!p.read || missingRequired(p, catalog).length) continue;
    const res = await readTool(p.read, cleanParams(p.params));
    if (res.kind === "data" && res.data) {
      p.vizcfg = derive.deriveVizConfig(res.data, p.viz, SERIES_COLORS);
    }
  }
}

/** The reason SAVE's field-config backstop trips (a table/line/stat panel still without fields), or null. */
function fieldConfigProblem(model) {
  for (let i = 0; i < model.panels.length; i++) {
    const p = model.panels[i];
    if (needsFieldConfig(p) && !hasFieldConfig(p)) {
      return (
        "Panel " + (i + 1) + ": add at least one field (use Auto-detect fields) or adjust the read so a sample is available."
      );
    }
  }
  return null;
}

/* ─────────────────────────── one panel editor ─────────────────────────── */

/* A panel editor is dispatched on kind: a composed (v2) metric editor or a read (v1) editor. Both share the head
   (Panel N + reorder/remove) and a "Composed / Read" kind toggle; switching kind swaps the panel to that kind's
   default shape (title + width are carried over) and rebuilds. */
function buildPanelEditor(p, index, ctx) {
  const kindToggle = buildKindToggle(p, index, ctx);
  return p.kind === "source"
    ? buildComposedPanelEditor(p, index, ctx, kindToggle)
    : buildReadPanelEditor(p, index, ctx, kindToggle);
}

/* The segmented "Composed metric / Read" toggle. Switching preserves title + width and resets the rest to the new
   kind's defaults (the two shapes share almost nothing), then rebuilds every panel so the swapped body renders. */
function buildKindToggle(p, index, ctx) {
  const seg = el("div", { class: "kind-toggle", role: "group", "aria-label": "Panel data source" });
  const opts = [
    { kind: "source", label: "Composed metric" },
    { kind: "read", label: "Read (advanced)" },
  ];
  for (const o of opts) {
    const active = (p.kind || "source") === o.kind;
    const btn = el("button", {
      class: "kind-opt" + (active ? " active" : ""),
      type: "button",
      text: o.label,
      "aria-pressed": active ? "true" : "false",
    });
    btn.addEventListener("click", () => {
      if ((p.kind || "source") === o.kind) return;
      const carried = { span: p.span || 1, title: p.title || "" };
      const next = o.kind === "source" ? newComposedPanel() : newReadPanel();
      Object.assign(next, carried);
      ctx.model.panels[index] = next;
      ctx.rebuildPanels();
    });
    seg.appendChild(btn);
  }
  return seg;
}

function buildReadPanelEditor(p, index, ctx, kindToggle) {
  const { catalog, fleet, model } = ctx;
  let lastSample = null;
  let previewTimer = null;

  const paramsBox = el("div", { class: "params-box" });
  const vizcfgBox = el("div", { class: "vizcfg-box" });
  const previewBox = el("div", { class: "panel-preview" });
  const bodyBox = el("div", { class: "panel-editor-body" });
  const headSub = el("span", { class: "panel-editor-sub" });

  const readSelect = buildReadSelect(catalog, p.read);
  readSelect.addEventListener("change", async () => {
    p.read = readSelect.value;
    p.params = defaultParams(catalog, p.read);
    p.vizcfg = {};
    rebuildParams();
    rebuildBody(); // reveal Parameters (or the "choose a read" hint) as prerequisites are met (M5)
    refreshHead();
    ctx.refreshSaveState();
    await refreshSampleAndDerive({ resetViz: true });
    rebuildBody(); // a viz may now be auto-suggested -> reveal Fields + Preview
    schedulePreview();
  });

  const vizSelect = buildVizSelect(catalog, p.viz);
  vizSelect.addEventListener("change", () => {
    p.viz = vizSelect.value;
    if (lastSample) p.vizcfg = derive.deriveVizConfig(lastSample, p.viz, SERIES_COLORS);
    rebuildVizcfg();
    rebuildBody(); // reveal Fields + Preview once a viz is chosen (M5)
    ctx.refreshSaveState();
    schedulePreview();
  });

  const spanSelect = el("select", { class: "editor-select", "aria-label": "Panel width" }, [
    el("option", { value: "1", text: "1 column" }),
    el("option", { value: "2", text: "2 columns (wide)" }),
  ]);
  spanSelect.value = String(p.span || 1);
  spanSelect.addEventListener("change", () => {
    p.span = spanSelect.value === "2" ? 2 : 1;
    schedulePreview();
  });

  const titleInput = el("input", { class: "editor-input", type: "text", placeholder: "Panel title", "aria-label": "Panel title" });
  titleInput.value = p.title;
  titleInput.addEventListener("input", () => {
    p.title = titleInput.value;
    refreshHead();
    schedulePreview();
  });

  /* "Auto-detect fields" first, then "Re-detect from sample" once a config exists — its real job is re-inspect-
     and-rebuild (it DISCARDS manual field edits), so the label distinguishes the first detect from a re-detect. */
  const autoBtn = el("button", { class: "btn small", type: "button" });
  function refreshAutoBtn() {
    autoBtn.textContent = hasVizcfg(p) ? "Re-detect from sample" : "Auto-detect fields";
  }
  autoBtn.addEventListener("click", async () => {
    await refreshSampleAndDerive({ resetViz: false, force: true });
    rebuildBody();
    schedulePreview();
  });
  const fieldsHelp = el("div", {
    class: "block-help",
    text: "Detected from a live sample; edit below or re-detect.",
  });

  const upBtn = el("button", { class: "btn small icon", type: "button", text: "↑", title: "Move up", "aria-label": "Move panel up" });
  const downBtn = el("button", { class: "btn small icon", type: "button", text: "↓", title: "Move down", "aria-label": "Move panel down" });
  const removeBtn = el("button", { class: "btn small danger", type: "button", text: "Remove", "aria-label": "Remove panel" });
  upBtn.disabled = index === 0;
  downBtn.disabled = index === model.panels.length - 1;
  upBtn.addEventListener("click", () => {
    swap(model.panels, index, index - 1);
    ctx.rebuildPanels();
  });
  downBtn.addEventListener("click", () => {
    swap(model.panels, index, index + 1);
    ctx.rebuildPanels();
  });
  removeBtn.addEventListener("click", () => {
    model.panels.splice(index, 1);
    if (!model.panels.length) model.panels.push(newPanel());
    ctx.rebuildPanels();
  });

  /* Stable section nodes composed by rebuildBody() — created once so their inputs keep focus/state across a
     progressive-disclosure rebuild. */
  const identityGrid = el("div", { class: "editor-grid" }, [
    field("Read", readSelect),
    field("Visualization", vizSelect),
    field("Width", spanSelect),
    field("Title", titleInput, "field-title"),
  ]);
  const paramsSection = labeledBlock("Parameters", paramsBox);
  const fieldsSection = labeledBlock(
    "Fields",
    el("div", { class: "vizcfg-wrap" }, [fieldsHelp, el("div", { class: "vizcfg-actions" }, [autoBtn]), vizcfgBox])
  );
  const previewSection = labeledBlock("Preview", previewBox);
  previewSection.classList.add("panel-preview-col");

  function rebuildParams() {
    mount(
      paramsBox,
      buildParamInputs(p, catalog, fleet, () => {
        refreshHead(); // a `server` param feeds the panel-header echo
        ctx.refreshSaveState();
        schedulePreview();
        maybeDeriveOnParamChange();
      })
    );
  }

  function rebuildVizcfg() {
    mount(vizcfgBox, buildVizcfgEditor(p, lastSample, schedulePreview));
    refreshAutoBtn();
  }

  /* The panel-header echo: "Panel N · <title or read> (<server param>)" so a long list of panels is scannable. */
  function refreshHead() {
    const label = p.title || p.read || "";
    const server = serverParamValue(p, catalog);
    let text = "";
    if (label) text += " · " + label;
    if (server) text += " (" + server + ")";
    headSub.textContent = text;
  }

  /* Progressive disclosure (M5) + two-column layout (M2): the identity row is always shown; Parameters appears
     once a read is chosen and Fields + the live Preview once a viz is chosen. With a preview present the body is a
     two-column grid (config left, sticky preview right) that collapses to stacked under ~900px. */
  function rebuildBody() {
    const config = [identityGrid];
    if (!p.read) {
      config.push(el("div", { class: "panel-hint", text: "Choose a read to begin." }));
    } else {
      config.push(paramsSection);
      if (p.viz) {
        config.push(fieldsSection);
      } else {
        config.push(el("div", { class: "panel-hint", text: "Choose a visualization to configure its fields and preview." }));
      }
    }
    const hasPreview = !!(p.read && p.viz);
    const kids = [el("div", { class: "panel-config" }, config)];
    if (hasPreview) kids.push(previewSection);
    bodyBox.className = "panel-editor-body" + (hasPreview ? " has-preview" : "");
    mount(bodyBox, kids);
  }

  /* If the operator finishes a previously-missing required param and there is no vizcfg yet, derive one. */
  async function maybeDeriveOnParamChange() {
    if (!p.read || hasVizcfg(p)) return;
    if (missingRequired(p, catalog).length) return;
    await refreshSampleAndDerive({ resetViz: !p.viz });
    rebuildBody();
  }

  /* Fetch one live sample with the current params and (re)seed the vizcfg from it. Skips when a required param
     is unfilled (nothing to fetch yet). `force` re-derives even when a config already exists (the button). */
  async function refreshSampleAndDerive({ resetViz, force }) {
    if (!p.read) return;
    if (missingRequired(p, catalog).length) {
      lastSample = null;
      return;
    }
    const res = await readTool(p.read, cleanParams(p.params));
    if (res.kind !== "data" || !res.data) {
      lastSample = null;
      if (force) mount(vizcfgBox, el("div", { class: "muted", text: sampleUnavailableText(res) }));
      return;
    }
    lastSample = res.data;
    if (resetViz || !p.viz) {
      p.viz = derive.suggestViz(lastSample);
      vizSelect.value = p.viz;
    }
    if (force || !hasVizcfg(p)) {
      p.vizcfg = derive.deriveVizConfig(lastSample, p.viz, SERIES_COLORS);
    }
    rebuildVizcfg();
    ctx.refreshSaveState();
  }

  /* Prime a sample for an EXISTING panel (populates the field dropdowns) WITHOUT clobbering its saved config. */
  async function primeSample() {
    if (!p.read || missingRequired(p, catalog).length) return;
    const res = await readTool(p.read, cleanParams(p.params));
    if (res.kind === "data" && res.data) {
      lastSample = res.data;
      rebuildVizcfg();
    }
  }

  function schedulePreview() {
    clearTimeout(previewTimer);
    previewTimer = setTimeout(renderPreview, PREVIEW_DEBOUNCE_MS);
  }

  function renderPreview() {
    const problem = previewBlocker(p, catalog);
    if (problem) {
      mount(previewBox, el("div", { class: "strip empty", text: problem }));
      return;
    }
    mount(previewBox, renderPanel(panelToDesc(p)));
  }

  rebuildParams();
  rebuildVizcfg();
  refreshHead();
  rebuildBody();
  primeSample();
  schedulePreview();

  return el("div", { class: "panel-editor card" }, [
    el("div", { class: "panel-editor-head" }, [
      el("span", { class: "panel-editor-title", text: "Panel " + (index + 1) }),
      headSub,
      el("div", { class: "spacer" }),
      kindToggle,
      upBtn,
      downBtn,
      removeBtn,
    ]),
    bodyBox,
  ]);
}

/** The reason a panel can't PREVIEW (missing read/viz/required param), or null when it can render. */
function previewBlocker(p, catalog) {
  if (!p.read) return "Choose a read to preview this panel.";
  if (!p.viz) return "Choose a visualization to preview this panel.";
  const missing = missingRequired(p, catalog);
  if (missing.length) {
    return "Fill required parameter" + (missing.length > 1 ? "s" : "") + ": " + missing.join(", ") + ".";
  }
  return null;
}

function sampleUnavailableText(res) {
  if (res.kind === "empty") return "No sample rows in this window yet — add fields manually or adjust the parameters.";
  if (res.kind === "error") return "Could not fetch a sample (" + res.message + ") — add fields manually.";
  return "No sample data — add fields manually.";
}

/* ─────────────────────────── composed (v2) panel editor ─────────────────────────── */

/* Display maps for the compose vocabularies (data-driven off /api/catalog; these only humanize the wire tokens). */
const VIZ_LABELS = {
  scatter: "Scatter (two measures)", line: "Line", area: "Area", stacked: "Stacked area", "stacked-bar": "Stacked bar", bar: "Bar", pie: "Pie / donut", table: "Table", stat: "Single value" };
const AGG_LABELS = { sum: "Sum", avg: "Average", min: "Minimum", max: "Maximum", count: "Count", percentile_cont: "95th percentile" };
const OP_LABELS = { eq: "is (=)", neq: "is not (≠)", like: "matches (LIKE)", gt: ">", gte: "≥", lt: "<", lte: "≤" };
const BUCKET_LABELS = { minute: "Per minute", hour: "Per hour", day: "Per day", auto: "Auto (fit to range)" };
const ARCHETYPE_LABELS = { Cumulative: "counter", Delta: "per-interval delta", Gauge: "gauge", PerEvent: "per-event" };
const SHAPE_LABELS = { timeseries: "Over time", topseries: "Top N over time", ranked: "Ranked", scalar: "Single value" };

/*
 * The v2 metric composer for one panel: pick a measure (or ratio), an aggregate, a SHAPE (over time / ranked /
 * single value ⇒ the timeBucket|topN mode), optional group-by + filters, then a chart coherent with that shape.
 * The live preview POSTs the composed spec to /api/compose/run (through compose.js) against the view's preview
 * scope. Progressive disclosure: only the metric picker shows until a measure is chosen; unit / per-panel time /
 * the percentile note live under an Advanced disclosure. The whole config re-renders on a structural change
 * (measure/aggregate/shape/group/viz) so dependent controls stay coherent; free-text edits update in place.
 */
function buildComposedPanelEditor(p, index, ctx, kindToggle) {
  const { catalog, model } = ctx;
  const headSub = el("span", { class: "panel-editor-sub" });

  const upBtn = el("button", { class: "btn small icon", type: "button", text: "↑", title: "Move up", "aria-label": "Move panel up" });
  const downBtn = el("button", { class: "btn small icon", type: "button", text: "↓", title: "Move down", "aria-label": "Move panel down" });
  const removeBtn = el("button", { class: "btn small danger", type: "button", text: "Remove", "aria-label": "Remove panel" });
  upBtn.disabled = index === 0;
  downBtn.disabled = index === model.panels.length - 1;
  upBtn.addEventListener("click", () => {
    swap(model.panels, index, index - 1);
    ctx.rebuildPanels();
  });
  downBtn.addEventListener("click", () => {
    swap(model.panels, index, index + 1);
    ctx.rebuildPanels();
  });
  removeBtn.addEventListener("click", () => {
    model.panels.splice(index, 1);
    if (!model.panels.length) model.panels.push(newPanel());
    ctx.rebuildPanels();
  });

  function refreshHead() {
    const m = composedMeasure(p, catalog);
    const label = p.title || (m ? m.displayName : "");
    headSub.textContent = label ? " · " + label : "";
  }

  /* The dashboard wraps the shared composer body in a panel head (kind toggle + reorder/remove). The body owns its
     own config rebuild + debounced preview; onChange only refreshes the head echo + the view-level save state. */
  const body = buildComposedPanelBody(p, {
    catalog,
    getVariables: () => ctx.model.variables,
    getScopeServer: () => scopeServerPlatform(ctx.model.variables, ctx.fleet),
    previewScope: ctx.previewScope,
    onChange: () => {
      refreshHead();
      ctx.refreshSaveState();
    },
    showWidth: true,
  });

  refreshHead();

  return el("div", { class: "panel-editor card" }, [
    el("div", { class: "panel-editor-head" }, [
      el("span", { class: "panel-editor-title", text: "Panel " + (index + 1) }),
      headSub,
      el("div", { class: "spacer" }),
      kindToggle,
      upBtn,
      downBtn,
      removeBtn,
    ]),
    body,
  ]);
}

/**
 * The composed (v2) panel COMPOSER BODY — the config form (metric / shape / group / filters / chart / advanced /
 * identity) plus the live WYSIWYG preview — extracted from buildComposedPanelEditor (#1563 D7) so it is shared
 * VERBATIM by BOTH the dashboard composer (wrapped in a panel head with a kind toggle + reorder/remove) and the
 * notebook composer (wrapped in a cell head). Returns the `.panel-editor-body` node; the caller owns the head.
 *
 * opts:
 *   catalog       — the compose catalog (compose = catalog.compose).
 *   getVariables  — () => the view/notebook's declared template variables (for the filter "$name" hints).
 *   getScopeServer— () => the concrete fleet card the view's $server resolves to (D4 per-server greying), or null
 *                   for All / multi / unprobed / unknown; omitted ⇒ never grey (only the Wave-1 badges show).
 *   previewScope  — () => the scope the live preview runs against (whole fleet + default range + variable defaults).
 *   onChange      — () => notify the wrapper a field changed (refresh its head echo + save state); the body owns
 *                   its own config rebuild + debounced preview, so onChange never rebuilds anything itself.
 *   showWidth     — show the Width (span 1|2) control; false for a notebook cell (single-column document flow).
 */
export function buildComposedPanelBody(p, opts) {
  const catalog = opts.catalog;
  const compose = catalog.compose || {};
  const getVariables = opts.getVariables || (() => []);
  const getScopeServer = opts.getScopeServer || (() => null);
  const previewScope = opts.previewScope || (() => ({}));
  const onChange = opts.onChange || (() => {});
  const showWidth = opts.showWidth !== false;
  let previewTimer = null;

  const bodyBox = el("div", { class: "panel-editor-body" });
  const previewBox = el("div", { class: "panel-preview" });
  const previewSection = labeledBlock("Preview", previewBox);
  previewSection.classList.add("panel-preview-col");

  function schedulePreview() {
    clearTimeout(previewTimer);
    previewTimer = setTimeout(renderComposedPreview, PREVIEW_DEBOUNCE_MS);
  }

  function renderComposedPreview() {
    const problem = composedPreviewBlocker(p, catalog);
    if (problem) {
      mount(previewBox, el("div", { class: "strip empty", text: problem }));
      return;
    }
    /* Render the exact saved card (title + framing) against the preview scope — WYSIWYG. */
    mount(previewBox, renderComposedPanelCard(composedPanelToDesc(p), previewScope()));
  }

  /* A structural change (measure/aggregate/shape/group/viz) may reveal or retune dependent controls — re-render
     the config. A free-text / live change (title/unit/topN/bucket/filters/time) only refreshes save + preview. */
  function onStructural() {
    onChange();
    rebuildBody();
    schedulePreview();
  }
  function onLive() {
    onChange();
    schedulePreview();
  }

  /* ── config sub-builders (fresh nodes each rebuild, read from the model) ── */

  function measureBlock(m) {
    /* D4: the concrete fleet server the view's $server resolves to (or null for All / multi / unprobed / unknown) —
       read live each rebuild so re-scoping re-greys the picker. */
    const scopeServer = getScopeServer();
    const sel = buildComposedMeasureSelect(compose, currentMeasureKey(p), scopeServer, (key) => {
      applyMeasureChoice(p, key, compose);
      onStructural();
    });
    return el("div", {}, [
      sel,
      m ? el("div", { class: "measure-caption", text: measureCaption(m, compose) }) : null,
      m ? measureAvailabilityNote(m, scopeServer) : null,
    ]);
  }

  function shapeAndAggBlock(m) {
    const kids = [];
    if (m.kind === "ratio") {
      kids.push(field("Aggregation", el("div", { class: "static-field muted", text: "Ratio (numerator ÷ denominator)" })));
    } else {
      kids.push(field("Aggregate", aggregateSelect(m)));
    }
    kids.push(field("Shape", shapeSegmented()));
    if (p.shape === "timeseries") kids.push(field("Bucket", bucketSelect()));
    else if (p.shape === "ranked") kids.push(field("Top N", topNInput()));
    else if (p.shape === "topseries") {
      kids.push(field("Bucket", bucketSelect()));
      kids.push(field("Top N", topNInput()));
      kids.push(field("Show rest", includeOtherToggle()));
    }
    return el("div", { class: "composed-fields" }, kids);
  }

  function aggregateSelect(m) {
    if (!p.aggregate) p.aggregate = m.defaultAggregate || (m.validAggregates || [])[0] || "";
    const sel = el("select", { class: "editor-select", "aria-label": "Aggregate" });
    for (const a of m.validAggregates || []) sel.appendChild(el("option", { value: a, text: AGG_LABELS[a] || a }));
    sel.value = p.aggregate || "";
    sel.addEventListener("change", () => {
      p.aggregate = sel.value;
      if (p.aggregate === "count") p.unit = "count";
      else if (p.unit === "count") p.unit = m.defaultUnit || "";
      onStructural();
    });
    return sel;
  }

  function shapeSegmented() {
    const seg = el("div", { class: "seg-control", role: "group", "aria-label": "Panel shape" });
    for (const v of ["timeseries", "topseries", "ranked", "scalar"]) {
      const active = p.shape === v;
      const btn = el("button", { class: "seg-opt" + (active ? " active" : ""), type: "button", text: SHAPE_LABELS[v], "aria-pressed": active ? "true" : "false" });
      btn.addEventListener("click", () => {
        if (p.shape === v) return;
        p.shape = v;
        if ((v === "timeseries" || v === "topseries") && !p.timeBucket) p.timeBucket = "auto";
        if ((v === "ranked" || v === "topseries") && !p.topN) p.topN = 10;
        if (v === "scalar") p.groupBy = [];
        if (!vizShapeCompatible(p.viz, p.shape)) p.viz = DEFAULT_VIZ_FOR_SHAPE[p.shape];
        onStructural();
      });
      seg.appendChild(btn);
    }
    return seg;
  }

  function bucketSelect() {
    const sel = el("select", { class: "editor-select", "aria-label": "Time bucket" });
    for (const b of (compose.timeBuckets || []).filter((x) => x !== "none")) {
      sel.appendChild(el("option", { value: b, text: BUCKET_LABELS[b] || b }));
    }
    sel.value = p.timeBucket || "auto";
    sel.addEventListener("change", () => {
      p.timeBucket = sel.value;
      onLive();
    });
    return sel;
  }

  function topNInput() {
    const inp = el("input", { class: "editor-input", type: "number", step: "1", min: "1", max: "1000", "aria-label": "Top N" });
    inp.value = String(p.topN || 10);
    inp.addEventListener("input", () => {
      p.topN = clampInt(inp.value, 1, 1000, 10);
      onLive();
    });
    return inp;
  }

  /* #2734: fold everything outside the top N into one "(other)" series, so the buckets still sum to the
     window total. Off by default — on, the chart is honest about the whole; off, it shows only the winners. */
  function includeOtherToggle() {
    const box = el("input", { type: "checkbox", class: "editor-check", "aria-label": "Include an (other) series for the rest" });
    box.checked = p.includeOther === true;
    box.addEventListener("change", () => {
      p.includeOther = box.checked;
      onLive();
    });
    return box;
  }

  function groupByBlock(m) {
    const dims = dimNamesForMeasure(m, compose);
    const disabled = p.shape === "scalar";
    const chips = dims.map((d) => {
      const on = (p.groupBy || []).includes(d);
      const btn = el("button", { class: "chip" + (on ? " on" : ""), type: "button", text: dimLabel(d), "aria-pressed": on ? "true" : "false" });
      btn.disabled = disabled;
      btn.addEventListener("click", () => {
        const idx = p.groupBy.indexOf(d);
        if (idx >= 0) p.groupBy.splice(idx, 1);
        else p.groupBy.push(d);
        onStructural();
      });
      return btn;
    });
    const row = el("div", { class: "chip-row" }, chips.length ? chips : [el("span", { class: "muted", text: "This measure has no group-by dimensions." })]);
    const hint = disabled
      ? el("div", { class: "block-help", text: "A single value can't group. Switch to Over time or Ranked to group." })
      : databaseGrainHint(m, compose);
    return el("div", {}, [row, hint]);
  }

  function filtersBlock(m) {
    const editor = buildComposedFiltersEditor(p, m, compose, onLive);
    const declared = (getVariables() || []).filter((v) => v.name).map((v) => "$" + v.name);
    const help = declared.length
      ? el("div", { class: "block-help", text: "Values are literals (comma-separated for is/is-not); reference a view variable as " + declared.join(", ") + "." })
      : el("div", { class: "block-help", text: "Values are literals (comma-separated for is/is-not). Declare a view variable to filter by $name." });
    return el("div", {}, [editor, help]);
  }

  function chartBlock(m) {
    const sel = buildComposedVizSelect(compose, p.shape, p.viz, (v) => {
      p.viz = v;
      onStructural();
    });
    const reason = vizModeReason(p.viz, p.shape, (p.groupBy || []).length, p.overlay);
    const kids = [field("Chart type", sel)];
    /* Second measure (#1606): scatter's y axis, or an ungrouped line/area's right-hand axis. Only offered
       where the server accepts one; the model keeps it across viz flips (composedPanelToDesc gates it). */
    if (p.viz === "scatter" || ((p.viz === "line" || p.viz === "area") && p.shape === "timeseries")) {
      kids.push(field(p.viz === "scatter" ? "Second measure (y axis)" : "Second measure (right axis)", overlayControl()));
    }
    if (reason) kids.push(el("div", { class: "block-help warn-hint", text: reason }));
    return el("div", {}, kids);
  }

  function overlayControl() {
    const sameSource = (compose.measures || []).filter((x) => x.source === p.source);
    const sel = el("select", { class: "editor-select", "aria-label": "Second measure" });
    sel.appendChild(el("option", { value: "", text: "— none —" }));
    for (const x of sameSource.sort((a, b) => a.displayName.localeCompare(b.displayName))) {
      sel.appendChild(el("option", { value: x.key, text: x.displayName + (x.kind === "ratio" ? " (ratio)" : "") }));
    }
    sel.value = p.overlay && p.overlay.measure ? p.overlay.measure : "";
    sel.addEventListener("change", () => {
      if (!sel.value) {
        p.overlay = null;
      } else {
        const chosen = sameSource.find((x) => x.key === sel.value);
        /* A ratio's aggregation is fixed (the server rejects an explicit one); a scalar takes its default. */
        p.overlay = {
          measure: sel.value,
          aggregate: chosen && chosen.kind !== "ratio" ? chosen.defaultAggregate || (chosen.validAggregates || [])[0] || "" : "",
          unit: chosen ? chosen.defaultUnit || "" : "",
        };
      }
      onStructural();
    });
    const note = p.overlay && p.overlay.measure
      ? el("div", { class: "block-help", text: "Aggregated with its default; the second axis carries its own unit." })
      : null;
    return el("div", {}, [sel, note]);
  }

  function advancedBlock(m) {
    const rows = [field("Unit / magnitude", unitControl(m)), field("Time range", timeOverrideControl())];
    if (vizDrawsThresholds(p.viz)) rows.push(field("Reference lines", thresholdsControl(m)));
    /* Event markers (design D5) overlay only on a time-series panel — the run endpoint rejects them otherwise. */
    if (p.shape === "timeseries" || p.shape === "topseries") rows.push(field("Event markers", annotationsControl()));
    if (p.aggregate === "percentile_cont") rows.push(el("div", { class: "block-help", text: "Percentile is fixed at p95." }));
    return el("details", { class: "composed-advanced" }, [el("summary", { text: "Advanced" }), el("div", { class: "cfg" }, rows)]);
  }

  /* Up to MAX_ANNOTATIONS event-annotation sources (design D5) to overlay as vertical markers on a time chart, a
     multi-select of chips off the catalog's annotationSources (data-driven — never a hardcoded list). Toggling a
     chip is a LIVE change (a local redraw so the Advanced section stays open + keeps its scroll), never a structural
     rebuild. A constrained source (appliesTo, D4) carries its availability as the chip's hover title. */
  function annotationsControl() {
    if (!Array.isArray(p.annotations)) p.annotations = [];
    const sources = compose.annotationSources || [];
    if (!sources.length) {
      return el("div", { class: "static-field muted", text: "No annotation sources are available." });
    }
    const box = el("div", { class: "annotations-picker" });

    function redraw() {
      const chips = sources.map((a) => {
        const on = p.annotations.includes(a.key);
        const avail = annotationAvailabilitySummary(a);
        const btn = el("button", { class: "chip" + (on ? " on" : ""), type: "button", text: a.displayName, "aria-pressed": on ? "true" : "false", title: avail || null });
        btn.disabled = !on && p.annotations.length >= MAX_ANNOTATIONS;
        btn.addEventListener("click", () => {
          const idx = p.annotations.indexOf(a.key);
          if (idx >= 0) p.annotations.splice(idx, 1);
          else if (p.annotations.length < MAX_ANNOTATIONS) p.annotations.push(a.key);
          redraw();
          onLive();
        });
        return btn;
      });
      mount(box, el("div", { class: "chip-row" }, chips));
    }

    redraw();
    const help = el("div", {
      class: "block-help",
      text: "Overlay event markers (deadlocks, blocked-process reports, …) as vertical lines on this time chart. Up to " + MAX_ANNOTATIONS + " sources.",
    });
    return el("div", {}, [box, help]);
  }

  /* Up to 4 render-only threshold reference lines (design D3), in the panel's unit. A value edit / add / remove is a
     LIVE change (its own local redraw so the number inputs keep focus) — never a structural rebuild. Blank/invalid
     inputs are stored as null and dropped by cleanThresholds on save + preview, so a half-typed value never renders. */
  function thresholdsControl(m) {
    if (!Array.isArray(p.thresholds)) p.thresholds = [];
    const list = p.thresholds;
    const box = el("div", { class: "item-list thresholds" });

    function redraw() {
      const rows = list.map((val, i) => {
        const inp = el("input", { class: "editor-input", type: "number", step: "any", "aria-label": "reference line value " + (i + 1) });
        inp.value = val == null ? "" : String(val);
        inp.addEventListener("input", () => {
          const n = parseFloat(inp.value);
          list[i] = inp.value.trim() === "" || isNaN(n) ? null : n;
          onLive();
        });
        const rm = el("button", { class: "btn small danger icon", type: "button", text: "×", title: "Remove reference line", "aria-label": "remove reference line" });
        rm.addEventListener("click", () => {
          list.splice(i, 1);
          redraw();
          onLive();
        });
        return el("div", { class: "item-row" }, [inp, rm]);
      });
      const addBtn = el("button", { class: "btn small", type: "button", text: "+ Add reference line" });
      addBtn.disabled = list.length >= 4;
      addBtn.addEventListener("click", () => {
        list.push(null);
        redraw();
        onLive();
      });
      mount(box, [...rows, addBtn]);
    }

    redraw();
    const unitText = p.aggregate === "count" ? "count" : p.unit || m.defaultUnit || "";
    const help = el("div", {
      class: "block-help",
      text: unitText
        ? "Dashed guide lines drawn at these values (in " + unitText + "). Up to 4; values outside the chart's range are skipped."
        : "Dashed guide lines drawn at these values. Up to 4; values outside the chart's range are skipped.",
    });
    return el("div", {}, [box, help]);
  }

  function unitControl(m) {
    if (p.aggregate === "count") return el("div", { class: "static-field muted", text: "Count is unitless." });
    const fam = (compose.unitFamilies || []).find((f) => f.name === m.unitFamily);
    const units = fam && fam.units.length ? fam.units.map((u) => u.name) : [m.defaultUnit];
    if (!p.unit || !units.includes(p.unit)) p.unit = m.defaultUnit;
    const sel = el("select", { class: "editor-select", "aria-label": "Unit" });
    for (const u of units) sel.appendChild(el("option", { value: u, text: u }));
    sel.value = p.unit;
    sel.addEventListener("change", () => {
      p.unit = sel.value;
      onLive();
    });
    return sel;
  }

  function timeOverrideControl() {
    const sel = el("select", { class: "editor-select", "aria-label": "Per-panel time range" });
    sel.appendChild(el("option", { value: "", text: "Use view default" }));
    for (const r of RANGE_OPTIONS) sel.appendChild(el("option", { value: String(r.hours), text: r.label }));
    sel.value = p.hours != null ? String(p.hours) : "";
    sel.addEventListener("change", () => {
      p.hours = sel.value ? parseInt(sel.value, 10) : null;
      onLive();
    });
    return sel;
  }

  function identityBlock() {
    const titleInput = el("input", { class: "editor-input", type: "text", placeholder: "Panel title", "aria-label": "Panel title" });
    titleInput.value = p.title || "";
    titleInput.addEventListener("input", () => {
      p.title = titleInput.value;
      onChange();
    });
    const fields = [field("Title", titleInput, "field-title")];
    if (showWidth) {
      const spanSelect = el("select", { class: "editor-select", "aria-label": "Panel width" }, [
        el("option", { value: "1", text: "1 column" }),
        el("option", { value: "2", text: "2 columns (wide)" }),
      ]);
      spanSelect.value = String(p.span || 1);
      spanSelect.addEventListener("change", () => {
        p.span = spanSelect.value === "2" ? 2 : 1;
      });
      fields.push(field("Width", spanSelect));
    }
    return el("div", { class: "composed-fields" }, fields);
  }

  function rebuildBody() {
    const m = composedMeasure(p, catalog);
    const config = [labeledBlock("Metric", measureBlock(m))];
    if (!m) {
      config.push(el("div", { class: "panel-hint", text: "Choose a metric to begin." }));
    } else {
      config.push(shapeAndAggBlock(m));
      config.push(labeledBlock("Group by", groupByBlock(m)));
      config.push(labeledBlock("Filters", filtersBlock(m)));
      config.push(labeledBlock("Chart", chartBlock(m)));
      config.push(advancedBlock(m));
      config.push(identityBlock());
    }
    const hasPreview = !!(m && p.viz);
    const kids = [el("div", { class: "panel-config" }, config)];
    if (hasPreview) kids.push(previewSection);
    bodyBox.className = "panel-editor-body" + (hasPreview ? " has-preview" : "");
    mount(bodyBox, kids);
  }

  rebuildBody();
  schedulePreview();
  return bodyBox;
}

/** The reason a composed panel can't PREVIEW yet (missing measure/aggregate/chart); subtler run errors surface in
 *  the preview body itself (the run endpoint is the authority). */
function composedPreviewBlocker(p, catalog) {
  const m = composedMeasure(p, catalog);
  if (!m) return "Choose a metric to preview this panel.";
  if (m.kind !== "ratio" && !p.aggregate) return "Choose an aggregate to preview this panel.";
  if (!p.viz) return "Choose a chart type to preview this panel.";
  /* Catch an incoherent shape/chart/group combo here so the preview shows a clean hint, not a red run error. */
  return vizModeReason(p.viz, p.shape, (p.groupBy || []).length, p.overlay);
}

/* The filters sub-editor (its own redraw, so a dimension change can retune the operator list — LIKE only shows on a
   LIKE-able dimension). A value edit updates the model + preview in place (no redraw, so the text input keeps focus). */
function buildComposedFiltersEditor(p, measure, compose, onLive) {
  const box = el("div", { class: "item-list filters" });
  const dims = dimNamesForMeasure(measure, compose);

  function redraw() {
    const rows = (p.filters || []).map((f, i) => filterRow(f, i));
    const addBtn = el("button", { class: "btn small", type: "button", text: "+ Add filter" });
    addBtn.disabled = !dims.length;
    addBtn.addEventListener("click", () => {
      p.filters.push({ dimension: dims[0] || "", op: "eq", value: "" });
      redraw();
      onLive();
    });
    mount(box, [...rows, addBtn]);
  }

  function filterRow(f, i) {
    const dimSel = el("select", { class: "editor-select", "aria-label": "filter dimension" });
    for (const d of dims) dimSel.appendChild(el("option", { value: d, text: dimLabel(d) }));
    dimSel.value = f.dimension || dims[0] || "";
    dimSel.addEventListener("change", () => {
      f.dimension = dimSel.value;
      if (f.op === "like" && !dimMeta(compose, measure.source, f.dimension).likeable) f.op = "eq";
      redraw();
      onLive();
    });

    const likeable = dimMeta(compose, measure.source, f.dimension || dims[0]).likeable;
    const opSel = el("select", { class: "editor-select", "aria-label": "filter operator" });
    for (const op of compose.filterOps || []) {
      if (op === "like" && !likeable) continue;
      opSel.appendChild(el("option", { value: op, text: OP_LABELS[op] || op }));
    }
    opSel.value = f.op || "eq";
    opSel.addEventListener("change", () => {
      f.op = opSel.value;
      onLive();
    });

    const valInput = el("input", { class: "editor-input", type: "text", "aria-label": "filter value", placeholder: valuePlaceholder(f.op) });
    valInput.value = f.value || "";
    valInput.addEventListener("input", () => {
      f.value = valInput.value;
      onLive();
    });

    const rm = el("button", { class: "btn small danger icon", type: "button", text: "×", title: "Remove filter", "aria-label": "remove filter" });
    rm.addEventListener("click", () => {
      p.filters.splice(i, 1);
      redraw();
      onLive();
    });

    return el("div", { class: "item-row" }, [dimSel, opSel, valInput, rm]);
  }

  redraw();
  return box;
}

function valuePlaceholder(op) {
  if (op === "eq" || op === "neq") return "value, value, …  or  $variable";
  if (op === "like") return "pattern with % wildcards";
  return "threshold  or  $variable";
}

/* ── composed pure helpers ── */

/** The measure object a composed panel references (by its measure or ratio key), or null. */
function composedMeasure(p, catalog) {
  const key = p.ratio || p.measure;
  if (!key) return null;
  return ((catalog.compose || {}).measures || []).find((m) => m.key === key) || null;
}

function currentMeasureKey(p) {
  return p.ratio || p.measure || "";
}

/* Apply a measure choice to the panel model: set the source, measure XOR ratio, a default aggregate + unit, and
   drop any group-by / filter dimension the new measure doesn't allow (so a measure swap never leaves an off-catalog
   dimension behind). A viz that is now shape-incompatible falls back to the shape's default. */
function applyMeasureChoice(p, key, compose) {
  const m = (compose.measures || []).find((x) => x.key === key);
  if (!m) {
    p.measure = "";
    p.ratio = "";
    p.source = "";
    return;
  }
  p.source = m.source;
  if (m.kind === "ratio") {
    p.ratio = m.key;
    p.measure = "";
    p.aggregate = "";
  } else {
    p.measure = m.key;
    p.ratio = "";
    p.aggregate = m.defaultAggregate || (m.validAggregates || [])[0] || "";
  }
  p.unit = p.aggregate === "count" ? "count" : m.defaultUnit || "";
  const allowed = new Set(dimNamesForMeasure(m, compose));
  p.groupBy = (p.groupBy || []).filter((d) => allowed.has(d));
  p.filters = (p.filters || []).filter((f) => !f.dimension || allowed.has(f.dimension));
  if (!p.viz || !vizShapeCompatible(p.viz, p.shape)) p.viz = DEFAULT_VIZ_FOR_SHAPE[p.shape] || "line";
}

/** The dimensions a measure can group/filter by: the universal `server` (fleet axis) plus its allowed dimensions. */
function dimNamesForMeasure(m, compose) {
  const names = [...((compose && compose.universalDimensions) || []), ...((m && m.allowedDimensions) || [])];
  return [...new Set(names)];
}

/** A dimension's metadata for a source ({likeable}); the virtual `server` dimension is exact-match only. */
function dimMeta(compose, source, name) {
  if (name === "server") return { name, likeable: false };
  const d = ((compose && compose.dimensions) || []).find((x) => x.source === source && x.name === name);
  return d ? { name, likeable: !!d.likeable } : { name, likeable: false };
}

/** A friendly label for a dimension name (the fleet axis is called out; the rest are humanized). */
function dimLabel(d) {
  return d === "server" ? "server (fleet)" : derive.humanizeKey(d);
}

/** The self-describing measure caption: category · archetype · group-by dimensions. */
function measureCaption(m, compose) {
  const dims = dimNamesForMeasure(m, compose);
  const grain = m.kind === "ratio" ? "ratio" : ARCHETYPE_LABELS[m.archetype] || m.archetype;
  const dimText = dims.length ? dims.map(dimLabel).join(", ") : "server only";
  return m.category + " · " + grain + " · groups by " + dimText;
}

/* The per-server-type constraints on an appliesTo gate (design D4), shared by measures AND annotation sources (both
   carry the identical appliesTo node — the same BuildAppliesToNode on the server). Returns null when it collects
   everywhere with no msdb dependency (the common case — no badge). `missing` names the platforms it can't collect on;
   `needsMsdb` flags a SQL-Agent (job) dependency. These drive the always-on informational BADGES; when the view's
   $server resolves to one concrete server (scopeServerPlatform) the measure picker layers a stronger per-server GREY
   on top (measureUnavailableOn). AWS RDS + msdb have no reliable per-server signal, so they stay badges only. */
function availabilityConstraints(a) {
  if (!a) return null;
  const missing = [];
  if (a.azureSqlDb === false) missing.push("Azure SQL DB");
  if (a.azureMi === false) missing.push("Azure MI");
  if (a.awsRds === false) missing.push("AWS RDS");
  if (a.onPrem === false) missing.push("on-prem");
  const needsMsdb = a.needsMsdb === true;
  if (!missing.length && !needsMsdb) return null;
  return { missing, needsMsdb };
}

/** A compact one-line availability summary from constraints (a picker option/chip's hover title), or "". */
function availabilitySummary(c) {
  if (!c) return "";
  const parts = [];
  if (c.missing.length) parts.push("not on " + c.missing.join(", "));
  if (c.needsMsdb) parts.push("needs SQL Agent (msdb)");
  return parts.join("; ");
}

function measureAvailabilityConstraints(m) {
  return availabilityConstraints(m && m.appliesTo);
}

/** A compact one-line availability summary for a measure (the picker option's hover title), or "" when unconstrained. */
function measureAvailabilitySummary(m) {
  return availabilitySummary(measureAvailabilityConstraints(m));
}

/** A compact one-line availability summary for an annotation source (the composer chip's hover title, design D5). */
function annotationAvailabilitySummary(a) {
  return availabilitySummary(availabilityConstraints(a && a.appliesTo));
}

/* ── D4 per-server auto-greying ── */

/* The view-scope $server signal: when the view's server-dimension template variable resolves to exactly ONE concrete,
   platform-probed fleet server, return that enriched fleet option so the measure picker can GREY the measures it
   can't collect. Returns null for every "no reliable signal" case — no $server variable (or more than one carrying a
   value), a blank / "All" / multi (comma/semicolon) / $-referencing default, an unknown server name, or a server
   whose engine edition hasn't been probed (null/0) — so the picker falls back to the always-on Wave-1 badges.
   `variables` is the view/notebook model's declared template variables; `fleet` is loadFleetOptions' enriched output. */
export function scopeServerPlatform(variables, fleet) {
  const serverVars = (variables || []).filter((v) => v && v.dimension === "server" && (v.default || "").trim());
  if (serverVars.length !== 1) return null;
  const raw = serverVars[0].default.trim();
  if (raw.charAt(0) === "$") return null; // references another variable — not a concrete server
  if (raw.toLowerCase() === "all") return null; // explicit whole-fleet
  if (/[,;]/.test(raw)) return null; // a multi-server list
  const card = (fleet || []).find((c) => c.value === raw || c.label === raw);
  if (!card || !(card.engineEdition > 0)) return null; // unknown, or not-yet-probed (null/0) = no signal
  return card;
}

/* The platform label for a probed fleet card, used in the D4 grey reason: the two Azure platforms are named; every
   box / RDS / on-prem edition collapses to "this server" (no reliable per-server RDS signal to distinguish them). */
function scopeServerLabel(card) {
  if (card && card.isAzureSqlDb) return "Azure SQL DB";
  if (card && card.isAzureMi) return "Azure MI";
  return "this server";
}

/* The platform a measure/annotation CANNOT collect on given the scoped fleet card (the D4 grey verdict), or null when
   it can. Only the three signals /api/fleet reliably exposes drive it — is_azure_sql_db, is_azure_mi, else the
   box/RDS/on-prem gate (engine_edition non-null and not 5/8, so appliesTo.onPrem); AWS RDS + msdb have no per-server
   signal so they never grey here (they stay Wave-1 badges). Defensive against an unprobed card (returns null). */
function measureUnavailableOn(appliesTo, card) {
  if (!appliesTo || !card || !(card.engineEdition > 0)) return null;
  const gate = card.isAzureSqlDb ? appliesTo.azureSqlDb : card.isAzureMi ? appliesTo.azureMi : appliesTo.onPrem;
  return gate === false ? scopeServerLabel(card) : null;
}

/** The availability line under the measure picker (design D4), or null. Always-on informational badges come from the
 *  measure's appliesTo; when the view's $server resolves to one concrete server the measure can't collect on, a
 *  stronger per-server GREY badge REPLACES the generic "Not on <platforms>" one (same fact, server-specific) while
 *  the orthogonal msdb note stays. `scopeServer` is the resolved fleet card, or null (⇒ Wave-1 badges unchanged). */
function measureAvailabilityNote(m, scopeServer) {
  const platform = measureUnavailableOn(m.appliesTo, scopeServer);
  const c = measureAvailabilityConstraints(m);
  const badges = [];
  if (platform) {
    badges.push(el("span", { class: "measure-badge grey", text: "Not collected on " + platform }));
    if (c && c.needsMsdb) badges.push(el("span", { class: "measure-badge", text: "Needs SQL Agent (msdb)" }));
  } else if (c) {
    if (c.missing.length) badges.push(el("span", { class: "measure-badge warn", text: "Not on " + c.missing.join(", ") }));
    if (c.needsMsdb) badges.push(el("span", { class: "measure-badge", text: "Needs SQL Agent (msdb)" }));
  }
  return badges.length ? el("div", { class: "measure-badges" }, badges) : null;
}

/** Whether a viz is compatible with a shape ignoring the group-by requirement (table works with any shape). */
function vizShapeCompatible(viz, shape) {
  if (viz === "table") return true;
  if (shape === "timeseries" || shape === "topseries") return viz === "line" || viz === "area" || viz === "stacked" || viz === "stacked-bar";
  if (shape === "ranked") return viz === "bar" || viz === "pie" || viz === "scatter";
  return viz === "stat";
}

/** Where a viz belongs (for a disabled option's reason on the wrong shape). */
function shapeHintFor(viz) {
  if (viz === "line" || viz === "area" || viz === "stacked" || viz === "stacked-bar") return "over time";
  if (viz === "bar" || viz === "pie" || viz === "scatter") return "ranked";
  if (viz === "stat") return "single value";
  return "";
}

/** Which composed vizzes draw threshold reference lines (design D3): the value-axis charts (line/area/stacked/
 *  stacked-bar draw them horizontally, the ranked bar vertically). Pie/stat/table have no value axis to mark. */
function vizDrawsThresholds(viz) {
  return viz === "line" || viz === "area" || viz === "stacked" || viz === "stacked-bar" || viz === "bar";
}

/* The full viz↔shape coherence reason (mirrors the server's ValidateVizMode), or null when coherent — drives the
   inline hint under the chart picker. */
function vizModeReason(viz, shape, groupCount, overlay) {
  if (!viz) return null;
  const hasOverlay = !!(overlay && overlay.measure);
  if (viz === "table") return hasOverlay ? "A table can't carry a second measure — overlays belong to scatter and ungrouped line/area." : null;
  if (shape === "timeseries" || shape === "topseries") {
    if (!["line", "area", "stacked", "stacked-bar"].includes(viz)) return "A " + (VIZ_LABELS[viz] || viz) + " chart isn't a time series — use line, area, stacked, or stacked bar.";
    if ((viz === "stacked" || viz === "stacked-bar") && groupCount === 0) return "A stacked chart needs a group-by (the parts that stack).";
    if (hasOverlay && !(viz === "line" || viz === "area")) return "A second measure (overlay) needs a line or area chart.";
    if (hasOverlay && groupCount > 0) return "A dual-axis chart can't also group by a dimension — drop the group-by or the second measure.";
    return null;
  }
  if (shape === "ranked") {
    if (!["bar", "pie", "scatter"].includes(viz)) return "A " + (VIZ_LABELS[viz] || viz) + " chart can't rank a top-N — use bar, pie, or scatter.";
    if (groupCount === 0) return "A " + (VIZ_LABELS[viz] || viz) + " chart needs a group-by (the categories to rank).";
    if (viz === "scatter" && !hasOverlay) return "A scatter needs a second measure (the y axis) — pick one under Second measure.";
    if (viz !== "scatter" && hasOverlay) return "A " + (VIZ_LABELS[viz] || viz) + " chart can't carry a second measure — use scatter.";
    return null;
  }
  if (groupCount > 0) return "A single value can't group — switch to Over time or Ranked.";
  if (viz !== "stat") return "A single value uses the stat chart (or a table).";
  return hasOverlay ? "A single value can't carry a second measure." : null;
}

function buildComposedMeasureSelect(compose, current, scopeServer, onChange) {
  const sel = el("select", { class: "editor-select", "aria-label": "Metric" });
  sel.appendChild(el("option", { value: "", text: "— choose a metric —" }));
  const byCat = new Map();
  for (const m of compose.measures || []) {
    if (!byCat.has(m.category)) byCat.set(m.category, []);
    byCat.get(m.category).push(m);
  }
  for (const [cat, ms] of [...byCat.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const group = el("optgroup", { label: cat });
    for (const m of ms.sort((a, b) => a.displayName.localeCompare(b.displayName))) {
      const suffix = m.kind === "ratio" ? " (ratio)" : "";
      const caption = measureCaption(m, compose);
      /* D4: when the view is scoped to one concrete server this measure can't collect on, grey (disable) the option
         — but keep an already-CHOSEN measure selectable so re-scoping never silently drops the panel's metric (the
         reason surfaces in the note under the picker). Otherwise fall back to the informational availability title. */
      const platform = measureUnavailableOn(m.appliesTo, scopeServer);
      const avail = measureAvailabilitySummary(m);
      const label = m.displayName + suffix + (platform ? " — not on " + platform : "");
      const title = platform ? caption + " · not collected on " + platform : avail ? caption + " · " + avail : caption;
      const opt = el("option", { value: m.key, text: label, title });
      if (platform && m.key !== current) opt.disabled = true;
      group.appendChild(opt);
    }
    sel.appendChild(group);
  }
  sel.value = current || "";
  sel.addEventListener("change", () => onChange(sel.value));
  return sel;
}

/* The chart picker: every compose viz, with the shapes it can't render disabled + annotated ("Bar — ranked"), so
   only the coherent charts are selectable for the current shape (design §4 shape-steering). */
function buildComposedVizSelect(compose, shape, current, onChange) {
  const sel = el("select", { class: "editor-select", "aria-label": "Chart type" });
  for (const v of compose.viz || []) {
    const compatible = vizShapeCompatible(v, shape);
    const label = (VIZ_LABELS[v] || v) + (compatible ? "" : " — " + shapeHintFor(v));
    const opt = el("option", { value: v, text: label });
    if (!compatible) opt.disabled = true;
    sel.appendChild(opt);
  }
  sel.value = current || "";
  sel.addEventListener("change", () => onChange(sel.value));
  return sel;
}

/* The grain-trap guide (design §5): when a measure has NO per-database breakdown, point to same-category measures
   that DO (else cross-category), so "group CPU by database" doesn't dead-end — returns null when the measure IS
   per-database or when no alternative exists (no dead-end nag). */
function databaseGrainHint(measure, compose) {
  if (!measure || (measure.allowedDimensions || []).includes("database_name")) return null;
  const alts = (compose.measures || []).filter((m) => m.key !== measure.key && (m.allowedDimensions || []).includes("database_name"));
  const sameCat = alts.filter((m) => m.category === measure.category);
  const pick = (sameCat.length ? sameCat : alts).slice(0, 4);
  if (!pick.length) return null;
  return el("div", { class: "grain-hint" }, [
    "No per-database breakdown for this measure. For per-database data, try: ",
    el("span", { class: "grain-alts", text: pick.map((m) => m.displayName).join(", ") }),
    ".",
  ]);
}

/* ─────────────────────────── view scope (variables + default range) ─────────────────────────── */

/* The view-level scope editor: the DEFAULT time range (the range the rendered view opens with; the viewer can
   change it) and, folded away, the template variables a panel filter can reference as $name. $database is offered
   as a one-click add (the common per-database focus). Adding/removing a variable — or changing the range — rebuilds
   the panels (so previews re-run and the filter var hints refresh) via onStructuralChange. */
export function buildViewScopeSection(model, catalog, onStructuralChange) {
  const compose = catalog.compose || {};
  const dimNames = variableDimensionNames(compose);

  const rangeSel = el("select", { class: "editor-select", "aria-label": "Default time range" });
  rangeSel.appendChild(el("option", { value: "", text: "Default (last 24 hours)" }));
  for (const r of RANGE_OPTIONS) rangeSel.appendChild(el("option", { value: String(r.hours), text: r.label }));
  rangeSel.value = model.rangeHours != null ? String(model.rangeHours) : "";
  rangeSel.addEventListener("change", () => {
    model.rangeHours = rangeSel.value ? parseInt(rangeSel.value, 10) : null;
    onStructuralChange();
  });

  const varsBox = el("div", { class: "item-list vars" });

  function redrawVars() {
    const rows = model.variables.map((v, i) => varRow(v, i));
    const addDb = el("button", { class: "btn small", type: "button", text: "+ $database" });
    addDb.addEventListener("click", () => addVariable("database", "database_name"));
    const addBtn = el("button", { class: "btn small", type: "button", text: "+ Add variable" });
    addBtn.addEventListener("click", () => addVariable("", dimNames[0] || "server"));
    mount(varsBox, [...rows, el("div", { class: "vizcfg-actions" }, [addDb, addBtn])]);
  }

  function addVariable(name, dimension) {
    if (name && model.variables.some((v) => v.name === name)) return;
    model.variables.push({ name, dimension, default: "" });
    redrawVars();
    onStructuralChange();
  }

  function varRow(v, i) {
    const nameInp = el("input", { class: "editor-input", type: "text", placeholder: "name", "aria-label": "variable name" });
    nameInp.value = v.name || "";
    nameInp.addEventListener("input", () => {
      v.name = nameInp.value.replace(/^\$+/, "");
    });
    const dimSel = el("select", { class: "editor-select", "aria-label": "variable dimension" });
    for (const d of dimNames) dimSel.appendChild(el("option", { value: d, text: d === "server" ? "server" : derive.humanizeKey(d) }));
    dimSel.value = v.dimension || dimNames[0] || "server";
    v.dimension = dimSel.value;
    dimSel.addEventListener("change", () => {
      v.dimension = dimSel.value;
      onStructuralChange(); // a var becoming / ceasing to be the $server re-greys the measure pickers (D4)
    });
    const defInp = el("input", { class: "editor-input", type: "text", placeholder: "default (optional)", "aria-label": "variable default" });
    defInp.value = v.default || "";
    defInp.addEventListener("input", () => {
      v.default = defInp.value;
    });
    // Commit (blur / Enter) re-greys the measure pickers for the new $server default (D4); not per-keystroke.
    defInp.addEventListener("change", () => onStructuralChange());
    const rm = el("button", { class: "btn small danger icon", type: "button", text: "×", title: "Remove variable", "aria-label": "remove variable" });
    rm.addEventListener("click", () => {
      model.variables.splice(i, 1);
      redrawVars();
      onStructuralChange();
    });
    return el("div", { class: "item-row" }, [el("span", { class: "var-dollar", text: "$" }), nameInp, dimSel, defInp, rm]);
  }

  redrawVars();

  return el("div", { class: "view-scope" }, [
    el("h3", { class: "section-title", text: "View scope" }),
    el("div", { class: "view-scope-row" }, [field("Default time range", rangeSel)]),
    el("details", { class: "vars-panel" }, [
      el("summary", { text: "Template variables (advanced)" }),
      el("div", { class: "vars-body" }, [
        el("div", {
          class: "block-help",
          text: "Declare a variable to scope panels view-wide: a panel filter can reference it as $name and the viewer picks its value. $database is the common one.",
        }),
        varsBox,
      ]),
    ]),
  ]);
}

/** The dimension names a template variable may bind to: every catalog dimension name plus the reserved `server`. */
function variableDimensionNames(compose) {
  const set = new Set(["server"]);
  for (const d of compose.dimensions || []) set.add(d.name);
  return [...set];
}

/* ─────────────────────────── read / viz pickers ─────────────────────────── */

function buildReadSelect(catalog, current) {
  const sel = el("select", { class: "editor-select", "aria-label": "Read" });
  sel.appendChild(el("option", { value: "", text: "— choose a read —" }));

  const byCategory = new Map();
  for (const r of catalog.reads || []) {
    if (!byCategory.has(r.category)) byCategory.set(r.category, []);
    byCategory.get(r.category).push(r);
  }
  for (const [category, reads] of [...byCategory.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const group = el("optgroup", { label: category });
    for (const r of reads.sort((a, b) => a.name.localeCompare(b.name))) {
      group.appendChild(el("option", { value: r.name, text: r.name, title: r.description || r.name }));
    }
    sel.appendChild(group);
  }

  sel.value = current || "";
  return sel;
}

function buildVizSelect(catalog, current) {
  const sel = el("select", { class: "editor-select", "aria-label": "Visualization" });
  sel.appendChild(el("option", { value: "", text: "— choose —" }));
  const vizList = catalog.viz && catalog.viz.length ? catalog.viz : Object.keys(VIZ);
  for (const v of vizList) sel.appendChild(el("option", { value: v, text: v }));
  sel.value = current || "";
  return sel;
}

/* ─────────────────────────── typed param inputs ─────────────────────────── */

function defaultParams(catalog, read) {
  const rd = readByName(catalog, read);
  const out = {};
  if (!rd) return out;
  for (const prm of rd.params || []) {
    if (prm.default !== null && prm.default !== undefined) out[prm.name] = prm.default;
  }
  return out;
}

function buildParamInputs(p, catalog, fleet, onChange) {
  const rd = readByName(catalog, p.read);
  if (!rd || !(rd.params || []).length) {
    return el("div", { class: "muted", text: p.read ? "This read takes no parameters." : "Choose a read first." });
  }
  return (rd.params || []).map((prm) => {
    const input = buildParamInput(prm, p.params[prm.name], fleet, (val) => {
      if (val === "" || val == null) delete p.params[prm.name];
      else p.params[prm.name] = val;
      onChange();
    });
    return el("label", { class: "param-field" + (prm.required ? " required" : "") }, [
      el("span", { class: "param-name" }, [prm.name, prm.required ? el("span", { class: "req", text: " *" }) : null]),
      input,
      hintFor(prm),
    ]);
  });
}

function buildParamInput(prm, currentValue, fleet, setValue) {
  switch (prm.type) {
    case "server": {
      const sel = el("select", { class: "editor-select", "aria-label": prm.name });
      sel.appendChild(el("option", { value: "", text: "(auto / all servers)" }));
      for (const o of fleet) sel.appendChild(el("option", { value: o.value, text: o.label }));
      sel.value = currentValue != null ? String(currentValue) : "";
      sel.addEventListener("change", () => setValue(sel.value));
      return sel;
    }
    case "bool": {
      const box = el("input", { type: "checkbox", class: "editor-check", "aria-label": prm.name });
      box.checked = currentValue != null ? currentValue === true || currentValue === "true" : !!prm.default;
      box.addEventListener("change", () => setValue(box.checked));
      return box;
    }
    case "int": {
      const inp = el("input", {
        class: "editor-input",
        type: "number",
        step: "1",
        "aria-label": prm.name,
        placeholder: prm.default != null ? String(prm.default) : "",
      });
      if (currentValue != null) inp.value = String(currentValue);
      inp.addEventListener("input", () => setValue(toNumber(inp.value, true)));
      return inp;
    }
    case "double": {
      const inp = el("input", {
        class: "editor-input",
        type: "number",
        step: "any",
        "aria-label": prm.name,
        placeholder: prm.default != null ? String(prm.default) : "",
      });
      if (currentValue != null) inp.value = String(currentValue);
      inp.addEventListener("input", () => setValue(toNumber(inp.value, false)));
      return inp;
    }
    default: {
      const inp = el("input", { class: "editor-input", type: "text", "aria-label": prm.name });
      if (currentValue != null) inp.value = String(currentValue);
      inp.addEventListener("input", () => setValue(inp.value));
      return inp;
    }
  }
}

function hintFor(prm) {
  let t = prm.type;
  if (prm.default !== null && prm.default !== undefined) t += " · default " + prm.default;
  return el("span", { class: "param-hint", text: t });
}

/* ─────────────────────────── vizcfg sub-editors ─────────────────────────── */

function buildVizcfgEditor(p, sample, onChange) {
  p.vizcfg = p.vizcfg || {};
  switch (p.viz) {
    case "table":
      return tableCfgEditor(p.vizcfg, sample, onChange);
    case "stat":
      return statCfgEditor(p.vizcfg, sample, onChange);
    case "line":
      return lineCfgEditor(p.vizcfg, sample, onChange);
    case "bandlist":
      return bandlistCfgEditor(p.vizcfg, sample, onChange);
    default:
      return el("div", { class: "muted", text: "Choose a visualization to configure its fields." });
  }
}

function tableCfgEditor(cfg, sample, onChange) {
  cfg.columns = cfg.columns || [];
  const arrayKeys = topLevelArrayKeys(sample);
  const rowsField = keyField(arrayKeys, cfg.rowsKey, (v) => {
    cfg.rowsKey = v;
    onChange();
  }, "rows key");
  const columns = itemList(
    "columns",
    cfg.columns,
    (col) => [
      keyField(rowKeys(sample, cfg.rowsKey), col.key, (v) => {
        col.key = v;
        onChange();
      }, "column key"),
      textField(col.label, (v) => {
        col.label = v;
        onChange();
      }, "column label"),
      formatSelect(col.format, (v) => {
        col.format = v;
        onChange();
      }),
    ],
    onChange,
    "+ Add column",
    () => ({ key: "", label: "", format: "text" })
  );
  return el("div", { class: "cfg" }, [field("Rows key", rowsField), labeledBlock("Columns", columns)]);
}

function statCfgEditor(cfg, sample, onChange) {
  cfg.stats = cfg.stats || [];
  const keys = topLevelScalarKeys(sample);
  const list = itemList(
    "stats",
    cfg.stats,
    (s) => [
      keyField(keys, s.key, (v) => {
        s.key = v;
        onChange();
      }, "stat key"),
      textField(s.label, (v) => {
        s.label = v;
        onChange();
      }, "stat label"),
      formatSelect(s.format, (v) => {
        s.format = v;
        onChange();
      }),
    ],
    onChange,
    "+ Add stat",
    () => ({ key: "", label: "", format: "text" })
  );
  return el("div", { class: "cfg" }, [labeledBlock("Stats", list)]);
}

function lineCfgEditor(cfg, sample, onChange) {
  cfg.series = cfg.series || [];
  const arrayKeys = topLevelArrayKeys(sample);
  const numericKeys = rowNumericKeys(sample, cfg.rowsKey);
  const rowsField = keyField(arrayKeys, cfg.rowsKey, (v) => {
    cfg.rowsKey = v;
    onChange();
  }, "rows key");
  const xField = keyField(rowKeys(sample, cfg.rowsKey), cfg.xKey, (v) => {
    cfg.xKey = v;
    onChange();
  }, "x-axis key");
  const series = itemList(
    "series",
    cfg.series,
    (s) => [
      keyField(numericKeys, s.key, (v) => {
        s.key = v;
        onChange();
      }, "series key"),
      textField(s.label, (v) => {
        s.label = v;
        onChange();
      }, "series label"),
      colorField(s.color, (v) => {
        s.color = v;
        onChange();
      }),
    ],
    onChange,
    "+ Add series",
    () => ({ key: "", label: "", color: SERIES_COLORS[0] })
  );
  const fmt = formatSelect(cfg.format || "text", (v) => {
    cfg.format = v === "text" ? undefined : v;
    onChange();
  });
  const unit = textField(cfg.unit, (v) => {
    cfg.unit = v || undefined;
    onChange();
  }, "unit");
  return el("div", { class: "cfg" }, [
    field("Rows key", rowsField),
    field("X-axis key", xField),
    labeledBlock("Series", series),
    field("Value format", fmt),
    field("Unit (optional)", unit),
  ]);
}

function bandlistCfgEditor(cfg, sample, onChange) {
  const arrayKeys = topLevelArrayKeys(sample);
  const rKeys = rowKeys(sample, cfg.rowsKey);
  const keyRow = (label, prop, aria) =>
    field(
      label,
      keyField(rKeys, cfg[prop], (v) => {
        cfg[prop] = v || undefined;
        onChange();
      }, aria)
    );
  return el("div", { class: "cfg" }, [
    field(
      "Rows key",
      keyField(arrayKeys, cfg.rowsKey, (v) => {
        cfg.rowsKey = v;
        onChange();
      }, "rows key")
    ),
    keyRow("Primary key", "primaryKey", "primary key"),
    keyRow("Band key", "bandKey", "band key"),
    keyRow("Band label key (optional)", "bandLabelKey", "band label key"),
    keyRow("Reason key (optional)", "reasonKey", "reason key"),
    keyRow("Nav key — server (optional)", "navKey", "nav key"),
  ]);
}

/* ─────────────────────────── small field builders ─────────────────────────── */

let _keyListId = 0;

/* A key input backed by a <datalist> of the sample's available keys — autocompletes from the sample while still
   allowing a hand-typed key (a sample is a subset; the operator may know a key not in the first row). */
function keyField(keys, current, onChange, ariaLabel) {
  const input = el("input", { class: "editor-input mono", type: "text", "aria-label": ariaLabel || "key" });
  if (current != null) input.value = String(current);
  input.addEventListener("input", () => onChange(input.value));
  if (keys && keys.length) {
    const id = "keys-" + ++_keyListId;
    const datalist = el("datalist", { id });
    for (const k of keys) datalist.appendChild(el("option", { value: k }));
    input.setAttribute("list", id);
    return el("span", { class: "keyfield" }, [input, datalist]);
  }
  return input;
}

function textField(current, onChange, ariaLabel) {
  const input = el("input", { class: "editor-input", type: "text", "aria-label": ariaLabel || "text" });
  if (current != null) input.value = String(current);
  input.addEventListener("input", () => onChange(input.value));
  return input;
}

function formatSelect(current, onChange) {
  const sel = el("select", { class: "editor-select", "aria-label": "format" });
  for (const f of FORMAT_OPTIONS) sel.appendChild(el("option", { value: f, text: f }));
  sel.value = current || "text";
  sel.addEventListener("change", () => onChange(sel.value));
  return sel;
}

/* Series color is CONSTRAINED (reconciliation #4): an <input type=color> only ever yields #rrggbb, and the
   palette buttons are charts.js constants — no free-text ever reaches the chart's style-attribute sink. */
function colorField(current, onChange) {
  const picker = el("input", { type: "color", class: "color-picker", "aria-label": "series color" });
  picker.value = normalizeColor(current);
  picker.addEventListener("input", () => onChange(picker.value));
  const palette = el(
    "span",
    { class: "palette" },
    SERIES_COLORS.map((c) =>
      el("button", {
        type: "button",
        class: "swatch-btn",
        title: c,
        style: "background:" + c,
        "aria-label": "use color " + c,
        onClick: () => {
          picker.value = c;
          onChange(c);
        },
      })
    )
  );
  return el("span", { class: "color-field" }, [picker, palette]);
}

/* A remove-able, add-able list of config rows (columns / series / stats). renderRow returns the row's field
   nodes; itemList appends the remove button and the add button, redrawing in place on any structural change. */
function itemList(className, items, renderRow, onChange, addLabel, makeNew) {
  const box = el("div", { class: "item-list " + className });

  function redraw() {
    const rows = items.map((item, i) => {
      const removeBtn = el("button", { class: "btn small danger icon", type: "button", text: "×", title: "Remove", "aria-label": "remove" });
      removeBtn.addEventListener("click", () => {
        items.splice(i, 1);
        redraw();
        onChange();
      });
      return el("div", { class: "item-row" }, [...renderRow(item, i), removeBtn]);
    });
    const addBtn = el("button", { class: "btn small", type: "button", text: addLabel });
    addBtn.addEventListener("click", () => {
      items.push(makeNew());
      redraw();
      onChange();
    });
    mount(box, [...rows, addBtn]);
  }

  redraw();
  return box;
}

export function field(label, control, extraClass) {
  return el("label", { class: "editor-field" + (extraClass ? " " + extraClass : "") }, [
    el("span", { class: "field-label", text: label }),
    control,
  ]);
}

export function labeledBlock(label, node) {
  return el("div", { class: "editor-block" }, [el("div", { class: "block-label", text: label }), node]);
}

function backHead(id) {
  return el("div", { class: "page-head" }, [
    el("a", { href: id != null && id !== "new" ? "#/view/" + encodeURIComponent(id) : "#/views", text: "← Back" }),
    el("h2", { text: "Composer" }),
  ]);
}

/* ─────────────────────────── sample-key helpers ─────────────────────────── */

function topLevelArrayKeys(sample) {
  if (!sample || typeof sample !== "object" || Array.isArray(sample)) return [];
  return Object.keys(sample).filter((k) => Array.isArray(sample[k]));
}

function topLevelScalarKeys(sample) {
  if (!sample || typeof sample !== "object" || Array.isArray(sample)) return [];
  return Object.keys(sample).filter((k) => sample[k] === null || typeof sample[k] !== "object");
}

function rowKeys(sample, rowsKey) {
  const first = firstRow(sample, rowsKey);
  return first ? Object.keys(first) : [];
}

function rowNumericKeys(sample, rowsKey) {
  const first = firstRow(sample, rowsKey);
  return first ? Object.keys(first).filter((k) => typeof first[k] === "number") : [];
}

function firstRow(sample, rowsKey) {
  const rows = sample && rowsKey ? sample[rowsKey] : null;
  return Array.isArray(rows) && rows.length && rows[0] && typeof rows[0] === "object" ? rows[0] : null;
}

/* ─────────────────────────── misc helpers ─────────────────────────── */

function readByName(catalog, name) {
  return (catalog.reads || []).find((r) => r.name === name) || null;
}

/* The value of a read's `server`-typed param (if any) — feeds the panel-header echo "Panel N · Title (SERVER)". */
function serverParamValue(p, catalog) {
  const rd = readByName(catalog, p.read);
  if (!rd) return "";
  const sp = (rd.params || []).find((prm) => prm.type === "server");
  if (!sp) return "";
  const v = p.params ? p.params[sp.name] : null;
  return v == null || v === "" ? "" : String(v);
}

function missingRequired(panel, catalog) {
  const rd = readByName(catalog, panel.read);
  if (!rd) return [];
  return (rd.params || [])
    .filter((prm) => prm.required && isEmpty(panel.params[prm.name]))
    .map((prm) => prm.name);
}

function hasVizcfg(p) {
  const c = p.vizcfg || {};
  return !!(
    (c.columns && c.columns.length) ||
    (c.series && c.series.length) ||
    (c.stats && c.stats.length) ||
    c.rowsKey ||
    c.primaryKey
  );
}

function isEmpty(v) {
  return v === null || v === undefined || v === "";
}

function toNumber(raw, isInt) {
  if (raw === "" || raw == null) return "";
  const n = isInt ? parseInt(raw, 10) : parseFloat(raw);
  return Number.isNaN(n) ? "" : n;
}

/** Parse an int and clamp it into [min, max], falling back to `dflt` on a non-number. */
function clampInt(raw, min, max, dflt) {
  const n = parseInt(raw, 10);
  if (Number.isNaN(n)) return dflt;
  return Math.max(min, Math.min(max, n));
}

function swap(arr, i, j) {
  const tmp = arr[i];
  arr[i] = arr[j];
  arr[j] = tmp;
}
