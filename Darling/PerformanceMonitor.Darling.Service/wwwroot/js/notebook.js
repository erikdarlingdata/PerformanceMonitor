/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Notebook mode (#1563 D7, scope B) — the COMPOSER for a notebook and the seed TEMPLATES. A notebook is a saved
 * custom view whose definition is { kind:"notebook", cells:[…], variables, range } instead of a dashboard's
 * { panels:[…], … }; both live in config.custom_views behind the same /api/views CRUD, and the backend validates
 * the notebook shape (the authority). A cell is either { type:"markdown", text } — rendered by the air-gapped
 * markdown.js — or { type:"panel", <a v2 composed panel spec> } — authored by the SAME composer body the dashboard
 * uses (buildComposedPanelBody, shared from editor.js) and rendered by compose.js. The DOCUMENT renderer lives in
 * pages/views.js beside the dashboard renderer (it reuses that page's server/time/variable scope bar); this module
 * owns the editor + templates. All user text reaches the DOM via util.el()/textContent (R4 — never innerHTML).
 *
 * The 60s poll in app.js SKIPS the notebook editor route (the poll-clobber guard), so an in-progress edit is never
 * discarded by a background refresh — the same guarantee the dashboard composer has.
 */

import { el, mount } from "./util.js";
import { renderMarkdown } from "./markdown.js";
import { cellRange, pinRangeLabel } from "./compose.js";
import {
  buildComposedPanelBody,
  buildViewScopeSection,
  loadFleetOptions,
  scopeServerPlatform,
  newComposedPanel,
  composedPanelToDesc,
  descToComposedPanel,
  composedBlocker,
  field,
} from "./editor.js";
import * as api from "./views-api.js";

/** The definition discriminator: a notebook definition carries kind:"notebook"; a dashboard omits it. */
export const NOTEBOOK_KIND = "notebook";

/** Whether a stored definition is a notebook (vs a dashboard). Null-safe. */
export function isNotebookDefinition(def) {
  return !!def && typeof def === "object" && def.kind === NOTEBOOK_KIND;
}

/** The view-level default range a new notebook opens with (the viewer can still change it). */
const DEFAULT_RANGE_HOURS = 24;

/** Abuse bounds mirrored from the server (DarlingWebEndpoints) so the composer reports them inline, not as a 400. */
const MAX_NOTEBOOK_CELLS = 100;
const MAX_MARKDOWN_BYTES = 32 * 1024;
const _utf8 = new TextEncoder();

/* ─────────────────────────── entry ─────────────────────────── */

export async function renderNotebookEditor(main, id, templateKey) {
  mount(main, el("div", { class: "strip loading", text: "Loading notebook composer…" }));

  const session = await api.getSession();
  if (!session.can_edit) {
    mount(main, [
      notebookBackHead(id),
      el("div", { class: "strip empty" }, [
        "Couldn't confirm your session, so the composer is read-only for now. Reload the page to edit; " +
          "you can still open and export notebooks.",
      ]),
    ]);
    return;
  }

  /* Fleet options carry the per-server platform for D4 measure auto-greying (loadFleetOptions is shared from the
     dashboard composer so the enrichment lives in one place). */
  const [catalog, fleet] = await Promise.all([api.getCatalog(), loadFleetOptions()]);

  let model;
  let editingId = null;
  let loadedVersion = null;

  if (id != null && id !== "new") {
    const res = await api.getView(id);
    if (res.kind !== "data" || !res.data) {
      mount(main, [notebookBackHead(id), el("div", { class: "strip error", text: res.message || "Could not load this notebook." })]);
      return;
    }
    /* If the id is actually a DASHBOARD, hand off to the dashboard composer (no import cycle — a hash redirect). */
    if (!isNotebookDefinition(res.data.definition)) {
      location.hash = "#/view/" + encodeURIComponent(id) + "/edit";
      return;
    }
    editingId = res.data.id;
    loadedVersion = res.data.version;
    model = notebookToModel(res.data);
  } else {
    const tpl = templateKey ? NOTEBOOK_TEMPLATES.find((t) => t.key === templateKey) : null;
    model = tpl ? notebookToModel(tpl.make()) : blankNotebookModel();
  }

  buildNotebookEditor(main, { model, editingId, loadedVersion, catalog, fleet });
}

/* ─────────────────────────── model <-> definition ─────────────────────────── */

/** A brand-new notebook: one empty markdown cell + one empty panel cell (both cell kinds visible to start). */
function blankNotebookModel() {
  return { name: "", description: "", cells: [markdownCellModel(""), panelCellModel()], variables: [], rangeHours: null };
}

function markdownCellModel(text) {
  return { type: "markdown", text: typeof text === "string" ? text : "" };
}

function panelCellModel() {
  return { type: "panel", panel: newComposedPanel() };
}

/** A stored view/template ({name, description, definition}) -> the editor model. Malformed cells are dropped; a
 *  notebook with no valid cells falls back to a single empty markdown cell (a notebook always has >=1 cell). */
function notebookToModel(view) {
  const def = view.definition || {};
  const cells = Array.isArray(def.cells) ? def.cells.map(cellToModel).filter(Boolean) : [];
  return {
    name: view.name || "",
    description: view.description || "",
    cells: cells.length ? cells : [markdownCellModel("")],
    variables: parseVariables(def.variables),
    rangeHours: def.range && typeof def.range.hours === "number" ? def.range.hours : null,
  };
}

/* A stored cell -> the editor's cell model. A markdown cell keeps its text; a panel cell's composed spec is parsed
   by the SAME descToComposedPanel the dashboard composer uses (it reads only the composed keys, ignoring `type`).
   The per-cell window pin (#2735) rides on the CELL model — not the shared panel model editor.js owns — so the
   dashboard composer's desc round-trip stays untouched. */
function cellToModel(c) {
  if (!c || typeof c !== "object" || Array.isArray(c)) return null;
  if (c.type === "markdown") return markdownCellModel(typeof c.text === "string" ? c.text : "");
  if (c.type === "panel") {
    const m = { type: "panel", panel: descToComposedPanel(c) };
    const range = cellRange(c);
    if (range) m.range = range;
    return m;
  }
  return null;
}

/** The view's stored template variables -> the model list ({name, dimension, default}); malformed entries dropped
 *  (a local mirror of the dashboard composer's parser — same shape, same rules). */
function parseVariables(node) {
  if (!Array.isArray(node)) return [];
  return node
    .filter((v) => v && typeof v === "object" && v.name)
    .map((v) => ({ name: String(v.name), dimension: v.dimension ? String(v.dimension) : "", default: v.default != null ? String(v.default) : "" }));
}

/** The editor model -> the stored notebook definition ({ kind:"notebook", cells, variables?, range? }). */
function modelToNotebookDefinition(model) {
  const def = { kind: NOTEBOOK_KIND, cells: model.cells.map(cellToStored) };
  const variables = (model.variables || [])
    .filter((v) => v.name && v.dimension)
    .map((v) => (v.default ? { name: v.name, dimension: v.dimension, default: v.default } : { name: v.name, dimension: v.dimension }));
  if (variables.length) def.variables = variables;
  if (model.rangeHours) def.range = { hours: model.rangeHours };
  return def;
}

/* A cell model -> its stored form. A panel cell serializes through the shared composedPanelToDesc, then drops
   `span` — a notebook is a single-column document, so a panel cell is always full width. The per-cell window
   pin (#2735) is re-attached from the cell model: dropping it here would silently unpin every pinned cell on
   any unrelated editor save. */
function cellToStored(cell) {
  if (cell.type === "markdown") return { type: "markdown", text: cell.text || "" };
  const { span, ...desc } = composedPanelToDesc(cell.panel);
  const stored = { type: "panel", ...desc };
  if (cell.range) stored.range = { ...cell.range };
  return stored;
}

/** The reason SAVE is blocked (name/cells + each panel cell's composed prerequisites), or null when savable. */
function notebookSaveBlocker(model, catalog) {
  if (!model.name || !model.name.trim()) return "Enter a notebook name.";
  if (!model.cells.length) return "Add at least one cell.";
  if (model.cells.length > MAX_NOTEBOOK_CELLS) return "A notebook can have at most " + MAX_NOTEBOOK_CELLS + " cells.";
  for (let i = 0; i < model.cells.length; i++) {
    const cell = model.cells[i];
    if (cell.type === "panel") {
      const problem = composedBlocker(cell.panel, catalog);
      if (problem) return "Cell " + (i + 1) + " (panel): " + problem;
    } else if (cell.type === "markdown" && _utf8.encode(cell.text || "").length > MAX_MARKDOWN_BYTES) {
      return "Cell " + (i + 1) + " (markdown) text is too long (max " + MAX_MARKDOWN_BYTES / 1024 + " KB).";
    }
  }
  return null;
}

/* ─────────────────────────── the editor shell ─────────────────────────── */

function buildNotebookEditor(main, ctx) {
  const { model } = ctx;

  const nameInput = el("input", { class: "editor-input", type: "text", placeholder: "Notebook name (required)", "aria-label": "Notebook name" });
  nameInput.value = model.name;
  nameInput.addEventListener("input", () => {
    model.name = nameInput.value;
    refreshSaveState();
  });

  const descInput = el("input", { class: "editor-input", type: "text", placeholder: "Description (optional)", "aria-label": "Notebook description" });
  descInput.value = model.description;
  descInput.addEventListener("input", () => {
    model.description = descInput.value;
  });

  const cellsBox = el("div", { class: "cells-box" });
  const saveStatus = el("div", { class: "save-status" });
  const saveBtn = el("button", { class: "btn primary", type: "button", text: ctx.editingId != null ? "Save changes" : "Create notebook" });
  const cancelBtn = el("button", { class: "btn", type: "button", text: "Cancel" });

  const backHash = ctx.editingId != null ? "#/notebook/" + encodeURIComponent(ctx.editingId) : "#/views";
  cancelBtn.addEventListener("click", () => {
    location.hash = backHash;
  });
  saveBtn.addEventListener("click", onSave);

  const addMdBtn = el("button", { class: "btn", type: "button", text: "+ Markdown cell" });
  addMdBtn.addEventListener("click", () => {
    if (model.cells.length >= MAX_NOTEBOOK_CELLS) return;
    model.cells.push(markdownCellModel(""));
    rebuildCells();
  });
  const addPanelBtn = el("button", { class: "btn", type: "button", text: "+ Panel cell" });
  addPanelBtn.addEventListener("click", () => {
    if (model.cells.length >= MAX_NOTEBOOK_CELLS) return;
    model.cells.push(panelCellModel());
    rebuildCells();
  });

  /* The scope a panel cell's LIVE preview runs against — the whole fleet + the notebook's default range + the
     declared variables bound to their defaults; read live so it always reflects the current scope model. */
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

  const inner = { ...ctx, rebuildCells, refreshSaveState, previewScope };

  /* Reuse the dashboard composer's view-scope editor verbatim (default time range + template variables). */
  const viewScopeSection = buildViewScopeSection(model, ctx.catalog, rebuildCells);

  function rebuildCells() {
    mount(cellsBox, model.cells.map((cell, i) => buildCellEditor(cell, i, inner)));
    refreshSaveState();
  }

  function refreshSaveState() {
    const problem = notebookSaveBlocker(model, ctx.catalog);
    saveBtn.disabled = problem != null;
    const atCap = model.cells.length >= MAX_NOTEBOOK_CELLS;
    addMdBtn.disabled = atCap;
    addPanelBtn.disabled = atCap;
    mount(saveStatus, problem ? el("span", { class: "muted", text: problem }) : null);
  }

  async function onSave() {
    saveBtn.disabled = true;
    const problem = notebookSaveBlocker(model, ctx.catalog);
    if (problem) {
      saveBtn.disabled = false;
      mount(saveStatus, el("span", { class: "strip error", text: problem }));
      return;
    }

    mount(saveStatus, el("span", { class: "muted", text: "Saving…" }));
    const body = {
      name: model.name.trim(),
      description: model.description ? model.description : null,
      definition: modelToNotebookDefinition(model),
    };

    let res;
    if (ctx.editingId != null) {
      body.version = ctx.loadedVersion;
      res = await api.updateView(ctx.editingId, body);
    } else {
      res = await api.createView(body);
    }

    if (res.kind === "data" && res.data) {
      location.hash = "#/notebook/" + encodeURIComponent(res.data.id);
      return;
    }

    saveBtn.disabled = false;
    handleNotebookSaveError(res, saveStatus, ctx, main);
  }

  mount(main, [
    el("div", { class: "page-head" }, [
      el("a", { href: backHash, text: "← Back" }),
      el("h2", { text: ctx.editingId != null ? "Edit notebook" : "New notebook" }),
    ]),
    el("div", { class: "editor-meta" }, [field("Name", nameInput), field("Description", descInput)]),
    viewScopeSection,
    el("h3", { class: "section-title", text: "Cells" }),
    cellsBox,
    el("div", { class: "cells-add" }, [addMdBtn, addPanelBtn]),
    el("div", { class: "editor-footer" }, [saveBtn, cancelBtn, saveStatus]),
  ]);

  rebuildCells();
}

/* On a 409 while EDITING, the row changed under us (stale version) or the name now collides — offer a reload of the
   latest (discarding local edits), mirroring the dashboard composer. */
function handleNotebookSaveError(res, saveStatus, ctx, main) {
  const parts = [el("span", { text: res.message || "Could not save the notebook." })];
  if (res.status === 409 && ctx.editingId != null) {
    const reload = el("button", { class: "btn small", type: "button", text: "Reload latest" });
    reload.addEventListener("click", () => renderNotebookEditor(main, ctx.editingId));
    parts.push(reload);
  }
  mount(saveStatus, el("div", { class: "strip error" }, parts));
}

/* ─────────────────────────── one cell editor ─────────────────────────── */

function buildCellEditor(cell, index, ctx) {
  return cell.type === "markdown" ? buildMarkdownCellEditor(cell, index, ctx) : buildPanelCellEditor(cell, index, ctx);
}

/* The shared cell head: "Cell N", a kind tag, and reorder/remove wired to model.cells (a11y: up/down buttons with
   labels + disabled at the ends, matching the dashboard panel reorder). Removing the last cell reseeds a markdown
   cell so a notebook always has >=1 cell. */
function cellHead(index, ctx, kindLabel) {
  const cells = ctx.model.cells;
  const upBtn = el("button", { class: "btn small icon", type: "button", text: "↑", title: "Move up", "aria-label": "Move cell up" });
  const downBtn = el("button", { class: "btn small icon", type: "button", text: "↓", title: "Move down", "aria-label": "Move cell down" });
  const removeBtn = el("button", { class: "btn small danger", type: "button", text: "Remove", "aria-label": "Remove cell" });
  upBtn.disabled = index === 0;
  downBtn.disabled = index === cells.length - 1;
  upBtn.addEventListener("click", () => {
    swap(cells, index, index - 1);
    ctx.rebuildCells();
  });
  downBtn.addEventListener("click", () => {
    swap(cells, index, index + 1);
    ctx.rebuildCells();
  });
  removeBtn.addEventListener("click", () => {
    cells.splice(index, 1);
    if (!cells.length) cells.push(markdownCellModel(""));
    ctx.rebuildCells();
  });
  return el("div", { class: "cell-editor-head" }, [
    el("span", { class: "cell-editor-title", text: "Cell " + (index + 1) }),
    el("span", { class: "cell-kind-tag", text: kindLabel }),
    el("div", { class: "spacer" }),
    upBtn,
    downBtn,
    removeBtn,
  ]);
}

/* A markdown cell: a textarea beside a live renderMarkdown preview (two columns, collapsing to stacked on narrow
   screens). Typing re-renders the preview locally (no full cell rebuild, so the textarea keeps focus + caret). */
function buildMarkdownCellEditor(cell, index, ctx) {
  const previewBox = el("div", { class: "md-preview markdown-body" });
  function renderPreview() {
    mount(previewBox, cell.text && cell.text.trim() ? renderMarkdown(cell.text) : el("div", { class: "muted", text: "Nothing to preview yet." }));
  }

  const textarea = el("textarea", {
    class: "md-textarea",
    rows: "8",
    placeholder: "Write markdown…  # heading, **bold**, *italic*, `code`, - list, 1. list, [text](url)",
    "aria-label": "Markdown for cell " + (index + 1),
  });
  textarea.value = cell.text || "";
  textarea.addEventListener("input", () => {
    cell.text = textarea.value;
    renderPreview();
  });

  renderPreview();

  const body = el("div", { class: "md-cell-body" }, [
    el("div", { class: "md-edit-col" }, [el("div", { class: "block-label", text: "Markdown" }), textarea]),
    el("div", { class: "md-preview-col" }, [el("div", { class: "block-label", text: "Preview" }), previewBox]),
  ]);

  return el("div", { class: "notebook-cell md-cell card" }, [cellHead(index, ctx, "Markdown"), body]);
}

/* A panel cell: the SHARED composer body (metric / shape / group / filters / chart / advanced / identity + live
   preview) with the Width control hidden (a notebook cell is always full width). onChange just re-checks save
   state; the body owns its own config rebuild + debounced preview. */
function buildPanelCellEditor(cell, index, ctx) {
  const body = buildComposedPanelBody(cell.panel, {
    catalog: ctx.catalog,
    getVariables: () => ctx.model.variables,
    getScopeServer: () => scopeServerPlatform(ctx.model.variables, ctx.fleet),
    previewScope: ctx.previewScope,
    onChange: () => ctx.refreshSaveState(),
    showWidth: false,
  });
  return el("div", { class: "notebook-cell panel-cell card" }, [cellHead(index, ctx, "Panel"), pinnedWindowStrip(cell, ctx), body]);
}

/* The per-cell window pin (#2735) in the EDITOR. The composer has no authoring UI for a pin yet (pins are
   authored over MCP or by import); a loaded pin must still be VISIBLE and removable here — carrying it
   silently would make the editor lie about what the cell renders, and dropping it (what the round-trip did
   before it learned the key) destroyed the pin on every unrelated save. The live preview runs on the
   notebook's own scope, not the pin — the strip says which window the saved cell will actually render. */
function pinnedWindowStrip(cell, ctx) {
  const range = cellRange(cell);
  if (!range) return null;
  const unpin = el("button", { class: "btn small", type: "button", text: "Unpin", "aria-label": "Remove this cell's pinned time window" });
  unpin.addEventListener("click", () => {
    delete cell.range;
    ctx.rebuildCells();
  });
  return el("div", { class: "cell-pin-strip" }, [
    el("span", { class: "pin-badge", text: "Pinned: " + pinRangeLabel(range) }),
    el("span", { class: "muted", text: "This cell keeps its own time window; the notebook's time range does not apply to it." }),
    unpin,
  ]);
}

function notebookBackHead(id) {
  return el("div", { class: "page-head" }, [
    el("a", { href: id != null && id !== "new" ? "#/notebook/" + encodeURIComponent(id) : "#/views", text: "← Back" }),
    el("h2", { text: "Notebook composer" }),
  ]);
}

function swap(arr, i, j) {
  const tmp = arr[i];
  arr[i] = arr[j];
  arr[j] = tmp;
}

/* ─────────────────────────── templates (scope B) ─────────────────────────── */

/*
 * Seed notebooks for common investigations. Each template is a full notebook DEFINITION — prose markdown headings
 * plus panel cells that reference REAL catalog measures / sources / dimensions / annotation sources (grounded
 * against MeasureCatalog so a seeded notebook validates + renders immediately). Templates declare no view
 * variables: every panel shows data out of the box, scoped by the built-in server + time bar (grouping gives the
 * per-database / per-object breakdowns). A count-aggregate panel is unitless ('count'); other panels carry the
 * measure's default unit for a labeled axis; ratios omit the aggregate.
 */

/** A markdown cell for a template. */
function md(text) {
  return { type: "markdown", text };
}

/** A time-series panel cell (bucketed over time). */
function tsPanel(o) {
  const cell = { type: "panel", source: o.source, viz: o.viz || "line", timeBucket: o.timeBucket || "hour", title: o.title };
  applyMetric(cell, o);
  if (Array.isArray(o.groupBy) && o.groupBy.length) cell.groupBy = o.groupBy;
  /* The #1606 dual-axis second measure. Only legal on an UNGROUPED line/area (the server rejects it
     anywhere else), so a template using it must not also set groupBy. */
  if (o.overlay) cell.overlay = o.overlay;
  if (Array.isArray(o.annotations) && o.annotations.length) cell.annotations = o.annotations;
  if (Array.isArray(o.thresholds) && o.thresholds.length) cell.thresholds = o.thresholds;
  return cell;
}

/** A single-value (stat tile) panel cell: no timeBucket and no topN is what makes it scalar mode. */
function statPanel(o) {
  const cell = { type: "panel", source: o.source, viz: o.viz || "stat", title: o.title };
  applyMetric(cell, o);
  return cell;
}

/** A ranked (top-N) panel cell. */
function rankedPanel(o) {
  const cell = { type: "panel", source: o.source, viz: o.viz || "bar", topN: o.topN || 10, title: o.title, groupBy: o.groupBy };
  applyMetric(cell, o);
  return cell;
}

/* Apply the metric fields common to both shapes: a ratio names `ratio` (no aggregate); a scalar measure names
   `measure` + `aggregate`. A `count` aggregate is unitless ('count'); otherwise carry the given unit. */
function applyMetric(cell, o) {
  if (o.ratio) {
    cell.ratio = o.ratio;
    if (o.unit) cell.unit = o.unit;
    return;
  }
  cell.measure = o.measure;
  cell.aggregate = o.aggregate;
  cell.unit = o.aggregate === "count" ? "count" : o.unit;
}

export const NOTEBOOK_TEMPLATES = [
  {
    key: "blocking-rca",
    label: "Blocking-chain RCA",
    description: "Root-cause a blocking incident: when it spiked, what was contended, which databases, which lock modes.",
    make() {
      return {
        name: "Blocking-chain RCA",
        description: "Root-cause analysis for a blocking incident.",
        definition: {
          kind: NOTEBOOK_KIND,
          cells: [
            md(
              "# Blocking-chain root cause analysis\n\n" +
                "Work top-to-bottom: find **when** blocking spiked, **what** was contended, **which** databases, and the " +
                "**lock modes** involved. Set the server and time range at the top to scope every panel. The timeline " +
                "carries deadlock markers so you can see whether blocking and deadlocks cluster together."
            ),
            md("## 1. Blocking over time"),
            tsPanel({
              title: "Blocked-process reports over time",
              source: "blocked_process_reports",
              measure: "bpr_wait_time_ms",
              aggregate: "count",
              viz: "line",
              annotations: ["deadlocks"],
            }),
            md("## 2. Most-contended objects"),
            rankedPanel({
              title: "Most-blocked objects",
              source: "blocked_process_reports",
              measure: "bpr_wait_time_ms",
              aggregate: "count",
              viz: "bar",
              groupBy: ["contentious_object"],
              topN: 10,
            }),
            md("## 3. Blocking by database"),
            rankedPanel({
              title: "Blocking by database",
              source: "blocked_process_reports",
              measure: "bpr_wait_time_ms",
              aggregate: "count",
              viz: "bar",
              groupBy: ["database_name"],
              topN: 10,
            }),
            md("## 4. Lock modes involved"),
            rankedPanel({
              title: "Lock modes",
              source: "blocked_process_reports",
              measure: "bpr_wait_time_ms",
              aggregate: "count",
              viz: "pie",
              groupBy: ["lock_mode"],
              topN: 8,
            }),
            md(
              "### Next steps\n\n" +
                "- Open the blocking chain for the spike window to find the head blocker.\n" +
                "- Cross-check the contended objects against index and query activity.\n" +
                "- If deadlocks cluster with the blocking, continue in the **Deadlock post-mortem** notebook."
            ),
          ],
        },
      };
    },
  },
  {
    key: "deadlock-postmortem",
    label: "Deadlock post-mortem",
    description: "Reconstruct a deadlock incident: frequency, affected databases, and correlated blocking.",
    make() {
      return {
        name: "Deadlock post-mortem",
        description: "Reconstruct and explain a deadlock incident.",
        definition: {
          kind: NOTEBOOK_KIND,
          cells: [
            md(
              "# Deadlock post-mortem\n\n" +
                "Reconstruct a deadlock incident: how often it fired, on which databases, and how it lines up with " +
                "blocking. Scope with the server and time controls above."
            ),
            md("## Deadlocks over time"),
            tsPanel({
              title: "Deadlocks over time",
              source: "deadlocks",
              measure: "deadlock_count",
              aggregate: "count",
              viz: "line",
              annotations: ["blocked_process_reports"],
            }),
            md("## Deadlocks by database"),
            rankedPanel({
              title: "Deadlocks by database",
              source: "deadlocks",
              measure: "deadlock_count",
              aggregate: "count",
              viz: "bar",
              groupBy: ["database_name"],
              topN: 10,
            }),
            md("## Correlated blocking"),
            tsPanel({
              title: "Blocked-process reports (deadlock markers)",
              source: "blocked_process_reports",
              measure: "bpr_wait_time_ms",
              aggregate: "count",
              viz: "line",
              annotations: ["deadlocks"],
            }),
            md(
              "### Reading the deadlock graph\n\n" +
                "Open the **Deadlocks** detail for the spike window to see the victim, the surviving session, and the " +
                "resources in the cycle. Look for a consistent object or index at the center of repeated cycles — that is " +
                "usually where an index or access-order change breaks the deadlock."
            ),
          ],
        },
      };
    },
  },
  {
    key: "what-changed",
    label: "What changed at 2am?",
    description: "Line up configuration / default-trace and system_health events against CPU to find what shifted.",
    make() {
      return {
        name: "What changed at 2am?",
        description: "Config-drift and default-trace correlation for an overnight change.",
        definition: {
          kind: NOTEBOOK_KIND,
          cells: [
            md(
              "# What changed at 2am?\n\n" +
                "Something shifted overnight. Line up configuration / default-trace events and `system_health` against " +
                "CPU to find the moment things changed. Set the time range to bracket the incident window."
            ),
            md("## Default-trace events over time"),
            tsPanel({
              title: "Default-trace events over time",
              source: "default_trace_events",
              measure: "deftrace_duration_us",
              aggregate: "count",
              viz: "line",
              annotations: ["system_health_events", "deadlocks"],
            }),
            md("## Events by type"),
            rankedPanel({
              title: "Default-trace events by type",
              source: "default_trace_events",
              measure: "deftrace_duration_us",
              aggregate: "count",
              viz: "bar",
              groupBy: ["event_name"],
              topN: 10,
            }),
            md("## Events by database"),
            rankedPanel({
              title: "Default-trace events by database",
              source: "default_trace_events",
              measure: "deftrace_duration_us",
              aggregate: "count",
              viz: "bar",
              groupBy: ["database_name"],
              topN: 10,
            }),
            md("## Correlate with CPU"),
            tsPanel({
              title: "SQL Server CPU % (event markers)",
              source: "cpu_utilization_stats",
              measure: "sqlserver_cpu_utilization",
              aggregate: "avg",
              unit: "percent",
              viz: "line",
              annotations: ["default_trace_events", "system_health_events"],
            }),
            md(
              "### Where to look next\n\n" +
                "- Check server and database configuration history for changes inside the window.\n" +
                "- Review trace flags and any Agent jobs that run overnight.\n" +
                "- If CPU jumped with no event, correlate with the query and wait notebooks."
            ),
          ],
        },
      };
    },
  },
  {
    key: "memory-oom",
    label: "Memory / OOM walkthrough",
    description: "Follow the memory story from the whole server down to clerks and query-memory grant pressure.",
    make() {
      return {
        name: "Memory / OOM walkthrough",
        description: "Diagnose server memory and query-memory grant pressure.",
        definition: {
          kind: NOTEBOOK_KIND,
          cells: [
            md(
              "# Memory / OOM walkthrough\n\n" +
                "Follow the memory story from the whole server down to individual clerks and query-memory grants. " +
                "`RESOURCE_SEMAPHORE` waits, grant timeouts, and a climbing used-grant % all point at query-memory pressure."
            ),
            md("## Server memory over time"),
            tsPanel({
              title: "Total server memory",
              source: "memory_stats",
              measure: "mem_total_server_mb",
              aggregate: "avg",
              unit: "mb",
              viz: "line",
            }),
            md("## Query-memory grants"),
            tsPanel({
              title: "Granted query memory",
              source: "memory_grant_stats",
              measure: "grant_granted_mb",
              aggregate: "avg",
              unit: "mb",
              viz: "line",
            }),
            tsPanel({
              title: "Query-memory grant used %",
              source: "memory_grant_stats",
              ratio: "grant_used_pct",
              unit: "percent",
              viz: "line",
              thresholds: [90],
            }),
            md("## Grant pressure: waiters & timeouts"),
            tsPanel({
              title: "Memory-grant waiters (peak)",
              source: "memory_grant_stats",
              measure: "grant_waiters",
              aggregate: "max",
              unit: "count",
              viz: "line",
            }),
            tsPanel({
              title: "Memory-grant timeouts",
              source: "memory_grant_stats",
              measure: "grant_timeouts",
              aggregate: "sum",
              unit: "count",
              viz: "line",
            }),
            md("## Top memory clerks"),
            rankedPanel({
              title: "Top memory clerks",
              source: "memory_clerks",
              measure: "clerk_memory_mb",
              aggregate: "max",
              unit: "mb",
              viz: "bar",
              groupBy: ["clerk_type"],
              topN: 10,
            }),
            md(
              "### If you see RESOURCE_SEMAPHORE waits\n\n" +
                "That is queries waiting for a memory grant. Cross-reference the wait stats, look for oversized grants in " +
                "Query Store, and review `MAX_GRANT_PERCENT` / Resource Governor limits."
            ),
          ],
        },
      };
    },
  },
  {
    key: "ag-health",
    label: "AG Health",
    description: "Availability Group replication health: secondary lag, send vs redo throughput, queue backlogs, and drain estimates.",
    make() {
      return {
        name: "AG Health",
        description: "Availability Group replication health and latency.",
        definition: {
          kind: NOTEBOOK_KIND,
          cells: [
            md(
              "# Availability Group health\n\n" +
                "How far behind the secondaries are, and why. Set the server and time range at the top to scope every " +
                "panel — **point them at the PRIMARY**: `sys.dm_hadr_*` only carries the local replica's rows on a " +
                "secondary, so a secondary-scoped view shows that one replica's self-view and nothing about its peers.\n\n" +
                "Reading order: lag says *how bad*, the rate and queue panels say *why*, and the drain estimate says " +
                "*how long until it clears at the current rate*."
            ),
            md("## 1. How far behind is each secondary?"),
            tsPanel({
              title: "Secondary lag by replica",
              source: "ag_database_replica_states",
              measure: "ag_secondary_lag",
              aggregate: "max",
              unit: "s",
              viz: "line",
              groupBy: ["replica_server_name"],
            }),
            md(
              "Lag climbing on one replica but not the others points at that node (network, disk, CPU); climbing on all " +
                "of them points at the primary's log-generation rate. A replica whose data movement is **suspended** " +
                "shows lag accruing here, so a spike is not automatically a performance problem — check the " +
                "synchronization state before chasing it."
            ),
            md("## 2. Send vs redo: which side is the bottleneck?"),
            tsPanel({
              title: "Log send rate vs redo rate",
              source: "ag_database_replica_states",
              measure: "ag_log_send_rate",
              aggregate: "avg",
              unit: "kb",
              viz: "line",
              overlay: { measure: "ag_redo_rate", aggregate: "avg", unit: "kb" },
            }),
            md(
              "Both are KB/second. Send outpacing redo means the secondary is receiving faster than it can replay — the " +
                "redo queue grows and failover time with it. Redo at or above send means the secondary is keeping up. " +
                "(Ungrouped by design: a dual-axis panel plots one series per axis.)"
            ),
            md("## 3. Where is the redo backlog?"),
            tsPanel({
              title: "Redo queue by database",
              source: "ag_database_replica_states",
              measure: "ag_redo_queue",
              aggregate: "max",
              unit: "kb",
              viz: "stacked",
              groupBy: ["database_name"],
            }),
            md("## 4. How long until the redo queue clears?"),
            statPanel({
              title: "Estimated redo drain (worst, minutes)",
              source: "ag_database_replica_states",
              measure: "ag_est_redo_drain_min",
              aggregate: "max",
              unit: "min",
            }),
            md(
              "Queue ÷ rate at each sample's own instant, so it reflects the rate that actually applied. **Blank means " +
                "there is no drain rate** — idle, caught up, or suspended — not that it drains instantly."
            ),
            md("## 5. Which databases are backing up on the send side?"),
            rankedPanel({
              title: "Log send queue by database",
              source: "ag_database_replica_states",
              measure: "ag_log_send_queue",
              aggregate: "max",
              unit: "kb",
              viz: "bar",
              groupBy: ["database_name"],
              topN: 10,
            }),
            md(
              "### Next steps\n\n" +
                "- Send-side backlog with a healthy redo rate is usually network or primary log throughput.\n" +
                "- Redo-side backlog is the secondary: check its CPU, disk latency, and whether redo is single-threaded " +
                "for that database.\n" +
                "- For primary-side commit cost, chart `Transaction Delay` and `Mirrored Write Transactions/sec` from " +
                "`perfmon_stats` — their ratio is the average delay per mirrored transaction — and cross-check " +
                "`HADR_SYNC_COMMIT` in wait stats."
            ),
          ],
        },
      };
    },
  },
];
