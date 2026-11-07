/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Custom Views (#1563): the list page and the renderer. A view is stored JSON {panels:[<renderPanel descriptor>]}
 * authored in the composer (editor.js); this page RENDERS it by mapping each stored panel through the UNMODIFIED
 * renderPanel into a `.panel-grid` — so a stored dashboard uses the exact same viz registry the built-in pages do
 * (per-panel isolation is free: renderPanel error-strips its own fetch/throw/unknown-viz). Before calling
 * renderPanel, each panel's read is checked against the cached catalog and its viz against the VIZ registry, so a
 * stale/unknown read renders a clean "unknown read" strip instead of an opaque 404 inside the panel.
 *
 * Starter DASHBOARD templates (#2476) sit beside the notebook ones in the "New from template" menu, and are what
 * the first-run hero now offers: nothing ships seeded, so without them a new user meets an empty page and a blank
 * canvas over an 82-read catalog. A dashboard template is scoped to ONE server, so the menu carries a server
 * picker; a notebook template is not, and links straight to the notebook composer as it always did. Creating one
 * POSTs an ordinary view the user owns and can edit or delete for good — nothing is re-seeded on upgrade.
 *
 * Edit affordances (New / Edit / Delete / Import) are shown when the session reports can_edit (the server is the
 * authority for every write); RENDER and EXPORT are available to every seat. All user text (names,
 * descriptions) reaches the DOM through el()/textContent (R4 — never innerHTML).
 */

import { el, mount, apiGetFleet, loadingStrip, errorStrip, emptyStrip, noticeStrip, relTime, localTime, localizeIsoText, localizeAlertDetail, fmtNum, storeHash } from "../util.js";
import { rollingHoursPicker } from "../page-range.js";
import { renderPanel, setPanelSignal, VIZ } from "../panels.js";
import { holdsPage, keepScroll, refreshGridInPlace, showAround, showPage, showRefreshError } from "../refresh-in-place.js";
import { orderServers } from "../server-order.js";
import { renderComposedPanelCard } from "../compose.js";
import { keepLonePanelsWide } from "../panel-layout.js";
import { renderMarkdown } from "../markdown.js";
import { NOTEBOOK_TEMPLATES, isNotebookDefinition } from "../notebook.js";
import { DASHBOARD_TEMPLATES } from "../view-templates.js";
import { resolveReadTable } from "../read-fields.js";
import * as api from "../views-api.js";
import { refreshChoiceOf } from "../refresh-policy.js";
import { buildRefreshControl } from "../refresh-control.js";
import { viewAccess, folderOf } from "../view-folders.js";

/* #4666: the saved view/notebook on screen and its effective auto-refresh choice. app.js reads it through
   currentViewRefresh() to time the page's own re-render, and is told about a change through
   onViewRefreshChange(). A choice made by a session that cannot save is remembered per view id for this page
   load only, so the 5-minute re-render does not snap the select back to the stored value. */
let currentRefresh = null;
let refreshListener = null;
const sessionRefreshChoice = new Map();

/** The loaded saved view's effective refresh choice ("off" | "1m" | "5m" | "15m"), or null off a view page. */
export function currentViewRefresh() {
  return currentRefresh;
}

/** app.js registers one callback, called whenever the operator changes the loaded view's choice. */
export function onViewRefreshChange(fn) {
  refreshListener = fn;
}

/** Clears the loaded view's choice; app.js calls it when the route leaves a view page. */
export function clearViewRefresh() {
  currentRefresh = null;
}

/** The labelled "Auto-refresh:" select for a saved view or notebook page. */
function viewRefreshControl(view, def, isNotebook, canEdit) {
  const id = String(view.id);
  const stored = refreshChoiceOf(def, isNotebook);
  const initial = sessionRefreshChoice.has(id) ? sessionRefreshChoice.get(id) : stored;
  currentRefresh = initial;
  const control = buildRefreshControl(initial, async (choice) => {
    currentRefresh = choice;
    if (refreshListener) refreshListener();
    if (!canEdit) {
      sessionRefreshChoice.set(id, choice);
      control.note("not saved: read-only");
      return;
    }
    control.note("Saving…");
    const res = await api.updateView(view.id, {
      name: view.name,
      description: view.description ?? null,
      definition: { ...def, refresh: choice },
      version: view.version,
    });
    if (res.kind === "data" && res.data) {
      view.version = res.data.version;
      def.refresh = choice;
      sessionRefreshChoice.delete(id);
      control.note("Saved");
    } else {
      sessionRefreshChoice.set(id, choice);
      control.note("not saved: " + (res.message || "save failed"));
    }
  });
  return control.root;
}

/* ─────────────────────────── list page ─────────────────────────── */

/* #7925: the key the list page is held under (refresh-in-place.js), so the 60 s poll can tell that the list on screen is its own. */
const LIST_KEY = "views";

/* #7925: each card's count chips by view id, with the version they were read at. The counts come from the view's full definition,
   which the list payload does not carry, so a card whose version is unchanged draws its chips from here instead of reading the
   definition again on every poll. */
const chipCache = new Map();

export async function renderViewList(main, opts) {
  /* #7925: the poll over the list on screen leaves it there while the list is read again. The import JSON a reader has pasted, an open
     Import or template menu and the server picked for a template are what they are working with, and a "Loading views…" strip in
     their place would throw them away. When the new list arrives only the cards are redrawn. */
  const inPlace = holdsPage(main, LIST_KEY, !!(opts && opts.poll === true));
  if (!inPlace) mount(main, [listHead(false, []), loadingStrip("Loading views…")]);

  /* #6243: the list needs only the session and the view list. The fleet read exists for the dashboard templates' server
     picker, so the list is drawn the moment those two answer; a slow or hung fleet read used to hold the page on
     "Loading views…". The picker is filled in when the fleet answers, if the page is still this one. The poll reads the
     fleet too: a server added since the first paint reaches the picker on screen once the read answers. */
  const serversRead = loadServerNames();
  const [session, res] = await Promise.all([api.getSession(), api.listViews()]);
  const canEdit = !!session.can_edit;
  const ourHash = location.hash;

  /* The page is kept through the poll only while it shows the hero or the cards. A first paint that failed left an error page under the
     key, and that is drawn again: there is no earlier list to keep under a refresh notice, and an empty list must draw the hero. */
  const held = inPlace && holdsPage(main, LIST_KEY, true) && showsViews(main);

  if (res.kind === "error") {
    if (held && showRefreshError(main, LIST_KEY, res.message)) return;
    showPage(main, LIST_KEY, [listHead(canEdit, []), errorStrip(res.message)]);
    return;
  }

  const views = Array.isArray(res.data) ? res.data : [];
  /* The affordances follow the session: a seat whose can_edit differs from what the page was drawn for (a probe that failed at the first
     paint and has since answered, #7928) gets the page drawn again. Only the cards are swapped otherwise, and the fleet answer goes into
     the pickers on screen when it arrives, so a slow fleet read does not hold the cards. */
  if (held && editShown(main) === canEdit && swapCards(main, views)) {
    const servers = await serversRead;
    if (location.hash === ourHash && holdsPage(main, LIST_KEY, true)) refreshPickers(main, servers);
    return;
  }

  const head = listHead(canEdit, []);
  const nodes = [head];

  /* Empty list -> a real first-run hero that sells the feature (M4), not a bare strip. The hero carries its own
     New view + Import affordances (can_edit only), so the standalone import panel is only shown once views exist. */
  if (!views.length) {
    nodes.push(firstRunHero(canEdit, []));
  } else {
    if (canEdit) nodes.push(importPanel());
    nodes.push(el("div", { class: "view-cards" }, views.map((v) => viewCard(v))));
  }
  showPage(main, LIST_KEY, nodes);

  const servers = await serversRead;
  if (!servers.length || location.hash !== ourHash || main.firstChild !== head) return;
  if (!views.length) {
    showPage(main, LIST_KEY, [listHead(canEdit, servers), firstRunHero(canEdit, servers)]);
    return;
  }
  /* Only the head holds the picker: its children are swapped in place so the cards, their own reads and the import box are not
     rebuilt, and the head stays the first child the poll knows the page by. */
  mount(head, Array.from(listHead(canEdit, servers).children));
}

/* #7925: the poll's redraw of a list that is on screen. The cards are replaced in one step, with the reader's scroll position kept,
   and the head and the import panel stay as they are; an earlier poll's failure notice comes down. True only when the page on screen
   still fits the list: the cards with views to draw into them, or the hero over a store that still has no views, which stays as it is.
   False otherwise, and the caller draws the whole page: the hero when views now exist, a list whose views are all gone, and an error
   page, which shows neither. */
function swapCards(main, views) {
  const children = () => Array.from(main.children || []);
  const live = children().find((n) => hasClass(n, "view-cards"));
  if (!live) return !views.length && children().some((n) => hasClass(n, "views-hero"));
  if (!views.length) return false;
  keepScroll(() => {
    const stale = children().find((n) => hasClass(n, "refresh-error"));
    if (stale) main.removeChild(stale);
    main.replaceChild(el("div", { class: "view-cards" }, views.map((v) => viewCard(v))), live);
  });
  return true;
}

/* True while the page shows the hero or the cards, the two pages a poll keeps on screen. An error page is neither. */
function showsViews(main) {
  return Array.from(main.children || []).some((n) => hasClass(n, "view-cards") || hasClass(n, "views-hero"));
}

/* The can_edit the page on screen was drawn for. The template menu is drawn only for a seat that can edit, in the head and in the hero
   alike, so its presence says which variant is up. */
function editShown(main) {
  return findAll(main, (n) => hasClass(n, "template-menu")).length > 0;
}

/* #7925: the poll's fleet answer, put into the pickers on screen: the head's template menu, and over the hero its menu and its starter
   row. A picker that is up keeps its element and its picked server, so an open menu stays open and a pick stays picked; only its
   options follow the fleet. A menu or starter row drawn while the fleet had no server gains the picker. An empty answer (a fleet with
   no server, or a failed read) leaves the pickers as they are, as the first paint leaves them. */
function refreshPickers(main, servers) {
  if (!servers.length) return;
  for (const menu of findAll(main, (n) => hasClass(n, "template-menu"))) {
    const select = findAll(menu, (n) => hasClass(n, "starter-select"))[0];
    const list = findAll(menu, (n) => hasClass(n, "template-list"))[0];
    if (select) refreshServerSelect(select, servers);
    else if (list) mount(list, Array.from(findAll(templateMenu(servers), (n) => hasClass(n, "template-list"))[0].children));
  }
  const row = findAll(main, (n) => hasClass(n, "hero-suggest"))[0];
  if (row) {
    const select = findAll(row, (n) => hasClass(n, "starter-select"))[0];
    if (select) refreshServerSelect(select, servers);
    else mount(row, Array.from(starterRow(servers).children));
  }
}

/* The picker's options follow the fleet, and the picked server stays picked while the fleet still has it. A fleet that matches the
   options already shown changes nothing. */
function refreshServerSelect(select, servers) {
  const shown = Array.from(select.children || []);
  if (shown.length === servers.length && shown.every((o, i) => o.getAttribute("value") === servers[i].value && o.textContent === servers[i].label)) return;
  const picked = select.value;
  mount(select, servers.map((s) => el("option", { value: s.value, text: s.label })));
  select.value = servers.some((s) => s.value === picked) ? picked : servers[0].value;
}

function hasClass(n, name) {
  return typeof n.className === "string" && (" " + n.className + " ").includes(" " + name + " ");
}

function findAll(node, pred, out = []) {
  if (pred(node)) out.push(node);
  for (const c of Array.from(node.children || [])) findAll(c, pred, out);
  return out;
}

/* First-run hero (M4): a centered pitch + a prominent New view + a few starting-point ideas + the (secondary)
   import. A read-only network seat can't compose, so it gets an explanatory variant (no New view / import). */
function firstRunHero(canEdit, servers) {
  if (!canEdit) {
    return el("div", { class: "views-hero" }, [
      el("div", { class: "hero-title", text: "No custom views yet" }),
      el("div", {
        class: "hero-pitch",
        text:
          "Your session couldn't be confirmed, so the composer is read-only right now — reload the page to create a " +
          "view. Once created, custom views appear here for every seat to open and export.",
      }),
    ]);
  }
  return el("div", { class: "views-hero" }, [
    el("div", { class: "hero-title", text: "Compose your own dashboards & notebooks" }),
    el("div", {
      class: "hero-pitch",
      text:
        "Build a DASHBOARD — pick a metric, aggregate it, group and filter it, and chart it as a line, bar, pie, " +
        "stacked area, or stat — or a NOTEBOOK that mixes markdown notes with those same charts into a guided " +
        "investigation. Both save for every seat and re-scope live by server and time range. No SQL to write.",
    }),
    el("div", { class: "hero-ctas" }, [
      el("a", { class: "btn primary hero-cta", href: storeHash("#/view/new"), text: "New view" }),
      el("a", { class: "btn hero-cta", href: storeHash("#/notebook/new"), text: "New notebook" }),
      templateMenu(servers),
    ]),
    starterRow(servers),
    importPanel(),
  ]);
}

/* The hero's starting points, which used to be three inert chips reading "Top waits by server", "CPU trend over
   time" and "Slowest procedures by database" — a promise with no handler behind it, on the one page a new user
   arrives at with nothing. They are now the real dashboard templates, and clicking one creates it. The server
   picker sits beside them because a dashboard is scoped to one server: choosing it here rather than making the
   reader set it on five panels in the composer is the whole point of a starting point. */
function starterRow(servers) {
  const status = el("div", { class: "starter-status" });
  if (!servers.length) {
    return el("div", { class: "hero-suggest" }, [
      el("span", { class: "hero-suggest-label", text: "Starting points" }),
      el("div", {
        class: "muted",
        text: "The ready-made dashboards are scoped to a server, so they appear once the fleet has one.",
      }),
    ]);
  }
  const select = serverSelect(servers);
  return el("div", { class: "hero-suggest" }, [
    el("span", { class: "hero-suggest-label", text: "Ready-made dashboards" }),
    el("label", { class: "starter-server" }, [el("span", { text: "for" }), select]),
    el(
      "div",
      { class: "hero-suggest-chips" },
      DASHBOARD_TEMPLATES.map((t) =>
        el("button", {
          class: "hero-chip clickable",
          type: "button",
          title: t.description,
          text: t.label,
          onClick: () => createFromTemplate(t, select.value, status),
        })
      )
    ),
    status,
  ]);
}

function listHead(canEdit, servers) {
  return el("div", { class: "page-head" }, [
    el("h2", { text: "Custom Views" }),
    el("div", { class: "spacer" }),
    canEdit ? templateMenu(servers) : null,
    canEdit ? el("a", { class: "btn", href: storeHash("#/notebook/new"), text: "New notebook" }) : null,
    canEdit ? el("a", { class: "btn primary", href: storeHash("#/view/new"), text: "New view" }) : null,
  ]);
}

/* The "New from template" menu: a <details> dropdown of the seed DASHBOARDS (#2476) and the seed NOTEBOOKS
   (#1563 D7). The two groups behave differently on purpose — a notebook template links to the composer
   pre-filled, because its composed panels re-scope live from the notebook's own server control, while a dashboard
   template is v1 read panels whose `server` param is static, so it is created against the server picked here.
   Built with el()/textContent like everything else. */
function templateMenu(servers) {
  const list = [];
  const status = el("div", { class: "starter-status" });

  list.push(el("div", { class: "template-group-label", text: "Dashboards" }));
  if (!(servers || []).length) {
    list.push(
      el("div", {
        class: "template-note",
        text: "Scoped to a server — available once the fleet has one.",
      })
    );
  } else {
    const select = serverSelect(servers);
    list.push(el("label", { class: "template-server" }, [el("span", { text: "Server" }), select]));
    for (const t of DASHBOARD_TEMPLATES) {
      list.push(
        el("button", { class: "template-item", type: "button", title: t.description, onClick: () => createFromTemplate(t, select.value, status) }, [
          el("span", { class: "template-label", text: t.label }),
          el("span", { class: "template-desc", text: t.description }),
        ])
      );
    }
    list.push(status);
  }

  list.push(el("div", { class: "template-group-label", text: "Notebooks" }));
  for (const t of NOTEBOOK_TEMPLATES) {
    list.push(
      el("a", { class: "template-item", href: storeHash("#/notebook/new/") + encodeURIComponent(t.key), title: t.description }, [
        el("span", { class: "template-label", text: t.label }),
        el("span", { class: "template-desc", text: t.description }),
      ])
    );
  }

  return el("details", { class: "template-menu" }, [
    el("summary", { class: "btn template-summary", text: "New from template ▾" }),
    el("div", { class: "template-list" }, list),
  ]);
}

/* The server <select> both template affordances share. Value is the stored server_name (what a read resolves
   against); label is the display name. */
function serverSelect(servers) {
  const sel = el(
    "select",
    { class: "starter-select", "aria-label": "Server for the new dashboard" },
    servers.map((s) => el("option", { value: s.value, text: s.label }))
  );
  sel.value = servers[0].value;
  return sel;
}

/* Create a template as an ORDINARY view and open it. The backend re-validates the definition as the authority, so
   a template that ever named a removed read is refused with its own message rather than saved broken — which is
   why the failure is surfaced verbatim here instead of being flattened to "could not create". A 409 means the name
   is taken, which on a second click of the same template is the likely case, so it gets its own sentence. */
async function createFromTemplate(template, server, status) {
  if (!server) {
    mount(status, errorStrip("Pick a server first."));
    return;
  }
  mount(status, loadingStrip("Creating “" + template.label + "”…"));
  const res = await api.createView(template.make(server));
  if (res.kind === "data" && res.data && res.data.id != null) {
    location.hash = storeHash("#/view/") + encodeURIComponent(res.data.id);
    return;
  }
  const message =
    res.status === 409
      ? "A view named “" + template.make(server).name + "” already exists — open it from the list, or rename it first."
      : res.message || "Could not create this dashboard.";
  mount(status, errorStrip(message));
}

/* The fleet's server names for the template picker. Deliberately its own tiny read rather than importing the
   composer's loadFleetOptions: that helper also enriches each option with the D4 platform fields the composed
   measure picker greys on, none of which a v1 read template uses, and importing it would couple this page to the
   editor module for a name and a label. Returns [] on any failure; every caller handles an empty fleet. */
async function loadServerNames() {
  const res = await apiGetFleet();
  if (res.kind !== "data" || !res.data) return [];
  return orderServers(res.data.cards || [])
    .map((c) => ({ value: c.server_name || c.display_name, label: c.display_name || c.server_name }))
    .filter((o) => o.value);
}

function viewCard(v) {
  const isNotebook = v.kind === "notebook";
  /* #6214: where the view sits. A personal view says whose it is; a shared one names its folder. */
  const folder = folderOf(v);
  const placed = v.personal === true ? "personal: " + (v.created_by || "unknown") + " · " : folder !== null ? "folder " + folder + " · " : "";
  const meta =
    placed + "v" + v.version + " · updated " + relTime(v.updated_at) + (v.updated_by ? " by " + v.updated_by : "");
  const chips = el("div", { class: "vc-chips" });
  /* The list summary carries a concrete `kind` (backend #56), so the link + "Notebook" badge are set SYNCHRONOUSLY
     here — no wait on the per-card definition fetch (which only fills in the count chips). */
  const href = storeHash((isNotebook ? "#/notebook/" : "#/view/") + encodeURIComponent(v.id));
  const card = el("a", { class: "view-card card", href }, [
    el("div", { class: "vc-name" }, [
      el("span", { class: "vc-name-text", text: v.name, title: v.name }),
      isNotebook ? el("span", { class: "notebook-badge", text: "Notebook" }) : null,
    ]),
    v.description ? el("div", { class: "vc-desc", text: v.description }) : null,
    chips,
    el("div", { class: "vc-meta", text: meta }),
  ]);
  enrichCard(v.id, v.version, chips, isNotebook);
  return card;
}

/* The list summary carries `kind` (link + badge already set in viewCard) but NOT the panel/cell counts, so those
   are fetched per card and filled in progressively — the card renders immediately and never blocks on this fetch,
   and a failed/empty fetch just leaves the (display:none-when-empty) chip row absent. `isNotebook` comes from the
   summary; the fetched definition is a robust fallback for the count shape. The chips a read gave are kept by view id
   and version (#7925), so a card whose version the list still reports draws them without a read. */
async function enrichCard(id, version, chips, isNotebook) {
  const kept = chipCache.get(String(id));
  if (kept && kept.version === version) {
    mount(chips, kept.chips.map((c) => el("span", { class: c.cls, text: c.text })));
    return;
  }
  const res = await api.getView(id);
  if (res.kind !== "data" || !res.data) return;
  const texts = chipTexts(res.data.definition || {}, isNotebook);
  chipCache.set(String(id), { version, chips: texts });
  mount(chips, texts.map((c) => el("span", { class: c.cls, text: c.text })));
}

/* The count chips for a definition, as class and text: a notebook's cells and the panels among them, or a dashboard's panels and
   the viz types they use. Empty for a definition with nothing to count. */
function chipTexts(def, isNotebook) {
  if (isNotebook || isNotebookDefinition(def)) {
    const cells = Array.isArray(def.cells) ? def.cells : [];
    if (!cells.length) return [];
    const panelCount = cells.filter((c) => c && c.type === "panel").length;
    const texts = [{ cls: "vc-chip count", text: cells.length + (cells.length === 1 ? " cell" : " cells") }];
    if (panelCount) texts.push({ cls: "vc-chip", text: panelCount + (panelCount === 1 ? " panel" : " panels") });
    return texts;
  }

  const panels = Array.isArray(def.panels) ? def.panels : [];
  if (!panels.length) return [];
  const vizTypes = [];
  for (const p of panels) {
    const viz = p && typeof p.viz === "string" ? p.viz : null;
    if (viz && !vizTypes.includes(viz)) vizTypes.push(viz);
  }
  const texts = [{ cls: "vc-chip count", text: panels.length + (panels.length === 1 ? " panel" : " panels") }];
  for (const viz of vizTypes) texts.push({ cls: "vc-chip viz", text: viz });
  return texts;
}

/* Paste-to-import (can_edit only): parse -> client-validate against the catalog -> POST (backend re-validates).
   Accepts an exported view/notebook {name, description?, definition} or a bare definition ({panels:[…]} dashboard
   or {kind:"notebook", cells:[…]} notebook) plus a name. */
function importPanel() {
  const textarea = el("textarea", {
    class: "import-box",
    rows: "4",
    placeholder: 'Paste exported view or notebook JSON — {"name": "...", "definition": { ... }}',
    "aria-label": "Paste exported view or notebook JSON",
  });
  const status = el("div", { class: "import-status" });
  const importBtn = el("button", { class: "btn", type: "button", text: "Import" });

  importBtn.addEventListener("click", async () => {
    mount(status, null);
    const raw = textarea.value.trim();
    if (!raw) {
      mount(status, errorStrip("Paste a view's JSON first."));
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      mount(status, errorStrip("That is not valid JSON: " + (e && e.message ? e.message : String(e))));
      return;
    }

    const def =
      parsed && parsed.definition ? parsed.definition : parsed && (parsed.panels || parsed.cells) ? parsed : null;
    if (!def) {
      mount(status, errorStrip("The JSON must contain a 'definition' (or be a definition with 'panels' or 'cells')."));
      return;
    }
    const name = (parsed.name || "").trim();
    if (!name) {
      mount(status, errorStrip('The imported JSON has no "name". Add a top-level "name" field and try again.'));
      return;
    }

    const catalog = await api.getCatalog();
    const invalid = api.validateDefinition(def, catalog);
    if (invalid) {
      mount(status, errorStrip(invalid));
      return;
    }

    importBtn.disabled = true;
    mount(status, el("div", { class: "strip loading", text: "Importing…" }));
    const result = await api.createView({ name, description: parsed.description || null, definition: def });
    importBtn.disabled = false;
    if (result.kind === "data" && result.data) {
      const route = storeHash(isNotebookDefinition(def) ? "#/notebook/" : "#/view/");
      location.hash = route + encodeURIComponent(result.data.id);
      return;
    }
    mount(status, errorStrip(result.message || "Could not import."));
  });

  return el("details", { class: "import-panel" }, [
    el("summary", { text: "Import a view or notebook" }),
    el("div", { class: "import-body" }, [textarea, el("div", { class: "import-actions" }, [importBtn]), status]),
  ]);
}

/* ─────────────────────────── renderer ─────────────────────────── */

/* Per-view scope memory: the picked server / time range / variable values survive the app's 60s background
   re-render (app.js refresh() → route() rebuilds the whole view), which otherwise snapped the range back to the
   view's default every minute. Keyed by view id, session-lived — a full page reload clears it, restoring the
   view's declared default. */
const viewScopeMemory = new Map();

/* The server scope a view is saved with (the morning walk, W4): its server-dimension variable's default, the same
   variable the editor greys measures by. The control opened on "All servers (fleet)" whatever the view saved, and the
   run body's explicit server "All" beat the variable on the service side, so a view saved for one server read the
   whole fleet. Only a concrete name that is in the fleet counts; "All", blank, a "$other" reference, several
   variables, or a name the fleet does not list keep the whole fleet. A comma or semicolon list is several servers. */
function savedServerScope(variables, fleet) {
  const serverVars = (variables || []).filter((v) => v && v.dimension === "server" && String(v.default || "").trim());
  if (serverVars.length !== 1) return "All";
  const raw = String(serverVars[0].default).trim();
  if (raw.charAt(0) === "$" || raw.toLowerCase() === "all") return "All";
  const names = [];
  for (const part of raw.split(/[,;]/)) {
    const want = part.trim();
    const o = want ? (fleet || []).find((x) => x.value === want || x.label === want) : null;
    if (o && !names.includes(o.value)) names.push(o.value);
  }
  return names.length ? names : "All";
}

/* The server a view's PANELS are set to (W4b of the release walk). A view can carry no server variable and instead filter
   every composed panel to one server ({dimension:"server", op:"eq", value:"SQL2025"} on each). The service ANDs a panel's
   filters with the scope, so such a view already read only that server while the picker said "All servers (fleet)", and
   picking another server meant every panel filtered to the first one and drew nothing. A view counts only when EVERY composed
   panel has exactly one server "eq" filter, all naming the same server (case ignored) and that server is in the fleet; a
   mixed view, a panel with any server filter other than that one "eq" (an "in", a "neq", a second "eq"), or a name the fleet
   does not list keeps the old behaviour. */
function serverFilters(p) {
  return (Array.isArray(p.filters) ? p.filters : []).filter((f) => f && f.dimension === "server");
}

function serverEqFilters(p) {
  return (Array.isArray(p.filters) ? p.filters : []).filter(
    (f) => f && f.dimension === "server" && f.op === "eq" && typeof f.value === "string" && f.value.trim()
  );
}

function panelFilterServer(panels, fleet) {
  const composed = (panels || []).filter((p) => p && typeof p === "object" && p.source != null);
  if (!composed.length) return null;
  let name = null;
  for (const p of composed) {
    const eq = serverEqFilters(p);
    /* The one "eq" must be the panel's ONLY server filter: panelUnderScope drops just the eq, so a leftover "in" or "neq" would
       still be ANDed with the picked server into an empty panel. */
    if (eq.length !== 1 || serverFilters(p).length !== 1) return null;
    const v = eq[0].value.trim().toLowerCase();
    if (name !== null && name !== v) return null;
    name = v;
  }
  const o = (fleet || []).find((x) => String(x.value).toLowerCase() === name || String(x.label).toLowerCase() === name);
  return o ? { value: o.value, label: o.label } : null;
}

/* True while the scope is exactly the server the panels are set to, the one case their own filter and the scope agree. */
function onPanelServer(state, panelServer) {
  return !!panelServer && Array.isArray(state.server) && state.server.length === 1 && state.server[0] === panelServer.value;
}

/* A panel as it runs under the picked scope. Choosing a server other than the one the panels are set to (or "All servers")
   REPLACES that setting for every panel: the panel's server filter is dropped so the scope decides, rather than the two
   being ANDed into an empty panel. Any other panel, and any scope that still matches, runs as saved. */
function panelUnderScope(p, state, panelServer) {
  if (!panelServer || onPanelServer(state, panelServer) || !p || typeof p !== "object" || p.source == null) return p;
  const kept = (Array.isArray(p.filters) ? p.filters : []).filter((f) => !serverEqFilters({ filters: [f] }).length);
  return { ...p, filters: kept };
}

/* "Your pick" is said only when the reader picked the server: state.picked is set by the server picker and remembered with the
   scope. A scope the view's own server variable chose is the view's setting, and the note says so. */
function panelServerNote(state, panelServer) {
  if (!panelServer) return "";
  if (onPanelServer(state, panelServer)) {
    return "This view sets its server: every panel is set to " + panelServer.label + ". Pick another server, or All servers, to replace that for every panel.";
  }
  if (state.picked) return "Showing your pick instead of " + panelServer.label + ", the server this view's panels are set to.";
  /* The view's own server variable can be "All" (or blank): then the scope is every server, not a server it names. */
  const allServers = state.server === "All" || state.server === "" || state.server == null || (Array.isArray(state.server) && state.server.length === 0);
  return allServers
    ? "Showing all servers, as this view's own setting says, instead of " + panelServer.label + ", the server this view's panels are set to."
    : "Showing the server this view's own setting names instead of " + panelServer.label + ", the server this view's panels are set to.";
}

function seedState(id, defaultHours, variables, fleet, panelServer) {
  const cached = viewScopeMemory.get(String(id));
  if (cached) {
    return { server: cached.server, hours: cached.hours, values: { ...cached.values }, picked: cached.picked === true };
  }
  const state = { server: savedServerScope(variables, fleet), hours: defaultHours, values: {}, picked: false };
  /* An explicit server variable decides first, "All" included: the panels' server seeds the scope only when the view has no
     server variable at all. */
  const hasServerVariable = (variables || []).some((v) => v && v.dimension === "server");
  if (state.server === "All" && panelServer && !hasServerVariable) state.server = [panelServer.value];
  for (const v of variables) {
    if (v.dimension !== "server" && v.default) state.values[v.name] = v.default;
  }
  return state;
}

function rememberScope(id, state) {
  viewScopeMemory.set(String(id), { server: state.server, hours: state.hours, values: { ...state.values }, picked: !!state.picked });
}

/* #6933: the grid swaps of the last poll, stopped when a newer render starts. */
let cancelViewRefresh = null;

export async function renderView(main, id, opts) {
  const ourHash = location.hash;
  const pageKey = "view:" + id;
  if (cancelViewRefresh) {
    cancelViewRefresh();
    cancelViewRefresh = null;
  }
  /* A poll reads again with the view still on screen, and swaps each panel's new body in when its own read returns. */
  const hold = holdsPage(main, pageKey, opts && opts.poll === true);
  if (!hold) mount(main, loadingStrip("Loading view…"));

  const [session, catalog, res, fleetRes] = await Promise.all([
    api.getSession(),
    api.getCatalog(),
    api.getView(id),
    apiGetFleet(),
  ]);

  /* If the user navigated away while we were loading, ignore this response — a late /api/fleet or view
     read should not replace the new page's content or start panels under the wrong route (#4204). */
  if (location.hash !== ourHash) return;

  if (res.kind === "error") {
    if (!showRefreshError(main, pageKey, res.message)) {
      mount(main, [el("div", { class: "page-head" }, [backToViews(), el("h2", { text: "View" })]), errorStrip(res.message)]);
    }
    return;
  }

  const view = res.data || {};
  const def = view.definition || {};

  /* A notebook is the same stored row with a kind:"notebook" definition — render it as a document, not a grid. On a poll it is
     refreshed in place like the grid below (#7924), so it is told whether the page is held and under which key. */
  if (isNotebookDefinition(def)) {
    renderNotebookDoc(main, { mode: "saved", view, session, catalog, fleetRes, hold, pageKey });
    return;
  }

  const panels = Array.isArray(def.panels) ? def.panels : [];
  /* #6214: another person's personal view can be read but not changed, so Edit goes and Delete is disabled. */
  const access = viewAccess(view, session);
  const canEdit = !!session.can_edit && !access.readOnly;
  const readSet = new Set((catalog.reads || []).map((r) => r.name));
  const sourceSet = new Set(((catalog.compose || {}).measures || []).map((m) => m.source));

  const status = el("div", { class: "view-status" });
  const head = el("div", { class: "page-head" }, [
    backToViews(),
    el("h2", { text: view.name || "View" }),
    view.description ? el("div", { class: "meta", text: view.description }) : null,
    personalNote(access),
    el("div", { class: "spacer" }),
    viewRefreshControl(view, def, false, canEdit),
    exportButton(view),
    canEdit ? el("a", { class: "btn small", href: storeHash("#/view/") + encodeURIComponent(id) + "/edit", text: "Edit" }) : null,
    canEdit ? deleteButton(id, status) : session.can_edit ? lockedDeleteButton(access) : null,
  ]);

  if (!panels.length) {
    showPage(main, pageKey, [head, status, emptyStrip("This view has no panels yet.")]);
    return;
  }

  /* View-level scope (Erik's Decision 1): server / time / template-variable controls at the top re-scope EVERY
     panel at once. The scope only affects composed panels (v1 read panels carry their own fixed params); flipping
     it re-runs the whole grid. `state` is the live control state; currentScope() snapshots it for a run. */
  const fleet = fleetOptions(fleetRes);
  const variables = Array.isArray(def.variables) ? def.variables.filter((v) => v && v.name) : [];
  const defaultHours = def.range && typeof def.range.hours === "number" ? def.range.hours : 24;
  const hasComposed = panels.some((p) => p && p.source != null);

  const panelServer = panelFilterServer(panels.filter((p) => p && p.source != null), fleet);
  const state = seedState(id, defaultHours, variables, fleet, panelServer);
  const scopeNote = panelServer && hasComposed ? el("div", { class: "meta view-scope-note", text: panelServerNote(state, panelServer) }) : null;

  function currentScope() {
    return {
      server: state.server,
      hours: state.hours,
      variables: variables.map((v) => ({ name: v.name, dimension: v.dimension, default: v.default || undefined })),
      values: { ...state.values },
    };
  }

  /* On a poll the grid already on screen stays and is refreshed in place (refreshGridInPlace), so the scope controls built below
     keep drawing into it. */
  const liveGrid = hold ? Array.from(main.children).find((n) => n.className === "panel-grid") : null;
  const gridBox = liveGrid || el("div", { class: "panel-grid" });
  if (!liveGrid) keepLonePanelsWide(gridBox); /* #7534: a half-width panel with no partner on its row takes the whole row. */
  /* #7257: the panels of a render start their reads two at a time. A new render ends the one before it. */
  let panelRender = null;
  function buildGrid() {
    if (scopeNote) scopeNote.textContent = panelServerNote(state, panelServer);
    if (panelRender) panelRender.end();
    const render = (panelRender = newPanelRender(ourHash));
    return panels.map((p, i) => panelOrError(panelUnderScope(p, state, panelServer), readSet, sourceSet, currentScope(), undefined, i, render.gate));
  }
  function renderGrid() {
    mount(gridBox, buildGrid());
  }

  /* The chrome is only meaningful when a composed panel can re-scope; a pure v1 read view skips it. */
  /* #6933: a scope change draws new-scope panels, so an in-place refresh still outstanding is for the old scope and must not land over them. */
  const onChange = () => {
    if (cancelViewRefresh) {
      cancelViewRefresh();
      cancelViewRefresh = null;
    }
    rememberScope(id, state);
    renderGrid();
  };
  const controls = hasComposed ? buildViewControls(fleet, variables, state, defaultHours, onChange) : null;

  if (liveGrid) {
    showAround(main, pageKey, liveGrid, [head, status, controls, scopeNote].filter(Boolean));
    const staged = buildGrid();
    const stagedRender = panelRender;
    const cancelSwap = refreshGridInPlace(liveGrid, staged);
    cancelViewRefresh = () => {
      cancelSwap();
      stagedRender.end();
    };
  } else {
    showPage(main, pageKey, [head, status, controls, scopeNote, gridBox]);
    renderGrid();
  }
  /* Clear any stale AbortSignal left by the previous page (e.g. server.js's per-render panelAbort) so it
     cannot bleed into SUBSEQUENT view renders — the onChange scope re-render and any poll-triggered
     renderView call. View panels carry no per-render abort controller, so undefined (unabortable) is the
     correct state after this grid is built (#4204). */
  setPanelSignal();
}

/* ─────────────────────────── notebook renderer (#1563 D7) ─────────────────────────── */

/*
 * Render a notebook definition as a vertical DOCUMENT: markdown cells through the air-gapped renderMarkdown, panel
 * cells through the SAME composed-panel card + validation the dashboard grid uses (panelOrError). It reuses this
 * page's server/time/variable scope bar (buildViewControls) exactly as a dashboard does, so flipping the scope
 * re-scopes every panel cell at once — EXCEPT a cell that overrides the window (a #2735 `range` pin or a
 * per-panel `hours` override), which wins over the scope bar's time range. The composed-panel card wears a
 * "Pinned" badge (compose.js pinBadge/effectivePin) whenever a cell overrides the window, so the exception is
 * visible and the scope bar can't silently lie for it; server scope still applies (#2788). Export/Delete/Edit
 * are shared with the dashboard renderer; Edit points at the notebook composer route.
 */
/**
 * `opts`:
 *   { mode: "saved", view, session, catalog, fleetRes, hold, pageKey }           — an ordinary saved notebook. `hold` is
 *     renderView's holdsPage answer: on a poll over the notebook already on screen (#7924) the document stays where it is and
 *     each cell is refreshed in place once its own new read returns, the way a dashboard's grid is; `pageKey` is the page key
 *     showPage/showAround remember it under.
 *   { mode: "alert", definition, alert, status, notes, canEdit, catalog, server, provenance, isLive, onOpenLive }
 *     — #4222's read-only alert-notebook render: an IN-MEMORY definition bound to one firing (no saved `view`
 *     row, no /api/fleet read — the server is fixed — and no scope bar, since there is nothing to re-scope).
 *     `server` is the link's server, the scope a panel cell falls back to when no alert row matched.
 *     `provenance` is the description string "Save as notebook" writes; `onOpenLive` is called (with no args)
 *     when the viewer clicks "Open live", and is this function's caller's job to turn into dropping the read
 *     cells' `as_of` and re-rendering — see triage.js. `isLive` (#4368) is an optional `() => boolean` the
 *     caller supplies to say whether its route is still the one on screen; each alert-mode read cell checks it
 *     right before mounting its settled result, so a result that lands after a route change is dropped instead
 *     of painted into a holder no longer attached to the visible document — the fetch and the limiter's
 *     release still run either way. Omitted (saved mode, or an alert-mode caller that doesn't pass it) means
 *     "always live", i.e. today's behaviour.
 * Saved-view behaviour (mode:"saved") is UNCHANGED by this refactor: same scope bar, same Export/Edit/Delete,
 * same panel-cell rendering. The alert mode is read-only: no scope bar (nothing to re-scope: read cells carry
 * their own `as_of`/`hours`, and a composed `panel` cell carries its own absolute `range`, which wins over the
 * scope's hours), no Export/Edit/Delete, and a header/status cell pair rendered directly from `alert`/`status`
 * rather than through a read. The other cell kinds render through saved mode's own renderCell (a markdown cell
 * as prose, a panel cell as a composed panel scoped to the alert's server); a kind this page cannot draw gets
 * a plain "not shown here" card, never nothing.
 *
 * Alert mode checks its read and panel cells against `opts.catalog`, the /api/catalog body the caller fetched
 * (triage.js passes it), exactly as saved mode does. A caller that passes none gets the empty catalog, so every
 * read and panel cell then fails its own catalog check with an error card instead of loading.
 */
export async function renderNotebookDoc(main, opts) {
  const isAlert = opts.mode === "alert";
  const def = isAlert ? (opts.definition || {}) : (opts.view.definition || {});
  const cells = Array.isArray(def.cells) ? def.cells : [];
  /* #6214: another person's personal notebook can be read but not changed, so Edit goes and Delete is disabled. */
  const access = isAlert ? null : viewAccess(opts.view, opts.session);
  const canEdit = isAlert ? !!opts.canEdit : !!opts.session.can_edit && !access.readOnly;
  /* Alert mode validates its cells against the catalog its caller passed, the same /api/catalog body saved mode
     uses: a read the catalog lists must render, not fail as "Unknown read" (it did while this was always the
     empty catalog). The empty catalog is only what an alert-mode caller that passes none gets. */
  const catalog = isAlert ? (opts.catalog || { reads: [], compose: {} }) : opts.catalog;
  const readSet = new Set((catalog.reads || []).map((r) => r.name));
  const sourceSet = new Set(((catalog.compose || {}).measures || []).map((m) => m.source));

  const status = el("div", { class: "view-status" });
  const head = isAlert
    ? el("div", { class: "page-head" }, [
        el("h2", { text: "Alert notebook" }),
        el("span", { class: "notebook-badge", text: "Notebook" }),
        el("div", { class: "spacer" }),
        canEdit ? saveAsNotebookButton(def, opts.provenance, status) : null,
        openLiveButton(opts.onOpenLive),
      ])
    : el("div", { class: "page-head" }, [
        backToViews(),
        el("h2", { text: opts.view.name || "Notebook" }),
        el("span", { class: "notebook-badge", text: "Notebook" }),
        opts.view.description ? el("div", { class: "meta", text: opts.view.description }) : null,
        personalNote(access),
        el("div", { class: "spacer" }),
        viewRefreshControl(opts.view, def, true, canEdit),
        exportButton(opts.view),
        canEdit ? el("a", { class: "btn small", href: storeHash("#/notebook/") + encodeURIComponent(opts.view.id) + "/edit", text: "Edit" }) : null,
        canEdit ? deleteButton(opts.view.id, status) : opts.session.can_edit ? lockedDeleteButton(access) : null,
      ]);

  if (!cells.length) {
    mount(main, [head, status, emptyStrip("This notebook has no cells yet.")]);
    return;
  }

  /* Alert mode has no scope bar: the server is fixed to the firing, read cells already carry as_of/hours, and a
     composed panel cell carries its own absolute `range`. A panel cell names no server, so it takes the one the
     endpoint sent: `opts.scopeServer`, the envelope's `scope_server`, which is the REGISTRY name (the endpoint
     resolves the matched alert row's server_id, else the link's server, to servers.server_name). Never a display
     name: the matched row's `server_name` and the link's own `server` are display names, and the compose runner
     filters on the registry name (`server_name = ANY(...)`), so a chart scoped by either drew "no data" under a
     firing alert on every server whose two names differ. `hours` is only the window a panel falls back to once
     "Open live" has dropped its `range` — while the cell carries one, the range wins over the scope's hours
     (compose.js buildRunBody). With no scope_server the scope names none, and each panel cell says so. Saved mode
     keeps its scope bar exactly as before. */
  let controls = null;
  let scopeNote = null;
  let refreshScopeNote = () => {};
  let cellUnderScope = (cell) => cell;
  let currentScope = () => ({ server: "All", hours: 24, variables: [], values: {} });
  if (isAlert) {
    const scopeServer = opts.scopeServer || "";
    currentScope = () => ({ server: scopeServer, hours: 24, variables: [], values: {} });
  }
  if (!isAlert) {
    const fleet = fleetOptions(opts.fleetRes);
    const variables = Array.isArray(def.variables) ? def.variables.filter((v) => v && v.name) : [];
    const defaultHours = def.range && typeof def.range.hours === "number" ? def.range.hours : 24;
    const hasPanels = cells.some((c) => c && c.type === "panel" && c.source != null);
    const panelServer = panelFilterServer(cells.filter((c) => c && c.type === "panel"), fleet);
    const state = seedState(opts.view.id, defaultHours, variables, fleet, panelServer);
    cellUnderScope = (cell) => (cell && cell.type === "panel" ? panelUnderScope(cell, state, panelServer) : cell);
    if (panelServer && hasPanels) {
      scopeNote = el("div", { class: "meta view-scope-note", text: panelServerNote(state, panelServer) });
      refreshScopeNote = () => { scopeNote.textContent = panelServerNote(state, panelServer); };
    }
    currentScope = () => ({
      server: state.server,
      hours: state.hours,
      variables: variables.map((v) => ({ name: v.name, dimension: v.dimension, default: v.default || undefined })),
      values: { ...state.values },
    });
    /* A scope change draws new-scope cells, so an in-place refresh still outstanding is for the old scope and must not land over them. */
    const onChange = () => {
      if (cancelViewRefresh) {
        cancelViewRefresh();
        cancelViewRefresh = null;
      }
      rememberScope(opts.view.id, state);
      renderDoc();
    };
    controls = hasPanels ? buildViewControls(fleet, variables, state, defaultHours, onChange) : null;
  }

  /* #7924: on a poll the document already on screen stays and is refreshed in place (refreshGridInPlace pairs its cells by index), so
     the scope controls built above keep drawing into it. */
  const liveDoc = !isAlert && opts.hold ? Array.from(main.children).find((n) => n.className === "notebook-doc") : null;
  const docBox = liveDoc || el("div", { class: "notebook-doc" });
  const limiter = isAlert ? new InFlightLimiter(3) : null;
  const ourHash = location.hash;
  let panelRender = null;
  function buildDoc() {
    if (isAlert) {
      /* Alert-mode cells load top to bottom, at most 3 in flight (#4222's cost rule) — renderCell/renderPanel
         return synchronously with the fetch outstanding, so the limiter gates ACQUIRING a slot before the cell
         node is even built, then releases it off renderPanel's onSettled callback. */
      const scope = currentScope();
      return cells.map((cell, i) => renderAlertCell(cell, i, readSet, sourceSet, scope, opts, limiter));
    }
    refreshScopeNote();
    /* #7257: a saved notebook starts its panel reads two at a time, and a new render (a scope change) ends the one before it. */
    if (panelRender) panelRender.end();
    const render = (panelRender = newPanelRender(ourHash));
    /* The cell's index is its slot: the identity under which a composed panel cell holds its zoom and drill (#7923), so the
       poll's rebuild of the cell draws them again. */
    return cells.map((cell, i) => renderCell(cellUnderScope(cell), readSet, sourceSet, currentScope(), undefined, render.gate, i));
  }
  function renderDoc() {
    mount(docBox, buildDoc());
  }

  if (liveDoc) {
    showAround(main, opts.pageKey, liveDoc, [head, status, controls, scopeNote].filter(Boolean));
    const staged = buildDoc();
    const stagedRender = panelRender;
    const cancelSwap = refreshGridInPlace(liveDoc, staged);
    cancelViewRefresh = () => {
      cancelSwap();
      stagedRender.end();
    };
    return;
  }
  if (isAlert) mount(main, [head, status, controls, scopeNote, docBox]);
  else showPage(main, opts.pageKey, [head, status, controls, scopeNote, docBox]);
  renderDoc();
}

/* A max-N in-flight gate (#4222 cost rule) for the alert notebook's client-side cell loads. A cell asks for a
   slot; if none is free it queues (FIFO) until release() frees one. Kept tiny and dependency-free — this is not
   a general scheduler, just the one "at most 3 outstanding reads" rule the cost design calls for. */
class InFlightLimiter {
  constructor(max) {
    this.max = max;
    this.active = 0;
    this.queue = [];
  }
  acquire() {
    if (this.active < this.max) {
      this.active++;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.queue.push(resolve));
  }
  release() {
    const next = this.queue.shift();
    if (next) {
      next();
    } else {
      this.active = Math.max(0, this.active - 1);
    }
  }
}

/* One alert-notebook cell -> a document block. header/status render directly from the endpoint's own facts (no
   read); a read cell queues on the limiter, then renders through the SAME v1 renderPanel dashboards use —
   explicit `viz` per cell, no derive.js guessing (#4222). A stale/empty firing (no alert, "Unknown" status, an
   empty read) renders honest empty sections, never an error page: header/status always render (their own null
   guards below), and renderPanel already degrades a read's own empty/error kinds to a strip, not a throw.
   markdown and panel cells (the templates' prose and charts) render through saved mode's own renderCell, so a
   cell looks and validates the same in an alert notebook and in a saved one; a kind with no renderer here gets
   a "not shown" card, never nothing. */
function renderAlertCell(cell, index, readSet, sourceSet, scope, opts, limiter) {
  if (!cell || typeof cell !== "object") return null;
  if (cell.type === "header") {
    return renderAlertHeaderCell(cell, opts);
  }
  if (cell.type === "status") {
    return el("div", { class: "notebook-panel panel card" }, [
      el("h3", {}, [cell.title || "Status"]),
      el("div", { class: "panel-body" }, [el("span", { class: "fv", text: localizeIsoText(cell.status || "Unknown") })]),
    ]);
  }
  if (cell.type === "read") {
    if (!cell.read || !readSet.has(cell.read) || !cell.viz || !VIZ[cell.viz]) {
      return panelErrorCard(cell.title, "Unknown read '" + (cell.read || "") + "' or visualization '" + (cell.viz || "") + "'.");
    }
    return gatedCell(opts, limiter, (release) => renderPanel(resolveReadTable(cell), release));
  }
  /* A markdown cell is prose: no load, so no slot — the same air-gapped renderMarkdown a saved notebook uses. */
  if (cell.type === "markdown") {
    return renderCell(cell, readSet, sourceSet, scope);
  }
  /* A composed panel cell (a chart): saved mode's own renderCell -> panelOrError -> renderComposedPanelCard path,
     scoped to the alert's server and windowed by the cell's own `range` (the scope's hours only apply once "Open
     live" has dropped it). It queues on the same limiter as a read cell — each one is a /api/compose/run — and
     panelOrError fires `release` itself for a cell that never starts a load, so a bad panel cannot hold a slot.
     Without a server there is nothing to scope the chart to, and running it unscoped would chart the whole
     fleet under an alert for one server, so the cell says so instead. The scope comes from the endpoint's
     `scope_server` only; a display name would pass the truthiness check and then match no rows. */
  if (cell.type === "panel") {
    if (!scope.server) {
      return notShownCard(cell.title, "This chart is not shown here: the alert link names no server to scope it to, or names one this service does not monitor.");
    }
    return gatedCell(opts, limiter, (release) => renderCell(cell, readSet, sourceSet, scope, release));
  }
  /* Any other kind has no renderer on this page. Say so: a cell that silently vanishes reads as evidence that
     does not exist. */
  return notShownCard(cell.title, "This notebook cell (type '" + String(cell.type) + "') is not shown here.");
}

/* A cell whose load is gated by the alert notebook's in-flight limiter. `build(release)` makes the cell's node
   once a slot is granted and must call `release` exactly once when its load ends (renderPanel and the composed
   card both take it as their onSettled callback).
   Cells load top to bottom: acquiring a slot is async, so a later cell's slot request cannot resolve before
   an earlier one queued first (the limiter's queue is FIFO) — build is only called once the slot is granted, at
   which point it starts the fetch and returns synchronously.
   #4368: `opts.isLive` — set by triage.js to a `location.hash === ourHash` check, the same route-change
   guard renderView already uses for its own await gap — is checked before mounting the settled result.
   A route change after the slot was granted still lets the fetch finish and the limiter release (so the
   NEXT page's own reads aren't starved by a request this page no longer owns); it only stops the result
   from painting into a holder that is no longer attached to the visible document. */
function gatedCell(opts, limiter, build) {
  const holder = el("div", { class: "notebook-panel" }, [loadingStrip()]);
  limiter.acquire().then(() => {
    /* A route change (a fresh #/triage link, or navigating off the page entirely) between queueing and the
       slot opening: this cell's holder is no longer attached to the visible document, and its own reads
       are for a firing nobody is looking at any more. The cell still runs and its own onSettled still
       releases the limiter's slot below — the fetch and release always happen, so a superseded batch never
       starves the page that replaced it — only the RESULT is dropped instead of painted into a dead node. */
    const stillLive = !opts.isLive || opts.isLive();
    const rendered = build(() => limiter.release());
    if (stillLive) mount(holder, rendered);
  });
  return holder;
}

/* A card that says a cell is not drawn on this page, and why — used where the cell cannot render here. The
   message goes through el()'s child-text path (never innerHTML), like every other string on this page. */
function notShownCard(title, message) {
  return el("div", { class: "notebook-panel panel card" }, [
    el("h3", {}, [title || "Cell"]),
    el("div", { class: "panel-body" }, [noticeStrip(message)]),
  ]);
}

/* The header cell: the endpoint's own alert facts, rendered directly (no read — they arrived with the envelope).
   A null `alert` (stale/no match) renders the honest empty state instead of blank fields. */
function renderAlertHeaderCell(cell, opts) {
  const a = opts.alert;
  const rows = [];
  if (a) {
    rows.push(["Server", a.server_name || "\u2014"]);
    rows.push(["Metric", a.metric_name || "\u2014"]);
    rows.push(["Fired", a.alert_time ? localTime(a.alert_time) : "\u2014"]);
    if (a.incident_since) rows.push(["Incident since", localTime(a.incident_since)]);
    if (a.involved_objects) rows.push(["Involved objects", a.involved_objects]);
    if (a.database) rows.push(["Database", a.database]);
    if (typeof a.total_occurrences === "number") rows.push(["Total occurrences", String(a.total_occurrences)]);
    /* current_value/threshold_value (#4368): same fmtNum(_, 1) triage.js's alertFields uses for the matched
       alert card, so the two pages format the same fields the same way. Omitted only when BOTH are absent —
       fmtNum already renders a lone missing side as "\u2014" rather than dropping the whole line. */
    if (typeof a.current_value === "number" || typeof a.threshold_value === "number") {
      rows.push(["Value vs threshold", "current " + fmtNum(a.current_value, 1) + " vs threshold " + fmtNum(a.threshold_value, 1)]);
    }
  }
  const body = a
    ? [
        /* #7918: a dismissed firing is still the firing the link names. The card says it was dismissed, so the reader
           knows the alert was acknowledged and why Alert History does not list it. */
        a.dismissed === true
          ? noticeStrip("This alert was dismissed. It still fired; dismissal only hides it from Alert History unless Show dismissed is on.")
          : null,
        el("div", { class: "detail-fields" }, rows.map(([k, v]) =>
          el("div", { class: "detail-field" }, [el("span", { class: "fk", text: k }), el("span", { class: "fv", text: v })])
        )),
        /* detail_text (#4368): the same <pre class="code"> block triage.js's alertFields uses — through el()'s
           text path (textContent), never innerHTML, since this is alert/query text the server only echoes. */
        a.detail_text ? el("pre", { class: "code", text: localizeAlertDetail(a.detail_text, a.newest_sample) }) : null,
      ]
    : emptyStrip("No matching alert history row for this link.");
  return el("div", { class: "notebook-panel panel card" }, [
    el("h3", {}, [cell.title || "Alert"]),
    el("div", { class: "panel-body" }, [body]),
  ]);
}

/* "Save as notebook" (#4222): POSTs the bound in-memory definition to /api/views, unmodified except for the
   description carrying the provenance string ("from alert template <id> v<version>, <metric> on <server> at
   <at>", built by the caller — triage.js — since it has the template/metric/server/at facts, not this module).
   Only mounted when the seat can edit (canEdit gate above), matching every other edit affordance on this page. */
function saveAsNotebookButton(definition, provenance, status) {
  const btn = el("button", { class: "btn small", type: "button", text: "Save as notebook" });
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    mount(status, el("div", { class: "strip loading", text: "Saving\u2026" }));
    const name = "Alert notebook " + new Date().toISOString();
    const res = await api.createView({ name, description: provenance || null, definition });
    if (res.kind === "data" && res.data && res.data.id != null) {
      location.hash = storeHash("#/notebook/") + encodeURIComponent(res.data.id);
      return;
    }
    btn.disabled = false;
    mount(status, errorStrip(res.message || "Could not save this notebook."));
  });
  return btn;
}

/* "Open live" (#4222): drops the absolute window pins (today: each read cell's `as_of`) so the notebook falls
   back to a relative/live view. `onOpenLive` is the caller's (triage.js's) re-render with as_of stripped — this
   module has no state of its own to mutate, since the definition is the caller's in-memory object. */
function openLiveButton(onOpenLive) {
  const btn = el("button", { class: "btn small", type: "button", text: "Open live" });
  btn.addEventListener("click", () => { if (onOpenLive) onOpenLive(); });
  return btn;
}

/* One notebook cell -> a document block: a markdown cell renders through renderMarkdown (XSS-safe); a panel cell
   through the shared panelOrError (unknown source/viz -> a clean strip, exactly as in the dashboard grid).
   `onSettled` is optional and only the alert notebook passes it (its in-flight limiter's release): see panelOrError. */
/* `slot` is the cell's index in a saved notebook: the stable identity a composed panel cell holds its zoom and drill under across
   the poll's rebuild (#7924). The alert notebook passes none, so its cells hold nothing, like the editor preview. */
function renderCell(cell, readSet, sourceSet, scope, onSettled, gate = null, slot = null) {
  if (!cell || typeof cell !== "object") return null;
  if (cell.type === "markdown") {
    return el("div", { class: "notebook-md markdown-body" }, [renderMarkdown(cell.text)]);
  }
  if (cell.type === "panel") {
    return el("div", { class: "notebook-panel" }, [panelOrError(cell, readSet, sourceSet, scope, onSettled, slot, gate)]);
  }
  /* A `read` cell (design D7, #4222) is a v1 read panel, FLAT like a panel cell: routed through the exact same
     panelOrError -> renderPanel path a dashboard read panel uses (its own fixed params, scope-independent).
     Minimal routing only; renderNotebookDoc's broader refactor is a later slice's job. */
  if (cell.type === "read") {
    return el("div", { class: "notebook-panel" }, [panelOrError(cell, readSet, sourceSet, scope, onSettled, slot, gate)]);
  }
  return null;
}

function backToViews() {
  return el("a", { href: storeHash("#/views"), text: "← Views" });
}

/* Fleet server options for the scope picker (value = stored server_name, label = display name). */
function fleetOptions(fleetRes) {
  if (fleetRes.kind !== "data" || !fleetRes.data) return [];
  return orderServers(fleetRes.data.cards || [])
    .map((c) => ({ value: c.server_name || c.display_name, label: c.display_name }))
    .filter((o) => o.value);
}

/* The view-scope control bar: a server scope picker (single / multi / All), a time-range select, and a value input
   per declared template variable (a server-typed variable is covered by the scope picker, so it is not repeated). */
function buildViewControls(fleet, variables, state, defaultHours, onChange) {
  const fields = [
    el("div", { class: "vc-field" }, [el("span", { class: "vc-label", text: "Servers" }), buildServerScopePicker(fleet, state, onChange)]),
    el("div", { class: "vc-field" }, [el("span", { class: "vc-label", text: "Time range" }), buildRangeSelect(defaultHours, state, onChange)]),
  ];
  for (const v of variables) {
    if (v.dimension === "server") continue;
    fields.push(el("div", { class: "vc-field" }, [el("span", { class: "vc-label", text: "$" + v.name }), varControl(v, state, onChange)]));
  }
  return el("div", { class: "view-controls" }, fields);
}

/* A template-variable value input. Bound to state.values on `change` (not each keystroke, so the grid re-runs on
   commit, not per character). Empty clears the binding (the panels referencing it then return no rows — the honest
   "pick a value" state, since the compose contract has no wildcard for an unbound $var). */
function varControl(v, state, onChange) {
  const inp = el("input", { class: "filter-box", type: "text", placeholder: "(pick a value)", "aria-label": v.name });
  inp.value = state.values[v.name] || "";
  inp.addEventListener("change", () => {
    const val = inp.value.trim();
    if (val) state.values[v.name] = val;
    else delete state.values[v.name];
    onChange();
  });
  return inp;
}

/* Mirror of the compose backend's window ceiling (ComposeLimits.MaxWindowHours = 90 days). The shared time range picker (#5562) greys
   out a longer length with the reason; the old Custom number input clamped it silently. */
const MAX_RANGE_HOURS = 24 * 90;

/* The view scope's time range: the shared picker in its rolling-only form, because the scope is "this many hours back from now" (the
   compose runner and the read cells take hours, not an end). It is bound to state.hours; the view's stored default and the current
   value show as the picker's label, so the 60s refresh shows exactly what is applied. */
function buildRangeSelect(defaultHours, state, onChange) {
  const picker = rollingHoursPicker({
    hours: state.hours || defaultHours,
    reachHours: MAX_RANGE_HOURS,
    label: "Time range",
    onChange: (hours) => {
      state.hours = hours;
      onChange();
    },
  });
  return el("span", { class: "range-select" }, [picker.node]);
}

/* The server scope picker: a <details> dropdown of checkboxes — "All servers (fleet)" plus each fleet server.
   state.server is "All" (or an empty selection ⇒ fleet) or an array of names; checking a server switches off All,
   and clearing every server falls back to All. <details> gives open/close + keyboard for free (no outside-click JS). */
function buildServerScopePicker(fleet, state, onChange) {
  const summary = el("summary", { class: "scope-summary" });
  const list = el("div", { class: "scope-list" });

  const isAll = () => state.server === "All" || (Array.isArray(state.server) && state.server.length === 0);
  const names = () => (Array.isArray(state.server) ? state.server : []);
  const labelFor = (val) => {
    const o = fleet.find((x) => x.value === val);
    return o ? o.label : val;
  };

  function toggle(val, on) {
    let arr = isAll() ? [] : [...names()];
    if (on) {
      if (!arr.includes(val)) arr.push(val);
    } else {
      arr = arr.filter((x) => x !== val);
    }
    state.server = arr.length ? arr : "All";
  }

  function refresh() {
    const n = names();
    summary.textContent = isAll() ? "All servers (fleet)" : n.length === 1 ? labelFor(n[0]) : n.length + " servers";
  }

  function redraw() {
    const rows = [
      checkRow("All servers (fleet)", isAll(), () => {
        state.server = "All";
        state.picked = true;
        redraw();
        onChange();
      }),
    ];
    for (const o of fleet) {
      const on = !isAll() && names().includes(o.value);
      rows.push(
        checkRow(o.label, on, () => {
          toggle(o.value, !on);
          state.picked = true;
          redraw();
          onChange();
        })
      );
    }
    mount(list, rows);
    refresh();
  }

  redraw();
  return el("details", { class: "scope-picker" }, [summary, list]);
}

function checkRow(label, checked, onToggle) {
  const box = el("input", { type: "checkbox", class: "editor-check" });
  box.checked = checked;
  box.addEventListener("change", onToggle);
  return el("label", { class: "scope-item" }, [box, el("span", { text: label })]);
}

/* Pre-check a panel against the catalog so an unknown read/source/viz is a clean strip, not an opaque 404 / crash.
   A composed panel (names a `source`) runs through /api/compose/run with the view scope; a read panel through the
   v1 renderPanel (its own fixed params, scope-independent).
   `onSettled` (optional; the alert notebook's in-flight limiter release, #4222) is called exactly once per call: by
   the renderer when its load ends, or right here when the panel cannot start a load and becomes an error card —
   so a bad cell cannot hold a limiter slot. Every other caller omits it. */
function panelOrError(p, readSet, sourceSet, scope, onSettled, slot = null, gate = null) {
  const bad = panelProblem(p, readSet, sourceSet);
  if (bad) {
    if (onSettled) onSettled();
    return panelErrorCard(bad.title, bad.message);
  }
  if (p.source != null) {
    return renderComposedPanelCard(p, scope, onSettled, slot, gate);
  }
  return renderPanel(p, onSettled, gate);
}

/* Why a panel cannot start a read (the catalog check), as {title, message}, or null when it can. A panel with a problem
   draws an error card and takes no limiter slot, and no Retry button, since running it again cannot fix it (#7257). */
function panelProblem(p, readSet, sourceSet) {
  if (!p || typeof p !== "object") {
    return { title: "Invalid panel", message: "This panel is not an object." };
  }
  if (p.path != null) {
    return { title: p.title, message: "This panel uses raw 'path' mode, which is not supported." };
  }
  if (p.source != null) {
    if (!sourceSet.has(p.source)) {
      return { title: p.title, message: "Unknown source '" + p.source + "'. It may have been renamed or removed." };
    }
    if (!p.viz) {
      return { title: p.title, message: "This composed panel has no chart type." };
    }
    return null;
  }
  if (!p.read || !readSet.has(p.read)) {
    return { title: p.title, message: "Unknown read '" + (p.read || "") + "'. It may have been renamed or removed." };
  }
  if (!p.viz || !VIZ[p.viz]) {
    return { title: p.title, message: "Unknown visualization '" + (p.viz || "") + "'." };
  }
  return null;
}

/* #7257: the most long reads one saved view starts at once. The service default for reads.maxHeavyReads is 4 (#7357) and the
   page cannot read the service's limit. The page stays at 2 so one saved view leaves slots for other readers. */
const SAVED_VIEW_READS = 2;

/* One render of a saved dashboard or notebook: its own limiter (the alert notebook's InFlightLimiter) and a way to end it.
   A render ends when a newer one replaces it (a scope change, an in-place refresh that was cancelled) or the route changes.
   A panel of an ended render that still waits for a slot never starts its read (#6933), and gives the slot straight back. */
function newPanelRender(ourHash) {
  const limiter = new InFlightLimiter(SAVED_VIEW_READS);
  const render = {
    ended: false,
    end() { render.ended = true; },
    /* `start` runs the panel's read and returns its promise. The slot is held until that settles, busy retry wait included.
       A retry is the viewer's own click on a panel that drew, so it runs even after a poll's refresh has ended the render. */
    gate(start, retry = false) {
      return limiter.acquire().then(async () => {
        if (!retry && (render.ended || location.hash !== ourHash)) {
          limiter.release();
          return;
        }
        try {
          await start();
        } finally {
          limiter.release();
        }
      });
    },
  };
  return render;
}

function panelErrorCard(title, message) {
  return el("div", { class: "panel card" }, [
    el("h3", {}, [title || "Panel"]),
    el("div", { class: "panel-body" }, [errorStrip(message)]),
  ]);
}

/* Export = a runtime Blob download of {name, description?, definition} (re-importable). Available to every seat;
   the Blob URL is created at runtime (never a source literal), so the air-gap scan stays green. */
function exportButton(view) {
  const btn = el("button", { class: "btn small", type: "button", text: "Export" });
  btn.addEventListener("click", () => {
    const payload = { name: view.name, description: view.description || null, definition: view.definition || {} };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = el("a", { href: url, download: safeFileName(view.name) + ".json" });
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  });
  return btn;
}

function safeFileName(name) {
  const cleaned = String(name || "view").replace(/[^A-Za-z0-9._-]+/g, "_").replace(/^_+|_+$/g, "");
  return cleaned || "view";
}

/* #6214: "Personal view of X" for a personal view (nothing for a shared one). */
function personalNote(access) {
  return access && access.personal ? el("div", { class: "meta view-personal-note", text: access.note }) : null;
}

/* The Delete the store would refuse (another person's personal view): shown, but disabled, with the reason. */
function lockedDeleteButton(access) {
  const btn = el("button", { class: "btn small danger", type: "button", text: "Delete", title: access.note + ": only its owner can delete it." });
  btn.disabled = true;
  return btn;
}

function deleteButton(id, status) {
  const btn = el("button", { class: "btn small danger", type: "button", text: "Delete" });
  btn.addEventListener("click", async () => {
    if (!window.confirm("Delete this view? This cannot be undone.")) {
      return;
    }
    btn.disabled = true;
    mount(status, el("div", { class: "strip loading", text: "Deleting…" }));
    const res = await api.deleteView(id);
    if (res.kind === "data") {
      location.hash = storeHash("#/views");
      return;
    }
    btn.disabled = false;
    mount(status, errorStrip(res.message || "Could not delete the view."));
  });
  return btn;
}
