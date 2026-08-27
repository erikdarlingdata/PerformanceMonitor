/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Darling Web entry point (#1562, extended for #1563): the hash router, the sidebar server + view lists, and the
 * refresh loop. Routes:
 *   #/fleet             — Fleet Overview (default)
 *   #/ag                — Availability Group topology (#991; nav entry revealed only when the store has AG data)
 *   #/server/{name}     — one server's detail (Overview)
 *   #/server/{name}/{tab} — one server's detail, opened on a named sub-tab (pages/server-tabs.js)
 *   #/alerts            — fleet-wide Alert History
 *   #/views             — Custom Views list (#1563)
 *   #/view/{id}         — a saved custom view, rendered (#1563)
 *   #/view/{id}/edit    — the composer editing a saved view (#1563)
 *   #/view/new          — the composer creating a new view (#1563)
 *   #/notebook/{id}     — a saved notebook, rendered as a document (#1563 D7; renderView kind-detects)
 *   #/notebook/{id}/edit— the notebook composer editing a saved notebook (#1563 D7)
 *   #/notebook/new      — the notebook composer creating a new notebook (optionally /new/{template})
 * The refresh loop re-renders the active page every 60s and PAUSES while the tab is hidden (the interval skips
 * work when document.hidden), refreshing once immediately when the tab becomes visible again. The 60s refresh
 * DELIBERATELY does NOT re-render either composer route (the editor-route poll guard) — a background rebuild there
 * would discard an in-progress edit — while the sidebar (server list + view list) still refreshes.
 */

import { el, mount, apiGet, bandClass, localTime } from "./util.js";
import { navigateServer } from "./panels.js";
import { renderFleet } from "./pages/fleet.js";
import { renderAg } from "./pages/ag.js";
import { renderServer } from "./pages/server.js";
import { renderAlerts } from "./pages/alerts.js";
import { renderViewList, renderView } from "./pages/views.js";
import { renderEditor } from "./editor.js";
import { renderNotebookEditor } from "./notebook.js";
import { getSession, listViews } from "./views-api.js";

const POLL_MS = 60000;

const main = document.getElementById("main");
const serverList = document.getElementById("server-list");
const viewList = document.getElementById("view-list");
const statusbar = document.getElementById("statusbar");

/* ─────────────────────────── routing ─────────────────────────── */

function currentRoute() {
  const h = location.hash || "#/fleet";
  if (h.startsWith("#/server/")) return serverRoute(h.slice("#/server/".length));
  if (h === "#/ag" || h === "#/ag/") return { name: "ag" };
  if (h.startsWith("#/alerts")) return { name: "alerts" };
  /* #/views (list) is checked before the #/view/ forms; and the /edit form is tested before the bare /view/. */
  if (h === "#/views" || h === "#/views/") return { name: "views" };
  if (h === "#/view/new") return { name: "editor", id: "new" };
  if (h.startsWith("#/view/") && h.endsWith("/edit")) {
    return { name: "editor", id: decodeURIComponent(h.slice("#/view/".length, h.length - "/edit".length)) };
  }
  if (h.startsWith("#/view/")) return { name: "view", id: decodeURIComponent(h.slice("#/view/".length)) };
  /* Notebook routes (#1563 D7): /new (optionally /new/{template}) + /{id} + /{id}/edit, tested most-specific first. */
  if (h === "#/notebook/new") return { name: "notebookEditor", id: "new" };
  if (h.startsWith("#/notebook/new/")) {
    return { name: "notebookEditor", id: "new", template: decodeURIComponent(h.slice("#/notebook/new/".length)) };
  }
  if (h.startsWith("#/notebook/") && h.endsWith("/edit")) {
    return { name: "notebookEditor", id: decodeURIComponent(h.slice("#/notebook/".length, h.length - "/edit".length)) };
  }
  if (h.startsWith("#/notebook/")) return { name: "notebook", id: decodeURIComponent(h.slice("#/notebook/".length)) };
  return { name: "fleet" };
}

/* #/server/{name} and #/server/{name}/{tab} — the sub-tab id rides in the hash so a tab is deep-linkable and
   survives the 60s refresh (which re-renders the whole route). The name is always encodeURIComponent'd by
   navigateServer, so a '/' inside a server name arrives as %2F and this split is unambiguous; a hand-typed hash
   carrying a RAW '/' in the name was already ambiguous before the tab segment existed. An unknown tab id is not
   an error — findServerTab falls back to Overview — so every pre-existing #/server/{name} link still works. */
function serverRoute(rest) {
  const slash = rest.indexOf("/");
  if (slash < 0) return { name: "server", param: decodeURIComponent(rest) };
  return {
    name: "server",
    param: decodeURIComponent(rest.slice(0, slash)),
    tab: decodeURIComponent(rest.slice(slash + 1)),
  };
}

function route() {
  const r = currentRoute();
  setActiveNav(r);
  if (r.name === "server") renderServer(main, r.param, r.tab);
  else if (r.name === "ag") renderAg(main);
  else if (r.name === "alerts") renderAlerts(main);
  else if (r.name === "views") renderViewList(main);
  else if (r.name === "view") renderView(main, r.id);
  else if (r.name === "editor") renderEditor(main, r.id);
  else if (r.name === "notebook") renderView(main, r.id); // renderView kind-detects -> notebook document
  else if (r.name === "notebookEditor") renderNotebookEditor(main, r.id, r.template);
  else renderFleet(main);
}

/* Whether a route targets a specific saved view/notebook (its renderer or composer) — the routes whose sidebar
   entry should light up, and which the poll guard must not clobber (for the composer forms). */
function isViewItemRoute(name) {
  return name === "view" || name === "editor" || name === "notebook" || name === "notebookEditor";
}

/* The sidebar's "Custom Views" nav stays lit across the list, both renderers, and both composers. */
function navKeyFor(r) {
  return r.name === "views" || isViewItemRoute(r.name) ? "views" : r.name;
}

function setActiveNav(r) {
  const navKey = navKeyFor(r);
  document.querySelectorAll(".nav a").forEach((a) => a.classList.toggle("active", a.dataset.route === navKey));
  updateServerActive(r);
  updateViewActive(r);
}

function updateServerActive(r) {
  serverList.querySelectorAll(".server-item").forEach((item) => {
    const active = r.name === "server" && (item.dataset.server === r.param || item.dataset.display === r.param);
    item.classList.toggle("active", active);
  });
}

function updateViewActive(r) {
  if (!viewList) return;
  viewList.querySelectorAll(".view-item").forEach((item) => {
    const active = isViewItemRoute(r.name) && item.dataset.view === String(r.id);
    item.classList.toggle("active", active);
  });
}

/* ─────────────────────────── sidebar ─────────────────────────── */

async function refreshSidebar() {
  const res = await apiGet("/api/fleet");
  if (res.kind !== "data") {
    mount(serverList, el("div", { class: "muted", style: "padding:0.5rem 1.25rem", text: res.kind === "error" ? "Fleet unavailable" : "" }));
    updateStatusBar(null);
    return;
  }

  const cards = [...(res.data.cards || [])].sort((a, b) => a.display_name.localeCompare(b.display_name));
  const r = currentRoute();
  mount(
    serverList,
    cards.map((c) => {
      const target = c.server_name || c.display_name;
      const active = r.name === "server" && (r.param === c.server_name || r.param === c.display_name);
      return el(
        "div",
        {
          class: "server-item" + (active ? " active" : ""),
          dataset: { server: target, display: c.display_name },
          onActivate: () => navigateServer(target),
        },
        [el("span", { class: "dot " + bandClass(c.band) }), el("span", { class: "name", text: c.display_name })]
      );
    })
  );
  updateStatusBar(res.data);
}

/* ─────────────────────────── availability-groups nav gate (#991) ─────────────────────────── */

/* Always On is opt-in and most fleets have none, so the Availability Groups entry stays hidden until the store
   actually has AG data — a permanent entry that only ever says "nothing here" is noise. The probe converges the
   way ComposeStoreAvailability's does: once AGs are seen the answer is cached for the session and the probe stops;
   while none are seen the 60s poll re-probes, so standing up an AG reveals the entry without a reload. The #/ag
   route itself is never gated — a deep link renders the page (with its own empty state) either way. */
let agNavRevealed = false;

async function refreshAgNav() {
  if (agNavRevealed) return;
  const link = document.querySelector('.nav a[data-route="ag"]');
  if (!link) return;

  const res = await apiGet("/api/ag");
  if (res.kind !== "data" || !res.data || !res.data.availability_group_count) return;

  agNavRevealed = true;
  link.hidden = false;
}

/* ─────────────────────────── sidebar view list (#1563) ─────────────────────────── */

/* Populated like refreshSidebar: the saved custom views + a "New view" affordance shown ONLY when the session
   reports can_edit. Kept fresh on the 60s poll even while the composer is open. */
async function refreshViewList() {
  if (!viewList) return;
  const [session, res] = await Promise.all([getSession(), listViews()]);
  const r = currentRoute();
  const items = [];

  if (res.kind === "data" && Array.isArray(res.data)) {
    for (const v of res.data) {
      const active = isViewItemRoute(r.name) && String(r.id) === String(v.id);
      /* The summary carries the view kind (definition->>'kind'), so the sidebar links + badges a notebook without
         fetching its definition; a notebook links to its own route and gets a document glyph (CSS). */
      const isNotebook = v.kind === "notebook";
      items.push(
        el("a", {
          class: "view-item" + (active ? " active" : "") + (isNotebook ? " is-notebook" : ""),
          href: (isNotebook ? "#/notebook/" : "#/view/") + encodeURIComponent(v.id),
          dataset: { view: String(v.id) },
          title: v.description || v.name,
          text: v.name,
        })
      );
    }
  }

  if (session.can_edit) {
    items.push(el("a", { class: "view-item new-view", href: "#/view/new", text: "＋ New view" }));
    items.push(el("a", { class: "view-item new-view", href: "#/notebook/new", text: "＋ New notebook" }));
  }

  if (!items.length) {
    items.push(el("div", { class: "muted", style: "padding:0.35rem 1.25rem", text: "No views yet" }));
  }

  mount(viewList, items);
}

/* ─────────────────────────── status bar ─────────────────────────── */

/* A fixed footer mirroring the WPF viewer's status bar: fleet server count, collectors healthy/failing across
   the fleet, and the last refresh time. Built from the SAME /api/fleet response the sidebar just read (no extra
   round-trip). Store size has no web endpoint, so it is deliberately omitted here. */
function updateStatusBar(d) {
  if (!statusbar) return;
  if (!d) {
    mount(statusbar, el("span", { class: "sb-item muted", text: "Fleet unavailable" }));
    return;
  }
  let healthy = 0;
  let failing = 0;
  for (const c of d.cards || []) {
    healthy += c.healthy_collector_count || 0;
    failing += c.failed_collector_count || 0;
  }
  const servers = d.total_servers || 0;
  mount(statusbar, [
    el("span", { class: "sb-item", text: servers + (servers === 1 ? " server" : " servers") }),
    el("span", { class: "sb-sep", text: "·" }),
    el("span", { class: "sb-item", text: healthy + " collectors healthy · " + failing + " failing" }),
    el("span", { class: "sb-sep", text: "·" }),
    el("span", { class: "sb-item", text: "Updated " + localTime(d.generated_at) }),
  ]);
}

/* ─────────────────────────── refresh loop ─────────────────────────── */

function refresh() {
  refreshSidebar();
  refreshViewList();
  refreshAgNav();
  /* Poll-clobber guard (#1563): never re-render a composer (dashboard OR notebook, #1563 D7) from the background
     poll — a rebuild would discard an in-progress edit. hashchange still routes to it normally; only this periodic
     refresh skips it. */
  const routeName = currentRoute().name;
  if (routeName === "editor" || routeName === "notebookEditor") return;
  route();
}

function start() {
  window.addEventListener("hashchange", route);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refresh();
  });
  setInterval(() => {
    if (!document.hidden) refresh();
  }, POLL_MS);

  refreshSidebar();
  refreshViewList();
  refreshAgNav();
  route();
}

start();
