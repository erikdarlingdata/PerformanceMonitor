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
 *   #/sweeps            — Fleet Sweep reports: the sweep-with-memory timeline (#3466)
 *   #/server/{name}     — one server's detail (Overview)
 *   #/server/{name}/{tab} — one server's detail, opened on a named sub-tab (pages/server-tabs.js)
 *   #/alerts            — fleet-wide Alert History
 *   #/investigations    — agent investigation requests, their reports and journal (#5904)
 *   #/alert-rules       — Custom Alert Rules list (#3285)
 *   #/alert-rule/new    — the alert-rule editor creating a new rule (optionally /new/{template})
 *   #/alert-rule/{id}   — the alert-rule editor editing a saved rule (#3285)
 *   #/views             — Custom Views list (#1563)
 *   #/view/{id}         — a saved custom view, rendered (#1563)
 *   #/view/{id}/edit    — the composer editing a saved view (#1563)
 *   #/view/new          — the composer creating a new view (#1563)
 *   #/notebook/{id}     — a saved notebook, rendered as a document (#1563 D7; renderView kind-detects)
 *   #/notebook/{id}/edit— the notebook composer editing a saved notebook (#1563 D7)
 *   #/notebook/new      — the notebook composer creating a new notebook (optionally /new/{template})
 * The refresh loop refreshes the shell (sidebar, view list, AG nav) every 60s and re-renders the active page on
 * that page's own interval (60s for the built-in pages; a saved view or notebook carries its own `refresh`
 * setting), backing off when a page renders slowly (refresh-policy.js). It PAUSES while the tab is hidden,
 * refreshing once immediately when the tab becomes visible again (the page only if it is due). It also pauses
 * on the operator's own say-so — the shell's "Pause all auto-refresh" master control (#4222d), persisted in
 * localStorage, which resuming clears by running one refresh immediately. The refresh DELIBERATELY does NOT re-render
 * either composer route or #/triage (the poll guard in refresh()) — a background rebuild would discard an
 * in-progress edit, or waste a read against a page opened to read once — while the sidebar (server list + view
 * list) still refreshes.
 */

import { el, mount, apiGet, apiGetFleet, readTool, localTime, hasInFlightReads, beginPageScope, readErrorCount, isSessionExpired, onSessionExpired, frontDoor, currentStore, routeHash, splitStoreHash, storeHash, storeHeaders, loadingStrip } from "./util.js";
import { initFrontDoor, refreshPicker, frontDoorStores, isKnownStore, locateServer, ambiguousStrip, locateFailureStrip } from "./store-picker.js";
import { makeScrollReset } from "./scroll-reset.js";
import { renderFleet } from "./pages/fleet.js";
import { renderAg } from "./pages/ag.js";
import { renderSweeps } from "./pages/sweeps.js";
import { renderAdmin } from "./pages/admin.js";
import { renderServer } from "./pages/server.js";
import { renderFinops } from "./pages/finops.js";
import { renderAlerts } from "./pages/alerts.js";
import { renderInvestigations } from "./pages/investigations.js";
import { renderAlertRuleList } from "./pages/alert-rules.js";
import { renderViewList, renderView, currentViewRefresh, onViewRefreshChange, clearViewRefresh } from "./pages/views.js";
import { REFRESH_CHOICES, PAGE_REFRESH_CHOICES, nextRefreshDelayMs, isBackedOff, defaultRefreshChoice, refreshLabel, loadPageRefreshChoice, savePageRefreshChoice, PAGE_REFRESH_KEY } from "./refresh-policy.js";
import { buildPageRefreshControl } from "./refresh-control.js";
import { renderTriage } from "./pages/triage.js";
import { renderEditor } from "./editor.js";
import { renderNotebookEditor } from "./notebook.js";
import { renderAlertEditor } from "./alert-editor.js";
import { getSession, listViews, onViewsChanged } from "./views-api.js";
import { paintViewList } from "./view-sidebar.js";
import { renderMuteRules } from "./pages/mute-rules.js";
import { renderKnownPatterns } from "./pages/known-patterns.js";
import { renderManageTags } from "./pages/manage-tags.js";
import { renderManageQueryTags } from "./pages/manage-query-tags.js";
import { renderQueryTagUsage } from "./pages/query-tag-usage.js";
import { renderJobHistory } from "./pages/job-history.js";
import { refreshAttention, clearViewGroups, onChange as onLocalChange } from "./viewer-local.js";
import { initSidebarCollapse, initSeverityColorSettings } from "./viewer-local-ui.js";
import { initSidebarSearch, paintServerList, paintStoreServerLists } from "./sidebar.js";
import { initNarrowNav } from "./narrow-nav.js";

/* The shell (sidebar, view list, AG nav) refreshes every POLL_MS; the page re-renders on its own interval. */
const POLL_MS = 60000;
/* How often the scheduler looks at the clock. It does no reads itself. */
const SCHEDULER_TICK_MS = 1000;

const main = document.getElementById("main");
const serverList = document.getElementById("server-list");
const viewList = document.getElementById("view-list");
const statusbar = document.getElementById("statusbar");

/* ─────────────────────────── routing ─────────────────────────── */

/* RULE for every route this switch returns: a route must never write, export or confirm anything on load. A link
   from another site can open the dashboard with a live session behind it (#6286), so a page reached by an address
   may only read and draw; a change happens after a click on a button inside the page, never from the address. */
function currentRoute() {
  /* #6840: a front door's #/s/{store}/ prefix is not part of the route; the store it names is applied in route(). */
  const h = routeHash() || "#/fleet";
  if (h.startsWith("#/server/")) return serverRoute(h.slice("#/server/".length));
  if (h === "#/finops" || h.startsWith("#/finops/")) return finopsRoute(h.slice("#/finops".length).replace(/^\//, ""));
  if (h === "#/ag" || h === "#/ag/") return { name: "ag" };
  if (h === "#/sweeps" || h === "#/sweeps/") return { name: "sweeps" };
  /* #/triage?server=...&metric=...&at=...&dedup=... (#2710) — the deep-link every alert webhook carries.
     The query rides INSIDE the hash (a static SPA route), so it is split off here and parsed by the page.
     Checked before #/alerts only for symmetry; the two prefixes cannot collide. */
  if (h.startsWith("#/triage")) {
    const q = h.indexOf("?");
    return { name: "triage", query: q >= 0 ? h.slice(q + 1) : "" };
  }
  if (h.startsWith("#/alerts")) return { name: "alerts" };
  if (h === "#/investigations" || h === "#/investigations/") return { name: "investigations" };
  /* Alert-rule routes (#3285): the list, the new form (optionally /new/{template}), then the bare /{id} edit form,
     tested most-specific first. Distinct from #/alerts above (which cannot collide: #/alert- never starts #/alerts). */
  if (h === "#/alert-rules" || h === "#/alert-rules/") return { name: "alertRules" };
  if (h === "#/alert-rule/new") return { name: "alertEditor", id: "new" };
  if (h.startsWith("#/alert-rule/new/")) {
    return { name: "alertEditor", id: "new", template: decodeURIComponent(h.slice("#/alert-rule/new/".length)) };
  }
  if (h.startsWith("#/alert-rule/")) return { name: "alertEditor", id: decodeURIComponent(h.slice("#/alert-rule/".length)) };
  /* #/admin and #/admin/{tab}: the read-only Admin page; an unknown tab falls back inside the page. */
  if (h === "#/admin" || h.startsWith("#/admin/")) return { name: "admin", tab: safeDecode(h.slice("#/admin".length).replace(/^\//, "")) };
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
  /* #/mute-rules[?server_name=..&metric_name=..] — the query pre-fills the create form (Alert History actions). */
  if (h === "#/mute-rules" || h.startsWith("#/mute-rules?")) {
    const q = h.indexOf("?");
    return { name: "muteRules", query: q >= 0 ? h.slice(q + 1) : "" };
  }
  if (h === "#/known-patterns") return { name: "knownPatterns" };
  if (h === "#/manage-tags") return { name: "manageTags" };
  if (h === "#/manage-query-tags") return { name: "manageQueryTags" };
  if (h === "#/query-tag-usage") return { name: "queryTagUsage" };
  if (h === "#/job-history" || h === "#/job-history/") return { name: "jobHistory" };
  return { name: "fleet" };
}

/* ─────────────────────────── front door: the store in the address (#6840) ─────────────────────────── */

/* Server name -> the store /api/front-door/locate said holds it, kept for the page's life so an in-app link written without a
   store does not ask again. */
const locatedStore = new Map();

/* The name a bare route points at when it is a server's page: #/server/{name}[/tab] and #/finops/{server}[/tab]. */
function routeServerName(r) {
  if (r.name === "server" && r.param) return r.param;
  if (r.name === "finops" && r.param) return r.param;
  return null;
}

/**
 * Applies the store the address names, before the route draws. Returns true when route() must stop here (a reload, a lookup
 * or a message is in charge of the page now) and false when the route can draw.
 *   #/s/{store}/...  the store is made current: the one in force already draws; another one reloads the page (the shell's
 *                    view list, alert counts and the rest belong to the store that was left); an unknown name says so.
 *   a bare route     keeps the current store, and the address is rewritten to carry it so a link or bookmark made from it
 *                    opens in the same store. A server's page asks the front door which store has the server (a person
 *                    can paste a link with no store); a name more than one store has lets the person pick.
 */
function gateStore(opts) {
  const split = splitStoreHash(location.hash || "");
  if (split.store) {
    if (split.store === currentStore()) return false;
    if (isKnownStore(split.store)) {
      localStorage.setItem("darling.frontDoor.store", split.store);
      location.reload();
      return true;
    }
    mount(main, el("div", { class: "strip error", role: "alert", text: "This front door has no store named " + split.store + "." }));
    return true;
  }

  const r = currentRoute();
  const name = routeServerName(r);
  const bare = location.hash || "#/fleet";
  if (!name) {
    history.replaceState(null, "", storeHash(bare));
    return false;
  }
  const known = locatedStore.get(name);
  if (known) {
    applyLocatedStore(known, bare, opts);
    return true;
  }
  mount(main, loadingStrip("Finding " + name + "…"));
  locateServer(name).then((answer) => {
    if ((location.hash || "#/fleet") !== bare) return; /* the person went elsewhere while the front door answered */
    if (answer.kind === "found") {
      locatedStore.set(name, answer.store);
      applyLocatedStore(answer.store, bare, opts);
    } else if (answer.kind === "ambiguous") {
      /* The store the page is working in wins when it is one of the candidates: that is a link from inside this store. */
      if (answer.stores.includes(currentStore())) applyLocatedStore(currentStore(), bare, opts);
      else mount(main, ambiguousStrip(answer, bare));
    } else {
      mount(main, locateFailureStrip(answer));
    }
  });
  return true;
}

function applyLocatedStore(store, bare, opts) {
  history.replaceState(null, "", storeHash(bare, store));
  if (store === currentStore()) {
    route(opts);
    return;
  }
  localStorage.setItem("darling.frontDoor.store", store);
  location.reload();
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

/* #/finops, #/finops/{server} and #/finops/{server}/{tab}: the server and the sub-tab id ride in the hash, as on
   the server page. The server name is encodeURIComponent'd, so a '/' inside it arrives as %2F. */
function safeDecode(s) {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

function finopsRoute(rest) {
  if (!rest) return { name: "finops" };
  const slash = rest.indexOf("/");
  if (slash < 0) return { name: "finops", param: safeDecode(rest) };
  return {
    name: "finops",
    param: safeDecode(rest.slice(0, slash)),
    tab: safeDecode(rest.slice(slash + 1)),
  };
}

/* #6159: another page, server or tab opens at the top; the poll re-render of the same route keeps the scroll position. */
const resetScrollOnRouteChange = makeScrollReset(() => window.scrollTo(0, 0));

/**
 * @param {object} [opts] — forwarded to renderServer; `{ poll: true }` marks this call as the 60s poll's own
 * refresh rather than a hashchange (sub-tab click, deep link) or the first paint (#4190/#4191). Also forwarded
 * to renderSweeps (#4214 round-1 review), which threads it into the store host card so a poll tick replays
 * that card's last-fetched payload instead of re-fetching (the profile it reports changes rarely — a hardware
 * or version change, never per-tick). Every other renderX() call below still ignores it.
 */
function route(opts) {
  /* The session-expired takeover owns the DOM from the moment it fires until the operator signs in again —
     see showSignedOutState/onSessionExpired below (#4187). hashchange keeps calling this (a stray click, the
     back button), and re-dispatching to a page here would just start a fresh round of reads that fail the
     same way; short-circuiting is simpler than unwiring every listener that can reach route(). */
  if (isSessionExpired()) return;

  /* #6793: going to another page (or a sub-tab, a deep link, the first paint) aborts the reads of the page being left
     and starts a fresh read count, so a read left behind cannot hold the new page's poll, Refresh button, tab-return
     refresh or render-time back-off. The poll and the Refresh button re-render the SAME page (`poll: true`): that page
     aborts its own older load, as before, and the count carries on. */
  if (!(opts && opts.poll === true)) beginPageScope();

  if (frontDoor() && gateStore(opts)) return;

  const r = currentRoute();
  resetScrollOnRouteChange(r, !!(opts && opts.poll === true));
  markPageRenderStart(r.name, !!(opts && opts.poll === true));
  setActiveNav(r);
  if (r.name === "server") renderServer(main, r.param, r.tab, opts);
  else if (r.name === "finops") renderFinops(main, r.param, r.tab, opts);
  else if (r.name === "ag") renderAg(main);
  else if (r.name === "sweeps") renderSweeps(main, opts);
  else if (r.name === "alerts") renderAlerts(main);
  else if (r.name === "investigations") renderInvestigations(main);
  else if (r.name === "alertRules") renderAlertRuleList(main);
  else if (r.name === "alertEditor") renderAlertEditor(main, r.id, r.template);
  else if (r.name === "admin") renderAdmin(main, r.tab);
  else if (r.name === "triage") renderTriage(main, r.query);
  else if (r.name === "views") renderViewList(main);
  else if (r.name === "view") renderView(main, r.id);
  else if (r.name === "editor") renderEditor(main, r.id);
  else if (r.name === "notebook") renderView(main, r.id); // renderView kind-detects -> notebook document
  else if (r.name === "notebookEditor") renderNotebookEditor(main, r.id, r.template);
  else if (r.name === "muteRules") renderMuteRules(main, r.query);
  else if (r.name === "knownPatterns") renderKnownPatterns(main);
  else if (r.name === "manageTags") renderManageTags(main);
  else if (r.name === "manageQueryTags") renderManageQueryTags(main);
  else if (r.name === "queryTagUsage") renderQueryTagUsage(main);
  else if (r.name === "jobHistory") renderJobHistory(main);
  else renderFleet(main);
}

/* Whether a route targets a specific saved view/notebook (its renderer or composer) — the routes whose sidebar
   entry should light up, and which the poll guard must not clobber (for the composer forms). */
function isViewItemRoute(name) {
  return name === "view" || name === "editor" || name === "notebook" || name === "notebookEditor";
}

/* The sidebar's "Custom Views" nav stays lit across the list, both renderers, and both composers; the "Alert Rules"
   nav stays lit across its list + editor. */
function navKeyFor(r) {
  if (r.name === "views" || isViewItemRoute(r.name)) return "views";
  if (r.name === "alertRules" || r.name === "alertEditor") return "alert-rules";
  if (r.name === "muteRules") return "mute-rules";
  if (r.name === "knownPatterns") return "known-patterns";
  if (r.name === "manageTags") return "manage-tags";
  if (r.name === "manageQueryTags") return "manage-query-tags";
  if (r.name === "queryTagUsage") return "query-tag-usage";
  if (r.name === "jobHistory") return "job-history";
  return r.name;
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

/* Front door: one roll-up per store, read together. A store that fails shows its own message in the sidebar; the rest draw. */
let lastStoreFleets = null;

async function refreshStoreSidebar() {
  const list = frontDoorStores();
  const groups = await Promise.all(list.map(async (s) => ({ store: s, res: await apiGetFleet(s.name) })));
  lastStoreFleets = groups;
  const own = groups.find((g) => g.store.name === currentStore());
  lastSidebarFleet = own && own.res.kind === "data" ? own.res : null;
  if (lastSidebarFleet) refreshAttention(readTool).then(() => paintSidebar());
  else updateStatusBar(null);
  paintSidebar();
}

async function refreshSidebar() {
  if (frontDoor()) {
    await refreshStoreSidebar();
    return;
  }
  const res = await apiGetFleet();
  if (res.kind !== "data") {
    mount(serverList, el("div", { class: "muted", style: "padding:0.5rem 1.25rem", text: res.kind === "error" ? "Fleet unavailable" : "" }));
    updateStatusBar(null);
    return;
  }

  lastSidebarFleet = res;
  /* One fleet-wide alert read per poll feeds every badge; it repaints the sidebar when it lands. */
  refreshAttention(readTool).then(() => paintSidebar());
  paintSidebar();
}

/* The last good fleet read, kept at module scope so a favourite or acknowledgement repaints without a refetch. */
let lastSidebarFleet = null;

function paintSidebar() {
  /* #6214: a signed-out page paints nothing: a late poll or a local change must not bring the old session's lists back. */
  if (isSessionExpired()) return;
  if (frontDoor()) {
    if (!lastStoreFleets) return;
    paintStoreServerLists(serverList, lastStoreFleets, currentRoute().name === "server" ? currentRoute().param : null, currentStore());
    updateStatusBar(lastSidebarFleet ? lastSidebarFleet.data : null);
    return;
  }
  const res = lastSidebarFleet;
  if (!res) return;
  const r = currentRoute();
  /* The search box, the group-by-tag view and the list rows live in sidebar.js; the term is its module state, so
     this repaint (the 60 s poll, a favourite, a route change) never resets it. */
  paintServerList(serverList, res.data, r.name === "server" ? r.param : null);
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

  // #4189: a count-only read, not the full topology /api/ag builds — this probe only ever checks the one field.
  const res = await apiGet("/api/ag/count");
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
  if (isSessionExpired()) return; // the session ended while the list was in flight: keep nothing of it
  lastViewList = { session, views: res.kind === "data" && Array.isArray(res.data) ? res.data : [] };
  paintViewListNow();
}

/* #6214: the last good list, kept so collapsing a folder (a viewer-local change) repaints without a refetch. The grouping,
   the order and the collapse state live in view-sidebar.js and view-folders.js. */
let lastViewList = null;

function paintViewListNow() {
  if (isSessionExpired() || !viewList || !lastViewList) return;
  const r = currentRoute();
  paintViewList(viewList, lastViewList.views, lastViewList.session, isViewItemRoute(r.name) ? r.id : null);
}

/* ─────────────────────────── status bar ─────────────────────────── */

/* A fixed footer mirroring the WPF viewer's status bar: fleet server count, collectors healthy/failing across
   the fleet, the store size, this session's seat, the collection state when it is not ok, and the last refresh
   time. The fleet figures come from the SAME /api/fleet response the sidebar just read (no extra round-trip).
   The extra items are filled in by refreshStatusExtras and never block or break the bar: each one that cannot
   be read is simply absent (the store size keeps its last good value, marked stale).

   Module scope on purpose: the 60s poll rebuilds the bar, so the cached size and the last ping state live here
   rather than inside updateStatusBar. The operator's "collection paused" flag is not on /api/ping (the
   unauthenticated health route, which also never names a person); it rides on the /api/fleet response the bar is
   already drawn from (d.collection: paused, and paused_by / paused_utc when the store still holds who and when),
   so showing it costs no extra read. */
const STORE_SIZE_TTL_MS = 5 * 60 * 1000;
const NON_OK_COLLECTION_STATES = new Set(["starting", "degraded", "stopped"]);
let statusFleet = null;
let statusSeat = null;
let statusCollection = null;
let storeSizeBytes = null;
let storeSizeStale = false;
let storeSizeAttemptAt = 0;
let storeSizeInFlight = false;

/* The Viewer's wording (ViewerSeatIndicator): "Seat: read-write" / "Seat: read-only" / "Seat: not connected". */
function seatLabel(session) {
  if (!session) return null;
  if (session.probe_failed) return "Seat: not connected";
  return session.can_edit ? "Seat: read-write" : "Seat: read-only";
}

/* The Viewer's FormatBytes: GB with one decimal from 1 GB up, whole MB below. */
function formatStoreSize(bytes) {
  const mb = 1024 * 1024;
  const gb = mb * 1024;
  return bytes >= gb ? (bytes / gb).toFixed(1) + " GB" : Math.round(bytes / mb) + " MB";
}

/* /api/ping answers 503 for degraded/stopped, so apiGet would classify it as an error and drop the body; this
   reads the body whatever the status. Any failure is null (the item is hidden). */
async function fetchCollectionState() {
  try {
    const resp = await fetch("/api/ping", { headers: storeHeaders({ Accept: "application/json" }) });
    const body = await resp.json();
    return body && typeof body.status === "string" ? body.status : null;
  } catch {
    return null;
  }
}

/* get_store_host is read at most once per STORE_SIZE_TTL_MS, failures included, so a store that cannot answer is
   not retried every poll. A failure keeps the last good size and marks it stale. */
async function refreshStoreSize(now) {
  if (storeSizeInFlight || (storeSizeAttemptAt && now - storeSizeAttemptAt < STORE_SIZE_TTL_MS)) return;
  storeSizeInFlight = true;
  storeSizeAttemptAt = now;
  try {
    const res = await readTool("get_store_host", {});
    const bytes = res && res.kind === "data" && res.data && res.data.store ? res.data.store.size_bytes : null;
    if (typeof bytes === "number" && isFinite(bytes) && bytes >= 0) {
      storeSizeBytes = bytes;
      storeSizeStale = false;
    } else {
      storeSizeStale = true;
    }
  } catch {
    storeSizeStale = true;
  } finally {
    storeSizeInFlight = false;
  }
}

async function refreshStatusExtras() {
  const [session, state] = await Promise.all([
    getSession().catch(() => null),
    fetchCollectionState(),
    refreshStoreSize(Date.now()),
  ]);
  statusSeat = seatLabel(session);
  statusCollection = state;
  renderStatusBar();
}

function renderStatusBar() {
  if (!statusbar) return;
  const d = statusFleet;
  if (!d) {
    mount(statusbar, el("span", { class: "sb-item muted", text: "Fleet unavailable" }));
    return;
  }
  let healthy = 0;
  let warning = 0;
  let failing = 0;
  let notCollected = 0;
  for (const c of d.cards || []) {
    healthy += c.healthy_collector_count || 0;
    warning += c.warning_collector_count || 0;
    failing += c.failed_collector_count || 0;
    notCollected += c.not_collected_collector_count || 0;
  }
  const servers = d.total_servers || 0;
  const items = [
    el("span", { class: "sb-item", text: servers + (servers === 1 ? " server" : " servers") }),
    el("span", { class: "sb-item", text: healthy + " collectors healthy · " + (warning > 0 ? warning + " warning · " : "") + failing + " failing" + (notCollected > 0 ? " · " + notCollected + " not collected" : "") }),
  ];
  if (storeSizeBytes != null) {
    items.push(el("span", { class: "sb-item sb-store-size" + (storeSizeStale ? " muted" : ""), text: "Database: " + formatStoreSize(storeSizeBytes) + (storeSizeStale ? " (stale)" : "") }));
  }
  if (statusSeat) items.push(el("span", { class: "sb-item sb-seat", text: statusSeat }));
  if (statusCollection && NON_OK_COLLECTION_STATES.has(statusCollection)) {
    items.push(el("span", { class: "sb-item sb-collection sb-warn", text: "Collection: " + statusCollection[0].toUpperCase() + statusCollection.slice(1) }));
  }
  /* A paused service collects and alerts on nothing, and the runtime state above still says "ok", so the bar says so
     in its own words, with who paused it and when when the store has that. */
  if (d.collection && d.collection.paused) {
    let text = "Collection: PAUSED";
    if (d.collection.paused_by) text += " by " + d.collection.paused_by;
    if (d.collection.paused_utc) text += " since " + localTime(d.collection.paused_utc);
    items.push(el("span", { class: "sb-item sb-collection sb-paused sb-warn", text }));
  }
  /* The stamp is the page's own last load (notePageUpdated), not the sidebar's fleet read: the fleet read is the
     shell's, so a server, FinOps or Job History page reloaded by hand or by auto-refresh left the time at the first
     load (the morning walk, W3/W10). Until the first page settles, the fleet roll-up's time stands in. */
  const updated = statusPageUpdatedAt != null ? new Date(statusPageUpdatedAt).toISOString() : d.generated_at;
  items.push(el("span", { class: "sb-item sb-updated", text: "Updated " + localTime(updated) }));
  items.push(el("span", { class: "sb-item", id: "refresh-hint" }));

  const children = [];
  items.forEach((item, i) => {
    if (i) children.push(el("span", { class: "sb-sep", text: "·" }));
    children.push(item);
  });
  mount(statusbar, children);
  updateRefreshHint();
}

/* When the current page last finished loading (epoch ms), set by every route's render: first load, auto-refresh
   poll and "Reload this page now" alike. */
let statusPageUpdatedAt = null;

function notePageUpdated(ms) {
  statusPageUpdatedAt = ms;
  /* Before the first fleet read lands there is no bar to repaint (it would flash "Fleet unavailable"). */
  if (statusFleet) renderStatusBar();
}

function updateStatusBar(d) {
  statusFleet = d || null;
  renderStatusBar();
  if (d) refreshStatusExtras();
}

/* ─────────────────────────── refresh loop ─────────────────────────── */

/* Routes whose page the poll never re-renders.
   Poll-clobber guard (#1563, extended #3285): never re-render an editor (dashboard/notebook composer OR the
   alert-rule editor) from the background poll — a rebuild would discard an in-progress edit. hashchange still
   routes to it normally; only the periodic refresh skips it.
   Triage cost guard (#4222d): #/triage is the alert-notebook deep link's landing page. Its cost item says the
   periodic poll must not re-render it every 60s — the same reason the composer routes are skipped, just for
   "don't waste a read against a page you opened to read once" rather than "don't discard an edit". hashchange
   still routes there normally on first load / a fresh alert link.
   Query Tag Usage (#6793): its figures are 24 hour totals and the fleet read can take minutes on a large store, so a 60 s
   re-read adds almost nothing and costs a lot. The page's own server picker and range picker still re-read it. */
function isNoPollRoute(routeName) {
  return routeName === "editor" || routeName === "notebookEditor" || routeName === "alertEditor"
    || routeName === "triage" || routeName === "queryTagUsage";
}

/* Per-page schedule (#4666). pageNextRefreshAt is when the page may re-render next (Infinity = never);
   pageRenderStart is set while a render's reads are outstanding, and its end is the moment hasInFlightReads()
   next reads false, on a hidden or paused tick as much as a visible one (#4774) — that duration feeds the
   back-off. */
let pageNextRefreshAt = Infinity;
let pageRenderStart = 0;
let pageRendering = false;
let pageLastRenderMs = 0;
let shellLastRefreshAt = Date.now();

/* The page's own interval in ms; 0 means no automatic re-render. */
function pageIntervalMs() {
  const name = currentRoute().name;
  if (isNoPollRoute(name)) return 0;
  if (name === "view" || name === "notebook") {
    const choice = currentViewRefresh() || defaultRefreshChoice(name === "notebook");
    return REFRESH_CHOICES[choice] ?? 0;
  }
  if (isPickerRoute(name)) return PAGE_REFRESH_CHOICES[pageRefreshChoice] ?? POLL_MS;
  return POLL_MS;
}

/* The server and FinOps pages carry the shell's interval selector and Refresh button (persisted in localStorage). */
let pageRefreshChoice = loadPageRefreshChoice(localStorage);
let pageRefreshControl = null;

function isPickerRoute(routeName) {
  return routeName === "server" || routeName === "finops";
}

function syncPageRefreshControl(routeName) {
  if (!pageRefreshControl) return;
  pageRefreshControl.root.hidden = !isPickerRoute(routeName);
}

/* The Refresh button: one shell refresh and one immediate page re-render. While any read is outstanding it does
   nothing (the button is disabled and marked busy until they settle), so a double-click sends one refresh. */
function syncRefreshBusy() {
  if (pageRefreshControl) pageRefreshControl.setBusy(hasInFlightReads());
}

function refreshPageNow() {
  if (isSessionExpired() || hasInFlightReads()) return;
  refreshShell();
  if (!isNoPollRoute(currentRoute().name)) route({ poll: true, manual: true });
  syncRefreshBusy();
}

/* Another tab saved a different interval: take it here too. */
function onPageRefreshStorage(event) {
  if (event.key !== null && event.key !== PAGE_REFRESH_KEY) return;
  pageRefreshChoice = loadPageRefreshChoice(localStorage);
  if (pageRefreshControl) pageRefreshControl.setChoice(pageRefreshChoice);
  if (!pageRendering) scheduleNextPageRefresh(Date.now());
  else updateRefreshHint();
}

function initPageRefreshControl() {
  const anchor = document.getElementById("auto-refresh-toggle");
  if (!anchor || !anchor.parentNode) return;
  pageRefreshControl = buildPageRefreshControl(pageRefreshChoice, (choice) => {
    pageRefreshChoice = savePageRefreshChoice(localStorage, choice);
    if (!pageRendering) scheduleNextPageRefresh(Date.now());
    else updateRefreshHint();
  }, refreshPageNow);
  anchor.parentNode.appendChild(pageRefreshControl.root);
  window.addEventListener("storage", onPageRefreshStorage);
  syncPageRefreshControl(currentRoute().name);
}

function markPageRenderStart(routeName, isPoll) {
  syncPageRefreshControl(routeName);
  if (!isPoll && (routeName === "view" || routeName === "notebook")) clearViewRefresh();
  /* Query Tag Usage is a no-poll page whose one read can run for minutes (#6793): its "Updated" time is written when that read
     settles, like every polled page, not at render start. The editors and #/triage read once or not at all, so they stamp now. */
  if (isNoPollRoute(routeName) && routeName !== "queryTagUsage") {
    pageRendering = false;
    pageNextRefreshAt = Infinity;
    notePageUpdated(Date.now());
    return;
  }
  pageRendering = true;
  pageRenderStart = Date.now();
  pageRenderErrorsAtStart = readErrorCount();
  pageNextRefreshAt = Infinity;
}

/* The read-error count when the current render began; a render whose reads raised it ended with a red strip. */
let pageRenderErrorsAtStart = 0;

function settlePageRender(now) {
  pageRendering = false;
  pageLastRenderMs = now - pageRenderStart;
  /* The footer says the page is current only when this render's reads settled without an error: a poll that failed keeps the last
     good time, and the red strip on the page is the failure's own notice. */
  if (readErrorCount() === pageRenderErrorsAtStart) notePageUpdated(now);
  scheduleNextPageRefresh(now);
}

function scheduleNextPageRefresh(from) {
  const delay = nextRefreshDelayMs(pageIntervalMs(), pageLastRenderMs);
  pageNextRefreshAt = delay === null ? Infinity : from + delay;
  updateRefreshHint();
}

function pageIsDue(now) {
  return !pageRendering && now >= pageNextRefreshAt;
}

function refreshShell() {
  shellLastRefreshAt = Date.now();
  /* The sidebar and the route() below both read /api/fleet in this same synchronous pass; apiGetFleet hands the
     second caller the first one's request, so a tick costs the store ONE fleet roll-up, not two (#3895). */
  if (frontDoor()) refreshPicker();
  refreshSidebar();
  refreshViewList();
  refreshAgNav();
}

/* Re-render the page from the poll. Editors and #/triage are never re-rendered from the poll (a rebuild would
   discard an in-progress edit, or waste a read against a page opened to read once), and neither is a page whose
   own last render's reads are still outstanding (#4191): a poll landing mid-load would fire every one of its
   panel reads a second time on top of the first, which is exactly what doubled audit_config on the Config tab.
   The check runs before refreshShell() starts any of THIS tick's reads (refresh() and schedulerTick() call the
   shell first, but its reads are the sidebar/view-list/AG-nav probes, not the page's), so it reflects only what
   the PREVIOUS render left running. The sidebar/view-list/AG-nav probe keep refreshing regardless — only the
   heavier per-page render waits for the last one to settle. */
function refreshPage() {
  if (isNoPollRoute(currentRoute().name) || hasInFlightReads()) return;
  route({ poll: true });
}

/* The refresh entry the master pause and the tab-visible handler use: always the shell, and the page when
   `force` is set or it is due. */
function refresh(force) {
  if (isSessionExpired()) return;
  refreshShell();
  if (force || pageIsDue(Date.now())) refreshPage();
}

function schedulerTick() {
  const now = Date.now();
  /* A render ends when its reads do, whether or not the tab is hidden or auto-refresh is paused (#4774). This
     check used to sit below the return that follows, so a render whose reads finished while the tab was hidden
     or paused stayed open until the first visible, unpaused tick: its duration then counted the whole span (10
     minutes hidden read as a 10-minute render), the back-off pushed the next refresh out to 15 minutes with
     "(slow page)", and pageIsDue never fired because it needs !pageRendering. Settling here stamps the end at
     the first tick that sees no reads outstanding and schedules the next refresh from that moment. */
  if (pageRendering && !hasInFlightReads()) settlePageRender(now);
  syncRefreshBusy();
  if (document.hidden || isAutoRefreshPaused() || isSessionExpired()) {
    updateRefreshHint();
    return;
  }
  /* Off pauses only the page's own render; the shell roll-up (sidebar, status bar, session check) keeps running. */
  if (now - shellLastRefreshAt >= POLL_MS) refreshShell();
  if (pageIsDue(now)) refreshPage();
  updateRefreshHint();
}

function hhmm(ms) {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function updateRefreshHint() {
  const hint = document.getElementById("refresh-hint");
  if (!hint) return;
  const interval = pageIntervalMs();
  let text;
  if (isAutoRefreshPaused()) text = "Auto-refresh paused";
  else if (interval === 0) text = "Auto-refresh off";
  else if (isBackedOff(interval, pageLastRenderMs) && Number.isFinite(pageNextRefreshAt)) {
    text = "Next refresh " + hhmm(pageNextRefreshAt) + " (slow page)";
  } else {
    const choices = isPickerRoute(currentRoute().name) ? PAGE_REFRESH_CHOICES : REFRESH_CHOICES;
    const choice = Object.keys(choices).find((k) => choices[k] === interval);
    text = "Auto-refresh: " + (choice ? refreshLabel(choice) : "1 min");
  }
  hint.textContent = text;
}

/* The session-expired takeover (#4187): the FIRST read anywhere on the page to report the session is gone (an
   expired/rotated session cookie, no token — see util.js's classifyResponse) replaces the whole shell with a
   sign-in prompt, once, instead of leaving every open panel to separately render its own "signed out" guess
   that looks like an unrelated failure.

   The sign-in link must RELOAD the document (#6278). A link to the page's own address, fragment included, is a
   same-document navigation: nothing reloads and no hashchange fires, so the click did nothing. The click always
   reloads THIS BROWSER's own current location, and the link's href is that same address. The 401 body's `login`
   value is deliberately never used (the #4187 design: the server's answer is "/", which would drop the page being
   read, and an address taken from a response could point anywhere). Reloading the current URL rather than "/" (#4221)
   keeps an alert link's #/triage route through a stale-session sign-in: the hash never left the address bar. */
function signInAgain() {
  location.reload();
}

function showSignedOutState(message, login) {
  /* #6214: drop the cached lists too, so nothing later repaints the old session's personal views. */
  lastViewList = null;
  lastSidebarFleet = null;
  clearViewGroups();
  mount(serverList, []);
  mount(viewList, []);
  mount(statusbar, el("span", { class: "sb-item muted", text: "Signed out" }));
  mount(main, el("div", { class: "strip error", role: "alert" }, [
    (message || "Your session is no longer valid.") + " ",
    el("a", {
      href: location.pathname + location.search + location.hash,
      text: "Sign in again",
      onClick: (e) => { e.preventDefault(); signInAgain(); },
    }),
  ]));
}

/* ─────────────────────────── auto-refresh play/pause (#4222d) ─────────────────────────── */

/* Shell-level control, not just alert notebooks: the tab-hidden pause (above) is automatic and invisible;
   this is the operator's OWN on/off switch for the 60s tick, usable from every page. Persisted in
   localStorage so it survives a reload/navigation — an operator investigating a live incident who paused the
   screen to read it stays paused after following a link. Resuming runs one refresh immediately rather than
   waiting out whatever is left of the 60s window, so "Resume" reads as "refresh now, and keep going". */
const AUTO_REFRESH_PAUSED_KEY = "darling.autoRefreshPaused";

function isAutoRefreshPaused() {
  return localStorage.getItem(AUTO_REFRESH_PAUSED_KEY) === "1";
}

function setAutoRefreshPaused(paused) {
  if (paused) localStorage.setItem(AUTO_REFRESH_PAUSED_KEY, "1");
  else localStorage.removeItem(AUTO_REFRESH_PAUSED_KEY);
}

function updateAutoRefreshToggle(button, paused) {
  const label = paused ? "Resume auto-refresh" : "Pause all auto-refresh";
  button.textContent = label;
  button.setAttribute("aria-label", label);
  button.setAttribute("title", label);
  button.setAttribute("aria-pressed", String(paused));
}

function initAutoRefreshToggle() {
  const button = document.getElementById("auto-refresh-toggle");
  if (!button) return;

  updateAutoRefreshToggle(button, isAutoRefreshPaused());
  button.addEventListener("click", () => {
    const paused = !isAutoRefreshPaused();
    setAutoRefreshPaused(paused);
    updateAutoRefreshToggle(button, paused);
    if (!paused) refresh(true);
    updateRefreshHint();
  });
}

/* #6840: ask the session whether this is a front door before anything else reads, because every read after it names a store. */
async function startFrontDoor() {
  const session = await getSession();
  if (!session || session.front_door !== true) return;
  await initFrontDoor(document.getElementById("store-picker"), location.hash || "");
  /* The menu's own links open in the store in use, so "open in a new tab" keeps it. */
  for (const a of document.querySelectorAll("#sidebar .nav a")) {
    const href = a.getAttribute("href");
    if (href && href.startsWith("#/")) a.setAttribute("href", storeHash(href));
  }
}

function start() {
  window.addEventListener("hashchange", route);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && !isAutoRefreshPaused()) refresh(false);
  });
  onViewRefreshChange(() => {
    if (!pageRendering) scheduleNextPageRefresh(Date.now());
    else updateRefreshHint();
  });
  setInterval(schedulerTick, SCHEDULER_TICK_MS);
  onSessionExpired(showSignedOutState);
  initAutoRefreshToggle();
  initPageRefreshControl();
  initSidebarCollapse(document.getElementById("app"), document.getElementById("sidebar-collapse"));
  initNarrowNav(document.getElementById("app"), document.getElementById("nav-toggle"), document.getElementById("sidebar"));
  initSidebarSearch(document.getElementById("server-search"), paintSidebar);
  initSeverityColorSettings(document.getElementById("viewer-settings"));
  onLocalChange(paintSidebar);
  onLocalChange(paintViewListNow);

  /* #6243: a saved, created or deleted view reaches the sidebar at once, not on the next 60 s poll. */
  onViewsChanged(refreshViewList);

  refreshSidebar();
  refreshViewList();
  refreshAgNav();
  route();
}

startFrontDoor().catch(() => {}).then(start);
