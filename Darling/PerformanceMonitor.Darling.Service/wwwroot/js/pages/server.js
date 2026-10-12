/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Server detail page (#1562, deepened for the web/desktop parity pass) — the per-server drill-down reached by
 * clicking a fleet card. This module is the SHELL: the header, the sub-tab bar, the time-range control, and the
 * panel grid. Every panel lives in pages/server-tabs.js as a descriptor run through the unmodified renderPanel
 * (the #1563 seam), which is why adding a tab is data, not plumbing.
 *
 * The sub-tabs are the web port of the desktop viewer's per-server TabControl (ViewerServerTab.xaml). The tab id
 * rides in the hash — #/server/{name}/{tab} — so a tab is DEEP-LINKABLE and survives the 60s refresh, which
 * re-renders the whole route. An unknown or absent tab id resolves to Overview rather than erroring, so every
 * pre-existing #/server/{name} link keeps working unchanged.
 *
 * WHICH tab set is a question about the SERVER, not about the shell (#2530): a PostgreSQL target gets the six
 * PostgreSQL tabs and a SQL Server target the twelve SQL Server ones, chosen by serverTabsFor() from the fleet
 * card's server-derived `is_postgres`. That fact arrives with the ONE /api/fleet read this page already made
 * for the header's band and reason — not a second request, and not a guess rendered first and corrected after.
 * Rendering the SQL Server set optimistically would flash twelve wrong tabs at every PostgreSQL target and
 * start nine reads that cannot answer, which is the exact experience #2530 was filed about.
 *
 * So the FIRST render of a server waits, behind a loading strip, and every render after it does not. The
 * engine is a property of the server rather than of the render — it changes about as often as the server's
 * operating system does — so `lastCard` remembers it by name and a repeat render picks its registry
 * SYNCHRONOUSLY, exactly as this page behaved before it was engine-aware. That matters because route()
 * re-renders on every sub-tab click AND on the 60s poll: gating those on a fetch would blank the tab bar and
 * the grid once a minute, and would make a sub-tab click wait on /api/fleet before the new tab's panels even
 * started loading. When the fresh card lands it always refreshes the header, and rebuilds the bar and grid
 * only if the registry it chooses is a different one — which is to say, essentially never.
 *
 * A card that does NOT arrive (a failed fleet read, a server the fleet does not carry) leaves a painted page
 * alone rather than repainting it as SQL Server: a transient read failure is not evidence that a server
 * stopped being PostgreSQL. With nothing painted yet it falls back to the SQL Server registry, which is what
 * an unclaimed server has always rendered.
 *
 * The time range is the web twin of ViewerServerTab.TimeRange.cs's preset picker: one page-level window that
 * every time-windowed panel is given. It is module state (like the fleet page's sort), so it survives the
 * refresh — but it is NOT persisted to localStorage, because a page that reopens on a 7-day window is slow for
 * a reason the reader cannot see. Panels whose read takes no window at all say "latest snapshot" in their own
 * subtitle rather than inheriting a label that would misdescribe them.
 */

import { el, mount, apiGetFleet, bandClass, loadingStrip, noticeStrip, setActiveRange, setActiveDatabaseFilter, localTime } from "../util.js";
import { getDatabaseFilter } from "../viewer-local.js";
import { databaseFilterControl, databaseFilterUnavailable, closeDatabaseFilter } from "./database-filter.js";
import { setPanelSignal } from "../panels.js";
import { serverTabsFor, isPostgresTarget, findServerTab, tabNote } from "./server-tabs.js";
import { metricBands } from "./fleet.js";
import { timeRangePicker, DEFAULT_REACH_HOURS } from "../time-range-picker.js";
import { serverCatalog, collectorIntervalFromCatalog, readsReachHours, offerReach } from "../page-range.js";
import { browserZone, resolveSpec, relativeSpec, fixedSpec, specName, wholeHours, readWindow, reachText, ROLLING_PRESETS, MINIMUM_SPAN_MS } from "../time-range.js";

/* How far back the tab on screen reaches (#5562 review r1 M4, ruling R2). Each tab lists the windowed reads it makes (`reachReads` in
   server-tabs.js: names, never hours), and its reach is the smallest `max_hours` the catalog gives them (tabReach below), so a tab
   made of reads that take 30 days offers 30 days, and a tab that also shows a list that takes 7 stays at 7. A read with no catalog
   row, a tab that lists none and a catalog that cannot be read all give the common DEFAULT_REACH_HOURS (168): the page never offers
   a range a read has not said it takes. The picker greys out a longer range and says why, and a range carried over from a tab that
   reads further is read at this tab's reach with a notice saying so (reachNoteText), never silently (#2802 did the same per panel).
   WebServerPageRangeTests runs every offered range through every tab of both registries. */

/** The short presets that are a whole number of hours: the ranges the reads take as `hours` alone, with no trimming. Every other
 *  range (30 minutes, Yesterday, 2 days 3 hours, a typed pair) is fetched as whole hours back from its end and trimmed to the exact
 *  pair, the custom path below. A tab takes the ones within its own reach. */
const RANGE_OPTIONS = ROLLING_PRESETS.map((spec) => wholeHours(spec))
  .filter((hours) => hours != null)
  .map((hours) => rangeOption(hours));

function rangeOption(hours) {
  return { hours, label: hours === 1 ? "last hour" : hours > 24 && hours % 24 === 0 ? "last " + hours / 24 + " days" : "last " + hours + " hours" };
}

/* The catalog each server answered with, or null where it could not be read: the answer, not the promise, so a tab's reach is a plain
   function (tabReach) that holds until the catalog is known. The reads' reach belongs to the read, not the server, so one answer per
   server is enough for the page's life. A catalog that could not be read is not kept: the next tab or range change asks again. */
const catalogsRead = new Map();
/* Servers whose catalog could not be read when a wide range waited for it: they read at the common reach, and do not wait again. */
const catalogGaveUp = new Set();
/* The servers whose catalog is being read for the panels' wait below: one wait and one redraw per server, whatever redraws meanwhile
   (#5562 review r2 L2). */
const catalogWaits = new Set();

/** How far back a tab's reads all reach, in hours (the smallest catalog `max_hours` among its `reachReads`). */
function tabReach(tab = current.tab, server = current.server) {
  return readsReachHours(server ? catalogsRead.get(server) : null, tab && tab.reachReads);
}

/** The refusal for a span past the tab's reach, or null. */
function tabReachRefusal(spanMs, reach = tabReach()) {
  return spanMs > reach * HOUR_MS ? "This tab reads up to " + reachText(reach) + "." : null;
}

/* Module state, deliberately not persisted — see the header comment. `gridNode` + the current server/tab let the
   range control redraw only the panels, so changing the window does not flash the header or refetch /api/fleet. */
let pageHours = 24;
let gridNode = null;
let current = { server: null, tab: null };

/* The render generation, and why it appeared with the engine branch rather than with the page.
 *
 * `route()` re-renders this page on every hash change — every sub-tab click — and again on every 60s poll, so
 * two renderServer() calls are routinely in flight at once, and /api/fleet does not promise to answer them in
 * order. Before the tab set depended on the card, the only async work here touched header nodes captured in
 * its own closure, which a newer render had already detached from the document: a late response wrote to
 * garbage and nobody saw it. Now the callback writes MODULE state — `current`, and the grid redrawPanels()
 * reads — so the last response to land wins regardless of which render is on screen. Two servers in flight
 * paints one server's panels under the other's header and URL.
 *
 * A generation counter rather than an AbortController because the losing render must not cancel the shared
 * /api/fleet fetch out from under the winning one; the fetch is fine, it is only its RESULT that is stale.
 *
 * This is deliberately separate from `panelAbort` below (#4191), which DOES cancel — it owns only the
 * per-panel reads a redraw starts, never the shared fleet fetch this generation counter protects. */
let renderGeneration = 0;

/* The current panel batch's AbortController (#4191) — see redrawPanels(), which creates and aborts it. Module
   state like renderGeneration and lastCard, for the same reason: redrawPanels can run from three places (a
   fresh renderServer, the loadServerCard callback, and the range-select handler) and every one of them must
   cancel the SAME previous batch, not just the one its own caller happens to remember. */
let panelAbort = null;

/* The last card seen for a server name, so a repeat render can choose its registry without a round trip. Keyed
   on the ROUTE's name (which may be either the server name or the display name — the same key loadServerCard
   matches on), module-scoped like pageHours, and bounded by the number of servers visited in one session. */
const lastCard = new Map();

/* The range the reader picked that is not one of RANGE_OPTIONS, keyed by server (module scope, so the 60s poll's rebuild keeps it and
   another server starts on the presets). Each value is `{ spec, live }`: the range as the picker names it (a length, a calendar
   period, a typed pair) and whether its end slides with now. A LIVE range keeps its meaning and slides to end at "now" on every
   rebuild, so the poll keeps refreshing it. Any other end is a fixed historical window: its data cannot change, so the poll leaves
   the panels on screen instead of reading the same hours again. */
const customRanges = new Map();
const LIVE_SLACK_MS = 60000;
const HOUR_MS = 3600000;

/**
 * Check a picked start and end and map them onto the reads' window (an end, `as_of`, and a whole number of `hours`).
 * The start rounds EARLIER to a whole hour for the fetch (`hours` is an integer of at least 1); the page trims what comes
 * back to the exact pair, so a range of 15 minutes is fetched as the hour that holds it. Returns `{ error }` for an unusable
 * pair (under 5 minutes, reversed, in the future, past the reach), else `{ hours, asOf, live, startMs, endMs }`; `asOf`
 * is null for a live range, whose reads are anchored at the server's own clock.
 */
export function resolveCustomRange(startMs, endMs, nowMs, reach = tabReach()) {
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return { error: "Enter both a start and an end." };
  if (endMs <= startMs) return { error: "The end must be after the start." };
  if (endMs > nowMs + LIVE_SLACK_MS) return { error: "The end cannot be in the future." };
  const span = endMs - startMs;
  if (span < MINIMUM_SPAN_MS) return { error: "The shortest range is 5 minutes. Drag across a chart to look at a shorter span." };
  const tooLong = tabReachRefusal(span, reach);
  if (tooLong) return { error: tooLong };
  const live = endMs >= nowMs - LIVE_SLACK_MS;
  return { hours: Math.max(1, Math.ceil(span / HOUR_MS)), asOf: live ? null : new Date(endMs).toISOString(), live, startMs, endMs };
}

/** Hold a picked range for a server: a whole-hour preset is the page's own range (the reads take it as `hours` alone), anything
 *  else is this server's custom range. Returns the error text when the range cannot be held, else null. */
function holdSpec(server, spec, nowMs) {
  const resolved = resolveSpec(spec, nowMs, browserZone());
  if (!resolved.ok) return resolved.error.message;
  const tooLong = tabReachRefusal(resolved.range.spanMs);
  if (tooLong) return tooLong;
  const hours = wholeHours(spec);
  if (hours != null && RANGE_OPTIONS.some((o) => o.hours === hours)) {
    customRanges.delete(server);
    pageHours = hours;
  } else {
    customRanges.set(server, { spec, live: resolved.range.live || resolved.range.endMs >= nowMs });
  }
  return null;
}

/** Apply a custom range to a server and redraw. Returns the error text, or null when the range was taken. `redraw: false`
 *  is for a caller that rebuilds the page itself (the chart menu's "at This Time" items route to a tab, #5230): a panel
 *  redraw would leave the range picker in the page head on the old preset, and from another tab it would start a batch
 *  of reads that the route change then aborts. */
export function applyCustomRange(server, startMs, endMs, nowMs = Date.now(), { redraw = true } = {}) {
  const r = resolveCustomRange(startMs, endMs, nowMs);
  if (r.error) return r.error;
  /* A range that ends now keeps its width and slides; any other end is a fixed pair. */
  customRanges.set(server, { spec: r.live ? relativeSpec(endMs - startMs) : fixedSpec(startMs, endMs), live: r.live });
  if (redraw) redrawPanels();
  return null;
}

/** The {hours,label} context every tab build() is given. A custom range also hands the util module its exact pair, so
 *  every read of this server inside it is anchored and trimmed there. */
export function rangeContext(nowMs = Date.now()) {
  const custom = current.server ? customRanges.get(current.server) : null;
  const reach = tabReach();
  if (custom) {
    const resolved = resolveSpec(custom.spec, nowMs, browserZone());
    if (resolved.ok) {
      /* A range carried over from a tab that reads further is read at THIS tab's reach: the last stretch of it, ending where it ends.
         The range stays held (the next tab may take all of it), and reachNoteText says what happened (#5562 review r1 M4). */
      const cut = tabReachRefusal(resolved.range.spanMs, reach) != null;
      const range = cut ? { ...resolved.range, startMs: resolved.range.endMs - reach * HOUR_MS, spanMs: reach * HOUR_MS } : resolved.range;
      const w = readWindow(range, nowMs);
      setActiveRange({ server: current.server, hours: w.hours, startMs: range.startMs, endMs: range.endMs, asOf: w.asOf });
      /* Totals and rankings are read over whole hours back from the end, so they can begin earlier than the picked start:
         when the span is not a whole number of hours the label says where they begin. */
      const aggregateFrom = w.endMs - w.hours * HOUR_MS;
      const rounded = aggregateFrom < range.startMs ? "; totals and rankings aggregate from " + localTime(new Date(aggregateFrom).toISOString()) : "";
      const times = localTime(new Date(range.startMs).toISOString()) + " to " + (range.live ? "now" : localTime(new Date(range.endMs).toISOString()));
      const picked = custom.spec.kind === "relative" || custom.spec.kind === "calendar" ? specName(custom.spec).toLowerCase() : "custom";
      const named = cut ? "last " + reachText(reach) + " of " + picked : picked;
      return { hours: w.hours, label: named + ": " + times + rounded, custom: true };
    }
  }
  setActiveRange(null);
  /* A held preset longer than this tab reads is read at the tab's reach (reachNoteText says so). */
  const held = RANGE_OPTIONS.find((o) => o.hours === pageHours) || RANGE_OPTIONS.find((o) => o.hours === 24) || RANGE_OPTIONS[0];
  const opt = held.hours > reach ? rangeOption(reach) : held;
  /* A held range that cannot resolve right now (a calendar period that ends at or before its start, Today at exactly midnight, R9) is
     kept, not dropped: the label says so and names the window shown instead, and the range reads again once it can (#5562 review r1 L3). */
  if (custom) {
    const unresolved = resolveSpec(custom.spec, nowMs, browserZone());
    if (!unresolved.ok) return { hours: opt.hours, label: specName(custom.spec).toLowerCase() + " cannot be read yet (" + unresolved.error.message + "); showing " + opt.label };
  }
  return { hours: opt.hours, label: opt.label };
}

/** The sentence for a range the reader picked that is longer than the tab on screen reads, or null when the tab takes all of it. The
 *  tab reads its own reach instead, so the page says so beside the panels, never silently (#5562 review r1 M4, ruling R2). */
export function reachNoteText(nowMs = Date.now()) {
  const reach = tabReach();
  const custom = current.server ? customRanges.get(current.server) : null;
  let spanMs = pageHours * HOUR_MS;
  let picked = rangeOption(pageHours).label;
  const resolved = custom ? resolveSpec(custom.spec, nowMs, browserZone()) : null;
  if (resolved && resolved.ok) {
    spanMs = resolved.range.spanMs;
    picked = custom.spec.kind === "relative" || custom.spec.kind === "calendar" ? specName(custom.spec).toLowerCase() : "custom range";
  }
  if (tabReachRefusal(spanMs, reach) == null) return null;
  return "The range you picked (" + picked + ") is longer than this tab reads. This tab reads up to " + reachText(reach) + ", so it shows the last " + reachText(reach) + " of that range.";
}

/* Set when the poll ticks over a fixed custom range: the panels the previous render drew stay on screen (see
   renderServer). `gridKey` names the server and tab that grid shows. */
let keepGrid = false;
let gridKey = "";

/**
 * @param {object} [opts] — `{ poll: true }` when this call is the 60s poll's own refresh (app.js's refresh(),
 * threaded through route()), as opposed to a sub-tab click / deep link (hashchange) or the first paint. Only
 * that case, and a server this page has no card for yet, re-fetch /api/fleet — see the comment at the call
 * below (#4190).
 */
export function renderServer(main, server, tabId, opts) {
  const isPoll = !!(opts && opts.poll === true);
  /* A tab click, another server or a deep link closes the Databases popover (W9); only the poll's rebuild keeps it open. */
  if (!isPoll) closeDatabaseFilter();
  const generation = ++renderGeneration;
  const custom = customRanges.get(server);
  /* A fixed custom range cannot change under the poll, so the poll keeps the panels it already drew (same server, same
     tab) instead of reading the same hours again. The Refresh button passes `manual: true`: a click asks for a re-read, so it rebuilds even then. A live range, a preset and a tab or server click rebuild as before. */
  const keep = isPoll && !(opts && opts.manual === true) && !!custom && !custom.live && !!gridNode && gridKey === server + "|" + (tabId || "");
  keepGrid = false;
  current = { server, tab: null };

  const dot = el("span", { class: "dot" });
  /* The route's server KEY until the fleet card is known; fillServerHead then swaps in the display name. */
  const title = el("h2", { text: server });
  const badgeSlot = el("span", { class: "server-band" });
  const engineSlot = el("span", { class: "server-engine" });
  const whySlot = el("div", { class: "server-why" });
  /* The database filter's button (#5245): placed once the card is known, because the card says which engine this is and which
     id the choice is stored under. */
  const dbSlot = el("span", { class: "db-filter-slot" });
  const head = el("div", { class: "page-head" }, [
    el("a", { href: "#/fleet", text: "← Fleet" }),
    el("span", { class: "server-title" }, [dot, title]),
    badgeSlot,
    engineSlot,
    el("div", { class: "spacer" }),
    rangeControl(),
    dbSlot,
  ]);

  /* The bar and the note share one slot because both are decided by the same card. */
  const tabsSlot = el("div", { class: "subtabs-slot" }, [loadingStrip()]);
  /* The notice for a carried-over range this tab reads only part of (reachNoteText), between the tab bar and the panels. */
  reachSlot = el("div", { class: "reach-note-slot" });
  if (!keep) gridNode = el("div", { class: "panel-grid" });
  mount(main, [head, whySlot, tabsSlot, reachSlot, gridNode]);

  /* Seen this server before? Then its engine is already known and the page paints now — no loading strip, and
     the tab's panels start fetching in this tick, which is what keeps the 60s poll and a sub-tab click feeling
     like they did before the tab set had a question to answer. */
  const remembered = lastCard.get(server);
  let painted = null;
  if (remembered) {
    fillServerHead(title, dot, badgeSlot, engineSlot, whySlot, remembered.card, remembered.reason);
    fillDatabaseFilter(dbSlot, server, remembered.card);
    painted = paintTabsKeeping(keep, tabsSlot, server, tabId, remembered.card);
  }

  /* /api/fleet is fetched again only to place a server this page has no card for yet, or on the poll's real
     refresh — a plain sub-tab click (isPoll false) with a remembered card reuses it instead of re-downloading
     the whole 66-81KB roll-up for one ~1.5KB card (#4190). The synchronous paint above already put that
     reused card on screen, so skipping the fetch here changes nothing about what a tab click shows; it only
     stops asking the store for an answer this page already has until the poll asks again. */
  if (!remembered || isPoll) {
    loadServerCard(server, (card, reason) => {
      /* A newer render has started since this fetch went out — everything below writes module state or mounts
         into nodes this render no longer owns, so the only correct thing to do with a stale answer is drop it. */
      if (generation !== renderGeneration) return;

      if (card) lastCard.set(server, { card, reason });
      fillServerHead(title, dot, badgeSlot, engineSlot, whySlot, card, reason);
      fillDatabaseFilter(dbSlot, server, card);

      /* Repaint only when there is nothing painted yet, or when the fresh card chooses a DIFFERENT registry —
         the two registries are module constants, so that comparison is exact. A null card never repaints over a
         painted page: a fleet read that failed says nothing about which engine this server runs. */
      if (!painted || (card && serverTabsFor(card) !== painted)) {
        painted = paintTabsKeeping(keep && !painted, tabsSlot, server, tabId, card);
      }
    });
  }
}

/* paintTabs, leaving the panels the previous render drew in place when `keep`. The flag is raised only for this one
   paint and always lowered after it, so a render that never paints (a failed fleet read) cannot leave it set for the
   next Apply or preset redraw to swallow. */
function paintTabsKeeping(keep, tabsSlot, server, tabId, card) {
  keepGrid = keep;
  try {
    return paintTabs(tabsSlot, server, tabId, card);
  } finally {
    keepGrid = false;
  }
}

/** Put the tab bar, its note and its panels on the page for a card, and return the registry that card chose. */
function paintTabs(tabsSlot, server, tabId, card) {
  const tabs = serverTabsFor(card);
  const tab = findServerTab(tabId, tabs);
  current = { server, tab };
  gridKey = server + "|" + (tabId || "");
  tabNoteSlot = el("div", { class: "tab-note-slot" }, [tabNote(tab, tabReach())]);
  mount(tabsSlot, [subtabBar(server, tab, tabs), tabNoteSlot]);
  redrawPanels();
  return tabs;
}

/* The two slots the tab's reach decides: its note (which names how far back the page shows) and the carried-over-range notice. */
let tabNoteSlot = null;
let reachSlot = null;

/** Show the reach the catalog gave the tab on screen: the picker's longest choice, the tab note's number and the carried-over-range
 *  notice. Run on every redraw and again when the catalog arrives. */
function applyTabReach() {
  const tab = current.tab;
  if (!tab || !current.server) return;
  const reach = tabReach();
  if (rangePicker && rangePicker.reachHours() !== reach) {
    rangePicker.setReach(reach);
    offerReach(rangePicker);
  }
  if (tabNoteSlot) mount(tabNoteSlot, [tabNote(tab, reach)]);
  if (reachSlot) {
    const note = reachNoteText();
    mount(reachSlot, note ? [noticeStrip(note)] : []);
  }
}

/** (Re)fill the panel grid for the current server + tab at the current range. No refetch of anything else. */
function redrawPanels() {
  if (!gridNode || !current.tab || !current.server) return;

  /* Before the keepGrid return: a poll that keeps the panels still built a new picker, which needs its reach and its note. */
  applyTabReach();
  applySampleNote();

  if (keepGrid) {
    keepGrid = false;
    return;
  }

  /* A range longer than the common reach cannot be read until the tab's reach is known (it may take the whole range, or only the
     last 7 days of it), so the panels wait for the catalog rather than read twice. A held range within the common reach reads now. */
  if (!catalogsRead.has(current.server) && !catalogGaveUp.has(current.server) && heldSpanMs() > DEFAULT_REACH_HOURS * HOUR_MS) {
    const server = current.server;
    if (panelAbort) panelAbort.abort();
    mount(gridNode, [loadingStrip()]);
    /* A range change or the 60 second refresh during the wait lands here again: it keeps the loading strip and adds no second wait, or
       two redraws would run when the catalog arrives and send the panels' reads twice (#5562 review r2 L2). The one redraw reads the
       tab and range held at that moment, so a change made during the wait is read once. */
    if (catalogWaits.has(server)) return;
    catalogWaits.add(server);
    serverCatalog(server).catch(() => null).then((catalog) => {
      catalogWaits.delete(server);
      if (catalog) catalogsRead.set(server, catalog);
      else catalogGaveUp.add(server);
      if (server === current.server) redrawPanels();
    });
    return;
  }

  /* Every redraw replaces the whole panel grid — a fresh render (poll tick or sub-tab click), or the range
     picker choosing a new window for the SAME tab — so it starts a whole new batch of panel reads and the
     PREVIOUS batch's, if still pending, are now for nobody. #4191: aborting them here is what stopped the
     Config tab's audit_config from running twice concurrently — the old batch is cancelled instead of left to
     finish. setPanelSignal (panels.js) is how renderPanel picks this up without build() or table()/stat()/
     line() threading a signal through every call — see its own comment. */
  if (panelAbort) panelAbort.abort();
  panelAbort = new AbortController();
  setPanelSignal(panelAbort.signal);

  applyDatabaseFilter();
  mount(gridNode, current.tab.build(current.server, rangeContext()));
}

/* The page's database filter (#5245), set before every panel redraw the way the custom range is: the chosen names for the card's
   id, or none for a PostgreSQL server (no filter there), a server whose card is not known yet, and a server with no choice. The
   card's id keys the choice, not the route's name, so a rename keeps it. */
function applyDatabaseFilter() {
  const card = current.server ? (lastCard.get(current.server) || {}).card : null;
  const databases = card && !isPostgresTarget(card) && card.server_id ? getDatabaseFilter(card.server_id) : [];
  setActiveDatabaseFilter({ server: current.server, databases });
}

/* The database filter's button in the page head: none for a PostgreSQL card; for no card (a fleet read that failed on the first
   paint) a disabled "Databases: unavailable" and no filter. A poll whose fleet read failed keeps the control it has. */
function fillDatabaseFilter(slot, server, card) {
  if (isPostgresTarget(card)) {
    mount(slot, []);
    return;
  }
  if (!card || !card.server_id) {
    if (!lastCard.has(server)) mount(slot, databaseFilterUnavailable());
    return;
  }
  mount(slot, databaseFilterControl({ serverId: card.server_id, server, onApply: redrawPanels }).node);
}

/* The sub-tab bar — the web port of ViewerServerTab.xaml's TabControl. Real <a href> links, not click handlers,
   so a tab can be middle-clicked, bookmarked and shared; the hash router does the rest. */
function subtabBar(server, active, tabs) {
  return el(
    "nav",
    { class: "subtabs", role: "tablist", "aria-label": "Server sections" },
    tabs.map((t) =>
      el("a", {
        class: "subtab" + (t.id === active.id ? " active" : ""),
        href: "#/server/" + encodeURIComponent(server) + "/" + t.id,
        role: "tab",
        "aria-selected": t.id === active.id ? "true" : "false",
        text: t.label,
      })
    )
  );
}

/** The time-range picker (time-range-picker.js): the short presets, the calendar periods, a typed range and a date pick, in the
 *  browser's zone, the zone every time on this page uses. Changing it redraws the panels in place, exactly like the fleet page's
 *  sort. A range longer than the tab's reads take (tabReach) is greyed out with the reason. */
function rangeControl() {
  const server = current.server;
  const saved = customRanges.get(server);
  const picker = timeRangePicker({
    spec: saved ? saved.spec : relativeSpec(pageHours * HOUR_MS),
    reachHours: tabReach(null, server),
    reachMessage: (hours) => "This tab reads up to " + reachText(hours) + ".",
    label: "Time range",
    onChange: (spec) => {
      if (holdSpec(server, spec, Date.now()) == null) redrawPanels();
    },
  });
  rangePicker = picker;
  return el("div", { class: "range-control" }, [el("span", { text: "Range" }), picker.node]);
}

/* The picker on screen, so the tab on screen can tell it how often its data is collected (#5562 R3). */
let rangePicker = null;

/** The "collected every N minutes" note for the tab on screen (#5562 R3). Each tab declares its main collector (`collector` in
 *  server-tabs.js); the server's catalog gives that collector's ACTUAL interval (a per-server schedule row over the fleet's over the
 *  shipped default) as `collector_interval_minutes`. The picker shows the note when the chosen span holds fewer than 3 samples at that
 *  interval, and never widens the range. A tab with no single main collector, or a catalog that cannot be read, shows no note. */
function applySampleNote() {
  const picker = rangePicker;
  const tab = current.tab;
  const server = current.server;
  if (!picker) return;
  if (!tab || !server) {
    picker.setSampleInterval(null);
    return;
  }
  /* The same catalog answer gives the tab its reach (M4) and its collector's interval (R3), so it is read for every tab. */
  serverCatalog(server).then((catalog) => {
    if (catalog) catalogsRead.set(server, catalog);
    if (picker !== rangePicker || tab !== current.tab || server !== current.server) return;
    applyTabReach();
    picker.setSampleInterval(tab.collector ? collectorIntervalFromCatalog(catalog, tab.collector) : null);
  }).catch(() => {
    /* A catalog that cannot be read shows no note (#5562 review r1 L2) and leaves the common reach; the next tab or range change asks again. */
    if (picker === rangePicker) picker.setSampleInterval(null);
  });
}

/** The length of the range the reader holds for the server on screen, in milliseconds (the preset, or the custom range's span). */
function heldSpanMs(nowMs = Date.now()) {
  const custom = current.server ? customRanges.get(current.server) : null;
  const resolved = custom ? resolveSpec(custom.spec, nowMs, browserZone()) : null;
  return resolved && resolved.ok ? resolved.range.spanMs : pageHours * HOUR_MS;
}

/* This server's fleet card, plus the reason sentence the fleet's worst-first ranking computed for it. ONE
 * /api/fleet read serves both jobs the page has for it — the header's band and reason, and the engine the tab
 * set is chosen by — because two callers of the same endpoint on one page render is a second request nobody
 * asked for.
 *
 * `onCard` is ALWAYS called, card or not. A fleet read that failed, and a server name the fleet does not carry,
 * still have to get tabs; a null card is "no engine claim", which serverTabsFor answers with the SQL Server
 * registry, exactly as this page behaved before it could ask. */
function loadServerCard(server, onCard) {
  (async () => {
    const res = await apiGetFleet();
    if (res.kind !== "data") return onCard(null, null);

    const matches = (c) => c.server_name === server || c.display_name === server;
    const card = (res.data.cards || []).find(matches) || null;
    /* The reason belongs to the RANKING, not to the card, so it travels as its own value rather than being
       stapled onto the card object — and a server outside the ranking has none, which stays a null here
       rather than becoming a sentence invented in the browser. */
    const ranked = (res.data.worst_servers || []).find((w) => w.display_name === server);
    onCard(card, ranked && ranked.reason ? ranked.reason : null);
  })();
}

/* The server header's title, status dot, band badge, engine badge and the WHY beneath them, all from the card above.
 *
 * The band word alone is not an answer. `Warning` has three unrelated causes — a genuine metric breach, a server
 * awaiting its first collection, and a collector error — so a badge reading "Warning" with no way to ask why is
 * the #2422 report rebuilt on a new surface, which is exactly what #2429 fixed on the desktop by attaching the
 * card's own metric rows. Nothing shown here is derived in the browser (R1): the per-metric severity chips are
 * rendered by fleet.js's own metricBands so there is one implementation rather than two, the reason is the same
 * sentence the fleet page shows, and the engine is the token the store recorded. A server outside the ranking
 * gets the chips and no sentence, because inventing one here is the second derivation this comment refuses. */
function fillServerHead(title, dot, badgeSlot, engineSlot, whySlot, card, reason) {
  if (!card) return;

  /* The title names the server the way the sidebar, the fleet cards and every other page do: by display name.
     The route, the sidebar link and every /api/read call keep the KEY (for an Azure SQL Database that is
     "host:database"), so only the words on screen change. A card with no display name leaves the key already
     in the heading. */
  if (card.display_name) title.textContent = card.display_name;

  dot.className = "dot " + bandClass(card.band);
  mount(
    badgeSlot,
    el("span", { class: "badge " + bandClass(card.band), text: card.status || card.band, title: reason || null })
  );

  /* The engine badge is the answer to "why does this server have six tabs and that one twelve". Only a card
     that made a claim gets one: an unstamped engine renders no badge rather than one reading "SQL Server",
     because the tabs it gets are a default, not a finding — which is why the SERVER sends null there rather
     than a description of the absence. The wording is MonitoredEngineKind's, not this file's (R1). */
  if (card.engine_description) {
    mount(engineSlot, [el("span", { class: "badge engine", text: card.engine_description })]);
  }

  mount(whySlot, [
    reason ? el("div", { class: "server-reason " + bandClass(card.band), text: reason }) : null,
    metricBands(card),
  ]);
}
