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

import { el, mount, apiGetFleet, bandClass, loadingStrip, setActiveRange, localTime } from "../util.js";
import { setPanelSignal } from "../panels.js";
import { serverTabsFor, findServerTab, tabNote } from "./server-tabs.js";
import { metricBands } from "./fleet.js";

/** The page time range: the desktop viewers' presets, which stop at 7 days. All but three ranged reads on these
 *  tabs (the collection log, current waits and blocking stats) take at most McpHelpers.MaxHoursBack (168) hours, so
 *  a wider choice was never served: those panels asked again for 7 days and said so. Custom Views offer longer
 *  windows: their composed panels read the store directly, through rollups for the query tables.
 *  WebServerPageRangeTests runs every option through every tab of both registries. */
const RANGE_OPTIONS = [
  { hours: 1, label: "last hour" },
  { hours: 4, label: "last 4 hours" },
  { hours: 12, label: "last 12 hours" },
  { hours: 24, label: "last 24 hours" },
  { hours: 24 * 7, label: "last 7 days" },
];

/** The widest preset. A tab note that names the longest window this page shows (the Blocking tab's) is given it. */
const WIDEST_RANGE_HOURS = Math.max(...RANGE_OPTIONS.map((o) => o.hours));

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

/* The custom start/end, keyed by server (module scope, so the 60s poll's rebuild keeps it and another server starts on
   the presets). Each value is `{ startMs, endMs, live, spanMs }`. A range whose end is within LIVE_SLACK_MS of now when
   it is applied is LIVE: it keeps its width and slides to end at "now" on every rebuild, so the poll keeps refreshing
   it. Any other end is a fixed historical window: its data cannot change, so the poll leaves the panels on screen
   instead of reading the same hours again. */
const customRanges = new Map();
const LIVE_SLACK_MS = 60000;
const HOUR_MS = 3600000;

/**
 * Check a picked start and end and map them onto the reads' window (an end, `as_of`, and a whole number of `hours`).
 * The start rounds EARLIER to a whole hour for the fetch (`hours` is an integer of at least 1); the page trims what comes
 * back to the exact pair. Returns `{ error }` for an unusable pair, else `{ hours, asOf, live, startMs, endMs }`; `asOf`
 * is null for a live range, whose reads are anchored at the server's own clock.
 */
export function resolveCustomRange(startMs, endMs, nowMs) {
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return { error: "Enter both a start and an end." };
  if (endMs <= startMs) return { error: "The end must be after the start." };
  if (endMs > nowMs + LIVE_SLACK_MS) return { error: "The end cannot be in the future." };
  const span = endMs - startMs;
  if (span < HOUR_MS) return { error: "Pick at least one hour. Drag across a chart to look at a shorter span." };
  if (span > WIDEST_RANGE_HOURS * HOUR_MS) {
    return { error: "The range can be at most " + WIDEST_RANGE_HOURS / 24 + " days, the widest preset." };
  }
  const live = endMs >= nowMs - LIVE_SLACK_MS;
  return { hours: Math.ceil(span / HOUR_MS), asOf: live ? null : new Date(endMs).toISOString(), live, startMs, endMs };
}

/** Apply a custom range to a server and redraw. Returns the error text, or null when the range was taken. */
export function applyCustomRange(server, startMs, endMs, nowMs = Date.now()) {
  const r = resolveCustomRange(startMs, endMs, nowMs);
  if (r.error) return r.error;
  customRanges.set(server, { startMs, endMs, live: r.live, spanMs: endMs - startMs });
  redrawPanels();
  return null;
}

/** The {hours,label} context every tab build() is given. A custom range also hands the util module its exact pair, so
 *  every read of this server inside it is anchored and trimmed there. */
export function rangeContext(nowMs = Date.now()) {
  const custom = current.server ? customRanges.get(current.server) : null;
  if (custom) {
    const endMs = custom.live ? nowMs : custom.endMs;
    const startMs = custom.live ? nowMs - custom.spanMs : custom.startMs;
    const r = resolveCustomRange(startMs, endMs, nowMs);
    if (!r.error) {
      setActiveRange({ server: current.server, hours: r.hours, startMs, endMs, asOf: r.asOf });
      /* Totals and rankings are read over whole hours back from the end, so they can begin earlier than the picked start:
         when the span is not a whole number of hours the label says where they begin. */
      const aggregateFrom = endMs - r.hours * HOUR_MS;
      const rounded = aggregateFrom < startMs ? "; totals and rankings aggregate from " + localTime(new Date(aggregateFrom).toISOString()) : "";
      return { hours: r.hours, label: "custom: " + localTime(new Date(startMs).toISOString()) + " to " + (custom.live ? "now" : localTime(new Date(endMs).toISOString())) + rounded, custom: true };
    }
    customRanges.delete(current.server);
  }
  setActiveRange(null);
  const opt = RANGE_OPTIONS.find((o) => o.hours === pageHours) || RANGE_OPTIONS[3];
  return { hours: opt.hours, label: opt.label };
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
  const generation = ++renderGeneration;
  const custom = customRanges.get(server);
  /* A fixed custom range cannot change under the poll, so the poll keeps the panels it already drew (same server, same
     tab) instead of reading the same hours again. A live range, a preset and a tab or server click rebuild as before. */
  const keep = isPoll && !!custom && !custom.live && !!gridNode && gridKey === server + "|" + (tabId || "");
  keepGrid = false;
  current = { server, tab: null };

  const dot = el("span", { class: "dot" });
  /* The route's server KEY until the fleet card is known; fillServerHead then swaps in the display name. */
  const title = el("h2", { text: server });
  const badgeSlot = el("span", { class: "server-band" });
  const engineSlot = el("span", { class: "server-engine" });
  const whySlot = el("div", { class: "server-why" });
  const head = el("div", { class: "page-head" }, [
    el("a", { href: "#/fleet", text: "← Fleet" }),
    el("span", { class: "server-title" }, [dot, title]),
    badgeSlot,
    engineSlot,
    el("div", { class: "spacer" }),
    rangeControl(),
  ]);

  /* The bar and the note share one slot because both are decided by the same card. */
  const tabsSlot = el("div", { class: "subtabs-slot" }, [loadingStrip()]);
  if (!keep) gridNode = el("div", { class: "panel-grid" });
  mount(main, [head, whySlot, tabsSlot, gridNode]);

  /* Seen this server before? Then its engine is already known and the page paints now — no loading strip, and
     the tab's panels start fetching in this tick, which is what keeps the 60s poll and a sub-tab click feeling
     like they did before the tab set had a question to answer. */
  const remembered = lastCard.get(server);
  let painted = null;
  if (remembered) {
    fillServerHead(title, dot, badgeSlot, engineSlot, whySlot, remembered.card, remembered.reason);
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
  mount(tabsSlot, [subtabBar(server, tab, tabs), tabNote(tab, WIDEST_RANGE_HOURS)]);
  redrawPanels();
  return tabs;
}

/** (Re)fill the panel grid for the current server + tab at the current range. No refetch of anything else. */
function redrawPanels() {
  if (!gridNode || !current.tab || !current.server) return;

  if (keepGrid) {
    keepGrid = false;
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

  mount(gridNode, current.tab.build(current.server, rangeContext()));
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

/** The time-range picker: the presets, and "Custom…" for a start and end. Changing it redraws the panels in place, exactly
 *  like the fleet page's sort. The start and end are typed in the browser's zone, the zone every time on this page uses. */
function rangeControl() {
  const CUSTOM = "custom";
  const server = current.server;
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": "Time range" },
    [...RANGE_OPTIONS.map((o) => el("option", { value: String(o.hours), text: o.label })), el("option", { value: CUSTOM, text: "Custom…" })]
  );
  const saved = customRanges.get(server);
  sel.value = saved ? CUSTOM : String(pageHours);

  const input = (label, ms) => {
    const box = el("input", { type: "datetime-local", class: "range-custom-input", "aria-label": label, step: "60" });
    if (ms != null) box.value = localInputValue(ms);
    return box;
  };
  const start = input("Range start", saved ? saved.startMs : null);
  const end = input("Range end", saved ? (saved.live ? Date.now() : saved.endMs) : null);
  /* An end the form filled in as "now" stays "now" however long the form sits open: Apply then reads the clock again.
     Typing in the end box makes it the reader's own. */
  let endIsNow = !saved || saved.live;
  end.addEventListener("input", () => { endIsNow = false; });
  const message = el("span", { class: "range-custom-error", role: "alert" });
  const apply = el("button", { type: "button", class: "btn range-custom-apply", text: "Apply" });
  const form = el("span", { class: "range-custom" }, [start, el("span", { text: "to" }), end, apply, message]);
  form.hidden = sel.value !== CUSTOM;

  apply.addEventListener("click", () => {
    const err = applyCustomRange(server, new Date(start.value).getTime(), endIsNow ? Date.now() : new Date(end.value).getTime());
    message.textContent = err || "";
  });
  sel.addEventListener("change", () => {
    if (sel.value === CUSTOM) {
      form.hidden = false;
      if (!end.value) {
        end.value = localInputValue(Date.now());
        endIsNow = true;
      }
      if (!start.value) start.value = localInputValue(Date.now() - 24 * HOUR_MS);
      return;
    }
    form.hidden = true;
    message.textContent = "";
    customRanges.delete(server);
    pageHours = Number(sel.value) || 24;
    redrawPanels();
  });
  return el("div", { class: "range-control" }, [el("span", { text: "Range" }), sel, form]);
}

/* A UTC-epoch instant as a datetime-local value in the browser's zone ("2026-01-02T03:04"). */
function localInputValue(ms) {
  const d = new Date(ms - new Date(ms).getTimezoneOffset() * 60000);
  return d.toISOString().slice(0, 16);
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
