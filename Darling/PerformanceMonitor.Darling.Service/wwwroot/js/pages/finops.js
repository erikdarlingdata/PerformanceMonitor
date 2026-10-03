/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * FinOps page: a server picker, a sub-tab strip and a body. The page is the web port of the desktop viewer's
 * FinOps tab. Route: #/finops, #/finops/{server} and #/finops/{server}/{tab}; the server and the tab id ride in
 * the hash so a view is deep-linkable and survives the shell poll, which re-renders the whole route.
 *
 * Each sub-tab lives in its own file under pages/finops/ and exports `tab = { id, label, build(server, ctx) }`.
 * The registry below is the one place the twelve are named, in the desktop tab strip's order, so a tab's own
 * file is the only thing a tab ever changes. The browser derives no band, score or percentage threshold: a tab
 * shows what the read returned.
 *
 * The server choice is also kept in localStorage ("finops.server") for a bare #/finops visit. Server Inventory is
 * fleet-wide: the picker stays visible on that tab and its build ignores the choice.
 */

import { el, mount, readTool, loadingStrip, errorStrip, emptyStrip } from "../util.js";
import { setPanelSignal } from "../panels.js";
import { tab as utilization } from "./finops/utilization.js";
import { tab as database_resources } from "./finops/database-resources.js";
import { tab as storage_growth } from "./finops/storage-growth.js";
import { tab as locking } from "./finops/locking.js";
import { tab as database_sizes } from "./finops/database-sizes.js";
import { tab as version_store } from "./finops/version-store.js";
import { tab as optimization } from "./finops/optimization.js";
import { tab as high_impact } from "./finops/high-impact.js";
import { tab as application_connections } from "./finops/application-connections.js";
import { tab as server_inventory } from "./finops/server-inventory.js";
import { tab as index_analysis } from "./finops/index-analysis.js";
import { tab as recommendations } from "./finops/recommendations.js";

/** The sub-tabs, in the desktop FinOps tab strip's order. */
export const FINOPS_TABS = [
  utilization,
  database_resources,
  storage_growth,
  locking,
  database_sizes,
  version_store,
  optimization,
  high_impact,
  application_connections,
  server_inventory,
  index_analysis,
  recommendations,
];

const SERVER_KEY = "finops.server";
const TAB_KEY = "finops.tab";

let renderGeneration = 0;
let panelAbort = null;

function storedGet(key) {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function storedSet(key, value) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* storage unavailable: the hash still carries the choice */
  }
}

function findTab(id) {
  return FINOPS_TABS.find((t) => t.id === id) || FINOPS_TABS[0];
}

function hashFor(server, tabId) {
  return "#/finops/" + encodeURIComponent(server) + "/" + tabId;
}

/** The server rows list_servers returned, whichever envelope shape carried them. */
function serverRows(data) {
  if (Array.isArray(data)) return data;
  if (data && Array.isArray(data.servers)) return data.servers;
  return [];
}

function tabBar(server, active) {
  return el(
    "nav",
    { class: "subtabs finops-tabs", role: "tablist", "aria-label": "FinOps sections" },
    FINOPS_TABS.map((t) =>
      el("a", {
        class: "subtab" + (t.id === active.id ? " active" : ""),
        href: hashFor(server, t.id),
        role: "tab",
        "aria-selected": t.id === active.id ? "true" : "false",
        text: t.label,
      })
    )
  );
}

function serverPicker(rows, server, tabId) {
  const sel = el(
    "select",
    { class: "range-select-inline finops-server-select", "aria-label": "Server" },
    rows.map((r) => el("option", { value: r.server_name, text: r.display_name || r.server_name }))
  );
  sel.value = server;
  sel.addEventListener("change", () => {
    storedSet(SERVER_KEY, sel.value);
    location.hash = hashFor(sel.value, tabId);
  });
  return el("label", { class: "range-control" }, [el("span", { text: "Server" }), sel]);
}

/**
 * @param {HTMLElement} main
 * @param {string} [server] server key from the hash; falls back to the stored choice, then the first server
 * @param {string} [tabId] sub-tab id from the hash; an unknown or absent id resolves to the first tab
 * @param {object} [opts] `{ poll: true }` when this call is the shell poll's refresh (see renderServer)
 */
export function renderFinops(main, server, tabId, opts) {
  const generation = ++renderGeneration;
  const active = findTab(tabId || storedGet(TAB_KEY));
  const head = el("div", { class: "page-head" }, [el("h2", { text: "FinOps" })]);
  const body = el("div", { class: "finops-body" });
  mount(main, [head, body]);
  mount(body, loadingStrip("Loading servers"));

  (async () => {
    const res = await readTool("list_servers", {});
    if (generation !== renderGeneration) return;
    if (res.kind === "error") return mount(body, errorStrip(res.message));
    const rows = res.kind === "data" ? serverRows(res.data) : [];
    if (rows.length === 0) return mount(body, emptyStrip("No servers are registered yet."));

    const known = (name) => rows.some((r) => r.server_name === name || r.display_name === name);
    const wanted = server || storedGet(SERVER_KEY);
    const chosen = known(wanted) ? wanted : rows[0].server_name;
    storedSet(SERVER_KEY, chosen);
    storedSet(TAB_KEY, active.id);

    head.appendChild(el("div", { class: "spacer" }));
    head.appendChild(serverPicker(rows, chosen, active.id));

    /* Every redraw replaces the tab body, so the previous build's pending reads are for nobody. */
    if (panelAbort) panelAbort.abort();
    panelAbort = new AbortController();
    setPanelSignal(panelAbort.signal);

    mount(body, [tabBar(chosen, active), el("div", { class: "finops-panel" }, [active.build(chosen, {})])]);
  })();
}
