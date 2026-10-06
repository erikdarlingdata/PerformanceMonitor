/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Stored plan viewer (#4843): a "Plan" action on a grid row. It reads the row's stored showplan XML and shows it,
   indented, in a panel under the button, with Copy and Download .sqlplan. The XML is only ever drawn as text
   (textContent of a <pre>), never as markup.

   A row names its plan with a SOURCE (#5228): { kind, ...the row's own key }. SOURCES says, per kind, which read
   answers it and how the key travels. The plan is fetched when the button is clicked and never carried in the
   grid payload, which keeps the pages small. Key values go to the read exactly as the row holds them: a
   timestamp is matched for equality in the store, so it is never put through a Date.

   Which plans are open, and what they came back with, live at MODULE scope keyed by server + source, so the
   page's 60 s rebuild draws the same panels open again instead of closing them. A read still in flight when its
   cell is rebuilt reports to the new cell as well as the old one. */

import { el, readTool } from "../util.js";
import { copyText } from "../grid-tools.js";

const NO_PLAN = "No stored plan was found for this query.";
const TRUNCATED_NOTE =
  "The stored plan is larger than the 500 KB the read returns, so it is cut off here. A cut-off plan will not open " +
  "in a plan viewer, so Download is turned off; Copy still copies what is shown.";

/* key -> { phase: "loading" | "done", result: { kind: "plan" | "none" | "error", ... } }. */
const openPlans = new Map();
/* key -> Set of redraw functions, one per cell currently showing the key. */
const views = new Map();
/* The grid build a cell belongs to. Each plan column factory below runs once per grid build, so each call starts a new
   generation, and a cell stamps its draw with the generation it registers under. `sweptGeneration` is the last one swept
   (see dropOlderDetached). */
let generation = 0;
let sweptGeneration = 0;

/* The plan sources. `read` is the tool; `params(source)` is its query (empty values are dropped by the read call);
   `stem(source)` names a download. `query_hash` keeps its original panel key (server|database|hash) so a panel open
   before this change stays the same panel. */
const SOURCES = {
  query_hash: {
    read: "get_plan_xml",
    params: (s) => ({ query_hash: s.query_hash, database_name: s.database_name || null }),
    key: (s) => [s.database_name || "", s.query_hash],
    stem: (s) => s.query_hash,
  },
  active_snapshot: {
    read: "get_active_query_plan_xml",
    params: (s) => ({
      collection_time: s.collection_time,
      session_id: s.session_id,
      request_id: s.request_id == null ? 0 : s.request_id,
      live: s.live ? "true" : null,
    }),
    key: (s) => [s.collection_time, s.session_id, s.request_id == null ? 0 : s.request_id, s.live ? "live" : "est"],
    stem: (s) => "active-" + s.session_id + "-" + s.collection_time + (s.live ? "-live" : ""),
  },
  query_store: {
    read: "get_query_store_plan_xml",
    params: (s) => ({ database_name: s.database_name, query_id: s.query_id, plan_id: s.plan_id == null ? null : s.plan_id }),
    key: (s) => [s.database_name, s.query_id, s.plan_id == null ? "" : s.plan_id],
    stem: (s) => "qs-" + s.database_name + "-" + s.query_id + (s.plan_id == null ? "" : "-" + s.plan_id),
  },
  procedure: {
    read: "get_procedure_plan_xml",
    params: (s) => ({ sql_handle: s.sql_handle }),
    key: (s) => [s.sql_handle],
    stem: (s) => s.sql_handle,
  },
  /* A blocked process report row (#5236) names the plan of one of its two sides by the report's event time (the XE
     stamp, as the row holds it), both sessions' ids and ecids, and `side`. The blocked side is the read's default, so
     it sends no `side`; a row's blocked and blocking plans are two panels. A missing ecid reads as 0, as the tool does.
     The row's database goes with it: a server that collects several databases (an Azure master target) numbers sessions
     per database, so two databases' reports can share every other part of the key. The database is in the panel key too,
     so their rows never share a panel; a row without one sends none and keys with an empty slot. */
  blocking: {
    read: "get_blocking_plan_xml",
    params: (s) => ({
      event_time: s.event_time,
      blocked_spid: s.blocked_spid,
      blocked_ecid: s.blocked_ecid == null ? 0 : s.blocked_ecid,
      blocking_spid: s.blocking_spid,
      blocking_ecid: s.blocking_ecid == null ? 0 : s.blocking_ecid,
      side: s.side === "blocking" ? "blocking" : null,
      database_name: s.database_name || null,
    }),
    key: (s) => [
      s.database_name || "",
      s.event_time,
      s.blocked_spid,
      s.blocked_ecid == null ? 0 : s.blocked_ecid,
      s.blocking_spid,
      s.blocking_ecid == null ? 0 : s.blocking_ecid,
      s.side === "blocking" ? "blocking" : "blocked",
    ],
    stem: (s) => (s.side === "blocking" ? "blocking-" + s.blocking_spid : "blocked-" + s.blocked_spid) + "-" + s.event_time,
  },
  /* A deadlock row's victim plan (#5236), by the row's collection and deadlock times. Neither stamp is unique on its own
     (one monitor pass can report two deadlocks with the same millisecond), so the victim's process id narrows it; a row
     without one reads by the two times alone, and the read refuses to pick when two deadlocks that name different victims
     match. The row's database narrows it too, and keys the panel, for the same reason as a blocking row's. The download
     name carries the victim, so two deadlocks with the same stamps do not download under one file name. */
  deadlock_victim: {
    read: "get_deadlock_plan_xml",
    params: (s) => ({
      collection_time: s.collection_time,
      deadlock_time: s.deadlock_time,
      victim_process_id: s.victim_process_id || null,
      database_name: s.database_name || null,
    }),
    key: (s) => [s.database_name || "", s.collection_time, s.deadlock_time, s.victim_process_id || ""],
    stem: (s) => "deadlock-victim-" + s.deadlock_time + (s.victim_process_id ? "-" + s.victim_process_id : ""),
  },
};

/* A panel's key: server, then the kind (left out for query_hash, whose key never had one), then the kind's own parts, then
   the source's `scope` when it has one. The kind in the key is what keeps two sources with equal-looking parts from sharing
   a panel. The scope does the same for one plan opened from two places: the Query Store History table names its own panel
   as the scope, so its Plan button opens its own cell and not the grid's Plan cell for that plan (#5234). The scope is
   never sent to the read. */
const keyOf = (server, source) => {
  const parts = SOURCES[source.kind].key(source);
  const scope = source.scope == null ? [] : ["@scope", source.scope];
  return [server, ...(source.kind === "query_hash" ? [] : ["@" + source.kind]), ...parts, ...scope].join("|");
};

/** The state of every open panel, for tests: a copy, so the caller cannot change the module's. */
export function openPlanKeys() {
  return [...openPlans.keys()];
}

/** Forgets every open panel. Tests only; the page never needs it. */
export function resetPlanViewer() {
  openPlans.clear();
  views.clear();
}

/** How many cells are registered to redraw for each key, as { key: count }, for tests. */
export function planViewerViewCounts() {
  return Object.fromEntries([...views].map(([key, set]) => [key, set.size]));
}

/* Splits XML text into tags (quote-aware, so a ">" inside an attribute value such as StatementText does not end the
   tag), comments, CDATA, processing instructions and text. Nothing is parsed into nodes and nothing is evaluated. */
const TOKEN = /<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|<\?[\s\S]*?\?>|<(?:[^>"']|"[^"]*"|'[^']*')*>|[^<]+/g;

/**
 * Indents XML text two spaces per level, one tag per line. An element whose only content is text stays on one line.
 * Returns text; text that is not XML comes back as it was. The result is for display and copy only.
 */
export function prettyPrintXml(xml) {
  const text = String(xml ?? "");
  const tokens = text.match(TOKEN);
  if (!tokens || !text.trimStart().startsWith("<")) return text;
  const lines = [];
  let depth = 0;
  const push = (s) => lines.push("  ".repeat(Math.max(depth, 0)) + s);
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t[0] !== "<") {
      if (t.trim()) push(t.trim());
      continue;
    }
    if (t.startsWith("</")) {
      depth--;
      push(t);
    } else if (t.startsWith("<!") || t.startsWith("<?") || t.endsWith("/>")) {
      push(t);
    } else {
      /* An open tag, then text, then its close tag: one line. */
      const body = tokens[i + 1];
      const close = tokens[i + 2];
      if (body && body[0] !== "<" && body.trim() && close && close.startsWith("</")) {
        push(t + body.trim() + close);
        i += 2;
      } else {
        push(t);
        depth++;
      }
    }
  }
  return lines.join("\n");
}

/** The file name a plan downloads as: the source's stem (the query hash for a stored query), made safe for a file
 *  name, plus .sqlplan. */
export function planFileName(stem) {
  return String(stem).replace(/[^A-Za-z0-9._-]/g, "_") + ".sqlplan";
}

const REVOKE_AFTER_MS = 10000;

/** Hands the plan XML to the browser as a .sqlplan file. */
export function downloadPlan(queryHash, xml) {
  const blob = new Blob([xml], { type: "application/xml" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = planFileName(queryHash);
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), REVOKE_AFTER_MS);
}

function redraw(key) {
  const set = views.get(key);
  if (!set) return;
  for (const draw of [...set]) {
    if (draw.host && draw.host.isConnected === false) set.delete(draw);
    else draw();
  }
  if (set.size === 0) views.delete(key);
}

/* Runs when the first cell of a generation registers: every draw of an OLDER generation whose cell has left the page goes,
   under whatever key. redraw() prunes only the key it is called for, and a row nobody clicks (or one that has left the grid)
   never reaches it, so without this each rebuild would leave a cell per row in the set for the life of the page (#5234).
   The generation is what makes this safe: the cells of the build in progress are not on the page yet (the grid is attached
   after its rows are drawn) and are never dropped here, and a cell of an older build that is still on the page stays too. */
const dropOlderDetached = () => {
  for (const [key, set] of views) {
    for (const draw of [...set]) {
      if (draw.generation < generation && draw.host && draw.host.isConnected === false) set.delete(draw);
    }
    if (set.size === 0) views.delete(key);
  }
};

/* What every plan read answers (the web route's wrapper around the DarlingMcpPlanTools reads): JSON { <the key it was
   read by>, plan_xml, truncated } for a stored plan (`truncated` is decided on the server), or a status envelope
   ("unavailable" / "not_collected") when there is no plan, which arrives as kind "empty". */

/** Turns a read result into { kind: "plan" | "none" | "error", ... }, or null when the read was abandoned. */
export function classifyPlanRead(res) {
  if (!res || res.kind === "aborted" || res.kind === "auth") return null;
  const text = typeof res.message === "string" ? res.message : "";
  if (res.kind === "data" && res.data && typeof res.data.plan_xml === "string") {
    return { kind: "plan", xml: res.data.plan_xml, truncated: res.data.truncated === true };
  }
  if (res.kind === "error") return { kind: "error", message: text || "The plan could not be read." };
  if (res.kind === "empty") return { kind: "none", message: text || NO_PLAN };
  return { kind: "none", message: NO_PLAN };
}

async function load(key, server, source) {
  const spec = SOURCES[source.kind];
  let res;
  try {
    res = await readTool(spec.read, { server, ...spec.params(source) });
  } catch (e) {
    res = { kind: "error", message: e && e.message ? e.message : String(e) };
  }
  if (!openPlans.has(key)) return; // closed while the read was out
  const outcome = classifyPlanRead(res);
  if (outcome === null) openPlans.delete(key);
  else openPlans.set(key, { phase: "done", result: outcome });
  redraw(key);
}

/** Opens the plan panel for a source, or closes it if it is already open. */
export function openPlanSource(server, source) {
  const key = keyOf(server, source);
  if (openPlans.has(key)) {
    openPlans.delete(key);
    redraw(key);
    return;
  }
  openPlans.set(key, { phase: "loading" });
  redraw(key);
  return load(key, server, source);
}

/** Opens the stored-plan panel for a query, or closes it if it is already open. */
export function openStoredPlan(server, queryHash, database) {
  return openPlanSource(server, { kind: "query_hash", query_hash: queryHash, database_name: database || null });
}

function panelFor(key, stem) {
  const state = openPlans.get(key);
  const status = el("span", { class: "grid-tools-status", role: "status", "aria-live": "polite" });
  if (state.phase === "loading") return el("div", { class: "plan-panel" }, [el("div", { class: "strip loading", text: "Loading the stored plan..." })]);
  const r = state.result;
  if (r.kind !== "plan") {
    return el("div", { class: "plan-panel" }, [el("div", { class: r.kind === "error" ? "strip error" : "strip empty", text: r.message })]);
  }
  const copy = el("button", { type: "button", class: "grid-tool", text: "Copy" });
  const pretty = prettyPrintXml(r.xml);
  copy.addEventListener("click", async () => {
    const out = await copyText(pretty);
    status.textContent = out.message;
  });
  const download = el("button", { type: "button", class: "grid-tool", text: "Download .sqlplan" });
  if (r.truncated) {
    download.disabled = true;
    download.setAttribute("disabled", "");
  } else {
    download.addEventListener("click", () => {
      try {
        downloadPlan(stem, r.xml);
        status.textContent = "Downloaded " + planFileName(stem) + ".";
      } catch (e) {
        status.textContent = "Download failed: " + (e && e.message ? e.message : "the browser refused it.");
      }
    });
  }
  return el("div", { class: "plan-panel" }, [
    el("div", { class: "grid-tools" }, [copy, download, status]),
    r.truncated ? el("div", { class: "strip notice", text: TRUNCATED_NOTE }) : null,
    el("pre", { class: "code plan-xml", text: pretty }),
  ]);
}

/**
 * The Plan cell for a plan source: a button, and the panel under it while that source's plan is open. A null source
 * (a row that has no key to read by) is a dash. `label` is the button text (default "Plan"); `title` its tooltip.
 */
export function planSourceCell(server, source, label, title) {
  if (!source) return document.createTextNode("—");
  const spec = SOURCES[source.kind];
  const key = keyOf(server, source);
  const stem = spec.stem(source);
  const text = label || "Plan";
  const host = el("div", { class: "plan-cell" });
  const draw = () => {
    while (host.firstChild) host.removeChild(host.firstChild);
    const isOpen = openPlans.has(key);
    const button = el("button", {
      type: "button",
      class: "grid-tool",
      text: isOpen ? "Hide plan" : text,
      "aria-expanded": isOpen ? "true" : "false",
      title: title || "Show the stored plan for this row",
    });
    button.addEventListener("click", () => openPlanSource(server, source));
    host.appendChild(button);
    if (isOpen) host.appendChild(panelFor(key, stem));
  };
  draw.host = host;
  draw.generation = generation;
  if (sweptGeneration !== generation) {
    sweptGeneration = generation;
    dropOlderDetached();
  }
  if (!views.has(key)) views.set(key, new Set());
  views.get(key).add(draw);
  draw();
  return host;
}

/**
 * The Plan cell for a grid row: a button when the row carries a query_hash, and the panel under it while that
 * query's plan is open. Nothing for a row without a hash.
 */
export function storedPlanCell(server, row) {
  const hash = row && row.query_hash;
  if (hash == null || hash === "") return planSourceCell(server, null);
  return planSourceCell(server, { kind: "query_hash", query_hash: hash, database_name: row.database_name || null }, "Plan", "Show the stored plan for this query");
}

/* A column's key decides whether `hideWhenEmpty` keeps it: the column is dropped when no row has a value at its key
   (0 and false count as values). Each factory below keys on the field that is present exactly when a button can be
   drawn, so the column shows when some row has a plan and is hidden when none does, and no two plan columns on one
   grid share a key. */

/** Active Queries: an "Estimated plan" and a "Live plan" column, each gated on the row's own presence flag (the flags
 *  arrive only when true, so a row without one gets a dash). */
export function activePlanColumns(server) {
  generation += 1;
  const source = (row, live) => ({
    kind: "active_snapshot",
    collection_time: row.collection_time,
    session_id: row.session_id,
    request_id: row.request_id == null ? 0 : row.request_id,
    live,
  });
  return [
    {
      key: "has_query_plan",
      label: "Plan",
      render: (row) =>
        planSourceCell(server, row && row.has_query_plan === true ? source(row, false) : null, "Plan", "Show the estimated plan captured with this request"),
      hideWhenEmpty: true,
      sortable: false,
      filter: false,
      csv: false,
      copy: false,
    },
    {
      key: "has_live_query_plan",
      label: "Live plan",
      render: (row) =>
        planSourceCell(server, row && row.has_live_query_plan === true ? source(row, true) : null, "Live plan", "Show the live plan captured with this request"),
      hideWhenEmpty: true,
      sortable: false,
      filter: false,
      csv: false,
      copy: false,
    },
  ];
}

/** Query Store: a plan button keyed by database, query and plan. Keyed on query_id, which every row has, so the
 *  column always shows; a query with no stored plan answers "No stored plan" in the panel. */
export function queryStorePlanColumn(server) {
  generation += 1;
  return {
    key: "query_id",
    label: "Plan",
    render: (row) =>
      planSourceCell(
        server,
        row && row.database_name != null && row.query_id != null
          ? { kind: "query_store", database_name: row.database_name, query_id: row.query_id, plan_id: row.plan_id == null ? null : row.plan_id }
          : null,
        "Plan",
        "Show the stored Query Store plan for this query"
      ),
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}

/** Top Procedures: a plan button keyed by sql_handle. Hidden when no row has one (the hourly tier carries none). */
export function procedurePlanColumn(server) {
  generation += 1;
  return {
    key: "sql_handle",
    label: "Plan",
    render: (row) =>
      planSourceCell(server, row && row.sql_handle ? { kind: "procedure", sql_handle: row.sql_handle } : null, "Plan", "Show the stored plan for this procedure"),
    hideWhenEmpty: true,
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}

/** Blocking (#5236): a "Blocked plan" and a "Blocking plan" column, each gated on the row's own presence flag. The flags
 *  arrive only when true (a report keeps a plan only when the statement was still in the plan cache, and a DMV row has
 *  none), so a row without one gets a dash and a grid with none drops the column. A row also needs the event time and both
 *  session ids the read keys on; without them there is nothing to read by, so it is a dash too. The row's database_name
 *  goes along when it has one, so a server that collects several databases reads each row's own. */
export function blockingPlanColumns(server) {
  const source = (row, side) => ({
    kind: "blocking",
    event_time: row.event_time,
    blocked_spid: row.blocked_spid,
    blocked_ecid: row.blocked_ecid == null ? 0 : row.blocked_ecid,
    blocking_spid: row.blocking_spid,
    blocking_ecid: row.blocking_ecid == null ? 0 : row.blocking_ecid,
    database_name: row.database_name || null,
    side,
  });
  const keyed = (row) => row.event_time != null && row.event_time !== "" && row.blocked_spid != null && row.blocking_spid != null;
  return [
    {
      key: "has_blocked_plan",
      label: "Blocked plan",
      render: (row) =>
        planSourceCell(server, row && row.has_blocked_plan === true && keyed(row) ? source(row, "blocked") : null, "Blocked plan", "Show the plan captured for the blocked statement"),
      hideWhenEmpty: true,
      sortable: false,
      filter: false,
      csv: false,
      copy: false,
    },
    {
      key: "has_blocking_plan",
      label: "Blocking plan",
      render: (row) =>
        planSourceCell(server, row && row.has_blocking_plan === true && keyed(row) ? source(row, "blocking") : null, "Blocking plan", "Show the plan captured for the blocking statement"),
      hideWhenEmpty: true,
      sortable: false,
      filter: false,
      csv: false,
      copy: false,
    },
  ];
}

/** Deadlocks (#5236): a "Victim plan" column gated on the row's presence flag (sent only when true, like the blocking
 *  flags). The read keys on the row's collection and deadlock times, with the victim's process id and the database_name
 *  when the row has them; a row missing either time is a dash. */
export function deadlockPlanColumn(server) {
  return {
    key: "has_victim_plan",
    label: "Victim plan",
    render: (row) =>
      planSourceCell(
        server,
        row && row.has_victim_plan === true && row.collection_time != null && row.collection_time !== "" && row.deadlock_time != null && row.deadlock_time !== ""
          ? {
              kind: "deadlock_victim",
              collection_time: row.collection_time,
              deadlock_time: row.deadlock_time,
              victim_process_id: row.victim_process_id || null,
              database_name: row.database_name || null,
            }
          : null,
        "Victim plan",
        "Show the plan captured for the deadlock victim"
      ),
    hideWhenEmpty: true,
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}

/** The grid column every plan-capable grid adds: hidden when no row carries a query_hash. */
export function planColumn(server) {
  generation += 1;
  return {
    key: "query_hash",
    label: "Plan",
    render: (row) => storedPlanCell(server, row),
    hideWhenEmpty: true,
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}
