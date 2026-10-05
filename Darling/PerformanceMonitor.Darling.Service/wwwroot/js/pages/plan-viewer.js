/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Stored plan viewer (#4843): a "Plan" action on a grid row that carries a query_hash. It reads the query's stored
   showplan XML (get_plan_xml) and shows it, indented, in a panel under the button, with Copy and Download .sqlplan.
   The XML is only ever drawn as text (textContent of a <pre>), never as markup.

   Which plans are open, and what they came back with, live at MODULE scope keyed by server + database + hash, so the
   page's 60 s rebuild draws the same panels open again instead of closing them. A read still in flight when its
   cell is rebuilt reports to the new cell as well as the old one. */

import { el, readTool } from "../util.js";
import { copyText } from "../grid-tools.js";

const PLAN_READ = "get_plan_xml";
const NO_PLAN = "No stored plan was found for this query.";
const TRUNCATED_NOTE =
  "The stored plan is larger than the 500 KB the read returns, so it is cut off here. A cut-off plan will not open " +
  "in a plan viewer, so Download is turned off; Copy still copies what is shown.";

/* key -> { phase: "loading" | "done", result: { kind: "plan" | "none" | "error", ... } }. */
const openPlans = new Map();
/* key -> Set of redraw functions, one per cell currently showing the key. */
const views = new Map();

const keyOf = (server, hash, database) => [server, database || "", hash].join("|");

/** The state of every open panel, for tests: a copy, so the caller cannot change the module's. */
export function openPlanKeys() {
  return [...openPlans.keys()];
}

/** Forgets every open panel. Tests only; the page never needs it. */
export function resetPlanViewer() {
  openPlans.clear();
  views.clear();
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

/** The file name a plan downloads as: the query hash, made safe for a file name, plus .sqlplan. */
export function planFileName(queryHash) {
  return String(queryHash).replace(/[^A-Za-z0-9._-]/g, "_") + ".sqlplan";
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
}

/* What the read answers (the web route's wrapper around DarlingMcpPlanTools.GetPlanXml): JSON { query_hash,
   database_name, plan_xml, truncated } for a stored plan (`truncated` is decided on the server), or a status envelope
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

async function load(key, server, hash, database) {
  const params = { server, query_hash: hash };
  if (database) params.database_name = database;
  let res;
  try {
    res = await readTool(PLAN_READ, params);
  } catch (e) {
    res = { kind: "error", message: e && e.message ? e.message : String(e) };
  }
  if (!openPlans.has(key)) return; // closed while the read was out
  const outcome = classifyPlanRead(res);
  if (outcome === null) openPlans.delete(key);
  else openPlans.set(key, { phase: "done", result: outcome });
  redraw(key);
}

/** Opens the stored-plan panel for a query, or closes it if it is already open. */
export function openStoredPlan(server, queryHash, database) {
  const key = keyOf(server, queryHash, database);
  if (openPlans.has(key)) {
    openPlans.delete(key);
    redraw(key);
    return;
  }
  openPlans.set(key, { phase: "loading" });
  redraw(key);
  return load(key, server, queryHash, database);
}

function panelFor(key, queryHash) {
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
        downloadPlan(queryHash, r.xml);
        status.textContent = "Downloaded " + planFileName(queryHash) + ".";
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
 * The Plan cell for a grid row: a button when the row carries a query_hash, and the panel under it while that
 * query's plan is open. Nothing for a row without a hash.
 */
export function storedPlanCell(server, row) {
  const hash = row && row.query_hash;
  if (hash == null || hash === "") return document.createTextNode("—");
  const database = row.database_name || null;
  const key = keyOf(server, hash, database);
  const host = el("div", { class: "plan-cell" });
  const draw = () => {
    while (host.firstChild) host.removeChild(host.firstChild);
    const isOpen = openPlans.has(key);
    const button = el("button", {
      type: "button",
      class: "grid-tool",
      text: isOpen ? "Hide plan" : "Plan",
      "aria-expanded": isOpen ? "true" : "false",
      title: "Show the stored plan for this query",
    });
    button.addEventListener("click", () => openStoredPlan(server, hash, database));
    host.appendChild(button);
    if (isOpen) host.appendChild(panelFor(key, hash));
  };
  draw.host = host;
  if (!views.has(key)) views.set(key, new Set());
  views.get(key).add(draw);
  draw();
  return host;
}

/** The grid column every plan-capable grid adds: hidden when no row carries a query_hash. */
export function planColumn(server) {
  return {
    key: "query_hash",
    label: "Plan",
    render: (row) => storedPlanCell(server, row),
    hideWhenEmpty: true,
    sortable: false,
    csv: false,
  };
}
