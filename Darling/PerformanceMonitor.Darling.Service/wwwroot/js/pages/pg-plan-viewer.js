/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* PostgreSQL captured-plan viewer (#5229): a "Plan" cell on a Captured Plans row that opens a panel showing that
   row's stored, redacted auto_explain JSON as text - pretty-printed, with Copy and Download .json.

   - The JSON is the row's OWN `plan` field from get_pg_plans. There is no second read, so there is no fetch here.
   - This is a separate module from plan-viewer.js on purpose: that one fetches showplan XML and offers .sqlplan;
     a PostgreSQL plan has no fetch, a different format and a different file type.
   - The summary line comes from this capture's own figures; other captures of the same shape may differ.
   - A Query Identifier inside the JSON is a 64-bit number that JSON.parse rounds in the browser. The grid's string
     queryid is the exact one; the panel says so.
   - XSS: everything is drawn through el() with text: or string children (textContent). Never any markup
     sink or parser. The plan is untrusted content.
 */

import { el } from "../util.js";
import { copyText, downloadText } from "../grid-tools.js";

const UNPARSED_NOTE = "This stored plan is not valid JSON (the log line may have been cut), so it is shown as stored and Download is off.";
const CAPTURE_NOTE = "This capture's figures; other captures of the same shape may differ.";
const QUERY_ID_NOTE = "Query ID is exact in the grid; a Query Identifier inside the JSON is a 64-bit number this page may round.";

/** Which plan panels are open, keyed server|queryid|plan_hash, so a 60 s rebuild keeps them open. */
const openPlans = new Set();

const keyOf = (server, row) => [server, row.queryid ?? "", row.plan_hash ?? ""].join("|");

export function resetPgPlanViewer() {
  openPlans.clear();
}

export function openPgPlanKeys() {
  return [...openPlans];
}

/** The plan as indented text. A string that parses is re-indented; one that does not is returned as stored. */
export function prettyPrintPgPlan(plan) {
  if (plan === null || plan === undefined) return "";
  if (typeof plan === "string") {
    try {
      return JSON.stringify(JSON.parse(plan), null, 2);
    } catch {
      return plan;
    }
  }
  return JSON.stringify(plan, null, 2);
}

const fmt = (n) => n.toLocaleString(undefined, { maximumFractionDigits: 2 });

/** [{ label, value }] for the figures the plan actually carries; a missing figure is left out, never shown as 0. */
export function pgPlanSummary(plan) {
  if (!plan || typeof plan !== "object" || !plan.Plan || typeof plan.Plan !== "object") return [];
  const root = plan.Plan;
  const wanted = [
    ["Total Cost", root["Total Cost"], ""],
    ["Plan Rows", root["Plan Rows"], ""],
    ["Actual Total Time", root["Actual Total Time"], " ms"],
    ["Actual Rows", root["Actual Rows"], ""],
    ["Planning Time", plan["Planning Time"], " ms"],
    ["Execution Time", plan["Execution Time"], " ms"],
  ];
  return wanted.filter(([, v]) => typeof v === "number").map(([label, v, unit]) => ({ label, value: fmt(v) + unit }));
}

export function pgPlanFileName(row) {
  const part = (v) => {
    const s = v === null || v === undefined || v === "" ? "unknown" : String(v);
    return s.replace(/[^A-Za-z0-9._-]/g, "_");
  };
  return "pg-plan-" + part(row.queryid) + "-" + part(row.plan_hash) + ".json";
}

function panelFor(row) {
  const plan = row.plan;
  const unparsed = typeof plan === "string";
  const pretty = prettyPrintPgPlan(plan);
  const status = el("span", { class: "grid-tools-status", role: "status", "aria-live": "polite" });

  const copy = el("button", { type: "button", class: "grid-tool", text: "Copy" });
  copy.addEventListener("click", async () => {
    const out = await copyText(pretty);
    status.textContent = out.message;
  });

  const download = el("button", { type: "button", class: "grid-tool", text: "Download .json" });
  if (unparsed) {
    download.disabled = true;
    download.setAttribute("disabled", "");
  } else {
    download.addEventListener("click", () => {
      try {
        downloadText(pgPlanFileName(row), [pretty], "application/json");
        status.textContent = "Downloaded " + pgPlanFileName(row) + ".";
      } catch (e) {
        status.textContent = "Download failed: " + (e && e.message ? e.message : "the browser refused it.");
      }
    });
  }

  const items = pgPlanSummary(plan);
  const summary = items.length > 0
    ? el("div", { class: "plan-summary" }, [
        ...items.map((i) => el("span", { text: i.label + ": " + i.value })),
        el("span", { class: "muted", text: CAPTURE_NOTE }),
      ])
    : null;
  const hasQueryIdentifier = !unparsed && plan && typeof plan === "object" && Object.prototype.hasOwnProperty.call(plan, "Query Identifier");

  return el("div", { class: "plan-panel" }, [
    el("div", { class: "grid-tools" }, [copy, download, status]),
    unparsed ? el("div", { class: "strip notice", text: UNPARSED_NOTE }) : null,
    summary,
    hasQueryIdentifier ? el("div", { class: "muted", text: QUERY_ID_NOTE }) : null,
    el("pre", { class: "code plan-json", text: pretty }),
  ]);
}

/** The Plan cell: a toggle button and, while open, the panel under it. "—" when the row carries no plan. */
export function pgPlanCell(server, row) {
  if (row.plan === null || row.plan === undefined || row.plan === "") return document.createTextNode("—");
  const key = keyOf(server, row);
  const host = el("div", { class: "plan-cell" });
  const draw = () => {
    while (host.firstChild) host.removeChild(host.firstChild);
    const open = openPlans.has(key);
    const button = el("button", { type: "button", class: "grid-tool", text: open ? "Hide plan" : "Plan" });
    button.setAttribute("aria-expanded", open ? "true" : "false");
    button.setAttribute("title", "Show the captured plan JSON");
    button.addEventListener("click", () => {
      if (openPlans.has(key)) openPlans.delete(key);
      else openPlans.add(key);
      draw();
    });
    host.appendChild(button);
    if (open) host.appendChild(panelFor(row));
  };
  draw();
  return host;
}

export function pgPlanColumn(server) {
  return { key: "plan", label: "Plan", render: (row) => pgPlanCell(server, row), hideWhenEmpty: true, sortable: false, csv: false, filter: false, copy: false };
}
