/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Index Analysis" tab: get_finops view index_analysis, shown as the caveat notes, the reclaimable-space
   roll-up (the overall row first, then one row per database) and the cleanup recommendations.
   Rows stay in payload order: recommendations largest index first, databases by total maximum savings. At most LIMIT
   recommendations (500, the most the view takes, #5238) and the first databases come back, and the notice says so: when the
   read cut the list it names the limit and how many are not shown, and points at the Database box. Differences from the desktop grid:
   the overall row has no workload figures, so those cells show "—", and its Collected cell shows "—" too (the desktop leaves it blank); an average wait of none shows 0.00; script and
   definition text is cut by the read unless the "Full script and definition text" box is ticked; analyzer notes sit in a
   collapsed block; there are no tooltips. A Database box (any name; the list suggests the databases shown by the last
   unfiltered read) and that box re-read the view; the controls stay put and only the content below them is remounted.
   The choice is kept per server in module state, so it survives the page's poll rebuilds while the page stays open. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool, fmtInt, fmtNum } from "../../util.js";

// The most recommendations the view lists (#5238). The desktop grid shows every finding, so the tab asks for the ceiling, as the
// Database Sizes tab asks for its 500 files; the view's own default of 10 and the 50 of its other top-N views are not for this tab.
const LIMIT = 500;
const DERIVED_KEYS = ["reads_breakdown", "index_size_gb_text", "captured_at"];

const ROLLUP_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "tables_analyzed", label: "Tables", format: "int" },
  { key: "index_count", label: "Indexes", format: "int" },
  { key: "total_size_gb", label: "Size GB", format: "num2" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "indexes_to_disable", label: "To Disable", format: "int" },
  { key: "indexes_to_merge", label: "To Merge", format: "int" },
  { key: "compressable_indexes", label: "Compressable", format: "int" },
  { key: "unused_indexes", label: "Unused", format: "int" },
  { key: "unused_size_gb", label: "Disable GB", format: "num2" },
  { key: "compression_min_savings_gb", label: "Compress Min GB", format: "num2" },
  { key: "compression_max_savings_gb", label: "Compress Max GB", format: "num2" },
  { key: "total_min_savings_gb", label: "Total Min GB", format: "num2" },
  { key: "total_max_savings_gb", label: "Total Max GB", format: "num2" },
  { key: "reads_breakdown", label: "Reads" },
  { key: "writes", label: "Writes", format: "int" },
  { key: "lock_wait_count", label: "Lock Waits", format: "int" },
  { key: "avg_lock_wait_ms", label: "Avg Lock Wait ms", format: "num2" },
  { key: "latch_wait_count", label: "Latch Waits", format: "int" },
  { key: "avg_latch_wait_ms", label: "Avg Latch Wait ms", format: "num2" },
  { key: "captured_at", label: "Collected", format: "time" },
];

const RECOMMENDATION_COLUMNS = [
  { key: "action", label: "Action" },
  { key: "result_kind", label: "Result Kind" },
  { key: "consolidation_rule", label: "Rule" },
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_size_gb_text", label: "Size GB", align: "right", sortValue: (r) => r.index_size_gb },
  { key: "index_rows", label: "Rows", format: "int" },
  { key: "index_reads", label: "Reads", format: "int" },
  { key: "index_writes", label: "Writes", format: "int" },
  { key: "target_index_name", label: "Target Index" },
  { key: "superseded_by", label: "Superseded / Related", wrap: true },
  { key: "additional_info", label: "Info", wrap: true },
  { key: "original_index_definition", label: "Original Definition", wrap: true, mono: true, pre: true },
  { key: "script", label: "Script", wrap: true, mono: true, pre: true },
  { key: "captured_at", label: "Collected", format: "time" },
];

// The reads cell as the desktop writes it. The overall row carries no counters, so its cell stays missing.
function rollupRow(r) {
  return { ...r, reads_breakdown: r.total_reads == null ? null : fmtInt(r.total_reads) + " (" + fmtInt(r.user_seeks) + " seeks, " + fmtInt(r.user_scans) + " scans, " + fmtInt(r.user_lookups) + " lookups)" };
}

// A recommendation carries no snapshot time of its own; its database's roll-up does (captured_at). The time is set only
// when exactly one listed database row has that name (ordinal, case-sensitive); two ids sharing a name, or a database the
// roll-up list does not hold (the web sees only the databases shown, not every roll-up), leave the cell missing.
function capturedByName(databases) {
  const seen = new Map();
  for (const d of databases) seen.set(d.database_name, seen.has(d.database_name) ? null : (d.captured_at ?? null));
  return seen;
}

function recommendationRow(r, captured) {
  return { ...r, index_size_gb_text: fmtNum(r.index_size_gb, 3), captured_at: captured.get(r.database_name) ?? null };
}

// The latest choice per server: { db, full, names } where names are the databases of the last unfiltered read.
const choices = new Map();

function choiceFor(server) {
  let c = choices.get(server);
  if (!c) {
    c = { db: "", full: false, names: [] };
    choices.set(server, c);
  }
  return c;
}

let listCounter = 0;

function noticeText(data, n, db) {
  // The read says when rows were cut (truncated, with the count of every finding), so the notice never guesses: a list of exactly
  // the limit that cut nothing says nothing. A cut list of the whole server points at the Database box, whose list is cut on its own.
  const rowsCut = n > 0 && data.truncated;
  const count = n === 0
    ? "No cleanup recommendations: indexes look clean."
    : (rowsCut
      ? "Showing the largest " + n + " of " + data.recommendation_count + " recommendations. The list stops at " + n + ", so " + (data.recommendation_count - n) + " more are not shown."
      : n + (n === 1 ? " recommendation." : " recommendations."));
  const narrow = rowsCut && !db ? " Choose a database above to see its own list." : "";
  const cut = data.databases_truncated ? " Showing the largest databases of " + data.database_count + "." : "";
  const workload = data.overall_workload_reason ? " Overall row: " + data.overall_workload_reason + "." : "";
  const filter = db ? " Database " + db + "." : "";
  return count + filter + narrow + cut + " Analyzed from each database's newest collected snapshot." + workload;
}

export const tab = {
  id: "index-analysis",
  label: "Index Analysis",
  build(server, ctx) {
    const choice = choiceFor(server);
    const content = el("div", {}, [loadingStrip()]);
    const listId = "index-analysis-databases-" + (++listCounter);
    const datalist = el("datalist", { id: listId });
    const fillNames = () => mount(datalist, choice.names.map((n) => el("option", { value: n })));
    fillNames();
    const dbInput = el("input", { type: "text", list: listId, class: "sort-select", autocomplete: "off", placeholder: "All databases" });
    dbInput.value = choice.draft ?? choice.db;
    const fullBox = el("input", { type: "checkbox" });
    fullBox.checked = choice.full;
    const controls = el("div", { class: "sort-control" }, [
      el("label", { class: "sort-control" }, [el("span", { text: "Database" }), dbInput]),
      datalist,
      el("label", { class: "sort-control" }, [fullBox, el("span", { text: "Full script and definition text" })]),
    ]);
    let generation = 0;

    async function load() {
      const mine = ++generation;
      const params = { server, view: "index_analysis", limit: LIMIT };
      if (choice.db) params.database_name = choice.db;
      if (choice.full) params.full_text = true;
      try {
        const res = await readTool("get_finops", params, ctx && ctx.signal);
        if (mine !== generation) return;
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(content, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(content, emptyStrip(res.message));
        const data = res.data || {};
        if (!choice.db) {
          choice.names = (data.databases || []).map((d) => d.database_name).filter(Boolean);
          fillNames();
        }
        const captured = capturedByName(data.databases || []);
        const recs = (data.recommendations || []).map((r) => recommendationRow(r, captured));
        const rollups = (data.overall ? [rollupRow({ ...data.overall, database_name: "ALL DATABASES" })] : [])
          .concat((data.databases || []).map(rollupRow));
        const lead = (data.uptime_warning ? 1 : 0) + (data.dedupe_only_applied ? 1 : 0);
        const notes = data.notes || [];
        const parts = [noticeStrip(noticeText(data, recs.length, choice.db))];
        notes.slice(0, lead).forEach((t) => parts.push(noticeStrip(t)));
        const more = notes.slice(lead);
        if (more.length) {
          parts.push(el("details", {}, [
            el("summary", { text: "Analyzer notes (" + more.length + ")" }),
            ...more.map((t) => el("p", { class: "finops-note", text: t })),
          ]));
        }
        parts.push(el("h3", { text: "Reclaimable Space (overall + per database)" }));
        parts.push(VIZ.table({ rows: rollups }, { rowsKey: "rows", columns: ROLLUP_COLUMNS, emptyText: "No index statistics collected yet." }));
        parts.push(el("h3", { text: "Recommendations" }));
        parts.push(VIZ.table({ rows: recs }, { rowsKey: "rows", columns: RECOMMENDATION_COLUMNS, emptyText: "No cleanup recommendations: indexes look clean." }));
        mount(content, parts);
      } catch (e) {
        if (mine === generation && e?.name !== "AbortError") mount(content, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    }

    function reread() {
      mount(content, loadingStrip());
      load();
    }

    // The panel is rebuilt on every poll. Keep the uncommitted text, the caret and the focus on the per-server
    // choice so the new box can take them back. A blur caused by the old panel leaving the page must not clear focus.
    // Chrome fires that blur DURING the removal, while the box is still connected, so the check waits a turn.
    dbInput.addEventListener("input", () => {
      choice.draft = dbInput.value;
      choice.caret = [dbInput.selectionStart, dbInput.selectionEnd];
    });
    dbInput.addEventListener("focus", () => {
      choice.focused = true;
    });
    dbInput.addEventListener("blur", () => {
      setTimeout(() => {
        if (dbInput.isConnected) choice.focused = false;
      }, 0);
    });
    dbInput.addEventListener("change", () => {
      choice.db = dbInput.value.trim();
      choice.draft = undefined;
      reread();
    });
    fullBox.addEventListener("change", () => {
      choice.full = fullBox.checked;
      reread();
    });
    load();
    if (choice.focused) {
      setTimeout(() => {
        if (dbInput.isConnected) {
          dbInput.focus();
          if (choice.caret) dbInput.setSelectionRange(choice.caret[0], choice.caret[1]);
        }
      }, 0);
    }
    return el("div", {}, [controls, content]);
  },
};
