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
   collapsed block; there are no tooltips. A Database box (database-box.js: any name, sent as one name; the list suggests the databases shown by the last
   unfiltered read, and a typed name that matches one ignoring case is sent in its stored spelling) and that box re-read the view; the controls stay put and only the content below them is remounted.
   The choice is kept per server in module state, so it survives the page's poll rebuilds while the page stays open. */

import { VIZ } from "../../panels.js";
import { databaseBox, newBoxChoice } from "./database-box.js";
import { el, mount, loadingStrip, captionStrip, noticeStrip, readErrorStrip, errorStrip, readTool, fmtInt, fmtMbFit, localTime } from "../../util.js";
import { plainText } from "../../plain-text.js";
import { gatedEmptyStrip } from "./gate.js";

// The most recommendations the view lists (#5238). The desktop grid shows every finding, so the tab asks for the ceiling, as the
// Database Sizes tab asks for its 500 files; the view's own default of 10 and the 50 of its other top-N views are not for this tab.
const LIMIT = 500;
// The roll-up's GB figures arrive to six places (#7702: three places printed a 2 MB index as 0.00). They print in the unit that fits
// (mbfit, #7556), so each has a derived text key beside its raw number, which stays the sort value.
const GB_KEYS = ["total_size_gb", "unused_size_gb", "compression_min_savings_gb", "compression_max_savings_gb", "total_min_savings_gb", "total_max_savings_gb"];
const gbText = (k) => k + "_text";
const gbColumn = (key, label) => ({ key: gbText(key), label, align: "right", sortValue: (r) => r[key] });
const DERIVED_KEYS = ["reads_breakdown", "index_size_gb_text", "captured_at", "status_text", ...GB_KEYS.map(gbText)];

const ROLLUP_COLUMNS = [
  { key: "database_name", label: "Database", name: true },
  // #7701: next to the database name, with the mark for a database whose snapshot is left over (in no total).
  { key: "captured_at", label: "Collected", format: "time" },
  { key: "status_text", label: "Status" },
  { key: "tables_analyzed", label: "Tables", format: "int" },
  { key: "index_count", label: "Indexes", format: "int" },
  gbColumn("total_size_gb", "Size"),
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "indexes_to_disable", label: "To Disable", format: "int" },
  { key: "indexes_to_merge", label: "To Merge", format: "int" },
  { key: "compressable_indexes", label: "Compressable", format: "int" },
  { key: "unused_indexes", label: "Unused", format: "int" },
  gbColumn("unused_size_gb", "Disable"),
  gbColumn("compression_min_savings_gb", "Compress Min"),
  gbColumn("compression_max_savings_gb", "Compress Max"),
  gbColumn("total_min_savings_gb", "Total Min"),
  gbColumn("total_max_savings_gb", "Total Max"),
  { key: "reads_breakdown", label: "Reads" },
  { key: "writes", label: "Writes", format: "int" },
  { key: "lock_wait_count", label: "Lock Waits", format: "int" },
  { key: "avg_lock_wait_ms", label: "Avg Lock Wait ms", format: "num2" },
  { key: "latch_wait_count", label: "Latch Waits", format: "int" },
  { key: "avg_latch_wait_ms", label: "Avg Latch Wait ms", format: "num2" },
];

const RECOMMENDATION_COLUMNS = [
  { key: "action", label: "Action" },
  { key: "result_kind", label: "Result Kind" },
  { key: "consolidation_rule", label: "Rule" },
  { key: "database_name", label: "Database", name: true },
  { key: "captured_at", label: "Collected", format: "time" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_size_gb_text", label: "Size", align: "right", sortValue: (r) => r.index_size_gb },
  { key: "index_rows", label: "Rows", format: "int" },
  { key: "index_reads", label: "Reads", format: "int" },
  { key: "index_writes", label: "Writes", format: "int" },
  { key: "target_index_name", label: "Target Index" },
  { key: "superseded_by", label: "Superseded / Related", wrap: true },
  { key: "additional_info", label: "Info", wrap: true },
  { key: "original_index_definition", label: "Original Definition", wrap: true, mono: true, pre: true },
  { key: "script", label: "Script", wrap: true, mono: true, pre: true },
];

// A size the read sends in GB, shown in the unit that fits it. The read rounds GB to six places, so an index of 0.4 MB arrives as
// 0.000391 GB, and fmtMbFit would print it back as 0.400384 MB: digits nobody measured. Under 1 MB it prints at most two decimals
// (0.4 MB); a positive size under 0.005 MB reads "<0.01 MB", never 0 (#7702 item 1). A negative size that tiny is not "<0.01 MB" (that
// would read as a small positive one): it reads 0 MB. From 1 MB up fmtMbFit already rounds.
function gbSizeText(gb) {
  const mb = gb * 1024;
  if (Math.abs(mb) >= 1 || mb === 0) return fmtMbFit(mb);
  const cents = Math.round(mb * 100);
  if (cents === 0) return mb > 0 ? "<0.01 MB" : fmtMbFit(0);
  return fmtMbFit(cents / 100);
}

// The reads cell as the desktop writes it. The overall row carries no counters, so its cell stays missing.
function rollupRow(r) {
  const gbs = {};
  for (const k of GB_KEYS) gbs[gbText(k)] = r[k] == null ? null : gbSizeText(r[k]);
  // #7701: a database whose snapshot is left over (left_over, set by the read) is in no total; the mark says since when.
  const status = r.left_over ? "Not collected since " + localTime(r.captured_at) : "";
  return { ...r, ...gbs, status_text: status, reads_breakdown: r.total_reads == null ? null : fmtInt(r.total_reads) + " (" + fmtInt(r.user_seeks) + " seeks, " + fmtInt(r.user_scans) + " scans, " + fmtInt(r.user_lookups) + " lookups)" };
}

// A recommendation carries no snapshot time of its own; its database's roll-up does (captured_at). The time is set only
// when exactly one listed database row has that name (ordinal, case-sensitive); two ids sharing a name, or a database the
// roll-up list does not hold (the web sees only the databases shown, not every roll-up), leave the cell missing.
function capturedByName(databases) {
  const seen = new Map();
  // A left-over database has no recommendations, so it does not blank the time of a current database that shares its name (#7701).
  for (const d of databases) if (!d.left_over) seen.set(d.database_name, seen.has(d.database_name) ? null : (d.captured_at ?? null));
  return seen;
}

function recommendationRow(r, captured) {
  return { ...r, index_size_gb_text: gbSizeText(r.index_size_gb), captured_at: captured.get(r.database_name) ?? null };
}

// The latest choice per server: the Database box's fields (db, draft, caret, focused; names are the databases of the last unfiltered
// read) plus `full`.
const choices = new Map();

function choiceFor(server) {
  let c = choices.get(server);
  if (!c) {
    c = { ...newBoxChoice(), full: false };
    choices.set(server, c);
  }
  return c;
}

function noticeText(data, n, db) {
  // The read says when rows were cut (truncated, with the count of every finding), so the notice never guesses: a list of exactly
  // the limit that cut nothing says nothing. A cut list of the whole server points at the Database box, whose list is cut on its own.
  const rowsCut = n > 0 && data.truncated;
  // The count left over reads "1 more is not shown" for one and "N more are not shown" for any other number.
  const left = data.recommendation_count - n;
  const count = n === 0
    ? "No cleanup recommendations: indexes look clean."
    : (rowsCut
      ? "Showing the largest " + n + " of " + data.recommendation_count + " recommendations. The list stops at " + n + ", so " + (left === 1 ? "1 more is" : left + " more are") + " not shown."
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
    const box = databaseBox(choice, { onCommit: () => reread() });
    const fullBox = el("input", { type: "checkbox" });
    fullBox.checked = choice.full;
    const controls = el("div", { class: "sort-control" }, [
      ...box.nodes,
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
        if (res.kind === "empty") return mount(content, gatedEmptyStrip(res, ctx));
        const data = res.data || {};
        if (!choice.db) {
          choice.names = (data.databases || []).map((d) => d.database_name).filter(Boolean);
          box.setNames(choice.names);
        }
        const captured = capturedByName(data.databases || []);
        const recs = (data.recommendations || []).map((r) => recommendationRow(r, captured));
        const rollups = (data.overall ? [rollupRow({ ...data.overall, database_name: "ALL DATABASES" })] : [])
          .concat((data.databases || []).map(rollupRow));
        /* #6653: the notes lead with the uptime caveat only when dedupe-only is not in effect (the tool leaves it out beside the dedupe notice). */
        const lead = (data.uptime_warning && !data.dedupe_only_applied ? 1 : 0) + (data.dedupe_only_applied ? 1 : 0);
        const notes = data.notes || [];
        // A plain count stays quiet. A cut list, a cut database list or the overall-workload reason is a caveat and stays yellow.
        const parts = [captionStrip(noticeText(data, recs.length, choice.db), (recs.length > 0 && data.truncated === true) || data.databases_truncated === true || !!data.overall_workload_reason)];
        notes.slice(0, lead).forEach((t) => parts.push(noticeStrip(t)));
        const more = notes.slice(lead);
        if (more.length) {
          parts.push(el("details", {}, [
            el("summary", { text: "Analyzer notes (" + more.length + ")" }),
            ...more.map((t) => el("p", { class: "finops-note", text: plainText(t) })),
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

    fullBox.addEventListener("change", () => {
      choice.full = fullBox.checked;
      reread();
    });
    load();
    return el("div", {}, [controls, content]);
  },
};
