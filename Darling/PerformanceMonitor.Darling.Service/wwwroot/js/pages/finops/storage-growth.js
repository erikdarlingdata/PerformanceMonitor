/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Storage Growth" tab, from get_finops (view storage_growth). Three levels held as state in the tab: databases
   (the default), objects (a click on a database: its fastest-growing tables with a 30-day size heatmap) and indexes (a
   click on a table: its indexes and their usage). Back returns one level; the breadcrumb names the path. Nothing goes
   in the URL. The heatmap is a matrix table; each cell's shade class comes from the service's band and the browser
   computes nothing. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool, applyFormat, relTime, localTime } from "../../util.js";

const HOURS = 24;
const OBJECT_LIMIT = 20;

// The drill per server: { level, database, object }. It lives at module scope because the page's 60 s poll calls build()
// again and a build-local state would drop the reader back to Databases mid-drill.
const drills = new Map();

function drillFor(server) {
  let d = drills.get(server);
  if (!d) {
    d = { level: "databases", database: null, object: null };
    drills.set(server, d);
  }
  return d;
}

// The service refuses an object that is no longer among the top objects with this wording.
const OBJECT_GONE = "is not among the";

// The desktop's yyyy-MM-dd HH:mm: slicing the server-local string, not a clock conversion.
function accessText(v) {
  return v == null ? v : String(v).slice(0, 16).replace("T", " ");
}

const DATABASE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "current_size_mb", label: "Current (MB)", format: "num2" },
  { key: "size_7d_ago_mb", label: "7 days ago (MB)", format: "num2" },
  { key: "size_30d_ago_mb", label: "30 days ago (MB)", format: "num2" },
  { key: "growth_7d_mb", label: "Growth 7 days (MB)", format: "num2" },
  { key: "growth_30d_mb", label: "Growth 30 days (MB)", format: "num2" },
  { key: "daily_growth_rate_mb", label: "Daily rate (MB)", format: "num2" },
  { key: "growth_pct_30d", label: "Growth % 30 days", format: "num1" },
  { key: "note", label: "Note", hideWhenEmpty: true, wrap: true },
];

const OBJECT_COLUMNS = [
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "reserved_mb", label: "Reserved (MB)", format: "num1" },
  { key: "used_mb", label: "Used (MB)", format: "num1" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "index_count", label: "Indexes", format: "int" },
  { key: "growth_mb", label: "Growth (MB)", format: "num1" },
  { key: "growth_pct", label: "Growth %", format: "num1" },
  { key: "daily_growth_rate_mb", label: "Daily rate (MB)", format: "num2" },
];

const INDEX_COLUMNS = [
  { key: "index_name", label: "Index" },
  { key: "classification", label: "Status" },
  { key: "index_type_desc", label: "Type" },
  { key: "reserved_mb", label: "Reserved (MB)", format: "num1" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "user_seeks", label: "Seeks", format: "int" },
  { key: "user_scans", label: "Scans", format: "int" },
  { key: "user_lookups", label: "Lookups", format: "int" },
  { key: "total_reads", label: "Total reads", format: "int" },
  { key: "user_updates", label: "Updates", format: "int" },
  { key: "last_user_access_server_local", label: "Last access (server time)" },
];

function databaseNotice(s) {
  const n = (s.rows || []).length;
  let text = n === 1 ? "1 database" : n + " databases";
  text += s.truncated ? "; the top " + n + " of " + (s.database_count ?? "more") + " by 30-day growth." : ".";
  return text;
}

function objectNotice(s) {
  let text = "Top " + (s.rows || []).length + " objects by growth over the last " + (s.window_days ?? "?") + " days";
  text += s.truncated ? "; more exist." : ".";
  return text;
}

function indexNotice(s) {
  const n = (s.rows || []).length;
  let text = n === 1 ? "1 index" : n + " indexes";
  text += s.truncated ? "; the first " + n + " of " + (s.index_count ?? "more") + "." : ".";
  // One snapshot for every row, so the notice says when it was collected; the read has no time bound, so it can be old.
  if (typeof s.captured_at === "string" && s.captured_at) text += " Collected " + relTime(s.captured_at) + " (" + localTime(s.captured_at) + ").";
  return text;
}

// A first column holding one small button per row; it has no data key, so it is not one of the table's row-function keys.
function drillColumn(label, onPick) {
  return {
    label,
    render: (row) => el("button", { type: "button", class: "btn", text: label, onClick: () => onPick(row) }),
  };
}

// The objects x days matrix. The band is the service's; its only use here is the class name.
function heatmap(section) {
  const days = section.days || [];
  const rows = section.rows || [];
  if (!days.length || !rows.length) return emptyStrip("No daily size samples for these objects.");
  const head = el("tr", {}, [
    el("th", { text: "Object" }),
    ...days.map((d) => el("th", { class: "num", text: String(d).slice(0, 10) })),
  ]);
  const body = rows.map((r) =>
    el("tr", {}, [
      el("td", { text: r.schema_name + "." + r.table_name }),
      ...days.map((d, i) => {
        const c = (r.cells || [])[i] || [null, null];
        const mb = c[0];
        const shade = c[1];
        const known = Number.isInteger(shade) && shade >= 0 && shade <= 7;
        return el("td", {
          class: known ? "num heat-band-" + shade : "num",
          title: r.schema_name + "." + r.table_name + " | " + String(d).slice(0, 10) + " | " + applyFormat("num1", mb) + " MB reserved",
          text: mb == null ? "" : applyFormat("num1", mb),
        });
      }),
    ])
  );
  return el("div", {}, [
    el("div", { class: "table-wrap" }, [
      el("table", { class: "data" }, [el("caption", { text: "Day (UTC)" }), el("thead", {}, [head]), el("tbody", {}, body)]),
    ]),
    el("p", { class: "muted", text: "Shade: the service's size band for each cell (log scale over this table); blank means no sample or zero." }),
  ]);
}

// One section by its own status: ok shows the notice and the table, empty an empty strip, not_collected its message.
function sectionContent(section, columns, emptyText, notice, lead) {
  const s = section || {};
  if (s.status === "ok") return [noticeStrip(notice(s)), VIZ.table(s, { rowsKey: "rows", columns: lead ? [lead, ...columns] : columns, emptyText })];
  if (s.status === "empty") return [emptyStrip(emptyText)];
  return [noticeStrip(s.message ?? "This section was not collected.")];
}

export const tab = {
  id: "storage-growth",
  label: "Storage Growth",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    const state = drillFor(server);
    let seq = 0;

    function crumb() {
      const parts = ["Storage Growth"];
      if (state.database != null) parts.push(state.database);
      if (state.object != null) parts.push(state.object);
      return el("div", { class: "muted", text: parts.join(" › ") });
    }

    function back() {
      if (state.level === "indexes") {
        state.level = "objects";
        state.object = null;
      } else {
        state.level = "databases";
        state.database = null;
      }
      load();
    }

    function chrome(content) {
      const items = [];
      if (state.level !== "databases") {
        items.push(el("button", { type: "button", class: "btn", text: "Back", onClick: back }));
        items.push(crumb());
      }
      mount(body, [...items, ...content]);
    }

    function openObjects(row) {
      state.level = "objects";
      state.database = row.database_name;
      state.object = null;
      load();
    }

    function openIndexes(row) {
      state.level = "indexes";
      state.object = row.object_name;
      load();
    }

    async function load() {
      const mine = ++seq;
      mount(body, [loadingStrip()]);
      const params = { server, view: "storage_growth", hours: HOURS };
      if (state.level === "objects") Object.assign(params, { database_name: state.database, limit: OBJECT_LIMIT });
      if (state.level === "indexes") Object.assign(params, { database_name: state.database, object_name: state.object });
      try {
        const res = await readTool("get_finops", params, ctx && ctx.signal);
        if (mine !== seq) return;
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error" && state.level === "indexes" && String(res.message).includes(OBJECT_GONE)) {
          state.level = "objects";
          state.object = null;
          return load();
        }
        if (res.kind === "error") return chrome([readErrorStrip(res.message)]);
        if (res.kind === "empty") return chrome([emptyStrip(res.message)]);
        const data = res.data || {};
        if (state.level === "databases") {
          chrome([el("h3", { text: "Database Size and Growth" }), ...sectionContent(data.databases, DATABASE_COLUMNS, "No storage growth data available yet", databaseNotice, drillColumn("Show objects", openObjects))]);
        } else if (state.level === "objects") {
          const s = data.objects || {};
          const parts = [el("h3", { text: "Objects by Growth" })];
          if (data.database && data.database.status === "empty") parts.push(noticeStrip("This database is not in the latest snapshot."));
          if (s.status === "ok") parts.push(noticeStrip(objectNotice(s)), heatmap(s), VIZ.table(s, { rowsKey: "rows", columns: [drillColumn("Show indexes", openIndexes), ...OBJECT_COLUMNS], emptyText: "No object size data for this database yet." }));
          else parts.push(...sectionContent(s, OBJECT_COLUMNS, "No object size data for this database yet.", objectNotice));
          chrome(parts);
        } else {
          chrome([el("h3", { text: "Indexes" }), ...sectionContent(data.indexes && data.indexes.rows ? { ...data.indexes, rows: data.indexes.rows.map((r) => ({ ...r, last_user_access_server_local: accessText(r.last_user_access_server_local) })) } : data.indexes, INDEX_COLUMNS, "No index detail for this object.", indexNotice)]);
        }
      } catch (e) {
        if (e?.name !== "AbortError" && mine === seq) chrome([errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e)))]);
      }
    }

    load();
    return body;
  },
};
