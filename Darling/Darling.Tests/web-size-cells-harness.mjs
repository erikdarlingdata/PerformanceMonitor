/* #7564, #7556: runs the shipped size formats (util.js) and the PostgreSQL Storage, Buffer Pool Residency, OS CPU by Query and SQL Server memory grids
   (pages/server-tabs/*.js through panels.js gridTable) on a small fake DOM and prints what their size cells show as one line of JSON.
   WebSizeCellsBehaviourTests starts it as
       node web-size-cells-harness.mjs <path to the js folder>
   Only the DOM is a stand-in. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); } }; this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.value = ""; }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  click() { this.fire("click", { target: this }); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  focus() { globalThis.document.activeElement = this; this.fire("focus"); }
  contains(n) { for (; n; n = n.parent) if (n === this) return true; return false; }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
let downloads = [];
globalThis.document = { activeElement: null, body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t), querySelectorAll: () => [] /* keepScroll lists the page's grids; this fake page draws none */ };
globalThis.location = { hash: "#/server/a/storage" };

const root = process.argv[2];
const imp = (f) => import(pathToFileURL(root + "/" + f).href);
const util = await imp("util.js");
const { gridTable } = await imp("panels.js");
const storage = await imp("pages/server-tabs/pg-storage.js");
const io = await imp("pages/server-tabs/pg-io.js");
const waits = await imp("pages/server-tabs/pg-waits.js");
const mem = await imp("pages/server-tabs/sql-memory.js");
const { READ_FIELDS } = await imp("read-fields.js");
const dbSizes = await imp("pages/finops/database-sizes.js");
const versionStore = await imp("pages/finops/version-store.js");
const utilization = await imp("pages/finops/utilization.js");
const { DASHBOARD_TEMPLATES } = await imp("view-templates.js");
const { VIZ } = await imp("panels.js");

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const norm = (t) => t.replace(/[▲▼▾ ]/g, "");
/* The text and the alignment class of one column's cell in every row, found by the header's label. */
const cells = (columns, rows, label) => {
  const grid = gridTable(rows, { columns, title: "t" });
  const table = all(grid).find((n) => n.tag === "table");
  const heads = table.find("thead").find("tr").children.map((h) => norm(h.textContent));
  const at = heads.indexOf(norm(label));
  if (at < 0) return { missing: heads };
  const body = table.children[1];
  return { texts: body.children.map((tr) => tr.children[at].textContent), aligned: body.children.map((tr) => /\bnum\b/.test(tr.children[at].className)) };
};

const out = {};
out.fmtMbFit = {};
for (const v of [0.45, 0.2, 0, null, 1.5, 2048, "x"]) out.fmtMbFit[String(v)] = util.fmtMbFit(v);

/* An index of 456 KB, one of 16 KB, one whose size was not measured (-1), and one of 21.4 MB. The MB fields are what the service rounds. */
const sizes = [
  { index_bytes: 466944, index_mb: 0.45, estimated_reclaimable_bytes: 46694, estimated_reclaimable_mb: 0 },
  { index_bytes: 16384, index_mb: 0.02, estimated_reclaimable_bytes: null, estimated_reclaimable_mb: null },
  { index_bytes: -1, index_mb: null, estimated_reclaimable_bytes: 0, estimated_reclaimable_mb: 0 },
  { index_bytes: 22487040, index_mb: 21.45, estimated_reclaimable_bytes: 4194304, estimated_reclaimable_mb: 4 },
];
out.byIndexSize = cells(storage.PG_INDEX_USAGE_COLUMNS, storage.PG_INDEX_USAGE_ROWS(sizes), "Size");
out.bloatSize = cells(storage.PG_INDEX_BLOAT_COLUMNS, sizes, "Size");
out.bloatReclaimable = cells(storage.PG_INDEX_BLOAT_COLUMNS, sizes, "Reclaimable");

const tables = [
  { heap_bytes: 466944, heap_mb: 0.45, bloat_bytes_estimate: 8192, bloat_mb_estimate: 0.01 },
  { heap_bytes: -1, heap_mb: null, bloat_bytes_estimate: null, bloat_mb_estimate: null },
];
out.tableHeap = cells(storage.PG_BLOAT_COLUMNS, storage.PG_BLOAT_ROWS(tables), "Heap");
out.tableBloat = cells(storage.PG_BLOAT_COLUMNS, tables, "Bloat (est.)");

/* #7564 review: the same -1 rows drawn through the panel path (VIZ.table with the page's row step), then sorted ascending by the Size header. An unmeasured
   row is a null to the grid, so it sorts last and its cell is a dash; as a raw -1 it sorted above "0 B". */
const sortedSizes = (columns, orderRows, rowsKey, rows, label) => {
  const grid = VIZ.table({ [rowsKey]: rows }, { rowsKey, columns, orderRows });
  const table = all(grid).find((n) => n.tag === "table");
  const heads = table.find("thead").find("tr").children;
  const at = heads.findIndex((h) => norm(h.textContent) === norm(label));
  heads[at].click();
  return { texts: table.children[1].children.map((tr) => tr.children[at].textContent) };
};
out.indexSizeSortedAsc = sortedSizes(storage.PG_INDEX_USAGE_COLUMNS, storage.PG_INDEX_USAGE_ROWS, "indexes",
  [{ index_bytes: 22487040 }, { index_bytes: -1 }, { index_bytes: 0 }, { index_bytes: 466944 }], "Size");
out.heapSortedAsc = sortedSizes(storage.PG_BLOAT_COLUMNS, storage.PG_BLOAT_ROWS, "tables",
  [{ heap_bytes: 466944 }, { heap_bytes: -1 }, { heap_bytes: 0 }], "Heap");

/* 29 buffers of 8 KB is 0.2% of a small pool and 232 KB; the reply rounds it to 0.2 MB. */
out.resident = cells(io.PG_BUFFER_USAGE_COLUMNS, [{ buffer_bytes: 29 * 8192, buffer_mb: 0.2 }, { buffer_bytes: 0, buffer_mb: 0 }], "Resident");
out.deviceRead = cells(waits.PG_KERNEL_COLUMNS, [{ device_read_bytes: 209715, device_read_mb: 0.2, device_write_bytes: 0, device_write_mb: 0 }], "Device Read");
out.deviceWrite = cells(waits.PG_KERNEL_COLUMNS, [{ device_read_bytes: 209715, device_read_mb: 0.2, device_write_bytes: 0, device_write_mb: 0 }], "Device Write");

/* The SQL Server grid columns that now use the fitting format: small values keep their MB, and the cell lines up with the numbers beside it. */
out.granted = cells(mem.GRANT_COLUMNS, [{ granted_memory_mb: 0.5 }, { granted_memory_mb: 0 }, { granted_memory_mb: 2.5 }, { granted_memory_mb: null }], "Granted");
out.clerk = cells(mem.CLERK_COLUMNS, [{ clerk_type: "MEMORYCLERK_SQLBUFFERPOOL", memory_mb: 0.75 }], "Memory");

/* Round 2 (#7564): the columns outside the server tabs that read a two-decimal or one-decimal MB field. */
const rf = (tool) => READ_FIELDS[tool].table.columns;
out.r2 = {
  semaphoreGranted: cells(rf("get_resource_semaphore"), [{ granted_memory_mb: 0.5 }, { granted_memory_mb: 2.5 }], "Granted MB"),
  semaphoreAvailable: cells(rf("get_resource_semaphore"), [{ available_memory_mb: 0.25 }], "Available MB"),
  grantsUsed: cells(rf("get_memory_grants"), [{ used_memory_mb: 0.5 }], "Used MB"),
  tempdbVersionStore: cells(rf("get_tempdb_trend"), [{ version_store_mb: 0.25 }], "Version store"),
  tempdbReserved: cells(rf("get_tempdb_trend"), [{ total_reserved_mb: 0.75 }], "Reserved"),
  tempdbInternal: cells(rf("get_tempdb_trend"), [{ internal_objects_mb: 0.5 }], "Internal objects"),
  tempdbUser: cells(rf("get_tempdb_trend"), [{ user_objects_mb: 0.5 }], "User objects"),
  dbTotal: cells(rf("get_database_sizes"), [{ total_size_mb: 0.5 }], "Total"),
  dbUsed: cells(rf("get_database_sizes"), [{ used_size_mb: 2.5 }], "Used"),
  fileSize: cells(rf("get_file_io_stats"), [{ size_mb: 0.5 }], "Size"),
  pvsSize: cells(rf("get_pvs_stats"), [{ pvs_size_mb: 0.25 }], "PVS size"),
  pvsData: cells(rf("get_pvs_stats"), [{ database_data_size_mb: 8.5 }], "Data size"),
  growth: cells(rf("get_default_trace_events"), [{ growth_mb: 0.5 }], "Growth"),
  finopsTotal: cells(dbSizes.COLUMNS, [{ total_size_mb: 0.5 }], "Total"),
  finopsUsed: cells(dbSizes.COLUMNS, [{ used_size_mb: 0.25 }], "Used"),
  finopsFree: cells(dbSizes.COLUMNS, [{ free_space_mb: 0.25 }], "Free"),
  finopsVolumeFree: cells(dbSizes.COLUMNS, [{ volume_free_mb: 0.5 }], "Volume free"),
  versionStoreData: cells(versionStore.COLUMNS, [{ database_data_size_mb: 4.5 }], "Database data size"),
};
/* The Available tiles of the two memory stat sets (the Utilization page and the Memory pressure dashboard template), each drawn through the stat viz.
   engine_edition 5 is an Azure SQL Database, where the same field is captioned "Available under limit". */
const tiles = (stats, data) => {
  const grid = VIZ.stat(data, { stats });
  return Object.fromEntries(all(grid).filter((n) => n.className && /(^| )stat( |$)/.test(n.className)).map((t) => [t.children[1].textContent, t.children[0].textContent]));
};
const templateStats = DASHBOARD_TEMPLATES.find((t) => t.key === "memory-pressure").make("a").definition.panels.find((p) => p.title === "Memory").stats;
out.r3 = {
  utilizationBox: tiles(utilization.MEMORY_STATS, { available_physical_memory_mb: 0.5, engine_edition: 3 }),
  utilizationAzure: tiles(utilization.MEMORY_STATS, { available_physical_memory_mb: 0.25, engine_edition: 5 }),
  templateBox: tiles(templateStats, { available_physical_memory_mb: 0.5, engine_edition: 3 }),
  templateAzure: tiles(templateStats, { available_physical_memory_mb: 0.25, engine_edition: 5 }),
};
console.log(JSON.stringify(out));
