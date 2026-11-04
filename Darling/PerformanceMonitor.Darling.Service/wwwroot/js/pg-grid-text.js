/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Short cell values for the PostgreSQL grids whose rows carry a paragraph (#6154).

   The PostgreSQL reads write one explanatory sentence or paragraph per row (bloat_finding, droppability_finding, cost_finding,
   last_scan_note, skipped_reason). An MCP client needs all of it, so the reads keep it. A grid cell cannot hold it: the row grows
   to 300 to 500 px. The grid therefore shows the short word from here and opens the full text on a click (the column's
   `detailKey`); text that is the same for every row sits in the column header's tooltip (`hint`).

   Each rule matches the START of the sentence the read writes. A sentence none of them knows keeps its own first words, so a new
   sentence in the read never shows as a blank cell. */

const RULES = {
  bloat: [
    [/^Estimated bloat is low/i, "Low"],
    [/^Estimated bloat is moderate/i, "Moderate"],
    [/^Estimated bloat is high/i, "High"],
  ],
  droppability: [
    [/^INVALID index/, "Invalid"],
    [/^Backs the PRIMARY KEY/, "No, primary key"],
    [/^Backs a UNIQUE or EXCLUSION/, "No, constraint"],
    [/^This index is the table's REPLICA/, "No, replica identity"],
    [/^Not enough history/, "Too early"],
    [/^In use/, "No, in use"],
    [/^No scans recorded/, "Candidate"],
  ],
  lastScan: [[/^No last-scan timestamp/, "None recorded"]],
  /* get_pg_database_stats writes the same three paragraphs on every row, so a cell shows the verdict word and the paragraph opens on a click (#7410). */
  spill: [
    [/^No temp files/, "None"],
    [/^This database wrote temp files[\s\S]*?A handful of very large/, "Few, very large files"],
    [/^This database wrote temp files[\s\S]*?Mid-sized spill files/, "Mid-sized files"],
    [/^This database wrote temp files[\s\S]*?Many small spill files/, "Many small files"],
  ],
  cache: [
    [/^No block accesses/, "No accesses"],
    [/^[\d.,]+% of block accesses[\s\S]*?At or above the conventional/, "At target"],
    [/^[\d.,]+% of block accesses[\s\S]*?Below the conventional 99% target but not alarming/, "Slightly below target"],
    [/^[\d.,]+% of block accesses[\s\S]*?Well below/, "Well below target"],
  ],
  rollback: [
    [/^No transactions completed/, "No transactions"],
    [/^[\d.,]+% of completed transactions[\s\S]*?A rollback storm/, "Rollback storm"],
    [/^[\d.,]+% of completed transactions[\s\S]*?Higher than a healthy/, "Higher than usual"],
    [/^[\d.,]+% of completed transactions[\s\S]*?A normal share/, "Normal"],
  ],
  fillfactor: [[/^This table has a non-default fillfactor/, "Reserved space"]],
  toast: [[/^This table has out-of-line TOAST/, "Has TOAST"]],
  indexSkip: [
    [/^partial index/, "Partial index"],
    [/^the parent table has never been analyzed/, "Table not analyzed"],
    [/^a key column is name-typed/, "Name-typed key"],
    [/^column widths are not visible/, "No column widths"],
    [/^the index occupies no pages/, "Empty index"],
    [/^the model predicts more pages/, "Model out of range"],
  ],
};

/** The first sentence of `text`, cut to `max` characters with an ellipsis. */
export function firstSentence(text, max = 40) {
  const t = String(text ?? "").trim();
  if (t === "") return "";
  const m = /^(.+?[.!?])(\s|$)/.exec(t);
  const s = m ? m[1] : t;
  return s.length > max ? s.slice(0, max - 1).trimEnd() + "…" : s;
}

function shortWord(kind, text) {
  if (typeof text !== "string" || text === "") return null;
  for (const [re, word] of RULES[kind]) if (re.test(text)) return word;
  return firstSentence(text);
}

/** "Low", "Moderate" or "High" for a table's bloat_finding; null for no finding (a suppressed estimate has none). */
export const bloatVerdict = (text) => shortWord("bloat", text);

/** "Can It Go?" in a word or two: Candidate, No, primary key, Too early, Invalid... */
export const droppabilityVerdict = (text) => shortWord("droppability", text);

/** What an index costs, in a few words: "1.2 GB, 12% of heap" from "1.2 GB. That is 12% of its table's heap. ...". */
export function costFigure(text) {
  if (typeof text !== "string" || text === "") return null;
  if (/^Index size was not measured/.test(text)) return "Not measured";
  const size = /^(\d[\d.,]* [A-Za-z]+)\./.exec(text);
  if (!size) return firstSentence(text);
  const share = /That is ([\d.]+)% of its table's heap/.exec(text);
  return share ? size[1] + ", " + share[1] + "% of heap" : size[1];
}

export const lastScanNoteWord = (text) => shortWord("lastScan", text);
export const fillfactorNoteWord = (text) => shortWord("fillfactor", text);
export const toastNoteWord = (text) => shortWord("toast", text);
export const indexSkipWord = (text) => shortWord("indexSkip", text);
export const spillWord = (text) => shortWord("spill", text);
export const cacheWord = (text) => shortWord("cache", text);
export const rollbackWord = (text) => shortWord("rollback", text);

/** A column that shows a short word and opens the field's full paragraph on a click (#6154, #7410). What is true of every row goes in `hint`, the header tooltip. */
export function detailColumn(key, label, word, hint, extra) {
  const short = (row) => word(row[key]);
  return { key, label, hint, display: (row) => short(row) ?? "—", sortValue: short, detailKey: key, ...extra };
}

/**
 * #7410: the one sentence over Top Query Shapes in place of the eviction note. It is built from the read's own fields (`evictions.known`
 * and `evictions.eviction_passes_in_window`), never from the note's text. Null when the counter was read and no pass happened, so the
 * strip then falls back to the note as given, which is also null.
 */
export function evictionNoteShort(data) {
  const e = data && typeof data === "object" ? data.evictions : null;
  if (!e || typeof e !== "object") return null;
  if (e.known === false) {
    return "This server does not report how often pg_stat_statements dropped entries. That needs PostgreSQL 14 or later with the extension at version 1.9 or newer.";
  }
  const n = e.eviction_passes_in_window;
  if (typeof n !== "number" || !(n > 0)) return null;
  return "pg_stat_statements dropped its least-used entries " + n.toLocaleString("en-US") + (n === 1 ? " time" : " times") +
    " in this window, so a rarely run statement may be missing and the totals can read low. A higher pg_stat_statements.max helps, and it needs a restart.";
}
