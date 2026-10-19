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
