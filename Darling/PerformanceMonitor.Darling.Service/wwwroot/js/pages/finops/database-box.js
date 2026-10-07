/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* The FinOps "Database" box (#5231): one text box with a suggestion list, shared by the FinOps tabs that read one database at a
   time (Index Analysis, Locking & Contention). Empty means "All databases"; any other value is ONE name, sent exactly as typed.
   There is no comma splitting and no trimming of a name: database names are chosen by whoever can create a database, so `A,B`,
   `x]`, ` SalesDb` (leading space), `O'Brien`, `50%+off` and `<img src=x onerror=alert(1)>` are each one name, and every name and
   notice here is drawn as text, never as markup (#5244 L6). A value that is only blanks is "All databases".

   The stored spelling (#5244 L8c): PostgreSQL `=` is case-sensitive and the store keeps the name as the monitored server spelled it,
   so a typed "salesdb" must reach the read as "SalesDb". When the typed text matches a suggestion ignoring case, the box sends the
   suggestion; a name that matches no suggestion is sent as typed. Two suggestions that differ only by case are ambiguous, so the
   typed text goes as typed unless it equals one of them exactly.

   The panel is rebuilt on every poll. The per-server `choice` object (the page's module state) keeps what the reader typed
   (draft), the caret and the focus, so the new box takes them back; a blur caused by the old panel leaving the page must not clear
   the focus. Chrome fires that blur DURING the removal, while the box is still connected, so the check waits a turn.

   Where the suggestions come from:
   - `server` omitted: the page hands names in with setNames (Index Analysis: the databases of its last unfiltered read).
   - `server` given: the box reads /api/server-databases?server=... itself. That route stops at 5,000 names and says so
     (truncated). A list that is not cut holds every name, so typing only filters it in the browser and reads nothing. A cut list
     asks the route again with `search` after a short pause as the reader types; each keystroke cancels the pending ask and the read
     in flight, and only the newest answer is shown (the same shape as the Databases picker, js/pages/database-filter.js). */

import { el, mount, apiGet } from "../../util.js";

/** The pause after the last keystroke before a cut list asks the route for the typed text, in milliseconds (the picker's pause). */
export const SEARCH_DEBOUNCE_MS = 250;

let listCounter = 0;

/**
 * The name to send for what was typed. A blank value is "" (All databases). A value equal to a suggestion is kept. Otherwise the
 * one suggestion that matches ignoring case is sent in its stored spelling, and anything else goes as typed (#5244 L8c).
 */
export function storedSpelling(typed, names) {
  if (typed == null || String(typed).trim() === "") return "";
  const value = String(typed);
  if (names.includes(value)) return value;
  const folded = value.toLowerCase();
  const same = names.filter((n) => typeof n === "string" && n.toLowerCase() === folded);
  return same.length === 1 ? same[0] : value;
}

/** What a page keeps per server for its box: { db, draft, caret, focused, names, cut, answer, ... } (this module owns those fields). */
export function newBoxChoice() {
  return { db: "", draft: undefined, caret: undefined, focused: false, names: [], cut: false, answer: null, searchSeq: 0, loadFailed: "" };
}

/**
 * Builds the box. `choice` is the page's per-server state (db is the name the page reads with), `onCommit()` runs after the reader
 * commits a name (choice.db is set), `server` turns on the route reads. Returns { nodes, input, setNames }: `nodes` are the label and
 * the suggestion list to put in the page's controls row.
 */
export function databaseBox(choice, { onCommit, server = null }) {
  const listId = "finops-database-box-" + (++listCounter);
  const datalist = el("datalist", { id: listId });
  const input = el("input", { type: "text", list: listId, class: "sort-select", autocomplete: "off", placeholder: "All databases" });
  const hint = el("span", { class: "finops-note" });
  const label = el("label", { class: "sort-control" }, [el("span", { text: "Database" }), input]);
  input.value = choice.draft ?? choice.db;

  const shown = () => (choice.answer ? choice.answer.names : choice.names);
  // The names a typed value may match: the first page and the newest search answer, so a name found by search is still matched.
  const known = () => (choice.answer ? [...new Set([...choice.names, ...choice.answer.names])] : choice.names);

  function paint() {
    mount(datalist, shown().map((n) => el("option", { value: n })));
    let text = "";
    if (choice.loadFailed) text = choice.loadFailed;
    else if (choice.cut && choice.answer && choice.answer.cut) text = "The search stops at " + choice.answer.names.length + " databases, so more matches are not shown. Type more of the name to narrow it.";
    else if (choice.cut && !choice.answer) text = "The list stops at " + choice.names.length + " databases, so more are not shown. Type part of a name to find it.";
    hint.textContent = text; // text, never markup
  }
  choice.paint = paint;

  function dropSearch() {
    clearTimeout(choice.searchTimer);
    choice.searchTimer = undefined;
    if (choice.searchAbort) choice.searchAbort.abort();
    choice.searchAbort = undefined;
    choice.searchSeq = (choice.searchSeq || 0) + 1;
  }

  async function runSearch(seq, query) {
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    choice.searchAbort = controller || undefined;
    const res = await apiGet("/api/server-databases?server=" + encodeURIComponent(server) + "&search=" + encodeURIComponent(query), controller ? controller.signal : undefined);
    if (choice.searchSeq !== seq) return; // a newer keystroke owns the list now
    choice.searchAbort = undefined;
    if (res.kind === "data" && res.data && Array.isArray(res.data.databases)) {
      choice.loadFailed = "";
      choice.answer = { query, names: res.data.databases.filter((d) => typeof d === "string"), cut: res.data.truncated === true };
    } else if (res.kind !== "aborted") {
      choice.answer = null;
      choice.loadFailed = "The search could not be run" + (res.message ? ": " + res.message : ".");
    }
    if (choice.paint) choice.paint();
  }

  // A keystroke. A list that is not cut already holds every name, so the browser's own filter is the whole search.
  function searchTyped(text) {
    if (!server || !choice.cut) return;
    dropSearch();
    const seq = choice.searchSeq;
    if (text.trim() === "") {
      if (choice.answer) {
        choice.answer = null;
        paint();
      }
      return;
    }
    choice.searchTimer = setTimeout(() => runSearch(seq, text), SEARCH_DEBOUNCE_MS);
  }

  async function load() {
    const res = await apiGet("/api/server-databases?server=" + encodeURIComponent(server));
    if (res.kind === "data" && res.data && Array.isArray(res.data.databases)) {
      choice.loadFailed = "";
      choice.names = res.data.databases.filter((d) => typeof d === "string");
      choice.cut = res.data.truncated === true;
      if (!choice.cut) choice.answer = null;
    } else if (res.kind !== "aborted") {
      choice.loadFailed = "The list of databases could not be loaded" + (res.message ? ": " + res.message : ".");
    }
    if (choice.paint) choice.paint();
  }

  input.addEventListener("input", () => {
    choice.draft = input.value;
    choice.caret = [input.selectionStart, input.selectionEnd];
    searchTyped(input.value);
  });
  input.addEventListener("focus", () => {
    choice.focused = true;
  });
  input.addEventListener("blur", () => {
    setTimeout(() => {
      if (input.isConnected) choice.focused = false;
    }, 0);
  });
  input.addEventListener("change", () => {
    choice.db = storedSpelling(input.value, known());
    choice.draft = undefined;
    input.value = choice.db;
    onCommit();
  });

  paint();
  if (server) load();
  if (choice.focused) {
    setTimeout(() => {
      if (input.isConnected) {
        input.focus();
        if (choice.caret) input.setSelectionRange(choice.caret[0], choice.caret[1]);
      }
    }, 0);
  }

  return {
    nodes: [label, datalist, hint],
    input,
    setNames(names) {
      choice.names = names;
      paint();
    },
  };
}
