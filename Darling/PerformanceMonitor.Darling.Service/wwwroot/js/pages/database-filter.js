/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The server page's database filter (#5245, part of #5244): a toolbar button ("Databases: All", "Databases: 2 of 37") and a
 * popover on multi-picker.js, the web twin of the desktop viewer's database picker. The choice is per browser and per server
 * (viewer-local.js); the page applies it to every read that can take it (util.js, database-filter-reads.js).
 *
 * Apply stores EXACTLY the checked set (desktop parity). A checked set is never turned into "All" because every offered name is
 * checked: the offered list leaves out master, model, msdb and tempdb, so "all offered" is not "all". Only the "All databases"
 * button, or an Apply with nothing checked, clears the filter. A stored name the inventory no longer offers stays checked and
 * listed (multi-picker.js lists a checked value after the options), so a database that was dropped can still be unchecked. A
 * check past 50 names or past the encoded byte budget is refused with a sentence. Every name reaches the DOM through el's text
 * path or a property setter, never as HTML.
 *
 * The inventory comes from /api/server-databases when the popover opens. The label reads "Databases: 2" until an inventory
 * has loaded and "Databases: 2 of 37" after (the desktop sets its total only when its popup opens, so before the first open
 * it would read "2 of 2").
 *
 * When the route cut the list at its cap (truncated), the names past it are not in the picker, so typing in the search box asks the
 * route again with the text (/api/server-databases?server=...&search=...), after a short pause, and the list shows the newest
 * answer; the route matches the text against every database the store has collected, not only the first page. A list that was
 * not cut already holds every name, so its search keeps filtering the loaded names and reads nothing. (#5314)
 *
 * The 60 s poll rebuilds the page head, so the open flag, the loaded inventories and the picker's draft live at module scope,
 * and a rebuilt control opens where the old one was.
 */

import { el, mount, apiGet } from "../util.js";
import { multiPicker, pickerState } from "../multi-picker.js";
import { getDatabaseFilter, setDatabaseFilter, isDatabaseName, MAX_DB_NAMES, MAX_DB_QUERY_BYTES, databaseQueryBytes } from "../viewer-local.js";

const inventories = new Map(); // server_id -> the database names the store offers
const cutInventories = new Set(); // server_ids whose offered list the route cut at its cap (more databases exist)
let openFor = null; // the server_id whose popover is open
let loadFailed = ""; // the sentence for the last failed inventory load of the open popover
const searchAnswers = new Map(); // server_id -> { query, names, cut }: the route's answer for the search text typed into a cut list
const searchSeqs = new Map(); // server_id -> the number of the newest search; an older answer that lands later is dropped
const searchTimers = new Map(); // server_id -> the pending debounce timer
const searchAborts = new Map(); // server_id -> the AbortController of the search read in flight
const painters = new Map(); // server_id -> the newest control's repaint, so an answer paints the control the page holds now

/** The pause after the last keystroke before a cut list's search asks the route, in milliseconds. */
export const SEARCH_DEBOUNCE_MS = 250;

const pickerKey = (serverId) => "database-filter:" + serverId;

/** Forgets a server's search answer and cancels its pending and in-flight search; the popover then lists the first page again. */
function dropSearch(serverId) {
  clearTimeout(searchTimers.get(serverId));
  searchTimers.delete(serverId);
  const inFlight = searchAborts.get(serverId);
  if (inFlight) inFlight.abort();
  searchAborts.delete(serverId);
  searchSeqs.set(serverId, (searchSeqs.get(serverId) || 0) + 1);
  searchAnswers.delete(serverId);
}

/** The button's text: "Databases: All", "Databases: 2" (no inventory yet) or "Databases: 2 of 37". */
export function databaseFilterLabel(chosen, offered) {
  if (!chosen) return "Databases: All";
  return offered == null ? "Databases: " + chosen : "Databases: " + chosen + " of " + offered;
}

/** The control for a server whose card is not known (the first paint's fleet read failed): disabled, and no filter is active. */
export function databaseFilterUnavailable() {
  return el("span", { class: "db-filter" }, [
    el("button", { type: "button", class: "btn db-filter-button", disabled: true, text: "Databases: unavailable" }),
  ]);
}

/** The refusal sentence for checking `value` on top of `checked` when the encoded query part would pass its budget, else null. */
function byteRefusal(value, checked) {
  let bytes = databaseQueryBytes(value);
  for (const n of checked) bytes += databaseQueryBytes(n);
  return bytes > MAX_DB_QUERY_BYTES ? "These names would make the request too long: uncheck a database to pick another." : null;
}

/**
 * The button and its popover for one server. `serverId` keys the stored choice and `server` is the route's key the inventory
 * route takes. `onApply` runs once per Apply or "All databases", after the choice is stored.
 */
export function databaseFilterControl({ serverId, server, onApply }) {
  const key = pickerKey(serverId);
  const button = el("button", { type: "button", class: "btn db-filter-button", "aria-haspopup": "dialog", "aria-expanded": "false" });
  const popover = el("div", { class: "db-filter-popover", role: "dialog", "aria-label": "Databases" });
  popover.hidden = true;
  const node = el("span", { class: "db-filter" }, [button, popover]);

  const paintLabel = () => {
    const inv = inventories.get(serverId);
    /* A cut list reads "of 5000+": the count is the cap, not the number of databases the server has. */
    const offered = inv ? (cutInventories.has(serverId) ? inv.length + "+" : inv.length) : null;
    button.textContent = databaseFilterLabel(getDatabaseFilter(serverId).length, offered);
  };

  let picker = null;
  const paintPopover = () => {
    const isOpen = openFor === serverId;
    popover.hidden = !isOpen;
    button.setAttribute("aria-expanded", isOpen ? "true" : "false");
    if (!isOpen) return;
    const inv = inventories.get(serverId);
    if (!inv && !loadFailed) {
      mount(popover, el("div", { class: "db-filter-loading", text: "Loading databases..." }));
      return;
    }
    /* A search answer replaces the list while the box holds its text; clearing the box shows the first page again. */
    const answer = searchAnswers.get(serverId);
    const listed = answer ? answer.names : inv;
    const listCut = answer ? answer.cut : cutInventories.has(serverId);
    const message = el("div", { class: "db-filter-message", role: "alert", text: loadFailed });
    picker = multiPicker({
      key,
      label: "Databases",
      options: listed || [],
      max: MAX_DB_NAMES,
      noun: "database",
      selectAll: false,
      defaultsLabel: null,
      defaults: () => [],
      refuse: byteRefusal,
      onSearch: (text) => searchTyped(text),
      /* The route matched this text itself, so its answer is listed as sent while the box still holds the text (#5314). */
      answeredFor: answer ? answer.query : null,
      onChange: () => {
        message.textContent = "";
      },
    });
    const finish = (names) => {
      if (!setDatabaseFilter(serverId, names)) {
        /* Two reasons a set is refused: it is over a size bound, or it holds a name the filter cannot keep (blank by the
           service's rule, or over 128 characters). Each gets its own sentence. */
        message.textContent = names.some((n) => !isDatabaseName(n))
          ? "That selection holds a name the filter cannot keep (blank, or over 128 characters). Uncheck it and apply again."
          : "That set of databases is too large to keep. Uncheck some and apply again.";
        return;
      }
      openFor = null;
      dropSearch(serverId);
      paintLabel();
      paintPopover();
      onApply();
    };
    const all = el("button", { type: "button", class: "btn db-filter-all", text: "All databases", onClick: () => finish([]) });
    const apply = el("button", { type: "button", class: "btn db-filter-apply", text: "Apply", onClick: () => finish(picker.checked()) });
    /* The route stops at its cap and says so (truncated), so the picker says the list is cut. The search box asks the route for the
       names that contain the text, so the note says to type a name; a search that is itself cut says to type more of it. */
    const cutNote = listCut && listed
      ? el("div", {
        class: "db-filter-cut",
        text: answer
          ? "The search stops at " + listed.length + " databases, so more matches are not shown. Type more of the name to narrow it."
          : "The list stops at " + listed.length + " databases, so more are not shown. Type part of a name in the search box to find it.",
      })
      : null;
    mount(popover, [picker.node, cutNote, message, el("div", { class: "db-filter-actions" }, [all, apply])]);
    picker.restoreFocus();
  };

  /* A keystroke in the search box. An uncut list already holds every name, so the picker's own filter is the whole search. A cut list
     asks the route for the text, after a pause: each keystroke cancels the pending ask and the read in flight, and only the
     newest answer is shown. An empty box drops the answer and shows the first page again. */
  const searchTyped = (text) => {
    if (!cutInventories.has(serverId)) return;
    const query = text.trim();
    clearTimeout(searchTimers.get(serverId));
    const seq = (searchSeqs.get(serverId) || 0) + 1;
    searchSeqs.set(serverId, seq);
    const inFlight = searchAborts.get(serverId);
    if (inFlight) inFlight.abort();
    searchAborts.delete(serverId);
    if (!query) {
      if (searchAnswers.delete(serverId)) painters.get(serverId)();
      return;
    }
    searchTimers.set(serverId, setTimeout(() => runSearch(seq, query), SEARCH_DEBOUNCE_MS));
  };

  const runSearch = async (seq, query) => {
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    if (controller) searchAborts.set(serverId, controller);
    const res = await apiGet(
      "/api/server-databases?server=" + encodeURIComponent(server) + "&search=" + encodeURIComponent(query),
      controller ? controller.signal : undefined);
    if (searchSeqs.get(serverId) !== seq) return; // a newer keystroke owns the list now
    searchAborts.delete(serverId);
    if (res.kind === "data" && res.data && Array.isArray(res.data.databases)) {
      loadFailed = "";
      searchAnswers.set(serverId, { query, names: res.data.databases.filter((d) => typeof d === "string"), cut: res.data.truncated === true });
    } else if (res.kind !== "aborted") {
      searchAnswers.delete(serverId);
      loadFailed = "The search could not be run" + (res.message ? ": " + res.message : ".");
    }
    painters.get(serverId)();
  };

  const load = async () => {
    loadFailed = "";
    const res = await apiGet("/api/server-databases?server=" + encodeURIComponent(server));
    if (res.kind === "data" && res.data && Array.isArray(res.data.databases)) {
      inventories.set(serverId, res.data.databases.filter((d) => typeof d === "string"));
      if (res.data.truncated === true) cutInventories.add(serverId);
      else cutInventories.delete(serverId);
    } else if (res.kind !== "aborted") {
      loadFailed = "The list of databases could not be loaded" + (res.message ? ": " + res.message : ".");
    }
    paintLabel();
    paintPopover();
  };

  button.addEventListener("click", () => {
    if (openFor === serverId) {
      openFor = null;
      dropSearch(serverId);
      paintPopover();
      return;
    }
    openFor = serverId;
    dropSearch(serverId);
    /* Opening starts from the stored choice: an earlier unapplied draft is dropped, as closing the desktop's popup drops it. */
    const state = pickerState(key);
    state.checked = new Set(getDatabaseFilter(serverId));
    state.search = "";
    paintPopover();
    load();
  });

  painters.set(serverId, () => {
    paintLabel();
    paintPopover();
  });
  paintLabel();
  paintPopover();
  /* A rebuild (the 60 s poll) while the popover is open and no inventory is held asks for it again. */
  if (openFor === serverId && !inventories.has(serverId) && !loadFailed) load();
  return { node, label: () => button.textContent };
}

/** Forgets the held inventories and the open flag. For tests. */
export function resetDatabaseFilterState() {
  inventories.clear();
  cutInventories.clear();
  for (const id of [...searchTimers.keys()]) dropSearch(id);
  searchAnswers.clear();
  searchSeqs.clear();
  painters.clear();
  openFor = null;
  loadFailed = "";
}
