/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, mount } from "../../util.js";
import { setPanelSignal, getPanelSignal } from "../../panels.js";
import { tablePanel, COLLECTION_LOG_COLUMNS } from "./shared.js";

/* #5227: the collector picked on the Collectors panel, keyed by server at MODULE scope so the 60 s rebuild of the tab
   keeps the choice. The Collection Log is drawn into a slot that redraws itself, so a pick re-requests only that read. */
const collectorPick = new Map();
/* #7416: the status picked above the log, keyed by server the same way. The read takes one status and applies it before its row cap, so
   "Failures" reaches an error from an hour ago that the 200 newest runs (minutes of a busy server's day) never show. */
const statusPick = new Map();
const collectionLogRedraw = new Map();
export const COLLECTION_LOG_STATUSES = ["SUCCESS", "ANALYZED", "SKIPPED", "YIELDED", "ABANDONED", "ERROR", "PERMISSIONS", "EXTENSION_MISSING", "SESSION_MISSING", "WARNING"];
export const FAILURE_STATUS = "ERROR";
/* The desktop viewer shows a collector's last 7 days. */
const COLLECTOR_HISTORY_HOURS = 168;

/* One AbortController per server for the Collection Log slot: a new draw aborts the previous 168-hour read. */
const collectionLogDraws = new Map();

export function pickCollector(server, collector) {
  collectorPick.set(server, collector);
  const redraw = collectionLogRedraw.get(server);
  if (redraw) redraw();
}

/* #7416: the status filter of the Collection Log. "Failures" is one click (the log's ERROR status), the select reaches every other status the
   log stores, and "All runs" clears it. The choice is a read parameter, so it applies before the row cap. */
export function collectionLogStatusBar(status, setStatus) {
  const seg = el("div", { class: "seg-control", role: "group", "aria-label": "Show runs" });
  const option = (label, value) => {
    const on = (status || null) === value;
    const btn = el("button", { class: "seg-opt" + (on ? " active" : ""), type: "button", text: label, "aria-pressed": on ? "true" : "false" });
    btn.addEventListener("click", () => setStatus(value));
    return btn;
  };
  seg.appendChild(option("All runs", null));
  seg.appendChild(option("Failures", FAILURE_STATUS));
  const select = el("select", { class: "editor-select", "aria-label": "Run status" }, [
    el("option", { value: "", text: "Any status" }),
    ...COLLECTION_LOG_STATUSES.map((s) => el("option", { value: s, text: s })),
  ]);
  select.value = status || "";
  /* #7690: `editor-select` is full width for editor forms; here it sizes to its text and sits on the buttons' row. */
  select.style.width = "auto";
  select.addEventListener("change", () => setStatus(select.value || null));
  const bar = el("div", { class: "log-status-bar" }, [seg, select]);
  Object.assign(bar.style, { gridColumn: "1 / -1", display: "flex", gap: "0.6rem", alignItems: "center", flexWrap: "wrap" });
  return bar;
}

export function collectionLogPanel(server, ctx) {
  const slot = el("div", {}, []);
  slot.style.display = "contents";
  const draw = () => {
    const previous = collectionLogDraws.get(server);
    if (previous) previous.abort();
    const mine = new AbortController();
    collectionLogDraws.set(server, mine);
    const outer = getPanelSignal();
    if (outer) outer.addEventListener("abort", () => mine.abort(), { once: true });
    const picked = collectorPick.get(server);
    const status = statusPick.get(server) || null;
    const params = picked ? { server, hours: COLLECTOR_HISTORY_HOURS, limit: 200, collector_name: picked } : { server, hours: ctx.hours, limit: 200 };
    if (status) params.status = status;
    const setStatus = (value) => {
      if (value) statusPick.set(server, value);
      else statusPick.delete(server);
      draw();
    };
    const chip = picked
      ? el(
          "button",
          { class: "chip", type: "button", title: "Show every collector again", onClick: () => { collectorPick.delete(server); draw(); } },
          ["collector " + picked + " \u00d7"]
        )
      : null;
    if (chip) Object.assign(chip.style, { gridColumn: "1 / -1", justifySelf: "start" });
    const statusBar = collectionLogStatusBar(status, setStatus);
    setPanelSignal(mine.signal);
    const logPanel = tablePanel(
        "Collection Log",
        "get_collection_log",
        params,
        "runs",
        COLLECTION_LOG_COLUMNS,
        (picked ? "runs of " + picked + ", newest first, over the last 7 days" : "individual runs, newest first, over the selected window") + (status ? ", status " + status : ""),
        (picked ? "No runs of " + picked + " in the last 7 days" : "No collector runs in the selected window") + (status ? " with status " + status : "") + "."
    );
    setPanelSignal(outer);
    mount(slot, [statusBar, chip, logPanel]);
  };
  collectionLogRedraw.set(server, draw);
  draw();
  return slot;
}

