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
const collectionLogRedraw = new Map();
/* The desktop viewer shows a collector's last 7 days. */
const COLLECTOR_HISTORY_HOURS = 168;

/* One AbortController per server for the Collection Log slot: a new draw aborts the previous 168-hour read. */
const collectionLogDraws = new Map();

export function pickCollector(server, collector) {
  collectorPick.set(server, collector);
  const redraw = collectionLogRedraw.get(server);
  if (redraw) redraw();
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
    const params = picked ? { server, hours: COLLECTOR_HISTORY_HOURS, limit: 200, collector_name: picked } : { server, hours: ctx.hours, limit: 200 };
    const chip = picked
      ? el(
          "button",
          { class: "chip", type: "button", title: "Show every collector again", onClick: () => { collectorPick.delete(server); draw(); } },
          ["collector " + picked + " \u00d7"]
        )
      : null;
    if (chip) Object.assign(chip.style, { gridColumn: "1 / -1", justifySelf: "start" });
    setPanelSignal(mine.signal);
    const logPanel = tablePanel(
        "Collection Log",
        "get_collection_log",
        params,
        "runs",
        COLLECTION_LOG_COLUMNS,
        picked ? "runs of " + picked + ", newest first, over the last 7 days" : "individual runs, newest first, over the selected window",
        picked ? "No runs of " + picked + " in the last 7 days." : "No collector runs in the selected window."
    );
    setPanelSignal(outer);
    mount(slot, [chip, logPanel]);
  };
  collectionLogRedraw.set(server, draw);
  draw();
  return slot;
}

