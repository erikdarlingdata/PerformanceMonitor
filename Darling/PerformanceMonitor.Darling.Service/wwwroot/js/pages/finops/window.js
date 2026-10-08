/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* The FinOps pages' window control (#5562 R5): the shared time range picker, in its Compact, rolling-only form. Every FinOps read is
   "this many hours back from now" (`hours`, no `as_of`), so the control offers no calendar period, no custom end and nothing under
   an hour, and a range the read cannot honor is greyed out with the reason rather than read differently. The reach is the read's
   own, from the catalog (`max_hours`); a view that reaches further than its read's catalog entry says passes its own. */

import { el } from "../../util.js";
import { pageRangePicker } from "../../page-range.js";
import { relativeSpec, ROLLING_ONLY_MIN_SPAN_MS } from "../../time-range.js";

const HOUR_MS = 3600000;

/**
 * @param {object} opts
 * @param {number} opts.hours the window to start on, in whole hours
 * @param {(hours: number) => void} opts.onChange raised with the whole hours the reader picked
 * @param {number} [opts.reachHours] a fixed reach for the view; prefer `opts.view`
 * @param {string} [opts.view] the get_finops view whose own reach (catalog `view_max_hours`) is longer than the read's `max_hours`
 * @param {number} [opts.minSpanMs] a floor above one hour (a daily read)
 * @param {number} [opts.stepMs] the unit a length must be a whole number of (default one hour)
 * @param {string} [opts.label] the control's label (default "Window")
 * @returns {{ node: HTMLElement, picker: object, hours: () => number }}
 */
export function finopsWindowControl(opts) {
  const label = opts.label || "Window";
  const { picker } = pageRangePicker({
    read: "get_finops",
    spec: relativeSpec(opts.hours * HOUR_MS),
    label,
    compact: true,
    rollingOnly: true,
    minSpanMs: opts.minSpanMs || ROLLING_ONLY_MIN_SPAN_MS,
    stepMs: opts.stepMs,
    reachHours: opts.reachHours,
    view: opts.view,
    /* No collector feeds the FinOps views as one read, so there is no "collected every N minutes" note here. */
    useCatalogInterval: false,
    onChange: () => opts.onChange(currentHours()),
  });
  const currentHours = () => {
    const w = picker.window();
    return w ? w.hours : opts.hours;
  };
  return { node: el("div", { class: "range-control" }, [el("span", { text: label }), picker.node]), picker, hours: currentHours };
}
