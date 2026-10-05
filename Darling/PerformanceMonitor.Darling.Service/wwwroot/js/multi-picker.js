/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * A checkbox list with a search box, Select All / Clear All / Top waits buttons and an optional metric switch: the
 * web twin of the desktop viewer's wait-type picker. It is pure UI state plus DOM; the caller owns the reads and the
 * chart, and is told through onChange when the checked set or the metric changed.
 *
 * The 60 s poll rebuilds the tab, so the checked set, the search text and the metric live at MODULE scope, keyed by
 * the caller's key (the server), and a rebuilt picker reads them back. The checked set starts as the caller's defaults
 * and stays the reader's once they touch it, even when it is empty. A checked value the rebuilt options no longer hold
 * (a wait that fell out of the top list on this poll) stays checked and stays listed after the options, so a poll never
 * unchecks what the reader chose. The search box's focus and caret are held too: the rebuild replaces the input, and
 * restoreFocus() puts the caret back in the new one when the old one had focus.
 *
 * R4: every label reaches the DOM through el's text path.
 */

import { el, mount } from "./util.js";

const states = new Map();

/** The held state for `key`, created empty (checked: null means "not touched yet, use the defaults"). */
export function pickerState(key) {
  let s = states.get(key);
  if (!s) {
    s = { checked: null, search: "", metric: null };
    states.set(key, s);
  }
  return s;
}

/** Forget every held state. For tests. */
export function resetPickerStates() {
  states.clear();
}

/**
 * The desktop's default wait set, bounded by `max`: the resource-starvation waits first, then the usual suspects and
 * the PAGELATCH_ family, then the heaviest of what is left, all drawn only from `available` (heaviest first).
 */
export function topWaitDefaults(available, max) {
  const poison = ["THREADPOOL", "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE"];
  const suspects = ["SOS_SCHEDULER_YIELD", "CXPACKET", "CXCONSUMER", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "WRITELOG"];
  const have = new Set(available);
  const picked = [];
  const add = (w) => {
    if (picked.length < max && have.has(w) && !picked.includes(w)) picked.push(w);
  };
  poison.forEach(add);
  suspects.forEach(add);
  available.filter((w) => w.toUpperCase().startsWith("PAGELATCH_")).forEach(add);
  available.forEach(add);
  return picked;
}

/**
 * Merge per-series trend rows into one row per time. `series` is [{ key, rows }]; each row holds `xKey` and the
 * value under `valueKey`. Rows sharing an x value share one merged row; the result is ordered by x.
 */
export function mergeSeriesRows(series, xKey, valueKey) {
  const byTime = new Map();
  for (const s of series) {
    for (const r of s.rows || []) {
      const t = r[xKey];
      let row = byTime.get(t);
      if (!row) {
        row = { [xKey]: t };
        byTime.set(t, row);
      }
      row[s.key] = r[valueKey];
    }
  }
  return [...byTime.values()].sort((a, b) => (a[xKey] < b[xKey] ? -1 : a[xKey] > b[xKey] ? 1 : 0));
}

/**
 * Build the picker. `opts`: key (state key), label, options (string values, heaviest first), max (cap on checked),
 * metrics (optional [{ value, label }]), onChange(checkedValuesInOptionOrder, metricValue). Optional wording and default
 * set for pickers that are not about waits: noun (default "wait"), defaultsLabel (default "Top waits") and
 * defaults(options, max) (default topWaitDefaults).
 * Returns { node, checked(), metric(), restoreFocus() }: call restoreFocus() once node is in the page.
 */
export function multiPicker(opts) {
  const { key, label, options, max, metrics = null, onChange, noun = "wait", defaultsLabel = "Top waits", defaults: defaultSet = topWaitDefaults } = opts;
  const state = pickerState(key);
  const defaults = () => defaultSet(options, max);
  state.checked = state.checked ? new Set(state.checked) : new Set(defaults());
  const known = new Set(options);
  /* The listed values: the caller's options, then any checked value they no longer hold. */
  const listedValues = [...options, ...[...state.checked].filter((v) => !known.has(v))];
  if (metrics && metrics.length && !metrics.some((m) => m.value === state.metric)) state.metric = metrics[0].value;

  const search = el("input", { type: "search", class: "mp-search", "aria-label": "Search " + label, placeholder: "Search" });
  search.value = state.search;
  const count = el("span", { class: "mp-count" });
  const hint = el("span", { class: "mp-hint" });
  const list = el("div", { class: "mp-list", role: "group", "aria-label": label });

  const visible = () => {
    const q = state.search.trim().toLowerCase();
    return q ? listedValues.filter((o) => o.toLowerCase().includes(q)) : listedValues;
  };
  const changed = () => {
    render();
    onChange(listedValues.filter((o) => state.checked.has(o)), state.metric);
  };
  const render = () => {
    const full = state.checked.size >= max;
    count.textContent = state.checked.size + " / " + max + " selected";
    hint.textContent = full ? "Limit reached: uncheck a " + noun + " to pick another." : "";
    const rows = visible().map((o) => {
      const on = state.checked.has(o);
      const box = el("input", { type: "checkbox", "aria-label": o });
      box.checked = on;
      box.disabled = full && !on;
      box.addEventListener("change", () => {
        if (box.checked) {
          if (state.checked.size >= max) {
            box.checked = false;
            return;
          }
          state.checked.add(o);
        } else {
          state.checked.delete(o);
        }
        changed();
      });
      return el("label", { class: "mp-item" }, [box, el("span", { text: o })]);
    });
    mount(list, rows.length ? rows : el("div", { class: "mp-none", text: "No " + noun + " matches the search." }));
  };

  search.addEventListener("input", () => {
    state.search = search.value;
    render();
  });
  /* The rebuild replaces the input. When the one still in the page had focus, remember its caret now, while it is
     still there, and put both back on the new input once it is mounted. */
  const was = typeof document !== "undefined" ? document.activeElement : null;
  const hadFocus = !!(was && was.dataset && was.dataset.mpKey === key);
  const caret = hadFocus ? { start: was.selectionStart, end: was.selectionEnd } : null;
  search.dataset.mpKey = key;
  const restoreFocus = () => {
    if (!hadFocus) return;
    search.focus();
    if (caret.start != null && typeof search.setSelectionRange === "function") search.setSelectionRange(caret.start, caret.end);
  };
  const button = (text, handler) => el("button", { type: "button", class: "btn mp-btn", text, onClick: handler });
  const selectAll = button("Select All", () => {
    for (const o of visible()) if (state.checked.size < max) state.checked.add(o);
    changed();
  });
  const clearAll = button("Clear All", () => {
    for (const o of visible()) state.checked.delete(o);
    changed();
  });
  const top = button(defaultsLabel, () => {
    state.checked = new Set(defaults());
    changed();
  });

  let metricSelect = null;
  if (metrics && metrics.length) {
    metricSelect = el("select", { class: "range-select-inline", "aria-label": "Metric" }, metrics.map((m) => el("option", { value: m.value, text: m.label })));
    metricSelect.value = state.metric;
    metricSelect.addEventListener("change", () => {
      state.metric = metricSelect.value;
      changed();
    });
  }

  render();
  const node = el("div", { class: "multi-picker" }, [
    el("div", { class: "mp-bar" }, [
      el("span", { class: "mp-label", text: label }),
      search,
      selectAll,
      clearAll,
      top,
      metricSelect ? el("label", { class: "range-control" }, [el("span", { text: "Metric" }), metricSelect]) : null,
      count,
    ]),
    hint,
    list,
  ]);
  return { node, checked: () => listedValues.filter((o) => state.checked.has(o)), metric: () => state.metric, restoreFocus };
}
