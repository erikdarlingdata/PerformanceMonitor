/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The shared time range picker on a page that shows ONE read (#5562, lane L2b): the alerts, sweeps, job history, FinOps, views and
 * view-editor pages. It mounts timeRangePicker with the page's range and fills in what only the catalog knows once /api/catalog
 * answers: the read's reach (`max_hours`, so a range the read refuses is greyed out with the reason, never clamped) and the main
 * collector's actual interval (`collector_interval_minutes`, so a span holding fewer than 3 samples says how often the data is
 * collected, and never widens the range). Until the catalog answers the picker holds the common 168 hour reach and no note.
 *
 *   const { picker, ready } = pageRangePicker({ read: "get_job_history", spec, onChange: (spec, range) => reload() });
 *   host.appendChild(picker.node);
 *   const w = picker.window();   // { hours, asOf, live, startMs, endMs }: send `hours`, and `as_of` when it is not null
 *
 * `rollingOnly` is the FinOps shape (R5): "this many hours back from now", no calendar periods, no custom end, at least an hour.
 */

import { timeRangePicker } from "./time-range-picker.js";
import { reachHours, collectorIntervalMs, resolveSpec, readWindow, browserZone, relativeSpec, ROLLING_PRESETS } from "./time-range.js";
import { getCatalog } from "./views-api.js";
import { apiGet } from "./util.js";

const HOUR_MS = 3600000;
const SERVER_CATALOG_TTL_MS = 5 * 60000;

const serverCatalogs = new Map();

/** The window a read takes for a held spec as of now: `{ hours, asOf, live, startMs, endMs }`, or null when the spec cannot be resolved
 *  (a calendar period under five minutes old). A page that keeps its choice at module scope (so a 60 s poll or a visit elsewhere keeps
 *  it) calls this on every read, so a live range slides with the clock. */
export function windowOfSpec(spec, nowMs = Date.now(), zone = browserZone()) {
  const r = resolveSpec(spec, nowMs, zone);
  return r.ok ? readWindow(r.range, nowMs) : null;
}

/** Like windowOfSpec, but says why a spec cannot be read: `{ window }` or `{ error: message }`. A page shows the message (a Today range
 *  in its first five minutes) instead of quietly reading a different window (#5562 review r1 L3). */
export function windowResultOfSpec(spec, nowMs = Date.now(), zone = browserZone()) {
  const r = resolveSpec(spec, nowMs, zone);
  return r.ok ? { window: readWindow(r.range, nowMs) } : { error: r.error.message };
}

/** One catalog entry by read name from the fleet-wide catalog, or null. */
export async function catalogEntryFor(read) {
  const catalog = await getCatalog();
  return (catalog.reads || []).find((r) => r && r.name === read) || null;
}

/** The catalog with the SERVER's own collector intervals (a per-server schedule row wins), cached per server. Null when it cannot be read. */
export function serverCatalog(server) {
  if (!serverCatalogs.has(server)) {
    /* Only a catalog that was read is kept (#5562 review r1 L2): a failed fetch is not cached as null for the page's life, and a
       schedule edited in Settings is seen again after the entry expires. */
    const fetched = apiGet("/api/catalog?server=" + encodeURIComponent(server)).then((r) => (r.kind === "data" && r.data ? r.data : null));
    serverCatalogs.set(server, fetched);
    fetched.then((catalog) => {
      if (catalog === null) serverCatalogs.delete(server);
      else setTimeout(() => serverCatalogs.delete(server), SERVER_CATALOG_TTL_MS);
    }, () => serverCatalogs.delete(server));
  }
  return serverCatalogs.get(server);
}

/** The interval, in milliseconds, of one collector on this server as the catalog serves it: every read of a collector carries the same
 *  schedule, so the first windowed read that names the collector answers. Null when none does (no note). */
export function collectorIntervalFromCatalog(catalog, collector) {
  if (!catalog || !collector) return null;
  for (const entry of catalog.reads || []) {
    const param = entry && Array.isArray(entry.params) ? entry.params.find((p) => p && p.name === "hours") : null;
    if (param && param.collector === collector) {
      const ms = collectorIntervalMs(entry);
      if (ms) return ms;
    }
  }
  return null;
}

/**
 * Mounts the picker for one read. `ready` resolves after the catalog entry has set the reach and the sample interval; a page that
 * must wait for the reach (to restore a stored range) can await it.
 *
 * @param {object} opts
 * @param {string} opts.read the catalog read the page calls (its `hours` param carries the reach and the collector's interval)
 * @param {number} [opts.reachHours] a reach the page knows better than the catalog; prefer `opts.view`, which reads it from the catalog
 * @param {string} [opts.view] a view of the read that reaches further than the read does (get_finops storage_growth): its reach is the
 *   catalog's `view_max_hours[view]`, the number the server validates
 * @param {string} [opts.server] ask the catalog for this server's own collector interval
 * @param {boolean} [opts.useCatalogInterval] default true; false for a read that no collector feeds
 * @param {boolean} [opts.offerReach] add the read's reach as the longest quick choice when it is longer than the shared 30 days (Alert History's 90)
 */
export function pageRangePicker(opts) {
  const picker = timeRangePicker(opts);
  const ready = (async () => {
    try {
      const entry = await catalogEntryFor(opts.read);
      const viewMax = opts.view && entry && entry.params ? Number((hoursParam(entry).view_max_hours || {})[opts.view]) : NaN;
      picker.setReach(viewMax > 0 ? viewMax : opts.reachHours > 0 ? opts.reachHours : reachHours(entry));
      if (opts.offerReach) offerReach(picker);
      if (opts.useCatalogInterval === false) return;
      if (opts.server) {
        const interval = collectorIntervalFromCatalog(await serverCatalog(opts.server), entry && entry.params ? hoursParam(entry).collector : null);
        picker.setSampleInterval(interval || collectorIntervalMs(entry));
      } else {
        picker.setSampleInterval(collectorIntervalMs(entry));
      }
    } catch {
      /* A catalog that cannot be read leaves the common reach and no note, exactly the picker's defaults. */
    }
  })();
  return { picker, ready };
}

/** The reach as the longest quick choice, when it is longer than the shared list's 30 days (#5562 review r1 M5, ruling R8). */
function offerReach(picker) {
  const hours = picker.reachHours();
  if (hours * HOUR_MS > ROLLING_PRESETS[ROLLING_PRESETS.length - 1].spanMs) picker.setExtraPresets([relativeSpec(hours * HOUR_MS)]);
}

function hoursParam(entry) {
  return (entry.params || []).find((p) => p && p.name === "hours") || {};
}

/**
 * A rolling-only picker for a page whose setting is "this many hours back from now" and whose reach is its own, not one read's: a
 * Custom View's scope (the compose runner takes up to 90 days) and the view editor's default range. It offers no calendar period
 * and no custom end, because the stored setting is a number of hours, and it greys out what is longer than `reachHours` with the
 * reason instead of clamping it.
 *
 * @param {object} opts
 * @param {number} opts.hours the length to start on, in whole hours
 * @param {number} opts.reachHours the longest length the page takes
 * @param {(hours: number) => void} opts.onChange raised with the whole hours picked
 * @param {string} [opts.label] the accessible name
 */
export function rollingHoursPicker(opts) {
  const picker = timeRangePicker({
    spec: relativeSpec(opts.hours * 3600000),
    label: opts.label || "Time range",
    reachHours: opts.reachHours,
    rollingOnly: true,
    onChange: () => {
      const w = picker.window();
      if (w) opts.onChange(w.hours);
    },
  });
  offerReach(picker);
  return picker;
}
