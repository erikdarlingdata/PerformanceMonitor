/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { trendSubject } from "../../util.js";


/* Density leads, not size: a huge index at 90% density is fine and a small one at 40% is the finding.
   skipped_reason is last and always present — a blank there is a measurement, a value there is an index
   nobody looked at, and the two must never be confused for one another. */
/* The checkpoint story in reading order: how many, how many were FORCED, and who paid for the writes.
   pct_checkpoints_requested leads the pair because the raw counts mean little apart — twenty checkpoints is
   healthy or alarming entirely depending on how many of them the server asked for.

   buffers_backend is here rather than buried with the other buffer counters because it is the one that
   lands on a user query: a backend writing its own dirty buffer is a query paying for the write. */
/* #7394: the Wait Trend as a chart, the one series of the desktop viewer's PgWaitTrendHost. `counter_reset` is a profile reset
   (pg_wait_sampling_reset_profile() or a restart): the interval reports everything since it, so it is marked rather than read as a spike. */
export function pgWaitTrendSpecs(ctx) {
  const tail = "; estimates from a sampling profiler, so the shape is the finding";
  return [
    {
      viz: "line",
      title: "Wait Trend",
      subtitle: ctx.label + ", the event that dominates this window, over time" + tail,
      /* #7428: names the event the read followed. */
      subtitleFrom: (data) => {
        const subject = trendSubject(data && data.wait_event, data && data.wait_event_source);
        return subject ? ctx.label + ", " + subject + ", over time" + tail : null;
      },
      rowsKey: "points",
      xKey: "collection_time",
      series: [{ key: "estimated_wait_ms_per_second", label: "Est. wait ms/sec" }],
      format: "num2",
      unit: "ms/s",
      span: 2,
      hideEmptySeries: true,
      resetKey: "counter_reset",
      resetLabel: "Profile reset",
      emptyText: "No samples for this wait event in the window. A trend needs at least two snapshots to difference, so a short window is legitimately empty here even while the event is being sampled.",
    },
  ];
}

/* #7428: what the Backends Seen column counts, by the instrument the answer names. Null where the answer does not say, so no claim is made. */
export function pgWaitSamplingBackendsNote(data) {
  if (!data || typeof data !== "object") return null;
  if (data.instrument === "extension_sampled") {
    return "Backends Seen is the number of distinct backends pg_wait_sampling has recorded for that wait since its profile last started, a running total that only rises until a restart or a reset. It is not how many were waiting in this window or at once.";
  }
  if (data.instrument === "service_sampled") {
    return "Backends Seen is the number of distinct backends this service saw in that wait during its last sampling window, not a total since the service started.";
  }
  return null;
}

export const PG_WAIT_SAMPLING_COLUMNS = [
  { key: "event_type", label: "Type" },
  { key: "wait_event", label: "Event" },
  { key: "queryid", label: "Query ID" },
  { key: "samples", label: "Samples", format: "int" },
  { key: "estimated_wait_ms", label: "Est. Wait", format: "ms" },
  /* #7428: not a count of backends waiting. On the extension it is the distinct backends the profile has recorded for the wait since it
     started, so it only rises until a reset, and on the service sampler it is the backends seen in the last window. Hence "Seen", and the
     note above the grid (pgWaitSamplingBackendsNote) says which. */
  { key: "backends", label: "Backends Seen", format: "int" },
  { key: "pct_of_samples", label: "% of Samples", format: "num1" },
];

/* #2629: OS CPU per query shape. Total first because it is the ranking, then the user/system split, because
   system time dominated by kernel work is a different finding from user time dominated by the planner.
   Device bytes are labelled "Device" rather than "Read"/"Write" — they are not logical I/O, and a zero here
   means the page cache served it. */
export const PG_KERNEL_COLUMNS = [
  { key: "queryid", label: "Query ID" },
  { key: "database_name", label: "Database" },
  { key: "cpu_ms", label: "CPU", format: "ms" },
  { key: "user_cpu_ms", label: "User CPU", format: "ms" },
  { key: "system_cpu_ms", label: "System CPU", format: "ms" },
  { key: "pct_of_total_cpu", label: "% of CPU", format: "num1" },
  /* mb, from the _mb fields the tool emits beside the byte counts — "bytes" is not one of the renderer's
     formats, and an unrecognised one falls through to raw text rather than erroring. */
  { key: "device_read_mb", label: "Device Read", format: "mb" },
  { key: "device_write_mb", label: "Device Write", format: "mb" },
  { key: "major_faults", label: "Major Faults", format: "int" },
];

export const PG_WAIT_COLUMNS = [
  { key: "wait_type", label: "Type" },
  { key: "wait_event", label: "Event" },
  { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
  { key: "waits", label: "Waits", format: "int" },
  /* num2 rather than ms: the average is routinely well under a millisecond, and fmtMs rounds those to "0 ms",
     which reads as "no wait" rather than as the small number it is. */
  { key: "avg_wait_time_ms", label: "Avg Wait (ms)", format: "num2" },
  { key: "pct_of_total_wait", label: "% of Wait", format: "num1" },
];

