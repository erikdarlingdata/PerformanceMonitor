/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Collection-gap rule shared by the line charts (charts.js) and the Overview strips (pages/overview-strips.js, #6291). It lives apart from
 * charts.js so a page can use it without the SVG renderer: the harnesses that stand in for charts.js keep the rule real.
 */

/* Collection gaps (#5644 W7). The service host is off overnight, so a series has a stretch with no samples; a line drawn
   point to point would run a straight diagonal across it and read as data. A gap is a jump between two neighbouring
   timestamps of more than GAP_FACTOR times the series' own median interval, never less than GAP_FLOOR_MS (a collector can
   miss a cycle without that being worth a break). Fewer than three points have no normal interval to compare against, so
   they never break. Every time-series line, area and stacked chart goes through this, so no caller inserts null points.
   A break is for a SAMPLED series that went silent, so it needs a cadence to be silent against. A series of events
   (clusters of rows with hours between them) has none: its spacing runs from seconds to hours, the median is tiny, and
   the floor would cut it into lone dots. A series has a cadence when its interquartile spacing is tight (the 75th
   percentile of the intervals is within GAP_FACTOR times the 25th); one with no cadence never breaks (#5644 H4b). */
const GAP_FACTOR = 3;
const GAP_FLOOR_MS = 5 * 60 * 1000;

/* The break threshold, in ms, for ascending timestamps (epoch ms); Infinity when there is nothing to compare against or
   the series has no regular cadence (events).

   A series that joins two grains has a seam (#7508): the Query Store Duration Trend is one point per hour before the instant
   the raw intervals start (routing.raw_from) and one point per Query Store interval from it. One threshold for the whole series
   cannot judge both: once the interval points outnumber the hourly ones the median drops to minutes and every hourly step reads
   as a gap, so the older part drew as lone dots. With a seam (seamMs, epoch ms) that has points on both sides, each side
   gets its own threshold, so the hourly side is judged against an hour and the raw side against its interval, and a real
   outage on either side still breaks. The result is then a function of a neighbouring pair's two times (a pair that
   crosses the seam is judged by the larger threshold). A side too short or too uneven for a spacing of its own borrows
   the other side's, so an outage next to it still breaks, and with neither side able to judge the seam adds nothing.
   Without a seam, or with every point on one side of it, this is the plain number it always was. splitAtGaps takes
   either. */
export function gapThresholdMs(times, seamMs = null, bucketMs = null) {
  /* A trend whose reply states its bucket (#7572) has a known cadence: a step longer than one bucket is a bucket with no reading,
     whatever the readings left in the series look like. Without this, a series thinned to the buckets that had reads (a quiet
     file's latency) has uneven steps, reads as events, and never breaks. */
  if (typeof bucketMs === "number" && Number.isFinite(bucketMs) && bucketMs > 0) return bucketMs;
  if (typeof seamMs === "number" && Number.isFinite(seamMs)) {
    const before = times.filter((t) => t < seamMs);
    const after = times.filter((t) => t >= seamMs);
    if (before.length > 0 && after.length > 0) {
      const beforeMs = gapThresholdMs(before);
      const afterMs = gapThresholdMs(after);
      if (Number.isFinite(beforeMs) || Number.isFinite(afterMs)) {
        const early = Number.isFinite(beforeMs) ? beforeMs : afterMs;
        const late = Number.isFinite(afterMs) ? afterMs : beforeMs;
        const crossing = Math.max(early, late);
        return (earlierMs, laterMs) => (earlierMs >= seamMs ? late : laterMs < seamMs ? early : crossing);
      }
    }
  }
  const deltas = [];
  for (let i = 1; i < times.length; i++) {
    const d = times[i] - times[i - 1];
    if (d > 0) deltas.push(d);
  }
  if (deltas.length < 2) return Infinity;
  deltas.sort((a, b) => a - b);
  const mid = deltas.length >> 1;
  const quantile = (p) => deltas[Math.round((deltas.length - 1) * p)];
  if (quantile(0.75) > GAP_FACTOR * quantile(0.25)) return Infinity;
  const median = deltas.length % 2 ? deltas[mid] : (deltas[mid - 1] + deltas[mid]) / 2;
  return Math.max(GAP_FACTOR * median, GAP_FLOOR_MS);
}

/* A reply's `bucket_minutes` as milliseconds, or null when it states none (#7572).
   A bucket is the chart's width (the Chart budget sizes it from the window, 1 minute for a day), not the collector's cadence: a collector
   scheduled slower than the bucket leaves a row every few buckets, and a step longer than one bucket would break all of them into dots.
   `rowTimes` (epoch ms of EVERY row the reply returned, rows with no figure included, since a row with no figure still marks a
   collection) tells the two apart: when the rows' own median step is longer than the bucket, that cadence is the step, with half
   again for a collection that lands a bucket early or late. When the rows keep the bucket (the usual case) it is the bucket. */
export function bucketMsOf(minutes, rowTimes = null) {
  const m = Number(minutes);
  const bucketMs = minutes != null && minutes !== "" && Number.isFinite(m) && m > 0 ? m * 60000 : null;
  if (bucketMs === null || !Array.isArray(rowTimes)) return bucketMs;
  const times = [...new Set(rowTimes.filter((t) => Number.isFinite(t)))].sort((a, b) => a - b);
  const steps = [];
  for (let i = 1; i < times.length; i++) steps.push(times[i] - times[i - 1]);
  if (!steps.length) return bucketMs;
  steps.sort((a, b) => a - b);
  const mid = steps.length >> 1;
  const median = steps.length % 2 ? steps[mid] : (steps[mid - 1] + steps[mid]) / 2;
  return median > bucketMs ? 1.5 * median : bucketMs;
}

/* Splits ascending positions (indices into times) into runs of neighbours no further apart than thresholdMs: a number, or
   the function gapThresholdMs returns for a series with a seam (it is given the pair's two times). */
export function splitAtGaps(indices, times, thresholdMs) {
  const runs = [];
  let run = null;
  let prev = -1;
  for (const i of indices) {
    if (run === null || times[i] - times[prev] > (typeof thresholdMs === "function" ? thresholdMs(times[prev], times[i]) : thresholdMs)) {
      run = [];
      runs.push(run);
    }
    run.push(i);
    prev = i;
  }
  return runs;
}
