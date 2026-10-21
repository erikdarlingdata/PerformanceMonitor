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
   the series has no regular cadence (events). */
export function gapThresholdMs(times) {
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

/* Splits ascending positions (indices into times) into runs of neighbours no further apart than thresholdMs. */
export function splitAtGaps(indices, times, thresholdMs) {
  const runs = [];
  let run = null;
  let prev = -1;
  for (const i of indices) {
    if (run === null || times[i] - times[prev] > thresholdMs) {
      run = [];
      runs.push(run);
    }
    run.push(i);
    prev = i;
  }
  return runs;
}
