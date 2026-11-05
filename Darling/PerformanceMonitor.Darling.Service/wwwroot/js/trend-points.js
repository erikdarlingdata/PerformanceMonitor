/* How many points a trend chart asks its read for (#7661).

   A web trend read used to answer at a fixed budget (1,500 points), so a day came back as 1-minute buckets whatever
   the width of the plot. At 1,114 px that is more than one point per pixel: a statement that runs every 5 minutes
   drew as a solid block, because the line goes from 0 to its one-minute rate and back faster than the plot can
   show. The page now asks for about one point per 2 px of plot, measured when the panel draws; the read picks the
   widest bucket from its ladder that keeps the window within that count (clamped on the server to between 60 and
   its old budget), so a 2560 px window gets more detail than a 1280 px one and neither draws a wall.

   A rate is still the true average over its bucket: the read already computes it per bucket, only the bucket width changes.
   Only a panel that draws a line asks (a table, a stat or a band list has no plot), and only for a rate series: a level chart draws just the
   bucket mean, so it keeps the old one-minute buckets and a short spike keeps its height. Brush-zoom and CSV export work from the points the
   chart was drawn with. */

/* The /api/read tools whose route takes the chart budget (DarlingWebEndpoints.WebReads.cs `ChartBudget`), split by what their chart draws.
   TrendPointsPageTests asserts the two sets together are exactly those routes, so a route added there must be classified here.

   A plot-sized ask makes the buckets wider, and a chart that draws only the bucket's MEAN shows a short spike at its mean. So only a read whose
   series is a true rate over the bucket (a counter delta per second: the mean is the honest value) takes the ask. A level read (CPU, memory,
   tempdb, grants, system_health CPU) keeps the old budget and its one-minute buckets, so a one-minute 100% spike still draws at 100%. */
export const TREND_POINT_READS = new Set([
  "get_lock_wait_trend", /* wait ms per second: a rate */
  "get_pg_query_duration_trend", /* calls and exec time per second: rates */
  "get_pg_io_trend", /* reads, writes and bytes per second: rates */
  "get_pg_database_trend", /* transactions and temp bytes per second: rates */
  "get_wait_trend", /* wait ms per second: a rate */
  "get_file_io_trend", /* latency and MB/s from counter deltas: ratios of summed deltas */
  "get_procedure_duration_trend", /* executions and worker time per second: rates */
  "get_query_duration_trend", /* executions and worker time per second: rates */
  "get_server_trend", /* only its rate metrics, see LEVEL_SERVER_TREND_METRICS */
  "get_perfmon_trend", /* only a per-second counter, see takesPlotPoints */
]);

/* The reads whose chart draws a level as a bucket mean: they never take a plot-sized ask. */
export const MEAN_ONLY_READS = new Set([
  "get_cpu_utilization",
  "get_pg_cpu_utilization",
  "get_tempdb_trend",
  "get_memory_trend",
  "get_memory_grants",
  "get_health_parser_system_health",
]);

/* get_server_trend metrics that are levels (the read averages them per bucket and the chart draws that mean); the others are rates over the bucket. */
export const LEVEL_SERVER_TREND_METRICS = new Set(["cpu_scheduler", "memory_clerks", "plan_cache", "session_stats", "collector_duration", "tempdb_size"]);

/** Whether this read, with these params, may take a plot-sized ask: a rate read, not a level one. A perfmon counter named "<x>/sec" is a rate;
 *  any other is a gauge or a non-rate change, drawn as its bucket mean. */
export function takesPlotPoints(read, params) {
  if (!TREND_POINT_READS.has(read)) return false;
  if (read === "get_server_trend") return !LEVEL_SERVER_TREND_METRICS.has(params && params.metric);
  if (read === "get_perfmon_trend") {
    const name = params && params.counter_name;
    return typeof name === "string" && name.endsWith("/sec");
  }
  return true;
}

/* One point per this many px of plot. */
export const PX_PER_POINT = 2;
/* The line chart's left and right margins (charts.js MARGINS plus its y-axis gutter), taken off a panel's width to get its plot. */
const PLOT_MARGIN_PX = 80;
/* The panel card's own padding, taken off when the width is estimated from the window instead of measured. */
const PANEL_PAD_PX = 32;
/* The left sidebar and the page's own padding, taken off the window's width when the panel is not on the page yet. */
const PAGE_CHROME_PX = 300;
const MIN_PLOT_PX = 120;

/** The point count for a plot `plotPx` wide: one per PX_PER_POINT px, whole, at least one. */
export function pointsForPlot(plotPx) {
  const w = Number(plotPx);
  return Number.isFinite(w) && w > 0 ? Math.max(1, Math.round(w / PX_PER_POINT)) : 0;
}

/** The plot width in px of a panel: measured from its body when that is on the page, else estimated from the window
 *  (the panel is built before it is attached, and its read starts at once). `span` is the panel's grid span (2 = full width). */
export function plotWidthFor(bodyWidthPx, windowWidthPx, span) {
  const measured = Number(bodyWidthPx);
  if (Number.isFinite(measured) && measured > 0) return Math.max(MIN_PLOT_PX, measured - PLOT_MARGIN_PX);
  const win = Number(windowWidthPx);
  if (!Number.isFinite(win) || win <= 0) return 0;
  const content = Math.max(320, win - PAGE_CHROME_PX);
  const panel = span === 2 ? content : content / 2;
  return Math.max(MIN_PLOT_PX, panel - PANEL_PAD_PX - PLOT_MARGIN_PX);
}

/** The plot width in px a panel that is not on the page yet is estimated at, from the window (a full-width panel's unless `span` is 1). A caller
 *  that draws a line chart passes it as `plotPx`; a caller that draws a table, a stat or a level chart passes none, and its read keeps the old budget. */
export function estimatedPlotPx(span = 2) {
  return plotWidthFor(0, typeof window !== "undefined" ? window.innerWidth : 0, span);
}

/** The plot width a saved-view panel asks its read for: its measured or estimated plot when it draws a line, and 0 (no ask) for a table, a stat or a
 *  band list, which have no plot and keep the rows the read always answered. */
export function panelPlotPx(desc, bodyWidthPx, windowWidthPx) {
  return desc && desc.viz === "line" ? plotWidthFor(bodyWidthPx, windowWidthPx, desc.span) : 0;
}

/** `params` for this read with `points` added when it is a rate read taking the chart budget and the caller named neither a
 *  width (`bucket_minutes`) nor its own `points`. Returns `params` itself when nothing is added. */
export function withPlotPoints(read, params, plotPx) {
  if (!takesPlotPoints(read, params)) return params;
  if (params && (params.bucket_minutes != null || params.points != null)) return params;
  const points = pointsForPlot(plotPx);
  return points > 0 ? { ...(params || {}), points } : params;
}
