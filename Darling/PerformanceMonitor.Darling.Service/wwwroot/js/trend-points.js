/* How many points a trend chart asks its read for (#7661).

   A web trend read used to answer at a fixed budget (1,500 points), so a day came back as 1-minute buckets whatever
   the width of the plot. At 1,114 px that is more than one point per pixel: a statement that runs every 5 minutes
   drew as a solid block, because the line goes from 0 to its one-minute rate and back faster than the plot can
   show. The page now asks for about one point per 2 px of plot, measured when the panel draws; the read picks the
   widest bucket from its ladder that keeps the window within that count (clamped on the server to between 60 and
   its old budget), so a 2560 px window gets more detail than a 1280 px one and neither draws a wall.

   A rate is still the true average over its bucket and a peak series is still the worst interval inside it: the
   read already computes both per bucket, only the bucket width changes. */

/* The /api/read tools whose route takes the chart budget (DarlingWebEndpoints.WebReads.cs `ChartBudget`). A panel
   that charts one of these sends `points`; every other read is left exactly as it was. */
export const TREND_POINT_READS = new Set([
  "get_lock_wait_trend",
  "get_cpu_utilization",
  "get_tempdb_trend",
  "get_pg_cpu_utilization",
  "get_pg_query_duration_trend",
  "get_pg_io_trend",
  "get_pg_database_trend",
  "get_wait_trend",
  "get_file_io_trend",
  "get_memory_trend",
  "get_server_trend",
  "get_perfmon_trend",
  "get_procedure_duration_trend",
  "get_query_duration_trend",
  "get_memory_grants",
  "get_health_parser_system_health",
]);

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

/** `params` for this read with `points` added when it is a trend read taking the chart budget and the caller named neither a
 *  width (`bucket_minutes`) nor its own `points`. Returns `params` itself when nothing is added. */
export function withPlotPoints(read, params, plotPx) {
  if (!TREND_POINT_READS.has(read)) return params;
  if (params && (params.bucket_minutes != null || params.points != null)) return params;
  const points = pointsForPlot(plotPx);
  return points > 0 ? { ...(params || {}), points } : params;
}
