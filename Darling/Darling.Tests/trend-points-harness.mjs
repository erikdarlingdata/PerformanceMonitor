/* #7661: what a trend chart asks its read for. Runs the shipped trend-points.js (pure functions, no DOM) and prints one line of JSON:
   the `points` the page sends for a panel at a window width, measured or estimated, for a trend read and for one that is not.
   TrendPointsPageTests starts it as
       node trend-points-harness.mjs <path to wwwroot/js> */
import { pathToFileURL } from "node:url";
import path from "node:path";

const jsDir = process.argv[2];
const mod = await import(pathToFileURL(path.join(jsDir, "trend-points.js")).href);
const { plotWidthFor, withPlotPoints, TREND_POINT_READS } = mod;

/* The panel is not on the page yet when its read starts (body width 0), so the window width decides. */
const ask = (read, params, windowPx, span, bodyPx = 0) => {
  const out = withPlotPoints(read, params, plotWidthFor(bodyPx, windowPx, span));
  return out && out.points != null ? out.points : null;
};

const params = { server: "s1", hours: 24 };
const result = {
  p1280: ask("get_pg_query_duration_trend", params, 1280, 2),
  p2560: ask("get_pg_query_duration_trend", params, 2560, 2),
  halfWidth2560: ask("get_pg_query_duration_trend", params, 2560, 1),
  measured1114: ask("get_pg_query_duration_trend", params, 1536, 2, 1114 + 80),
  cpu1280: ask("get_cpu_utilization", { server: "s1", hours: 24 }, 1280, 2),
  notATrend: ask("get_alert_history", params, 2560, 2),
  namedWidth: ask("get_pg_query_duration_trend", { ...params, bucket_minutes: 10 }, 2560, 2),
  namedPoints: ask("get_pg_query_duration_trend", { ...params, points: 99 }, 2560, 2),
  unmeasurable: ask("get_pg_query_duration_trend", params, 0, 2),
  untouched: withPlotPoints("get_alert_history", params, 1000) === params,
  readCount: TREND_POINT_READS.size,
};
console.log(JSON.stringify(result));
