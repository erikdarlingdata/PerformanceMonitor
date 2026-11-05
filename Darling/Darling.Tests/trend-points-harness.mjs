/* #7661: what a trend chart asks its read for. Runs the shipped trend-points.js and util.js (readTool, with fetch stubbed so the URL it
   would request is captured) and prints one line of JSON: the `points` the page sends for a panel at a window width, measured or
   estimated, for a rate read, a level read, a table panel and a read that is not a trend.
   TrendPointsPageTests starts it as
       node trend-points-harness.mjs <path to wwwroot/js> */
import { pathToFileURL } from "node:url";
import path from "node:path";

const jsDir = process.argv[2];

/* readTool's request URL is what the server sees, so capture it. */
const requested = [];
globalThis.fetch = async (url) => {
  requested.push(String(url));
  return { ok: true, status: 200, headers: new Map(), json: async () => ({}), text: async () => "{}" };
};

const mod = await import(pathToFileURL(path.join(jsDir, "trend-points.js")).href);
const { plotWidthFor, withPlotPoints, panelPlotPx, TREND_POINT_READS, MEAN_ONLY_READS } = mod;
const util = await import(pathToFileURL(path.join(jsDir, "util.js")).href);

/* The panel is not on the page yet when its read starts (body width 0), so the window width decides. */
const ask = (read, params, windowPx, span, bodyPx = 0) => {
  const out = withPlotPoints(read, params, plotWidthFor(bodyPx, windowPx, span));
  return out && out.points != null ? out.points : null;
};

/* The `points` the page's readTool sends for a read, or null when it sends none. `window` is a 1280 px page, so a fallback that estimates
   from the window (the old behaviour for a caller that named no plot) would show up here. */
globalThis.window = { innerWidth: 1280 };
const sent = async (read, params, options) => {
  requested.length = 0;
  await util.readTool(read, params, undefined, options);
  const m = /[?&]points=(\d+)/.exec(requested[0] || "");
  return m ? Number(m[1]) : null;
};

const params = { server: "s1", hours: 24 };
const result = {
  p1280: ask("get_pg_query_duration_trend", params, 1280, 2),
  p2560: ask("get_pg_query_duration_trend", params, 2560, 2),
  halfWidth2560: ask("get_pg_query_duration_trend", params, 2560, 1),
  measured1114: ask("get_pg_query_duration_trend", params, 1536, 2, 1114 + 80),
  cpu1280: ask("get_cpu_utilization", { server: "s1", hours: 24 }, 1280, 2),
  pgCpu1280: ask("get_pg_cpu_utilization", { server: "s1", hours: 24 }, 1280, 2),
  notATrend: ask("get_alert_history", params, 2560, 2),
  namedWidth: ask("get_pg_query_duration_trend", { ...params, bucket_minutes: 10 }, 2560, 2),
  namedPoints: ask("get_pg_query_duration_trend", { ...params, points: 99 }, 2560, 2),
  unmeasurable: ask("get_pg_query_duration_trend", params, 0, 2),
  untouched: withPlotPoints("get_alert_history", params, 1000) === params,
  keptReads: [...TREND_POINT_READS].sort(),
  meanOnlyReads: [...MEAN_ONLY_READS].sort(),
  /* A get_server_trend metric is a level or a rate; a perfmon counter is a rate only when it ends in /sec. */
  serverLevel: ask("get_server_trend", { ...params, metric: "cpu_scheduler" }, 1280, 2),
  serverRate: ask("get_server_trend", { ...params, metric: "total_waits" }, 1280, 2),
  perfmonGauge: ask("get_perfmon_trend", { ...params, counter_name: "Page life expectancy" }, 1280, 2),
  perfmonRate: ask("get_perfmon_trend", { ...params, counter_name: "Batch Requests/sec" }, 1280, 2),
  /* A saved-view panel measures a plot only when it draws a line. */
  panelLine: panelPlotPx({ viz: "line", span: 2 }, 1114 + 80, 1280),
  panelTable: panelPlotPx({ viz: "table", span: 2 }, 1114 + 80, 1280),
  panelStat: panelPlotPx({ viz: "stat", span: 1 }, 600, 1280),
  panelBandlist: panelPlotPx({ viz: "bandlist", span: 2 }, 1114 + 80, 1280),
  /* The real readTool: a caller that names no plot (a table, a stat) sends none even on a 1280 px page, a line caller sends its own. */
  readToolNoPlot: await sent("get_pg_query_duration_trend", params),
  readToolNoPlotCaller: await sent("get_pg_query_duration_trend", params, {}),
  readToolLinePlot: await sent("get_pg_query_duration_trend", params, { plotPx: 1114 }),
  readToolCpuLinePlot: await sent("get_cpu_utilization", params, { plotPx: 1114 }),
};
console.log(JSON.stringify(result));
