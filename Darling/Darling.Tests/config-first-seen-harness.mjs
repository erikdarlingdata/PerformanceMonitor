/* #7662: runs the shipped sql-changes.js and util.js and prints as one line of JSON the "First seen" column's label and cells and the
   "latest snapshot" subtitle with its age. ConfigFirstSeenTests starts it as
       node config-first-seen-harness.mjs <path to wwwroot/js> */
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];
const changes = await import(pathToFileURL(path.join(jsDir, "pages", "server-tabs", "sql-changes.js")).href);
const util = await import(pathToFileURL(path.join(jsDir, "util.js")).href);

const col = changes.SERVER_CHANGE_COLUMNS.find((c) => c.key === "change_time");
const dbCol = changes.DB_CHANGE_COLUMNS.find((c) => c.key === "change_time");
const threeHoursAgo = new Date(Date.now() - 3 * 3600 * 1000 - 60 * 1000).toISOString();
console.log(JSON.stringify({
  label: col.label === dbCol.label ? col.label : "labels differ: " + col.label + " / " + dbCol.label,
  span: col.display({ change_time: "2026-11-05T12:50:45", previous_capture_time: "2026-11-04T18:38:00" }),
  single: dbCol.display({ change_time: "2026-11-05T12:50:45", previous_capture_time: null }),
  subtitle: util.snapshotAgeSubtitle("latest snapshot", { captured_at: threeHoursAgo }),
  otherSubtitle: util.snapshotAgeSubtitle("hourly collection", { captured_at: threeHoursAgo }),
}));
