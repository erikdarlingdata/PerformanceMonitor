/* Runs the shipped util.js formatter registry and prints, as one line of JSON, what the "semaphore" format shows for a few values.
   WebSemaphoreFormatTests starts it as
       node web-semaphore-format-harness.mjs <path to wwwroot/js>
   Nothing is stubbed: util.js is imported as it ships. */
import path from "node:path";
import { pathToFileURL } from "node:url";

const util = await import(pathToFileURL(path.join(process.argv[2], "util.js")).href);
const show = (v) => util.applyFormat("semaphore", v);
console.log(JSON.stringify({
  registered: typeof util.FORMATTERS.semaphore === "function",
  regular: show(0),
  smallQuery: show(1),
  fromText: show("1"),
  nullCell: show(null),
  unknownId: show(7),
}));
