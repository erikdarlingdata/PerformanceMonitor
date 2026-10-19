/* Runs the shipped util.js formatter registry and prints, as one line of JSON, what the "bytes" format shows for a few values.
   WebBytesFormatTests starts it as
       node web-bytes-format-harness.mjs <path to wwwroot/js>
   Nothing is stubbed: util.js is imported as it ships. */
import path from "node:path";
import { pathToFileURL } from "node:url";

const util = await import(pathToFileURL(path.join(process.argv[2], "util.js")).href);
const show = (v) => util.applyFormat("bytes", v);
console.log(JSON.stringify({
  registered: typeof util.FORMATTERS.bytes === "function",
  nullCell: show(null),
  zero: show(0),
  bytes: show(512),
  kb: show(1536),
  mb: show(5 * 1024 * 1024 + 512 * 1024),
  gb: show(3 * 1024 * 1024 * 1024),
  notANumber: show("x"),
}));
