/* Runs the web viewer's shipped config-values.js (#7416 item 3) and prints the From and To cells for each case as one line of JSON.
   ConfigValueWhitespaceTests starts it as
       node config-values-harness.mjs <path to wwwroot/js> */
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];
const mod = await import(pathToFileURL(path.join(jsDir, "pages", "server-tabs", "config-values.js")).href);

const cases = [
  { old_value: "%m [%p] ", new_value: "%m [%p]" },
  { old_value: "a\tb", new_value: "a b" },
  { old_value: "FULL", new_value: "SIMPLE" },
  { old_value: null, new_value: "on" },
  { old_value: "", new_value: " " },
];
console.log(JSON.stringify(cases.map((r) => [mod.configValueCell(r, "old_value", "new_value"), mod.configValueCell(r, "new_value", "old_value")])));
