/* Runs the shipped wwwroot/js/plain-text.js on the texts in HARNESS_INPUT (a JSON array of strings) and prints
   {"out": [...]} as one line. WebPlainTextTests starts it as
       node web-plain-text-harness.mjs <path to wwwroot/js>
   The module has no imports, so it is loaded straight from the js folder. */
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];
const mod = await import(pathToFileURL(path.join(jsDir, "plain-text.js")).href);
const inputs = JSON.parse(process.env.HARNESS_INPUT);
console.log(JSON.stringify({ out: inputs.map((t) => mod.plainText(t)), line: mod.NOT_COLLECTED_LINE }));
