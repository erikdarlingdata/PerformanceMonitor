/* Runs the web page's real read-field catalog lookup (wwwroot/js/read-fields.js, resolveReadTable) on whole cells.
       node read-fields-resolve-harness.mjs <path to read-fields.js> <path to a JSON file holding an array of cells>
   Prints one line of JSON: for each cell, the fields its viz ends up with. read-fields.js is an ES module, so its
   `export` keywords are dropped and the source is run as a plain script. */
import fs from "node:fs";
import vm from "node:vm";

const [fieldsPath, cellsPath] = process.argv.slice(-2);
const source = fs.readFileSync(fieldsPath, "utf8").replace(/^export /gm, "");
if (/^import /m.test(source)) {
  throw new Error("read-fields.js now has an import: the harness cannot strip it");
}
const context = vm.createContext({ console });
vm.runInContext(source, context);

const keysOf = (arr) => (Array.isArray(arr) ? arr.map((c) => c.key) : []);
const cells = JSON.parse(fs.readFileSync(cellsPath, "utf8"));
const resolved = cells.map((cell) => {
  const d = context.resolveReadTable(cell);
  return { read: cell.read, viz: cell.viz, rowsKey: d.rowsKey || null, xKey: d.xKey || null, columns: keysOf(d.columns), series: keysOf(d.series), stats: keysOf(d.stats) };
});
const cat = vm.runInContext("READ_FIELDS", context);
console.log(JSON.stringify({ resolved, catalogReads: Object.keys(cat) }));
