/* Runs the web shell's real view-scope seeding (the region of wwwroot/js/pages/views.js from "const viewScopeMemory" to
   "function rememberScope") and prints as one line of JSON the server scope a view opens on.
   ViewSavedServerScopeTests starts it as: node view-scope-harness.mjs <path to views.js> */
import fs from "node:fs";
import vm from "node:vm";

const viewsPath = process.argv[2];
const full = fs.readFileSync(viewsPath, "utf8").replace(/\r\n/g, "\n");
const start = full.indexOf("const viewScopeMemory");
const end = full.indexOf("function rememberScope");
if (start < 0 || end < start) throw new Error("views.js layout changed: the seeding region was not found");

const context = vm.createContext({ Map, String, Array });
vm.runInContext(full.slice(start, end) + "\nthis.seedState = seedState;", context);

const fleet = [
  { value: "sql2025", label: "SQL2025" },
  { value: "sql2022", label: "SQL2022" },
];
const seed = (id, variables) => context.seedState(id, 24, variables, fleet).server;
const sv = (def) => [{ name: "server", dimension: "server", default: def }];

console.log(JSON.stringify({
  none: seed("a", []),
  byName: seed("b", sv("sql2025")),
  byDisplayName: seed("c", sv("SQL2022")),
  list: seed("d", sv("sql2025; SQL2022 ,nope")),
  all: seed("e", sv("All")),
  blank: seed("f", sv("  ")),
  reference: seed("g", sv("$other")),
  unknown: seed("h", sv("nope")),
  twoServerVariables: seed("i", [...sv("sql2025"), { name: "s2", dimension: "server", default: "sql2022" }]),
  otherDimension: seed("j", [{ name: "db", dimension: "database", default: "sql2025" }]),
}));
