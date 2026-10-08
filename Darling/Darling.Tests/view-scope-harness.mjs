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
vm.runInContext(
  full.slice(start, end) +
    "\nthis.seedState = seedState; this.panelFilterServer = panelFilterServer; this.panelUnderScope = panelUnderScope; this.panelServerNote = panelServerNote;",
  context,
);

const fleet = [
  { value: "sql2025", label: "SQL2025" },
  { value: "sql2022", label: "SQL2022" },
];
const seed = (id, variables, panelServer) => context.seedState(id, 24, variables, fleet, panelServer).server;
const sv = (def) => [{ name: "server", dimension: "server", default: def }];

/* W4b: a view with NO server variable whose every composed panel filters to one server (custom view 3's shape). */
const eq = (v) => ({ dimension: "server", op: "eq", value: v });
const composed = (...filters) => ({ source: "wait_stats", measure: "x", filters });
const panelsServer = (panels) => { const r = context.panelFilterServer(panels, fleet); return r ? r.value : null; };
const wholeView = [composed(eq("SQL2025")), composed(eq("SQL2025"), { dimension: "database", op: "eq", value: "d" }), composed(eq("sql2025"))];
const viewServer = context.panelFilterServer(wholeView, fleet);
const under = (p, server) => context.panelUnderScope(p, { server }, viewServer);

console.log(JSON.stringify({
  panelSeed: seed("p1", [], viewServer),
  panelSeedVariableWins: seed("p2", sv("sql2022"), viewServer),
  panelSeedNone: seed("p3", [], null),
  panelServerOfWholeView: panelsServer(wholeView),
  panelServerMixed: panelsServer([composed(eq("SQL2025")), composed(eq("SQL2022"))]),
  panelServerOnePanelUnfiltered: panelsServer([composed(eq("SQL2025")), composed()]),
  panelServerTwoServerFilters: panelsServer([composed(eq("SQL2025"), eq("SQL2022"))]),
  panelServerInOp: panelsServer([composed({ dimension: "server", op: "in", value: "SQL2025" })]),
  panelServerNotInFleet: panelsServer([composed(eq("SQL2019"))]),
  panelServerReadPanelsIgnored: panelsServer([composed(eq("SQL2025")), { read: "get_x", params: {} }]),
  panelServerNoComposed: panelsServer([{ read: "get_x" }]),
  underSame: under(wholeView[1], ["sql2025"]).filters.length,
  underOther: under(wholeView[1], ["sql2022"]).filters.map((f) => f.dimension),
  underAll: under(wholeView[0], "All").filters.length,
  underReadPanel: under({ read: "get_x" }, "All"),
  noteOnServer: context.panelServerNote({ server: ["sql2025"] }, viewServer),
  noteOnOther: context.panelServerNote({ server: "All", picked: true }, viewServer),
  noteOnOtherNotPicked: context.panelServerNote({ server: ["sql2022"], picked: false }, viewServer),
  panelSeedVariableAll: seed("p4", sv("All"), viewServer),
  panelSeedVariableReference: seed("p5", sv("$other"), viewServer),
  panelSeedVariableUnknown: seed("p6", sv("nope"), viewServer),
  panelSeedOtherDimensionVariable: seed("p7", [{ name: "db", dimension: "database", default: "x" }], viewServer),
  pickedFlags: (() => {
    const fresh = context.seedState("m0", 24, [], fleet, viewServer).picked;
    const variable = context.seedState("m0b", 24, sv("sql2022"), fleet, viewServer).picked;
    vm.runInContext('viewScopeMemory.set("m1", { server: "All", hours: 24, values: {}, picked: true }); viewScopeMemory.set("m2", { server: "All", hours: 12, values: {}, picked: false });', context);
    return { fresh, variable, remembered: context.seedState("m1", 24, [], fleet, viewServer).picked, rememberedHoursOnly: context.seedState("m2", 24, [], fleet, viewServer).picked };
  })(),
  panelServerEqPlusIn: panelsServer([composed(eq("SQL2025"), { dimension: "server", op: "in", value: "SQL2022" })]),
  panelServerEqPlusNeq: panelsServer([composed(eq("SQL2025"), { dimension: "server", op: "neq", value: "SQL2022" })]),
  panelServerEqPlusOtherDimension: panelsServer([composed(eq("SQL2025"), { dimension: "database", op: "in", value: "d" })]),
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
