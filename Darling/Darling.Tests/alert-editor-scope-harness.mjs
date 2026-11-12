/* Runs the shipped alert rule editor's definition <-> model mapping (wwwroot/js/alert-editor.js) under Node and prints, as one
   line of JSON, how the editor reads a saved scope and what a Save would write back for it (#8077): a servers list with no mode,
   a tagId with no mode, both keys with no mode, and an explicit mode beside a key from another mode. AlertEditorScopeBehaviourTests
   starts it as
       node alert-editor-scope-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

/* The editor module only needs a window, a document and storage to import; nothing here renders. */
const noop = () => {};
const element = () => ({ style: {}, dataset: {}, classList: { add: noop, remove: noop, toggle: noop }, setAttribute: noop, appendChild: (c) => c, addEventListener: noop });
globalThis.window = globalThis;
globalThis.document = { createElement: element, createTextNode: (t) => ({ text: String(t) }), addEventListener: noop, body: element(), documentElement: element(), getElementById: () => null, querySelector: () => null };
const storage = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.localStorage = storage;
globalThis.sessionStorage = storage;
globalThis.location = { hash: "#/alert-rules", pathname: "/", search: "", href: "http://localhost/#/alert-rules", origin: "http://localhost" };
globalThis.fetch = async () => { throw new Error("the scope harness makes no requests"); };

/* The mapping functions are module-private, so the shipped scripts are copied aside and the copy of alert-editor.js is given one
   export line; the product file is not touched. */
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "alert-editor-scope-"));
let mod;
try {
  fs.cpSync(jsDir, dir, { recursive: true });
  const file = path.join(dir, "alert-editor.js");
  fs.appendFileSync(file, "\nexport { definitionToModel as __scopeHarnessLoad, modelToDefinition as __scopeHarnessSave };\n");
  mod = await import(pathToFileURL(file).href);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}

const metric = { source: "wait_stats", measure: "wait_time_ms", aggregate: "sum", hours: 1 };
const predicate = { op: "ge", warnThreshold: 1000 };

function roundTrip(scope) {
  const model = mod.__scopeHarnessLoad({ metric, predicate, scope }, "rule", "", true);
  return { scopeMode: model.scopeMode, scopeServers: model.scopeServers, scopeTagId: model.scopeTagId, saved: mod.__scopeHarnessSave(model).scope };
}

console.log(JSON.stringify({
  serversOnly: roundTrip({ servers: ["alpha", "beta"] }),
  tagOnly: roundTrip({ tagId: 7 }),
  both: roundTrip({ servers: ["alpha"], tagId: 7 }),
  explicitAllWithServers: roundTrip({ mode: "all", servers: ["alpha"] }),
}));
