/* Runs the shipped Mute Rules page's decision functions (wwwroot/js/pages/mute-rules.js) under Node and prints
   the result of a scenario as one line of JSON. MuteRulesBehaviourTests starts it as
       node mute-rules-harness.mjs <path to mute-rules.js> <scenario>
   The page's import lines become stand-ins and its `export` keywords are dropped; the functions are the page as shipped. */
import fs from "node:fs";
import vm from "node:vm";

const [pagePath, scenario] = process.argv.slice(-2);
const source = fs.readFileSync(pagePath, "utf8").replace(/^import .*;[ \t]*\r?$/gm, "").replace(/^export /gm, "");
if (/^import /m.test(source)) throw new Error("mute-rules.js layout changed: the harness cannot strip a multi-line import");

const context = vm.createContext({ console, fetch: async () => ({}), window: {} });
vm.runInContext(source + "\nglobalThis.__p = { formToCreateBody, buildPatch, isEmptyCreate, interpretWrite, EMPTY_CREATE_WARNING };", context);
const p = context.__p;
const j = (x) => JSON.parse(JSON.stringify(x));

const original = { id: "r1", server_name: "SRV1", metric_name: "High CPU", reason: "noisy", database_pattern: null, expires_at_utc: null, enabled: true };
const expiring = { ...original, expires_at_utc: "2026-08-01T00:00:00.0000000Z" };
const formOf = (over) => ({ server_name: "SRV1", metric_name: "High CPU", reason: "noisy", database_pattern: "", expires_at_utc: "", ...over });

const scenarios = {
  patch: () => ({
    onlyChanged: j(p.buildPatch(original, formOf({ reason: "now quiet", expires_at_utc: "" }))),
    nothing: j(p.buildPatch(original, formOf({}))),
    withEnabled: Object.keys(p.buildPatch({ ...original, enabled: true }, formOf({ reason: "x" }))).includes("enabled"),
  }),
  clear: () => ({ cleared: j(p.buildPatch(original, formOf({ server_name: "   " }))), expiryCleared: j(p.buildPatch(expiring, formOf({}))) }),
  empty: () => ({
    blank: p.isEmptyCreate(p.formToCreateBody({})),
    reasonOnly: p.isEmptyCreate(p.formToCreateBody({ reason: "x" })),
    scoped: p.isEmptyCreate(p.formToCreateBody({ metric_name: "High CPU" })),
    body: j(p.formToCreateBody({ server_name: " A ", reason: "", metric_name: "M" })),
    warning: p.EMPTY_CREATE_WARNING,
  }),
  writes: () => ({
    exists: j(p.interpretWrite(409, { status: "already_exists", message: "dup", rule_id: "abc" })),
    invalid: j(p.interpretWrite(400, { status: "invalid", message: "'expires_at_utc' must be an ISO-8601 UTC timestamp" })),
    notfound: j(p.interpretWrite(404, { status: "not_found", message: "No mute rule" })),
    readonly: j(p.interpretWrite(403, { error: "forbidden" })),
    created: j(p.interpretWrite(201, { status: "created", mute_rule: { id: "n1" } })),
  }),
};
console.log(JSON.stringify(scenarios[scenario]()));
