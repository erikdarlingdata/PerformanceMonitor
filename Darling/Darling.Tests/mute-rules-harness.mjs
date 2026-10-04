/* Runs the shipped Mute Rules page's decision functions (wwwroot/js/pages/mute-rules.js) under Node and prints
   the result of a scenario as one line of JSON. MuteRulesBehaviourTests starts it as
       node mute-rules-harness.mjs <path to mute-rules.js> <scenario>
   The page's import lines become stand-ins and its `export` keywords are dropped; the functions are the page as shipped. */
import fs from "node:fs";
import vm from "node:vm";
import path from "node:path";

const [pagePath, scenario] = process.argv.slice(-2);
const source = fs.readFileSync(pagePath, "utf8").replace(/^import .*;[ \t]*\r?$/gm, "").replace(/^export /gm, "");
if (/^import /m.test(source)) throw new Error("mute-rules.js layout changed: the harness cannot strip a multi-line import");

const ctxSource = fs.readFileSync(path.join(path.dirname(pagePath), "..", "mute-context.js"), "utf8").replace(/^export /gm, "");
let expiredCalls = 0;
let nextResponse = { status: 200, text: "" };
const windowStub = { history: { replaceState() {} }, location: { hash: "" } };
const context = vm.createContext({
  console, URLSearchParams, window: windowStub,
  fetch: async () => ({ status: nextResponse.status, text: async () => nextResponse.text }),
  reportSessionExpired: () => { expiredCalls++; },
});
vm.runInContext(source + "\nglobalThis.__p = { formToCreateBody, buildPatch, isEmptyCreate, interpretWrite, EMPTY_CREATE_WARNING, send, closeForm, parsePrefill, openForm: (f) => { form = { errors: {}, ...f }; }, setLast: (v) => { lastPrefillQuery = v; }, getLast: () => lastPrefillQuery };", context);
const ctx2 = vm.createContext({});
vm.runInContext(ctxSource + "\nglobalThis.__c = { mutePrefillParams, parseDetailContext };", ctx2);
const c = ctx2.__c;
const utilSource = fs.readFileSync(path.join(path.dirname(pagePath), "..", "util.js"), "utf8").replace(/\r\n/g, "\n");
const buildQuerySource = /export function buildQuery[\s\S]*?\n}\n/.exec(utilSource)[0].replace(/^export /, "");
const qctx = vm.createContext({ encodeURIComponent });
vm.runInContext(buildQuerySource + "\nglobalThis.__q = buildQuery;", qctx);
const buildQuery = qctx.__q;
/* The body the page POSTs when the user clicks "Mute this alert" on a row and saves the form untouched. */
const bodyFor = (row) => j(p.formToCreateBody(p.parsePrefill(buildQuery(c.mutePrefillParams(row)))));
const p = context.__p;
const j = (x) => JSON.parse(JSON.stringify(x));

const original = { id: "r1", server_name: "SRV1", metric_name: "High CPU", reason: "noisy", database_pattern: null, expires_at_utc: null, enabled: true };
const expiring = { ...original, expires_at_utc: "2026-08-01T00:00:00.0000000Z" };
const formOf = (over) => ({ server_name: "SRV1", metric_name: "High CPU", reason: "noisy", database_pattern: "", expires_at_utc: "", ...over });

const alertRow = (over) => ({ server_id: 7, server_name: "Display Name", stored_server_name: "stored-host", metric_name: "High CPU", detail_text: null, ...over });

const scenarios = {
  bodies: () => ({
    engine: bodyFor(alertRow({})),
    engineDetail: bodyFor(alertRow({ detail_text: "  Database: Sales\n  Wait Type: LCK_M_X" })),
    self: bodyFor(alertRow({ server_id: 0, server_name: "Display", stored_server_name: "Monitor Store", metric_name: "Disk Pressure" })),
  }),
  prefill: () => ({
    engine: j(c.mutePrefillParams(alertRow({}))),
    self: j(c.mutePrefillParams(alertRow({ server_id: 0, server_name: "Display", stored_server_name: "Monitor Store", metric_name: "Disk Pressure" }))),
    oldPayload: j(c.mutePrefillParams(alertRow({ stored_server_name: undefined }))),
    detail: j(c.mutePrefillParams(alertRow({ detail_text: "Alert\n  Database: Sales\n  Wait Type: LCK_M_X\n  Job Name: Nightly\n  Blocked Query: SELECT 1\nFROM t\n  Other: x" }))),
    custom: j(c.mutePrefillParams(alertRow({ metric_name: "Custom:abc", detail_text: "  Database: master" }))),
    longQuery: c.mutePrefillParams(alertRow({ detail_text: "  Query: " + "q".repeat(300) })).query_text_pattern.length,
    parsed: j(p.parsePrefill("?server_id=7&server_name=stored-host&metric_name=High%20CPU")),
    body: j(p.formToCreateBody(p.parsePrefill("?server_id=7&server_name=stored-host&metric_name=High%20CPU"))),
  }),
  expiry: async () => {
    const out = {};
    nextResponse = { status: 401, text: JSON.stringify({ error: "Session expired" }) };
    out.s401 = j(await p.send("POST", "/api/mute-rules", {})); out.after401 = expiredCalls;
    nextResponse = { status: 403, text: "{}" };
    out.s403 = j(await p.send("POST", "/api/mute-rules", {})); out.after403 = expiredCalls;
    nextResponse = { status: 200, text: "<html>sign in</html>" };
    out.html200 = j(await p.send("POST", "/api/mute-rules", {}));
    nextResponse = { status: 200, text: "" };
    out.empty200 = j(await p.send("DELETE", "/api/mute-rules/x"));
    nextResponse = { status: 201, text: JSON.stringify({ status: "created", mute_rule: { id: "n1" } }) };
    out.json201 = j(await p.send("POST", "/api/mute-rules", {}));
    out.calls = expiredCalls;
    return out;
  },
  reopen: () => {
    p.setLast("?server_id=7");
    p.openForm({ mode: "create", values: {} });
    p.closeForm();
    return { last: p.getLast() };
  },
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
console.log(JSON.stringify(await scenarios[scenario]()));
