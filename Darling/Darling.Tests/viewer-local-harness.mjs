/* Runs the web dashboard's real per-browser state module (wwwroot/js/viewer-local.js) against a stubbed
   localStorage and prints the result of a scenario as one line of JSON. ViewerLocalStateBehaviourTests starts it as
       node viewer-local-harness.mjs <path to viewer-local.js> <scenario>
   The module has no imports; its `export` keywords are dropped so it runs in a vm context, exactly as shipped. */
import fs from "node:fs";
import vm from "node:vm";

const [path, scenario] = process.argv.slice(-2);
const source = fs.readFileSync(path, "utf8").replace(/^export /gm, "");

function makeStorage(initial = {}, opts = {}) {
  const data = { ...initial };
  return {
    data,
    getItem: (k) => { if (opts.throwOnRead) throw new Error("denied"); return Object.prototype.hasOwnProperty.call(data, k) ? data[k] : null; },
    setItem: (k, v) => { if (opts.throwOnWrite) throw new Error("quota"); data[k] = String(v); },
    removeItem: (k) => { delete data[k]; },
  };
}

/* A fresh module instance over the given storage: this is "the page was rebuilt / reloaded". */
function load(storage) {
  const ctx = vm.createContext({ console, Date, localStorage: storage });
  vm.runInContext(source + "\n;globalThis.api = { " +
    "KEY_FAVORITES, KEY_ACKS, KEY_SEVERITY_COLORS, KEY_SIDEBAR, isFavorite, toggleFavorite, favoritesFirst, updateAttention, attentionFor, " +
    "acknowledge, isAcknowledged, severityColor, setSeverityColor, applySeverityColors, isSidebarCollapsed, setSidebarCollapsed, " +
    "refreshAttention, summarizeAlerts };", ctx);
  return ctx.api;
}

const row = (id, t, extra = {}) => ({ server_id: id, alert_time: t, metric_name: "M", severity: "warning", muted: false, ...extra });
const out = {};

if (scenario === "favorites") {
  const st = makeStorage();
  const a = load(st);
  a.toggleFavorite(3);
  const cards = [{ server_id: 1, display_name: "a" }, { server_id: 3, display_name: "c" }, { server_id: 2, display_name: "b" }];
  const byName = (x, y) => x.display_name.localeCompare(y.display_name);
  out.sorted = cards.slice().sort(a.favoritesFirst(byName)).map((c) => c.server_id);
  const b = load(st); // rebuilt
  out.afterReload = b.isFavorite(3);
  b.toggleFavorite(3);
  out.afterUntoggle = load(st).isFavorite(3);
  out.stored = JSON.parse(st.data[a.KEY_FAVORITES]);
} else if (scenario === "ack") {
  const st = makeStorage();
  const a = load(st);
  a.updateAttention([row(1, "2026-01-01T10:00:00"), row(1, "2026-01-01T10:05:00", { severity: "critical" }), row(2, "2026-01-01T10:00:00", { muted: true }), row(2, "2026-01-01T10:01:00", { severity: "resolution" }), row(4, "2026-01-01T10:00:00", { dismissed: true })]);
  out.before = a.attentionFor(1);
  out.mutedAndResolved = a.attentionFor(2);
  out.dismissed = a.attentionFor(4);
  a.acknowledge(1, Date.parse("2026-01-01T10:10:00Z"));
  out.acked = a.attentionFor(1);
  const b = load(st);
  b.updateAttention([row(1, "2026-01-01T10:00:00"), row(1, "2026-01-01T10:05:00")]); // same rows after a reload
  out.afterReloadSameRows = b.attentionFor(1);
  b.updateAttention([row(1, "2026-01-01T10:20:00"), row(1, "2026-01-01T10:05:00")]); // a newer alert
  out.afterNewer = b.attentionFor(1);
  out.persistedCleared = load(st).isAcknowledged(1);
} else if (scenario === "corrupt") {
  const cases = {
    garbage: "{not json",
    arrayRoot: "[1,2,3]",
    nullRoot: "null",
    foreignVersion: JSON.stringify({ v: 99, ids: [1], acks: { 1: 5 }, colors: { critical: "#112233" }, collapsed: true }),
    wrongTypes: JSON.stringify({ v: 1, ids: "x", acks: [1], colors: "red", collapsed: "yes" }),
    badEntries: JSON.stringify({ v: 1, ids: [1, -2, "3", 4.5, null, 7], acks: { a: 1, "-1": 5, 2: "now", 3: 100 }, colors: { critical: "red", warning: "#12345", info: "url(x)" }, collapsed: 1 }),
  };
  out.results = {};
  for (const [name, raw] of Object.entries(cases)) {
    const st = makeStorage({ "darling.local.favorites.v1": raw, "darling.local.acks.v1": raw, "darling.local.severityColors.v1": raw, "darling.local.sidebar.v1": raw });
    let a;
    try { a = load(st); } catch (e) { out.results[name] = "THREW " + e.message; continue; }
    a.updateAttention([row(1, "2026-01-01T10:00:00"), row(3, "2026-01-01T10:00:00")]);
    out.results[name] = {
      fav: [1, 3, 7].filter(a.isFavorite),
      ack: [1, 2, 3].filter(a.isAcknowledged),
      colors: [a.severityColor("critical"), a.severityColor("warning"), a.severityColor("info")],
      collapsed: a.isSidebarCollapsed(),
    };
  }
  const a = load(makeStorage({}, { throwOnRead: true }));
  out.unreadable = { fav: a.isFavorite(1), collapsed: a.isSidebarCollapsed() };
  const w = load(makeStorage({}, { throwOnWrite: true }));
  w.toggleFavorite(5);
  out.unwritableStillWorksInSession = w.isFavorite(5);
  const n = vm.runInContext(source + ";isFavorite(1)", vm.createContext({ console, Date })); // no localStorage at all
  out.noStorage = n;
} else if (scenario === "colors") {
  const st = makeStorage();
  const a = load(st);
  a.setSeverityColor("critical", "#AABBCC");
  out.good = a.severityColor("critical");
  for (const bad of ["red", "#abc", "#gggggg", "url(javascript:x)", "#aabbcc;x:y", "#aabbccdd", 5, null]) {
    a.setSeverityColor("warning", "#112233");
    a.setSeverityColor("warning", bad);
  }
  out.afterBad = a.severityColor("warning");
  a.setSeverityColor("nonsense", "#112233");
  out.unknownSeverity = a.severityColor("nonsense");
  const props = {};
  a.applySeverityColors({ setProperty: (k, v) => { props[k] = v; }, removeProperty: (k) => { delete props[k]; } });
  out.props = props;
  out.afterReload = load(st).severityColor("critical");
  // a hand-edited stored value is dropped on load
  st.data[a.KEY_SEVERITY_COLORS] = JSON.stringify({ v: 1, colors: { critical: "red; background:url(x)", warning: "#00ff00" } });
  const b = load(st);
  out.handEdited = [b.severityColor("critical"), b.severityColor("warning")];
} else if (scenario === "sidebar") {
  const st = makeStorage();
  const a = load(st);
  out.initial = a.isSidebarCollapsed();
  a.setSidebarCollapsed(true);
  out.afterReload = load(st).isSidebarCollapsed();
  a.setSidebarCollapsed(false);
  out.afterExpand = load(st).isSidebarCollapsed();
} else if (scenario === "oneRead") {
  const st = makeStorage();
  const a = load(st);
  let reads = 0;
  const readTool = async (tool, params) => { reads++; return { kind: "data", data: { alerts: [row(1, "2026-01-01T10:00:00")], tool, params } }; };
  await a.refreshAttention(readTool, 1000);
  await a.refreshAttention(readTool, 2000);
  out.readsWithinTtl = reads;
  out.count = a.attentionFor(1).count;
  await a.refreshAttention(readTool, 100000);
  out.readsAfterTtl = reads;
  await a.refreshAttention(async () => ({ kind: "error" }), 300000);
  out.countAfterFailedRead = a.attentionFor(1).count;
} else {
  throw new Error("unknown scenario " + scenario);
}
console.log(JSON.stringify(out));
