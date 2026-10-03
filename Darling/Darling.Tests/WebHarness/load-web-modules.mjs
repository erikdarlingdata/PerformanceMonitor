import { readdirSync } from "node:fs";
import { pathToFileURL } from "node:url";

/* Loads EVERY module under wwwroot/js (app.js included, each file on its own) the way the browser does, with a
   stubbed DOM and fetch. Catches a link-time missing export and an unimported name used at module top level.
   A name used only inside a function body does not throw here; the source pin in WebModuleLoadTests covers that.
   Every stub is inert: no timer fires, no request leaves, no listener runs. */
const noop = () => {};
const node = () => ({
  style: {}, classList: { add: noop, remove: noop, toggle: noop, contains: () => false }, appendChild: noop, append: noop,
  prepend: noop, remove: noop, replaceChildren: noop, setAttribute: noop, removeAttribute: noop, addEventListener: noop,
  removeEventListener: noop, focus: noop, querySelector: () => null, querySelectorAll: () => [], children: [], dataset: {},
});
globalThis.document = {
  createElement: node, createElementNS: node, createTextNode: node, createDocumentFragment: node,
  getElementById: node, querySelector: () => null, querySelectorAll: () => [], addEventListener: noop,
  removeEventListener: noop, body: node(), documentElement: node(), cookie: "", hidden: false,
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState: noop, replaceState: noop, back: noop };
globalThis.fetch = async () => ({ ok: false, status: 500, json: async () => ({}), text: async () => "" });
const store = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.localStorage = store;
globalThis.sessionStorage = store;
globalThis.addEventListener = noop;
globalThis.removeEventListener = noop;
globalThis.matchMedia = () => ({ matches: false, addEventListener: noop, removeEventListener: noop });
globalThis.requestAnimationFrame = noop;
globalThis.cancelAnimationFrame = noop;
globalThis.setInterval = () => 0;
globalThis.clearInterval = noop;
globalThis.setTimeout = () => 0;
globalThis.clearTimeout = noop;

function walk(dir) {
  const out = [];
  for (const e of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const p = dir + "/" + e.name;
    if (e.isDirectory()) out.push(...walk(p));
    else if (e.name.endsWith(".js")) out.push(p);
  }
  return out;
}

const root = process.argv[2];
const files = walk(root);
let count = 0;
for (const f of files) {
  const name = f.slice(root.length + 1);
  let m;
  try {
    m = await import(pathToFileURL(f).href);
  } catch (e) {
    console.error("module " + name + " failed to load: " + (e && e.stack ? e.stack.split("\n").slice(0, 3).join(" | ") : e));
    process.exit(1);
  }
  if (name.startsWith("pages/finops/")) {
    if (!m.tab || typeof m.tab.build !== "function" || !m.tab.id) {
      console.error(name + " does not export tab = { id, build }");
      process.exit(1);
    }
  } else if (name === "pages/finops.js" && (typeof m.renderFinops !== "function" || !Array.isArray(m.FINOPS_TABS))) {
    console.error(name + " does not export renderFinops and FINOPS_TABS");
    process.exit(1);
  }
  console.log("loaded " + name);
  count++;
}
console.log("loaded " + count + " modules");
process.exit(0);
